# Backfill Run Isolation (Review Findings #8 and #9) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make concurrent/retried backfill executions safe: a second run fails fast instead of corrupting a live one (#8), and a retried worker overwrites its own fork instead of leaving a stale duplicate (#9).

**Architecture:** #9 replaces the worker's `uuid4` fork name with a content-derived deterministic name (sha256 of the batch's file keys), so a Step Functions retry overwrites its own S3 object. #8 gives the backfill branch a real lifecycle: `init` refuses to touch an existing `backfill` branch unless the execution input carries `force: true`, and `promote` deletes the branch after a successful compare-and-swap — so in the happy path the branch never pre-exists and `force` is only ever needed to restart after a *failed* run. The resort branch keeps its reset semantics: overlap there is already ruled out by the resort Lambda's `reserved_concurrent_executions=1`.

**Tech Stack:** Python 3.12, icechunk, AWS CDK (Python), Step Functions, pytest. All commands run from the repo root with `uv run --frozen`.

**Spec:** 2026-08-20 pipeline review, findings #8 and #9 (restated below — this plan is self-contained).

- **#8:** `initialize_backfill_store` resets a pre-existing `backfill` branch (`processor.py` ~line 200) with no execution lock anywhere. Run B starting while run A is live resets the branch under A's workers; A's slot-numbered region writes then land on B's (possibly shifted) axis, the promote gate never checks slot-to-granule bindings, and misplaced references get served from `main`.
- **#9:** `worker.py` uploads its fork as `{uuid4().hex}.pkl` before returning; the SFN retry (`max_attempts=2`) after a post-upload timeout produces two forks for the same batch. `reduce` merges every fork last-writer-wins. If a source granule was republished between attempts, the stale attempt can win and the promoted store's affected time slices fail their checksum on every read.

## Global Constraints

- Work on branch `fix/promote-idempotency-and-batch-write-safety` (exists locally at `500cbc6`, off `main`); Task 5 cherry-picks the new commits onto `air4us-test`.
- Gate before declaring done: `uv run --frozen pytest -q` (expect 256+ passed / 2 skipped), `uv run --frozen ruff check .`, `uv run --frozen ruff format --check .`, `uv run --frozen mypy lambda cdk scripts/verify_store.py`.
- Never read or write `.env_hcho` / `.env_no2` / `.env.local` (hook-enforced).
- `git push` is unavailable in the sandbox; commit locally only.
- Do not put AWS account IDs, bucket names, or function ARNs from operator logs into this repo — it is public.

---

### Task 1: Deterministic fork names (#9)

**Files:**
- Modify: `lambda/backfill/backfill_handlers/worker.py`
- Modify: `cdk/stack_constructs/backfill_pipeline.py` (the stale retry comment above `worker.add_retry`)
- Test: `tests/backfill_handlers/test_worker.py`

**Interfaces:**
- Consumes: worker event shape `{"fork_in_uri": str, "file_keys": list[str], "forks_out_prefix": str}` (unchanged).
- Produces: fork object key = `sha256("\n".join(file_keys)).hexdigest() + ".pkl"` under `forks_out_prefix`. `forks_out_prefix` is already unique per execution and partition, so names cannot collide across runs; distinct batches in a partition have distinct key sets, so they cannot collide within one.

- [ ] **Step 1: Write the failing test**

Append to `tests/backfill_handlers/test_worker.py`:

```python
def test_retried_worker_overwrites_its_own_fork(
    tempo_pipeline: SimpleNamespace,
    lambda_context: MagicMock,
) -> None:
    """A Step Functions retry of the same batch must reuse the same fork
    name (overwriting), never add a second fork for reduce to merge."""
    fork_result = _forked(tempo_pipeline, lambda_context)
    event = {
        "fork_in_uri": fork_result["fork_in_uri"],
        "forks_out_prefix": fork_result["forks_out_prefix"],
        "file_keys": tempo_pipeline.urls[:3],
    }
    first = worker.handler(event, lambda_context)
    second = worker.handler(event, lambda_context)

    assert first["child_fork_uri"] == second["child_fork_uri"]
    assert len(fork_store.list_forks(fork_result["forks_out_prefix"])) == 1

    other = dict(event, file_keys=tempo_pipeline.urls[3:5])
    third = worker.handler(other, lambda_context)
    assert third["child_fork_uri"] != first["child_fork_uri"]
    assert len(fork_store.list_forks(fork_result["forks_out_prefix"])) == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --frozen pytest tests/backfill_handlers/test_worker.py::test_retried_worker_overwrites_its_own_fork -v`
Expected: FAIL on `first["child_fork_uri"] == second["child_fork_uri"]` (uuid4 differs per call).

- [ ] **Step 3: Implement**

In `lambda/backfill/backfill_handlers/worker.py`, replace `import uuid` with `import hashlib` and replace the name line:

```python
# Deterministic, batch-keyed name: an SFN retry that re-runs this batch
# overwrites its own fork instead of leaving a stale sibling for reduce
# to merge last-writer-wins (review finding #9).
batch_key = hashlib.sha256("\n".join(event["file_keys"]).encode()).hexdigest()
child_fork_uri = f"{event['forks_out_prefix']}{batch_key}.pkl"
```

In `cdk/stack_constructs/backfill_pipeline.py`, update the comment above `worker.add_retry` — replace "A retried worker that already saved its fork writes a second, identical one; merge is last-writer-wins." with "A retried worker overwrites its own deterministically-named fork, so the retry can never leave a stale duplicate for reduce."

- [ ] **Step 4: Run tests**

Run: `uv run --frozen pytest tests/backfill_handlers/ tests/cdk/ -q`
Expected: PASS (existing `test_worker_writes_child_fork` asserts prefix + count 1, still holds).

- [ ] **Step 5: Commit**

```bash
git add lambda/backfill/backfill_handlers/worker.py cdk/stack_constructs/backfill_pipeline.py tests/backfill_handlers/test_worker.py
git commit -m "fix: name worker forks by batch content so retries overwrite, not duplicate

Review finding #9: a Step Functions retry after a post-upload timeout
left two differing uuid-named forks for the same batch, and reduce
merges forks last-writer-wins - a granule republished between the two
attempts could promote stale refs that fail checksum on read."
```

---

### Task 2: Init fails fast on an existing backfill branch (#8, guard)

**Files:**
- Modify: `lambda/virtualizarr-processor/virtualizarr_processor/processor.py` (`initialize_backfill_store`, ~lines 183-229; comment in `initialize_resort_store` ~line 268)
- Modify: `lambda/backfill/backfill_handlers/init.py`
- Modify: `tests/stub_processor.py` (mirror the signature)
- Test: `tests/backfill_handlers/test_init.py`

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: `class BackfillBranchExistsError(RuntimeError)` in `virtualizarr_processor/processor.py`; `initialize_backfill_store(self, repo, inventory, *, force: bool = False)`; init handler reads optional `event["force"]` (bool). Task 3 relies on promote deleting the branch so `force` stays exceptional; Task 4 delivers `force` through the state machine.

- [ ] **Step 1: Write the failing tests**

In `tests/backfill_handlers/test_init.py`, replace `test_init_resets_leftover_branch_from_failed_run` with:

```python
def test_init_refuses_existing_branch_without_force(
    tempo_pipeline: SimpleNamespace,
    lambda_context: MagicMock,
) -> None:
    """A pre-existing backfill branch means a live or failed run; a bare
    re-run must fail fast instead of resetting it (review finding #8)."""
    init.handler({"inventory_uri": tempo_pipeline.inventory_uri}, lambda_context)

    with pytest.raises(BackfillBranchExistsError, match="force"):
        init.handler({"inventory_uri": tempo_pipeline.inventory_uri}, lambda_context)


def test_init_force_resets_leftover_branch_from_failed_run(
    tempo_pipeline: SimpleNamespace,
    lambda_context: MagicMock,
) -> None:
    """Restarting after a failed run: force resets the leftover branch."""
    first = init.handler(
        {"inventory_uri": tempo_pipeline.inventory_uri}, lambda_context
    )

    second = init.handler(
        {"inventory_uri": tempo_pipeline.inventory_uri, "force": True},
        lambda_context,
    )

    assert second["branched_from"] == first["branched_from"]  # main untouched
    repo = Processor().open_backfill_repo()
    assert repo.lookup_branch("backfill") == second["base_snapshot"]
    group = zarr.open_group(repo.readonly_session("backfill").store, mode="r")
    np.testing.assert_array_equal(np.asarray(group["time"][:]), tempo_pipeline.times)
```

Add the imports the file needs: `import pytest` and `from virtualizarr_processor.processor import BackfillBranchExistsError`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run --frozen pytest tests/backfill_handlers/test_init.py -v`
Expected: ImportError on `BackfillBranchExistsError` (not defined yet).

- [ ] **Step 3: Implement**

In `virtualizarr_processor/processor.py`, near the top-level definitions:

```python
class BackfillBranchExistsError(RuntimeError):
    """The ``backfill`` branch already exists: a run is live or a failed
    run left it behind. Never reset it blind - see finding #8."""
```

Change the method signature and the branch block (~lines 183-203):

```python
    def initialize_backfill_store(
        self, repo: Repository, inventory: BackfillInventory, *, force: bool = False
    ) -> BranchInit:
```

```python
        main_tip = repo.lookup_branch("main")
        # A pre-existing branch is either a LIVE run (resetting it would let
        # two executions interleave slot writes on different axes - finding
        # #8) or a failed run's leftover (promote deletes the branch on
        # success). Only an explicit force - the operator confirming no
        # execution is RUNNING - may reset it.
        if "backfill" in repo.list_branches():
            if not force:
                raise BackfillBranchExistsError(
                    "the 'backfill' branch already exists - a backfill is "
                    "either running or a previous run failed. Confirm no "
                    "execution is RUNNING, then restart with force "
                    "(start_backfill.sh -f)."
                )
            repo.reset_branch("backfill", main_tip)
        else:
            repo.create_branch("backfill", main_tip)
```

In `initialize_resort_store` (~line 268), above the existing reset, add the one-line comment:

```python
        # Reset is safe here, unlike backfill init: the resort Lambda's
        # reserved_concurrent_executions=1 rules out overlapping runs, and
        # every scheduled fold must be able to reclaim the branch.
```

In `lambda/backfill/backfill_handlers/init.py`, pass the flag through:

```python
    init_result = processor.initialize_backfill_store(
        repo, backfill_inventory, force=bool(event.get("force", False))
    )
```

In `tests/stub_processor.py`, mirror the reference implementation so the stub documents the same mechanics — signature `def initialize_backfill_store(self, repo: Repository, *, force: bool = False) -> BranchInit:` with the same raise-unless-force block (define `BackfillBranchExistsError = RuntimeError` alias or import nothing — in the stub, `raise RuntimeError("backfill branch exists; pass force=True")` is enough; the stub's callers in `tests/test_backfill.py` each use a fresh repo and are unaffected).

- [ ] **Step 4: Run tests**

Run: `uv run --frozen pytest tests/backfill_handlers/ tests/test_backfill.py -q`
Expected: PASS. `test_end_to_end.py` runs init exactly once per repo, unaffected.

- [ ] **Step 5: Commit**

```bash
git add lambda/virtualizarr-processor/virtualizarr_processor/processor.py lambda/backfill/backfill_handlers/init.py tests/backfill_handlers/test_init.py tests/stub_processor.py
git commit -m "fix: refuse to reset an existing backfill branch without force

Review finding #8: init reset the shared backfill branch
unconditionally, so a second execution starting while one was live
would move the axis under the first run's workers and promote
misplaced references. Init now fails fast; restarting after a failed
run takes an explicit force flag in the execution input."
```

---

### Task 3: Promote deletes the backfill branch on success (#8, lifecycle)

**Files:**
- Modify: `lambda/backfill/backfill_handlers/promote.py`
- Test: `tests/backfill_handlers/test_promote.py`

**Interfaces:**
- Consumes: Task 2's fail-fast (this task is what keeps `force` out of the happy path).
- Produces: after a successful promote, `"backfill" not in repo.list_branches()`; a follow-up run's init takes the `create_branch` path with no force.

- [ ] **Step 1: Write the failing test**

Append to `tests/backfill_handlers/test_promote.py`:

```python
def test_promote_deletes_backfill_branch(
    tempo_pipeline: SimpleNamespace,
    lambda_context: MagicMock,
) -> None:
    """After promote, the branch is gone, so the next run's init never
    needs force (branch-exists then always means live or failed)."""
    init_result = init.handler(
        {"inventory_uri": tempo_pipeline.inventory_uri}, lambda_context
    )
    run_workers(tempo_pipeline)
    promote.handler(
        {
            "inventory_uri": tempo_pipeline.inventory_uri,
            "branched_from": init_result["branched_from"],
        },
        lambda_context,
    )

    repo = Processor().open_backfill_repo()
    assert "backfill" not in repo.list_branches()
    # A fresh run now inits cleanly without force.
    again = init.handler(
        {"inventory_uri": tempo_pipeline.inventory_uri}, lambda_context
    )
    assert again["branched_from"] == repo.lookup_branch("main")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --frozen pytest tests/backfill_handlers/test_promote.py::test_promote_deletes_backfill_branch -v`
Expected: FAIL on `"backfill" not in repo.list_branches()`.

- [ ] **Step 3: Implement**

In `lambda/backfill/backfill_handlers/promote.py`, after the `backfill.promote(...)` call and before the final log/return:

```python
    # main now points at the promoted snapshot; the work branch has served
    # its purpose. Deleting it keeps init's branch-exists check meaningful
    # (finding #8): existing == live or failed, never "finished". A failed
    # delete must not fail an execution whose promote already landed - the
    # next init will just ask for force.
    try:
        repo.delete_branch("backfill")
    except Exception:
        logger.warning("promote succeeded but deleting 'backfill' failed")
```

Also extend the module docstring's last paragraph: change "Nothing runs after the CAS." to "Only branch cleanup runs after the CAS, and it cannot fail the execution."

- [ ] **Step 4: Run tests**

Run: `uv run --frozen pytest tests/backfill_handlers/ -q`
Expected: PASS, including `test_promote_gates_then_moves_main_to_backfill_tip` — it asserts `repo.lookup_branch("main") == repo.lookup_branch("backfill")`, which now raises on the deleted branch: update that assertion to compare `main` against the promoted tip instead:

```python
    assert repo.lookup_branch("main") == init_result["base_snapshot"] or True  # see below
```

Do NOT commit that form — the correct edit is: capture the backfill tip before promoting and assert against it:

```python
    repo = Processor().open_backfill_repo()
    expected_tip = repo.lookup_branch("backfill")
    result = promote.handler(...)
    assert result["promoted"] is True
    repo = Processor().open_backfill_repo()
    assert repo.lookup_branch("main") == expected_tip
```

(The existing test's promote call and asserts move around accordingly; `expected_tip` is looked up after `run_workers`.)

- [ ] **Step 5: Commit**

```bash
git add lambda/backfill/backfill_handlers/promote.py tests/backfill_handlers/test_promote.py
git commit -m "fix: delete the backfill branch after a successful promote

With init refusing pre-existing branches (finding #8), the branch must
not outlive a successful run or every second backfill would need
force. Cleanup runs after the CAS and logs instead of raising, so a
delete hiccup cannot fail an already-promoted execution."
```

---

### Task 4: Deliver `force` through the state machine and launcher (#8, plumbing)

**Files:**
- Modify: `cdk/stack_constructs/backfill_pipeline.py` (InitTask, ~lines 172-181)
- Modify: `scripts/start_backfill.sh`
- Test: `tests/cdk/test_backfill_pipeline.py`

**Interfaces:**
- Consumes: init handler's `event.get("force", False)` from Task 2.
- Produces: execution input `{"inventory_uri": ..., "force": true|false}`; `start_backfill.sh -f` sets force. Bare `aws stepfunctions start-execution --input '{"inventory_uri": "s3://..."}'` (the CfnOutput's documented form) keeps working and defaults to fail-fast.

- [ ] **Step 1: Write the failing test**

In `tests/cdk/test_backfill_pipeline.py`, add to `test_state_machine_shape` (which uses the `_state_machine_asl()` helper):

```python
    # InitTask forwards the whole execution state (no Parameters payload),
    # so the optional `force` flag reaches the handler and the documented
    # bare {"inventory_uri": ...} input stays valid. Only Partition and
    # Promote name inventory_uri explicitly.
    assert asl.count('"inventory_uri.$":"$.inventory_uri"') == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --frozen pytest tests/cdk/test_backfill_pipeline.py::test_state_machine_shape -v`
Expected: FAIL (count is 3 — Partition, Init, Promote all name it today).

- [ ] **Step 3: Implement**

In `cdk/stack_constructs/backfill_pipeline.py`, drop InitTask's payload so the Lambda receives the state input as-is:

```python
        init = tasks.LambdaInvoke(
            self,
            "InitTask",
            lambda_function=self.functions["init"],
            # No payload: the handler gets the whole state input, so the
            # optional `force` restart flag flows through without making it
            # a required input key (a bare {"inventory_uri": ...} input
            # must keep working - it's the CfnOutput's documented form).
            payload_response_only=True,
            result_path="$.initResult",
        )
```

In `scripts/start_backfill.sh`: add `f` to getopts as `":s:r:e:fh"`, add `FORCE=false` beside the other defaults, `f) FORCE=true ;;` in the case, document it in the usage heredoc as `-f    restart after a FAILED run: reset the leftover backfill branch`, and change the input line to:

```bash
  --input "{\"inventory_uri\": \"$INVENTORY_URI\", \"force\": $FORCE}"
```

- [ ] **Step 4: Run tests**

Run: `uv run --frozen pytest tests/cdk/ -q && bash -n scripts/start_backfill.sh`
Expected: PASS; `bash -n` exits 0.

- [ ] **Step 5: Commit**

```bash
git add cdk/stack_constructs/backfill_pipeline.py scripts/start_backfill.sh tests/cdk/test_backfill_pipeline.py
git commit -m "feat: thread the backfill force-restart flag through SFN and the launcher

InitTask now forwards the raw execution input so the optional force
flag (finding #8 restart path) reaches the handler; start_backfill.sh
gains -f. A bare inventory_uri-only input still works and fails fast."
```

---

### Task 5: Full gate and deploy-branch cherry-picks

**Files:**
- Modify: none (verification + git surgery only)

**Interfaces:**
- Consumes: the four commits from Tasks 1-4 on `fix/promote-idempotency-and-batch-write-safety`.
- Produces: `air4us-test` carrying the same four commits (it already carries `9d9c23f`, the #1/#4 pick), both branches gate-clean.

- [ ] **Step 1: Full gate on the fix branch**

Run: `uv run --frozen pytest -q && uv run --frozen ruff check . && uv run --frozen ruff format --check . && uv run --frozen mypy lambda cdk scripts/verify_store.py`
Expected: all clean (pytest ≥ 258 passed / 2 skipped).

- [ ] **Step 2: Cherry-pick onto the deploy branch**

```bash
git log --oneline main..fix/promote-idempotency-and-batch-write-safety  # note the 4 new SHAs
git switch air4us-test
git cherry-pick <task1-sha> <task2-sha> <task3-sha> <task4-sha>
```

Expected: clean picks (these files do not differ between `main` and `air4us-test`).

- [ ] **Step 3: Full gate on air4us-test**

Run: `uv run --frozen pytest -q && uv run --frozen ruff check . && uv run --frozen ruff format --check . && uv run --frozen mypy lambda cdk scripts/verify_store.py`
Expected: all clean.

- [ ] **Step 4: Report**

No commit needed. Note for the session summary and human: both branches ready to push; the PR now closes findings #1, #4, #8, #9; the full backfill's remaining prerequisite is operational (drain measurements), not code.

---

## Self-Review Notes

- **Spec coverage:** #9 → Task 1 (deterministic names) — the review's exact recommended fix. #8 → Tasks 2-4 (fail fast + lifecycle + plumbing) — the review offered "fail fast … or a state-machine-level mutex"; fail-fast was chosen because it needs no new AWS resources and the branch itself is the natural lock. The resort half of #8 is addressed by documentation only (Task 2's comment) — its reset is required by the drain loop and already serialized by reserved concurrency 1.
- **Placeholder scan:** none; all code blocks are concrete. Task 3 Step 4 deliberately shows the wrong quick fix and the correct one to stop an executor from taking the shortcut.
- **Type consistency:** `BackfillBranchExistsError` defined in Task 2, imported in Task 2's test only. `force` is a keyword-only bool everywhere (`initialize_backfill_store`, handler event, shell flag renders JSON `true`/`false`). Fork name interface (Task 1) is internal to the worker.
