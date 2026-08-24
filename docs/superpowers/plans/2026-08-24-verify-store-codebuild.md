# Verify Store in CodeBuild Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run `scripts/verify_store.py` in-region via the stack's existing InventoryBuild CodeBuild project, started from a laptop with one flag.

**Architecture:** Reuse the InventoryBuild CodeBuild project instead of adding a second one: a new buildspec (`scripts/verify_buildspec.yml`) is selected at start time with CodeBuild's `--buildspec-override`, the CDK project definition gains the processor env vars and a read-only grant on the store prefix, and the launcher `scripts/build_inventory_remote.sh` — no longer inventory-specific — is renamed to `scripts/run_codebuild.sh` and gains a `-V` flag that starts a verify run instead of an inventory build. Everything else (source zip upload pinned to `git archive HEAD`, EARTHDATA_TOKEN from Secrets Manager, build polling, log pointers) is already built and is reused as-is.

**Tech Stack:** AWS CDK (Python), CodeBuild, bash, uv, pytest with `aws_cdk.assertions`.

**Spec:** No standalone spec. The request: "run the verify_store script in CodeBuild." Requirements derive from `README.md` §Verification (verify runs after promotes and periodically), `scripts/verify_store.py`'s module docstring (its env contract; it explicitly supports CodeBuild — no EC2 IMDS needed), and the existing InventoryBuild pattern (`cdk/stack.py:573-628`, `scripts/inventory_buildspec.yml`, `scripts/build_inventory_remote.sh`).

## Global Constraints

- Work on the existing branch `test/deploy-sandbox` in `/workspace/repos/tempo-virtual-zarr-pipeline` (already checked out; do not create a new branch).
- No new dependencies; Python >= 3.12; run tests with `uv run pytest`.
- Do not `git push` (sandbox denies it); commit locally with conventional-style messages.
- The verifier must never receive write access to the store — the repo deliberately separates writer and reader credentials (README: "the pipeline's own writers never hold chunk-read access"; the converse also holds).
- All shell must pass the repo pre-commit hooks (`uv run pre-commit run --files <changed files>`).

## Design Notes (why reuse, what's skipped)

- **One CodeBuild project, not two.** The InventoryBuild project already has everything expensive: S3 source wiring, the AL2023 image, the EARTHDATA_TOKEN Secrets Manager injection, an 8 h timeout, and a launcher script with account-mismatch guards. Verify differs only in buildspec, env, and IAM — the first is a start-time override, the last two are a few lines in CDK. A second `codebuild.Project` would duplicate all of it. `--buildspec-override` is a first-class `start-build` parameter.
- **Skipped: scheduled (EventBridge) verify runs.** README says "periodically", but nothing today runs on a schedule and the human is actively debugging the test deployment; a manual launcher is the deliverable. Add a `events.Rule` targeting the project when routine cadence is actually wanted.
- **Skipped: separate verify IAM role.** The inventory role gains read on the store prefix. It already writes the inventory prefix; read on the store is strictly less power than the pipeline lambdas hold. A dedicated role is warranted only if the project is ever exposed beyond stack operators.
- **Both `EARTHDATA_TOKEN` and `EARTHDATA_SECRET_ARN` may end up set** on the project (the first from the existing Secrets Manager env var, the second from `processor_env`). That is fine: `virtualizarr_processor.granule` checks `EARTHDATA_TOKEN` first, and the role already has `secretsmanager:GetSecretValue` via the existing `grant_read`.
- **The launcher is renamed (`build_inventory_remote.sh` → `run_codebuild.sh`), the CDK project is not.** Once the script starts verify runs too, its inventory-specific name misleads; the rename touches only four files. The CDK construct id `InventoryBuild` stays: renaming it would replace the deployed CodeBuild project and log group and break the `InventoryBuildProject` stack output, all for a cosmetic gain — a comment noting that verify runs share the project covers it.
- **Timeout/compute reused** (8 h / SMALL). A default verify (8 samples, 5×5 windows) reads a few MB of ranged data; `--completeness` pages CMR listings. Both fit far inside the inventory build's envelope.

---

### Task 1: Verify buildspec

**Files:**
- Create: `scripts/verify_buildspec.yml`

**Interfaces:**
- Consumes: nothing (standalone file, referenced by name from Task 2's tests and Task 3's launcher).
- Produces: a buildspec at the exact path `scripts/verify_buildspec.yml` that runs `uv run scripts/verify_store.py ${VERIFY_ARGS}`; Tasks 2 and 3 hard-code this path.

There is no unit test for buildspecs in this repo (`inventory_buildspec.yml` has none); validity is covered by the pre-commit YAML hook, matching the existing precedent.

- [ ] **Step 1: Create the buildspec**

```yaml
# Buildspec for verify runs of the stack's InventoryBuild CodeBuild project,
# selected per-build with --buildspec-override by
# scripts/run_codebuild.sh -V (the project's default buildspec stays
# scripts/inventory_buildspec.yml). Runs scripts/verify_store.py in-region:
# the processor's obstore registry needs no EC2 IMDS, so s3:// sources work
# from CodeBuild (see the script's docstring). The store location and
# Earthdata material come from the project env (processor env contract plus
# the EARTHDATA_TOKEN secret); VERIFY_ARGS carries extra flags, e.g.
# "--completeness" or "--samples 16".
version: 0.2

phases:
  install:
    commands:
      - curl -LsSf https://astral.sh/uv/install.sh | sh
  build:
    commands:
      - export PATH="$HOME/.local/bin:$PATH"
      - uv run scripts/verify_store.py ${VERIFY_ARGS}
```

Note: `${VERIFY_ARGS}` is deliberately unquoted so `--samples 16` splits into two argv entries; an empty value expands to nothing.

- [ ] **Step 2: Run the pre-commit hooks on the new file**

Run: `cd /workspace/repos/tempo-virtual-zarr-pipeline && uv run pre-commit run --files scripts/verify_buildspec.yml`
Expected: all hooks pass (or report "no hooks ran" for this file type).

- [ ] **Step 3: Commit**

```bash
git add scripts/verify_buildspec.yml
git commit -m "feat: add buildspec for in-region verify_store runs

Selected with --buildspec-override on the InventoryBuild project; the
default buildspec is unchanged."
```

---

### Task 2: CDK — teach InventoryBuild to serve verify runs

**Files:**
- Modify: `cdk/stack.py:573-628` (`_build_inventory_project`)
- Test: `tests/cdk/test_inventory_build.py`

**Interfaces:**
- Consumes: `self.processor_env` (a `dict[str, str]` set in `__init__` at `cdk/stack.py:183-194`, always populated before `_build_inventory_project` is called at line 424); `self.icechunk_bucket` (`s3.IBucket`); `settings.icechunk_storage_prefix` (str, e.g. `"tempo/hcho/v04"`, possibly empty).
- Produces: the InventoryBuild project carries `ICECHUNK_BUCKET`, `ICECHUNK_REGION`, and (when configured) `TEMPO_COLLECTION`, `VIRTUAL_CHUNK_PREFIX`, `ICECHUNK_PREFIX`, `EARTHDATA_SECRET_ARN` as PLAINTEXT env vars, plus an empty-default `VERIFY_ARGS`; its role can read `s3://<icechunk bucket>/<storage prefix>/*`. Task 3's launcher relies on `VERIFY_ARGS` being overridable and the read grant existing.

- [ ] **Step 1: Write the failing tests**

Append to `tests/cdk/test_inventory_build.py`:

```python
def test_inventory_build_carries_processor_env() -> None:
    """Verify runs (scripts/verify_buildspec.yml via --buildspec-override)
    resolve the store from the same env contract as the Lambdas, so the
    project must carry it; VERIFY_ARGS is the per-build flags override."""
    _template().has_resource_properties(
        "AWS::CodeBuild::Project",
        Match.object_like(
            {
                "Environment": Match.object_like(
                    {
                        "EnvironmentVariables": Match.array_with(
                            [
                                {
                                    "Name": "ICECHUNK_BUCKET",
                                    "Type": "PLAINTEXT",
                                    "Value": "ice-test",
                                },
                                {
                                    "Name": "ICECHUNK_PREFIX",
                                    "Type": "PLAINTEXT",
                                    "Value": "tempo/hcho/v04",
                                },
                                {
                                    "Name": "VERIFY_ARGS",
                                    "Type": "PLAINTEXT",
                                    "Value": "",
                                },
                            ]
                        )
                    }
                )
            }
        ),
    )


def test_inventory_build_reads_store_but_never_writes_it() -> None:
    """verify_store.py reads the icechunk store; the project must be able
    to read the storage prefix and must not gain writes outside the
    inventory prefix."""
    from conftest import actions_of, iam_statements, resources_of

    stmts = list(iam_statements(_template(), "inventorybuild"))
    assert any(
        any(a.startswith("s3:Get") for a in actions_of(s))
        and any(r.endswith("ice-test/tempo/hcho/v04/*") for r in resources_of(s))
        for s in stmts
    )
    for s in stmts:
        if any(a.startswith(("s3:Put", "s3:Delete")) for a in actions_of(s)):
            assert all(
                r.endswith("/inventory/*") for r in resources_of(s)
            ), f"unexpected write grant: {s}"
```

(`_template()` and `Match` already exist in this file; `conftest` helpers are the same ones `test_icechunk_grants.py` uses.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd /workspace/repos/tempo-virtual-zarr-pipeline && uv run pytest tests/cdk/test_inventory_build.py -v`
Expected: the two new tests FAIL (missing env vars / no matching read statement); the two existing tests PASS.

- [ ] **Step 3: Implement in `_build_inventory_project`**

In `cdk/stack.py`, extend the `env` dict (currently lines 583-591) — insert after the `"MAX_COUNT"` entry, before the `if settings.EARTHDATA_SECRET_ARN:` block:

```python
            # Extra flags for verify runs (scripts/verify_buildspec.yml via
            # --buildspec-override); empty for inventory builds.
            "VERIFY_ARGS": codebuild.BuildEnvironmentVariable(value=""),
        }
        # Verify runs open the store with the same env contract as the
        # Lambdas. setdefault keeps the Secrets-Manager EARTHDATA_TOKEN
        # entry (added below) authoritative over any plaintext collision.
        for key, value in self.processor_env.items():
            env.setdefault(key, codebuild.BuildEnvironmentVariable(value=value))
```

(the literal `}` above replaces the dict's existing closing brace — the result is the dict literal ending at `"VERIFY_ARGS"`, followed by the loop.)

After the existing `self.icechunk_bucket.grant_put(...)` call (line 616-618), add:

```python
        # Verify runs read the store; nothing in this project ever writes it.
        self.icechunk_bucket.grant_read(
            self.inventory_build,
            f"{settings.icechunk_storage_prefix}/*"
            if settings.icechunk_storage_prefix
            else "*",
        )
```

Extend the method docstring (after the sentence ending "Costs nothing while idle."):

```python
        The same project also runs ``scripts/verify_store.py`` when started
        with ``run_codebuild.sh -V`` (a ``--buildspec-override`` to
        ``scripts/verify_buildspec.yml``), which is why it carries the
        processor env and read access to the store prefix.
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd /workspace/repos/tempo-virtual-zarr-pipeline && uv run pytest tests/cdk/test_inventory_build.py tests/cdk/test_icechunk_grants.py -v`
Expected: ALL PASS (`test_icechunk_grants.py` is included because it sweeps every IAM policy for bucket-wide writes; `grant_read` adds none, so it must stay green).

- [ ] **Step 5: Commit**

```bash
git add cdk/stack.py tests/cdk/test_inventory_build.py
git commit -m "feat: let the InventoryBuild project run verify_store

Carry the processor env (plus a VERIFY_ARGS override slot) on the project
and grant read-only access to the store prefix, so a verify buildspec
override can open the store in-region. No new project: buildspec, env
override, and IAM were the only differences from inventory builds."
```

---

### Task 3: Rename the launcher, add the verify flag, update docs

The script stops being inventory-specific here, so it is renamed. `git mv`
preserves history; only three other files reference the old name (found via
`grep -rln build_inventory_remote --exclude-dir=.git .`): `README.md`,
`scripts/inventory_buildspec.yml` (a comment), and `cdk/stack.py` (a
docstring and a CfnOutput description). The CDK construct id
`InventoryBuild` is deliberately NOT renamed — see Design Notes.

**Files:**
- Rename: `scripts/build_inventory_remote.sh` → `scripts/run_codebuild.sh` (via `git mv`, then modify)
- Modify: `scripts/inventory_buildspec.yml` (comment only)
- Modify: `cdk/stack.py:578` (docstring) and `cdk/stack.py:626-627` (CfnOutput description)
- Modify: `README.md` (every mention of the old name, plus the verification bullet at line 166)

**Interfaces:**
- Consumes: `scripts/verify_buildspec.yml` (Task 1, exact path) and the project's `VERIFY_ARGS` env var (Task 2).
- Produces: `scripts/run_codebuild.sh -e ENV_FILE` (inventory build, behavior unchanged) and `scripts/run_codebuild.sh -e ENV_FILE -V [-a "FLAGS"]` (verify run; exits non-zero if the build — and therefore the verification — fails).

- [ ] **Step 0: Rename the script**

```bash
git mv scripts/build_inventory_remote.sh scripts/run_codebuild.sh
```

- [ ] **Step 1: Add `-V` / `-a` to the launcher**

In `scripts/run_codebuild.sh`:

a. Usage heredoc — change its first line to

```
Usage: run_codebuild.sh -e ENV_FILE [-m MAX_COUNT] [-u S3_URI] [-V [-a ARGS]] [-n]
```

and add after the `-u S3_URI` line:

```
  -V            run scripts/verify_store.py instead of building an inventory
                (starts the same project with scripts/verify_buildspec.yml)
  -a ARGS       extra verify_store.py flags, e.g. -a "--completeness"
                (only meaningful with -V)
```

b. Option parsing — replace

```bash
ENV_FILE=""
MAX_COUNT=""
S3_URI=""
DRY_RUN=""
while getopts ":e:m:u:nh" opt; do
  case "$opt" in
    e) ENV_FILE="$OPTARG" ;;
    m) MAX_COUNT="$OPTARG" ;;
    u) S3_URI="$OPTARG" ;;
    n) DRY_RUN="1" ;;
```

with

```bash
ENV_FILE=""
MAX_COUNT=""
S3_URI=""
DRY_RUN=""
VERIFY=""
VERIFY_ARGS=""
while getopts ":e:m:u:a:Vnh" opt; do
  case "$opt" in
    e) ENV_FILE="$OPTARG" ;;
    m) MAX_COUNT="$OPTARG" ;;
    u) S3_URI="$OPTARG" ;;
    a) VERIFY_ARGS="$OPTARG" ;;
    V) VERIFY="1" ;;
    n) DRY_RUN="1" ;;
```

c. Dry run — add directly after the `echo "CodeBuild project:   $PROJECT" >&2` line:

```bash
  if [ -n "$VERIFY" ]; then
    echo "Mode:                verify (scripts/verify_buildspec.yml${VERIFY_ARGS:+, args: $VERIFY_ARGS})" >&2
  else
    echo "Mode:                inventory build" >&2
  fi
```

d. Start — replace

```bash
OVERRIDES=(name=MAX_COUNT,value="$MAX_COUNT",type=PLAINTEXT)
[ -n "$S3_URI" ] && OVERRIDES+=(name=S3_URI,value="$S3_URI",type=PLAINTEXT)
BUILD_ID="$(aws codebuild start-build ${REGION_ARGS[@]+"${REGION_ARGS[@]}"} \
  --project-name "$PROJECT" \
  --environment-variables-override "${OVERRIDES[@]}" \
  --query 'build.id' --output text)"
```

with

```bash
OVERRIDES=(name=MAX_COUNT,value="$MAX_COUNT",type=PLAINTEXT)
[ -n "$S3_URI" ] && OVERRIDES+=(name=S3_URI,value="$S3_URI",type=PLAINTEXT)
BUILD_ARGS=()
if [ -n "$VERIFY" ]; then
  BUILD_ARGS+=(--buildspec-override scripts/verify_buildspec.yml)
  OVERRIDES+=(name=VERIFY_ARGS,value="$VERIFY_ARGS",type=PLAINTEXT)
fi
BUILD_ID="$(aws codebuild start-build ${REGION_ARGS[@]+"${REGION_ARGS[@]}"} \
  --project-name "$PROJECT" \
  --environment-variables-override "${OVERRIDES[@]}" \
  ${BUILD_ARGS[@]+"${BUILD_ARGS[@]}"} \
  --query 'build.id' --output text)"
```

(the `${ARR[@]+...}` expansion form matches `REGION_ARGS` above and is safe under `set -u` with an empty array.)

e. Header comment — retitle for the widened scope. Replace the opening line

```bash
# Build a backfill inventory in-region via the stack's CodeBuild project.
```

with

```bash
# Run an in-region job via the stack's CodeBuild project: a backfill
# inventory build (default) or a verify_store.py run (-V).
```

replace the `Usage:` line with

```bash
#   scripts/run_codebuild.sh -e ENV_FILE [-m MAX_COUNT] [-u S3_URI] [-V [-a ARGS]] [-n]
```

update the two `scripts/build_inventory_remote.sh` occurrences in the Examples block to `scripts/run_codebuild.sh`, and append to the Examples:

```bash
#   # In-region verification of the deployed store:
#   scripts/run_codebuild.sh -e .env_hcho -V
#   scripts/run_codebuild.sh -e .env_hcho -V -a "--completeness"
```

- [ ] **Step 2: Syntax-check and smoke the launcher**

Run: `cd /workspace/repos/tempo-virtual-zarr-pipeline && bash -n scripts/run_codebuild.sh && bash scripts/run_codebuild.sh -h; echo "exit=$?"`
Expected: no syntax errors; usage text (including the new `-V` and `-a` lines) printed; `exit=2`.

Also run: `uv run pre-commit run --files scripts/run_codebuild.sh`
Expected: hooks pass (shellcheck, if configured, is the real check here).

- [ ] **Step 3: Update the other references to the old name**

In `scripts/inventory_buildspec.yml`, the header comment says "Started, with its source zip, by `scripts/build_inventory_remote.sh`;" — change the script name to `scripts/run_codebuild.sh`.

In `cdk/stack.py` (`_build_inventory_project`): the docstring sentence "``scripts/build_inventory_remote.sh`` uploads ``git archive HEAD`` as the project's source zip..." becomes "``scripts/run_codebuild.sh`` uploads..."; the CfnOutput description "start one with scripts/build_inventory_remote.sh" becomes "start one with scripts/run_codebuild.sh".

In `README.md`, replace every remaining occurrence of `build_inventory_remote.sh` with `run_codebuild.sh` (find them with `grep -n build_inventory_remote README.md`), and rewrite the verification bullet at line 166 from

```
5. Run `uv run --env-file .env_hcho --env-file .env.local scripts/verify_store.py` after the promote (and periodically) to spot-check the store against its sources.
```

to

```
5. Run `uv run --env-file .env_hcho --env-file .env.local scripts/verify_store.py` after the promote (and periodically) to spot-check the store against its sources — or run it in-region with `scripts/run_codebuild.sh -e .env_hcho -V` (add `-a "--completeness"` for extra flags), which starts the stack's CodeBuild project with a verify buildspec override.
```

Gate: `grep -rn build_inventory_remote --exclude-dir=.git --exclude-dir=docs .` from the repo root must return nothing (`docs/` is excluded because plan documents record history).

- [ ] **Step 4: Commit**

```bash
git add -A scripts/ cdk/stack.py README.md
git commit -m "feat: rename launcher to run_codebuild.sh; -V runs verify_store in-region

The script is no longer inventory-specific: -V starts the same CodeBuild
project with a buildspec override (scripts/verify_buildspec.yml) and a
VERIFY_ARGS env override, reusing the source-zip pinning, account guard,
and polling unchanged. Build failure == verification failure. The CDK
construct id InventoryBuild is kept to avoid replacing deployed infra."
```

---

### Task 4: Full-suite gate

**Files:** none (verification only).

**Interfaces:**
- Consumes: everything above.
- Produces: a green tree on `test/deploy-sandbox`.

- [ ] **Step 1: Run the full test suite**

Run: `cd /workspace/repos/tempo-virtual-zarr-pipeline && uv run pytest`
Expected: all tests pass.

- [ ] **Step 2: Run lint and type checks**

Run: `cd /workspace/repos/tempo-virtual-zarr-pipeline && uv run ruff check . && uv run ruff format --check . && uv run mypy cdk`
Expected: clean. (If the repo's pre-commit config runs mypy differently, prefer `uv run pre-commit run --all-files` and expect it clean.)

- [ ] **Step 3: Nothing to commit** — if either step changed files (formatter), re-run the suite and amend the relevant task's commit.

---

## Operator runbook (post-deploy, for the human — not an implementation task)

The sandbox has no AWS credentials, so the deployed behavior is exercised by the human against the test stack:

1. `cdk deploy` (or the repo's usual deploy path) to pick up the project env + grant changes.
2. Dry run first: `scripts/run_codebuild.sh -e .env_hcho -V -n` — confirms account, project, and mode without starting anything.
3. `scripts/run_codebuild.sh -e .env_hcho -V` — uploads `git archive HEAD`, starts the build, polls to completion. Exit 0 == store verified; on failure the script prints the `aws logs tail` command for the discrepancy list (verify_store writes findings to stderr, which lands in the build log).
4. `-a "--completeness"` for the CMR diff; `-a "--offline"` if CMR is flaky.

## Self-review notes

- Env contract covered: `ICECHUNK_BUCKET/REGION/PREFIX`, `TEMPO_COLLECTION`, `VIRTUAL_CHUNK_PREFIX` via `processor_env` merge; Earthdata via the existing `EARTHDATA_TOKEN` Secrets Manager env var (checked first by `granule.py`'s credential chain). `VIRTUAL_CHUNK_REGION` defaults to us-west-2 in `processor.py`, correct for TEMPO.
- IAM covered: store read via new `grant_read`; secret read pre-existing; virtual chunk reads use in-process Earthdata temporary credentials (`icechunk_virtual_credentials`), not the build role, so no grant on `asdc-prod-protected` is needed — same as the Lambdas.
- `Repository.open_or_create` on an existing store only opens; read-only credentials suffice (the store exists in any deployment worth verifying — a missing store fails loudly, which is the correct verify outcome).
- Names consistent across tasks: `scripts/verify_buildspec.yml`, `scripts/run_codebuild.sh`, `VERIFY_ARGS`, `-V`/`-a` appear identically in Tasks 1, 2, and 3.
