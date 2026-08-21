# Manifest-in-Icechunk Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the store manifest and pending ledger from side-car S3 JSON files into the icechunk repository itself — manifest as two axis-aligned string arrays plus a root attribute, ledger as a root attribute — so state and data commit atomically and their divergence becomes unrepresentable.

**Architecture:** The store template gains two variable-length-string arrays (`granule_ur`, `granule_url`) on the time dimension; scalar manifest metadata becomes root attribute `tempo_store` and the pending ledger becomes root attribute `pending_ledger`. `manifest.py` is rewritten to (de)serialize `BackfillInventory`/`GranuleEntry` against a zarr store instead of a URI. All producers (backfill init, forward consumer, resort) write state through their session so it rides the same commit as the data; promote becomes a bare validate + compare-and-swap; the resort handler pins one snapshot for all its reads. The repair script, drift validators, and state-URI plumbing are deleted.

**Tech Stack:** Python 3.12, zarr 3.2.1, icechunk 2.1.1, pydantic-zarr, xarray/virtualizarr, AWS CDK, pytest + moto. All pinned in the repo's `.venv`.

**Spec:** `docs/superpowers/specs/2026-08-20-manifest-in-icechunk-design.md` (and the review that motivates it: `/workspace/out/tempo-pipeline-review.md`).

## Global Constraints

- Run everything from the repo root `/workspace/repos/tempo-virtual-zarr-pipeline`; tests with `.venv/bin/pytest`, lint with `.venv/bin/ruff check .`, types with `.venv/bin/mypy .`.
- No new dependencies. dtype for the manifest arrays is zarr's `"str"` (v3 `string` data type, vlen-utf8 codec) — verified working on the pinned zarr 3.2.1 + icechunk 2.1.1, including pydantic-zarr spec round-trip and `resize`.
- `BackfillInventory` / `GranuleEntry` models and their validators are unchanged. `GranuleEntry` fields are exactly `url: str`, `granule_ur: str`, `time: float`.
- The schema id string is `"tempo-backfill-inventory/1"` everywhere.
- Do not touch the CMR poll watermark (stays in S3) or `exploration/`.
- Commit after each task on the branch `claude/manifest-in-icechunk`.

---

### Task 1: Declare the manifest state in the template layer

**Files:**
- Modify: `lambda/virtualizarr-processor/virtualizarr_processor/manifest.py` (add constants at top; rest of file untouched until Task 2)
- Modify: `lambda/virtualizarr-processor/virtualizarr_processor/template.py:56-80` (`build_template`)
- Modify: `lambda/virtualizarr-processor/virtualizarr_processor/store_template.py:186-231` (`validate_store`)
- Modify: `scripts/generate_template.py` (no code change expected — rerun it)
- Regenerate: `lambda/virtualizarr-processor/virtualizarr_processor/collections/{hcho_template.json,no2_template.json,coordinates.npz}`
- Test: `tests/test_store_template.py`, `tests/test_tempo_fixtures.py`

**Interfaces:**
- Consumes: existing `build_template(paths, config)`, `validate_store(spec, group, *, allow_extra)`.
- Produces (later tasks rely on these exact names):
  - `manifest.MANIFEST_ARRAYS: tuple[str, str] = ("granule_ur", "granule_url")`
  - `manifest.STORE_META_ATTRIBUTE = "tempo_store"`
  - `manifest.PENDING_LEDGER_ATTRIBUTE = "pending_ledger"`
  - `manifest.PIPELINE_STATE_ATTRIBUTES: frozenset[str]` (the two attribute names)
  - `validate_store(spec, group, *, allow_extra: bool = False, volatile: Collection[str] = ()) -> None`
  - Templates built by `build_template` contain `/granule_ur` and `/granule_url` string arrays on the append dimension.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_store_template.py`:

```python
def test_template_declares_manifest_arrays(tmp_path: pathlib.Path) -> None:
    import sys

    sys.path.insert(0, str(pathlib.Path(__file__).parent))
    from tempo_fixtures import build_tiny_collection
    from virtualizarr_processor.collection import load_collection, load_template
    from virtualizarr_processor.manifest import MANIFEST_ARRAYS

    tiny = build_tiny_collection(tmp_path / "collection", n=2)
    spec = load_template(load_collection(str(tiny.config_path)))
    flat = spec.to_flat()
    for name in MANIFEST_ARRAYS:
        node = flat[f"/{name}"]
        assert node.data_type == "string"
        assert node.dimension_names == ("time",)
        # time_chunk_size in the tiny fixture's TOML is 8
        assert tuple(node.chunk_grid["configuration"]["chunk_shape"]) == (8,)


def test_validate_store_volatile_silences_state_attributes(
    tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
) -> None:
    import sys

    import icechunk
    import zarr

    sys.path.insert(0, str(pathlib.Path(__file__).parent))
    from tempo_fixtures import build_tiny_collection
    from virtualizarr_processor.collection import load_collection, load_template
    from virtualizarr_processor.manifest import PIPELINE_STATE_ATTRIBUTES
    from virtualizarr_processor.store_template import create_empty_store, validate_store

    tiny = build_tiny_collection(tmp_path / "collection", n=2)
    spec = load_template(load_collection(str(tiny.config_path)))
    repo = icechunk.Repository.create(storage=icechunk.in_memory_storage())
    session = repo.writable_session("main")
    create_empty_store(spec, session.store)
    group = zarr.open_group(session.store, mode="a")
    group.attrs["tempo_store"] = {"schema": "tempo-backfill-inventory/1"}
    group.attrs["pending_ledger"] = []
    with caplog.at_level("WARNING", logger="virtualizarr_processor.store_template"):
        validate_store(
            spec,
            zarr.open_group(session.store, mode="r"),
            allow_extra=True,
            volatile=PIPELINE_STATE_ATTRIBUTES,
        )
    assert "tempo_store" not in caplog.text and "pending_ledger" not in caplog.text
```

(Match the existing import style at the top of `tests/test_store_template.py`; add `import pathlib` / `import pytest` only if missing.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_store_template.py -q -k "manifest_arrays or volatile"`
Expected: FAIL — `ImportError: cannot import name 'MANIFEST_ARRAYS'`.

- [ ] **Step 3: Implement**

At the top of `manifest.py` (after the imports), add:

```python
# The manifest's storage representation inside the store itself: two
# vlen-string arrays on the append dimension, plus two root attributes.
MANIFEST_ARRAYS: tuple[str, str] = ("granule_ur", "granule_url")
STORE_META_ATTRIBUTE = "tempo_store"
PENDING_LEDGER_ATTRIBUTE = "pending_ledger"
PIPELINE_STATE_ATTRIBUTES: frozenset[str] = frozenset(
    {STORE_META_ATTRIBUTE, PENDING_LEDGER_ATTRIBUTE}
)
```

In `template.py`'s `build_template`, immediately after `reference.vz.to_icechunk(session.store, validate_containers=False)` and before `GroupSpec.from_zarr(...)`, create the arrays through the same real write path the rest of the template is captured from:

```python
    from virtualizarr_processor.manifest import MANIFEST_ARRAYS

    # The store's own manifest: one granule UR and source URL per axis slot,
    # captured into the template like every other array.
    for name in MANIFEST_ARRAYS:
        zarr.create_array(
            session.store,
            name=name,
            shape=(1,),
            chunks=(config.time_chunk_size,),
            dtype="str",
            dimension_names=(config.append_dim,),
        )
```

(Move the import to the top of `template.py`; no circular import — `manifest` does not import `template`.)

In `store_template.py`, change `validate_store`'s signature and the attribute call:

```python
def validate_store(
    spec: AnyGroupSpec,
    group: zarr.Group,
    *,
    allow_extra: bool = False,
    volatile: Collection[str] = (),
) -> None:
```

and in its body replace `volatile=(),` with `volatile=volatile,` in the `_attribute_differences(...)` call. Update the docstring's attribute sentence: "Attribute names in ``volatile`` are neither required nor warned about."

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_store_template.py tests/test_tempo_fixtures.py -q`
Expected: PASS.

- [ ] **Step 5: Regenerate the packaged collection artifacts**

Run: `.venv/bin/python scripts/generate_template.py --data-dir /workspace/context/data`
Expected: rewrites `collections/hcho_template.json`, `collections/no2_template.json`, `collections/coordinates.npz`. Verify: `grep -c granule_ur lambda/virtualizarr-processor/virtualizarr_processor/collections/hcho_template.json` prints ≥ 1.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: declare granule_ur/granule_url manifest arrays in the store template"
```

---

### Task 2: Rewrite StoreManifest and PendingLedger against the store

**Files:**
- Modify: `lambda/virtualizarr-processor/virtualizarr_processor/manifest.py` (rewrite; keep `storage_prefix` and the Task 1 constants)
- Test: `tests/test_manifest.py` (rewrite)

**Interfaces:**
- Consumes: Task 1 constants; `BackfillInventory` / `GranuleEntry` from `inventory.py`; a store carrying the template arrays and a written `time` axis.
- Produces (exact signatures later tasks call; `Store` is `zarr.abc.store.Store` — an icechunk `session.store` satisfies it):
  - `StoreManifest.read(store: Store) -> BackfillInventory | None` — `None` when the `tempo_store` attribute is missing or the axis is empty; otherwise reconstructs the inventory (running all its validators) from the meta attribute + `granule_ur`/`granule_url` arrays + `time` axis.
  - `StoreManifest.write(store: Store, inventory: BackfillInventory) -> None` — resizes both arrays to the inventory length, writes all values, sets `tempo_store`.
  - `PendingLedger.read(store: Store) -> tuple[GranuleEntry, ...]`
  - `PendingLedger.write(store: Store, entries: Iterable[GranuleEntry]) -> None`
  - `PendingLedger.append(store: Store, entries: Iterable[GranuleEntry]) -> None` — dedupes by `granule_ur`.
- Deleted (later tasks must not import): `StoreManifest.validate_against_axis`, `default_state_uri`, `_read_bytes`, `_write_bytes`, `_is_s3`, `_split`, `_s3_client`. `storage_prefix()` **stays** (it addresses the repository, and `processor.open_backfill_repo` uses it).

- [ ] **Step 1: Rewrite the tests**

Replace `tests/test_manifest.py` with (keep the existing `test_storage_prefix_combines_like_the_cdk_stack` test verbatim — it still applies; drop `test_default_state_uri_matches_the_deployed_layout`):

```python
"""StoreManifest / PendingLedger round-trips against an icechunk store."""

import icechunk
import numpy as np
import pytest
import zarr
from virtualizarr_processor.inventory import BackfillInventory, GranuleEntry
from virtualizarr_processor.manifest import (
    MANIFEST_ARRAYS,
    PendingLedger,
    StoreManifest,
)


def entry(i: int) -> GranuleEntry:
    return GranuleEntry(url=f"s3://b/g{i}.nc", granule_ur=f"G{i}", time=float(i))


def inventory(n: int) -> BackfillInventory:
    return BackfillInventory(
        schema_id="tempo-backfill-inventory/1",
        collection="TEMPO_HCHO_L3",
        concept_id="C1",
        time_units="seconds since 1980-01-06",
        built_at="2026-08-20T00:00:00Z",
        granules=tuple(entry(i) for i in range(n)),
    )


@pytest.fixture()
def store() -> zarr.abc.store.Store:
    """A minimal store: time axis + manifest arrays, as the template creates."""
    repo = icechunk.Repository.create(storage=icechunk.in_memory_storage())
    session = repo.writable_session("main")
    zarr.create_array(
        session.store, name="time", shape=(0,), chunks=(8,), dtype="float64",
        dimension_names=("time",),
    )
    for name in MANIFEST_ARRAYS:
        zarr.create_array(
            session.store, name=name, shape=(0,), chunks=(8,), dtype="str",
            dimension_names=("time",),
        )
    return session.store


def test_manifest_round_trip(store: zarr.abc.store.Store) -> None:
    inv = inventory(3)
    zarr.open_array(store, path="time").resize((3,))
    zarr.open_array(store, path="time")[:] = inv.times()
    StoreManifest.write(store, inv)
    assert StoreManifest.read(store) == inv


def test_manifest_read_none_before_write(store: zarr.abc.store.Store) -> None:
    assert StoreManifest.read(store) is None


def test_manifest_read_runs_inventory_validators(
    store: zarr.abc.store.Store,
) -> None:
    inv = inventory(2)
    zarr.open_array(store, path="time").resize((2,))
    zarr.open_array(store, path="time")[:] = inv.times()
    StoreManifest.write(store, inv)
    # Corrupt: duplicate UR directly in the array.
    zarr.open_array(store, path="granule_ur")[1] = "G0"
    with pytest.raises(ValueError, match="duplicate granule_ur"):
        StoreManifest.read(store)


def test_pending_ledger_round_trip_and_dedupe(store: zarr.abc.store.Store) -> None:
    assert PendingLedger.read(store) == ()
    PendingLedger.append(store, [entry(1)])
    PendingLedger.append(store, [entry(1), entry(2)])  # redelivery of 1
    assert [e.granule_ur for e in PendingLedger.read(store)] == ["G1", "G2"]
    PendingLedger.write(store, [entry(2)])
    assert [e.granule_ur for e in PendingLedger.read(store)] == ["G2"]


def test_state_rides_the_commit() -> None:
    """Manifest + ledger written through a session survive commit, and an
    attrs-only change is a committable session change (no empty commit)."""
    repo = icechunk.Repository.create(storage=icechunk.in_memory_storage())
    session = repo.writable_session("main")
    zarr.create_array(
        session.store, name="time", shape=(1,), chunks=(8,), dtype="float64",
        dimension_names=("time",),
    )
    for name in MANIFEST_ARRAYS:
        zarr.create_array(
            session.store, name=name, shape=(1,), chunks=(8,), dtype="str",
            dimension_names=("time",),
        )
    inv = inventory(1)
    zarr.open_array(session.store, path="time")[:] = inv.times()
    StoreManifest.write(session.store, inv)
    session.commit("init")

    deferral = repo.writable_session("main")
    PendingLedger.append(deferral.store, [entry(7)])  # attrs-only change
    deferral.commit("defer")  # must not raise NoChangesToCommitError

    readonly = repo.readonly_session("main")
    assert StoreManifest.read(readonly.store) == inv
    assert [e.granule_ur for e in PendingLedger.read(readonly.store)] == ["G7"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_manifest.py -q`
Expected: FAIL — `StoreManifest.write` has the old `(uri, inventory)` signature.

- [ ] **Step 3: Rewrite manifest.py**

Replace everything below `storage_prefix` (i.e. `default_state_uri` through the end of the file) with:

```python
class StoreManifest:
    """The store's typed inventory, stored in the store itself.

    ``granule_ur``/``granule_url`` are vlen-string arrays on the append
    dimension (template-declared), the scalars live in the root attribute
    ``tempo_store``, and the time values are the store's own axis — so the
    manifest is committed atomically with the data it describes and cannot
    drift from it.
    """

    @staticmethod
    def read(store: Store) -> BackfillInventory | None:
        """Reconstruct the inventory, or None if the store carries none.

        Runs the full ``BackfillInventory`` validation (strictly increasing
        times, no duplicate URs), so a corrupted store fails loudly here.
        """
        group = zarr.open_group(store, mode="r")
        meta = group.attrs.get(STORE_META_ATTRIBUTE)
        axis = np.asarray(zarr.open_array(store, path="time")[:])
        if meta is None or not axis.size:
            return None
        urs = zarr.open_array(store, path=MANIFEST_ARRAYS[0])[:]
        urls = zarr.open_array(store, path=MANIFEST_ARRAYS[1])[:]
        return BackfillInventory.model_validate(
            dict(meta)
            | {
                "granules": [
                    {"url": str(url), "granule_ur": str(ur), "time": float(t)}
                    for url, ur, t in zip(urls, urs, axis, strict=True)
                ]
            }
        )

    @staticmethod
    def write(store: Store, inventory: BackfillInventory) -> None:
        """Write the arrays and meta attribute (does not touch the axis)."""
        n = len(inventory.granules)
        columns = {
            MANIFEST_ARRAYS[0]: [e.granule_ur for e in inventory.granules],
            MANIFEST_ARRAYS[1]: [e.url for e in inventory.granules],
        }
        for name, values in columns.items():
            array = zarr.open_array(store, path=name)
            array.resize((n,))
            array[:] = np.array(values, dtype=object)
        group = zarr.open_group(store, mode="a")
        group.attrs[STORE_META_ATTRIBUTE] = inventory.model_dump(
            by_alias=True, exclude={"granules"}
        )


class PendingLedger:
    """Out-of-order arrivals awaiting the re-sort job, deduped by granule UR.

    Stored as the root attribute ``pending_ledger``, so updates commit
    atomically with the batch that produced them; concurrent writers
    surface as icechunk commit conflicts instead of lost updates.
    """

    @staticmethod
    def read(store: Store) -> tuple[GranuleEntry, ...]:
        raw = zarr.open_group(store, mode="r").attrs.get(PENDING_LEDGER_ATTRIBUTE, [])
        return tuple(GranuleEntry.model_validate(item) for item in raw)

    @staticmethod
    def write(store: Store, entries: Iterable[GranuleEntry]) -> None:
        group = zarr.open_group(store, mode="a")
        group.attrs[PENDING_LEDGER_ATTRIBUTE] = [e.model_dump() for e in entries]

    @classmethod
    def append(cls, store: Store, entries: Iterable[GranuleEntry]) -> None:
        existing = list(cls.read(store))
        seen = {entry.granule_ur for entry in existing}
        for entry in entries:
            if entry.granule_ur not in seen:
                existing.append(entry)
                seen.add(entry.granule_ur)
        cls.write(store, existing)
```

Fix the imports: drop `json`, `os` (if now unused — `storage_prefix` still uses `os`), `Path`, `Any`, `Collection`; add `import zarr` and `from zarr.abc.store import Store`; keep `numpy`, `BackfillInventory`, `GranuleEntry`, `Iterable`. Drop the now-unused `StoreValidationError` import. Rewrite the module docstring to the one-paragraph version: state lives in the store, committed with the data; URIs and single-writer caveats are gone.

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_manifest.py -q`
Expected: PASS. (Other suites are still red — they migrate in Tasks 3–7.)

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: StoreManifest/PendingLedger read and write the store itself"
```

---

### Task 3: Backfill path writes state in-branch; promote becomes bare CAS

**Files:**
- Modify: `lambda/virtualizarr-processor/virtualizarr_processor/processor.py:156-353` (`initialize_backfill_store`, `validate_backfill_store`, all `validate_store` calls)
- Modify: `lambda/backfill/backfill_handlers/promote.py`
- Test: `tests/test_tempo_processor.py` (backfill tests), `tests/backfill_handlers/conftest.py`, `tests/backfill_handlers/test_promote.py`, `tests/backfill_handlers/test_init.py`, `tests/backfill_handlers/test_end_to_end.py`

**Interfaces:**
- Consumes: `StoreManifest.write/read`, `PendingLedger.write`, `PIPELINE_STATE_ATTRIBUTES` (Tasks 1–2).
- Produces: after `initialize_backfill_store` + workers + merge + promote, `main` carries the manifest arrays, `tempo_store`, and an empty `pending_ledger`; `validate_backfill_store` fails if the manifest arrays disagree with the inventory; `promote.py`'s handler performs no writes after the CAS.

- [ ] **Step 1: Write/adjust the failing tests**

In `tests/test_tempo_processor.py::test_backfill_end_to_end`, append at the end:

```python
    from virtualizarr_processor.manifest import PendingLedger, StoreManifest

    repo = processor.open_backfill_repo()
    manifest = StoreManifest.read(repo.readonly_session("main").store)
    assert manifest is not None
    assert manifest.granules == tiny.inventory.granules
    assert PendingLedger.read(repo.readonly_session("main").store) == ()
```

In `tests/test_tempo_processor.py::test_promote_gate_rejects_axis_inventory_mismatch`, no change to the scenario; additionally add a new test right after it:

```python
def test_promote_gate_rejects_manifest_array_mismatch(tiny: TinyCollection) -> None:
    import zarr

    processor = Processor()
    repo = processor.open_backfill_repo()
    processor.initialize_backfill_store(repo, tiny.inventory)
    session = repo.writable_session("backfill")
    zarr.open_array(session.store, path="granule_ur")[0] = "someone-else"
    session.commit("corrupt a manifest slot")
    with pytest.raises(StoreValidationError, match="granule_ur"):
        processor.validate_backfill_store(repo, tiny.inventory, branch="backfill")
```

In `tests/backfill_handlers/conftest.py`, delete the two `STORE_MANIFEST_URI` / `PENDING_LEDGER_URI` `monkeypatch.setenv` lines. In `tests/test_tempo_processor.py`'s `tiny` fixture, delete the same two lines. In `tests/backfill_handlers/test_promote.py`, change assertions that read the manifest from a file path to `StoreManifest.read(repo.readonly_session("main").store)` (the handler no longer takes or writes any URI); delete any assertion about a warning when `STORE_MANIFEST_URI` is unset.

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_tempo_processor.py -q -k "backfill or promote"`
Expected: FAIL — manifest arrays exist but are empty strings (nothing writes them yet).

- [ ] **Step 3: Implement**

In `processor.py`:

1. Update the manifest import to `from virtualizarr_processor.manifest import (MANIFEST_ARRAYS, PIPELINE_STATE_ATTRIBUTES, PendingLedger, StoreManifest)` and delete `_store_manifest_uri` / `_pending_ledger_uri` (their remaining callers go away in Task 4; if the interpreter complains mid-task, leave them until Task 4 — but they must be gone by the end of Task 4).
2. In `initialize_backfill_store`, after the coordinates loop and before `validate_store`, add:

```python
        # The manifest and an empty ledger ride the same branch, so the
        # promote lands data and state in one atomic reset.
        StoreManifest.write(session.store, inventory)
        PendingLedger.write(session.store, ())
```

3. Add `volatile=PIPELINE_STATE_ATTRIBUTES` to **every** `validate_store(...)` call in `processor.py` (there are four: `initialize_backfill_store`, `initialize_resort_store`, `initialize_repo`, `validate_backfill_store`).
4. In `validate_backfill_store`, after the coordinates loop, add:

```python
        urs = [str(v) for v in zarr.open_array(session.store, path="granule_ur")[:]]
        if urs != [e.granule_ur for e in inventory.granules]:
            differences.append("store granule_ur array differs from the inventory")
        urls = [str(v) for v in zarr.open_array(session.store, path="granule_url")[:]]
        if urls != [e.url for e in inventory.granules]:
            differences.append("store granule_url array differs from the inventory")
```

In `promote.py`, delete the `import os`, the `StoreManifest` import, and everything from `manifest_uri = ...` through the `else:` warning block; the handler body ends with the `logger.info("Promoted main to backfill tip")` and `return {"promoted": True}`. Update the module docstring: the manifest was committed on the branch by Init, so the CAS is the entire promote — nothing runs after it.

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_tempo_processor.py tests/backfill_handlers/test_promote.py tests/backfill_handlers/test_init.py tests/backfill_handlers/test_end_to_end.py -q`
Expected: backfill/promote/init/end-to-end tests PASS. Forward and resort tests may still be red (Tasks 4–5).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: backfill branch carries manifest+ledger; promote is a bare CAS"
```

---

### Task 4: Forward path — in-session state, moved-timestamp gate, no empty commits

**Files:**
- Modify: `lambda/virtualizarr-processor/virtualizarr_processor/processor.py:442-607` (`initialize_repo`, `process_file`, `commit_processed_files`; delete `_manifest_entries`, `_manifest_entry_at`, `_store_manifest_uri`, `_pending_ledger_uri`)
- Modify: `lambda/process_messages/handler.py` (comment only)
- Test: `tests/test_tempo_processor.py` (forward tests), `tests/test_handler.py`

**Interfaces:**
- Consumes: Tasks 1–3.
- Produces: `process_file` routes using the `granule_ur` array; DEFERRED writes the ledger **through the session** (uncommitted until `commit_processed_files`); a granule whose UR already owns a slot but whose time misses the axis is REJECTED; `commit_processed_files` appends/overwrites the manifest arrays, re-validates via `StoreManifest.read`, and commits — an all-DEFERRED batch commits fine (the attrs change is a session change).

- [ ] **Step 1: Adjust and extend the failing tests**

In `tests/test_tempo_processor.py`:

- `test_forward_appends_in_order`: replace the last three lines (env read, `urls()` assertion, `validate_against_axis`) with:

```python
    from virtualizarr_processor.manifest import StoreManifest

    manifest = StoreManifest.read(repo.readonly_session("main").store)
    assert manifest is not None and manifest.urls()[-1] == f"file://{new}"
```

- `test_forward_defers_out_of_order_granule`: replace the ledger read with:

```python
    from virtualizarr_processor.manifest import PendingLedger

    repo = processor.open_backfill_repo()
    ledger = PendingLedger.read(repo.readonly_session("main").store)
```

  (keep the assertions on `ledger` unchanged; note the read must come *after* `forward(...)`, which commits).

- Add two new tests:

```python
def test_forward_all_deferred_batch_commits(tiny: TinyCollection) -> None:
    """A batch of only out-of-order granules must still commit (the ledger
    write is a session change), not raise NoChangesToCommitError."""
    from virtualizarr_processor.manifest import PendingLedger

    processor = backfilled(tiny)
    between = write_tempo_granule(
        tiny.granule_paths[0].parent / "between.nc",
        time_value=tiny.times[0] + 1800.0,
    )
    assert forward(processor, [f"file://{between}"]) == [ProcessOutcome.DEFERRED]
    repo = processor.open_backfill_repo()
    assert [e.granule_ur for e in
            PendingLedger.read(repo.readonly_session("main").store)] == ["between"]


def test_forward_rejects_republication_with_moved_timestamp(
    tiny: TinyCollection,
) -> None:
    """Same UR as an ingested granule, shifted time: reject loudly instead of
    poisoning the pending ledger with a UR the manifest already owns."""
    from virtualizarr_processor.manifest import PendingLedger

    processor = backfilled(tiny)
    moved = write_tempo_granule(
        tiny.granule_paths[0].parent / f"{tiny.granule_paths[1].stem}.nc",
        time_value=tiny.times[1] + 7.0,  # off-axis, before the end
    )
    assert forward(processor, [f"file://{moved}"]) == [ProcessOutcome.REJECTED]
    repo = processor.open_backfill_repo()
    assert PendingLedger.read(repo.readonly_session("main").store) == ()
```

(`backfilled` and `forward` are the file's existing helpers. The second test rewrites granule 1's file at a shifted time — same filename, hence same UR.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_tempo_processor.py -q -k forward`
Expected: FAIL — routing still reads the deleted file-based manifest; `all_deferred` raises `NoChangesToCommitError` (surfaced as a commit failure).

- [ ] **Step 3: Implement**

In `processor.py`:

1. Delete `_store_manifest_uri`, `_pending_ledger_uri`, `_manifest_entries`, `_manifest_entry_at`.
2. In `initialize_repo`, inside the `if "time" not in group:` block after the coordinates loop, add (import `STORE_META_ATTRIBUTE` from `manifest`):

```python
            root = zarr.open_group(session.store, mode="a")
            root.attrs[STORE_META_ATTRIBUTE] = {
                "schema": "tempo-backfill-inventory/1",
                "collection": self.config.collection_shortname,
                "concept_id": self.config.concept_id,
                "time_units": self.config.time_units,
                "built_at": datetime.now(timezone.utc).isoformat(),
            }
            PendingLedger.write(session.store, ())
```

3. In `process_file`, replace the occupied-slot ownership lookup:

```python
            if occupied.size == 1:
                index = int(occupied[0])
                known_ur = str(
                    zarr.open_array(session.store, path="granule_ur")[index]
                )
                if known_ur != entry.granule_ur:
```

   (log message unchanged, using `known_ur` in place of `known.granule_ur`), and replace the DEFERRED branch with:

```python
            # Out of order: appending would break axis monotonicity.
            owned = {
                str(v)
                for v in zarr.open_array(session.store, path="granule_ur")[:]
            }
            if entry.granule_ur in owned:
                # Same granule, shifted time: folding it in would give the
                # manifest a duplicate UR and wedge every future re-sort.
                # Reject to the DLQ for operator review instead.
                logger.error(
                    "process_file: %s already owns a slot but its time %r no "
                    "longer matches the axis; rejecting republication with a "
                    "moved timestamp",
                    entry.granule_ur,
                    time_value,
                )
                return ProcessOutcome.REJECTED
            # Record it for the scheduled re-sort job; the ledger update is
            # part of this session and commits with the batch.
            PendingLedger.append(session.store, [entry])
```

   (keep the existing `logger.info(... deferred ...)` and `return ProcessOutcome.DEFERRED`).

4. Replace `commit_processed_files` wholesale:

```python
    def commit_processed_files(self, session: Session) -> str:
        """Update the manifest arrays for the batch's writes, then commit.

        The manifest is re-read (running the full inventory validation)
        before the commit, so duplicate URs or a non-monotonic axis fail
        the batch rather than committing state the store cannot describe.
        An all-DEFERRED batch commits too: the ledger attribute update is
        itself a session change.
        """
        if self._appended or self._replaced:
            axis_size = zarr.open_array(session.store, path="time").shape[0]
            ur_array = zarr.open_array(session.store, path="granule_ur")
            url_array = zarr.open_array(session.store, path="granule_url")
            ur_array.resize((axis_size,))
            url_array.resize((axis_size,))
            start = axis_size - len(self._appended)
            for offset, entry in enumerate(self._appended):
                ur_array[start + offset] = entry.granule_ur
                url_array[start + offset] = entry.url
            for index, entry in self._replaced.items():
                ur_array[index] = entry.granule_ur
                url_array[index] = entry.url
            if StoreManifest.read(session.store) is None:
                raise StoreValidationError(
                    ["store carries no manifest metadata; run the backfill "
                     "or the initialize Lambda first"]
                )
        snapshot = cast(str, session.commit(f"Append to {session.snapshot_id}"))
        self._appended = []
        self._replaced = {}
        return snapshot
```

   Drop the now-unused `BackfillInventory` import if nothing else in the file uses it (`initialize_backfill_store` still does — keep it).

5. In `lambda/process_messages/handler.py`, update the commit-failure comment (lines 109–111): the ledger write is now part of the failed session, so nothing persisted for DEFERRED records either; the retry re-defers cleanly. No code change.

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_tempo_processor.py tests/test_handler.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: forward path routes and defers via in-store state, atomically with the batch"
```

---

### Task 5: Resort pins one snapshot and drains the ledger in-commit

**Files:**
- Modify: `lambda/virtualizarr-processor/virtualizarr_processor/processor.py:209-325` (`initialize_resort_store`, `reindex_resort_slots`; delete `process_resort_file`, loosen `process_backfill_file`)
- Modify: `lambda/backfill/backfill_handlers/resort.py`
- Modify: `lambda/virtualizarr-processor/virtualizarr_processor/typing.py` (only if Task 7 has not yet removed the Protocol — otherwise nothing)
- Test: `tests/backfill_handlers/test_resort.py`

**Interfaces:**
- Consumes: Tasks 1–4.
- Produces:
  - `initialize_resort_store(self, repo: Repository, merged: BackfillInventory, *, from_tip: str) -> BranchInit` — branches `resort` off the **caller-supplied** tip (never re-looks-up main), writes the merged manifest onto the branch, returns `BranchInit(snapshot, branched_from=from_tip)`.
  - `process_backfill_file(self, file_key: str, fork: ForkSession | Session) -> bool` (loosened annotation; `process_resort_file` deleted).
  - The resort handler reads ledger + manifest + axis from one readonly session pinned at the captured tip, writes the drained ledger into the resort branch commit, and performs **no writes after promote**.

- [ ] **Step 1: Adjust and extend the failing tests**

In `tests/backfill_handlers/test_resort.py`: replace file-based `PendingLedger`/`StoreManifest` setup/assertions with store-based ones — pending entries are seeded by running the forward consumer on an out-of-order granule (or by opening a writable main session, calling `PendingLedger.append(session.store, [...])`, and committing), and post-resort assertions read `StoreManifest.read(repo.readonly_session("main").store)` / `PendingLedger.read(...)`. Add one new test:

```python
def test_resort_concurrent_append_fails_the_cas(
    tempo_pipeline: "SimpleNamespace", lambda_context: "MagicMock",
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An append landing on main after the resort pinned its snapshot must
    fail the promote CAS — never be silently erased (review finding #2)."""
    import icechunk
    import pytest
    from virtualizarr_processor import backfill as backfill_module
    from backfill_handlers import resort

    # ... run backfill, defer one granule so the ledger is non-empty
    # (reuse this file's existing setup helper) ...

    real_promote = backfill_module.promote

    def promote_after_concurrent_append(repo, **kwargs):  # type: ignore[no-untyped-def]
        # Simulate the consumer committing between the pin and the CAS.
        session = repo.writable_session("main")
        zarr.open_group(session.store, mode="a").attrs["raced"] = True
        session.commit("concurrent consumer commit")
        return real_promote(repo, **kwargs)

    monkeypatch.setattr(resort.backfill, "promote", promote_after_concurrent_append)
    with pytest.raises(icechunk.IcechunkError):
        resort.handler({}, lambda_context)
```

(Adapt the setup lines to this file's existing helpers; the assertion that matters is that the handler raises out of the CAS instead of succeeding.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/backfill_handlers/test_resort.py -q`
Expected: FAIL — handler still reads `STORE_MANIFEST_URI` from the environment.

- [ ] **Step 3: Implement**

In `processor.py`:

1. `initialize_resort_store` signature becomes `def initialize_resort_store(self, repo: Repository, merged: BackfillInventory, *, from_tip: str) -> BranchInit:`; delete the `tip = repo.lookup_branch("main")` line and use `from_tip` for the branch reset/create and the returned `branched_from`. After the `zarr.open_array(session.store, path="time")[:] = merged.times()` line, add `StoreManifest.write(session.store, merged)` (the resize loop has already resized the manifest arrays — they carry the append dim). Update the docstring: the caller pins main's tip before reading any state and passes it here, so a concurrent append moves main and fails the promote CAS instead of being erased.
2. In `reindex_resort_slots`, extend the axis-skip:

```python
            if path.lstrip("/") in (self.config.append_dim, *MANIFEST_ARRAYS):
                continue  # rewritten wholesale at init, not relocated
```

3. Delete `process_resort_file`; change `process_backfill_file`'s signature to `def process_backfill_file(self, file_key: str, fork: ForkSession | Session) -> bool:` and remove the `# type: ignore[arg-type]` that the wrapper carried.

Replace `resort.py`'s handler body with:

```python
@logger.inject_lambda_context()
@tracer.capture_lambda_handler
def handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:
    max_fold = int(os.environ.get("RESORT_MAX_FOLD", DEFAULT_MAX_FOLD))

    processor = Processor()
    repo = processor.open_backfill_repo()
    # Pin main's tip FIRST; every read below comes from this snapshot, and
    # the promote CAS targets it, so an append landing mid-run fails the
    # CAS instead of being erased.
    tip = repo.lookup_branch("main")
    pinned = repo.readonly_session(snapshot_id=tip).store

    pending = PendingLedger.read(pinned)
    if not pending:
        logger.info("Pending ledger is empty; nothing to resort")
        return {"resorted": False, "reason": "ledger empty"}
    fold = sorted(pending, key=lambda entry: entry.time)[:max_fold]

    manifest = StoreManifest.read(pinned)
    if manifest is None:
        raise RuntimeError("store carries no manifest; is it initialized?")

    merged = merge_pending(manifest, fold)  # collisions raise here
    shift_index = first_shifted_index(manifest, merged)
    logger.info(
        "Resorting",
        extra={
            "pending": len(pending),
            "folding": len(fold),
            "first_shifted_index": shift_index,
            "relocations": len(manifest.granules) - shift_index,
        },
    )

    processor.initialize_resort_store(repo, merged, from_tip=tip)
    session = repo.writable_session("resort")
    processor.reindex_resort_slots(session, manifest, merged)
    fold_urs = {entry.granule_ur for entry in fold}
    for entry in merged.granules:
        if entry.granule_ur not in fold_urs:
            continue
        if not processor.process_backfill_file(entry.url, session):
            raise RuntimeError(f"resort insert failed for {entry.url}")
    # Drain the folded entries inside the same commit that folds them.
    PendingLedger.write(
        session.store, [e for e in pending if e.granule_ur not in fold_urs]
    )
    session.commit(
        f"Resort: insert {len(fold)} granules, "
        f"relocate slots {shift_index}..{len(merged.granules) - 1}"
    )

    processor.validate_backfill_store(repo, merged, branch="resort")
    backfill.promote(repo, source="resort", expected_target_tip=tip)
    remaining = len(pending) - len(fold)
    logger.info("Resort promoted to main", extra={"remaining": remaining})
    return {
        "resorted": True,
        "inserted": len(fold),
        "remaining": remaining,
        "first_shifted_index": shift_index,
    }
```

Drop the now-unused imports (`numpy`, `zarr`, `StoreManifest.validate_against_axis` usage is gone; keep `StoreManifest`, `PendingLedger`, `merge_pending`, `first_shifted_index`). Update the module docstring: state is read from a pinned snapshot and drained in the fold commit; the promote is the only step that touches main and nothing runs after it.

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/backfill_handlers/ tests/test_tempo_processor.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: resort pins one snapshot, drains the ledger in-commit, nothing after promote"
```

---

### Task 6: CMR poller bootstrap + CDK/settings cleanup

**Files:**
- Modify: `lambda/cmr_poller/handler.py:96-108,172-181`
- Modify: `cdk/settings.py:59-60`, `cdk/stack.py` (state-URI block ~160-184, poller env ~527-535, backfill extra_env ~528-533)
- Test: `tests/test_cmr_poller.py`, `tests/cdk/test_forward_ops.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: `initial_watermark(now: datetime) -> datetime` (reads optional `$POLL_START_ISO`); no `STORE_MANIFEST_URI`/`PENDING_LEDGER_URI` anywhere in `cdk/` or handler envs; new optional `StackSettings.POLL_START_ISO: str | None = None` passed to the poller env when set.

- [ ] **Step 1: Adjust the failing tests**

In `tests/test_cmr_poller.py`, update the `initial_watermark` tests: it now takes only `now`; with `POLL_START_ISO` set (monkeypatch) it returns that instant, otherwise `now - DEFAULT_LOOKBACK`:

```python
def test_initial_watermark_uses_poll_start_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POLL_START_ISO", "2026-08-01T00:00:00+00:00")
    now = datetime(2026, 8, 20, tzinfo=timezone.utc)
    assert handler.initial_watermark(now) == datetime(
        2026, 8, 1, tzinfo=timezone.utc
    )


def test_initial_watermark_falls_back_to_lookback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("POLL_START_ISO", raising=False)
    now = datetime(2026, 8, 20, tzinfo=timezone.utc)
    assert handler.initial_watermark(now) == now - handler.DEFAULT_LOOKBACK
```

(Adjust the module alias to match the file's existing import of the poller handler.) Delete the old manifest-`built_at` bootstrap tests. In `tests/cdk/test_forward_ops.py`, delete the `STORE_MANIFEST_URI`/`PENDING_LEDGER_URI` `Match.any_value()` lines.

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_cmr_poller.py tests/cdk -q`
Expected: FAIL — `initial_watermark` still takes `(manifest_uri, now)`.

- [ ] **Step 3: Implement**

In `lambda/cmr_poller/handler.py`, replace `initial_watermark`:

```python
def initial_watermark(now: datetime) -> datetime:
    """Choose the starting point for a first poll (no watermark yet).

    ``$POLL_START_ISO`` (typically the backfill inventory's build time,
    covering everything published while the backfill ran) wins; otherwise
    a fixed lookback. The overlap window and the consumer's idempotent
    routing absorb any imprecision.
    """
    start = os.environ.get("POLL_START_ISO")
    if start:
        return datetime.fromisoformat(start)
    return now - DEFAULT_LOOKBACK
```

and its call site becomes `watermark = read_watermark(watermark_uri) or initial_watermark(started)`. Remove the `STORE_MANIFEST_URI` mention from the module docstring.

In `cdk/settings.py`, delete the `STORE_MANIFEST_URI` and `PENDING_LEDGER_URI` fields; add `POLL_START_ISO: str | None = None` beside the poller settings. In `cdk/stack.py`: delete `self.store_manifest_uri` / `self.pending_ledger_uri` and their derivation (keep `state_prefix` — the watermark still uses it); delete both keys from `self.processor_env`; in `poller_env`, replace the `STORE_MANIFEST_URI` entry (and its comment) with:

```python
            if settings.POLL_START_ISO:
                poller_env["POLL_START_ISO"] = settings.POLL_START_ISO
```

(after the dict literal); and in `_build_backfill`'s `extra_env` key tuple, delete `"STORE_MANIFEST_URI", "PENDING_LEDGER_URI"`. Then run `grep -rn "STORE_MANIFEST_URI\|PENDING_LEDGER_URI" cdk lambda tests scripts docs README.md` and fix every remaining hit (docs hits are prose updates; `scripts/` hits fall to Task 7).

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_cmr_poller.py tests/cdk tests/test_ops_handlers.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: POLL_START_ISO bootstrap; drop state-URI wiring from CDK"
```

---

### Task 7: verify_store reads the repo; delete the repair script and the Protocol

**Files:**
- Modify: `scripts/verify_store.py:299-341` (`main`)
- Delete: `scripts/rebuild_manifest.py`, `tests/scripts/test_rebuild_manifest.py`
- Modify: `lambda/virtualizarr-processor/virtualizarr_processor/typing.py` (keep only `BranchInit` and `ProcessOutcome`)
- Modify: `lambda/virtualizarr-processor/virtualizarr_processor/backfill.py:1-9` (docstring), `tests/test_example.py` (drop the Protocol type-check)
- Test: `tests/scripts/test_verify_store.py`

**Interfaces:**
- Consumes: `StoreManifest.read` / `PendingLedger.read` (Task 2).
- Produces: `verify_store.py` resolves manifest and ledger from `repo.readonly_session("main")` — one snapshot for sampling, manifest, and ledger; `typing.py` exports exactly `BranchInit` and `ProcessOutcome`.

- [ ] **Step 1: Adjust the failing tests**

In `tests/scripts/test_verify_store.py`, replace any file-URI manifest/ledger setup with store state (the store under test now carries it after a backfill; assertions on `main()`'s exit code are unchanged). Delete `tests/scripts/test_rebuild_manifest.py`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/scripts -q`
Expected: FAIL — `verify_store.main` still resolves `STORE_MANIFEST_URI`.

- [ ] **Step 3: Implement**

In `scripts/verify_store.py::main`, replace the manifest/ledger resolution (drop the `default_state_uri` import):

```python
    processor = Processor()
    repo = processor.open_backfill_repo()
    pinned = repo.readonly_session("main").store
    manifest = StoreManifest.read(pinned)
    if manifest is None:
        print("FAIL: store carries no manifest", file=sys.stderr)
        return 1
    lookup = None if args.offline else cmr_lookup_for(processor.config.concept_id)
    problems = verify_store(
        repo, manifest, samples=args.samples, window=args.window,
        seed=args.seed, cmr_lookup=lookup,
    )
    if args.completeness:
        ledger_urs = {entry.granule_ur for entry in PendingLedger.read(pinned)}
        problems += verify_completeness(
            processor.config.concept_id, manifest, ledger_urs
        )
```

Update the module docstring's ledger sentence accordingly. `git rm scripts/rebuild_manifest.py tests/scripts/test_rebuild_manifest.py` — the divergence it repaired can no longer occur; check nothing else imports it (`grep -rn rebuild_manifest lambda scripts tests cdk docs README.md` and fix prose hits).

In `typing.py`: delete the `VirtualizarrProcessor` Protocol class and the now-unused imports (`Protocol`, `runtime_checkable`, `ForkSession`, `Repository`, `Session`, `icechunk`, `datetime`, `TYPE_CHECKING`/`BackfillInventory` — keep exactly what `BranchInit` and `ProcessOutcome` need: `enum`, `NamedTuple`). In `tests/test_example.py`, delete `protocol_type_check` and its import; if the file becomes empty, delete it. In `backfill.py`, reword the docstring's first sentence to "These operations are processor-independent, so they live here rather than on the Processor." In `processor.py:97`, reword the class docstring to "The TEMPO L3 processor: parsing, validation, routing, and store lifecycle."

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/scripts tests/test_example.py -q` (drop `tests/test_example.py` from the command if deleted)
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor: verify from the repo snapshot; delete rebuild_manifest and the Protocol"
```

---

### Task 8: Docs sweep and full verification

**Files:**
- Modify: `docs/superpowers/backfill-pipeline-overview.md`, `README.md`, module docstrings still naming state URIs (`processor.py:23-24`, `manifest.py`, `process_messages/handler.py`)
- Test: whole suite

- [ ] **Step 1: Docs sweep**

Run `grep -rn "STORE_MANIFEST_URI\|PENDING_LEDGER_URI\|rebuild_manifest\|store-manifest.json\|pending-ledger.json\|validate_against_axis" README.md docs lambda scripts cdk` — every remaining hit is stale prose; rewrite each to describe the in-store state (manifest arrays + `tempo_store`/`pending_ledger` attributes, atomic with the data, resort pinned-snapshot flow, `POLL_START_ISO`).

- [ ] **Step 2: Full suite, lint, types**

Run: `.venv/bin/pytest tests -q && .venv/bin/ruff check . && .venv/bin/ruff format --check . && .venv/bin/mypy .`
Expected: all PASS/clean. Fix anything surfaced (unused imports are the likely stragglers).

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "docs: describe in-store manifest/ledger; full suite green"
```

---

## Self-Review Notes

- Spec coverage: layout → Tasks 1–2; atomic promote → Task 3; deferral atomicity + moved-timestamp gate + empty-commit → Task 4; resort pinning + in-commit drain + conflict surfacing → Task 5; poller bootstrap + CDK → Task 6; deletions (`rebuild_manifest`, `validate_against_axis` [removed in Task 2's rewrite], URI plumbing [Task 2], Protocol, `process_resort_file` [Task 5]) → Tasks 2/5/7; validation `volatile` → Tasks 1/3. Non-goals (watermark, Iceberg, migration) have no tasks by design.
- Names used consistently: `MANIFEST_ARRAYS`, `STORE_META_ATTRIBUTE` (`"tempo_store"`), `PENDING_LEDGER_ATTRIBUTE` (`"pending_ledger"`), `PIPELINE_STATE_ATTRIBUTES`, `StoreManifest.read/write(store, ...)`, `PendingLedger.read/write/append(store, ...)`, `initialize_resort_store(repo, merged, *, from_tip)`, `initial_watermark(now)`.
- Known sequencing note: Tasks 3–5 migrate `processor.py` incrementally; between Task 2 and the end of Task 4 some forward/resort suites are expected red — each task's Step 4 names exactly which suites must be green at that point.
