# TEMPO Inventory & Processor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the synthetic sample processor with the TEMPO-specific,
config-driven processor: typed backfill inventory carrying exact per-granule
time values, declarative per-collection store templates, and layered
validation on every insertion.

**Architecture:** A `CollectionConfig` (TOML → pydantic) selects committed
artifacts (pydantic-zarr `GroupSpec` template JSON + reference coordinate
arrays) per collection. The inventory is a typed JSON manifest whose `time`
values are read from the files themselves. Init resizes the template to the
inventory length, writes the axis, validates, commits; workers parse with
`HDFParser`, flatten groups to root, promote `weight`, validate against the
template + reference grid, and region-write.

**Tech Stack:** pydantic v2, pydantic-zarr 0.10, virtualizarr 2.7 (`[hdf]`),
icechunk 2.1, zarr 3.2, obstore/obspec-utils, h5py (tests/scripts only).

**Spec:** `docs/superpowers/specs/2026-08-20-tempo-inventory-and-processor-design.md`

## Global Constraints

- Run everything with `uv run` from the repo root (uv workspace).
- Gate: `uv run pytest`, `uv run ruff check . && uv run ruff format --check .`,
  `uv run mypy .` all clean before done.
- Tests must not touch the network. Real-granule tests read
  `$TEMPO_TEST_DATA` (default `/workspace/context/data`) and skip if absent;
  unit tests use tiny synthetic HDF5 fixtures.
- No datetime conversion of axis values anywhere — raw float64
  seconds-since-epoch end to end.
- Worker/handler contracts (`file_keys: list[str]`, fork artifact flow) stay
  unchanged except: init receives `inventory_uri`.
- Commit after each task on branch `claude/tempo-processor`.

---

### Task 1: Collection config module + volatile-attribute fix

**Files:**
- Create: `lambda/virtualizarr-processor/virtualizarr_processor/collection.py`
- Create: `lambda/virtualizarr-processor/virtualizarr_processor/collections/hcho.toml`, `.../no2.toml`
- Modify: `lambda/virtualizarr-processor/virtualizarr_processor/store_template.py` (add the four `geospatial_*` bounds to `TEMPO_L3_VOLATILE_ATTRIBUTES`)
- Modify: `lambda/virtualizarr-processor/pyproject.toml` (add `pydantic>=2`; package data)
- Test: `tests/test_collection.py`

**Interfaces (Produces):**
```python
class CollectionConfig(BaseModel, frozen=True):
    name: str                      # "hcho" | "no2"
    collection_shortname: str      # "TEMPO_HCHO_L3"
    concept_id: str
    append_dim: str                # "time"
    time_units: str
    flatten_groups: tuple[str, ...]
    promote_to_time: tuple[str, ...]
    drop_variables: tuple[str, ...]
    volatile_attributes: frozenset[str]   # base list ∪ TOML extras
    time_chunk_size: int           # axis chunk override (16384); a 1-granule
                                   # template would otherwise chunk time (1,)
    template_file: str             # e.g. "hcho_template.json"
    coordinates_file: str          # "coordinates.npz"

def load_collection(name: str | None = None) -> CollectionConfig   # None → $TEMPO_COLLECTION
def load_template(config: CollectionConfig) -> AnyGroupSpec        # packaged JSON
def load_coordinates(config: CollectionConfig) -> dict[str, np.ndarray]
```

- [ ] Step 1: failing tests — TOML loads for both names, env selection,
  unknown name raises, volatile set includes `geospatial_lat_min` etc.,
  `load_template`/`load_coordinates` raise `FileNotFoundError` until Task 5
  commits artifacts (assert error message names the file).
- [ ] Step 2: implement (`tomllib` + `importlib.resources`), fix the
  volatile list, wire package data via hatch `force-include`/package-data.
- [ ] Step 3: pytest, ruff, mypy pass. Commit.

### Task 2: Typed backfill inventory

**Files:**
- Create: `lambda/virtualizarr-processor/virtualizarr_processor/inventory.py`
- Test: `tests/test_inventory.py`

**Interfaces (Produces):**
```python
class GranuleEntry(BaseModel, frozen=True):
    url: str          # must end ".nc"
    granule_ur: str
    time: float       # exact /time[0], float64 seconds since epoch

class BackfillInventory(BaseModel, frozen=True):
    schema_id: Literal["tempo-backfill-inventory/1"]  # alias "schema"
    collection: str
    concept_id: str
    time_units: str
    built_at: str
    granules: tuple[GranuleEntry, ...]
    # validators: non-empty; unique granule_ur; strictly increasing time
    @classmethod
    def from_json(cls, data: bytes | str) -> "BackfillInventory"
    def to_json(self) -> str
    def times(self) -> np.ndarray  # float64
    def urls(self) -> list[str]
```

- [ ] Step 1: failing tests — round trip preserves float64 exactly
  (1471196538.0244286), rejects: empty, unsorted, duplicate time, duplicate
  UR, non-`.nc` url, wrong schema id.
- [ ] Step 2: implement with pydantic v2 (`model_validate_json`,
  `model_dump_json`; JSON float round-trip is exact for binary64).
- [ ] Step 3: green + lint + commit.

### Task 3: Synthetic TEMPO granule fixtures

**Files:**
- Create: `tests/tempo_fixtures.py`
- Modify: `tests/conftest.py` (fixtures `tempo_granule_dir`, `real_data_dir`)
- Modify: root `pyproject.toml` dev deps (`h5py`)

**Interfaces (Produces):**
```python
def write_tempo_granule(
    path: Path, *, time_value: float, collection_shortname: str = "TEMPO_HCHO_L3",
    lat: np.ndarray = TINY_LAT, lon: np.ndarray = TINY_LON,
    attrs: dict[str, Any] | None = None,      # override/extend root attrs
    weight_scale: float = 1.0,                # make per-scan weight distinct
) -> Path
```
Writes an h5py file mimicking TEMPO L3 at tiny grid (lat=4, lon=6): root
`time/latitude/longitude/weight`, groups `product` (`vertical_column`
float64 + `main_data_quality_flag` int16), `geolocation`
(`solar_zenith_angle` float32), `support_data` (`surface_pressure`
float32), with dimension scales attached, `_FillValue`s, shuffle+deflate on
3-D vars, contiguous 1-D coords, and root attrs including
`collection_shortname`, `time_coverage_start_since_epoch == time_value`,
plus volatile ones (`history`, `geospatial_lat_min`, ...). Values are
deterministic functions of `time_value` so read-back can be asserted.

- [ ] Step 1: test that a written fixture opens with h5py and has
  `time[0] == attrs["time_coverage_start_since_epoch"]`.
- [ ] Step 2: implement; commit.

### Task 4: Granule parsing & transform

**Files:**
- Create: `lambda/virtualizarr-processor/virtualizarr_processor/granule.py`
- Modify: `lambda/virtualizarr-processor/pyproject.toml`
  (`virtualizarr[hdf]`, `obstore`, `obspec-utils`)
- Test: `tests/test_granule.py`

**Interfaces (Produces):**
```python
def make_registry(url: str) -> ObjectStoreRegistry
    # file:// → LocalStore; s3://bucket → S3Store(from env);
    # https://host → HTTPStore with $EARTHDATA_TOKEN bearer header
def open_flat_granule(url: str, config: CollectionConfig,
                      registry: ObjectStoreRegistry | None = None) -> xr.Dataset
    # HDFParser → virtual datatree → flatten config.flatten_groups to root
    # (collision = error), promote config.promote_to_time via expand_dims,
    # drop config.drop_variables, root attrs = tree root attrs
def granule_time(vds: xr.Dataset) -> float
    # exactly one time step; raises GranuleValidationError when
    # time[0] != attrs["time_coverage_start_since_epoch"] (spec V3)
```

- [ ] Step 1: failing tests on synthetic fixtures — flat dataset has all
  group vars at root with `(time, lat, lon)` dims; `weight` promoted and
  virtual (ManifestArray); collision (inject dup name) raises;
  `granule_time` returns exact value; mismatched epoch attr raises.
- [ ] Step 2: real-granule test (skipif no `$TEMPO_TEST_DATA`): HCHO granule
  yields 20 root data vars, NO2 yields 33; lat/lon match across both.
- [ ] Step 3: implement (port `flatten_product_subset`/`promote_time_invariant`
  logic from `exploration/tempo_virtual.py`, generalized to all groups).
- [ ] Step 4: green + lint + commit.

### Task 5: Template generator + committed artifacts

**Files:**
- Create: `scripts/generate_template.py` (project script, `uv run`)
- Create (generated): `virtualizarr_processor/collections/hcho_template.json`,
  `no2_template.json`, `coordinates.npz`
- Test: `tests/test_generate_template.py`

**Interfaces (Produces):**
```python
# scripts/generate_template.py
def build_template(paths: list[Path], config: CollectionConfig)
        -> tuple[AnyGroupSpec, dict[str, np.ndarray]]
    # virtualize each via open_flat_granule(file://...), write first to
    # in-memory icechunk (to_icechunk, validate_containers=False),
    # GroupSpec.from_zarr, strip_attributes(volatile), then override the
    # time array's chunk grid to config.time_chunk_size (spec §5: a
    # 1-granule template would otherwise ship a (1,)-chunked axis).
    # Cross-check: for the remaining granules run validate_granule(template,
    # vds, coordinates=..., volatile=...) — any divergence is a hard error
    # naming the attribute.
# CLI: uv run scripts/generate_template.py [--data-dir ...] writes artifacts
# into the package and prints a summary.
```

- [ ] Step 1: failing tests with synthetic fixtures — template contains all
  root vars at full-granule shape with `time` dim; volatile attrs absent;
  shared attrs present; the `time` array's chunk shape equals
  `(config.time_chunk_size,)`; a granule set with a divergent non-volatile
  attr fails with that attr named; determinism (two runs → identical JSON).
- [ ] Step 2: implement.
- [ ] Step 3: run against `/workspace/context/data` and commit the three
  real artifacts; Task 1's `load_template`/`load_coordinates` tests flip to
  asserting real content (2950/7750 grid, 20 vs 33+ arrays).
- [ ] Step 4: green + lint + commit.

### Task 6: TEMPO Processor (backfill path) + protocol update

**Files:**
- Rewrite: `lambda/virtualizarr-processor/virtualizarr_processor/processor.py`
- Modify: `lambda/virtualizarr-processor/virtualizarr_processor/typing.py`
  (`initialize_backfill_store(self, repo, inventory: BackfillInventory) -> str`)
- Create: `tests/stub_processor.py` (move the synthetic sample; keep the
  existing template/handler tests running against it)
- Modify: `lambda/backfill/backfill_handlers/init.py` (read `inventory_uri`
  from event → `inventory.read_inventory`), `lambda/backfill/backfill_handlers/inventory.py`
  (parse typed doc, return `BackfillInventory`), `partition.py` (slice
  `.urls()` into manifests)
- Test: `tests/test_tempo_processor.py`, update `tests/test_backfill.py`,
  `tests/test_handler.py` imports

**Interfaces (Produces):**
```python
class Processor:  # virtualizarr_processor.processor
    def __init__(self, config: CollectionConfig | None = None)  # None → load_collection()
    def open_backfill_repo(self) -> Repository
        # env: ICECHUNK_BUCKET/PREFIX/REGION or ICECHUNK_LOCAL_PATH;
        # virtual container prefix from $VIRTUAL_CHUNK_PREFIX
        # (e.g. "s3://asdc-prod-protected/", tests use "file:///")
    def initialize_backfill_store(self, repo, inventory) -> str
        # assert inventory.collection == config.collection_shortname;
        # resize(template, {"time": N}); create_empty_store on "backfill";
        # write time (inventory.times()), lat/lon (artifact); validate_store;
        # commit
    def process_backfill_file(self, file_key: str, fork: ForkSession) -> bool
        # open_flat_granule → granule_time → validate_granule(template, vds,
        # coordinates={lat,lon}, volatile) → drop lat/lon →
        # to_icechunk(fork.store, region="auto", validate_containers=False,
        #             last_updated_at=<parse start, UTC>)   # spec I3 stamping
        # any failure: log + return False (worker raises → run fails)
```

- [ ] Step 1: move synthetic processor to `tests/stub_processor.py`; point
  existing tests at it; suite green before the rewrite lands.
- [ ] Step 2: failing end-to-end test (synthetic fixtures, tiny grid):
  build 3 fixture granules + inventory (exact times) → local-FS repo →
  init → fork/process×3/merge/commit (use `backfill.create_fork`/
  `merge_and_commit`) → promote → open via xarray:
  time axis equals inventory times exactly; `vertical_column` values equal
  h5py ground truth for every scan; `weight` differs per scan (promotion
  proven); store root attrs contain no volatile attributes.
- [ ] Step 3: failure-mode tests — granule with wrong lat grid → False;
  time not in inventory/axis → False; epoch-attr mismatch → False; nothing
  written to the fork in each case (fork store unchanged).
- [ ] Step 4: real-granule e2e (skipif): 2 HCHO context granules through
  init+worker+merge, read back a small slice vs h5py.
- [ ] Step 5: implement; update handlers + their tests (init event gains
  `inventory_uri`; partition slices urls).
- [ ] Step 6: green + lint + commit.

### Task 7: Store manifest & pending ledger

The two forward-processing state artifacts from spec §5 (I4). Both reuse
the Task 2 models; S3 I/O mirrors `backfill_handlers.inventory` (boto3,
`parse_s3_uri`), with local paths for tests.

**Files:**
- Create: `lambda/virtualizarr-processor/virtualizarr_processor/manifest.py`
- Test: `tests/test_manifest.py` (moto for the S3 side)

**Interfaces (Produces):**
```python
class StoreManifest:  # thin wrapper over BackfillInventory at a URI
    @classmethod
    def read(cls, uri: str) -> BackfillInventory        # s3:// or file path
    @staticmethod
    def write(uri: str, inventory: BackfillInventory) -> None
    @staticmethod
    def validate_against_axis(inventory: BackfillInventory,
                              axis: np.ndarray) -> None
        # bit-exact equality of times, else StoreValidationError (spec I4)

class PendingLedger:  # JSON list[GranuleEntry] at a URI; duplicates deduped
    @classmethod
    def read(cls, uri: str) -> tuple[GranuleEntry, ...]  # () when absent
    @staticmethod
    def append(uri: str, entries: Iterable[GranuleEntry]) -> None
    @staticmethod
    def remove(uri: str, granule_urs: Collection[str]) -> None
```

- [ ] Step 1: failing tests — round trips (file + moto-S3); absent ledger
  reads as empty; append dedupes by `granule_ur`;
  `validate_against_axis` passes on exact match, raises on one differing
  bit and on length mismatch.
- [ ] Step 2: implement; green + lint + commit.

### Task 8: Forward-processing consumer (routing rules)

**Files:**
- Modify: `lambda/virtualizarr-processor/virtualizarr_processor/processor.py`,
  `.../typing.py` (`process_file` returns `ProcessOutcome`)
- Modify: `lambda/process_messages/handler.py` (batch pre-sort by parsed
  time; treat `DEFERRED` as success; metrics/log per outcome)
- Test: extend `tests/test_tempo_processor.py`, `tests/test_handler.py`

**Interfaces (Produces):**
```python
class ProcessOutcome(enum.Enum):
    WRITTEN = "written"      # appended or overwritten in place
    DEFERRED = "deferred"    # recorded in the pending ledger
    REJECTED = "rejected"    # validation failure — SQS retry → DLQ

def initialize_repo(self) -> Repository      # open_or_create; if empty store,
    # create template at time=0 via resize(template, {"time": 0}), write
    # lat/lon, commit, and write an empty-granule store manifest
def initialize_session(self, repo) -> Session  # writable "main"
def process_file(self, file_key: str, session: Session) -> ProcessOutcome
    # parse + validate V1–V3, then route on t vs axis (spec §5 table):
    #   t in axis & same granule_ur in manifest → region="auto" overwrite
    #   t in axis & different granule_ur       → REJECTED (hard)
    #   t > axis max                           → append_dim="time"
    #   t <= axis max, t not in axis           → PendingLedger.append; DEFERRED
    # every write passes last_updated_at=<parse start> (spec I3)
def commit_processed_files(self, session) -> str
    # commit, then StoreManifest.write with the appended/overwritten entries
def garbage_collect(self, expiry_time) -> GCSummary
```

- [ ] Step 1: failing tests — in-order append of two fixtures reads back
  exactly; redelivered duplicate (same UR, same time) → WRITTEN and store
  values unchanged (idempotent); same time + different UR → REJECTED and
  store unchanged; historical granule → DEFERRED, ledger contains it,
  axis unchanged; republication (same UR/time, changed source values) →
  WRITTEN and read-back shows the new values.
- [ ] Step 2: implement processor + handler changes.
- [ ] Step 3: green + lint + commit.

### Task 9: Re-sort job (scheduled insertion)

**Files:**
- Create: `lambda/backfill/backfill_handlers/resort.py`
- Modify: `lambda/virtualizarr-processor/virtualizarr_processor/processor.py`
  (add `initialize_resort_store`)
- Test: `tests/test_resort.py`

**Interfaces (Produces):**
```python
def merge_pending(manifest: BackfillInventory,
                  pending: tuple[GranuleEntry, ...]) -> BackfillInventory
    # sorted union; model validators reject collisions loudly
def first_shifted_index(manifest: BackfillInventory,
                        merged: BackfillInventory) -> int   # earliest insert
class Processor:
    def initialize_resort_store(self, repo,
                                merged: BackfillInventory) -> str
        # on branch "resort" off main: resize time-bearing arrays by +k,
        # rewrite the axis to merged.times(), validate, commit clean base
# resort.py handler: merged inventory → suffix urls (index ≥ s) →
# reuse partition/fork/worker/reduce over them → promote(main ← resort) →
# StoreManifest.write(merged) → PendingLedger.remove(folded URs)
```

- [ ] Step 1: failing end-to-end test — store of 3 in-order fixtures
  (via Task 8 appends) + ledger of 2 (one adjacent swap, one deep
  historical) → run resort → axis strictly increasing and equals merged
  inventory; every scan's `vertical_column` matches h5py ground truth
  (both moved and unmoved slices); ledger empty; manifest matches axis.
- [ ] Step 2: collision test — pending granule whose time equals an
  existing slot with a different UR → job aborts before any branch write.
- [ ] Step 3: implement; green + lint + commit.

### Task 10: CDK wiring

**Files:**
- Modify: `cdk/stack.py`, `cdk/settings.py`
- Test: `tests/cdk/` assertions

Changes: Init state payload gains `inventory_uri` (from execution input);
forward consumer Lambda gets reserved concurrency 1 (spec §5 —
single-writer appends; SQS `max_concurrency` cannot go below 2);
`RESORT_SCHEDULE` (EventBridge rule, default daily, disabled when
backfill-only) triggering the resort job; `STORE_MANIFEST_URI` /
`PENDING_LEDGER_URI` env for the Lambdas.

- [ ] Step 1: failing CDK assertions for each; Step 2: implement;
  Step 3: green + lint + commit.

### Task 11: Inventory builder upgrade

**Files:**
- Modify: `exploration/build_backfill_inventory.py`
- Test: `tests/exploration/test_build_backfill_inventory.py`

Changes: dedupe republications (keep newest revision per granule UR stem);
read each granule's exact `/time[0]` (`h5py` over `earthaccess.open()`
files, bounded concurrency + backoff); emit `BackfillInventory` JSON via
the Task 2 models; `--skip-exact-times` escape hatch removed — exact times
are mandatory (spec). Offline tests: pure helpers take fake granule objects
+ a `read_time(url) -> float` injectable; assert dedupe, ordering by file
time (not CMR time), duplicate-time failure, model round-trip.

- [ ] Step 1: failing tests; Step 2: implement; Step 3: green + lint + commit.

### Task 12: Post-promote QA script, docs, full gate

**Files:**
- Create: `scripts/verify_store.py` — sample N random time steps, map each
  slice to its source URL via the store manifest (Task 7), compare a
  configurable slice of each variable read through the store against h5py
  reads of the source file; exit non-zero on any mismatch.
- Modify: `README.md` (processor status section: config, artifacts,
  inventory format, forward routing rules, how to run
  generator/builder/resort/verify)
- Test: `tests/test_verify_store.py` (fixture store with one corrupted
  reference → non-zero; clean store → zero)

- [ ] Step 1: tests; Step 2: implement; Step 3: README.
- [ ] Step 4: full gate — `uv run pytest`, `uv run ruff check . && uv run
  ruff format --check .`, `uv run mypy`; fix all; final commit.

## Self-review notes

- Spec §1 → Tasks 2, 11; §2 → Tasks 6, 10; §3 → Tasks 1, 5, 6, 8; §4 →
  Tasks 4, 6, 8, 12; §5 (forward) → Tasks 7, 8, 9, 10; limitations
  documented in spec only (no code) — deliberate.
- Names used across tasks: `CollectionConfig`, `load_collection`,
  `load_template`, `load_coordinates`, `BackfillInventory`, `GranuleEntry`,
  `open_flat_granule`, `granule_time`, `make_registry`,
  `write_tempo_granule`, `StoreManifest`, `PendingLedger`,
  `ProcessOutcome`, `merge_pending`, `first_shifted_index`,
  `initialize_resort_store` — consistent.
- Known risk (called out for the implementer): whether xarray region writes
  require dropping `latitude`/`longitude` from the vds — Task 6's e2e test
  settles it either way; drop-after-validate is the default.
- Task 1 was implemented before `time_chunk_size` was added; folding the
  field into `CollectionConfig` + both TOMLs is the first step when
  implementation resumes (Task 5 consumes it).
