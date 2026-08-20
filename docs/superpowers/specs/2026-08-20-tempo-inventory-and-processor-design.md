# TEMPO Backfill Inventory & Processor — Design

**Status:** approved design for implementation (plan:
`docs/superpowers/plans/2026-08-20-tempo-inventory-and-processor.md`)

## Goal

Replace the template's synthetic sample processor with the TEMPO-specific
pipeline: virtualize **all variables** of `TEMPO_HCHO_L3` V04 and
`TEMPO_NO2_L3` V04 into one Icechunk repository per collection, driven by a
**declarative, strongly typed configuration**, with validation at every
insertion such that **a user can never read wrong data from the virtual
store**. This design covers (a) the file enumerating the granules to be
backfilled (the *inventory*), (b) the backfill approach, (c) declarative
initial store creation, and (d) validation on insertion.

## Established facts this design rests on

Verified against the ten granules in `/workspace/context/data/` and the
profiling in `context/findings.txt` (20-granule HCHO sweep):

1. **The in-file `/time` value is NOT the filename/CMR timestamp.**
   `TEMPO_HCHO_L3_V04_20260819T174200Z_S009.nc` has
   `/time[0] = 1471196538.0244286` (= 2026-08-19T17:42:18.02), while the
   filename, `time_coverage_start`, and CMR `BeginningDateTime` say
   17:42:00. An axis built from CMR times would never equal the file
   values.
2. **`time_coverage_start_since_epoch` (root attr) equals `/time[0]`
   bit-exactly** in all 10 local granules (both collections). The file is
   internally consistent; either source works once you open the file.
3. **`latitude`/`longitude` are bit-identical across granules and across
   the two collections**, but are *not* reconstructible as a float32
   linspace (verified: no start/step formula reproduces them bit-exactly).
   They must ship as a data artifact.
4. Structure (shapes, dtypes, chunk grids, codecs: shuffle + deflate(1))
   is uniform across granules; only 4 root attrs vary beyond the known
   volatile list: `geospatial_lat_min/max`, `geospatial_lon_min/max`.
   These four are **missing from `TEMPO_L3_VOLATILE_ATTRIBUTES`** today
   and would make `validate_granule` reject every granule that doesn't
   match the reference — they must be added to the volatile set.
5. Flattening every group's variables to the root group is collision-free
   for both collections (all variable names distinct), and flat-at-root is
   the layout proven end-to-end with titiler-multidim.
6. `weight` is stored per scan as `(latitude, longitude)` with chunks
   (590, 1550) but varies per scan — it must be promoted to
   `(time, latitude, longitude)` via `expand_dims` (zero-copy on
   `ManifestArray`) or a reader silently gets scan 1's weights for every
   scan.
7. `to_icechunk(..., region="auto")` locates the target slice by looking
   the granule's loaded `time` coordinate up in the store's `time` index
   (exact match, via xarray). Wrong/unknown time ⇒ hard error, never a
   silent misplacement.
8. CMR publishes out of order (~7% of adjacent pairs), republishes rarely
   (0.4%, short-window corrective), and the V04 historical archive is
   still being drip-fed backwards. An inventory goes stale within days;
   forward processing must expect historical granules interleaved with
   new scans.

## 1. The inventory file (granule enumeration)

### Why the current format is insufficient

`exploration/build_backfill_inventory.py` emits a bare JSON array of URLs
ordered by CMR `BeginningDateTime`. Because of fact 1, init cannot build
the store's time axis from that file, and the axis is the linchpin of the
whole backfill: `region="auto"` aligns each granule against it, and its
values are what users read as the time coordinate.

### New format: a typed granule manifest

The inventory becomes a JSON document validated by pydantic models that
live in `virtualizarr_processor.inventory` (shared by the builder script,
the init step, and the partition handler):

```json
{
  "schema": "tempo-backfill-inventory/1",
  "collection": "TEMPO_HCHO_L3",
  "concept_id": "C3685897141-LARC_CLOUD",
  "time_units": "seconds since 1980-01-06T00:00:00Z",
  "built_at": "2026-08-20T00:00:00Z",
  "granules": [
    {
      "url": "s3://asdc-prod-protected/TEMPO/TEMPO_HCHO_L3_V04/2026.08.19/TEMPO_HCHO_L3_V04_20260819T174200Z_S009.nc",
      "granule_ur": "TEMPO_HCHO_L3_V04_20260819T174200Z_S009",
      "time": 1471196538.0244286
    }
  ]
}
```

- `time` is the **exact float64 `/time[0]` value read from the file**
  (equivalently `time_coverage_start_since_epoch`), serialized via
  `repr`-round-trip-safe JSON floats. No datetime conversion anywhere in
  the pipeline — raw axis values end to end, so there is no precision
  seam.
- Model validators enforce, at parse time (both when building and when
  reading): non-empty; every URL ends in `.nc`; granule URs unique;
  entries sorted by `time`, strictly increasing (no duplicates — merge is
  last-writer-wins, so two granules on one slot would corrupt silently);
  `collection` matches the deployed collection config.

### Construction

The builder (`exploration/build_backfill_inventory.py`, upgraded):

1. Queries CMR for all granules of the collection (windowable).
2. Dedupes republished granules: group by nominal scan (granule UR minus
   revision), keep the latest revision.
3. Opens each file (in-region S3 via earthaccess/obstore; low
   concurrency + backoff over HTTPS otherwise) and reads the exact
   `/time` value. This is the one unavoidable per-file read (~a few KB of
   HDF5 metadata each); it is also a cheap early check that every file is
   openable at all.
4. Emits the manifest through the same pydantic models (so an invalid set
   fails loudly at build time, not mid-backfill).

Staleness (fact 8) is handled operationally, exactly as the README's
sequencing section describes: enable SNS→SQS buffering *before* building
the inventory, backfill, then drain the queue via forward processing.

## 2. Backfill approach

Unchanged from the proven fork/merge pipeline (partition → init → per
partition: fork → distributed workers → reduce → promote), with these
TEMPO-specific bindings:

- **Init** now receives `inventory_uri` (small Step Functions/CDK change:
  the state machine already gets it as execution input for the partition
  step). `initialize_backfill_store(repo, inventory)`:
  1. loads the collection's committed template (`GroupSpec` JSON),
  2. `resize(template, {"time": len(inventory.granules)})`,
  3. `create_empty_store` on the `backfill` branch (metadata only),
  4. writes the `time` axis from the manifest's values and
     `latitude`/`longitude` from the committed reference artifact,
  5. `validate_store(...)` and commits — the clean base snapshot.
- **Partition** slices `inventory.granules` into manifests of bare URLs —
  the worker contract (`file_keys: list[str]`) is unchanged; workers do
  not need manifest times because the file itself carries the same value
  (fact 2) and `region="auto"` enforces agreement with the store axis.
- **Worker** (`process_backfill_file`): parse → transform → validate →
  region-write; never commits. Any failure raises: the Step Functions run
  fails before promote rather than shipping a hole.
- **Reduce/Promote**: unchanged; promote re-runs `validate_store` plus a
  full time-axis equality check against the store's own committed axis
  before fast-forwarding `main`.
- Partition size / concurrency: keep template defaults
  (`BACKFILL_PARTITION_SIZE=500` ⇒ ~28 commits for 13.6k granules;
  `BACKFILL_MAX_CONCURRENCY=50` is safe against ASDC only with in-region
  `s3://` access — inventory `--access direct` is the production default).

## 3. Declarative initial store creation

Three committed, versioned artifacts per collection, all consumed through
strongly typed loaders:

1. **Collection config** (`virtualizarr_processor/collections/{hcho,no2}.toml`)
   parsed into a frozen pydantic `CollectionConfig`:
   - `name`, `collection_shortname`, `concept_id`
   - `append_dim = "time"`, `time_units`
   - `flatten_groups = ["product", "geolocation", "qa_statistics", "support_data"]`
   - `promote_to_time = ["weight"]`
   - `drop_variables = []` (the "all variables" default; the knob the
     assignment asks for)
   - `volatile_attributes` (extends the base TEMPO list with the four
     `geospatial_*` bounds)
   - `template = "hcho_template.json"`, `coordinates = "coordinates.npz"`
   The deployed instance selects its collection via `TEMPO_COLLECTION`
   (one repo per collection, per the README).
2. **Store template** (`GroupSpec` JSON, one per collection): generated by
   `scripts/generate_template.py` from local reference granules by running
   the *actual ingest path* — parse with `HDFParser`, flatten, promote,
   write to a throwaway in-memory Icechunk store via `to_icechunk`, then
   `GroupSpec.from_zarr` and `strip_attributes(volatile)`. Generating
   through the write path guarantees the template's dtypes/chunks/codecs
   are exactly what workers will region-write into, and the generator
   cross-checks **all** provided granules (both collections' five local
   files): any attribute that varies and is not declared volatile is a
   hard generation error, forcing explicit classification instead of a
   silent choice.
3. **Reference coordinates** (`coordinates.npz`, shared by both
   collections since the grids are bit-identical): the float32
   `latitude`/`longitude` arrays (~42 KB), written by the same generator.

`initialize_repo` (forward path) uses the same template: open-or-create;
if the store is empty, create it at `time=0` length so forward appends
can begin without a backfill.

## 4. Validation on insertion ("no wrong data")

Layered checks, all of which turn into a raised
`GranuleValidationError`/`StoreValidationError` → failed worker → failed
run (backfill) or failed/redriven SQS batch (forward). Nothing is ever
committed for a granule that failed a check.

Per granule (worker, before writing):

| # | Check | Wrong-data mode it prevents |
|---|-------|------------------------------|
| V1 | `validate_granule(template, vds, volatile=...)`: every shared attribute the template declares must match; structure mismatches (dtype/chunks/shape) surface as `to_icechunk` errors against the fixed store arrays | a structurally divergent granule (reprocessed with different chunking/compression) written where readers decode with the wrong codec |
| V2 | `coordinates=` reference lat/lon: bit-exact `np.array_equal` against the committed artifact | a regridded/shifted granule silently landing on the standard grid |
| V3 | time integrity: granule has exactly one time step and `/time[0] == time_coverage_start_since_epoch` (fact 2) | a corrupt/inconsistent file defining its own position |
| V4 | `region="auto"`: the granule's time must exist in the store axis (built from the manifest) — a granule whose time is unknown or already differs errors out | misplacement; a stale inventory entry vs. a republished file with a changed time |
| V5 | `weight` promotion is applied before write | per-scan weights silently frozen at scan 1 |

Store-level:

- Init: `validate_store(resized template, ...)` after creation, before
  the base commit.
- Promote: `validate_store` again on the final tip + time axis strictly
  increasing and equal to the manifest's values; only then fast-forward
  `main`. (Completeness of data chunks is guaranteed by orchestration:
  any worker failure fails the run before promote.)
- Post-promote QA (script, sample-based): open the store, pick random
  granules, compare a data slice read through the virtual store against
  the same slice read directly from the source file with h5py.

Forward path (`process_file`): same V1–V3, then the routing rules of §5.

## 5. Forward processing

Forward processing cannot be "append per SQS message" for TEMPO: fact 8
says ~7% of adjacent new scans arrive swapped and the historical archive
is drip-feeding *years*-old granules interleaved with new ones. Appending
whatever arrives would make the time axis non-monotonic — xarray then
raises on `sel(time=slice(...))` and titiler's time handling degrades —
while rejecting everything out of order would shunt a large, growing
fraction of the collection into a dead letter queue with no way back in.

### Invariants (shared with backfill)

- **I1** `main`'s time axis is strictly increasing with no fill values.
- **I2** Each slice's axis value equals the in-file `/time` of the granule
  whose references occupy it.
- **I3** Every virtual ref is stamped with `last_updated_at` (supported by
  `to_icechunk` and checked by icechunk at read): if a source object is
  overwritten after ingest — exactly what a republication does — reads of
  stale refs **fail loudly** instead of returning bytes from a changed
  file. This closes the one silent-wrong-data hole validation-at-insert
  cannot see.
- **I4** A **store manifest** — the same `BackfillInventory` document,
  now a living artifact in S3 next to the repo — enumerates
  `(url, granule_ur, time)` for every slice, in axis order. Backfill's
  promote writes it (= the backfill inventory); the forward consumer and
  the re-sort job update it; every consumer of it first validates it
  against the store's actual axis (bit-exact) and aborts on divergence.
  It is what maps a slice back to its source file — needed by the
  re-sort job and the QA sampler.

### Consumer routing rules

The SQS consumer parses + validates each granule (V1–V3), sorts the batch
by time, dedupes, then routes each granule with time `t` against the
store axis (max `T`):

| Case | Action |
|------|--------|
| `t` ∈ axis, manifest entry has same `granule_ur` | **Overwrite in place** via `region="auto"`: republication of a known scan, or an at-least-once redelivery. Idempotent; refreshes refs and their `last_updated_at` stamp. |
| `t` ∈ axis, different `granule_ur` | **Hard reject** — two distinct granules claiming one time step is a data inconsistency to investigate, never to write. |
| `t > T` | **Append** (`append_dim="time"`), then update the store manifest. |
| `t ≤ T`, `t` ∉ axis | **Defer**: record `(url, granule_ur, time)` in the *pending ledger* (S3 JSON, `GranuleEntry` list) and consume the message successfully. The re-sort job folds it in. |

At-least-once SQS delivery is safe by construction: a redelivered
already-appended granule hits the overwrite case; a redelivered deferral
is deduped by the ledger. The consumer must be effectively single-writer
(Lambda reserved concurrency 1 — concurrent appends conflict on the
array resize and on the manifest update; note SQS `max_concurrency`
cannot go below 2, so reserved concurrency is the mechanism).

### The re-sort job (scheduled insertion)

A scheduled job (e.g. daily, plus on-demand) folds the pending ledger in:

1. Merge store manifest + ledger into one typed inventory (its validators
   enforce strict monotonicity — a collision aborts loudly).
2. On a `resort` branch: resize every time-bearing array by +k, rewrite
   the time axis to the merged values, and compute the first shifted
   index `s` (earliest inserted time).
3. Re-virtualize and region-write **every granule with index ≥ s** — the
   pending ones and the existing ones whose slots shifted — from their
   manifest URLs, through the *same* worker path as backfill
   (fork/merge partitions; the Step Functions machinery already scales
   for a deep `s`).
4. `validate_store` + axis == merged inventory, fast-forward `main`,
   write the new store manifest, clear the folded ledger entries.

Adjacent-scan swaps (the routine case) give `s` near the axis end —
pennies. The historical drip-feed gives a deep `s` once per scheduled
run — a bounded partial backfill, not a per-arrival cost. Re-keying by
copying existing refs (no re-parse) is a possible optimization, but
re-virtualizing from source reuses the proven path and re-stamps I3
checksums; a full re-backfill into a fresh branch remains the recovery
hammer.

### Design consequences for the rest of the pipeline

- **Time-axis chunking**: a template generated naively from a one-granule
  store would chunk `time` as `(1,)` — after resize, 13.6k eight-byte
  chunks and a pathological open for every reader, plus a per-batch
  axis read in the consumer. The collection config therefore declares
  `time_chunk_size` (default 16384: one chunk covers years) and the
  template generator overrides the axis chunk grid accordingly. This
  fixes backfill too, and was found only by planning forward processing.
- **Stamping everywhere**: backfill workers, the consumer, and the
  re-sort job all pass `last_updated_at` (the worker's parse start time)
  to `to_icechunk`.
- **`process_file` outcome** is three-way (`written | deferred |
  rejected`), not a bool — deferral is success to SQS but must be
  visible in logs/metrics.
- The protocol's dormant `cron_processing` hook becomes the re-sort
  entry point.

## 6. Limitations / deferred

- Reading ~13.6k file headers at inventory build (~minutes in-region) is
  accepted as the price of an exact axis; there is no metadata-only
  source for the exact values (fact 1).
- `_NCProperties` (netcdf/hdf5 library versions) stays in the template:
  if ASDC upgrades their writer, granules will be *rejected* until the
  volatile list or template is updated. Strict-by-default is intentional;
  rejection is loud, wrong data is silent.
- Between a republication landing in S3 and its SNS message being
  consumed, readers of the affected slice get I3 failures (loud, not
  wrong). Accepted: the window is minutes and the alternative (no
  stamping) is silent corruption.
- The re-sort job serializes with forward consumption (pause the queue
  consumer during promote, or accept one rebase retry); the CDK task
  gates the event source mapping during a resort run.
