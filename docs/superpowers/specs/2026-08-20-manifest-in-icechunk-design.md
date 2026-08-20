# Design: Store manifest and pending ledger inside the Icechunk repo

**Problem.** The store manifest (`store-manifest.json`) and pending ledger
(`pending-ledger.json`) live as plain S3 objects beside the repository,
written *after* the icechunk commits they describe. Every window between a
commit and its state write is a failure mode: a crash leaves main with no
matching manifest, a resort reading state before capturing main's tip can
erase a concurrent append, and the ledger's unguarded read-modify-write is
raced by two Lambdas. A whole-repo review confirmed five distinct
serve-wrong-data or wedge-the-pipeline findings rooted in this split.

**Decision.** The manifest and ledger become part of the store itself,
committed atomically with the data they describe. A reader at any snapshot
gets exactly the state that describes that snapshot; drift is
unrepresentable rather than checked for.

## Storage layout

- **Manifest arrays** — two variable-length-string arrays on the append
  dimension, declared in the store template like every other array:
  - `granule_ur` — the granule UR owning each time slot
  - `granule_url` — the source object URL for each slot
  - dtype `string` (zarr v3 vlen-utf8 codec, verified on the pinned
    zarr 3.2.1 + icechunk 2.1.1), chunk shape `(time_chunk_size,)` matching
    the time axis, `dimension_names=("time",)`, fill value `""`.
  - Alignment with the axis is enforced by zarr shape, not by validation
    code. The arrays ride the same code paths as the data: resized by
    `resize()`, created by `create_empty_store`, resized wholesale on
    resort, appended on forward ingest.
- **Store metadata attribute** — root-group attribute `tempo_store`:
  `{"schema": "tempo-backfill-inventory/1", "collection", "concept_id",
  "time_units", "built_at"}`. The scalars that used to head the manifest
  JSON.
- **Pending ledger attribute** — root-group attribute `pending_ledger`:
  a JSON list of `GranuleEntry` dicts. Not axis-aligned (its entries are
  precisely the granules *not* in the store), small and bounded
  (`RESORT_MAX_FOLD`-scale), so an attribute, not an array.

`BackfillInventory` stays the interchange and validation model (strictly
increasing times, no duplicate URs); the arrays/attributes are its storage
representation with a to/from adapter in `manifest.py`.

## Behavioral consequences

- **Promote is atomic.** The backfill/resort branch commit already carries
  manifest and ledger; promote is validate + one compare-and-swap. No
  post-CAS writes, no crash window, `scripts/rebuild_manifest.py` deleted.
- **Resort pins one snapshot.** The handler captures main's tip *first*,
  reads axis + manifest + ledger from a readonly session at that snapshot,
  and passes the tip to `initialize_resort_store`. A consumer append
  landing mid-run moves main, the promote CAS fails, and the run retries —
  the append can no longer be silently erased.
- **Deferral is atomic with the batch.** The consumer writes the ledger
  through the session (root-attr update), so an all-DEFERRED batch has
  session changes and can never raise `NoChangesToCommitError`; a failed
  batch leaves no orphaned ledger entries.
- **No ledger races.** Consumer and resort both update the ledger through
  icechunk commits; concurrent writes surface as commit conflicts / CAS
  failures to retry instead of silent lost updates.
- **Moved-timestamp republication rejected at the gate.** A granule whose
  UR already owns a manifest slot but whose in-file time no longer matches
  any axis slot is REJECTED (loudly, to the DLQ) instead of DEFERRED —
  the poison entry that permanently crash-looped resort can no longer
  enter the ledger.
- **Poller bootstrap simplifies.** The stdlib-only CMR poller can no
  longer read `built_at` from a manifest JSON. First-poll start becomes
  `$POLL_START_ISO` when set (operator supplies the backfill inventory's
  build time), else `now - DEFAULT_LOOKBACK`.
- **Store validation tolerates state attributes.** `validate_store` gains
  a `volatile` parameter; callers pass the pipeline-state attribute names
  so `tempo_store`/`pending_ledger` don't warn as unexpected.
- **User-visible provenance.** The manifest arrays are visible to store
  readers as per-timestep provenance; that is a feature, not a leak.

## Deletions this unlocks

- `scripts/rebuild_manifest.py` and its tests (repairs a divergence that
  can no longer occur).
- `StoreManifest.validate_against_axis` and every call site;
  `_manifest_entries`/`_manifest_entry_at` consistency guards.
- All state-URI plumbing in `manifest.py` (`default_state_uri`,
  `_read_bytes`/`_write_bytes`/`_is_s3`/`_split`/`_s3_client`) and the
  `STORE_MANIFEST_URI`/`PENDING_LEDGER_URI` env vars, settings fields, and
  CDK wiring. `storage_prefix()` stays (it addresses the repo itself).
- The `VirtualizarrProcessor` Protocol in `typing.py` (single
  implementation, no polymorphic caller; existed as cross-repo templating).
  `BranchInit` and `ProcessOutcome` stay.
- `process_resort_file` (one-line delegation; `process_backfill_file`'s
  session parameter is loosened to `ForkSession | Session`).

## Non-goals

- The CMR poll watermark stays in S3: it is owned by a stdlib-only Lambda,
  deliberately imprecise, and not part of the store's self-description.
- No Iceberg (per the veda-odd#460 discussion, its tabular machinery is
  oversized for a manifest of this shape and scale).
- No migration path: no production store exists yet; stores are rebuilt by
  running the backfill.

## Assumptions / ceilings

- Backfill initialization requires a fresh `main` (pre-existing template
  arrays make `create_empty_store` raise); unchanged from today.
- `commit_processed_files` re-reads the full manifest arrays once per
  batch for validation — O(n) strings once an hour, fine for decades of
  TEMPO cadence.
