# tempo-virtual-zarr-pipeline

Virtual Zarr / Icechunk ingestion pipeline for TEMPO Level 3 gridded products, supporting data delivery for the AIR4US portal ([NASA-IMPACT/veda-odd#438](https://github.com/NASA-IMPACT/veda-odd/issues/438)). It targets two collections hosted at NASA ASDC:

| Collection | Concept ID | DOI |
|---|---|---|
| `TEMPO_HCHO_L3` V04 — gridded formaldehyde total column | `C3685897141-LARC_CLOUD` | [10.5067/IS-40e/TEMPO/HCHO_L3.004](https://doi.org/10.5067/IS-40e/TEMPO/HCHO_L3.004) |
| `TEMPO_NO2_L3` V04 — gridded NO2 tropospheric and stratospheric columns | `C3685896708-LARC_CLOUD` | [10.5067/IS-40E/TEMPO/NO2_L3.004](https://doi.org/10.5067/IS-40E/TEMPO/NO2_L3.004) |

The repository was instantiated from the [virtualizarr-data-pipelines](https://github.com/developmentseed/virtualizarr-data-pipelines) template, which provides the AWS CDK infrastructure documented below. Each collection gets its own Icechunk repository, deployed as a separate instance of the same stack. Improvements that generalize beyond TEMPO belong in the template.

## The virtual stores

Each store presents one collection as a single dataset: all variables from every group, flattened to the root group (the layout titiler-multidim requires), concatenated along `time`. Every 3-D variable has per-granule dims `(time, latitude, longitude)` = (1, 2950, 7750) with the source files' shuffle + deflate(1) codecs, chunked (1, 738, 1938) for float64 and (1, 984, 2584) for the rest. The 1-D coordinates are contiguous in the source netCDF-4, so they are loaded and stored as native chunks (`[native]`); everything else is virtual references into the source files.

**`TEMPO_HCHO_L3` V04** — 13,611 scans as of 2026-07-30:

```
/                                       dims: time (append dim), latitude=2950, longitude=7750
├── time         (time)                 float64, seconds since 1980-01-06T00:00:00Z  [native]
├── latitude     (latitude)             float32  [native]
├── longitude    (longitude)            float32  [native]
├── weight       (time, latitude, longitude)  float32  # promoted; stored per scan without a time dim
├── vertical_column                          float64
├── vertical_column_uncertainty              float64
├── main_data_quality_flag                   int16
├── solar_zenith_angle                       float32
├── viewing_zenith_angle                     float32
├── relative_azimuth_angle                   float32
├── num_vertical_column_samples              int32
├── min_vertical_column_sample               float64
├── max_vertical_column_sample               float64
├── fitted_slant_column                      float64
├── fitted_slant_column_uncertainty          float64
├── albedo                                   float32
├── amf                                      float32
├── eff_cloud_fraction                       float32
├── amf_cloud_fraction                       float32
├── amf_cloud_pressure                       float32
├── surface_pressure                         float32
├── terrain_height                           int16
├── snow_ice_fraction                        float32
└── pbl_height                               int16
```

**`TEMPO_NO2_L3` V04** — 13,618 scans as of 2026-07-30. Same coordinates and layout, with the NO2 variable set: `vertical_column_troposphere`, `vertical_column_stratosphere`, `vertical_column_total` and their uncertainties, twelve `qa_statistics` min/max/count variables, `amf_total`/`amf_troposphere`/`amf_stratosphere`, `tropopause_pressure`, and the same geolocation and ancillary variables as HCHO (36 data variables in all).

Points worth knowing:

- The two time axes are independent (a handful of scans exist in only one collection, and both grow separately), which is why each collection gets its own repository. Joint analysis aligns at read time.
- `latitude`/`longitude` are bit-identical between the two products and fixed across scans.
- `weight` varies per scan but is stored without a time dimension in the source files. It is promoted to `(time, latitude, longitude)` at ingest; without that, concatenation would silently keep only the first scan's values.
- Production stores reference `s3://asdc-prod-protected/...` in us-west-2. Readers authorize the virtual chunk container with temporary credentials from <https://data.asdc.earthdata.nasa.gov/s3credentials>. EDL-authed HTTPS also works but is rate-limited by CloudFront.

## How the pipeline works

**Collection configuration.** `virtualizarr_processor/collections/{hcho,no2}.toml` declares each collection: which groups to flatten, which variables to promote or drop, the volatile (per-granule) attributes, the time-axis chunk size, and the names of two generated artifacts — the store template (a pydantic-zarr `GroupSpec` as JSON) and the reference `latitude`/`longitude` arrays. A deployment selects its collection with `TEMPO_COLLECTION`. Regenerate the artifacts from reference granules with `uv run scripts/generate_template.py`; generation fails if the granules disagree on anything not declared volatile.

**Backfill inventory.** `uv run exploration/build_backfill_inventory.py` produces the input for a backfill: a validated JSON document with one entry per granule — its `.nc` link, its granule UR, and its exact in-file `/time[0]`. The in-file time differs from the CMR and filename timestamps (`...T174200Z` has `/time` = 17:42:18.02), and the store's time axis is built from these exact values, so the builder reads a few KB of every granule's header. The document is rejected if it is empty, unsorted, or contains duplicate times or granule URs — checked again when the pipeline reads it.

**Backfill.** The Step Functions run partitions the inventory, creates the full-shape store on a `backfill` branch (metadata plus the native coordinates, nothing else), and fans out workers. Each worker parses its granule, validates it, finds its slot by matching the granule's time against the axis exactly, and writes its references into a disjoint region of an Icechunk fork; a reducer merges each partition's forks into one commit. Any worker failure fails the run before anything reaches `main`. The manifest (as two vlen-string arrays on the time axis) and an empty pending ledger are committed on the `backfill` branch alongside the data, so the final promote step only re-validates the store against the template, the axis and manifest against the inventory, and the coordinates against the reference arrays, then moves `main` to the already-committed backfill tip. The move is a compare-and-swap against the tip the branch was created from, so a commit that landed on `main` mid-run fails the promote instead of being discarded; nothing is written after the CAS.

**Validation.** A granule is written only if it matches the template's shared attributes, carries the bit-identical reference grid, and its `/time[0]` equals its own `time_coverage_start_since_epoch` attribute. Every virtual reference is stamped with the source object's observed modification time, so if a source file is later overwritten, reads of the stale references fail instead of returning bytes from a changed file.

**Forward processing.** A scheduled Lambda polls CMR for granules whose revision date advanced past a persisted watermark and enqueues them (ASDC publishes no SNS topic for the bucket; see the note below). The SQS consumer routes each granule:

| Situation | Action |
|---|---|
| time is after the axis end | append |
| time occupies a slot, same granule UR | overwrite the slot in place (republication or redelivery) |
| time occupies a slot, different granule UR | reject to the DLQ |
| time is out of order | record in the pending ledger, consume the message |

Out-of-order arrivals are common: TEMPO's historical archive is still being back-filled, and about 7% of adjacent scans publish swapped. A scheduled re-sort job pins `main`'s tip first, reads the axis, manifest, and pending ledger from a readonly session at that snapshot, and merges the pending ledger into the store on a `resort` branch built from it. Already-ingested slots at or after the earliest insertion are relocated with icechunk's `reindex_array`, a metadata-only move that never re-reads a source file, so deep historical insertions are cheap; only the inserted granules are parsed. Folded ledger entries are drained inside the same commit that performs the fold, and `main` moves by the same compare-and-swap as the backfill promote, so a consumer append that landed on `main` mid-run fails the promote instead of being silently erased. One run folds at most `RESORT_MAX_FOLD` pending granules, earliest first, and promotes that as durable partial progress; the rest drain on later runs. The consumer runs at reserved concurrency 1 because concurrent appends conflict.

The store manifest (which granule owns which slot, as two arrays on the time axis) and the pending ledger live inside the Icechunk store itself, as root-group attributes and arrays committed atomically with the data they describe; only the CMR poll watermark lives in `s3://<icechunk bucket>/<prefix>state/`. The poller's first poll starts from `$POLL_START_ISO` when set (typically the backfill inventory's build time), else a fixed lookback.

**Verification.** `uv run scripts/verify_store.py` samples random time steps and, for each, asks CMR for the granule nearest that time, independently of the pipeline's own bookkeeping. The file CMR points at must match the store's axis time exactly, and random windows of every variable are compared both raw (store bytes against h5py reads) and CF-decoded (the read path users take). Because the URL comes from CMR, a store still referencing a superseded revision is caught even when the old object is intact. `--completeness` diffs CMR's granule listing against the manifest and pending ledger; `--offline` falls back to manifest-provided URLs. The script authorizes the virtual chunk container itself, with the same Earthdata material the workers use (or ambient AWS access to the source bucket) — the pipeline's own writers never hold chunk-read access. Any mismatch or read failure exits non-zero.

**Recovery.** The store manifest and pending ledger are written through the same session as the data they describe, so they commit atomically with it and cannot drift from it or race a concurrent writer; no repair script is needed. A promote rejected by the compare-and-swap needs no repair either: nothing was consumed, and the next scheduled run retries against the new `main` tip. A same-time/different-UR collision between the manifest and the pending ledger is different: it aborts the resort run by design (a loud, repeatable failure rather than a silent overwrite) and needs an operator to remove the bad entry from the ledger by hand — `rebuild_manifest.py` no longer exists — with a small Icechunk commit that reads the `pending_ledger` root attribute, drops the offending entry, and writes it back.

**Source credentials.** When Earthdata Login material is configured (`EARTHDATA_TOKEN`, `EARTHDATA_USERNAME`/`EARTHDATA_PASSWORD`, or a Secrets Manager secret at `EARTHDATA_SECRET_ARN` holding JSON with `token` or `username`+`password`), workers exchange it for temporary S3 credentials at the bucket's `s3credentials` endpoint (`EARTHDATA_S3_CREDENTIALS_ENDPOINT` overrides). Without it, reads use the Lambda role's ambient IAM access, which requires a bucket-policy grant on the source bucket.

> **Feeding the queue:** ASDC does not publish an SNS notification topic for
> `asdc-prod-protected`, so this pipeline polls CMR instead. Duplicate
> enqueues are harmless (the consumer routing is idempotent), and a
> 30-minute poll cadence is negligible next to the product's ~3 h median
> production lag. A provider-side SNS topic would still be worth
> requesting from ASDC: the queue could subscribe directly, with the
> poller kept as a backstop for missed notifications.

## Deploying and running

Each collection deploys as its own stack from a committed env file:
[`.env_hcho`](./.env_hcho) and [`.env_no2`](./.env_no2). Fill in `ACCOUNT_ID`
and `ICECHUNK_BUCKET`, one shared bucket in us-west-2 created once with
`aws s3 mb s3://<bucket> --region us-west-2`; `S3_PREFIX` plus the
per-collection `ICECHUNK_PREFIX` keep the stacks' output separate. Both files
ship backfill-first: forward processing (consumer, poller, re-sort job) stays
undeployed while the backfill runs.

Per collection, hcho shown:

1. Deploy: `uv run --env-file .env_hcho cdk deploy`
2. Build and upload the inventory: `uv run exploration/build_backfill_inventory.py --collection hcho --s3-uri s3://<bucket>/tempo/inventory/hcho.json`
3. Start the backfill: `./scripts/start_backfill.sh -e .env_hcho hcho-backfill-<date> s3://<bucket>/tempo/inventory/hcho.json`. A failed run can be restarted under a new execution name; Init resets the leftover branch.
4. When it has promoted, set `FORWARD_QUEUE_ENABLED=true` and `POLL_START_ISO` to the inventory's build time in `.env_hcho`, then redeploy, so the poller's first poll picks up granules published while the backfill ran; the re-sort job folds in anything that arrived out of order.
5. Run `uv run --env-file .env_hcho scripts/verify_store.py` after the promote (and periodically) to spot-check the store against its sources.

Then repeat with `.env_no2` for the second stack:

```bash
uv run --env-file .env_no2 cdk deploy
uv run exploration/build_backfill_inventory.py --collection no2 \
  --s3-uri s3://<bucket>/tempo/inventory/no2.json
./scripts/start_backfill.sh -e .env_no2 no2-backfill-<date> \
  s3://<bucket>/tempo/inventory/no2.json
```

Settings live in [`cdk/settings.py`](./cdk/settings.py) and a `.env` file ([sample](./.env.sample)). The ones that matter most:

| Setting | Default | Meaning |
|---|---|---|
| `TEMPO_COLLECTION` | — | `hcho` or `no2`; one deployment per collection |
| `ICECHUNK_BUCKET` | — | existing bucket for the store; must be in the stack's region (checked at deploy) |
| `ICECHUNK_BUCKET_NAME` | — | bucket to create when `ICECHUNK_BUCKET` is unset |
| `S3_PREFIX` | — | common key prefix for all pipeline output (run artifacts land at `<S3_PREFIX>/backfill/`) |
| `ICECHUNK_PREFIX` | — | the repository's key prefix, relative to `S3_PREFIX` |
| `DATA_BUCKET_NAME` | — | source bucket workers read granules from |
| `BACKFILL_ENABLED` | `false` | deploy the backfill Step Functions pipeline |
| `BACKFILL_PARTITION_SIZE` | 500 | files per partition (one merged commit each) |
| `BACKFILL_MAX_ITEMS_PER_BATCH` | 10 | files per worker Lambda invocation |
| `BACKFILL_MAX_CONCURRENCY` | 50 | parallel workers per partition |
| `FORWARD_QUEUE_ENABLED` | inverse of backfill | enable the SQS consumer |
| `SQS_BATCH_SIZE` | 10 | files per consumer invocation (one commit each) |
| `POLL_SCHEDULE_MINUTES` | 30 | CMR poller cadence |
| `POLL_START_ISO` | — | first-poll start time (else a fixed lookback from now); set to the backfill inventory's build time when enabling forward processing |
| `RESORT_SCHEDULE_HOURS` | 24 | re-sort job cadence |
| `RESORT_MAX_FOLD` | 500 | max pending granules parsed per re-sort run |
| `EARTHDATA_SECRET_ARN` | — | Secrets Manager secret with EDL credentials for source reads |
| `GARBAGE_COLLECTION_FREQUENCY` | — | days between Icechunk GC runs (needs `VPC_ID`) |
| `GC_EXPIRY_DAYS` | 30 | snapshot expiry for GC runs — also the store's rollback window |
| `ALARM_EMAIL` | — | notification email for the DLQ-depth and scheduled-job-failure alarms |

The Lambda images install against [`lambda/constraints.txt`](./lambda/constraints.txt), an export of the repo's `uv.lock`, so deploys run the dependency versions the test suite ran; regenerate it with the command in its header whenever the lock changes. Concurrent backfill runs are not supported.

![Backfill](./docs/backfill-fork-merge-dark.png#gh-dark-mode-only)
![Backfill](./docs/backfill-fork-merge.png#gh-light-mode-only)

![Architecture](./docs/architecture-dark.png#gh-dark-mode-only)
![Architecture](./docs/architecture.png#gh-light-mode-only)

## Development

```bash
./scripts/setup.sh          # set up the environment
uv run pytest               # tests
uv run ruff check . && uv run ruff format --check .
uv run mypy
uv run --env-file .env cdk synth   # review infrastructure before deploying
uv run --env-file .env cdk deploy
```

The `Processor` class in [`virtualizarr_processor/processor.py`](./lambda/virtualizarr-processor/virtualizarr_processor/processor.py) is the sole (non-polymorphic) implementation; the template's synthetic reference implementation lives on as `tests/stub_processor.py` and still exercises the generic fork/merge mechanics.

## Exploration

`exploration/` holds standalone [PEP 723](https://peps.python.org/pep-0723/) scripts used to characterize the source data and to build test stores. Run them with `uv run exploration/<script>.py`; each declares its own dependencies. All take `--collection {hcho,no2}` (default `hcho`) plus `--concept-id` for any other collection, and need Earthdata Login credentials in `~/.netrc`.

- [`tempo_dataset_info.py`](./exploration/tempo_dataset_info.py) — CMR/UMM-C collection report: extents, granule count, distribution info, most recent granule.
- [`inspect_granule_metadata.py`](./exploration/inspect_granule_metadata.py) — HDF5 structure dump of granules: chunk layouts, codecs, fill values, attributes, and a cross-granule comparison of what varies.
- [`combine_twenty_spread_virtual.py`](./exploration/combine_twenty_spread_virtual.py) — virtualizes N granules spread across the collection's temporal extent, combines them in an in-memory Icechunk store, and reads data back to prove the path end to end.
- [`build_titiler_test_store.py`](./exploration/build_titiler_test_store.py) — small local store (12 recent granules) for titiler-multidim smoke tests.
- [`build_s3_test_store.py`](./exploration/build_s3_test_store.py) — realistic S3-hosted store (100 recent granules, credential-less virtual chunk container); run on in-region compute.
- [`build_backfill_inventory.py`](./exploration/build_backfill_inventory.py) — the production inventory builder described above.

### titiler-multidim smoke test takeaways (2026-08-06)

A 12-granule store per collection was served through titiler-multidim's `feat/http-virtual-chunk-auth` branch; all checked endpoints (`/variables`, `/info`, `/tiles`, `/point`) passed for both collections. What carries forward:

- Flat-at-root layout is required: titiler-xarray does not walk nested groups or resolve group-inherited coordinates. The production stores use it.
- Clients must always select a time step (`sel=time=...&sel_method=nearest`); a multi-time variable reaching the renderer fails with "Source data must be 1 band".
- Tile latency is dominated by per-request virtual-chunk fetches and scales with the area a tile covers (z2 tiles 3–7 s, z4/z6 tiles 1.5–2.7 s over HTTPS). A dataset/session cache is advisable in production, and Lambda's 1024 file-descriptor limit is a real constraint under tile bursts.
- Per-scan coverage is inherently partial (single east-west scans, daylight-only retrieval, occasional short rapid-scan slices); portal time sliders and "latest available" defaults need to account for it.
- Map clients should set `noWrap`/`maxBounds`; tiles crossing ±180 hit an antimeridian error upstream in rio-tiler.
- CMR publication behavior measured here shaped the forward-processing design: ~7% of adjacent scans publish out of order, republication is rare (0.4%) and short-window, median production lag is ~3 h, and the V04 historical archive is still being back-filled at ~1,000 granules/week.
