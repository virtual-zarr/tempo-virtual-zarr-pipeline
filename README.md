# tempo-virtual-zarr-pipeline

This pipeline builds virtual Zarr / Icechunk stores for TEMPO Level 3 gridded
products. It backs data delivery for the AIR4US portal
([NASA-IMPACT/veda-odd#438](https://github.com/NASA-IMPACT/veda-odd/issues/438))
and targets two collections hosted at NASA ASDC:

| Collection | Concept ID | DOI |
|---|---|---|
| `TEMPO_HCHO_L3` V04 — gridded formaldehyde total column | `C3685897141-LARC_CLOUD` | [10.5067/IS-40e/TEMPO/HCHO_L3.004](https://doi.org/10.5067/IS-40e/TEMPO/HCHO_L3.004) |
| `TEMPO_NO2_L3` V04 — gridded NO2 tropospheric and stratospheric columns | `C3685896708-LARC_CLOUD` | [10.5067/IS-40E/TEMPO/NO2_L3.004](https://doi.org/10.5067/IS-40E/TEMPO/NO2_L3.004) |

The repo was instantiated from the
[virtualizarr-data-pipelines](https://github.com/developmentseed/virtualizarr-data-pipelines)
template, which provides the AWS CDK infrastructure. Each collection gets its
own Icechunk repository, deployed as a separate instance of the same stack.
Improvements that aren't TEMPO-specific belong in the template, not here.

[docs/architecture.md](./docs/architecture.md) has diagrams of the design
(virtual stores, backfill, forward routing, re-sort) and a glossary.

## The virtual stores

Each store presents one collection as a single dataset: all variables from
every group, flattened into the root group (titiler-multidim needs this
layout), concatenated along `time`.

Layout and encoding:

- Every 3-D variable has per-granule dims `(time, latitude, longitude)` =
  (1, 2950, 7750) and keeps the source files' shuffle + deflate(1) codecs.
- Chunks are (1, 738, 1938) for float64 and (1, 984, 2584) for everything
  else.
- The 1-D coordinates are contiguous in the source netCDF-4, so they're
  loaded and stored as native chunks — `[native]` below.
- Everything else is virtual references into the source files.

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

**`TEMPO_NO2_L3` V04** — 13,618 scans as of 2026-07-30. Same coordinates and
layout, with the NO2 variable set: `vertical_column_troposphere`,
`vertical_column_stratosphere`, `vertical_column_total` and their
uncertainties, twelve `qa_statistics` min/max/count variables,
`amf_total`/`amf_troposphere`/`amf_stratosphere`, `tropopause_pressure`, and
the same geolocation and ancillary variables as HCHO. 36 data variables in
all.

Things worth knowing before you build on these:

- The two time axes are independent. A handful of scans exist in only one
  collection, and both grow separately — that's why each collection gets its
  own repository. Joint analysis aligns at read time.
- `latitude`/`longitude` are bit-identical between the two products and fixed
  across scans.
- `weight` varies per scan but the source files store it without a time
  dimension. The pipeline promotes it to `(time, latitude, longitude)` at
  ingest; without that, concatenation would silently keep only the first
  scan's values.
- Production stores reference `s3://asdc-prod-protected/...` in us-west-2.
  Readers authorize the virtual chunk container with temporary credentials
  from <https://data.asdc.earthdata.nasa.gov/s3credentials>. EDL-authed HTTPS
  also works but CloudFront rate-limits it.

## How the pipeline works

### Collection configuration

`virtualizarr_processor/collections/{hcho,no2}.toml` declares each
collection:

- which groups to flatten, and which variables to promote or drop
- the volatile (per-granule) attributes
- the time-axis chunk size
- the names of two generated artifacts: the store template (a pydantic-zarr
  `GroupSpec` as JSON) and the reference `latitude`/`longitude` arrays

A deployment selects its collection with `TEMPO_COLLECTION`.

Regenerate the artifacts from reference granules with
`uv run scripts/generate_template.py`. Generation fails if the granules
disagree on anything not declared volatile.

### Backfill inventory

`uv run scripts/build_backfill_inventory.py` produces the input for a
backfill: a validated JSON document with one entry per granule — its `.nc`
link, its granule UR, and its exact in-file `/time[0]`.

The in-file time is the important part. It differs from both the CMR and
filename timestamps (`...T174200Z` has `/time` = 17:42:18.02), and the
store's time axis is built from these exact values. That's why the builder
reads a few KB of every granule's header.

The document is rejected if it's empty, unsorted, or contains duplicate times
or granule URs. The pipeline re-checks all of that when it reads the file.

### Backfill

The Step Functions run:

1. partitions the inventory,
2. creates the full-shape store on a `backfill` branch — metadata plus the
   native coordinates, nothing else,
3. fans out workers. Each worker parses its granule, validates it, finds its
   slot by matching the granule's time against the axis exactly, and writes
   its references into a disjoint region of an Icechunk fork,
4. merges each partition's forks into one commit (the reducer),
5. promotes `backfill` to `main`.

Any worker failure fails the run before anything reaches `main`.

The manifest (two vlen-string arrays on the time axis recording which granule
owns which slot) and an empty pending ledger are committed on the `backfill`
branch alongside the data. That leaves the promote step with only
re-validation to do:

- the store against the template,
- the axis and manifest against the inventory,
- the coordinates against the reference arrays,
- every data array's stored chunk-reference count against its chunk grid.

The last check exists because an unwritten slot reads as fill values and
passes every metadata check.

The promote is careful about concurrency, in two ways. First, the branch tip
is looked up once and pinned: the gate validates that snapshot and the move
promotes that same snapshot, so a concurrent run resetting the branch
mid-promote can't swap in an unfilled store.

Second, the move is a compare-and-swap against the tip `main` had when the
branch was created. A commit that landed on `main` mid-run fails the promote
instead of being discarded, and nothing is written after the CAS.

### Validation

A granule is written only if it matches the template's shared attributes,
carries the bit-identical reference grid, and its `/time[0]` equals its own
`time_coverage_start_since_epoch` attribute.

Every virtual reference is also stamped with the source object's observed
modification time. If a source file is later overwritten, reads of the stale
references fail instead of returning bytes from a changed file.

### Forward processing

A scheduled Lambda polls CMR for granules whose revision date advanced past a
persisted watermark and enqueues them. (ASDC publishes no SNS topic for the
bucket; see the note below.) The SQS consumer routes each granule:

| Situation | Action |
|---|---|
| time is after the axis end | append |
| time occupies a slot, same granule UR | overwrite the slot in place (republication or redelivery) |
| time occupies a slot, different granule UR | reject to the DLQ |
| time is out of order | record in the pending ledger, consume the message |

Out-of-order arrivals are routine, not an edge case: in a recent 14-day
window ~43% of adjacent publications were out of scan-time order — mostly
historical-archive granules drip-fed between new scans, plus 2–3% genuinely
swapped adjacent scans. Re-measure with
`uv run scripts/measure_publish_order.py`.

A scheduled re-sort job folds the pending ledger back into the store. It pins
`main`'s tip first, reads the axis, manifest, and ledger from a readonly
session at that snapshot, and merges on a `resort` branch built from it.

Deep historical insertions are cheap. Already-ingested slots at or after the
earliest insertion are relocated with icechunk's `reindex_array`, a
metadata-only move that never re-reads a source file; only the inserted
granules are parsed.

The re-sort promote follows the same rules as the backfill promote. Folded
ledger entries are drained inside the same commit that performs the fold, and
`main` moves by compare-and-swap, so a consumer append that landed mid-run
fails the promote instead of being silently erased. One run folds at most
`RESORT_MAX_FOLD` pending granules, earliest first, and promotes that as
durable partial progress; the rest drain on later runs.

The consumer runs at reserved concurrency 1 because concurrent appends
conflict.

The manifest and pending ledger live inside the Icechunk store itself, as
root-group attributes and arrays committed atomically with the data they
describe. The only state outside the store is the CMR poll watermark, at
`s3://<icechunk bucket>/<prefix>state/`. The poller's first poll starts from
`POLL_START_ISO` when set (typically the backfill inventory's build time),
else a fixed lookback.

> **Feeding the queue:** ASDC does not publish an SNS notification topic for
> `asdc-prod-protected`, so this pipeline polls CMR instead. Duplicate
> enqueues are harmless (the consumer routing is idempotent), and a 30-minute
> poll cadence is negligible next to the product's ~3 h median production
> lag. A provider-side SNS topic would still be worth requesting from ASDC:
> the queue could subscribe directly, with the poller kept as a backstop for
> missed notifications.

### Verification

`uv run scripts/verify_store.py` spot-checks the store against its sources,
independently of the pipeline's own bookkeeping. It samples random time steps
and, for each, asks CMR for the granule nearest that time. The file CMR
points at must match the store's axis time exactly.

Random windows of every variable are then compared two ways: raw (store bytes
against h5py reads) and CF-decoded (the read path users take). Because the
URL comes from CMR, a store still referencing a superseded revision is caught
even when the old object is intact.

Two flags: `--completeness` diffs CMR's granule listing against the manifest
and pending ledger, and `--offline` falls back to manifest-provided URLs.

The script authorizes the virtual chunk container itself, with the same
Earthdata material the workers use (or ambient AWS access to the source
bucket). The pipeline's own writers never hold chunk-read access. Any
mismatch or read failure exits non-zero.

### Recovery

There's less to recover than you might expect:

- The manifest and pending ledger commit atomically with the data they
  describe (same session), so they can't drift from it or race a concurrent
  writer. No repair script exists or is needed.
- A promote rejected by the compare-and-swap needs no repair either: nothing
  was consumed, and the next scheduled run retries against the new `main`
  tip.
- The one case that needs an operator: a same-time/different-UR collision
  between the manifest and the pending ledger. This aborts the resort run by
  design — a loud, repeatable failure rather than a silent overwrite. Fix it
  by hand (`rebuild_manifest.py` no longer exists) with a small Icechunk
  commit that reads the `pending_ledger` root attribute, drops the offending
  entry, and writes it back.

### Source credentials

Workers can authenticate with Earthdata Login material from any of:
`EARTHDATA_TOKEN`, `EARTHDATA_USERNAME`/`EARTHDATA_PASSWORD`, or a Secrets
Manager secret at `EARTHDATA_SECRET_ARN` holding JSON with `token` or
`username`+`password`. They exchange it for temporary S3 credentials at the
bucket's `s3credentials` endpoint (`EARTHDATA_S3_CREDENTIALS_ENDPOINT`
overrides).

Without any of those, reads use the Lambda role's ambient IAM access, which
requires a bucket-policy grant on the source bucket.

## Deploying and running

### Env files

Each collection deploys as its own stack from a committed env file:
[`.env_hcho`](./.env_hcho) and [`.env_no2`](./.env_no2). Both are currently
filled in for a test run in a sandbox sub-account (us-west-2).

The committed files hold only dataset config. Account- and operator-specific
values (`ACCOUNT_ID`, `AWS_PROFILE`, `OWNER`, `EARTHDATA_SECRET_ARN`, ...) go
in a gitignored `.env.local` shared by both collections — copy
[`.env.local.sample`](./.env.local.sample) and fill it in. Pass both files to
every command, `.env.local` last so it can also override dataset settings for
a local run:

```bash
uv run --env-file .env_hcho --env-file .env.local cdk deploy
```

A pre-commit hook rejects commits that put a value for one of the local-only
keys back into a tracked env file.

Both collections share one bucket (`ICECHUNK_BUCKET`, in us-west-2, created
once with `aws s3 mb s3://<bucket> --region us-west-2 --profile <profile>`).
The per-collection `S3_PREFIX` (`tempo/hcho`, `tempo/no2`) keeps the stacks'
output separate, and every IAM grant in a stack is scoped to its own prefix,
so neither stack's roles can touch the other's keys. To deploy into a
different account, change `.env.local` and `ICECHUNK_BUCKET`.

Both env files ship backfill-first: forward processing (consumer, poller,
re-sort job) stays undeployed while the backfill runs.

### One-time sandbox setup

```bash
./scripts/setup.sh   # uv deps + Node and the cdk CLI, installed into the uv venv
cp .env.local.sample .env.local                        # then fill it in
aws sso login --profile <profile>                      # or however the profile authenticates
uv run --env-file .env_hcho --env-file .env.local cdk bootstrap aws://<ACCOUNT_ID>/us-west-2   # fresh account only
aws s3 mb s3://tempo-virtual-store-sandbox --region us-west-2 --profile <profile>
aws secretsmanager create-secret --name tempo-earthdata \
  --secret-string '{"token":"<EDL token>"}' \
  --region us-west-2 --profile <profile>
```

Paste the ARN the last command returns into `EARTHDATA_SECRET_ARN` in
`.env.local` (shared by both collections). The secret is required in the
sandbox: the account has no bucket-policy grant on `asdc-prod-protected`, so
without it every worker granule read fails with AccessDenied. The deploy
itself would still succeed, which makes this an annoying failure to debug
after the fact.

Also check the account's Lambda concurrent-executions quota. Fresh
sub-accounts can start as low as 10, and the backfill fans out to
`BACKFILL_MAX_CONCURRENCY=50`; request an increase or lower that setting.

`AWS_PROFILE` is set inside `.env.local`, so every `uv run --env-file ...`
command and `start_backfill.sh -e ...` targets the sandbox without exporting
anything (the scripts read keys missing from the `-e` file out of
`.env.local` automatically).

### Trial run vs. full backfill

The steps below are written for the first trial: a backfill of only the 50
most recent granules (`--max-count 50` on the inventory command) into a
scratch store. The time axis is sized from the inventory, so `.env_hcho`
points at `ICECHUNK_PREFIX=v04-trial` rather than the real `v04`.

To graduate to the full backfill: drop `--max-count`, rebuild and re-upload
the inventory, set `ICECHUNK_PREFIX=v04`, and redeploy **first** — the prefix
is baked into the Lambda environment.

### Running a backfill (hcho shown)

1. Deploy:

   ```bash
   uv run --env-file .env_hcho --env-file .env.local cdk deploy
   ```

2. Build and upload the inventory:

   ```bash
   ./scripts/run_codebuild.sh -e .env_hcho -m 50
   ```

   `-m 50` is the trial cap; drop it for the full run.

   This runs the committed `build_backfill_inventory.py` inside the stack's
   CodeBuild project, because the DAAC's temporary S3 credentials only work
   from us-west-2 — a laptop run with the default `--access direct` fails on
   every granule read. The Earthdata token comes from the stack's
   `EARTHDATA_SECRET_ARN`, and the inventory lands at
   `s3://<bucket>/<INVENTORY_PREFIX>/hcho.json`, the only prefix the
   partition Lambda may read.

   On a us-west-2 machine, running
   `uv run --env-file .env_hcho --env-file .env.local scripts/build_backfill_inventory.py ...`
   directly still works, with Earthdata credentials from `~/.netrc` or
   `$EARTHDATA_TOKEN`.

3. Start the backfill:

   ```bash
   ./scripts/start_backfill.sh -e .env_hcho s3://tempo-virtual-store-sandbox/tempo/hcho/inventory/hcho.json
   ```

   The execution name defaults to `<stack>-backfill-<UTC timestamp>`; pass
   one explicitly as an extra argument before the URI if you want a memorable
   name. A failed run can simply be rerun — the fresh timestamp satisfies
   Step Functions' 90-day execution-name uniqueness, and Init resets the
   leftover branch.

4. When the backfill has promoted, set `FORWARD_QUEUE_ENABLED=true` and
   `POLL_START_ISO` to the inventory's build time in `.env_hcho`, then
   redeploy. The poller's first poll then picks up granules published while
   the backfill ran, and the re-sort job folds in anything that arrived out
   of order.

5. Run
   `uv run --env-file .env_hcho --env-file .env.local scripts/verify_store.py`
   after the promote, and periodically after that — or run it in-region with
   `scripts/run_codebuild.sh -e .env_hcho -V` (add `-a "--completeness"` for
   extra flags), which starts the stack's CodeBuild project with a verify
   buildspec override.

Then repeat with `.env_no2` for the second stack:

```bash
uv run --env-file .env_no2 --env-file .env.local cdk deploy
./scripts/run_codebuild.sh -e .env_no2
./scripts/start_backfill.sh -e .env_no2 \
  s3://tempo-virtual-store-sandbox/tempo/no2/inventory/no2.json
```

To trial the no2 stack the same way, add `-m 50` and set
`ICECHUNK_PREFIX=v04-trial` in `.env_no2` first — `.env_no2` ships pointed at
the real `v04`.

### Teardown

```bash
uv run --env-file .env_hcho --env-file .env.local cdk destroy
uv run --env-file .env_no2  --env-file .env.local cdk destroy
```

The shared bucket is not stack-owned; empty and delete it separately. For the
later client deployment, also set the `CLIENT` tag in `.env.local` and
`STAGE=prod` in the env files.

### Settings

Settings live in [`cdk/settings.py`](./cdk/settings.py) and a `.env` file
([sample](./.env.sample)). The ones that matter most:

| Setting | Default | Meaning |
|---|---|---|
| `TEMPO_COLLECTION` | — | `hcho` or `no2`; one deployment per collection |
| `ICECHUNK_BUCKET` | — | existing bucket for the store; must be in the stack's region (checked at deploy) |
| `ICECHUNK_BUCKET_NAME` | — | bucket to create when `ICECHUNK_BUCKET` is unset |
| `S3_PREFIX` | — | common key prefix for all pipeline output (run artifacts land at `<S3_PREFIX>/backfill/`); per collection, since every IAM grant is scoped under it |
| `ICECHUNK_PREFIX` | — | the repository's key prefix, relative to `S3_PREFIX` |
| `INVENTORY_PREFIX` | `<S3_PREFIX>/inventory` | key prefix the backfill partition Lambda may read inventories from |
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
| `OWNER` | — | `Owner` cost-allocation tag on every resource; unset applies no tag |
| `CLIENT` | — | `Client` cost-allocation tag on every resource; unset applies no tag |

The Lambda images install against
[`lambda/constraints.txt`](./lambda/constraints.txt), an export of the repo's
`uv.lock`, so deploys run the dependency versions the test suite ran.
Regenerate it with the command in its header whenever the lock changes.

Concurrent backfill runs are not supported.

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
uv run --env-file .env --env-file .env.local cdk synth   # review infrastructure before deploying
uv run --env-file .env --env-file .env.local cdk deploy
```

The `Processor` class in
[`virtualizarr_processor/processor.py`](./lambda/virtualizarr-processor/virtualizarr_processor/processor.py)
is the sole (non-polymorphic) implementation. The template's synthetic
reference implementation lives on as `tests/stub_processor.py` and still
exercises the generic fork/merge mechanics.

## Exploration

`exploration/` holds standalone [PEP 723](https://peps.python.org/pep-0723/)
scripts used to characterize the source data and to build test stores. Run
them with `uv run exploration/<script>.py`; each declares its own
dependencies. All take `--collection {hcho,no2}` (default `hcho`) plus
`--concept-id` for any other collection, and need Earthdata Login credentials
in `~/.netrc`.

- [`tempo_dataset_info.py`](./exploration/tempo_dataset_info.py) — CMR/UMM-C
  collection report: extents, granule count, distribution info, most recent
  granule.
- [`inspect_granule_metadata.py`](./exploration/inspect_granule_metadata.py) —
  HDF5 structure dump of granules: chunk layouts, codecs, fill values,
  attributes, and a cross-granule comparison of what varies.
- [`combine_twenty_spread_virtual.py`](./exploration/combine_twenty_spread_virtual.py) —
  virtualizes N granules spread across the collection's temporal extent,
  combines them in an in-memory Icechunk store, and reads data back to prove
  the path end to end.
- [`build_titiler_test_store.py`](./exploration/build_titiler_test_store.py) —
  small local store (12 recent granules) for titiler-multidim smoke tests.
- [`build_s3_test_store.py`](./exploration/build_s3_test_store.py) —
  realistic S3-hosted store (100 recent granules, credential-less virtual
  chunk container); run on in-region compute.

The production inventory builder
([`scripts/build_backfill_inventory.py`](./scripts/build_backfill_inventory.py),
described above) lives in `scripts/` with the other production tooling; it
follows the same PEP 723 + `--collection` conventions.

### titiler-multidim smoke test takeaways (2026-08-06)

A 12-granule store per collection was served through titiler-multidim's
`feat/http-virtual-chunk-auth` branch; all checked endpoints (`/variables`,
`/info`, `/tiles`, `/point`) passed for both collections. What carries
forward:

- Flat-at-root layout is required: titiler-xarray does not walk nested groups
  or resolve group-inherited coordinates. The production stores use it.
- Clients must always select a time step (`sel=time=...&sel_method=nearest`).
  A multi-time variable reaching the renderer fails with "Source data must be
  1 band".
- Tile latency is dominated by per-request virtual-chunk fetches and scales
  with the area a tile covers: z2 tiles took 3–7 s, z4/z6 tiles 1.5–2.7 s
  over HTTPS. A dataset/session cache is advisable in production, and
  Lambda's 1024 file-descriptor limit is a real constraint under tile bursts.
- Per-scan coverage is inherently partial (single east-west scans,
  daylight-only retrieval, occasional short rapid-scan slices). Portal time
  sliders and "latest available" defaults need to account for it.
- Map clients should set `noWrap`/`maxBounds`; tiles crossing ±180 hit an
  antimeridian error upstream in rio-tiler.
- CMR publication behavior shaped the forward-processing design: publication
  order routinely diverges from scan order (the ~43% figure quoted under
  [Forward processing](#forward-processing) came from an August 2026 14-day
  window, with the historical archive back-filling at ~1,000 granules/week),
  republication is rare (~0.3%) and short-window, and median production lag
  is ~3 h.
