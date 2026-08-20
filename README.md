## tempo-virtual-zarr-pipeline

Virtual Zarr / Icechunk ingestion pipeline for TEMPO Level 3 gridded products, supporting optimized data delivery for the AIR4US portal ([NASA-IMPACT/veda-odd#438](https://github.com/NASA-IMPACT/veda-odd/issues/438)). It targets two collections hosted at NASA ASDC:

| Collection | Concept ID | DOI |
|---|---|---|
| `TEMPO_HCHO_L3` V04 — gridded formaldehyde total column | `C3685897141-LARC_CLOUD` | [10.5067/IS-40e/TEMPO/HCHO_L3.004](https://doi.org/10.5067/IS-40e/TEMPO/HCHO_L3.004) |
| `TEMPO_NO2_L3` V04 — gridded NO2 tropospheric and stratospheric columns | `C3685896708-LARC_CLOUD` | [10.5067/IS-40E/TEMPO/NO2_L3.004](https://doi.org/10.5067/IS-40E/TEMPO/NO2_L3.004) |

This repository was instantiated from the [virtualizarr-data-pipelines](https://github.com/developmentseed/virtualizarr-data-pipelines) template, which provides the AWS CDK infrastructure documented below. The intended layout is one Icechunk repository per collection, deployed as separate instances of the same stack; improvements that generalize beyond TEMPO belong in the template.

### Exploration :mag:

`exploration/` holds standalone [PEP 723](https://peps.python.org/pep-0723/) scripts used to characterize the source data before building the processor. Run them with `uv run exploration/<script>.py` — each declares its own dependencies, so no project install is needed. All of them take `--collection {hcho,no2}` (default `hcho`; the mapping lives in `exploration/tempo_collections.py`) plus `--concept-id` to target any other collection, and need Earthdata Login credentials in `~/.netrc`.

- [`tempo_dataset_info.py`](./exploration/tempo_dataset_info.py) — full CMR/UMM-C collection report: description, citation, spatial/temporal extents, granule count, direct-S3 distribution information, and the most recent granule.
- [`inspect_granule_metadata.py`](./exploration/inspect_granule_metadata.py) — native HDF5 dump of one granule via h5py: groups, datasets, chunk layouts, filter pipelines (codecs), fill values, storage vs. logical sizes, and all attributes.
- [`combine_first_three_virtual.py`](./exploration/combine_first_three_virtual.py) — virtualizes the collection's first three granules with VirtualiZarr's `HDFParser`, concatenates them along `time`, writes the virtual references into an in-memory Icechunk repository, and reads real data back through its virtual chunk container.
- [`combine_twenty_spread_virtual.py`](./exploration/combine_twenty_spread_virtual.py) — the same end-to-end flow for N granules (default 20) spread evenly across the collection's temporal extent, with throttle-aware retries.
- [`build_titiler_test_store.py`](./exploration/build_titiler_test_store.py) — builds a small persistent local Icechunk store (12 most recent granules, flattened to the root group, credential-less HTTP virtual chunk container) for each collection, feeding the titiler-multidim smoke test below.
- [`build_s3_test_store.py`](./exploration/build_s3_test_store.py) — builds a more realistic S3-hosted Icechunk store at `s3://nasa-eodc-scratch/icechunk/<concept-id>`: 100 most recent granules, `s3://asdc-prod-protected` virtual chunk references, and a credential-less S3 virtual chunk container that readers authorize with temporary ASDC credentials at open time. Must run on in-region compute (e.g. a us-west-2 JupyterHub) with write access to the store bucket.
- [`build_backfill_inventory.py`](./exploration/build_backfill_inventory.py) — builds the typed backfill inventory (`tempo-backfill-inventory/1`): queries CMR for every granule of the collection (optionally windowed with `--start`/`--end`), dedupes republications keeping the newest revision, reads each granule's **exact in-file `/time[0]`** from a few KB of its header (needs Earthdata credentials; run in-region with `--access direct`), and writes the validated JSON document the pipeline's partition/init steps consume. `--s3-uri` also uploads the file so its URI can be passed straight to `scripts/start_backfill.sh`.

Findings so far: both collections share the same 2950×7750 grid, the same group layout (`product`, `geolocation`, `support_data`, `qa_statistics`), and the same shuffle + deflate(level 1) filter pipeline, with chunk shape (1, 738, 1938) for float64 variables and (1, 984, 2584) for float32/int16 variables. ASDC's CloudFront distribution rate-limits bursts of HTTPS range requests (403 "Request blocked"), so bulk virtualization should use in-region S3 access, or low concurrency with retries over HTTPS.

#### Virtual view of each collection

The trees below (derived from `inspect_granule_metadata.py`) show each collection as the virtual store a reader would see after concatenating granules along `time` — one tree per collection because the two time axes are independent (see considerations below). Shared facts, stated once: every 3-D variable has per-granule dims `(time, latitude, longitude)` = (1, 2950, 7750) with the shuffle + deflate(1) pipeline, chunked (1, 738, 1938) for float64 and (1, 984, 2584) for float32/int16/int32; the 1-D coordinates are contiguous and uncompressed in the source netCDF-4, so VirtualiZarr loads them and they are written as native chunks (marked `[native]`), while everything else stays virtual.

**`TEMPO_HCHO_L3` V04** — 13,611 scans as of 2026-07-30:

```
/                                       dims: time (append dim), latitude=2950, longitude=7750
├── time         (time)                 float64, seconds since 1980-01-06T00:00:00Z  [native]
├── latitude     (latitude)             float32  [native]
├── longitude    (longitude)            float32  [native]
├── weight       (time, latitude, longitude)  float32  # promoted; stored per scan without a time dim
├── product/                            # all group variables: (time, latitude, longitude)
│   ├── vertical_column                          float64
│   ├── vertical_column_uncertainty              float64
│   └── main_data_quality_flag                   int16
├── geolocation/
│   ├── solar_zenith_angle                       float32
│   ├── viewing_zenith_angle                     float32
│   └── relative_azimuth_angle                   float32
├── qa_statistics/
│   ├── num_vertical_column_samples              int32
│   ├── min_vertical_column_sample               float64
│   └── max_vertical_column_sample               float64
└── support_data/
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

**`TEMPO_NO2_L3` V04** — 13,618 scans as of 2026-07-30:

```
/                                       dims: time (append dim), latitude=2950, longitude=7750
├── time         (time)                 float64, seconds since 1980-01-06T00:00:00Z  [native]
├── latitude     (latitude)             float32  [native]
├── longitude    (longitude)            float32  [native]
├── weight       (time, latitude, longitude)  float32  # promoted; stored per scan without a time dim
├── product/                            # all group variables: (time, latitude, longitude)
│   ├── vertical_column_troposphere              float64
│   ├── vertical_column_troposphere_uncertainty  float64
│   ├── vertical_column_stratosphere             float64
│   └── main_data_quality_flag                   int16
├── geolocation/
│   ├── solar_zenith_angle                       float32
│   ├── viewing_zenith_angle                     float32
│   └── relative_azimuth_angle                   float32
├── qa_statistics/
│   ├── num_vertical_column_troposphere_samples              int32
│   ├── min_vertical_column_troposphere_sample               float64
│   ├── max_vertical_column_troposphere_sample               float64
│   ├── num_vertical_column_troposphere_uncertainty_samples  int32
│   ├── min_vertical_column_troposphere_uncertainty_sample   float64
│   ├── max_vertical_column_troposphere_uncertainty_sample   float64
│   ├── num_vertical_column_stratosphere_samples             int32
│   ├── min_vertical_column_stratosphere_sample              float64
│   ├── max_vertical_column_stratosphere_sample              float64
│   ├── num_vertical_column_total_samples                    int32
│   ├── min_vertical_column_total_sample                     float64
│   └── max_vertical_column_total_sample                     float64
└── support_data/
    ├── vertical_column_total                    float64
    ├── vertical_column_total_uncertainty        float64
    ├── fitted_slant_column                      float64
    ├── fitted_slant_column_uncertainty          float64
    ├── albedo                                   float32
    ├── amf_total                                float32
    ├── amf_troposphere                          float32
    ├── amf_stratosphere                         float32
    ├── tropopause_pressure                      float32
    ├── eff_cloud_fraction                       float32
    ├── amf_cloud_fraction                       float32
    ├── amf_cloud_pressure                       float32
    ├── surface_pressure                         float32
    ├── terrain_height                           int16
    ├── snow_ice_fraction                        float32
    └── pbl_height                               int16
```

#### Virtual view considerations

- **One Icechunk repository per collection.** The time axes are independent: 13,610 scans are shared, 1 is HCHO-only, 8 are NO2-only (as of 2026-07-30), and both sets grow independently as new scans are published. Neither store should assume the other's axis; joint NO2+HCHO analysis aligns at read time (intersection, or union with fill).
- **Spatial alignment is free.** `latitude`/`longitude` are bit-identical between the two products (and fixed across scans), written once as native chunks per store.
- **Concatenation along `time` is clean.** Chunk grids, dtypes, and codecs are uniform across granules and across both collections, so every 3-D variable appends without rechunking: ~220 virtual chunk references per granule, on the order of 3M references per collection at full backfill (referencing several TB of source netCDF).
- **`weight` is promoted to `(time, latitude, longitude)`.** The regridding weight varies per scan (verified empirically) but has no `time` dimension in the source files, so a concat that trusts the file's data model silently keeps only the first scan's values. The combine scripts therefore `expand_dims` each granule's virtual `weight` before concatenating — zero-copy, since `ManifestArray` implements `expand_dims` — and any future append path must do the same for `append_dim="time"` to pick it up.
- **Reference URLs should be `s3://` for production.** The exploration stores reference EDL-authed HTTPS (subject to CloudFront rate limiting and token expiry); production stores should reference `s3://asdc-prod-protected/...` in us-west-2 with reader-supplied temporary credentials from <https://data.asdc.earthdata.nasa.gov/s3credentials>.

#### titiler-multidim smoke test findings

On 2026-08-06 the end-to-end path — local Icechunk store, EDL-authenticated HTTPS virtual chunks, titiler-multidim tile rendering — was smoke-tested against `build_titiler_test_store.py` output for both collections (12 most recent granules each, subset to the primary column variable plus `main_data_quality_flag`, flattened from the `product` group to the root group with inherited coordinates), served by titiler-multidim's `feat/http-virtual-chunk-auth` branch (typed HTTP bearer virtual chunk credentials) run locally with `TITILER_MULTIDIM_ENABLE_CACHE=false`.

**Endpoint results: pass for both stores, all four checklist endpoints.** `/variables` returned the expected two-variable set for each store in under 20ms. `/info` returned North-America-scale bounds (lon −168..−13, lat 14..73), `epsg:4326`, and 12 correctly decoded ISO datetimes in under 40ms — HCHO's 12 scans span 2026-08-05T23:14 through 2026-08-06T16:24 UTC, NO2's span a narrow rapid-scan window, 2026-08-06T16:14 through 18:04 UTC. Column-variable tiles at z2/z4/z6 and two time steps each, plus `main_data_quality_flag` tiles, all returned HTTP 200 with visually plausible spatial structure (the characteristic trapezoidal TEMPO swath and colormap-varied signal, not solid fill or all-transparent) for both stores; the one exception was NO2's z6/15/24 cell (an upper-Midwest close-up chosen for HCHO), which returned 200 but rendered blank because NO2's narrow west-of-center swath does not intersect that tile — a tile-pick mismatch, not a rendering failure. `/point` returned 12-value time series for both stores. The Chicago HCHO point matched the previously known-good value (7.10e16 at 11:14 UTC) and reproduced the day/night illumination pattern already noted mid-run: valid values 23:14–00:34 UTC and 11:14–14:14 UTC, NaN elsewhere. NO2 points required trying four candidate longitudes to find ones under the narrow swath — (−115, 35) and (−110, 32) returned valid values at all 12 time steps, while (−120, 34) and (−105, 30) were NaN at all 12 (outside the scan footprint, not a data defect).

**Tile latency by zoom, first vs. repeat.** With caching disabled, repeat requests showed no consistent speedup over the first request — z4 HCHO: first 1.90s vs. repeats 2.66s/2.40s/1.89s; z4 NO2: first 1.67s vs. repeats 1.81s/1.64s/1.80s — latency is dominated by per-request virtual-chunk resolution and the HTTPS fetch to ASDC, not amortized by any warm state. Latency tracks the geographic extent a tile covers rather than zoom level per se: wide z2 tiles took 6.4–7.2s for HCHO and 2.9–3.2s for NO2 (narrower swath, fewer intersecting source chunks); a supplementary whole-domain z1/0/0 NO2 tile took 12.9s; z4 and z6 tiles, covering far less area, consistently took 1.5–2.7s regardless of store, variable, or time step.

**Rate limiting and stability.** This systematic run produced no upstream 403 rate-limiting (30 checklist requests succeeded, one expected antimeridian 500, one expected out-of-bounds 404; the server log's 31st 200 was the startup `/healthz` check, not a checklist request) — unlike the earlier live map-viewer pan/zoom test, this checklist's request volume was lower and did not reproduce a burst. `ulimit -n 10240`, set in the shell before starting uvicorn, was necessary groundwork: mid-run map-viewer testing had exhausted macOS's default 256 file-descriptor soft limit under a tile burst ("Too many open files", confirmed not a leak — FDs stayed flat across sequential and 10x-concurrent `/info` calls). That FD pressure is directly relevant to a production Lambda's 1024-FD default limit and argues for a dataset/session cache in front of repeated tile requests in production, contrary to the `ENABLE_CACHE=false` setting used here for a clean smoke test.

**Mandatory time selection.** Tiles requested without `sel=time=...&sel_method=nearest` fail with a 500 ("Source data must be 1 band") because the store's 12-band time axis reaches the renderer uncollapsed; the map viewer shows this as a generic "invalid tile". Any production client, including the AIR4US portal, must always select a time step explicitly; titiler-multidim itself could usefully default or error more clearly on multi-time variables.

**World-wrap and antimeridian behavior.** Panning the map viewer across world copies produces 404s for out-of-range tiles (shown as "invalid tile"), 500s with an antimeridian error for the seam column exactly at ±180 (reproduced here at z2/4/1; also seen at z6/64/24), and 200s for duplicate-world tiles further out; in-bounds data tiles are unaffected. A production portal client should set `noWrap`/`maxBounds`; the antimeridian 500 looks like upstream rio-tiler behavior worth raising as an issue rather than something to fix in this pipeline.

**Per-scan partial coverage.** Coverage is inherently partial per scan, not a bug: each L3 granule is a single east-west scan, retrievals require daylight, and some scans are short partial west-slices only 10 minutes apart — NO2's entire 12-scan window is one such rapid-scan sequence, spanning roughly lon −135..−100. HCHO's 2026-08-06T12:34 UTC scan has the fullest domain coverage of its 12 and was used for the full-coverage tile checks. A production portal's time slider and "latest available" default need to account for both day/night gaps and partial-scan footprints.

**CMR publication behavior, relevant to forward-processing design.** Out-of-order publication is routine — about 7% of adjacent scan pairs across both collections arrive out of ingest order, typically small adjacent-scan swaps. Forward production lag is a median of ~3h after scan start (p95 ~3.5h, max 21h). Republication within V04 is rare (0.4% of granules) and limited to short-window corrective replacements 0–2 days after the original scan, with no long-tail republication observed. Most significantly, the V04 historical archive is still being back-filled as of this test: files for 2023-08 through 2024-04 scans were produced 2026-01 through 2026-08 (713 files in August alone, for the oldest-2000-granule sample) — an ongoing months-long drip-feed, not a one-off recent endpoint — and HCHO's granule count grew by over 1,000 in one week — roughly 930 more than the ~90 new scans/week expected from ongoing operations alone. A backfill inventory therefore goes stale within days, and forward processing must expect historical granules interleaved with new ones, not just new scans.

**Implications for production store design.** The flat-at-root layout used for this smoke-test store — product-group variables promoted to the root group with inherited coordinates — is what let titiler-multidim's `/variables`, `/info`, `/tiles`, and `/point` endpoints work without modification; titiler-xarray does not walk nested groups or resolve group-inherited coordinates on its own. Given that constraint, the production store should carry the same flattening forward: either flatten at write time (as this smoke-test builder does) or confirm a titiler-side fix for nested-group coordinate inheritance before committing to a nested layout. Until then, flat-at-root is the layout known to work end-to-end.

### Backfill vs Forward Processing

Virtualizarr Data Pipelines supports two complementary paths for getting files virtualized and into an Icechunk store:

- **Backfill processing** is a one time, high-throughput bulk load of a large body of
  *existing* files. It initializes
  the Icechunk store with full shape (for example, every time step covered by the existing files) and uses a partitioned Icechunk **fork and merge**
  [cooperative distributed write](https://icechunk.io/en/stable/understanding/parallel/#cooperative-distributed-writes) approach so many thousands of files can be processed in parallel with a small number of commits. It uses AWS Step Functions to orchestrate this work and is disabled by default.
- **Forward processing** is the path for processing *new production files as they
  become available*. Files are announced to an SQS queue (typically via S3/SNS
  notifications), consumed by a Lambda, appended to the `main` branch, and committed
  per batch.

A typical project uses both: run **backfill** once to load the historical archive, then
rely on **forward processing** to keep the store current as new files land.

### Pipeline status :rocket:

The TEMPO-specific processor is implemented (design:
[`docs/superpowers/specs/2026-08-20-tempo-inventory-and-processor-design.md`](./docs/superpowers/specs/2026-08-20-tempo-inventory-and-processor-design.md)).
The moving parts:

- **Declarative collection config** — `virtualizarr_processor/collections/{hcho,no2}.toml`
  (pydantic `CollectionConfig`; instance selected via `$TEMPO_COLLECTION`) declares the
  flatten/promote/drop transforms, the volatile-attribute set, the time-axis chunk size,
  and the names of two generated artifacts: the store template (pydantic-zarr
  `GroupSpec` JSON, produced through the actual `to_icechunk` write path) and the
  bit-exact reference `latitude`/`longitude` arrays. Regenerate with
  `uv run scripts/generate_template.py` — any reference granule that diverges on a
  non-volatile attribute or the grid fails generation loudly.
- **Typed backfill inventory** (`tempo-backfill-inventory/1`), built by
  `uv run exploration/build_backfill_inventory.py`: one entry per granule with its
  `.nc` link, its granule UR (filename stem), and its **exact in-file `/time[0]`**
  (which differs from the CMR/filename timestamp — e.g. `...T174200Z` has
  `/time` = 17:42:18.02). Init builds the store's axis from these values; workers
  locate each granule's slot by exact raw-float match, so a granule not in the
  inventory is rejected, never misplaced. Validators reject empty/unsorted/
  duplicate-time/duplicate-UR sets on both the build and consume side.
- **Validation on insertion** — every granule must match the template's shared
  attributes, carry the bit-exact reference grid, and agree with its own
  `time_coverage_start_since_epoch`; failures reject the granule and (in backfill)
  fail the run before promote. The promote gate re-validates the store, the axis
  against the inventory, and the native coordinates against the artifact. All
  virtual refs carry a `last_updated_at` checksum anchored to the source object's
  observed mtime, so a source file overwritten after ingest fails reads loudly
  instead of serving bytes from a changed file.
- **Forward processing** routes each granule: append when after the axis end;
  overwrite in place when its time slot and granule UR match the **store manifest**
  (republications; makes at-least-once SQS delivery idempotent); hard-reject a
  different granule claiming an occupied slot; defer out-of-order arrivals (the
  ongoing historical back-fill, adjacent-scan swaps) to a **pending ledger**. A
  scheduled **re-sort job** folds the ledger in by rewriting the shifted suffix on
  a `resort` branch and fast-forwarding `main`. The queue is fed by the scheduled
  **CMR poller** (see the TEMPO/ASDC note above). The consumer runs at reserved
  concurrency 1; the store manifest, ledger, and poll watermark live under
  `s3://<icechunk bucket>/<prefix>state/`.
- **Post-promote QA** — `uv run scripts/verify_store.py` samples random time steps,
  maps them to their source granules via the store manifest, and compares windows
  read through the virtual store against h5py reads of the source; any mismatch or
  checksum failure exits non-zero.

The sections below are the template's documentation for building a processor and deploying the infrastructure.

### Creating a processor :package:
The first step is building your own dataset specific processor module. There is a sample
[processor.py](./lambda/virtualizarr-processor/virtualizarr_processor/processor.py) in the repo that uses an in-memory Icechunk store and a fake virtual dataset to
demonstrate how a processor works.  Replace this with your own `processor.py`
file.  Your class should follow the [VirtualizarrProcessor protocol](./lambda/virtualizarr-processor/virtualizarr_processor/typing.py).

You can specify the dependencies for your processor module in its [pyproject.toml](./lambda/virtualizarr-processor/pyproject.toml).

You should create tests for your module in the [tests](./tests) directory. There are sample fixtures for an in memory Icechunk store and some basic sample tests for the sample processor module in the template repo that you can use as a guide.

The Virtualizarr Data Pipelines CDK infrastructure will use this module to create Docker images, Lambda functions and an AWS Batch job for initializing the Icechunk store, consuming SQS messages for files and appending them to the store and running Icechunk garbage collection as well as the backfill Step Functions orchestration described below.

### Configuring the deployment :wrench:
Virtualizarr Data Pipelines uses a strongly-typed [settings module](./cdk/settings.py) that allows you to configure things like bucket names and external SNS topics used by the CDK infrastructure when you deploy it.  Many of the settings include defaults but you can also specify and override values with a `.env` file.  A [sample file](./.env.sample) is provided as an example.


### Backfill Processing :building_construction:

Backfill processes a large set of existing files in a single, highly
parallel run. Instead of appending each file to `main`, where many concurrent workers
would contend for the branch tip. It declares the store at its **full shape** up front on a dedicated `backfill` branch and then uses Icechunk's **fork and merge** model.

1. The coordinator creates an Icechunk store with the dataset's full dimension extent for the files included in the input file inventory.
2. A coordinator splits the file inventory into partitions.  Each
   partition will be processed serially and will be written to the Icechunk
   store as a single commit.  You'll want to balance your partitioning size so you're
   making a reasonably small number of commits but not losing too much work if
   one of the jobs in your partition fails (which means all the files in that
   partition will not be committed).
3. For the first partition, the coordinator forks a clean, committed base snapshot.
4. For the partition the coordinator spawns a number of Lambda workers.  Each worker copies the fork and writes its set of files to a **disjoint** region of the array via
`vds.vz.to_icechunk(fork.store, region="auto")` without committing.
Region distjointness is the operator's responsibility, trying to write to the same region will result in merge failures.
5. After it has written it's files to the fork the worker copies the pickled
   fork to S3.
6. When all the partition workers have completed, a reducer function merges all the pickled forks into **one commit for the partition** and finally `main` is fast-forwarded to the backfill tip. Because every worker writes to an independent fork and only the reducer commits, there is no tip contention and the writes-per-commit ratio is maximized.
7. Each partition is processed serially so after the first partition is
   committed a new fork is created and used by the next partition.

The pipeline is orchestrated by AWS Step Functions: an outer serial Map over partitions,
each running Fork → an inner Distributed Map of parallel worker Lambdas → Reduce, followed by a final Promote.

![Backfill](./docs/backfill-fork-merge-dark.png#gh-dark-mode-only)
![Backfill](./docs/backfill-fork-merge.png#gh-light-mode-only)

#### Backfill Processor Methods

For backfill your processor needs to implement these [VirtualizarrProcessor protocol](./lambda/virtualizarr-processor/virtualizarr_processor/typing.py)
methods:

- **initialize_backfill_store** Set up the empty Icechunk store workers will fill. Runs once at Init: it creates a backfill branch off the current main tip (staging the load to the side until the final promote), creates the array(s) at their full final shape with coordinate arrays (e.g. time) written as metadata, and commits. It must commit and leave nothing pending, since workers fork from this snapshot and a fork can only be merged if its base is a clean committed snapshot.

- **open_backfill_repo** Open the Icechunk repository and return a Repository handle. Every backfill step that touches the store (Init, Fork, Reduce, Promote) calls this to get the same `repo`.

- **process_backfill_file** Write a single file's virtual dataset into the worker's fork at via `vz.to_icechunk(store, region="auto")`. It must **not** commit.

#### Backfill Configuration

Backfill is configured through the same [settings module](./cdk/settings.py) / `.env` file as the rest of the deployment. Settings specific to backfill:

- **BACKFILL_ENABLED** (default `false`) — deploy the backfill Step Functions pipeline.
  Leave this off if you only need forward queue processing.
- **BACKFILL_PARTITION_SIZE** (default `500`) — number of files per partition. Each
  partition becomes one merged commit.
- **BACKFILL_MAX_ITEMS_PER_BATCH** (default `10`) — number of file keys processed by each worker Lambda (the inner Distributed Map's batch size). Each batch becomes one child fork.  Keep Lambda timeout limits in mind when configuring this.
- **BACKFILL_MAX_CONCURRENCY** (default `50`) — maximum number of worker Lambdas running in parallel within a partition.  Note that if you are using dependent rate limited APIs like NASA EDL use appropriate settings here to avoid service throttling.
- **ICECHUNK_BUCKET_NAME** - the name for the S3 bucket to create holding the Icechunk store and the per-run fork artifacts.
- **DATA_BUCKET_NAME** - the source bucket workers read files from.

#### Running Backfill Processing
To start backfill processing run:
```bash
./scripts/start_backfill.sh <execution-name> <inventory-uri>
```
Where `execution-name` is a unique id to identify your Step Function run and
`inventory-uri` is an s3 path to `json` file containing an array of string keys
for the files to be processed.  The inventory file must be in a bucket that the backfill lambda functions have permission to access.


### Forward Processing :arrow_forward:
Forward processing handles **new production files as they become available**.
It uses an SQS queue to receive notifications about new files and control the
rate of processing.

Each message is a file to parse and append to the `main` branch, and the queue consumer
Lambda commits once per batch of files (the number of file messages sent to a single
Lambda invocation is controlled by `SQS_BATCH_SIZE`.

For S3 buckets where new data is continually added you can enable an [SNS topic for new data](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ways-to-add-notification-config-to-bucket.html) which the Virtualizarr Data Pipelines queue can subscribe to, so files are processed as they land.  This can be configured using `SNS_TOPIC` which will automatically wire up notifications to the queue.

> **TEMPO/ASDC note:** ASDC does not currently publish an SNS notification
> topic for `asdc-prod-protected`, so `SNS_TOPIC` is left unset and this
> pipeline feeds the queue itself by **polling CMR**: a scheduled Lambda
> enqueues every granule whose CMR `revision_date` advanced past a
> persisted watermark (with an overlap window; the consumer's routing
> rules make duplicate enqueues harmless). Revision-date polling captures
> new scans, republications, and the ongoing historical back-fill alike,
> and the ~30-minute polling cadence is immaterial next to the product's
> ~3 h median production lag. It would be great if ASDC added a
> provider-side SNS topic for new/updated objects — the queue could then
> subscribe directly for lower latency and less CMR load, with the poller
> retained as a missed-notification backstop; consider requesting this
> through the ASDC/LARC DAAC support channel.

![Architecture](./docs/architecture-dark.png#gh-dark-mode-only)
![Architecture](./docs/architecture.png#gh-light-mode-only)

#### Forward Processor Methods

The `processor` protocol methods below drive **forward processing**:

- **initialize_session** This method takes the repository from above and returns
  a writable Icechunk session.

- **process_file** This method should take a file uri and a session and use a Virtualizarr parser to parse it and add the resulting ManifestStore or virtual dataset to the Icechunk store.

- **commit_processed_files** This method commits all the changes made during the
  session in a single commit.

- **garbage_collect** This method runs Icechunk garbage collection and snapshot
  removal for snapshots older than a given expiry time. It is shared by both
  processing modes and is invoked on the schedule set by `GARBAGE_COLLECTION_FREQUENCY`.

#### Forward Processing Configuration
- **ICECHUNK_BUCKET_NAME** - the name for the S3 bucket to create holding the Icechunk store and the per-run fork artifacts.
- **DATA_BUCKET_NAME** - the source bucket workers read files from.
- **SNS_TOPIC** - the SNS topic ARN for the data bucket to subscribe to
  notifications for newly published files.
- **SQS_BATCH_SIZE** - the number of files that each forward processing Lambda
  execution will process at once.

### Complete Workflow Sequencing :1234:
Most projects will require both backfill and forward processing to create a
complete, regularly updated Icechunk store.  To properly sequence commits
and reduce merge conflicts the optimal approach is to

1. Configure your pipeline with `BACKFILL_ENABLED` set to `True` and your Icechunk
   store initialized to the extent of the files in your inventory.
2. Configure an SNS_TOPIC and any new files published after deployment will be
   pushed to the SQS queue.  While the backfill is processing, forward processing
   is disabled and any new files will be buffered into the queue.
3. Trigger backfill processing for your inventory and monitor it's status.  When
   it is complete, set `FORWARD_QUEUE_ENABLED` to `True` and re-deploy.
4. The forward processing pipeline will now begin to pull messages from the queue
   and append them to the store.
5. Between the creation of the inventory and the SQS queue receiving SNS messages it
   is possible that new files were published to the bucket that are not included
   in the inventory or were not enqueued.  These can be manually pushed to the
   SQS queue for processing to ensure no data gaps.

### Project commands :hammer:
#### To set up the development environment
```
./scripts/setup.sh
```

#### Run tests
```
uv run pytest
```

#### Review your infrastructure before deploying
```
cp .env.sample .env
uv run --env-file .env cdk synth  # after customizing .env
```

#### Deploy the CDK infrastructure.

```
uv run --env-file .env cdk deploy
```
