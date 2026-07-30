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

Findings so far: both collections share the same 2950×7750 grid, the same group layout (`product`, `geolocation`, `support_data`, `qa_statistics`), and the same shuffle + deflate(level 1) filter pipeline, with chunk shape (1, 738, 1938) for float64 variables and (1, 984, 2584) for float32/int16 variables. ASDC's CloudFront distribution rate-limits bursts of HTTPS range requests (403 "Request blocked"), so bulk virtualization should use in-region S3 access, or low concurrency with retries over HTTPS.

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

The TEMPO-specific processor has not been written yet — the template's sample processor is still in place, and the exploration scripts above are informing its design. The sections below are the template's documentation for building the processor and deploying the infrastructure.

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
