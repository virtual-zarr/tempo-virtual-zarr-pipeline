# Settings

Settings live in [`cdk/settings.py`](https://github.com/virtual-zarr/tempo-virtual-zarr-pipeline/blob/main/cdk/settings.py) and the
per-collection env files (`.env_hcho` / `.env_no2`, plus the gitignored
`.env.local`; samples: [`.env.sample`](https://github.com/virtual-zarr/tempo-virtual-zarr-pipeline/blob/main/.env.sample),
[`.env.local.sample`](https://github.com/virtual-zarr/tempo-virtual-zarr-pipeline/blob/main/.env.local.sample)). The ones that matter most:

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
| `ALARM_EMAIL` | — | notification email for all alarms (see [Monitoring](monitoring.md)) |
| `OWNER` | — | `Owner` cost-allocation tag on every resource; unset applies no tag |
| `CLIENT` | — | `Client` cost-allocation tag on every resource; unset applies no tag |

The Lambda images install against
[`lambda/constraints.txt`](https://github.com/virtual-zarr/tempo-virtual-zarr-pipeline/blob/main/lambda/constraints.txt), an export of the repo's
`uv.lock`, so deploys run the dependency versions the test suite ran.
Regenerate it with the command in its header whenever the lock changes.

Concurrent backfill runs are not supported.
