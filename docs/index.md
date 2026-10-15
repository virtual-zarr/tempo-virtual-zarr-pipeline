# tempo-virtual-zarr-pipeline

Virtual Zarr / Icechunk stores for the TEMPO Level 3 gridded products hosted
at NASA ASDC: `TEMPO_HCHO_L3` and `TEMPO_NO2_L3`, both V04.

The stores do not copy NASA's data. Each store is an Icechunk repository
whose chunks are byte-range references into the original netCDF granules.
A reader opens a few kilobytes of metadata and then fetches bytes from ASDC.
A pipeline on AWS keeps each store current as new scans are published.

## Reading the stores

Start here if you want the data.

- [The virtual stores](reference/stores.md): what each store contains, and
  what you need to read it.
- [Quickstart: in AWS us-west-2](examples/quickstart.ipynb): read chunks
  from ASDC's bucket. This is the fast path.
- [Quickstart: from anywhere](examples/quickstart-https.ipynb): read the
  same chunks over HTTPS from a laptop.

## How it works

- [Architecture](reference/architecture.md): the design, with figures, and
  why the TEMPO source needs it.
- [The pipeline](reference/pipeline.md): backfill, forward processing,
  verification, and publishing, in detail.

## Operating

- [Deploying and running](reference/deploying.md): env files, deploying a
  stack, and running a backfill.
- [Settings](reference/settings.md), [Monitoring](reference/monitoring.md),
  and [Alarm email notifications](reference/alarm-email-notifications.md).
- [Testing forward processing](reference/testing-forward-processing.md):
  exercise the consumer one granule at a time.
- Runbooks for the incidents seen so far: a
  [pending-ledger backlog](reference/runbook-drain-pending-backlog.md), a
  [non-empty dead-letter queue](reference/runbook-redrive-dlq.md), and
  [store lag](reference/runbook-production-lag.md). Two more cover
  [publishing to Source Cooperative](reference/runbook-mirror-to-source-coop.md)
  and [migrating a store to relative references](reference/runbook-relativize-virtual-refs.md).

## Development

[Development](reference/development.md) covers the test and lint commands
and the exploration scripts.
