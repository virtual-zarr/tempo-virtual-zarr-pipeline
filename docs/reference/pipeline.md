# How the pipeline works

## Collection configuration

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

## Backfill inventory

`scripts/build_backfill_inventory.py` produces the input for a backfill: a
validated JSON document with one entry per granule — its `.nc` link, its
granule UR, and its exact in-file `/time[0]`. Because it reads every
granule's header, the standard way to run it is in-region through the
stack's CodeBuild project — `scripts/run_codebuild.sh -e <env file>` — see
[Deploying and running](deploying.md).

The in-file time is the important part. It differs from both the CMR and
filename timestamps (`...T174200Z` has `/time` = 17:42:18.02), and the
store's time axis is built from these exact values. That's why the builder
reads a few KB of every granule's header.

The document is rejected if it's empty, unsorted, or contains duplicate times
or granule URs. The pipeline re-checks all of that when it reads the file.

## Backfill

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
instead of being discarded. The only thing that happens after the CAS is
deleting the now-served `backfill` branch, which cannot fail the execution
(a retried promote that finds the branch already gone converges instead of
erroring).

## Validation

A granule is written only if it matches the template's shared attributes,
carries the bit-identical reference grid, and its `/time[0]` equals its own
`time_coverage_start_since_epoch` attribute.

Every virtual reference is also stamped with the source object's observed
modification time. If a source file is later overwritten, reads of the stale
references fail instead of returning bytes from a changed file.

## Forward processing

A scheduled Lambda polls CMR for granules whose revision date advanced past a
persisted watermark and enqueues them. (ASDC publishes no SNS topic for the
bucket; see the note below.) The SQS consumer routes each granule:

| Situation | Action |
|---|---|
| granule UR owns a slot (or ledger entry) and the source object is unchanged | skip: no parse, no write (`UNCHANGED`) |
| time is after the axis end | append |
| time occupies a slot, same granule UR | overwrite the slot in place (genuine republication) |
| time occupies a slot, different granule UR | reject to the DLQ |
| time is out of order | record in the pending ledger, consume the message |

"Unchanged" is decided by one HEAD request: each slot records the exact
`last_updated_at` stamp its references were written with (the
`granule_stamp` array), and an equal stamp means the existing references
still read correctly — the same change signal the read path lives by. A
batch of only unchanged redeliveries commits nothing, so redeliveries in
the poller's overlap window stop producing no-op snapshots.

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
`s3://<icechunk bucket>/<prefix>/state/`. The poller's first poll starts from
`POLL_START_ISO` when set (typically the backfill inventory's build time),
else a fixed lookback.

> **Feeding the queue:** ASDC does not publish an SNS notification topic for
> `asdc-prod-protected`, so this pipeline polls CMR instead. Duplicate
> enqueues are harmless (the consumer routing is idempotent), and a 30-minute
> poll cadence is negligible next to the product's ~3 h median production
> lag. A provider-side SNS topic would still be worth requesting from ASDC:
> the queue could subscribe directly, with the poller kept as a backstop for
> missed notifications.

A hand-driven recipe for exercising this path one granule at a time is in
[Testing forward processing (small)](testing-forward-processing.md).

## Verification

`scripts/verify_store.py` spot-checks the store against its sources,
independently of the pipeline's own bookkeeping (run it in-region via
`scripts/run_codebuild.sh -V` — s3:// source reads use region-locked DAAC
credentials, so laptop runs 403). It samples random time steps and, for
each, asks CMR for the granule nearest that time. The file CMR points at
must match the store's axis time exactly.

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

## Publishing to Source Cooperative

`scripts/mirror_to_source_coop.py` copies a collection's store to the public
Source Cooperative repository: every object under
`s3://$ICECHUNK_BUCKET/<prefix>/` to
`s3://us-west-2.opendata.source.coop/pangeo/tempo-virtual-icechunk/<prefix>/`,
plus a zip of the same files beside it as `<prefix>.zip`, where `<prefix>` is
`S3_PREFIX/ICECHUNK_PREFIX`, e.g. `tempo/no2/v04`. Objects stream through
the process, one GET and one PUT each, and into a zip built at
`stores/<prefix>.zip` (gitignored) that is uploaded at the end; only the
zip touches disk. Nothing is compared, ordered or deleted: a rerun copies
everything again, and a crash leaves a partial copy until the next run.
Run it from the VEDA JupyterHub, which is in us-west-2 with both buckets;
[the runbook](runbook-mirror-to-source-coop.md) covers the scoped
credentials and the steps.

```bash
uv run --env-file .env_no2 --env-file .env.local scripts/mirror_to_source_coop.py
```

Source reads use your own AWS credentials. Destination writes go through
Source Coop's S3-compatible proxy (`https://data.source.coop`, bucket `pangeo`)
with the temporary keys Source Coop issued for the repository,
`SOURCE_COOP_ACCESS_KEY_ID`, `SOURCE_COOP_SECRET_ACCESS_KEY` and
`SOURCE_COOP_SESSION_TOKEN` in `.env.local` (see the sample). They are not
AWS keys: the raw bucket rejects them with `InvalidAccessKeyId`. The
pre-commit hook rejects them in a tracked env file. Runs are manual, one
per collection, so the public copy is only as fresh as the last run.

The zip unpacks to a directory that opens with
`icechunk.local_filesystem_storage`. The copy publishes the store's metadata
and native arrays, not the granule bytes: the virtual chunks still point at
`asdc-prod-protected`, so readers of the public copy need Earthdata
credentials and in-region compute exactly as readers of the private store do.

## Recovery

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
  by hand with a small Icechunk commit that reads the `pending_ledger` root attribute, drops the offending
  entry, and writes it back.

## Source credentials

Workers can authenticate with Earthdata Login material from any of:
`EARTHDATA_TOKEN`, `EARTHDATA_USERNAME`/`EARTHDATA_PASSWORD`, or a Secrets
Manager secret at `EARTHDATA_SECRET_ARN` holding JSON with `EARTHDATA_TOKEN`
or `EARTHDATA_USERNAME`+`EARTHDATA_PASSWORD` (the same shape
titiler-multidim reads, so services can share one secret), or a plain token
string. They exchange it for temporary S3 credentials at the
bucket's `s3credentials` endpoint (`EARTHDATA_S3_CREDENTIALS_ENDPOINT`
overrides).

Without any of those, reads use the Lambda role's ambient IAM access, which
requires a bucket-policy grant on the source bucket.
