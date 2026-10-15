---
theme: default
title: TEMPO virtual Zarr pipeline
info: A technical deep dive into the pipeline that keeps the TEMPO L3 virtual Icechunk stores current.
colorSchema: light
transition: fade
mdc: true
fonts:
  sans: Inter
addons:
  - '@alukach/slidev-addon-qrcode'
layout: cover
class: gradient text-center
---

<span class="eyebrow">Technical deep dive · October 2026</span>

# TEMPO virtual Zarr pipeline

How the stores are built, kept current, watched, and what it would take to own one

<div class="mt-10 flex justify-center">
  <DeckQRCode url="https://tempo.virtualzarr.cloud/" :size="140" />
</div>

<p class="text-sm opacity-80">tempo.virtualzarr.cloud</p>

<!--
Follow-up to the TEMPO virtualization meeting. The ask was a deeper look at the pipeline for the people who may run one like it.
-->

---

<span class="eyebrow">Part 1 · Why we are here</span>

# Context

<div class="cards">
  <div class="card"><b>AIR4US Beta Portal</b>VEDA portal renders TEMPO through virtual store, for serving dynamic maps, timeseries, and analysis without data duplication.</div>
  <div class="card"><b>ESDIS-owned virtualization</b>The goal is a pipeline ESDIS can run as an enterprise service. Everything here is infrastructure as code so it can move.</div>
  <div class="card"><b>This talk</b>A tour of what exists today, then the store, the pipeline stage by stage, lag, incidents, and the hand-over questions.</div>
</div>

<div class="mt-8 text-sm">

Find out more: <a href="https://tempo.virtualzarr.cloud/">tempo.virtualzarr.cloud</a>

</div>

---

<span class="eyebrow">Part 1 · The portal</span>

# What the portal shows

<img src="/screenshots/air4us-portal.png" alt="AIR4US showing TEMPO NO2 tropospheric column for one scan" class="rounded border mt-2" style="max-height: 340px; margin-inline: auto" />

<p class="text-sm mt-1 text-center">TEMPO NO₂ tropospheric column, one scan, picked on the time slider. Every tile is a titiler-multidim request against the virtual store.</p>

<p class="text-sm text-center"><span class="eyebrow" style="display:inline; margin-right:.5rem">AIR4US ODSI lead</span>Jeanné le Roux</p>

---

<span class="eyebrow">Part 1 · The virtual stores</span>

# Two virtual Icechunk stores

<div class="cards">
  <div class="card"><b>Private bucket, continuously updated</b>The pipeline's own stores, one per collection (NO₂ and HCHO L3 V04). A new scan is appended within minutes of CMR publishing it. This is what the portal reads.</div>
  <div class="card"><b>Source Cooperative, public snapshot</b><code>s3://us-west-2.opendata.source.coop/<wbr>pangeo/tempo-virtual-icechunk/<wbr>tempo/{no2,hcho}/v04/</code> Refreshed by hand from the mirror runbook, with a zip of each for local use.</div>
  <div class="card"><b>What is inside</b>An Icechunk repository of byte-range references into ASDC's netCDF granules. Nothing is copied. A reader opens a few kilobytes of metadata, then fetches bytes from ASDC with Earthdata credentials.</div>
</div>

<p class="text-sm mt-6">In motion: a snapshot in an ASDC bucket, as the interim step toward ESDIS ownership.</p>

---

<span class="eyebrow">Part 1 · The docs</span>

# tempo.virtualzarr.cloud

<div class="grid grid-cols-[minmax(0,3fr)_minmax(0,1fr)] gap-6">
<div class="cards" style="grid-template-columns: 1fr">
  <div class="card"><b>Reading the stores</b>Two quickstart notebooks. One reads chunks in us-west-2 through the DAAC's temporary S3 credentials. The other reads the same chunks over HTTPS from a laptop with an Earthdata token. Both end with a map of one scan and a week at one pixel.</div>
  <div class="card"><b>How it works</b>The architecture page with its figures, and the pipeline stage by stage: backfill, forward processing, verification, publishing.</div>
  <div class="card"><b>Operating</b>Deploying, settings, monitoring, alarm email, and five runbooks for the incidents seen so far.</div>
</div>
<div class="flex flex-col items-center justify-center">
  <DeckQRCode url="https://tempo.virtualzarr.cloud/" :size="150" />
  <p class="text-xs mt-2 text-center">tempo.virtualzarr.cloud</p>
</div>
</div>

---

<span class="eyebrow">Part 1 · The monitoring</span>

# Monitoring

<img src="/screenshots/cloudwatch-dashboard-1.png" alt="The tempo-no2 CloudWatch dashboard: alarm status, store lag, queue age, DLQ depth, pending ledger, granule routing, consumer duration" class="rounded border mt-1" style="max-height: 215px; margin-inline: auto" />

<div class="email mt-2">
  <div class="email-subject">ALARM: "tempo-hcho-PollerErrorsAlarm" in US West (Oregon)</div>
  <div class="email-row"><span>Name</span>tempo-hcho-PollerErrorsAlarm</div>
  <div class="email-row"><span>Description</span>The scheduled CMR poller failed: nothing is feeding the queue, so the store goes stale until it recovers. Check the poller Lambda's CloudWatch logs (the FunctionName dimension below). The poll watermark only advances on success, so the next clean run re-covers the gap by itself.</div>
  <div class="email-row"><span>State change</span>OK -> ALARM &nbsp;·&nbsp; Threshold Crossed: 1 datapoint [1.0] was greater than the threshold (0.0) &nbsp;·&nbsp; Wednesday 23 September, 2026 21:08:54 UTC</div>
</div>

<p class="text-xs mt-2">Each stack renders its own dashboard from the same metric objects the alarms use. Deploy, and it exists. The five alarms across the top page by email, as above. The name carries the stack prefix, and the description says what broke, where to look, and what recovers on its own. `AxisEndLagAlarm` treats missing data as breaching on purpose, because a dead poller emits no error metric at all.</p>

<style>
.email { border: 1px solid var(--vz-divider); border-radius: 10px; background: var(--vz-soft); padding: .6rem .9rem; font-size: .72rem; line-height: 1.35; }
.email-subject { font-weight: 600; font-size: .85rem; margin-bottom: .35rem; }
.email-row { display: grid; grid-template-columns: 6.5rem 1fr; gap: .5rem; }
.email-row span { font-family: var(--vz-mono); font-size: .62rem; text-transform: uppercase; letter-spacing: .08em; color: var(--vz-muted); padding-top: .15rem; }
</style>

---

<span class="eyebrow">Part 1 · The rendering service</span>

# Serving the store

<div class="grid grid-cols-[minmax(0,3fr)_minmax(0,2fr)] gap-6">
<div>

<img src="/figures/fig6-serving.svg" alt="Serving through titiler-multidim and deck.gl-raster" class="figure" />

</div>
<div class="text-sm">

- `/tiles` renders one scan per request with `sel=time` and nearest matching
- `/point` reads a pixel time series
- `/statistics` is next: zonal statistics over a polygon across time
- titiler opens the repo at a snapshot and fetches referenced bytes from ASDC with EDL-authorized virtual chunk reads
- Per-request latency is those fetches. A dataset and session cache is advisable in production
- deck.gl-raster is the long-term path with no tile server: the browser reads the references and WebGL renders

</div>
</div>

---

<span class="eyebrow">Part 1 · The rendering service</span>

# Custom, dynamic filtering for TEMPO data

<div class="grid grid-cols-2 gap-4 mt-2">
  <div>
    <img src="/screenshots/titiler-nofilter.png" alt="NO2 tiles with no filter" class="rounded border" style="max-height: 240px; margin-inline: auto" />
    <p class="text-sm mt-1"><span class="eyebrow">Raw</span>Every retrieval, cloudy or not</p>
  </div>
  <div>
    <img src="/screenshots/titiler-filter.png" alt="NO2 tiles masked by cloud fraction, solar zenith angle, and quality flag" class="rounded border" style="max-height: 240px; margin-inline: auto" />
    <p class="text-sm mt-1"><span class="eyebrow">where</span>Clouds and bad retrievals drop out. The basemap shows through.</p>
  </div>
</div>

```text
…&variable=vertical_column_troposphere&where=eff_cloud_fraction<=0.5&where=solar_zenith_angle<=80&where=main_data_quality_flag<=1
```

<!--
Repeatable where parameters on every titiler-multidim endpoint, any variable in the store, ANDed. The mask runs inside xarray's lazy indexing before warping. No second product, no server code. The same conditions a scientist writes as ds.where() in the quickstart notebook.
-->

---

<span class="eyebrow">Part 1 · The cost</span>

# Cost

<div class="grid grid-cols-[1fr_2fr] gap-6 mt-4">
<div class="card stat">
  <span class="eyebrow">Keeping one day current</span>
  <b>$0.30 <small>/ day</small></b>
  <em>continuous forward processing, from Cost Explorer</em>
</div>
<div>
  <img src="/screenshots/costs.png" alt="Cost Explorer, daily cost by service, August to October 2026" class="rounded border" />
</div>
</div>

<div class="grid grid-cols-2 gap-6 text-sm mt-3">
<div>

| A quiet day | |
|---|---|
| CloudWatch | $0.19 |
| Lambda | $0.09 |
| SQS | $0.01 |
| CodeBuild, Step Functions, SNS | $0.00 |

</div>
<div>

- The late-August band is the backfill. A few dollars a day while the historical archive loads
- The September spike is one verification and re-sort heavy day
- Source reads are byte-range requests against ASDC's bucket, in region. No data is copied, so there is no second archive to pay for

</div>
</div>

---

<span class="eyebrow">Part 2 · The store</span>

# What a virtual store is

<div class="grid grid-cols-[minmax(0,3fr)_minmax(0,2fr)] gap-6">
<div>

<img src="/figures/fig1-virtual-store.svg" alt="The store holds references to bytes in the source bucket" class="figure" />

</div>
<div>

The store does not copy NASA's data.

Each store is an Icechunk repository whose chunks are byte-range references into the original netCDF granules.

A reader opens a few kilobytes of metadata and then fetches bytes from ASDC.

Each reference records the source object's modification time. If a granule is overwritten in place upstream, reads of the stale reference fail instead of returning the wrong scan.

</div>
</div>

---

<span class="eyebrow">Part 2 · The store</span>

# One day of scans

<div class="grid grid-cols-2 gap-4 mt-2">
  <div>
    <SlidevVideo autoplay loop muted class="rounded">
      <source src="/media/tempo-hcho-scan-day.mp4" type="video/mp4" />
    </SlidevVideo>
    <p class="text-sm mt-1">Each hourly scan lands as one granule. The axis accumulates them.</p>
  </div>
  <div v-click>
    <SlidevVideo autoplay loop muted class="rounded">
      <source src="/media/tempo-hcho-store-growth.mp4" type="video/mp4" />
    </SlidevVideo>
    <p class="text-sm mt-1">The time axis growing as the store is appended.</p>
  </div>
</div>

<!--
Both animations were rendered by reading pixels through the virtual store's chunk references, so they demonstrate the read path.
-->

---

<span class="eyebrow">Part 2 · The store</span>

# Layout

<div class="grid grid-cols-[minmax(0,3fr)_minmax(0,2fr)] gap-6">
<div>

```text
/                       dims: time (append), latitude=2950, longitude=7750
├── time         (time)                         float64  [native]
├── latitude     (latitude)                     float32  [native]
├── longitude    (longitude)                    float32  [native]
├── weight       (time, latitude, longitude)    float32
├── vertical_column                             float64
├── vertical_column_uncertainty                 float64
├── main_data_quality_flag                      int16
├── solar_zenith_angle                          float32
├── eff_cloud_fraction                          float32
└── … 14 more variables, all virtual
```

</div>
<div class="text-sm">

- One store per collection, every group flattened into the root and concatenated along `time`. titiler-multidim needs the flat layout
- Per-granule shape (1, 2950, 7750), keeping the source files' shuffle and deflate codecs
- Chunks (1, 738, 1938) for float64, (1, 984, 2584) for the rest
- 1-D coordinates are stored natively. Everything else is a reference

</div>
</div>

---

<span class="eyebrow">Part 2 · The store</span>

# Reading it

<div class="grid grid-cols-2 gap-4">
<div>

**In us-west-2**, through the DAAC's temporary S3 credentials

```python
import icechunk, xarray as xr
from earthaccess_auth.adapters.icechunk import (
    earthdata_containers_credentials,
)

storage = icechunk.s3_storage(
    bucket="us-west-2.opendata.source.coop",
    prefix="pangeo/tempo-virtual-icechunk/tempo/no2/v04",
    region="us-west-2", anonymous=True,
)
repo = icechunk.Repository.open(storage)
repo = repo.reopen(
    authorize_virtual_chunk_access=earthdata_containers_credentials(repo)
)
ds = xr.open_zarr(repo.readonly_session("main").store,
                  consolidated=False, chunks=None)
```

</div>
<div>

**From anywhere**, over HTTPS with an EDL bearer token

```python
https_prefix = "https://data.asdc.earthdata.nasa.gov/asdc-prod-protected/"
config = repo.config
config.set_virtual_chunk_container(
    icechunk.VirtualChunkContainer(
        https_prefix,
        icechunk.http_store(headers={"Authorization": f"Bearer {token}"}),
        name="asdc",
    )
)
repo = repo.reopen(
    config=config,
    authorize_virtual_chunk_access=icechunk.containers_credentials(
        {https_prefix: icechunk.credentials.HttpAccess}
    ),
)
```

<p class="text-sm mt-2">Public copies on <a href="https://source.coop/pangeo/tempo-virtual-icechunk">Source Cooperative</a>, refreshed by hand. The pipeline's own stores live in a private bucket.</p>

</div>
</div>

---

<span class="eyebrow">Part 3 · The pipeline</span>

# Stack components

<div class="grid grid-cols-[minmax(0,3fr)_minmax(0,2fr)] gap-6">
<div>

<img src="/figures/fig2-forward-architecture.svg" alt="One CDK stack per collection" class="figure" />

</div>
<div class="text-sm">

One CDK stack per collection, configured through typed settings.

- A scheduled Lambda polls CMR for new revisions
- An SQS queue feeds a consumer Lambda with reserved concurrency 1
- Scheduled jobs run the re-sort, garbage collection, and the backfill state machine
- An on-demand CodeBuild project runs the in-region jobs: inventory builds, stamp recording, store verification
- The manifest and pending ledger live inside the Icechunk repo and commit atomically with the data they describe

</div>
</div>

---

<span class="eyebrow">Part 3 · The pipeline</span>

# Backfill

<div class="grid grid-cols-[minmax(0,3fr)_minmax(0,2fr)] gap-6">
<div>

<img src="/figures/fig3-backfill-promote.svg" alt="Backfill fork/merge partitions and the promote gate" class="figure" />

</div>
<div class="text-sm">

- The inventory is built in-region by CodeBuild, because the DAAC's temporary credentials only work from us-west-2
- The validated inventory defines the full time axis up front
- Workers write disjoint slot ranges into Icechunk forks. Each partition merges into one commit on the `backfill` branch
- Promote re-validates at a pinned snapshot, then moves `main` by compare-and-swap. `main` never points at a partial store
- The chunk-reference count check catches unwritten slots, which otherwise read as fill values and pass metadata checks

</div>
</div>

---

<span class="eyebrow">Part 3 · The pipeline</span>

# Forward processing

<div class="grid grid-cols-[minmax(0,3fr)_minmax(0,2fr)] gap-6">
<div>

<img src="/figures/fig4-forward-routing.svg" alt="How the consumer routes incoming granules" class="figure" />

</div>
<div class="text-sm">

- Publication order routinely diverges from scan-time order. In a 14-day window in August 2026, about 43% of adjacent publications were out of order
- Each granule is routed against the axis and the ownership manifest
- Append and same-owner overwrite are the only automatic writes
- Out-of-order arrivals go to the pending ledger
- A time collision with a different granule UR goes to the DLQ for an operator

</div>
</div>

---

<span class="eyebrow">Part 3 · The pipeline</span>

# Re-sort

<div class="grid grid-cols-[minmax(0,3fr)_minmax(0,2fr)] gap-6">
<div>

<img src="/figures/fig5-resort-fold.svg" alt="Re-sort folds the pending ledger into place" class="figure" />

</div>
<div class="text-sm">

- A scheduled job folds the pending ledger into the store
- Icechunk's `reindex_array` relocates the slots after the insertion point as metadata, without re-reading any source file. Only the inserted granules are parsed
- The fold and the ledger drain are one commit. `main` moves by compare-and-swap
- If the consumer appended meanwhile, the CAS fails, nothing is consumed, and the next run retries against the new tip

</div>
</div>

---

<span class="eyebrow">Part 3 · The pipeline</span>

# Infrastructure as code

<div class="grid grid-cols-2 gap-6">
<div>

Settings come from env files. One per collection, plus a gitignored local file for account-specific values.

```bash
uv run --env-file .env_hcho --env-file .env.local cdk synth
uv run --env-file .env_hcho --env-file .env.local cdk deploy
```

Repeat with `.env_no2`. Settings are baked into the Lambda environment at deploy time, so changing one is a redeploy.

</div>
<div class="text-sm">

**What runs where**

- Anything that reads source granules runs in us-west-2 through the stack's CodeBuild project
- `cdk deploy`, backfill start, queue operations, CMR queries, and store metadata reads are laptop-safe

**Where it comes from**

- Instantiated from the <a href="https://github.com/developmentseed/virtualizarr-data-pipelines">virtualizarr-data-pipelines</a> template
- Improvements that are not TEMPO-specific go back to the template

</div>
</div>

---

<span class="eyebrow">Part 3 · The pipeline</span>

# Why this design

Three properties of the TEMPO source drive everything unusual. If your dataset lacks a property, you do not need the machinery in that row.

| Source property | Forces | Delete it when |
|---|---|---|
| ASDC publishes no SNS topic for the source bucket | CMR poller plus watermark | ASDC provides a notification topic, so the queue subscribes directly |
| The in-file `/time` differs from the CMR and filename timestamps, so a granule's slot on the axis is unknowable without reading the file | The ownership manifest. The pending ledger plus re-sort job, because a slot cannot be pre-created for a granule nobody has read | the metadata times become exact |
| The DAAC revises and republishes granules to the same S3 URI | UR-checked routing. Modification-time-stamped references, so stale reads fail loudly | never, for this DAAC |

<p class="text-sm mt-4">Everything else is the standard machinery of the <a href="https://github.com/developmentseed/virtualizarr-data-pipelines">template</a>: fork/merge backfill, compare-and-swap promotes, pinned-tip validation.</p>

---

<span class="eyebrow">Part 4 · Running it</span>

# Lag

The store's freshness metric bundles the whole journey of the newest granule. The pipeline owns minutes of it.

| | Stage | Owner | Normal share |
|---|---|---|---|
| T0 | scan start | | |
| T0 to T1 | L3 processing | TEMPO SDC | the bulk of the 3 hours |
| T1 to T2 | delivery and catalog | ASDC | minutes to an hour |
| T2 to T3 | poll wait | **pipeline** | at most 30 minutes |
| T3 to T4 | queue and commit | **pipeline** | minutes |

<div class="grid grid-cols-[minmax(0,3fr)_minmax(0,2fr)] gap-4 mt-3">
  <div class="rounded border overflow-hidden" style="aspect-ratio: 755 / 285">
    <img src="/screenshots/cloudwatch-dashboard-2.png" alt="Lag attribution widget: production, CMR, and virtualization shares of the lag" style="width: 203%; max-width: none; transform: translate(-50.6%, -51.4%)" />
  </div>
  <div class="text-sm">
    <span class="eyebrow">Lag attribution, one week</span>
    Blue is L3 production, orange is CMR, green is the pipeline. The gaps are nights. Overnight the freshness metric climbs to 12 to 16 hours with nothing wrong, so the alarm uses a 24-hour window.
    <p class="mt-2"><b>What ASDC could delete.</b> An SNS topic on the source bucket removes the poll wait. The queue would subscribe directly.</p>
  </div>
</div>

---

<span class="eyebrow">Part 5 · Handing it over</span>

# What ESDIS would take on

<div class="grid grid-cols-2 gap-6 text-sm">
<div>

**Already in motion**

- A snapshot of the virtual store is being moved to an ASDC bucket as the interim step
- Public copies on Source Cooperative show the store works from outside the account
- References are relative, so a store can move buckets without a rewrite

**What the pipeline needs from ESDIS**

- An account with the CDK bootstrap and the two env files
- Earthdata credentials for the source bucket
- Optionally an SNS topic on the source bucket, which deletes the poller

</div>
<div>

**Open questions**

- Which ASDC bucket hosts the snapshot, and can it be public as before
- Icechunk inside Cumulus ingest: also a TRT focus, which we would like to help with
- Level 2: Sean, Tom, and others are interested in supporting this over the next few months
- Who owns metadata stewardship for a virtual product

</div>
</div>

---
layout: center
class: gradient text-center
---

# Discussion

<div class="mt-6 flex justify-center">
  <DeckQRCode url="https://tempo.virtualzarr.cloud/" :size="140" />
</div>

<p class="text-sm mt-4">
tempo.virtualzarr.cloud · github.com/developmentseed/virtualizarr-data-pipelines · source.coop/pangeo/tempo-virtual-icechunk
</p>

---

<span class="eyebrow">Extra slides</span>

# Incidents and runbooks

<div class="cards">
  <div class="card"><b>Pending-ledger backlog</b>August 2026. The nightly re-sort timed out folding the whole backlog in one run, so the ledger grew to hundreds of granules. Fix: a fold size that fits the Lambda timeout, drained serially. Every promoted run is durable partial progress.</div>
  <div class="card"><b>Non-empty dead-letter queue</b>August 2026. A manual backfill clearing left the store transiently unprocessable and about 2,400 good granules dead-lettered. The DLQ is terminal and expires from original enqueue, so triage promptly. Fix: redrive, triage the survivors, verify.</div>
  <div class="card"><b>Store lag</b>A recurring question rather than an incident. The runbook attributes lag across T0 to T4 so you know whether the pipeline is slow or the upstream is.</div>
</div>

<p class="text-sm mt-6">Two more runbooks cover publishing to Source Cooperative and migrating a store to relative references. All are in the docs under Operating.</p>

