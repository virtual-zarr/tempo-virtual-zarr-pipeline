# Runbook: attribute store freshness lag

Use this when the *Store lag (scan -> store)* tile, or `AxisEndLag`, shows
hours of lag and you need to know whether the pipeline is slow or the
upstream product is. Expect most of it to be upstream. A TEMPO scan takes
about 3 hours to be processed and catalogued in CMR before the pipeline can
see it. That was measured in August and September 2026, and Step 1
re-measures it in a minute. The dashboard's *Lag attribution* widget shows
the split as hourly averages (production, CMR, virtualization). This runbook
is how to verify it from the source data, and how to find the pipeline's
share when it grows.

## What the number measures

`AxisEndLag` is *now minus the newest slot's scan time*. The consumer emits
it after each commit, and the re-sort after each promote. It bundles the
whole journey of the newest granule, most of which the pipeline does not
own:

| | Stage | Timestamp source | Owner | Normal share |
|---|---|---|---|---|
| T0 | scan start | in-file `/time`, about UMM `BeginningDateTime` | — | — |
| T0 to T1 | L3 processing | UMM `DataGranule.ProductionDateTime` | TEMPO SDC | the bulk of the 3 hours |
| T1 to T2 | delivery + catalog | CMR `meta.revision-date` | ASDC | minutes to an hour |
| T2 to T3 | poll wait | poller `Poll complete` log | **pipeline** | at most `POLL_SCHEDULE_MINUTES` (30) |
| T3 to T4 | queue + commit | consumer `Processed granule` log | **pipeline** | minutes |

Two structural effects to keep in mind before reading any chart:

- **TEMPO is daylight-only.** Overnight, `AxisEndLag` legitimately climbs
  to 12 to 16 hours with nothing wrong anywhere. Compare like with like:
  the daily *minimum* (the lag right after the freshest append), not the
  average.
- The pipeline can never beat T0 to T2. If the upstream median is 3 hours,
  a perfectly healthy store shows `AxisEndLag` of about 3 hours plus up to
  one poll cadence, every day, forever.

All commands assume the collection's env, as in the other runbooks:

```bash
export AWS_PROFILE=<profile>   # or rely on .env.local via uv run
STACK_NAME=tempo-hcho          # repeat for the other stack
```

## Step 0: quantify what you are seeing

Daily minimum of `AxisEndLag` over the last two weeks. The dimensions are
logical, so no physical resource names are needed. `Stage` is the env
file's `STAGE`:

```bash
aws cloudwatch get-metric-statistics --namespace TempoPipeline \
  --metric-name AxisEndLag \
  --dimensions Name=Collection,Value=hcho Name=Stage,Value=dev \
  --start-time "$(date -u -d '14 days ago' +%FT%TZ)" \
  --end-time "$(date -u +%FT%TZ)" \
  --period 86400 --statistics Minimum --query \
  'sort_by(Datapoints,&Timestamp)[].[Timestamp,Minimum]' --output table
```

The healthy baseline is Step 1's upstream median plus up to one poll
cadence: daily minima of 3 to 4 hours (11,000 to 15,000 seconds) as of
September 2026. This runbook is about explaining that baseline and catching
drift above it. Total staleness over 24 hours is `AxisEndLagAlarm`'s job,
and a dead poller or re-sort has its own alarms.

## Step 1: measure the upstream share (laptop, no credentials)

```bash
uv run scripts/measure_publish_order.py --collection hcho --days 14
```

Along with the publish-order stats, this prints the production-lag block,
computed from CMR alone. It counts fresh scans only. Historical archive
arrivals and republications are excluded so they cannot distort it:

```
  production lag, scan start -> CMR publication (N fresh scans): median X h, p90 Y h
    scan -> ProductionDateTime: median A h
    ProductionDateTime -> publication: median B h
```

Read it as: **A** is science processing at the TEMPO SDC, **B** is delivery
to ASDC plus catalog ingest, and **X = A + B** is the floor under everything
the pipeline does.

- **A dominates (expected).** This is inherent product latency, not fixable
  from AWS. Record the measurement. Escalate to the product team only if it
  materially exceeds the product's own documented latency target, and make
  sure the portal's "latest available" messaging assumes hours, not
  minutes.
- **B dominates.** Worth raising with ASDC. It also strengthens the
  standing ask ([Feeding the queue](pipeline.md#forward-processing)) for a
  provider SNS topic.

## Step 2: measure the pipeline share (per granule)

Take the few most recent granules from CMR with their publication times:

```bash
curl -s "https://cmr.earthdata.nasa.gov/search/granules.umm_json?collection_concept_id=C3685897141-LARC_CLOUD&sort_key=-start_date&page_size=5" \
  | jq -r '.items[] | .meta."revision-date" + "  " + .umm.GranuleUR'
```

For each, find when the consumer committed it. It logs `Processed granule`
with the url and outcome:

```bash
LG=$(aws lambda list-functions \
  --query "Functions[?contains(FunctionName, \`processmessages\`) && contains(FunctionName, \`$STACK_NAME\`)].LoggingConfig.LogGroup | [0]" \
  --output text)
aws logs filter-log-events --log-group-name "$LG" --start-time "$(date -d '2 days ago' +%s)000" \
  --filter-pattern '"<granule file>.nc"' \
  --query 'events[].[eventId,message]' --output text | head
```

Log-event time minus revision-date is the pipeline's share, T2 to T4.
**Healthy is under `POLL_SCHEDULE_MINUTES` plus `SQS_MAX_BATCHING_WINDOW_SECONDS`
plus about 5 minutes.** If that
holds across a handful of granules, the pipeline is exonerated. Everything
else in Step 0's number is Step 1's upstream lag, and you are done.

## Step 3: if the pipeline share is large, localize it

Work back along T2 to T4:

- **Poll wait (T2 to T3).** The poller logs `Poll complete` with `granules`
  and `enqueued` every cycle. Gaps mean missed schedules. Check
  `PollerErrorsAlarm` history, or the poller's log group. A stale watermark
  shows up here too. The state file holds one timestamp, which should be
  within one cadence of now:

  ```bash
  aws s3 cp "s3://$ICECHUNK_BUCKET/tempo/hcho/<ICECHUNK_PREFIX>/state/cmr-watermark.json" -
  ```

- **Queue (T3 to T4).** The dashboard's queue widget, or
  `ApproximateAgeOfOldestMessage` on `$STACK_NAME-queue`. Consumer
  *throttles* are expected, since reserved concurrency is 1 and SQS
  redelivers. So is an age up to `SQS_MAX_BATCHING_WINDOW_SECONDS`: a
  partial batch waits that long to fill before the consumer is invoked.
  Sustained age growth is not. Look at `CommitFailures` (a
  failed batch commit redelivers the whole batch after the visibility
  timeout, which reads as minutes of added latency per retry) and sustained
  `PromoteFailures` (writers fighting the re-sort).
- **A frozen axis end with a flowing queue.** If everything commits but
  `AxisEndLag` still grows, the *newest* granule specifically is not
  landing. Check the DLQ and the dashboard's *Rejected granules* table. A
  UR/time collision on the newest scan freezes the axis end while all older
  traffic proceeds normally. That is the operator case in
  [Recovery](pipeline.md#recovery) and
  [the DLQ runbook](./runbook-redrive-dlq.md).

## Step 4: remediation by stage

| Dominant stage | Lever |
|---|---|
| T0 to T1 processing | none in this repo. Document it, set portal expectations, and escalate with Step 1 numbers if out of spec |
| T1 to T2 delivery/catalog | raise with ASDC. Renew the SNS-topic request, which would also delete T2 to T3 |
| T2 to T3 poll wait | lower `POLL_SCHEDULE_MINUTES` and redeploy. Each poll is one CMR metadata query, so the cost is negligible. 10 minutes is reasonable |
| T3 to T4 queue/consumer | fix whatever Step 3 found: commit failures, a DLQ'd newest granule, visibility-timeout retry loops |

Do **not** "fix" the baseline by tightening `AxisEndLagAlarm`. Its 24-hour
threshold deliberately sits above upstream median plus overnight gap plus
poll cadence. Anything tighter pages on the product's own rhythm.

## Step 5: keep the number honest

Re-run Step 1 after upstream announcements (reprocessing campaigns, SDC
changes) and occasionally otherwise. If the median moves materially, update
the figures quoted above and in
[Forward processing](pipeline.md#forward-processing), so the next operator
starts from the right baseline.
