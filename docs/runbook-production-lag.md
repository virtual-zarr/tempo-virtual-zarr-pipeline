# Runbook: attribute store freshness lag (who owns the hours?)

Use this when the *Store freshness* tile / `AxisEndLag` shows hours of lag
and you need to know whether the pipeline is slow or the upstream product
is. Written 2026-09-16, when the ~3 h median production lag from the
2026-08 smoke test surprised: that number is dominated by the upstream
product, but nothing on the dashboard says so — this runbook is how to
prove the split, and how to find the pipeline's share when it grows.

## What the number actually measures

`AxisEndLag` = *now − the newest slot's scan time*, emitted by the consumer
after each commit and the re-sort after each promote. It bundles the whole
journey of the newest granule, most of which the pipeline does not own:

| | Stage | Timestamp source | Owner | Normal share |
|---|---|---|---|---|
| T0 | scan start | in-file `/time` ≈ UMM `BeginningDateTime` | — | — |
| T0→T1 | L3 processing | UMM `DataGranule.ProductionDateTime` | TEMPO SDC | the bulk of the ~3 h |
| T1→T2 | delivery + catalog | CMR `meta.revision-date` | ASDC | minutes–an hour |
| T2→T3 | poll wait | poller `Poll complete` log | **pipeline** | ≤ `POLL_SCHEDULE_MINUTES` (30) |
| T3→T4 | queue + commit | consumer `Processed granule` log | **pipeline** | minutes |

Two structural effects to keep in mind before reading any chart:

- **TEMPO is daylight-only.** Overnight, `AxisEndLag` legitimately climbs
  to ~12–16 h with nothing wrong anywhere. Compare like with like: daily
  *minimum* (the lag right after the freshest append), not average.
- The pipeline can never beat T0→T2. If upstream median is ~3 h, a
  perfectly healthy store shows `AxisEndLag` ≈ 3 h + up to one poll
  cadence, every day, forever.

All commands assume the collection's env, as in the other runbooks:

```bash
export AWS_PROFILE=<profile>   # or rely on .env.local via uv run
STACK_NAME=tempo-hcho          # repeat for the other stack
```

## Step 0 — quantify what you're seeing

Daily minimum of `AxisEndLag` over the last two weeks (dimensions are
logical, no physical resource names needed — `Stage` is the env file's
`STAGE`):

```bash
aws cloudwatch get-metric-statistics --namespace TempoPipeline \
  --metric-name AxisEndLag \
  --dimensions Name=Collection,Value=hcho Name=Stage,Value=prod \
  --start-time "$(date -u -d '14 days ago' +%FT%TZ)" \
  --end-time "$(date -u +%FT%TZ)" \
  --period 86400 --statistics Minimum --query \
  'sort_by(Datapoints,&Timestamp)[].[Timestamp,Minimum]' --output table
```

Daily minima of ~3–4 h (≈ 11–15k seconds) are the healthy baseline. This
runbook is about explaining that baseline and catching drift above it;
total staleness > 24 h is `AxisEndLagAlarm`'s job, and a dead poller or
re-sort has its own alarms.

## Step 1 — measure the upstream share (laptop, no credentials)

```bash
uv run scripts/measure_publish_order.py --collection hcho --days 14
```

Along with the publish-order stats, this prints the production-lag block,
computed from CMR alone (fresh scans only; historical archive arrivals and
republications are excluded so they can't distort it):

```
  production lag, scan start -> CMR publication (N fresh scans): median X h, p90 Y h
    scan -> ProductionDateTime: median A h
    ProductionDateTime -> publication: median B h
```

Read it as: **A** is science processing at the TEMPO SDC, **B** is
delivery to ASDC plus catalog ingest, **X = A + B** is the floor under
everything the pipeline does.

- **A dominates (expected):** inherent product latency. Not fixable from
  AWS. Record the measurement; escalate to the product team only if it
  materially exceeds the product's own documented latency target, and
  make sure the portal's "latest available" messaging assumes hours, not
  minutes.
- **B dominates:** worth raising with ASDC — and strengthens the standing
  ask (README, Forward processing note) for a provider SNS topic.

## Step 2 — measure the pipeline share (per granule)

Take the few most recent granules from CMR with their publication times:

```bash
curl -s "https://cmr.earthdata.nasa.gov/search/granules.umm_json?collection_concept_id=C3685897141-LARC_CLOUD&sort_key=-start_date&page_size=5" \
  | jq -r '.items[] | .meta."revision-date" + "  " + .umm.GranuleUR'
```

For each, find when the consumer committed it — it logs `Processed
granule` with the url and outcome:

```bash
LG=$(aws lambda list-functions \
  --query "Functions[?contains(FunctionName, \`processmessages\`) && contains(FunctionName, \`$STACK_NAME\`)].LoggingConfig.LogGroup | [0]" \
  --output text)
aws logs filter-log-events --log-group-name "$LG" --start-time "$(date -d '2 days ago' +%s)000" \
  --filter-pattern '"<granule file>.nc"' \
  --query 'events[].[eventId,message]' --output text | head
```

Log-event time − revision-date is the pipeline's share, T2→T4. **Healthy:
under `POLL_SCHEDULE_MINUTES` + ~5 min.** If that holds across a handful
of granules, the pipeline is exonerated — everything else in Step 0's
number is Step 1's upstream lag, and you are done.

## Step 3 — if the pipeline share is large, localize it

Work back along T2→T4:

- **Poll wait (T2→T3).** The poller logs `Poll complete` with `granules`
  and `enqueued` every cycle; gaps mean missed schedules
  (`PollerErrorsAlarm` history, or the poller's log group). A stale
  watermark shows up here too — the state file holds one timestamp:

  ```bash
  aws s3 cp "s3://$ICECHUNK_BUCKET/tempo/hcho/<ICECHUNK_PREFIX>/state/cmr-watermark.json" -
  ```

  (the default `POLL_WATERMARK_URI`; the timestamp inside should be
  within one cadence of now).
- **Queue (T3→T4).** The dashboard's queue widget, or
  `ApproximateAgeOfOldestMessage` on `$STACK_NAME-queue`. Consumer
  *throttles* are expected (reserved concurrency 1, SQS redelivers);
  sustained age growth is not — look at `CommitFailures` (a failed batch
  commit redelivers the whole batch after the visibility timeout, which
  reads as minutes of added latency per retry) and sustained
  `PromoteFailures` (writers fighting the re-sort).
- **A frozen axis end with a flowing queue.** If everything commits but
  `AxisEndLag` still grows: the *newest* granule specifically isn't
  landing. Check the DLQ and the dashboard's *Rejected granules* table —
  a UR/time collision on the newest scan freezes the axis end while all
  older traffic proceeds normally. That is the operator case in
  [README → Recovery](../README.md#recovery) /
  [runbook-redrive-dlq](./runbook-redrive-dlq.md).

## Step 4 — remediation by stage

| Dominant stage | Lever |
|---|---|
| T0→T1 processing | none in this repo — document, set portal expectations, escalate with Step 1 numbers if out of spec |
| T1→T2 delivery/catalog | raise with ASDC; renew the SNS-topic request (would also delete T2→T3) |
| T2→T3 poll wait | lower `POLL_SCHEDULE_MINUTES` (each poll is one CMR metadata query — cost is negligible; 10 min is reasonable), redeploy |
| T3→T4 queue/consumer | fix whatever Step 3 found: commit failures, DLQ'd newest granule, visibility-timeout retry loops |

Do **not** "fix" the baseline by tightening `AxisEndLagAlarm`: its 24 h
threshold deliberately sits above upstream median + overnight gap + poll
cadence, and anything tighter pages on the product's own rhythm.

## Step 5 — keep the number honest

Re-run Step 1 after upstream announcements (V04 reprocessing campaigns,
SDC changes) and occasionally otherwise; if the median moves materially,
update the ~3 h figure quoted in the README's smoke-test takeaways so the
next operator isn't surprised in the other direction.
