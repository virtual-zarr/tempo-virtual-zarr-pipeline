# Monitoring

Each deployment renders its own CloudWatch dashboard, named after the stack.
The `DashboardUrl` stack output links to it. It is built in `cdk/stack.py`
from the same metric objects the alarms use, so there is nothing to import
or configure. Deploy, and it exists.

## Alarms

Up to seven alarms page, via `ALARM_EMAIL` when it is set. `EarthdataTokenExpiryAlarm` exists only when `EARTHDATA_SECRET_ARN` is set:

| Alarm | Fires when | It usually means |
|---|---|---|
| `DlqMessagesAlarm` | anything lands in the DLQ | granules rejected 20 times: a UR/time collision or a persistent parse failure. The dashboard's *Rejected granules* table shows which, by url for validation rejections and by SQS message id for granules that raised mid-processing |
| `ConsumerErrorsAlarm` | the SQS consumer throws | check the consumer's log group |
| `EarthdataAuthErrorsAlarm` | the consumer gets a 401 from the Earthdata `s3credentials` endpoint | the Earthdata token is expired or revoked, so every granule fails. Put a new token in the secret, then redrive the DLQ. The consumer drops its cached credentials after each 401, so the new token takes effect on the next batch. A 5xx or connection error from the endpoint does not count. Those keep the normal parse retries |
| `EarthdataTokenExpiryAlarm` | the token in the Earthdata secret has under 7 days left, which the CMR poller emits as `EarthdataTokenTimeLeft` on every run | rotate the token before it lapses. Earthdata user tokens last 60 days |
| `PollerErrorsAlarm` | the CMR poller fails its run and both async retries | CMR unreachable, or watermark state unreadable |
| `ResortErrorsAlarm` | the re-sort job fails its run and both async retries. A single failure that a retry heals, such as a lost promote CAS, does not page | the fold failed before promoting. The ledger keeps growing until fixed |
| `AxisEndLagAlarm` | the store's newest time slot is more than 24 h old, **or the `AxisEndLag` metric goes missing, for 24 consecutive hours** | the top-line staleness check. See below |

`AxisEndLagAlarm` treats missing data as breaching on purpose. A dead poller
or a re-sort killed by its timeout emits no error metric at all. The
freshness metric going quiet is the only signal. The 24-hour evaluation
window exists because TEMPO is daylight-only: the emitters legitimately go
quiet overnight, and a single quiet hour must not page. The alarm is only
created when forward processing is enabled. A fresh deployment may hold
ALARM for up to its first day. Its evaluation window predates the first
emission, and those missing hours count as breaching until the first
committed batch or promoted re-sort.

The two scheduled-job alarms count `AsyncEventsDropped`, not `Errors`.
EventBridge invokes them asynchronously and Lambda retries a failed run
twice, so a single failure is usually healed a minute later. To see how
often that happens, the *Poller* and *Re-sort failures* widgets plot both
series. Failed attempts minus failed-after-retries is the healed share.

Throttles on the consumer are *expected*, since its reserved concurrency is
1 and SQS redelivers. They are shown on the dashboard but never alarmed.

## Custom metrics

The handlers emit CloudWatch metrics in the `TempoPipeline` namespace,
dimensioned by `Collection` and `Stage`, so queries never need physical
resource names:

| Metric | Emitted by | How to read it |
|---|---|---|
| `AxisEndLag` (seconds) | consumer after each commit; re-sort after each promote | store freshness. Production lag is normally a few hours, mostly upstream (scan to CMR publication). See [the lag runbook](runbook-production-lag.md) for the attribution |
| `GranulesRouted` (dimension `Route`) | consumer, per consumed batch | `APPENDED` = growth. `UNCHANGED` = redeliveries of unchanged sources skipped without a write (the steady band; its absence with a flowing queue is the anomaly). `OVERWRITTEN` = genuine republications (rare). `PENDING` = out-of-order arrivals headed for the re-sort (routinely a large share). `REJECTED` = collisions headed for the DLQ, counted on first delivery only |
| `ProductionLag` / `CmrLag` (seconds) | poller, per fresh arrival (scan within 24 h, first seen this poll) | upstream share of the lag: scan start to `ProductionDateTime`, and `ProductionDateTime` to CMR `revision-date`. Stacked with `VirtualizationLag` on the *Lag attribution* widget |
| `VirtualizationLag` (seconds) | consumer, per `APPENDED` granule whose message carries the poller's `published` | the pipeline's share: CMR publication to store commit. Healthy is under `POLL_SCHEDULE_MINUTES` plus a few minutes |
| `PendingLedgerDepth` | consumer and re-sort | nonzero is healthy. Trending up across days means the re-sort is not keeping pace |
| `FoldedGranules` | re-sort (0 when it ran with an empty ledger) | pinned at `RESORT_MAX_FOLD` every run means falling behind |
| `PromoteFailures` | re-sort, when its promote raises | occasional ones are the single-writer design working (a concurrent commit won the CAS). Sustained ones mean writers are fighting, or S3 trouble. The counter does not distinguish |
| `CommitFailures` | consumer, when its batch commit raises | shown on the *Consumer duration* widget. The whole batch redelivers |
| `PartitionsDone` / `PartitionsTotal` | backfill reduce / partition steps | backfill progress. The dashboard plots their running sum against the carried-forward total |
| `CompletenessDelta` | `verify_store.py --completeness` (CodeBuild) | granules CMR lists that the store lacks, plus store entries CMR dropped. One point per verify run, so the series is sparse |

Emission is best-effort. A metric failure never fails a batch or a re-sort
run. If a widget shows *no data*, first check that the corresponding job has
actually run. For example, `CompletenessDelta` appears only after a verify
run.

## During a backfill

The dashboard's backfill section, rendered when `BACKFILL_ENABLED` is set,
shows Step Functions executions, the cumulative partitions-done/total graph,
and worker errors. Watch it during the initial fill. Afterward, the two
numbers worth a daily glance are the *Store lag (scan -> store)* and
*Pending ledger depth* tiles. The *Lag attribution* widget next to them says
how much of the lag is upstream.
