# Design: In-stack CloudWatch dashboard

**Problem.** Monitoring today is four "anything above zero" alarms
(`DlqMessagesAlarm`, `ConsumerErrorsAlarm`, `ResortErrorsAlarm`,
`PollerErrorsAlarm`) on an optional SNS email. They answer "did something
throw?" and nothing else. The failure that actually cost us a week answered
neither: the nightly re-sort ran, `merge_pending` succeeded, then the
invocation died on the 15-minute Lambda ceiling. No error metric fired, the
pending ledger grew from 91 to 300 slots behind, and nobody noticed for four
days. Diagnosing it meant hand-querying alarm history, store metadata and the
resort log group.

**Decision.** A `cloudwatch.Dashboard` declared in `cdk/stack.py`, built from
the same `cloudwatch.Metric` objects the alarms already construct. One
dashboard per stack deployment, so `hcho` and `no2` each render their own from
one block of code. No cross-account role, no second repo, no UI step, no
dashboards-as-code mechanism to invent — the stack that owns the resources
owns the view of them.

This does not close the door on the Grafana dashboard
(`docs/grafana-monitoring-plan.md`). Both read the same CloudWatch metrics;
the metric-emission work (Phase 1 there, "Deferred metrics" here) is shared
and neither frontend blocks the other. This one ships without a cross-account
IAM dependency on another team's Terraform, so it ships first.

## Construction pattern

Mirror the existing `_alarm` helper (`cdk/stack.py:650`): components are built
inside setting-gated branches, so widgets are appended at each construction
site rather than assembled at the end from `getattr` probes.

- `__init__` initialises `self._widgets: list[cloudwatch.IWidget] = []` and
  `self._alarms: list[cloudwatch.Alarm] = []` before the first component.
- `_alarm` appends its return value to `self._alarms` (it already returns the
  alarm; every call site currently discards it).
- Each component block appends its widgets immediately after the component and
  its alarm — one or two lines next to the existing `self._alarm(...)` calls at
  `stack.py:127`, `stack.py:237`, `stack.py:482`, `stack.py:530`.
- A final `self._dashboard(settings)` after `_build_inventory_project`
  constructs `cloudwatch.Dashboard` from the accumulated list and emits a
  `CfnOutput` with the console URL, matching the `InventoryBuildProject`
  output idiom at `stack.py:642`.

Dashboard name: `f"{settings.STACK_NAME}"` — already collection- and
stage-qualified, so the two deployments cannot collide.

**Thresholds live in alarms, not in the dashboard.** The "red when" column of
the original design is implemented by an `AlarmStatusWidget` over
`self._alarms` rather than by duplicating threshold values into widget
annotations. One place to change a threshold, and the strip is red for exactly
the conditions that page someone. The only annotations used are reference
lines for values that are *not* alarm conditions (Lambda timeout,
`RESORT_MAX_FOLD`).

## Layout

CloudWatch dashboards are a 24-column grid. Widgets are listed in render
order. CloudWatch has no collapsible sections; `TextWidget` headers separate
the bands instead.

### Band 0 — status (always present)

| Widget | Spec |
|---|---|
| `AlarmStatusWidget` | `alarms=self._alarms`, `width=24, height=2` |

### Band 1 — top-line state (always present, 4 × `width=6, height=4`)

| Tile | Metric |
|---|---|
| Store freshness | `cloudwatch.Metric(namespace="TempoPipeline", metric_name="AxisEndLag", dimensions_map={"Collection": ..., "Stage": ...}, statistic="Maximum", period=Duration.minutes(5))` — **deferred**, see below |
| Queue oldest message age | `self.queue.metric_approximate_age_of_oldest_message(period=Duration.minutes(5), statistic="Maximum")` |
| DLQ depth | `self.dlq.metric_approximate_number_of_messages_visible(period=Duration.minutes(5), statistic="Maximum")` |
| Pending ledger depth | `PendingLedgerDepth`, same namespace/dimensions, `statistic="Maximum"` — **deferred** |

All four are `SingleValueWidget(metrics=[m], sparkline=True)`. `sparkline=True`
is what makes "pending ledger trending upward" readable without a second
widget — the tile shows the value and its recent shape.

### Band 2 — forward processing (always present)

1. **Granule routing**, `width=12, height=6`.
   `GraphWidget(title="Granule routing", stacked=True, left=[appended, overwritten, rejected, pending])`, each
   `cloudwatch.Metric(metric_name="GranulesRouted", dimensions_map={..., "Route": r}, statistic="Sum", period=Duration.minutes(30))`
   for `r` in `APPENDED`, `OVERWRITTEN`, `REJECTED`, `PENDING`. **Deferred.**
   A high `PENDING` share is normal (~43% of adjacent publications arrive out
   of scan-time order); a `REJECTED` step change is a UR collision brewing.

2. **Consumer duration**, `width=12, height=6`.
   `GraphWidget(left=[metric_duration(statistic=s) for s in ("p50","p95","Maximum")], right=[metric_throttles(statistic="Sum")], left_annotations=[cloudwatch.HorizontalAnnotation(value=300000, label="Lambda timeout (5 min)", color=cloudwatch.Color.RED)])`.
   The annotation is the **Lambda timeout** (`Duration.minutes(5)` at
   `stack.py:228`, 300000 ms), not the SQS visibility timeout — 1800 s
   (`stack.py:108`) is the outer redelivery bound, but 300 s is what actually
   kills an invocation. Throttles go on the right axis and are expected under
   burst at `reserved_concurrent_executions=1`; they are displayed, never
   alarmed.

3. **Poller**, `width=12, height=6`.
   `GraphWidget(left=[self.cmr_poller_lambda.metric_invocations(statistic="Sum"), self.cmr_poller_lambda.metric_errors(statistic="Sum")], right=[WatermarkLag])`.
   Gated on `POLL_SCHEDULE_MINUTES`. `WatermarkLag` is **deferred**; the
   invocation/error pair is free and useful alone.

4. **Re-sort**, `width=12, height=6`.
   `GraphWidget(left=[FoldedGranules (Sum)], right=[self.resort_lambda.metric_duration(statistic="Maximum")], left_annotations=[HorizontalAnnotation(value=settings.RESORT_MAX_FOLD, label="RESORT_MAX_FOLD")], right_annotations=[HorizontalAnnotation(value=900000, label="Lambda timeout (15 min)")])`.
   Gated on `RESORT_SCHEDULE_HOURS`. `FoldedGranules` is **deferred**; the
   duration-against-timeout trace is free and is precisely the signal the
   week-long outage lacked — a run pinned near 900 s is falling over even when
   the error metric stays flat. Folds hitting `RESORT_MAX_FOLD` every run
   means falling behind.
   Also plot `PromoteCasRejections` (deferred) on the left axis.

### Band 3 — backfill (gated on `BACKFILL_ENABLED`)

1. **Executions**, `width=12, height=6`. `GraphWidget` over
   `self.backfill_pipeline.state_machine.metric_started()`, `.metric_succeeded()`,
   `.metric_failed()` (all `statistic="Sum"`), with `.metric_time(statistic="Maximum")`
   on the right axis. All free.
2. **Partitions done vs total**, `width=6, height=6`.
   `GaugeWidget(metrics=[MathExpression("100 * done / total", using_metrics={"done": PartitionsDone, "total": PartitionsTotal})], left_y_axis=cloudwatch.YAxisProps(min=0, max=100))`.
   **Deferred.** No ETA panel — extrapolating from partition rate is
   misleading when worker durations vary by an order of magnitude.
3. **Worker failures**, `width=6, height=6`. `GraphWidget` over `self.backfill_pipeline.functions["worker"].metric_errors(statistic="Sum")`. Free.

### Band 4 — data quality (always present)

1. **CMR-vs-store delta**, `width=8, height=6`. `GraphWidget` over
   `CompletenessDelta` (`statistic="Maximum"`). **Deferred** — and note the
   asymmetry: this one is emitted by `put_metric_data` from
   `scripts/verify_store.py`, not EMF, because CodeBuild logs are not
   EMF-parsed. It is a sawtooth, not a continuous series: one point per verify
   run.
2. **Rejected granules**, `width=16, height=6`.
   ```python
   cloudwatch.LogQueryWidget(
       log_group_names=[self.process_messages_log_group.log_group_name],
       view=cloudwatch.LogQueryVisualizationType.TABLE,
       query_lines=[
           "fields @timestamp, granule_ur, reason",
           "filter outcome = 'REJECTED'",
           "sort @timestamp desc",
           "limit 50",
       ],
   )
   ```
   Free, native, no Logs Insights setup. Requires holding a reference to the
   consumer's log group: `function_log_group(self, "process-messages-logs")` is
   currently constructed inline at `stack.py:220` and discarded — assign it to
   `self.process_messages_log_group` first. The query assumes the consumer logs
   structured JSON with `outcome` and `granule_ur` fields; that lands with the
   routing-metric work.
3. **Store growth**, `width=8, height=6`. Time-axis length over time. Emitted
   alongside `AxisEndLag` from the same commit path. **Deferred.**

### Band 5 — cost

**Not built.** CloudWatch has no per-resource cost, and the honest proxies are
thin: Lambda GB-seconds via
`MathExpression("dur / 1000 * 2", using_metrics={"dur": self.process_messages_lambda.metric_duration(statistic="Sum")})`
(the `2` being the 2048 MB memory size at `stack.py:229`, known at synth), plus
`self.inventory_build.metric_duration()` and `AWS/S3 BucketSizeBytes`. That is
three numbers that look like a cost panel and are not one.

`AWS/Billing EstimatedCharges` is account-wide and this account is shared, so
it would attribute other projects' spend to this pipeline.

If cost becomes a real requirement, the answer is Cost Explorer or CUR-in-Athena
with resource tags — not a proxy panel. Recommend against building this band;
it is specified here so the decision is on the record rather than an omission.

## Deferred metrics

Six numbers CloudWatch cannot know. Widgets referencing them synth fine and
render "no data" until emission lands, so the dashboard is useful before them
and does not need reordering after.

| Metric | Emitted from | Widget |
|---|---|---|
| `AxisEndLag` | consumer + re-sort, after commit | Band 1 tile, Band 4 growth |
| `PendingLedgerDepth` | consumer + re-sort, after commit | Band 1 tile |
| `GranulesRouted` (dim `Route`) | consumer, per message | Band 2.1 |
| `FoldedGranules`, `PromoteCasRejections` | re-sort handler | Band 2.4 |
| `PartitionsDone` / `PartitionsTotal` | backfill reduce step | Band 3.2 |
| `CompletenessDelta` | `scripts/verify_store.py`, `put_metric_data` | Band 4.1 |

Namespace `TempoPipeline`, default dimensions `Collection` and `Stage`.
Emission mechanism, `ProcessOutcome.WRITTEN` splitting into
`APPENDED`/`OVERWRITTEN`, and the powertools-vs-helper decision are specified
in `docs/grafana-monitoring-plan.md` Phase 1 and are not restated here — that
work is frontend-independent.

## One alarm to add

`AxisEndLag > 86400` (Maximum over 1 h, `treat_missing_data=BREACHING`). It is
the top-line SLI and the only signal that catches a silently-dead poller or a
re-sort that fails without throwing — the exact shape of the incident that
motivated this. Missing-data-breaching is deliberate and differs from the
`NOT_BREACHING` default in `_alarm` (`stack.py:661`): no data *is* the failure
here. This needs its own construction rather than the `_alarm` helper.

## Gating

| Widget band | Gate | Behaviour when off |
|---|---|---|
| 0, 1, 2.1, 2.2 | none — queue, DLQ and consumer are unconditional | always rendered |
| 2.3 poller | `POLL_SCHEDULE_MINUTES` | widgets omitted |
| 2.4 re-sort | `RESORT_SCHEDULE_HOURS` | widgets omitted |
| 3 backfill | `BACKFILL_ENABLED` | band omitted, including its `TextWidget` header |
| 4 | none | always rendered |

Garbage collection (`GARBAGE_COLLECTION_FREQUENCY`) gets no widget: AWS Batch
publishes no per-job duration or success metric worth graphing, and the job is
a monthly housekeeping run whose failure is not a data-integrity event.

## Tests

`tests/cdk/` already has the harness — `Template.from_stack`, and
`test_stack_gating.py::_template` already parameterises the gates this needs.
Add `tests/cdk/test_dashboard.py`:

1. `test_dashboard_created` — `resource_count_is("AWS::CloudWatch::Dashboard", 1)`.
2. `test_dashboard_name_is_stack_qualified` — `has_resource_properties` with
   `DashboardName` matching `STACK_NAME`, so hcho and no2 cannot collide.
3. `test_backfill_widgets_omitted_when_disabled` — synth with
   `backfill=False`, parse `DashboardBody` (it is a `Fn::Join`, so reuse
   `resolve_joins` from `tests/cdk/conftest.py`), assert no widget title
   contains "Backfill"; and the converse with `backfill=True`.
4. `test_axis_end_lag_alarm_breaches_on_missing_data` — assert the alarm's
   `TreatMissingData` is `breaching`, since that is the whole point of it and a
   copy-paste from `_alarm` would silently get it wrong.

`aws-cdk-lib` is pinned at 2.232.2 (`uv.lock:70`); every widget class used here
(`GaugeWidget`, `LogQueryWidget`, `AlarmStatusWidget`, `MathExpression`) exists
in that version.

## Non-goals

- **Replacing the alarms.** They remain the paging path; the dashboard is for
  diagnosis after a page, or during a deliberate look. Nothing here is a
  substitute for the `AxisEndLag` alarm above.
- **A single cross-collection view.** Two deployments render two dashboards
  from one block of CDK. A combined view is what the Grafana `$collection`
  variable is for.
- **Dashboards-as-JSON.** The dashboard is code in the stack, not a checked-in
  JSON file. The Grafana plan's `monitoring/grafana/tempo-pipeline.json` is a
  separate artifact for a separate frontend.
- **Cost attribution.** See Band 5.
- **Alerting on throttles.** Expected at concurrency 1.

## Assumptions / ceilings

- **Free tier is 3 dashboards, then $3/month each.** Two collections fit free.
- **`DashboardBody` is a synth-time JSON blob.** Widget content that depends on
  a resource attribute renders as a CloudFormation ref inside that string,
  which is why the gating tests resolve `Fn::Join` before asserting.
- **Log-query widgets are region- and account-local.** Fine here by
  construction; it is the property that makes the Grafana route need a
  cross-account role and this one not.
- **No collapsible bands.** `TextWidget` headers and ordering are the
  substitute. If the dashboard grows past roughly 20 widgets this gets
  unwieldy and the answer is a second dashboard (`-backfill`), not nesting.
- **`sparkline=True` needs a single metric per `SingleValueWidget`.** The Band 1
  tiles are one metric each by design; adding a second metric to a tile
  silently drops the sparkline.
