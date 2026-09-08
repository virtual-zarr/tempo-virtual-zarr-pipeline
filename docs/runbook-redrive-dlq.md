# Runbook: triage and redrive the dead-letter queue

Use this when `<stack>-Dlq` is non-empty (the depth alarm fired, or you
noticed messages parked there). Written from the 2026-08-25→31 incident:
a manual backfill clearing left the store transiently unprocessable, so
a poll wave of ~2,400 perfectly good granules burned their 20 receives
and dead-lettered. The DLQ is terminal — SQS never retries out of it, and
messages expire after 14 days **measured from their original enqueue**
(moving to the DLQ does not reset the clock), so a stale backlog silently
self-purges. Triage promptly.

Everything here is safe to repeat: peeking uses `--visibility-timeout 0`
(consumes nothing), and the consumer's routing is idempotent — a redriven
granule already in the store re-resolves as `OVERWRITTEN`, an out-of-order
one defers to the pending ledger (deduped by granule UR), and anything
genuinely broken re-rejects back to the DLQ with a fresh retention clock.
The worst case of a redrive is ending up where you started.

All commands assume the collection's env, e.g.:

```bash
export AWS_PROFILE=<profile>
STACK_NAME=tempo-hcho          # repeat for the other stack — an incident
                               # that hit one collection often hit both
DLQ_URL=$(aws sqs get-queue-url --queue-name "${STACK_NAME}-Dlq" \
  --query QueueUrl --output text)
```

## Step 0 — stale backlog or live failure?

Depth over time answers it (queue names are case-sensitive: `-Dlq`):

```bash
aws cloudwatch get-metric-statistics --namespace AWS/SQS \
  --metric-name ApproximateNumberOfMessagesVisible \
  --dimensions Name=QueueName,Value="${STACK_NAME}-Dlq" \
  --start-time <14 days ago, ISO> --end-time <now, ISO> \
  --period 21600 --statistics Maximum \
  --query 'sort_by(Datapoints,&Timestamp)[].[Timestamp,Maximum]' --output text
```

(Don't use the DLQ's `NumberOfMessagesSent` — redrive-policy moves don't
increment it.)

- **Flat since a past incident window** → stale backlog, proceed to Step 1.
- **Still climbing** → **stop.** Something is failing right now; find the
  reason before recycling messages into it. Sample the bodies and grep the
  consumer's log for the granule (the `process_file:` error line states the
  rejection reason; `Commit failed` lines mean whole batches — good
  granules included — are cycling to the DLQ):

  ```bash
  aws sqs receive-message --queue-url "$DLQ_URL" --max-number-of-messages 10 \
    --visibility-timeout 0 --attribute-names SentTimestamp \
    | jq -r '.Messages[] | [(.Attributes.SentTimestamp|tonumber/1000|todate),
                            (.Body|fromjson|.url)] | @tsv'

  LG=$(aws lambda list-functions \
    --query "Functions[?contains(FunctionName, \`processmessages\`) && contains(FunctionName, \`$STACK_NAME\`)].LoggingConfig.LogGroup | [0]" \
    --output text)
  aws logs tail "$LG" --since 24h --format short | grep <granule scan id>
  ```

  `SentTimestamp` is the original enqueue time, so it dates the incident.
  Exact-duplicate urls are normal: the poller's 24 h overlap re-enqueues
  across polls, so distinct granules number well below the message count.

## Step 1 — redrive

Moves every message back to the queue it dead-lettered from, as brand-new
messages (receive count 0, fresh 14-day retention). Asynchronous;
`--max-number-of-messages-per-second` caps the pace if you want the
consumer to chew through it gradually:

```bash
DLQ_ARN=$(aws sqs get-queue-attributes --queue-url "$DLQ_URL" \
  --attribute-names QueueArn --query Attributes.QueueArn --output text)
aws sqs start-message-move-task --source-arn "$DLQ_ARN"
aws sqs list-message-move-tasks --source-arn "$DLQ_ARN"   # progress
```

Expected behaviors mid-redrive, none of which need intervention:

- The consumer logs floods of `written` (slot exists in the axis: appended
  or refreshed in place, including already-ingested duplicates) and
  `deferred` (historical granules headed for the pending ledger — the
  scheduled re-sort folds them in; see the drain runbook if it backs up).
- DLQ depth drops to ~0, then some messages trickle back over the next
  hour or two as genuine failures re-exhaust their 20 receives.

## Step 2 — triage the survivors

Whatever re-accumulates is the real problem set, current as of today.
Grep the consumer's log (as in Step 0) for each surviving granule: the
`process_file:` line names the cause — `validation failed` (traceback
adjacent), `refusing to overwrite` (a different granule claims the slot),
or `moved timestamp` (a republication whose nominal time shifted). Those
need an operator decision, not a redrive; see [README → Recovery](../README.md#recovery).

## Step 3 — verify

Completeness check against CMR — a granule in neither the manifest nor
the ledger was lost (likely expired out of the DLQ) and needs re-enqueueing
(re-poll with an earlier watermark, or a backfill):

```bash
./scripts/run_codebuild.sh -e .env_hcho -V -a "--completeness"
```

Repeat the runbook for the sibling stack's DLQ.
