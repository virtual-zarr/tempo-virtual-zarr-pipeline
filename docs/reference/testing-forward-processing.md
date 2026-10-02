# Testing forward processing (small)

The consumer and the poller are gated separately: `FORWARD_QUEUE_ENABLED=true`
enables the SQS→consumer mapping, while the poller and its schedule are
created only when `POLL_SCHEDULE_MINUTES` is non-zero — and it **defaults to
30**, so set `POLL_SCHEDULE_MINUTES=0` explicitly or the poller comes up with
forward processing and (without `POLL_START_ISO`) floods the queue from its
8-day first-poll lookback. Deploying with the consumer on and the poller off
(`FORWARD_QUEUE_ENABLED=true`, `POLL_SCHEDULE_MINUTES=0`, redeploy) lets you
feed the queue one hand-sent message at a time — the routing is idempotent,
so a duplicate or re-sent message is harmless.

The test deployment's backfill was run with `run_codebuild.sh -m 5`, so the
store holds the five most recent granules as of the inventory build — with
hourly TEMPO scans, an axis a few hours long. That makes every routing
outcome easy to trigger: almost everything in CMR is older than the store
(the defer case), and a fresh append candidate is published within the hour.
First confirm what the store holds:

```bash
aws s3 cp "s3://$ICECHUNK_BUCKET/tempo/hcho/inventory/hcho.json" - \
  | jq -r '.granules[] | "\(.granule_ur) \(.url)"'
```

then list CMR's most recent granules for the collection (`concept_id` is in
`lambda/virtualizarr-processor/virtualizarr_processor/collections/<collection>.toml`;
metadata needs no Earthdata credentials):

```bash
curl -s "https://cmr.earthdata.nasa.gov/search/granules.umm_json?collection_concept_id=C3685897141-LARC_CLOUD&sort_key=-start_date&page_size=10" \
  | jq -r '.items[].umm | .GranuleUR + " " + (.RelatedUrls[] | select(.Type=="GET DATA VIA DIRECT ACCESS" and (.URL|endswith(".nc"))) | .URL)'
```

Pick the test case by where the granule falls relative to the store. For the
inventory built 2026-08-24 19:04 UTC — `S002`–`S006` (11:40–14:40 UTC) of
2026-08-24 — the shortest full pass was (urls abbreviated to the granule
file, all under
`s3://asdc-prod-protected/TEMPO/TEMPO_HCHO_L3_V04/<YYYY.MM.DD>/`):

| Message url | Why | Expected consumer outcome |
|---|---|---|
| `TEMPO_HCHO_L3_V04_20260824T154044Z_S007.nc` | first scan after the newest slot (`S006`) | `APPENDED` — appended to the axis |
| `TEMPO_HCHO_L3_V04_20260824T144044Z_S006.nc` (send twice) | newest slot itself, same UR | first `OVERWRITTEN` — the slot's stamp was unknown, refreshed in place and stamped; second `UNCHANGED` — equal stamp, consumed without a parse, write, or commit |
| `TEMPO_HCHO_L3_V04_20260824T110012Z_S001.nc` | before the oldest slot (`S002`) | `DEFERRED` — pending ledger; the re-sort job folds it in later |

Send appends oldest-first (`S007` before `S008`): an append lands only past
the axis end, so a skipped-then-sent scan defers instead. Use CMR's `s3://`
url form — it is what the poller enqueues — even where a backfill
inventory recorded EDL HTTPS urls; the consumer resolves either. The queue
is named `<stack>-queue`, the message shape is the poller's:

```bash
aws sqs send-message \
  --queue-url "$(aws sqs get-queue-url --queue-name "$STACK_NAME-queue" --query QueueUrl --output text)" \
  --message-body '{"url": "s3://asdc-prod-protected/TEMPO/TEMPO_HCHO_L3_V04/2026.08.24/TEMPO_HCHO_L3_V04_20260824T154044Z_S007.nc"}'
```

If nothing seems to happen, first confirm the consumer's event-source
mapping is actually on (it prints `Enabled`; in the console this is the
Lambda's Configuration → Triggers):

```bash
aws lambda list-event-source-mappings \
  --query 'EventSourceMappings[?contains(FunctionArn, `processmessages`)].State' --output text
```

Then watch the consumer's log for the outcome (logged lowercase:
`appended` / `overwritten` / `deferred`; a `rejected` granule retries and
lands in `<stack>-Dlq`, which should stay empty). In the console: the function's Monitor tab → View CloudWatch logs →
Live Tail; the queue's own Monitoring tab graphs messages waiting/in flight.

```bash
aws logs tail "$(aws lambda list-functions \
  --query 'Functions[?contains(FunctionName, `processmessages`)].LoggingConfig.LogGroup | [0]' \
  --output text)" --follow
```

The store's axis and manifest are native icechunk data, readable from a
laptop (only virtual-chunk reads are region-locked), so the result is one
snippet away — an append grows the slot count and changes the newest
granule; a deferred granule changes neither and shows up in the pending
ledger instead:

```bash
uv run --env-file .env_hcho --env-file .env.local python -c "
import zarr
from virtualizarr_processor.processor import Processor
from virtualizarr_processor.manifest import StoreManifest
store = Processor().open_backfill_repo().readonly_session('main').store
print(zarr.open_array(store, path='time').shape[0], 'slots; newest:',
      StoreManifest.read(store).granules[-1].granule_ur)
"
```

Close the loop with `scripts/run_codebuild.sh -e .env_hcho -V`: an appended
granule becomes sampleable, and `-a "--completeness"` shows a deferred
granule sitting in the pending ledger.

To test the ledger's other half — the fold — don't wait out
`RESORT_SCHEDULE_HOURS`: the re-sort handler ignores its event payload, so
invoke it directly (reserved concurrency 1 makes this safe against the
schedule; avoid folding while an append is in flight — the mid-fold append
correctly fails the promote's compare-and-swap):

```bash
aws lambda invoke --cli-binary-format raw-in-base64-out --payload '{}' \
  --function-name "$(aws lambda list-functions \
    --query 'Functions[?contains(FunctionName, `resortlambda`)].FunctionName | [0]' --output text)" \
  /dev/stdout
```

Afterwards the ledger is empty, the slot count grew, and the deferred
granule sits at its correct axis position — the relocation of every
already-ingested slot behind it is the part of the pipeline nothing else
exercises. Verify with samples ≥ the slot count
(`-a "--samples 8 --completeness"`) to check every slot's bytes, relocated
ones included, against CMR. Once this works, enabling the poller
for real is step 4 of [Running a backfill](deploying.md#running-a-backfill-hcho-shown) — drop `POLL_SCHEDULE_MINUTES=0` (restoring the
30-minute default) and set `POLL_START_ISO` to a recent time first, so the
first poll enqueues a handful of granules, not the full 8-day lookback.
