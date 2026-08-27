# Runbook: drain a pending-ledger backlog

Use this when the scheduled re-sort job has been failing and the pending
ledger has grown to hundreds of granules, so the store is missing a large
fraction of known granules. Written from the 2026-08-27 incident: the
re-sort Lambda timed out nightly trying to fold the whole backlog in one
invocation — `RESORT_MAX_FOLD`'s default (500) does not fit inside Lambda's
15-minute ceiling once inserts dominate, because every *inserted* granule is
parsed from source (relocations are metadata-only and cheap).

Everything here is safe to repeat: each promoted run is durable partial
progress, a promote rejected by the compare-and-swap consumes nothing, and
the re-sort Lambda has reserved concurrency 1. The one rule: **never run two
folds at once** — invoke serially and let each call return before the next.

All commands assume the collection's env, e.g.:

```bash
export AWS_PROFILE=<profile>   # or rely on .env.local via uv run
STACK_NAME=tempo-hcho          # repeat the whole runbook for the other stack
```

## Step 0 — confirm the failure mode

Do not drain blind; the ledger-collision case needs a different fix.

```bash
LG=$(aws lambda list-functions \
  --query "Functions[?contains(FunctionName, \`resortlambda\`) && contains(FunctionName, \`$STACK_NAME\`)].LoggingConfig.LogGroup | [0]" \
  --output text)
aws logs tail "$LG" --since 26h --format short
```

- `Task timed out after 900 seconds` (or a `REPORT` with Duration ≈ 900 s)
  after a successful `Resorting` log line → **this runbook applies.**
- An exception out of `merge_pending` (same-time/different-UR collision
  between manifest and ledger) → **stop.** That is the operator case in
  [README → Recovery](../README.md#recovery): drop the offending ledger
  entry with a small Icechunk commit first.

Record the current state (laptop-safe; metadata reads are not
region-locked):

```bash
uv run --env-file .env_hcho --env-file .env.local python -c "
import zarr
from virtualizarr_processor.processor import Processor
from virtualizarr_processor.manifest import StoreManifest, PendingLedger
store = Processor().open_backfill_repo().readonly_session('main').store
print(zarr.open_array(store, path='time').shape[0], 'slots; newest:',
      StoreManifest.read(store).granules[-1].granule_ur)
print(len(PendingLedger.read(store)), 'pending')
"
```

## Step 1 — set a fold size that fits the timeout

Lambda's 15-minute ceiling cannot be raised; the fold size must come down.
Start at `RESORT_MAX_FOLD=25` (worked comfortably at trial-store scale — for
comparison, backfill workers take 10 granules per invocation). Set it in the
collection env file and redeploy — the value is baked into the Lambda env:

```bash
uv run --env-file .env_hcho --env-file .env.local cdk deploy
```

The drain loop in Step 2 doubles as the measurement for the *proper* value:
each run's handler JSON logs `folding`/`relocations` and the `REPORT` line
gives Duration and Max Memory Used. Fit
`duration(N) ≈ overhead + per_granule × N` from two run sizes and pick the
largest N under **~60 % of 900 s** (headroom for p95 source-read latency).
Watch Max Memory Used too: a deep re-sort builds the shifted suffix's
manifest updates in memory, and at full axis length memory can bind before
time does.

## Step 2 — drain, serially, until `remaining` is 0

The handler returns `{"resorted": true, "inserted": N, "remaining": M}`, so
the invoke response is the loop condition. Invoke synchronously;
`--cli-read-timeout 0` matters — the CLI's default 60 s socket timeout would
abandon (not stop) a running fold:

```bash
FN=$(aws lambda list-functions \
  --query "Functions[?contains(FunctionName, \`resortlambda\`) && contains(FunctionName, \`$STACK_NAME\`)].FunctionName | [0]" \
  --output text)

while :; do
  out=$(aws lambda invoke --cli-binary-format raw-in-base64-out \
    --cli-read-timeout 0 --payload '{}' \
    --function-name "$FN" /dev/stdout) || break
  echo "$out"
  echo "$out" | grep -q '"remaining": 0' && break
  echo "$out" | grep -q '"resorted"' || break   # error payload: stop, read the log
done
```

Expected behaviors mid-drain, none of which need intervention:

- **Promote rejected by compare-and-swap**: a consumer append landed while
  the fold ran (the poller is live every 30 min). Nothing was consumed —
  the invoke fails, just run it again. If it happens repeatedly, re-invoke
  right after a consumer commit lands rather than disabling the poller.
- **`{"resorted": false, "reason": "ledger empty"}`**: done.
- A timeout at the new size: halve `RESORT_MAX_FOLD`, redeploy, continue.

## Step 3 — verify

1. Re-run the Step 0 snippet: slots should equal old slots + old pending
   (minus anything newly appended), pending 0 or a handful of fresh
   arrivals.
2. Full verification in-region, with samples ≥ the slot count so relocated
   slots' bytes are checked against CMR too:

   ```bash
   ./scripts/run_codebuild.sh -e .env_hcho -V -a "--samples <slots> --completeness"
   ```

   `--completeness` should report every CMR granule in the manifest or
   ledger. A granule missing from both is a separate problem (e.g. one
   rejected to the DLQ) — this runbook does not fix that.

## Step 4 — keep it fixed

- Commit the measured `RESORT_MAX_FOLD` to the collection env files; the
  repo default should drop to the measured safe value (500 is unreachable
  inside Lambda's ceiling when inserts dominate).
- Watch the next two scheduled runs (`ResortErrorsAlarm` history, or the
  log group): they should promote and the ledger should stay near zero —
  hitting the cap every run means the cadence is falling behind the arrival
  rate, and the fold size, schedule, or runtime (Lambda → Batch) needs
  revisiting.
