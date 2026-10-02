# Deploying and running

> **What runs where.** Anything that reads source granules must run in
> us-west-2 — the DAAC's temporary S3 credentials reject requests from
> anywhere else, so laptop runs fail every read with 403 regardless of
> Earthdata credentials. `scripts/run_codebuild.sh` is the standard way to
> do that: it ships the committed repo to the stack's CodeBuild project and
> runs the inventory build (default; `-m N` for trials) or store
> verification (`-V`) in-region, printing the build log tail when done.
> `-n` dry-runs the whole launch (account, project, mode, Earthdata
> wiring). Laptop-safe: `cdk deploy`, `start_backfill.sh`, queue
> operations, CMR queries, and store *metadata* reads (axis, manifest,
> ledger) — only virtual-chunk and source reads are region-locked.

## Env files

Each collection deploys as its own stack from a committed env file:
[`.env_hcho`](https://github.com/virtual-zarr/tempo-virtual-zarr-pipeline/blob/main/.env_hcho) and [`.env_no2`](https://github.com/virtual-zarr/tempo-virtual-zarr-pipeline/blob/main/.env_no2). Both are currently
filled in for a test run in a sandbox sub-account (us-west-2).

The committed files hold only dataset config. Account- and operator-specific
values (`ACCOUNT_ID`, `AWS_PROFILE`, `OWNER`, `EARTHDATA_SECRET_ARN`, ...) go
in a gitignored `.env.local` shared by both collections — copy
[`.env.local.sample`](https://github.com/virtual-zarr/tempo-virtual-zarr-pipeline/blob/main/.env.local.sample) and fill it in. Pass both files to
every command, `.env.local` last so it can also override dataset settings for
a local run:

```bash
uv run --env-file .env_hcho --env-file .env.local cdk deploy
```

A pre-commit hook rejects commits that put a value for one of the local-only
keys back into a tracked env file.

Both collections share one bucket (`ICECHUNK_BUCKET`, in us-west-2, created
once with `aws s3 mb s3://<bucket> --region us-west-2 --profile <profile>`).
The per-collection `S3_PREFIX` (`tempo/hcho`, `tempo/no2`) keeps the stacks'
output separate, and every IAM grant in a stack is scoped to its own prefix,
so neither stack's roles can touch the other's keys. To deploy into a
different account, change `.env.local` and `ICECHUNK_BUCKET`.

Both env files ship backfill-first: forward processing (consumer, poller,
re-sort job) stays undeployed while the backfill runs.

## One-time sandbox setup

```bash
./scripts/setup.sh   # uv deps + Node and the cdk CLI, installed into the uv venv
cp .env.local.sample .env.local                        # then fill it in
aws sso login --profile <profile>                      # or however the profile authenticates
uv run --env-file .env_hcho --env-file .env.local cdk bootstrap aws://<ACCOUNT_ID>/us-west-2   # fresh account only
aws s3 mb s3://tempo-virtual-store-sandbox --region us-west-2 --profile <profile>
aws secretsmanager create-secret --name tempo-earthdata \
  --secret-string '{"EARTHDATA_TOKEN":"<EDL token>"}' \
  --region us-west-2 --profile <profile>
```

Paste the ARN the last command returns into `EARTHDATA_SECRET_ARN` in
`.env.local` (shared by both collections). The secret is required in the
sandbox: the account has no bucket-policy grant on `asdc-prod-protected`, so
without it every worker granule read fails with AccessDenied. The deploy
itself would still succeed, which makes this an annoying failure to debug
after the fact.

Also check the account's Lambda concurrent-executions quota. Fresh
sub-accounts can start as low as 10, and the backfill fans out to
`BACKFILL_MAX_CONCURRENCY=50`; request an increase or lower that setting.

`AWS_PROFILE` is set inside `.env.local`, so every `uv run --env-file ...`
command and `start_backfill.sh -e ...` targets the sandbox without exporting
anything (the scripts read keys missing from the `-e` file out of
`.env.local` automatically).

## Trial run vs. full backfill

The steps below are written for the first trial: a backfill of only the 50
most recent granules (`-m 50` on the inventory build) into a scratch store.
The time axis is sized from the inventory, so `.env_hcho` points at
`ICECHUNK_PREFIX=v04-trial` rather than the real `v04`.

To graduate to the full backfill: set `ICECHUNK_PREFIX=v04` and redeploy
**first** — the prefix is baked into the Lambda environment — then rebuild
the inventory without `-m` (the full ~13.6k-granule header sweep takes
hours; the project's 8 h timeout is sized for it) and start the backfill
from the new inventory.

## Running a backfill (hcho shown)

1. Deploy:

   ```bash
   uv run --env-file .env_hcho --env-file .env.local cdk deploy
   ```

2. Build and upload the inventory:

   ```bash
   ./scripts/run_codebuild.sh -e .env_hcho -m 50
   ```

   `-m 50` is the trial cap; drop it for the full run.

   This runs the committed `build_backfill_inventory.py` inside the stack's
   CodeBuild project, because the DAAC's temporary S3 credentials only work
   from us-west-2 — a laptop run with the default `--access direct` fails on
   every granule read. The Earthdata token comes from the stack's
   `EARTHDATA_SECRET_ARN`, and the inventory lands at
   `s3://<bucket>/<INVENTORY_PREFIX>/hcho.json`, the only prefix the
   partition Lambda may read.

   On a us-west-2 machine, running
   `uv run --env-file .env_hcho --env-file .env.local scripts/build_backfill_inventory.py ...`
   directly still works, with Earthdata credentials from `~/.netrc` or
   `$EARTHDATA_TOKEN`.

3. Start the backfill:

   ```bash
   ./scripts/start_backfill.sh -e .env_hcho s3://tempo-virtual-store-sandbox/tempo/hcho/inventory/hcho.json
   ```

   The execution name defaults to `<stack>-backfill-<UTC timestamp>`; pass
   one explicitly as an extra argument before the URI if you want a memorable
   name. A FAILED run can be rerun with `start_backfill.sh -f` — the fresh
   timestamp satisfies Step Functions' 90-day execution-name uniqueness, and
   `-f` resets the leftover `backfill` branch. Only pass `-f` once you've
   confirmed no execution is still RUNNING; Init refuses to reset a live
   run's branch out from under it.

4. When the backfill has promoted, set `FORWARD_QUEUE_ENABLED=true` and
   `POLL_START_ISO` to the inventory's build time in `.env_hcho`, then
   redeploy. The poller's first poll then picks up granules published while
   the backfill ran, and the re-sort job folds in anything that arrived out
   of order. To smoke-test the consumer with hand-sent messages before
   turning the poller on, see
   [Testing forward processing (small)](testing-forward-processing.md).

5. Run `scripts/run_codebuild.sh -e .env_hcho -V` after the promote, and
   periodically after that (add `-a "--completeness"` for the CMR diff); it
   starts the stack's CodeBuild project with a verify buildspec override.
   Verification must run in-region (CodeBuild or CloudShell in us-west-2):
   the s3:// source reads use the DAAC's region-locked temporary
   credentials, so a laptop run fails every slot with 403 PermissionDenied
   regardless of Earthdata credentials.

Then repeat with `.env_no2` for the second stack:

```bash
uv run --env-file .env_no2 --env-file .env.local cdk deploy
./scripts/run_codebuild.sh -e .env_no2
./scripts/start_backfill.sh -e .env_no2 \
  s3://tempo-virtual-store-sandbox/tempo/no2/inventory/no2.json
```

To trial the no2 stack the same way, add `-m 50` and set
`ICECHUNK_PREFIX=v04-trial` in `.env_no2` first — `.env_no2` ships pointed at
the real `v04`.

## Teardown

```bash
uv run --env-file .env_hcho --env-file .env.local cdk destroy
uv run --env-file .env_no2  --env-file .env.local cdk destroy
```

The shared bucket is not stack-owned; empty and delete it separately. For the
later client deployment, also set the `CLIENT` tag in `.env.local` and
`STAGE=prod` in the env files.
