# Deploying and running

> **What runs where.** Anything that reads source granules must run in
> us-west-2. The DAAC's temporary S3 credentials reject requests from
> anywhere else, so laptop runs fail every read with 403 regardless of
> Earthdata credentials. `scripts/run_codebuild.sh` is the standard way to
> run in-region: it ships the committed repo to the stack's CodeBuild
> project and runs the inventory build (the default, with `-m N` for
> trials), stamp recording (`-R`), or store verification (`-V`), then
> prints the build log tail. `-n` dry-runs the launch and shows the
> account, project, mode, and Earthdata wiring. Laptop-safe: `cdk deploy`,
> `start_backfill.sh`, queue operations, CMR queries, and store *metadata*
> reads (axis, manifest, ledger). Only virtual-chunk and source reads are
> region-locked.

## Env files

Each collection deploys as its own stack from a committed env file:
[`.env_hcho`](https://github.com/virtual-zarr/tempo-virtual-zarr-pipeline/blob/main/.env_hcho)
and
[`.env_no2`](https://github.com/virtual-zarr/tempo-virtual-zarr-pipeline/blob/main/.env_no2).
Both describe the current deployment: one shared bucket, `ICECHUNK_BUCKET`,
holds both stores under `tempo/hcho/v04` and `tempo/no2/v04`, with forward
processing and garbage collection on.

The committed files hold only dataset config. Account- and operator-specific
values (`ACCOUNT_ID`, `AWS_PROFILE`, `OWNER`, `EARTHDATA_SECRET_ARN`,
`VPC_ID`, ...) go in a gitignored `.env.local` shared by both collections.
Copy
[`.env.local.sample`](https://github.com/virtual-zarr/tempo-virtual-zarr-pipeline/blob/main/.env.local.sample)
and fill it in. Pass both files to every command, `.env.local` last so it
can also override dataset settings for a local run:

```bash
uv run --env-file .env_hcho --env-file .env.local cdk deploy
```

A pre-commit hook rejects commits that put a value for one of the
local-only keys back into a tracked env file.

The per-collection `S3_PREFIX` (`tempo/hcho`, `tempo/no2`) keeps the stacks'
output separate in the shared bucket. Every IAM grant in a stack is scoped to
its own prefix, so neither stack's roles can touch the other's keys.

## Deploying a change

Review the synthesized template, then deploy. Repeat for the other
collection.

```bash
uv run --env-file .env_hcho --env-file .env.local cdk synth
uv run --env-file .env_hcho --env-file .env.local cdk deploy
```

Settings are baked into the Lambda environment at deploy time. Changing one
means a redeploy.

## Setting up a new account

Do this once per account. `<bucket>` is the shared store bucket, which must
be in us-west-2.

```bash
./scripts/setup.sh   # uv deps + Node and the cdk CLI, installed into the uv venv
cp .env.local.sample .env.local                        # then fill it in
aws sso login --profile <profile>                      # or however the profile authenticates
uv run --env-file .env_hcho --env-file .env.local cdk bootstrap aws://<ACCOUNT_ID>/us-west-2   # fresh account only
aws s3 mb s3://<bucket> --region us-west-2 --profile <profile>
aws secretsmanager create-secret --name tempo-earthdata \
  --secret-string '{"EARTHDATA_TOKEN":"<EDL token>"}' \
  --region us-west-2 --profile <profile>
```

Paste the ARN the last command returns into `EARTHDATA_SECRET_ARN` in
`.env.local`. Both collections share it. The secret is required unless the
account has a bucket-policy grant on `asdc-prod-protected`. Without either,
every worker granule read fails with AccessDenied. The deploy itself still
succeeds, which makes this an annoying failure to debug after the fact.

Check the account's Lambda concurrent-executions quota. Fresh sub-accounts
can start as low as 10, and the backfill fans out to
`BACKFILL_MAX_CONCURRENCY=50`. Request an increase or lower that setting.

Garbage collection runs in an AWS Batch cluster inside a VPC. Set `VPC_ID`
in `.env.local` before deploying with `GARBAGE_COLLECTION_FREQUENCY`.

`AWS_PROFILE` is set inside `.env.local`, so every `uv run --env-file ...`
command and `start_backfill.sh -e ...` targets the account without exporting
anything. The scripts read keys missing from the `-e` file out of
`.env.local` automatically.

## Running a backfill

A backfill fills a new store from the historical archive. Deploy it
backfill-first: with `BACKFILL_ENABLED=true` and `FORWARD_QUEUE_ENABLED`
unset, the consumer, poller, and re-sort job stay undeployed until the
backfill promotes.

The time axis is sized from the inventory, so a trial backfill of a few
granules needs its own store. Point `ICECHUNK_PREFIX` at a scratch prefix
such as `v04-trial` and cap the inventory with `-m N`. To graduate to the
full backfill, set `ICECHUNK_PREFIX` back to `v04` and redeploy **first**,
because the prefix is baked into the Lambda environment. Then rebuild the
inventory without `-m` and start the backfill from the new inventory. The
full header sweep of about 13.6k granules takes hours. The CodeBuild
project's 8-hour timeout is sized for it.

The steps, for hcho:

1. Deploy:

   ```bash
   uv run --env-file .env_hcho --env-file .env.local cdk deploy
   ```

2. Build and upload the inventory:

   ```bash
   ./scripts/run_codebuild.sh -e .env_hcho -m 50
   ```

   `-m 50` is a trial cap. Drop it for the full run.

   This runs the committed `build_backfill_inventory.py` inside the stack's
   CodeBuild project, because the DAAC's temporary S3 credentials only work
   from us-west-2. The Earthdata token comes from the stack's
   `EARTHDATA_SECRET_ARN`. The inventory lands at
   `s3://<bucket>/<INVENTORY_PREFIX>/hcho.json`, the only prefix the
   partition Lambda may read.

   On a us-west-2 machine, running
   `uv run --env-file .env_hcho --env-file .env.local scripts/build_backfill_inventory.py ...`
   directly also works, with Earthdata credentials from `~/.netrc` or
   `$EARTHDATA_TOKEN`.

3. Start the backfill:

   ```bash
   ./scripts/start_backfill.sh -e .env_hcho s3://<bucket>/tempo/hcho/inventory/hcho.json
   ```

   The execution name defaults to `<stack>-backfill-<UTC timestamp>`. Pass
   one explicitly as an extra argument before the URI if you want a
   memorable name. A FAILED run can be rerun with `start_backfill.sh -f`.
   The fresh timestamp satisfies Step Functions' 90-day execution-name
   uniqueness, and `-f` resets the leftover `backfill` branch. Only pass
   `-f` once you have confirmed no execution is still RUNNING. Init refuses
   to reset a live run's branch out from under it.

4. When the backfill has promoted, set `FORWARD_QUEUE_ENABLED=true` and
   `POLL_START_ISO` to the inventory's build time in `.env_hcho`, then
   redeploy. If you also turn `BACKFILL_ENABLED` off, set `STORE_EXISTS=true`
   so the stack skips the deploy-time initialize Lambda. The poller's first
   poll then picks up granules published while the backfill ran, and the
   re-sort job folds in anything that arrived out of order. To smoke-test the
   consumer with hand-sent messages before turning the poller on, see
   [Testing forward processing](testing-forward-processing.md).

5. Verify after the promote, and periodically after that:

   ```bash
   ./scripts/run_codebuild.sh -e .env_hcho -V -a "--completeness"
   ```

   This starts the stack's CodeBuild project with a verify buildspec
   override. `--completeness` adds the CMR diff. Verification must run
   in-region, because the `s3://` source reads use the DAAC's region-locked
   temporary credentials.

Then repeat with `.env_no2` for the second stack.

## Teardown

```bash
uv run --env-file .env_hcho --env-file .env.local cdk destroy
uv run --env-file .env_no2  --env-file .env.local cdk destroy
```

The shared bucket is not stack-owned. Empty and delete it separately. For a
client deployment, also set the `CLIENT` tag in `.env.local` and
`STAGE=prod` in the env files.
