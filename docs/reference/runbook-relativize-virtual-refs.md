# Runbook: relativize a store's virtual references

Use this once for each store that was built before references were written
relative to the virtual chunk container. These stores hold absolute
references, such as `s3://asdc-prod-protected/TEMPO/...`. Icechunk resolves
an absolute reference only through a container with that exact prefix. So
the store can be read only from us-west-2. After the migration the
references have the form `vcc://asdc/TEMPO/...`. A reader can then point
the container named `asdc` at the HTTPS distribution and read the store
from anywhere. See [The virtual stores](stores.md).

`scripts/relativize_refs.py` does the rewrite. For each granule in the
store manifest it parses the source file again, as a backfill worker does,
and writes the references into the slot the granule already has. The time
axis, the manifest, the pending ledger, the stamps and the commit history
do not change. The script commits one batch of slots at a time. A stopped
run continues from the last commit. Rewriting a slot a second time is
harmless.

The source reads need the DAAC's temporary credentials, which work only in
us-west-2. So the script runs in the stack's CodeBuild project, with
`run_codebuild.sh -R`. A full collection has about 13.6k granules. Parsing
them on an 8-core instance takes hours. The build has an 8-hour timeout. If
the build times out, start it again and the script continues.

All commands assume the collection's env, for example:

```bash
export AWS_PROFILE=<profile>   # or rely on .env.local via uv run
ENV=.env_no2                   # repeat the whole runbook for the other stack
STACK_NAME=tempo-no2
```

## Step 0 — name the container in the store's config

This step is safe to run from a laptop. It changes only the store's
`config.yaml`. The container in the store's config has no name yet, and
relative references need the name to resolve. The name does not affect
absolute references. So do this step before anything writes a relative
reference.

```bash
uv run --env-file $ENV --env-file .env.local scripts/check_virtual_containers.py --no-read
# FAIL ... no container is named 'asdc'
uv run --env-file $ENV --env-file .env.local scripts/check_virtual_containers.py --fix --no-read
uv run --env-file $ENV --env-file .env.local scripts/check_virtual_containers.py --no-read
# OK: containers cover every reference
```

## Step 1 — deploy

The deploy ships the processor code that writes relative references. It
also gives the CodeBuild project write access to the store.

```bash
uv run --env-file $ENV --env-file .env.local cdk deploy
```

From now on, forward processing writes relative references for new slots.
A store with both kinds of references reads fine, because the container
has both a name and a prefix.

## Step 2 — pause the writers

The script commits to `main`. If something else commits in between, the
script's current batch fails. Nothing partial is written, but the batch
has to run again. So disable the consumer's queue trigger and the re-sort
schedule. The poller can keep running. Its messages wait in the queue,
which keeps them for 14 days. The consumer processes them after it is
enabled again. The garbage collection job does not commit, so it can keep
running.

```bash
CONSUMER=$(aws lambda list-functions \
  --query "Functions[?contains(FunctionName, \`processmessageslambda\`) && contains(FunctionName, \`$STACK_NAME\`)].FunctionName | [0]" \
  --output text)
MAPPING=$(aws lambda list-event-source-mappings --function-name "$CONSUMER" \
  --query 'EventSourceMappings[0].UUID' --output text)
aws lambda update-event-source-mapping --uuid "$MAPPING" --no-enabled

RESORT_RULE=$(aws events list-rules \
  --query "Rules[?contains(Name, \`ResortSchedule\`) && contains(Name, \`$STACK_NAME\`)].Name | [0]" \
  --output text)
aws events disable-rule --name "$RESORT_RULE"
```

Wait until running invocations finish before you start the rewrite. The
mapping state goes from `Disabling` to `Disabled`. Check the re-sort
Lambda's log group for a run in progress. One run takes at most 15
minutes.

```bash
aws lambda get-event-source-mapping --uuid "$MAPPING" --query State --output text
```

## Step 3 — run the rewrite

```bash
./scripts/run_codebuild.sh -e $ENV -R
```

This uploads the committed repo, starts the build on a LARGE instance with
an 8-hour timeout, and prints the end of the build log when the build
finishes. The build logs one line per batch, such as
`committed slots [a, b): <snapshot>`. The first batches show the rate. The default batch is 100
slots. `-a "--batch 50"` halves the work lost when a batch fails.

If the build times out or fails, read the log. A granule that fails
validation fails its batch, and the log names the granule. Fix the cause
if there is one, then run the same command again. The script reads the
commit message at the branch tip and continues after the last committed
batch. If the tip commit came from something else, the script starts from
slot 0. That is correct, only slower.

The last lines of a successful run:

```
committed slots [13500, 13612): ...
OK: 13612 slots relative; named container persisted
```

## Step 4 — resume the writers

```bash
aws lambda update-event-source-mapping --uuid "$MAPPING" --enabled
aws events enable-rule --name "$RESORT_RULE"
```

The consumer then processes the messages the poller queued during the
migration.

## Step 5 — verify and republish

Run the in-region verification, as after a backfill:

```bash
./scripts/run_codebuild.sh -e $ENV -V
uv run --env-file $ENV --env-file .env.local scripts/check_virtual_containers.py --no-read
```

The copy on Source Cooperative is a copy of the store's objects. It keeps
the old references until you refresh it. See
[Mirror to Source Cooperative](runbook-mirror-to-source-coop.md). After the
refresh, run `check_virtual_containers.py` against the published copy. It
must report the `asdc` container.
