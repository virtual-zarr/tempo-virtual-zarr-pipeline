# Runbook: relativize a store's virtual references

Use this once for each store that was built before references were written
relative to the virtual chunk container. These stores hold absolute
references, such as `s3://asdc-prod-protected/TEMPO/...`. Icechunk resolves
an absolute reference only through a container with that exact prefix. So
the store can be read only from us-west-2. After the migration the
references have the form `vcc://asdc/TEMPO/...`. A reader can then point
the container named `asdc` at the HTTPS distribution and read the store
from anywhere. See [The virtual stores](stores.md).

`scripts/relativize_refs.py` does the rewrite. It reads every reference
back from the store through VirtualiZarr's `IcechunkParser`, rewrites the
location, and writes it into the slot it came from. It reads nothing from
the source bucket. So it runs from a laptop, with write access to the
store, in minutes. The time axis, the manifest, the pending ledger, the
stamps and the commit history do not change. The whole rewrite is one
commit. A stopped run leaves nothing behind. Run it again.

Icechunk stores a `last_updated_at` stamp with each reference. It does not
read it back. The rewrite writes each slot with the stamp the pipeline
recorded for it in the store's stamp array. The backfill never recorded
one, so `scripts/record_stamps.py` fills the unknown stamps first. It asks
each source for its last-modified time, one HEAD request per slot, which
is what the backfill stamped the slot with. A source modified after the
commit that wrote its slot has changed since ingest. Such a slot keeps an
unknown stamp and the script names it. The rewrite refuses a store with
unknown stamps. Both scripts commit once.

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

The deploy ships the processor code that writes relative references.

```bash
uv run --env-file $ENV --env-file .env.local cdk deploy
```

From now on, forward processing writes relative references for new slots.
A store with both kinds of references reads fine, because the container
has both a name and a prefix.

## Step 2 — pause the writers

The script commits to `main`. If something else commits in between, the
script's commit fails. Nothing partial is written, but the run has to
start over. So disable the consumer's queue trigger and the re-sort
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

## Step 3 — record the stamps

The HEAD requests need the DAAC's temporary credentials, which work only
in us-west-2. So this runs in the stack's CodeBuild project.

```bash
./scripts/run_codebuild.sh -e $ENV -R
```

The last line of a successful run:

```
OK: every stamp known
```

A run that ends in `FAIL: N sources changed after their slot was written`
names the granules. Their references are stale, and the rewrite refuses
the store until every stamp is known. Resume the writers (Step 5),
redeliver those granules to the consumer, and start again at Step 2.

## Step 4 — run the rewrite

From the laptop:

```bash
uv run --env-file $ENV --env-file .env.local scripts/relativize_refs.py
```

A full collection has about 13.6k slots. The script holds every reference
in memory, around a gigabyte for a full collection, and commits once. The
last line of a successful run:

```
OK: 13612 slots relative in <snapshot>; container persisted
```

If the run fails, read the error. A commit conflict means something else
committed to `main`. Check Step 2 and run the same command again.

## Step 5 — resume the writers

```bash
aws lambda update-event-source-mapping --uuid "$MAPPING" --enabled
aws events enable-rule --name "$RESORT_RULE"
```

The consumer then processes the messages the poller queued during the
migration.

## Step 6 — verify and republish

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
