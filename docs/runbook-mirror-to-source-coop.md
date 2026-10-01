# Runbook: mirror a store to Source Cooperative

Use this to publish (or refresh) a collection's Icechunk store on Source
Cooperative. `scripts/mirror_to_source_coop.py` streams every object
under the store prefix to
`s3://us-west-2.opendata.source.coop/pangeo/tempo-virtual-icechunk/`,
building a zip of them on the way that it uploads beside them
(details in [README → Publishing to Source Cooperative](../README.md#publishing-to-source-cooperative)).

Run it from a terminal on the VEDA JupyterHub. The hub is in us-west-2,
where the store bucket and Source Coop's bucket both live, so the store
streams through the pod in-region (fast, no egress charge). Both sides
use short-lived credentials scoped to this job: the source side an SSO
permission set that can only read the store, the destination side the
keys Source Coop issued.

The script is not idempotent and never deletes: every run copies
everything again and overwrites what is there. That is fine; it just
costs time.

## Step 0 — once: a read-only permission set for the store

Ask an Identity Center administrator for a permission set in the
pipeline account (say `TempoStoreReader`) with this inline policy and a
session duration of 12 hours, assigned to whoever runs the mirror. It is
exactly the read half of what the stack grants its own Lambdas
(`cdk/stack_constructs/grants.py`): listing and reading under the
collection prefixes, nothing else in the account.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::<ICECHUNK_BUCKET>/tempo/*"
    },
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::<ICECHUNK_BUCKET>",
      "Condition": {"StringLike": {"s3:prefix": "tempo/*"}}
    }
  ]
}
```

Without it, the fallback is to log in with a broader permission set and
assume a role carrying the same policy (`aws sts assume-role --role-arn
... --policy file://scoped.json`), exporting the three keys it returns.
Those are capped at one hour by role chaining, and the script does not
refresh them, so the whole copy of objects (everything but the final zip
upload) has to finish inside the hour. Prefer the permission set.

## Step 1 — set up on the hub

In a hub terminal, with the Python (Pangeo) image:

```bash
git clone <this repository> && cd tempo-virtual-zarr-pipeline
uv sync                                    # curl -LsSf https://astral.sh/uv/install.sh | sh if missing
aws --version                              # must be v2 for SSO login
```

Check free space in the home volume: only the zip lands on disk, so the
copy needs room for about the store's size. Store size, after Step 2:

```bash
PREFIX=$(uv run python -c \
  "from virtualizarr_processor.manifest import storage_prefix; print(storage_prefix())")
aws s3 ls "s3://$ICECHUNK_BUCKET/$PREFIX/" --recursive --summarize | tail -2
df -h ~
```

(`PREFIX` is `S3_PREFIX/ICECHUNK_PREFIX` joined the way the stack joins
them; either may be unset.)

## Step 2 — credentials

**Source.** Log in with the scoped permission set. The hub pod already
carries a role of its own in the environment; `AWS_PROFILE` takes
precedence over it in boto3's credential chain, so the script and the
CLI both use the SSO session.

```bash
aws configure sso --profile tempo-reader   # SSO start URL, pipeline account, TempoStoreReader, us-west-2
export AWS_PROFILE=tempo-reader
set -a; source .env_no2; set +a            # or .env_hcho
aws sts get-caller-identity               # ...assumed-role/AWSReservedSSO_TempoStoreReader_.../<you>
```

The device-code login prints a URL and a code; open it in your laptop's
browser. The session lasts the permission set's duration and the CLI
refreshes credentials within it on its own.

**Destination.** Put the temporary keys Source Coop issued for the
repository in the gitignored `.env.local` (template: `.env.local.sample`):

```
SOURCE_COOP_ACCESS_KEY_ID=...
SOURCE_COOP_SECRET_ACCESS_KEY=...
SOURCE_COOP_SESSION_TOKEN=...
```

They are used from the first object on, and last for the zip upload at
the end.

## Step 3 — run

In `tmux`, so a closed browser tab does not kill the copy (a culled pod
still will; keep the tab open or start early in the day):

```bash
tmux new -s mirror
uv run --env-file .env_no2 --env-file .env.local scripts/mirror_to_source_coop.py
```

It prints the object count when the copy starts, `uploading` when the
zip goes up, and `done:` at the end. Repeat for the other
collection with `.env_hcho`.

## Step 4 — verify

Count objects on both sides; the destination should have the source's
count plus one (the zip):

```bash
aws s3 ls "s3://$ICECHUNK_BUCKET/$PREFIX/" --recursive --summarize | tail -2
aws s3 ls --no-sign-request \
  "s3://us-west-2.opendata.source.coop/pangeo/tempo-virtual-icechunk/$PREFIX/" \
  --recursive --summarize | tail -2
aws s3 ls --no-sign-request \
  "s3://us-west-2.opendata.source.coop/pangeo/tempo-virtual-icechunk/$PREFIX.zip"
```

Then open the public copy as a reader would (metadata only; no Earthdata
credentials needed for that):

```bash
uv run python -c "
import icechunk, zarr
repo = icechunk.Repository.open(icechunk.s3_storage(
    bucket='us-west-2.opendata.source.coop',
    prefix='pangeo/tempo-virtual-icechunk/$PREFIX',
    region='us-west-2', anonymous=True))
print(zarr.open_group(repo.readonly_session('main').store, mode='r').tree())
"
```

## Step 5 — clean up

The home volume persists between sessions, so leave nothing behind:

```bash
rm -rf stores/ .env.local
aws sso logout                             # drops the cached SSO token
```

The zip holds nothing that is not already in both buckets.
