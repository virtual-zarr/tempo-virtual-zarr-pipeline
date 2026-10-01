# Runbook: mirror a store to Source Cooperative

Use this to publish (or refresh) a collection's Icechunk store on Source
Cooperative. `scripts/mirror_to_source_coop.py` downloads every object
under the store prefix, zips the directory, and uploads both to
`s3://us-west-2.opendata.source.coop/pangeo/tempo-virtual-icechunk/`
(details in [README → Publishing to Source Cooperative](../README.md#publishing-to-source-cooperative)).
The store streams through the machine running the script, so run it in
**us-west-2**, where the store bucket and Source Coop's bucket both live:
transfers stay in-region (fast, no egress charge) and the copy fits in a
few hours instead of a day.

The script is not idempotent and never deletes: every run copies
everything again and overwrites what is there. That is fine; it just
costs time.

## Step 0 — size the job

From anywhere with read access to the store:

```bash
export AWS_PROFILE=<profile>
set -a; source .env_no2; set +a            # or .env_hcho
PREFIX=$(uv run python -c \
  "from virtualizarr_processor.manifest import storage_prefix; print(storage_prefix())")
aws s3 ls "s3://$ICECHUNK_BUCKET/$PREFIX/" --recursive --summarize | tail -2
```

(`PREFIX` is `S3_PREFIX/ICECHUNK_PREFIX` joined the way the stack joins
them; either may be unset.)

The machine needs free disk for twice that total (the directory plus the
zip).

## Step 1 — a machine in us-west-2

Launch an EC2 instance in us-west-2 (any current general-purpose type;
network bandwidth matters more than CPU) with:

- an instance role allowing `s3:ListBucket` and `s3:GetObject` on the
  store bucket (the stack's reader policy, or any role you already use to
  read the store);
- a volume with the free space from Step 0;
- Session Manager or SSH access.

Then install the project on it:

```bash
sudo dnf install -y git                    # Amazon Linux; apt on Ubuntu
curl -LsSf https://astral.sh/uv/install.sh | sh && source ~/.bashrc
git clone <this repository> && cd tempo-virtual-zarr-pipeline
uv sync
```

## Step 2 — credentials

Source Coop writes need the keys it issued for the repository (Source
Coop → the repository → *Manage* → *API keys*). Put them in the
gitignored `.env.local` (template: `.env.local.sample`):

```
SOURCE_COOP_ACCESS_KEY_ID=...
SOURCE_COOP_SECRET_ACCESS_KEY=...
```

Source reads use the instance role; nothing else to configure.

## Step 3 — run

In `tmux` (or `nohup`), so a dropped session does not kill the copy:

```bash
tmux new -s mirror
uv run --env-file .env_no2 --env-file .env.local scripts/mirror_to_source_coop.py
```

It prints the object count at the start of each of its three steps
(download, zip, upload) and `done:` at the end. Repeat for the other
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

Terminate the instance (or `rm -rf stores/`). The local copy holds
nothing that is not already in both buckets.
