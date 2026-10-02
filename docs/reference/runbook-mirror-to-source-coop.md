# Runbook: mirror a store to Source Cooperative

Use this to publish (or refresh) a collection's Icechunk store on Source
Cooperative. `scripts/mirror_to_source_coop.py` streams every object
under the store prefix to
`s3://us-west-2.opendata.source.coop/pangeo/tempo-virtual-icechunk/`,
building a zip of them on the way that it uploads beside them
(details in [README → Publishing to Source Cooperative](https://github.com/virtual-zarr/tempo-virtual-zarr-pipeline/blob/main/README.md#publishing-to-source-cooperative)).

The stores, from the tracked env files:

| collection | env file    | source                                            | destination (under `pangeo/tempo-virtual-icechunk/`) |
|------------|-------------|---------------------------------------------------|------------------------------------------------------|
| NO2        | `.env_no2`  | `s3://airquality-data-store-develop/tempo/no2/v04/`  | `tempo/no2/v04/` and `tempo/no2/v04.zip`             |
| HCHO       | `.env_hcho` | `s3://airquality-data-store-develop/tempo/hcho/v04/` | `tempo/hcho/v04/` and `tempo/hcho/v04.zip`           |

Run it from a terminal on the VEDA JupyterHub. The hub is in us-west-2
with the store bucket, so reads stay in-region; writes go through Source
Coop's proxy at `https://data.source.coop`, which stores them in its
us-west-2 bucket. Both sides
use short-lived credentials scoped to this job: the source side an SSO
permission set that can only read the store, the destination side the
keys Source Coop issued.

The script is not idempotent and never deletes: every run copies
everything again and overwrites what is there. That is fine; it just
costs time.

## Step 0 — once: a read-only permission set for the store

Ask an Identity Center administrator for a permission set in the
account that owns `airquality-data-store-develop` (say
`TempoStoreReader`) with this inline policy and a session duration of
12 hours, assigned to whoever runs the mirror. It is exactly the read
half of what the stack grants its own Lambdas
(`cdk/stack_constructs/grants.py`): listing and reading under
`tempo/`, which covers both collections and nothing else in the
account.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::airquality-data-store-develop/tempo/*"
    },
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::airquality-data-store-develop",
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
copy needs room for about the store's size. Store size, after Step 2's
login:

```bash
aws s3 ls s3://airquality-data-store-develop/tempo/no2/v04/ --recursive --summarize | tail -2
df -h ~
```

## Step 2 — credentials

**Source.** Log in with the scoped permission set. The hub pod already
carries a role of its own in the environment; `AWS_PROFILE` takes
precedence over it in boto3's credential chain, so the script and the
CLI both use the SSO session.

```bash
aws configure sso --profile tempo-reader   # SSO start URL, the store's account, TempoStoreReader, us-west-2
export AWS_PROFILE=tempo-reader
aws sts get-caller-identity               # ...assumed-role/AWSReservedSSO_TempoStoreReader_.../<you>
aws s3 ls s3://airquality-data-store-develop/tempo/no2/v04/ | head -3   # repo, config.yaml, chunks/
```

The device-code login prints a URL and a code; open it in your laptop's
browser. The session lasts the permission set's duration and the CLI
refreshes credentials within it on its own.

**Destination.** Put the temporary keys Source Coop issued for the
repository in the gitignored `.env.local` (template: `.env.local.sample`).
They are Source Coop keys, not AWS ones, valid only at its proxy, which
is where the script sends writes; all three are required:

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
uv run --env-file .env_no2  --env-file .env.local scripts/mirror_to_source_coop.py
uv run --env-file .env_hcho --env-file .env.local scripts/mirror_to_source_coop.py
```

The env file supplies `ICECHUNK_BUCKET`, `S3_PREFIX` and
`ICECHUNK_PREFIX`; nothing in it needs editing. Each run prints the
object count when the copy starts, `uploading` when the zip goes up, and
`done:` at the end.

## Step 4 — verify

Count objects on both sides; the destination should have the source's
count plus one (the zip). For NO2 (swap `no2` for `hcho`):

```bash
aws s3 ls s3://airquality-data-store-develop/tempo/no2/v04/ --recursive --summarize | tail -2
aws s3 ls --no-sign-request \
  s3://us-west-2.opendata.source.coop/pangeo/tempo-virtual-icechunk/tempo/no2/v04/ \
  --recursive --summarize | tail -2
aws s3 ls --no-sign-request \
  s3://us-west-2.opendata.source.coop/pangeo/tempo-virtual-icechunk/tempo/no2/v04.zip
```

Then open the public copy as a reader would (metadata only; no Earthdata
credentials needed for that):

```bash
uv run python -c "
import icechunk, zarr
repo = icechunk.Repository.open(icechunk.s3_storage(
    bucket='us-west-2.opendata.source.coop',
    prefix='pangeo/tempo-virtual-icechunk/tempo/no2/v04',
    region='us-west-2', anonymous=True))
print(zarr.open_group(repo.readonly_session('main').store, mode='r').tree())
"
```

### Verify the Icechunk store

The public copy should be the same repository as the source, at the same
version. Compare the snapshot `main` points at on each side and the
length of its history; a run that stopped partway, or a source that
moved on since the copy, shows up as a mismatch:

```bash
uv run --env-file .env_no2 python -c "
import icechunk, os
from virtualizarr_processor.manifest import storage_prefix
p = storage_prefix()
sides = {
    'source': icechunk.s3_storage(bucket=os.environ['ICECHUNK_BUCKET'], prefix=p,
                                  region='us-west-2', from_env=True),
    'public': icechunk.s3_storage(bucket='us-west-2.opendata.source.coop',
                                  prefix=f'pangeo/tempo-virtual-icechunk/{p}',
                                  region='us-west-2', anonymous=True),
}
seen = set()
for name, storage in sides.items():
    repo = icechunk.Repository.open(storage)
    main = repo.lookup_branch('main')
    n = len(list(repo.ancestry(branch='main')))
    print(f'{name}: main={main} snapshots={n}')
    seen.add((main, n))
print('OK' if len(seen) == 1 else 'MISMATCH')
"
```

Then read it the way a user would. `check_virtual_containers.py`, pointed
at the public copy anonymously, confirms the store declares its virtual
chunk containers, covers every manifest URL with them, and reads a chunk
back through them. That chunk read needs Earthdata credentials in the
environment (EARTHDATA_TOKEN or username/password; see the script's
docstring):

```bash
uv run --env-file .env_no2 python scripts/check_virtual_containers.py \
  --bucket us-west-2.opendata.source.coop \
  --prefix pangeo/tempo-virtual-icechunk/tempo/no2/v04 \
  --region us-west-2 --anonymous
```

Never pass `--fix` here; the public copy is written only by the mirror.
To fix a container, fix the source store and mirror again.

Finally, look at it. `compare_to_gibs.py` renders a scan of the public
copy beside the GIBS image Worldview shows for the same scan (the imagery
https://tempo.si.edu/data_for_public.html links to), with a per-pixel
difference panel; its docstring says what a good result looks like. It
reads chunks, so it needs the same Earthdata credentials:

```bash
uv run scripts/compare_to_gibs.py --collection no2    # writes gibs-no2-<scan>.png
uv run scripts/compare_to_gibs.py --collection hcho --time 2026-10-01T18:30   # the scan Worldview shows at 18:30
```

### Verify the zip

Check the zip from the local copy at `stores/<prefix>.zip` (it is
what was uploaded). No script checks the zip itself, but both store
checks open a local directory when `ICECHUNK_BUCKET` is empty and
`ICECHUNK_LOCAL_PATH` is set. Clear the bucket with `env` inside
`uv run`, so the env file cannot set it again:

```bash
unzip -tq stores/tempo/no2/v04.zip                      # every entry's CRC
unzip -l stores/tempo/no2/v04.zip | tail -1             # file count = source object count
unzip -q stores/tempo/no2/v04.zip -d /tmp/v04
uv run --env-file .env_no2 env ICECHUNK_BUCKET= ICECHUNK_LOCAL_PATH=/tmp/v04 \
  python scripts/check_virtual_containers.py
uv run --env-file .env_no2 env ICECHUNK_BUCKET= ICECHUNK_LOCAL_PATH=/tmp/v04 \
  python scripts/verify_store.py --samples 8
rm -rf /tmp/v04
```

`check_virtual_containers.py` confirms the store declares its virtual
chunk containers, covers every manifest URL with them, and reads a chunk
back; `verify_store.py` compares sampled time steps against CMR and the
source granules. Both read granule bytes, so export EARTHDATA_TOKEN (or
EARTHDATA_USERNAME and EARTHDATA_PASSWORD) first. Without it they fall
back to the hub's own AWS role, which `asdc-prod-protected` refuses with
`AccessDenied`. Neither compares the zip's file list
against the source prefix, so the count from `unzip -l` is the only check
of that.

## Step 5 — clean up

The home volume persists between sessions, so leave nothing behind:

```bash
rm -rf stores/ .env.local
aws sso logout                             # drops the cached SSO token
```

The zip holds nothing that is not already in both buckets.
