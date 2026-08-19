# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "earthaccess>=0.14",
#     "virtualizarr[hdf,icechunk]>=2.7",
#     "obspec-utils",
#     "obstore",
#     "xarray",
# ]
# ///
"""Build a realistic S3-hosted virtual Icechunk store from in-region compute.

Intended to run on a JupyterHub in us-west-2 (e.g. the NASA VEDA/EODC hub) with
in-region S3 access. Virtualizes the N most recent granules (default 100) of the
selected collection (HCHO by default, ``--collection no2`` for NO2) by reading
directly from ``s3://asdc-prod-protected`` with temporary ASDC credentials,
concatenates them along ``time``, keeps only the smoke-test variables (the primary
column variable plus ``main_data_quality_flag``), flattens them from the ``product``
group to the root group with inherited coordinates, and commits the result to an
Icechunk repository at ``s3://<store-bucket>/icechunk/<concept-id>``. The persisted
repository config registers the S3 virtual chunk container WITHOUT credentials;
readers supply temporary ASDC credentials at open time. Finishes by reopening the
store read-only that way and loading a data sample through the virtual chunk
container.

The store bucket is written with ambient AWS credentials (``from_env``), so the
hub's role must allow writes to the target prefix. The ASDC temporary credentials
last one hour, which comfortably covers the default granule count.

With ``--concept-id`` the variable subset is still chosen by ``--collection``.

Requires Earthdata Login credentials in ~/.netrc.

Usage:
    uv run exploration/build_s3_test_store.py
    uv run exploration/build_s3_test_store.py --collection no2
"""

import argparse
import sys
from typing import Any

import earthaccess
import icechunk
import xarray as xr
from obspec_utils.registry import ObjectStoreRegistry
from obstore.store import S3Store
from tempo_collections import VARIABLES, add_collection_argument, resolve_concept_id
from tempo_virtual import (
    combine_trees,
    flatten_product_subset,
    manifest_totals,
    recent_granule_urls,
    virtualize_urls,
)

DATA_BUCKET = "asdc-prod-protected"
DATA_REGION = "us-west-2"
S3_CONTAINER_PREFIX = f"s3://{DATA_BUCKET}/"
DEFAULT_STORE_BUCKET = "nasa-eodc-scratch"
DEFAULT_N_GRANULES = 100
# In-region S3 is not behind the CloudFront rate limit, so parse with more workers
# than the HTTPS scripts.
S3_PARSE_WORKERS = 8


def asdc_s3_credentials() -> dict[str, Any]:
    """Temporary (1 hour) credentials for direct S3 access to ASDC buckets."""
    earthaccess.login(strategy="netrc")
    creds: dict[str, Any] = earthaccess.get_s3_credentials(daac="ASDC")
    missing = [
        key
        for key in ("accessKeyId", "secretAccessKey", "sessionToken")
        if not creds.get(key)
    ]
    if missing:
        raise RuntimeError(f"ASDC s3credentials response missing {missing}")
    return creds


def make_s3_registry(creds: dict[str, Any]) -> ObjectStoreRegistry:
    store = S3Store(
        DATA_BUCKET,
        region=DATA_REGION,
        access_key_id=creds["accessKeyId"],
        secret_access_key=creds["secretAccessKey"],
        token=creds["sessionToken"],
    )
    return ObjectStoreRegistry({f"s3://{DATA_BUCKET}": store})


def make_storage(bucket: str, prefix: str) -> icechunk.Storage:
    """Icechunk storage in the store bucket, authed by ambient AWS credentials."""
    return icechunk.s3_storage(
        bucket=bucket, prefix=prefix, region=DATA_REGION, from_env=True
    )


def create_repo(storage: icechunk.Storage) -> icechunk.Repository:
    """Create the repo, persisting a credential-less S3 container config."""
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            S3_CONTAINER_PREFIX, icechunk.s3_store(region=DATA_REGION)
        )
    )
    repo = icechunk.Repository.create(storage=storage, config=config)
    repo.save_config()
    return repo


def open_readonly_with_credentials(
    storage: icechunk.Storage, creds: dict[str, Any]
) -> xr.Dataset:
    """Open the store the way readers will: temporary ASDC credentials at open time."""
    repo = icechunk.Repository.open(
        storage=storage,
        authorize_virtual_chunk_access=icechunk.containers_credentials(
            {
                S3_CONTAINER_PREFIX: icechunk.s3_credentials(
                    access_key_id=creds["accessKeyId"],
                    secret_access_key=creds["secretAccessKey"],
                    session_token=creds["sessionToken"],
                )
            }
        ),
    )
    return xr.open_dataset(
        repo.readonly_session("main").store,
        engine="zarr",
        consolidated=False,
        zarr_format=3,
    )


def main() -> int:
    parser_cli = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser_cli.add_argument("--n-granules", type=int, default=DEFAULT_N_GRANULES)
    parser_cli.add_argument("--workers", type=int, default=S3_PARSE_WORKERS)
    parser_cli.add_argument(
        "--store-bucket",
        default=DEFAULT_STORE_BUCKET,
        help=f"Store output bucket (default {DEFAULT_STORE_BUCKET})",
    )
    parser_cli.add_argument(
        "--store-prefix",
        default=None,
        help="Store output prefix (default icechunk/<concept-id>)",
    )
    add_collection_argument(parser_cli)
    args = parser_cli.parse_args()
    concept_id = resolve_concept_id(args)
    variables = VARIABLES[args.collection]

    store_prefix = args.store_prefix or f"icechunk/{concept_id}"
    store_uri = f"s3://{args.store_bucket}/{store_prefix}"

    creds = asdc_s3_credentials()
    storage = make_storage(args.store_bucket, store_prefix)
    if icechunk.Repository.exists(storage):
        sys.exit(f"Repository already exists at {store_uri}; delete it to rebuild")

    print(f"Selecting the {args.n_granules} most recent granules of {concept_id}:")
    urls = recent_granule_urls(args.n_granules, concept_id, access="direct")
    outside = [u for u in urls if not u.startswith(S3_CONTAINER_PREFIX)]
    if outside:
        sys.exit(f"Direct links outside {S3_CONTAINER_PREFIX}: {outside[:3]}")
    print(f"  first: {urls[0].rsplit('/', 1)[-1]}")
    print(f"  last:  {urls[-1].rsplit('/', 1)[-1]}")

    registry = make_s3_registry(creds)
    print(f"\nVirtualizing {len(urls)} granules with {args.workers} workers:")
    trees = virtualize_urls(urls, registry, workers=args.workers)

    print("\nCombining and subsetting...")
    combined = combine_trees(trees)
    flat = flatten_product_subset(combined, variables)
    flat_tree = xr.DataTree(dataset=flat)
    chunks, total_bytes = manifest_totals(flat_tree)
    print(
        f"Flat dataset: {list(flat.data_vars)} referencing {chunks} chunks, "
        f"{total_bytes / 1e9:.2f} GB of source data"
    )

    print(f"\nWriting virtual references to {store_uri}...")
    repo = create_repo(storage)
    session = repo.writable_session("main")
    flat.vz.to_icechunk(session.store)
    snapshot = session.commit(
        f"Add {len(urls)} recent {concept_id} granules ({', '.join(variables)})"
    )
    print(f"Committed snapshot {snapshot}")

    print("\nReading back with reader-style temporary ASDC credentials:")
    ds = open_readonly_with_credentials(storage, creds)
    print(
        f"  time steps ({ds.sizes['time']}): "
        f"{ds['time'].values.min()} .. {ds['time'].values.max()}"
    )
    center = {"latitude": slice(1474, 1476), "longitude": slice(3874, 3876)}
    for name in variables:
        sample = ds[name].isel(time=[0, -1], **center).load()
        print(f"  {name} sample (first/last time step):\n{sample.values}")

    print(f"\nSuccess: store ready at {store_uri}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
