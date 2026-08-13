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
"""Build a small persistent virtual Icechunk store for titiler-multidim smoke testing.

Virtualizes the N most recent granules of the selected collection (HCHO by default,
``--collection no2`` for NO2) over Earthdata-authed HTTPS, concatenates them along
``time``, keeps only the smoke-test variables (the primary column variable plus
``main_data_quality_flag``), flattens them from the ``product`` group to the root
group with inherited coordinates, and commits the result to a local-filesystem
Icechunk repository. The persisted repository config registers the HTTP virtual
chunk container WITHOUT credentials; readers supply the bearer token at open time.
Finishes by reopening the store read-only with the token and loading a data sample
through the virtual chunk container.

With ``--concept-id`` the variable subset is still chosen by ``--collection``.

Requires Earthdata Login credentials in ~/.netrc.

Usage:
    uv run exploration/build_titiler_test_store.py
    uv run exploration/build_titiler_test_store.py --collection no2
"""

import argparse
import sys
from pathlib import Path

import earthaccess
import icechunk
import xarray as xr

from tempo_collections import add_collection_argument, resolve_concept_id
from tempo_virtual import (
    CONTAINER_PREFIX,
    PARSE_WORKERS,
    combine_trees,
    earthdata_token,
    make_registry,
    manifest_totals,
    virtualize_urls,
)

VARIABLES = {
    "hcho": ["vertical_column", "main_data_quality_flag"],
    "no2": ["vertical_column_troposphere", "main_data_quality_flag"],
}
DEFAULT_N_GRANULES = 12


def recent_granule_urls(n: int, concept_id: str) -> list[str]:
    """The n most recent granules of the collection, in chronological order."""
    granules = earthaccess.search_data(concept_id=concept_id, count=n, sort_key="-start_date")
    urls = []
    for granule in granules:
        links = [u for u in granule.data_links(access="external") if u.endswith(".nc")]
        if not links:
            raise RuntimeError(f"No .nc data link for granule {granule['meta']['concept-id']}")
        urls.append(links[0])
    return list(reversed(urls))


def flatten_product_subset(tree: xr.DataTree, variables: list[str]) -> xr.Dataset:
    """Root-group dataset holding the selected product variables with inherited coords."""
    product = tree["product"].to_dataset()  # inherit=True pulls root time/lat/lon coords
    missing = [name for name in variables if name not in product]
    if missing:
        raise RuntimeError(f"Variables missing from product group: {missing}")
    flat = product[variables]
    flat.attrs = dict(tree.attrs)
    return flat


def create_repo(store_dir: Path) -> icechunk.Repository:
    """Create the local repo, persisting a credential-less HTTP container config."""
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(CONTAINER_PREFIX, icechunk.http_store())
    )
    repo = icechunk.Repository.create(
        storage=icechunk.local_filesystem_storage(str(store_dir)), config=config
    )
    repo.save_config()
    return repo


def open_readonly_with_token(store_dir: Path, token: str) -> xr.Dataset:
    """Open the store the way titiler will: runtime config carries the bearer token."""
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            CONTAINER_PREFIX,
            icechunk.http_store(headers={"Authorization": f"Bearer {token}"}),
        )
    )
    repo = icechunk.Repository.open(
        storage=icechunk.local_filesystem_storage(str(store_dir)),
        config=config,
        authorize_virtual_chunk_access={CONTAINER_PREFIX: icechunk.credentials.HttpAccess},
    )
    return xr.open_dataset(
        repo.readonly_session("main").store, engine="zarr", consolidated=False, zarr_format=3
    )


def main() -> int:
    parser_cli = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser_cli.add_argument("--n-granules", type=int, default=DEFAULT_N_GRANULES)
    parser_cli.add_argument("--workers", type=int, default=PARSE_WORKERS)
    parser_cli.add_argument(
        "--store-dir",
        default=None,
        help="Store output directory (default stores/tempo-<collection>-test)",
    )
    add_collection_argument(parser_cli)
    args = parser_cli.parse_args()
    concept_id = resolve_concept_id(args)
    variables = VARIABLES[args.collection]

    store_dir = Path(args.store_dir or f"stores/tempo-{args.collection}-test")
    if store_dir.exists():
        sys.exit(f"{store_dir} already exists; remove it first to rebuild")
    store_dir.parent.mkdir(parents=True, exist_ok=True)

    token = earthdata_token()

    print(f"Selecting the {args.n_granules} most recent granules of {concept_id}:")
    urls = recent_granule_urls(args.n_granules, concept_id)
    for url in urls:
        print(f"  {url.rsplit('/', 1)[-1]}")

    registry = make_registry(token)
    print(f"\nVirtualizing {len(urls)} granules with {args.workers} workers:")
    trees = virtualize_urls(urls, registry, workers=args.workers)

    print("\nCombining and subsetting...")
    combined = combine_trees(trees)
    flat = flatten_product_subset(combined, variables)
    flat_tree = xr.DataTree(dataset=flat)
    chunks, total_bytes = manifest_totals(flat_tree)
    print(f"Flat dataset: {list(flat.data_vars)} referencing {chunks} chunks, "
          f"{total_bytes / 1e9:.2f} GB of source data")

    print(f"\nWriting virtual references to {store_dir}...")
    repo = create_repo(store_dir)
    session = repo.writable_session("main")
    flat.vz.to_icechunk(session.store)
    snapshot = session.commit(
        f"Add {len(urls)} recent {concept_id} granules ({', '.join(variables)})"
    )
    print(f"Committed snapshot {snapshot}")

    print("\nReading back with token-authorized runtime config (titiler open pattern):")
    ds = open_readonly_with_token(store_dir, token)
    print(f"  time steps ({ds.sizes['time']}): {ds['time'].values.min()} .. {ds['time'].values.max()}")
    center = {"latitude": slice(1474, 1476), "longitude": slice(3874, 3876)}
    for name in variables:
        sample = ds[name].isel(time=[0, -1], **center).load()
        print(f"  {name} sample (first/last time step):\n{sample.values}")

    print(f"\nSuccess: store ready at {store_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
