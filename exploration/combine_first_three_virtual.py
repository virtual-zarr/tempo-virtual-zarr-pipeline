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
"""Combine the first three granules of a TEMPO L3 collection into an in-memory virtual Icechunk store.

Finds the three earliest granules of the selected collection (HCHO by default,
``--collection no2`` for NO2) via earthaccess, virtualizes each with VirtualiZarr's
HDFParser over Earthdata-authed HTTPS, concatenates the per-file virtual DataTrees
along ``time``, writes the result
as virtual references into an in-memory Icechunk repository, then reads it back
through Icechunk — including loading a small slice of real data through the virtual
chunk container — to prove the store works end to end.

Requires Earthdata Login credentials in ~/.netrc.

Usage:
    uv run exploration/combine_first_three_virtual.py
    uv run exploration/combine_first_three_virtual.py --collection no2
"""

import argparse
import sys

import earthaccess
import icechunk
import numpy as np
import xarray as xr
from obspec_utils.registry import ObjectStoreRegistry
from obstore.store import HTTPStore
from virtualizarr.manifests import ManifestArray
from virtualizarr.parsers.hdf import HDFParser

from tempo_collections import add_collection_argument, resolve_concept_id

N_FILES = 3
DATA_HOST = "https://data.asdc.earthdata.nasa.gov"


def earthdata_token() -> str:
    earthaccess.login(strategy="netrc")
    token = (getattr(earthaccess.__auth__, "token", None) or {}).get("access_token")
    if not token:
        raise RuntimeError("earthaccess.login() produced no bearer token; check ~/.netrc")
    return token


def first_granule_urls(n: int, concept_id: str) -> list[str]:
    granules = earthaccess.search_data(concept_id=concept_id, count=n, sort_key="start_date")
    urls = []
    for granule in granules:
        links = [u for u in granule.data_links(access="external") if u.endswith(".nc")]
        if not links:
            raise RuntimeError(f"No .nc data link for granule {granule['meta']['concept-id']}")
        urls.append(links[0])
    return urls


def describe_tree(tree: xr.DataTree, label: str) -> None:
    print(f"\nStructure of {label}:")
    for node in tree.subtree:
        ds = node.to_dataset(inherit=False)
        dims = ", ".join(f"{k}={v}" for k, v in ds.sizes.items())
        print(f"  {node.path}  ({dims or 'no dims'})")
        for name, var in ds.variables.items():
            kind = "virtual" if isinstance(var.data, ManifestArray) else "loaded"
            print(f"    {name}: {var.dims} {var.dtype} [{kind}]")


def promote_time_invariant(ds: xr.Dataset) -> xr.Dataset:
    """Add a length-1 time axis to data variables stored without one (e.g. ``weight``).

    TEMPO L3 stores ``weight`` as (latitude, longitude) even though its values differ
    per scan; without promotion, concat would silently keep only the first scan's copy.
    ``np.expand_dims`` dispatches to ManifestArray's implementation, so virtual
    variables stay virtual.
    """
    for name in list(ds.data_vars):
        var = ds[name].variable
        if "time" in ds.dims and "time" not in var.dims:
            ds[name] = xr.Variable(
                ("time", *var.dims),
                np.expand_dims(var.data, axis=0),
                attrs=var.attrs,
                encoding=var.encoding,
            )
    return ds


def combine_trees(trees: list[xr.DataTree]) -> xr.DataTree:
    """Concatenate matching groups along time; copy time-invariant groups from the first."""
    combined: dict[str, xr.Dataset] = {}
    for node in trees[0].subtree:
        datasets = [
            promote_time_invariant(t[node.path].to_dataset(inherit=False)) for t in trees
        ]
        if "time" in datasets[0].dims:
            combined[node.path] = xr.concat(
                datasets,
                dim="time",
                data_vars="minimal",
                coords="minimal",
                compat="override",
                join="override",
                combine_attrs="override",
            )
            print(f"  {node.path}: concatenated along time")
        else:
            combined[node.path] = datasets[0]
            print(f"  {node.path}: no time dim, taken from first file")
    return xr.DataTree.from_dict(combined)


def manifest_totals(tree: xr.DataTree) -> tuple[int, int]:
    chunks = total_bytes = 0
    for node in tree.subtree:
        for var in node.to_dataset(inherit=False).variables.values():
            if isinstance(var.data, ManifestArray):
                entries = var.data.manifest.dict().values()
                chunks += len(entries)
                total_bytes += sum(e["length"] for e in entries)
    return chunks, total_bytes


def make_repo(token: str) -> icechunk.Repository:
    container_prefix = f"{DATA_HOST}/"
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            container_prefix,
            icechunk.http_store(headers={"Authorization": f"Bearer {token}"}),
        )
    )
    return icechunk.Repository.open_or_create(
        storage=icechunk.in_memory_storage(),
        config=config,
        authorize_virtual_chunk_access={container_prefix: icechunk.credentials.HttpAccess},
    )


def main() -> int:
    parser_cli = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    add_collection_argument(parser_cli)
    args = parser_cli.parse_args()
    concept_id = resolve_concept_id(args)

    token = earthdata_token()

    print(f"Finding the first {N_FILES} granules of {concept_id}...")
    urls = first_granule_urls(N_FILES, concept_id)
    for url in urls:
        print(f"  {url}")

    store = HTTPStore.from_url(
        DATA_HOST, client_options={"default_headers": {"Authorization": f"Bearer {token}"}}
    )
    registry = ObjectStoreRegistry({DATA_HOST: store})
    parser = HDFParser()

    trees = []
    for url in urls:
        print(f"Virtualizing {url.rsplit('/', 1)[-1]}...")
        manifest_store = parser(url=url, registry=registry)
        trees.append(manifest_store.to_virtual_datatree())

    describe_tree(trees[0], urls[0].rsplit("/", 1)[-1])

    print("\nCombining virtual datatrees:")
    combined = combine_trees(trees)
    chunks, total_bytes = manifest_totals(combined)
    print(f"Combined tree references {chunks} chunks, {total_bytes / 1e9:.2f} GB of source data")

    print("\nWriting virtual references to in-memory Icechunk repository...")
    repo = make_repo(token)
    session = repo.writable_session("main")
    combined.vz.to_icechunk(session.store)
    snapshot = session.commit(f"Combine first {N_FILES} TEMPO_HCHO_L3 granules (virtual refs)")
    print(f"Committed snapshot {snapshot}")

    print("\nReading back through Icechunk:")
    readback = xr.open_datatree(
        repo.readonly_session(branch="main").store, engine="zarr", consolidated=False
    )
    print(readback)

    print("\nLoading real data through the virtual chunk container:")
    times = readback["time"].values
    print(f"  time ({len(times)} steps): {times}")
    product = readback["product"].to_dataset()
    var_name = "vertical_column" if "vertical_column" in product else next(iter(product.data_vars))
    center = {
        dim: slice(size // 2, size // 2 + 3)
        for dim, size in product[var_name].sizes.items()
        if dim != "time"
    }
    sample = product[var_name].isel(center).load()
    print(f"  {var_name}{tuple(sample.sizes.values())} sample:\n{sample.values}")

    print("\nSuccess: three granules combined into one virtual Icechunk store, data readable.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
