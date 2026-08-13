# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "earthaccess>=0.14",
#     "matplotlib",
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
chunk container — to prove the store works end to end. Finally renders a map of the
first time step of ``vertical_column`` to a PNG (``--plot-path``).

Requires Earthdata Login credentials in ~/.netrc.

Usage:
    uv run exploration/combine_first_three_virtual.py
    uv run exploration/combine_first_three_virtual.py --collection no2
"""

import argparse
import sys

import earthaccess
import xarray as xr
from virtualizarr.manifests import ManifestArray

from tempo_collections import add_collection_argument, resolve_concept_id
from tempo_plot import save_map_png
from tempo_virtual import (
    combine_trees,
    earthdata_token,
    make_in_memory_repo,
    make_registry,
    manifest_totals,
    virtualize_urls,
)

N_FILES = 3


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


def main() -> int:
    parser_cli = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser_cli.add_argument(
        "--plot-path",
        default="vertical_column_first_three.png",
        help="Where to write the PNG map of the first time step",
    )
    add_collection_argument(parser_cli)
    args = parser_cli.parse_args()
    concept_id = resolve_concept_id(args)

    token = earthdata_token()

    print(f"Finding the first {N_FILES} granules of {concept_id}...")
    urls = first_granule_urls(N_FILES, concept_id)
    for url in urls:
        print(f"  {url}")

    registry = make_registry(token)
    trees = virtualize_urls(urls, registry, workers=1)

    describe_tree(trees[0], urls[0].rsplit("/", 1)[-1])

    print("\nCombining virtual datatrees along time...")
    combined = combine_trees(trees)
    chunks, total_bytes = manifest_totals(combined)
    print(f"Combined tree references {chunks} chunks, {total_bytes / 1e9:.2f} GB of source data")

    print("\nWriting virtual references to in-memory Icechunk repository...")
    repo = make_in_memory_repo(token)
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

    print(f"\nRendering a map of {var_name} at the first time step...")
    map_slice = product[var_name].isel(time=0).load()
    png_path = save_map_png(map_slice, args.plot_path)
    print(f"  wrote {png_path}")

    print("\nSuccess: three granules combined into one virtual Icechunk store, data readable.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
