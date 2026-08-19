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
"""Combine 20 time-spread TEMPO L3 granules into an in-memory virtual Icechunk store.

Splits the selected collection's temporal extent (2023-08-02 to now; HCHO by
default, ``--collection no2`` for NO2) into 20 equal windows, takes the first
granule in each window, virtualizes them with VirtualiZarr's
HDFParser over Earthdata-authed HTTPS, concatenates the virtual DataTrees along
``time``, writes virtual references into an in-memory Icechunk repository, and
reads data back through the virtual chunk container to verify. Finally renders a
map of the first time step of ``vertical_column`` to a PNG (``--plot-path``).

Requires Earthdata Login credentials in ~/.netrc.

Usage:
    uv run exploration/combine_twenty_spread_virtual.py
    uv run exploration/combine_twenty_spread_virtual.py --n-granules 10
    uv run exploration/combine_twenty_spread_virtual.py --collection no2
"""

import argparse
import sys
from datetime import UTC, datetime

import earthaccess
import xarray as xr
from tempo_collections import add_collection_argument, resolve_concept_id
from tempo_plot import save_map_png
from tempo_virtual import (
    PARSE_WORKERS,
    combine_trees,
    earthdata_token,
    make_in_memory_repo,
    make_registry,
    manifest_totals,
    virtualize_urls,
)

COLLECTION_START = datetime(2023, 8, 2, tzinfo=UTC)


def spread_granule_urls(n: int, concept_id: str) -> list[str]:
    """First granule in each of *n* equal temporal windows across the collection."""
    end = datetime.now(UTC)
    step = (end - COLLECTION_START) / n
    urls = []
    for i in range(n):
        window = (COLLECTION_START + i * step, COLLECTION_START + (i + 1) * step)
        granules = earthaccess.search_data(
            concept_id=concept_id, temporal=window, count=1, sort_key="start_date"
        )
        if not granules:
            print(
                f"  window {i + 1}/{n} ({window[0]:%Y-%m-%d} to {window[1]:%Y-%m-%d}): "
                "empty, skipped"
            )
            continue
        links = [
            u for u in granules[0].data_links(access="external") if u.endswith(".nc")
        ]
        if links:
            urls.append(links[0])
            print(f"  window {i + 1}/{n}: {links[0].rsplit('/', 1)[-1]}")
    return urls


def main() -> int:
    parser_cli = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser_cli.add_argument("--n-granules", type=int, default=20)
    parser_cli.add_argument("--workers", type=int, default=PARSE_WORKERS)
    parser_cli.add_argument(
        "--plot-path",
        default="vertical_column_twenty_spread.png",
        help="Where to write the PNG map of the first time step",
    )
    add_collection_argument(parser_cli)
    args = parser_cli.parse_args()
    concept_id = resolve_concept_id(args)

    token = earthdata_token()

    print(f"Selecting {args.n_granules} granules spread across {concept_id}:")
    urls = spread_granule_urls(args.n_granules, concept_id)
    print(f"Selected {len(urls)} granules")

    registry = make_registry(token)

    print(f"\nVirtualizing {len(urls)} granules with {args.workers} workers:")
    trees = virtualize_urls(urls, registry, workers=args.workers)

    print("\nCombining virtual datatrees along time...")
    combined = combine_trees(trees)
    chunks, total_bytes = manifest_totals(combined)
    print(
        f"Combined tree references {chunks} chunks, "
        f"{total_bytes / 1e9:.2f} GB of source data"
    )

    print("\nWriting virtual references to in-memory Icechunk repository...")
    repo = make_in_memory_repo(token)
    session = repo.writable_session("main")
    combined.vz.to_icechunk(session.store)
    snapshot = session.commit(
        f"Combine {len(urls)} spread granules of {concept_id} (virtual refs)"
    )
    print(f"Committed snapshot {snapshot}")

    print("\nReading back through Icechunk:")
    readback = xr.open_datatree(
        repo.readonly_session(branch="main").store, engine="zarr", consolidated=False
    )
    times = readback["time"].values
    print(f"  time ({len(times)} steps), spread check:")
    for t in times:
        print(f"    {t}")

    product = readback["product"].to_dataset()
    var_name = (
        "vertical_column"
        if "vertical_column" in product
        else next(iter(product.data_vars))
    )
    center = {
        dim: slice(size // 2, size // 2 + 2)
        for dim, size in product[var_name].sizes.items()
        if dim != "time"
    }
    sample = (
        product[var_name]
        .isel(time=[0, len(times) // 2, len(times) - 1], **center)
        .load()
    )
    print(f"\n  {var_name} sample from first/middle/last time steps:\n{sample.values}")

    print(f"\nRendering a map of {var_name} at the first time step...")
    map_slice = product[var_name].isel(time=0).load()
    png_path = save_map_png(map_slice, args.plot_path)
    print(f"  wrote {png_path}")

    print(
        f"\nSuccess: {len(urls)} granules combined into one virtual Icechunk store, "
        "data readable."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
