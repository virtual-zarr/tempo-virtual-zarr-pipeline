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
"""Combine 20 granules of a TEMPO L3 collection, spread evenly through time, into an in-memory virtual Icechunk store.

Splits the selected collection's temporal extent (2023-08-02 to now; HCHO by
default, ``--collection no2`` for NO2) into 20 equal windows, takes the first
granule in each window, virtualizes them with VirtualiZarr's
HDFParser over Earthdata-authed HTTPS, concatenates the virtual DataTrees along
``time``, writes virtual references into an in-memory Icechunk repository, and
reads data back through the virtual chunk container to verify.

Requires Earthdata Login credentials in ~/.netrc.

Usage:
    uv run exploration/combine_twenty_spread_virtual.py
    uv run exploration/combine_twenty_spread_virtual.py --n-granules 10
    uv run exploration/combine_twenty_spread_virtual.py --collection no2
"""

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

import earthaccess
import icechunk
import numpy as np
import xarray as xr
from obspec_utils.registry import ObjectStoreRegistry
from obstore.store import HTTPStore
from virtualizarr.manifests import ManifestArray
from virtualizarr.parsers.hdf import HDFParser

from tempo_collections import add_collection_argument, resolve_concept_id

DATA_HOST = "https://data.asdc.earthdata.nasa.gov"
COLLECTION_START = datetime(2023, 8, 2, tzinfo=UTC)
# ASDC's CloudFront rate-limits bursts of range requests (403 "Request blocked"),
# so keep parallelism low and retry with backoff.
PARSE_WORKERS = 2
PARSE_ATTEMPTS = 4
BACKOFF_SECONDS = (10, 30, 60)


def earthdata_token() -> str:
    earthaccess.login(strategy="netrc")
    token = (getattr(earthaccess.__auth__, "token", None) or {}).get("access_token")
    if not token:
        raise RuntimeError("earthaccess.login() produced no bearer token; check ~/.netrc")
    return token


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
            print(f"  window {i + 1}/{n} ({window[0]:%Y-%m-%d} to {window[1]:%Y-%m-%d}): empty, skipped")
            continue
        links = [u for u in granules[0].data_links(access="external") if u.endswith(".nc")]
        if links:
            urls.append(links[0])
            print(f"  window {i + 1}/{n}: {links[0].rsplit('/', 1)[-1]}")
    return urls


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
        else:
            combined[node.path] = datasets[0]
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
    parser_cli.add_argument("--n-granules", type=int, default=20)
    parser_cli.add_argument("--workers", type=int, default=PARSE_WORKERS)
    add_collection_argument(parser_cli)
    args = parser_cli.parse_args()
    concept_id = resolve_concept_id(args)

    token = earthdata_token()

    print(f"Selecting {args.n_granules} granules spread across {concept_id}:")
    urls = spread_granule_urls(args.n_granules, concept_id)
    print(f"Selected {len(urls)} granules")

    store = HTTPStore.from_url(
        DATA_HOST, client_options={"default_headers": {"Authorization": f"Bearer {token}"}}
    )
    registry = ObjectStoreRegistry({DATA_HOST: store})
    parser = HDFParser()

    def virtualize(url: str) -> xr.DataTree:
        name = url.rsplit("/", 1)[-1]
        for attempt in range(PARSE_ATTEMPTS):
            try:
                tree = parser(url=url, registry=registry).to_virtual_datatree()
                print(f"  virtualized {name}")
                return tree
            except Exception as error:
                if attempt == PARSE_ATTEMPTS - 1:
                    raise
                delay = BACKOFF_SECONDS[min(attempt, len(BACKOFF_SECONDS) - 1)]
                print(f"  {name}: {type(error).__name__} (attempt {attempt + 1}), retrying in {delay}s")
                time.sleep(delay)
        raise AssertionError("unreachable")

    print(f"\nVirtualizing {len(urls)} granules with {args.workers} workers:")
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        trees = list(pool.map(virtualize, urls))

    print("\nCombining virtual datatrees along time...")
    combined = combine_trees(trees)
    chunks, total_bytes = manifest_totals(combined)
    print(f"Combined tree references {chunks} chunks, {total_bytes / 1e9:.2f} GB of source data")

    print("\nWriting virtual references to in-memory Icechunk repository...")
    repo = make_repo(token)
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
    var_name = "vertical_column" if "vertical_column" in product else next(iter(product.data_vars))
    center = {
        dim: slice(size // 2, size // 2 + 2)
        for dim, size in product[var_name].sizes.items()
        if dim != "time"
    }
    sample = product[var_name].isel(time=[0, len(times) // 2, len(times) - 1], **center).load()
    print(f"\n  {var_name} sample from first/middle/last time steps:\n{sample.values}")

    print(f"\nSuccess: {len(urls)} granules combined into one virtual Icechunk store, data readable.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
