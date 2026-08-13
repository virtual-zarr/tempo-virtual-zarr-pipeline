"""Shared virtualization helpers for the TEMPO exploration scripts.

Importable from sibling scripts because ``uv run exploration/<script>.py`` puts the
script's directory on ``sys.path``.
"""

import time
from concurrent.futures import ThreadPoolExecutor

import earthaccess
import icechunk
import numpy as np
import xarray as xr
from obspec_utils.registry import ObjectStoreRegistry
from obstore.store import HTTPStore
from virtualizarr.manifests import ManifestArray
from virtualizarr.parsers.hdf import HDFParser

DATA_HOST = "https://data.asdc.earthdata.nasa.gov"
CONTAINER_PREFIX = f"{DATA_HOST}/"
# ASDC's CloudFront rate-limits bursts of range requests (403 "Request blocked"),
# so keep parallelism low and retry with backoff.
PARSE_WORKERS = 2
PARSE_ATTEMPTS = 4
BACKOFF_SECONDS = (10, 30, 60)


def earthdata_token() -> str:
    earthaccess.login(strategy="netrc")
    token: str | None = (getattr(earthaccess.__auth__, "token", None) or {}).get(
        "access_token"
    )
    if not token:
        raise RuntimeError(
            "earthaccess.login() produced no bearer token; check ~/.netrc"
        )
    return token


def recent_granule_urls(n: int, concept_id: str, access: str = "external") -> list[str]:
    """The n most recent granules of the collection, in chronological order.

    ``access="external"`` returns EDL-authed HTTPS URLs; ``access="direct"`` returns
    in-region ``s3://`` URLs.
    """
    granules = earthaccess.search_data(
        concept_id=concept_id, count=n, sort_key="-start_date"
    )
    urls = []
    for granule in granules:
        links = [u for u in granule.data_links(access=access) if u.endswith(".nc")]
        if not links:
            raise RuntimeError(
                f"No .nc data link for granule {granule['meta']['concept-id']}"
            )
        urls.append(links[0])
    return list(reversed(urls))


def flatten_product_subset(tree: xr.DataTree, variables: list[str]) -> xr.Dataset:
    """Root-group dataset of the selected product variables with inherited coords."""
    product = tree[
        "product"
    ].to_dataset()  # inherit=True pulls root time/lat/lon coords
    missing = [name for name in variables if name not in product]
    if missing:
        raise RuntimeError(f"Variables missing from product group: {missing}")
    flat = product[variables]
    flat.attrs = dict(tree.attrs)
    return flat


def make_registry(token: str) -> ObjectStoreRegistry:
    store = HTTPStore.from_url(
        DATA_HOST,
        client_options={"default_headers": {"Authorization": f"Bearer {token}"}},
    )
    return ObjectStoreRegistry({DATA_HOST: store})


def virtualize_urls(
    urls: list[str], registry: ObjectStoreRegistry, workers: int = PARSE_WORKERS
) -> list[xr.DataTree]:
    """Virtualize granule URLs in parallel with HDFParser, retrying on throttling."""
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
                print(
                    f"  {name}: {type(error).__name__} (attempt {attempt + 1}), "
                    f"retrying in {delay}s"
                )
                time.sleep(delay)
        raise AssertionError("unreachable")

    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(virtualize, urls))


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
    """Concatenate groups along time; copy time-invariant groups from the first."""
    combined: dict[str, xr.Dataset] = {}
    for node in trees[0].subtree:
        datasets = [
            promote_time_invariant(t[node.path].to_dataset(inherit=False))
            for t in trees
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


def make_in_memory_repo(token: str) -> icechunk.Repository:
    """In-memory Icechunk repo with an EDL-authed HTTP virtual chunk container."""
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            CONTAINER_PREFIX,
            icechunk.http_store(headers={"Authorization": f"Bearer {token}"}),
        )
    )
    return icechunk.Repository.open_or_create(
        storage=icechunk.in_memory_storage(),
        config=config,
        authorize_virtual_chunk_access={
            CONTAINER_PREFIX: icechunk.credentials.HttpAccess
        },
    )
