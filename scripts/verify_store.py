#!/usr/bin/env python3
"""Compare store contents against the sources CMR currently advertises.

Samples random time steps and, for each, asks CMR for the granule nearest
that time — independently of the pipeline's own bookkeeping — opens the
file CMR points at, and requires its in-file ``/time[0]`` to match the
store's axis value exactly. Random windows of every data variable are then
compared twice: raw bytes through the virtual store against raw h5py reads
(the primary oracle), and CF-decoded values through xarray against the
source values masked by the source's own fill value (the read path users
take). Because the source URL comes from CMR rather than the store
manifest, a store still referencing a superseded revision is caught even
when the old object is intact; a manifest URL that disagrees with CMR is
reported as its own finding.

``--offline`` falls back to manifest-provided URLs (no CMR access), which
still detects corrupt references and mutated source objects.
``--completeness`` additionally lists the collection's granules from CMR
and diffs their URs against the store manifest plus the pending ledger,
both read from the `main` branch's own snapshot.

Any mismatch, missing granule, or read failure (including the checksum
error a source object overwritten after ingest produces) is reported and
the script exits non-zero.

Uses the same environment variables as the processor Lambdas; the manifest
and pending ledger live inside the store itself, so a per-collection env
file is enough. Reading the store's virtual chunks and the s3:// sources
requires Earthdata credentials (EARTHDATA_TOKEN, username/password, or
EARTHDATA_SECRET_ARN) or ambient AWS access to the source bucket; CMR
metadata does not.

Usage:
    uv run scripts/verify_store.py --samples 8 --window 5
    uv run scripts/verify_store.py --completeness
    uv run scripts/verify_store.py --offline
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.parse
import urllib.request
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterator, Optional, cast

import h5py
import numpy as np
import xarray as xr
import zarr
from icechunk import Repository
from obspec_utils.readers import BlockStoreReader
from virtualizarr_processor.granule import make_registry
from virtualizarr_processor.inventory import BackfillInventory
from virtualizarr_processor.manifest import (
    MANIFEST_ARRAYS,
    PendingLedger,
    StoreManifest,
)

COORDINATES = ("time", "latitude", "longitude")
TEMPO_EPOCH = datetime(1980, 1, 6, tzinfo=timezone.utc)
CMR_GRANULES_URL = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"
# Wide enough to absorb the nominal-vs-in-file time offset (tens of
# seconds), narrow enough to exclude the closest neighboring scan (8 min).
SEARCH_WINDOW = timedelta(minutes=4)

# when -> (url, granule_ur) of the CMR granule nearest that time, or None.
CmrLookup = Callable[[datetime], Optional[tuple[str, str]]]


@contextmanager
def open_source(url: str) -> Iterator[h5py.File]:
    """Open the source granule with h5py over cached ranged reads.

    Uses the processor's own registry (obstore + Earthdata credentials), the
    read path the deployed workers use: unlike earthaccess it needs no EC2
    IMDS to prove it is in-region, so s3:// sources work from CloudShell,
    CodeBuild, and Lambda. The block reader LRU-caches 1 MiB ranges, which
    suits h5py's many small scattered reads.
    """
    registry = make_registry(url)
    store, path = registry.resolve(url)
    with BlockStoreReader(store, path) as reader, h5py.File(reader) as h5:
        yield h5


def axis_datetime(time_value: float) -> datetime:
    """Convert a raw axis value (seconds since the TEMPO epoch) to UTC."""
    return TEMPO_EPOCH + timedelta(seconds=time_value)


def _cmr_search(
    params: dict[str, Any], search_after: str | None = None
) -> tuple[list[dict], str | None]:
    request = urllib.request.Request(
        f"{CMR_GRANULES_URL}?{urllib.parse.urlencode(params)}"
    )
    if search_after:
        request.add_header("CMR-Search-After", search_after)
    with urllib.request.urlopen(request, timeout=60) as response:
        payload = json.loads(response.read())
        return payload.get("items", []), response.headers.get("CMR-Search-After")


def _direct_s3_url(umm: dict[str, Any]) -> str | None:
    for related in umm.get("RelatedUrls", []):
        url = related.get("URL", "")
        if (
            related.get("Type") == "GET DATA VIA DIRECT ACCESS"
            and url.startswith("s3://")
            and url.endswith(".nc")
        ):
            return str(url)
    return None


def _beginning(umm: dict[str, Any]) -> datetime:
    raw = umm["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"]
    return datetime.fromisoformat(raw.replace("Z", "+00:00"))


def cmr_lookup_for(concept_id: str) -> CmrLookup:
    """Build the live CMR lookup used outside of tests."""

    def lookup(when: datetime) -> tuple[str, str] | None:
        start = (when - SEARCH_WINDOW).isoformat()
        end = (when + SEARCH_WINDOW).isoformat()
        items, _ = _cmr_search(
            {
                "collection_concept_id": concept_id,
                "temporal": f"{start},{end}",
                "page_size": 10,
            }
        )
        candidates = [item for item in items if _direct_s3_url(item["umm"])]
        if not candidates:
            return None
        nearest = min(candidates, key=lambda i: abs(_beginning(i["umm"]) - when))
        umm = nearest["umm"]
        url = _direct_s3_url(umm)
        assert url is not None
        return url, str(umm.get("GranuleUR", ""))

    return lookup


def _mask_fill(values: np.ndarray, dataset: h5py.Dataset) -> np.ndarray:
    """Apply the source's own fill-value convention, as a CF reader would."""
    fill = dataset.attrs.get("_FillValue")
    if fill is None:
        return values
    fill_value = np.asarray(fill).ravel()[0]
    return np.where(values == fill_value, np.nan, values.astype("float64"))


def _values_match(a: np.ndarray, b: np.ndarray) -> bool:
    a, b = np.asarray(a), np.asarray(b)
    if a.dtype.kind == "f" or b.dtype.kind == "f":
        return bool(
            np.array_equal(a.astype("float64"), b.astype("float64"), equal_nan=True)
        )
    return bool(np.array_equal(a, b))


def verify_store(
    repo: Repository,
    manifest: BackfillInventory,
    *,
    samples: int = 8,
    window: int = 5,
    seed: int = 0,
    cmr_lookup: CmrLookup | None = None,
    open_source_file: Callable[[str], Any] = open_source,
) -> list[str]:
    """Return every discrepancy found, one line each; empty means clean.

    With ``cmr_lookup`` the source URL for each sampled slot comes from
    CMR, independently of the manifest; without it (offline mode) the
    manifest's own URL is used.
    """
    session = repo.readonly_session("main")
    group = zarr.open_group(session.store, mode="r")
    decoded = xr.open_dataset(
        cast(Any, session.store), engine="zarr", consolidated=False
    )
    axis = np.asarray(zarr.open_array(session.store, path="time")[:])

    rng = np.random.default_rng(seed)
    indices = sorted(
        int(i)
        for i in rng.choice(axis.size, size=min(samples, axis.size), replace=False)
    )
    skip = COORDINATES + MANIFEST_ARRAYS
    variables = [name for name in group.array_keys() if name not in skip]

    problems: list[str] = []
    for index in indices:
        before = len(problems)
        try:
            entry = manifest.granules[index]
            url = entry.url
            if cmr_lookup is not None:
                found = cmr_lookup(axis_datetime(float(axis[index])))
                if found is None:
                    problems.append(
                        f"slot {index}: CMR has no granule near "
                        f"{axis_datetime(float(axis[index])).isoformat()} "
                        f"(manifest says {entry.granule_ur})"
                    )
                    continue
                url, granule_ur = found
                if url != entry.url:
                    problems.append(
                        f"slot {index}: manifest url {entry.url} differs from "
                        f"CMR's current url {url} ({granule_ur}); comparing "
                        "against CMR's"
                    )
            _check_slot(
                problems,
                index,
                url,
                group,
                decoded,
                variables,
                rng,
                window,
                open_source_file,
                axis,
            )
        finally:
            # Narrate every slot, findings or not: a green log should say
            # what was checked, not just that nothing failed.
            new = len(problems) - before
            print(
                f"slot {index} @ {axis_datetime(float(axis[index])).isoformat()}"
                f" ({url}): {'ok' if new == 0 else f'{new} problem(s)'}",
                file=sys.stderr,
            )
    return problems


def _check_slot(
    problems: list[str],
    index: int,
    url: str,
    group: zarr.Group,
    decoded: xr.Dataset,
    variables: list[str],
    rng: np.random.Generator,
    window: int,
    open_source_file: Callable[[str], Any],
    axis: np.ndarray,
) -> None:
    """Compare one sampled slot against its source, appending findings."""
    try:
        with open_source_file(url) as h5:
            if float(h5["time"][0]) != float(axis[index]):
                problems.append(
                    f"slot {index}: source /time {float(h5['time'][0])!r} != "
                    f"store axis {float(axis[index])!r} ({url})"
                )
                return
            for name in variables:
                dataset = _source_dataset(h5, name)
                if dataset is None:
                    problems.append(
                        f"slot {index}: variable {name!r} missing from source {url}"
                    )
                    continue
                array = cast(zarr.Array, group[name])
                ny, nx = array.shape[1], array.shape[2]
                y0 = int(rng.integers(0, max(1, ny - window)))
                x0 = int(rng.integers(0, max(1, nx - window)))
                win = np.s_[y0 : y0 + window, x0 : x0 + window]
                source = np.asarray(
                    dataset[0][win] if dataset.ndim == 3 else dataset[win]
                )
                # Index the window directly so only its chunks are read.
                stored = np.asarray(array[(index, *win)])
                if not np.array_equal(stored, source):
                    problems.append(
                        f"slot {index}: {name}[{y0}:{y0 + window},"
                        f"{x0}:{x0 + window}] raw bytes differ from {url}"
                    )
                    continue
                # Window before loading: .values on the full slice would
                # fetch every chunk of a 2950x7750 decoded field to
                # compare a tiny window.
                read = np.asarray(decoded[name].isel(time=index)[win].values)
                if not _values_match(read, _mask_fill(source, dataset)):
                    problems.append(
                        f"slot {index}: {name}[{y0}:{y0 + window},"
                        f"{x0}:{x0 + window}] decoded values differ from "
                        f"the source's fill convention ({url})"
                    )
    except Exception as error:  # a read failure is a finding, not a crash
        problems.append(
            f"slot {index}: reading {url} failed: {type(error).__name__}: {error}"
        )


def _source_dataset(h5: h5py.File, name: str) -> h5py.Dataset | None:
    """Find the source dataset behind a flattened variable name."""
    if name in h5:
        return h5[name]  # type: ignore[no-any-return]
    for item in h5.values():
        if isinstance(item, h5py.Group) and name in item:
            return item[name]  # type: ignore[no-any-return]
    return None


def verify_completeness(
    concept_id: str,
    manifest: BackfillInventory,
    ledger_urs: set[str],
    search: Callable[..., tuple[list[dict], str | None]] = _cmr_search,
) -> list[str]:
    """Diff CMR's granule URs against the manifest plus the pending ledger."""
    cmr_urs: set[str] = set()
    search_after: str | None = None
    while True:
        items, search_after = search(
            {"collection_concept_id": concept_id, "page_size": 2000},
            search_after,
        )
        cmr_urs |= {str(item["umm"]["GranuleUR"]) for item in items}
        if not items or not search_after:
            break

    print(
        f"completeness: CMR lists {len(cmr_urs)} granules; manifest has "
        f"{len(manifest.granules)}, pending ledger {len(ledger_urs)}",
        file=sys.stderr,
    )
    known = {entry.granule_ur for entry in manifest.granules} | ledger_urs
    problems = [
        f"completeness: {ur} exists in CMR but is neither in the store "
        "manifest nor the pending ledger"
        for ur in sorted(cmr_urs - known)
    ]
    problems += [
        f"completeness: {ur} is in the store but CMR no longer lists it"
        for ur in sorted({e.granule_ur for e in manifest.granules} - cmr_urs)
    ]
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--samples", type=int, default=8)
    parser.add_argument("--window", type=int, default=5)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument(
        "--offline",
        action="store_true",
        help="use manifest URLs instead of querying CMR",
    )
    parser.add_argument(
        "--completeness",
        action="store_true",
        help="also diff CMR's granule listing against manifest + ledger",
    )
    args = parser.parse_args()

    from virtualizarr_processor.processor import Processor

    processor = Processor()
    # Readers must authorize the virtual chunk container; without it,
    # icechunk blocks every chunk fetch as UnauthorizedVirtualChunkContainer.
    repo = processor.open_backfill_repo(authorize_virtual_reads=True)
    pinned = repo.readonly_session("main").store
    manifest = StoreManifest.read(pinned)
    if manifest is None:
        print("FAIL: store carries no manifest", file=sys.stderr)
        return 1
    lookup = None if args.offline else cmr_lookup_for(processor.config.concept_id)
    problems = verify_store(
        repo,
        manifest,
        samples=args.samples,
        window=args.window,
        seed=args.seed,
        cmr_lookup=lookup,
    )
    if args.completeness:
        ledger_urs = {entry.granule_ur for entry in PendingLedger.read(pinned)}
        problems += verify_completeness(
            processor.config.concept_id, manifest, ledger_urs
        )

    if problems:
        print(f"FAIL: {len(problems)} discrepancies", file=sys.stderr)
        for line in problems:
            print(f"  {line}", file=sys.stderr)
        return 1
    print(f"OK: {args.samples} sampled slices match their sources", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
