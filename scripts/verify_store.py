#!/usr/bin/env python3
"""Compare random store slices against the source files.

Samples N time steps, maps each slice back to its source granule via the
store manifest, and compares a random window of every data variable read
through the virtual store against the same window read directly from the
source file with h5py. Any mismatch, missing variable, or read failure
(including the checksum error a source object overwritten after ingest
produces) is reported and the script exits non-zero.

Uses the same environment variables as the processor Lambdas
(ICECHUNK_BUCKET or ICECHUNK_LOCAL_PATH, VIRTUAL_CHUNK_PREFIX,
TEMPO_COLLECTION, STORE_MANIFEST_URI). Reading s3:// sources requires
Earthdata credentials.

Usage:
    uv run scripts/verify_store.py --samples 8 --window 5
"""

from __future__ import annotations

import argparse
import os
import sys
from contextlib import contextmanager
from typing import Any, Callable, Iterator

import h5py
import numpy as np
import zarr
from icechunk import Repository
from virtualizarr_processor.inventory import BackfillInventory
from virtualizarr_processor.manifest import StoreManifest

COORDINATES = ("time", "latitude", "longitude")


@contextmanager
def open_source(url: str) -> Iterator[h5py.File]:
    """Open the source granule with h5py, locally or via earthaccess."""
    if url.startswith("file://"):
        with h5py.File(url.removeprefix("file://")) as h5:
            yield h5
        return
    import earthaccess

    [f] = earthaccess.open([url])
    with h5py.File(f) as h5:
        yield h5


def _source_dataset(h5: h5py.File, name: str) -> h5py.Dataset | None:
    """Find the source dataset behind a flattened variable name."""
    if name in h5:
        return h5[name]  # type: ignore[no-any-return]
    for item in h5.values():
        if isinstance(item, h5py.Group) and name in item:
            return item[name]  # type: ignore[no-any-return]
    return None


def verify_store(
    repo: Repository,
    manifest: BackfillInventory,
    *,
    samples: int = 8,
    window: int = 5,
    seed: int = 0,
    open_source_file: Callable[[str], Any] = open_source,
) -> list[str]:
    """Return every discrepancy found, one line each; empty means clean."""
    session = repo.readonly_session("main")
    group = zarr.open_group(session.store, mode="r")
    axis = np.asarray(zarr.open_array(session.store, path="time")[:])
    StoreManifest.validate_against_axis(manifest, axis)

    rng = np.random.default_rng(seed)
    indices = sorted(
        int(i)
        for i in rng.choice(axis.size, size=min(samples, axis.size), replace=False)
    )
    variables = [name for name in group.array_keys() if name not in COORDINATES]

    problems: list[str] = []
    for index in indices:
        entry = manifest.granules[index]
        try:
            with open_source_file(entry.url) as h5:
                if float(h5["time"][0]) != entry.time:
                    problems.append(
                        f"slot {index}: source /time {float(h5['time'][0])!r} != "
                        f"manifest time {entry.time!r} ({entry.granule_ur})"
                    )
                    continue
                for name in variables:
                    dataset = _source_dataset(h5, name)
                    if dataset is None:
                        problems.append(
                            f"slot {index}: variable {name!r} missing from "
                            f"source {entry.granule_ur}"
                        )
                        continue
                    ny, nx = group[name].shape[1], group[name].shape[2]
                    y0 = int(rng.integers(0, max(1, ny - window)))
                    x0 = int(rng.integers(0, max(1, nx - window)))
                    win = np.s_[y0 : y0 + window, x0 : x0 + window]
                    stored = np.asarray(group[name][index][win])
                    source = np.asarray(
                        dataset[0][win] if dataset.ndim == 3 else dataset[win]
                    )
                    if not np.array_equal(stored, source):
                        problems.append(
                            f"slot {index}: {name}[{y0}:{y0 + window},"
                            f"{x0}:{x0 + window}] differs from source "
                            f"{entry.granule_ur}"
                        )
        except Exception as error:  # a read failure is a finding, not a crash
            problems.append(
                f"slot {index}: reading {entry.granule_ur} failed: "
                f"{type(error).__name__}: {error}"
            )
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--samples", type=int, default=8)
    parser.add_argument("--window", type=int, default=5)
    parser.add_argument("--seed", type=int, default=0)
    args = parser.parse_args()

    from virtualizarr_processor.processor import Processor

    processor = Processor()
    repo = processor.open_backfill_repo()
    manifest = StoreManifest.read(os.environ["STORE_MANIFEST_URI"])
    problems = verify_store(
        repo, manifest, samples=args.samples, window=args.window, seed=args.seed
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
