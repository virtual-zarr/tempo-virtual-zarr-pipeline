# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "earthaccess>=0.14",
#     "h5py",
#     "matplotlib",
#     "numpy",
# ]
# ///
"""Plot the HDF5 chunk structure of an original TEMPO L3 granule.

Opens the first granule of the selected collection (HCHO by default,
``--collection no2`` for NO2) over Earthdata-authed HTTPS, reads only HDF5
metadata (no data chunks), prints every dataset's shape, chunk shape, and
compression, and renders a PNG (``--plot-path``) drawing the chunk grid of each
distinct chunking over the (latitude, longitude) index space.

Requires Earthdata Login credentials in ~/.netrc.

Usage:
    uv run exploration/plot_chunk_structure.py
    uv run exploration/plot_chunk_structure.py --collection no2
"""

import argparse
import sys
import textwrap
from collections import defaultdict
from math import prod

import earthaccess
import h5py
import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np

from tempo_collections import add_collection_argument, resolve_concept_id

GRID_COLOR = "#4269d0"


def collect_datasets(h5: h5py.File) -> list[tuple[str, h5py.Dataset]]:
    datasets: list[tuple[str, h5py.Dataset]] = []
    h5.visititems(
        lambda name, obj: datasets.append((name, obj)) if isinstance(obj, h5py.Dataset) else None
    )
    return datasets


def plot_chunk_grid(ax, shape: tuple, chunks: tuple, names: list[str]) -> None:
    """Draw chunk boundaries of a (time, latitude, longitude) dataset in index space."""
    *_, n_lat, n_lon = shape
    *lead_chunks, c_lat, c_lon = chunks
    ax.add_patch(
        plt.Rectangle((0, 0), n_lon, n_lat, fill=False, edgecolor="#555555", linewidth=1.2)
    )
    for x in range(c_lon, n_lon, c_lon):
        ax.axvline(x, color=GRID_COLOR, linewidth=0.7)
    for y in range(c_lat, n_lat, c_lat):
        ax.axhline(y, color=GRID_COLOR, linewidth=0.7)
    ax.set_xlim(0, n_lon)
    ax.set_ylim(0, n_lat)
    ax.set_xlabel("longitude index")
    ax.set_ylabel("latitude index")
    grid_lat = -(-n_lat // c_lat)
    grid_lon = -(-n_lon // c_lon)
    ax.set_title(
        f"chunks {'×'.join(str(c) for c in chunks)} -> "
        f"{grid_lat}×{grid_lon} = {grid_lat * grid_lon} chunks per time step"
    )
    listing = textwrap.fill(
        ", ".join(name.rsplit("/", 1)[-1] for name in sorted(names)), width=55
    )
    ax.annotate(
        f"{len(names)} dataset{'s' if len(names) != 1 else ''}: {listing}",
        xy=(0.03, 0.96),
        xycoords="axes fraction",
        va="top",
        fontsize=8,
        color="#555555",
        bbox={"boxstyle": "round", "facecolor": "white", "edgecolor": "#cccccc", "alpha": 0.9},
    )


def main() -> int:
    parser_cli = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser_cli.add_argument(
        "--plot-path",
        default="tempo_chunk_structure.png",
        help="Where to write the PNG of the chunk grid",
    )
    add_collection_argument(parser_cli)
    args = parser_cli.parse_args()
    concept_id = resolve_concept_id(args)

    earthaccess.login(strategy="netrc")

    print(f"Finding the first granule of {concept_id}...")
    granules = earthaccess.search_data(concept_id=concept_id, count=1, sort_key="start_date")
    if not granules:
        raise RuntimeError(f"No granules found for {concept_id}")

    print("Opening granule over HTTPS and reading HDF5 metadata...")
    links = [u for u in granules[0].data_links(access="external") if u.endswith(".nc")]
    name = links[0].rsplit("/", 1)[-1] if links else "granule"
    h5 = h5py.File(earthaccess.open(granules)[0], "r")
    datasets = collect_datasets(h5)

    print(f"\n{'dataset':<42} {'shape':<20} {'chunks':<18} {'dtype':<10} compression")
    for path, ds in datasets:
        chunk_str = str(ds.chunks) if ds.chunks else "contiguous"
        compression = ds.compression or "none"
        if ds.compression_opts is not None:
            compression += f"({ds.compression_opts})"
        if ds.shuffle:
            compression += "+shuffle"
        print(f"{path:<42} {str(ds.shape):<20} {chunk_str:<18} {str(ds.dtype):<10} {compression}")

    # Group the gridded (…, latitude, longitude) datasets by their chunking so each
    # distinct chunk grid gets one panel.
    by_chunking: dict[tuple, list[str]] = defaultdict(list)
    for path, ds in datasets:
        if ds.ndim >= 2 and ds.chunks:
            by_chunking[(ds.shape, ds.chunks)].append(path)
    if not by_chunking:
        print("\nNo chunked gridded datasets found; nothing to plot.")
        return 1

    chunkings = sorted(by_chunking.items(), key=lambda kv: kv[0])
    fig, axes = plt.subplots(
        1, len(chunkings), figsize=(7 * len(chunkings), 4.5), constrained_layout=True, squeeze=False
    )
    for ax, ((shape, chunks), names) in zip(axes[0], chunkings):
        uncompressed = prod(chunks) * 8 / 1e6
        print(f"\nChunking {chunks} for {len(names)} datasets (~{uncompressed:.0f} MB/chunk at 8 bytes)")
        plot_chunk_grid(ax, shape, chunks, names)
    fig.suptitle(f"HDF5 chunk structure of {name}")
    fig.savefig(args.plot_path, dpi=150)
    print(f"\nWrote {args.plot_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
