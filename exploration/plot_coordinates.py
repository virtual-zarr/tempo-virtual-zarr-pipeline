# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "earthaccess>=0.14",
#     "h5netcdf",
#     "h5py",
#     "matplotlib",
#     "numpy",
#     "xarray",
# ]
# ///
"""Plot the coordinate structure of a TEMPO L3 granule to characterize its grid.

Opens the first granule of the selected collection (HCHO by default,
``--collection no2`` for NO2) over Earthdata-authed HTTPS, reads only the
``latitude`` and ``longitude`` coordinate arrays, and reports whether the grid is
curvilinear (2D coordinates) or rectilinear (1D coordinates), and whether the
spacing is uniform. Renders a PNG (``--plot-path``) with the coordinate values
and their point-to-point spacing.

Requires Earthdata Login credentials in ~/.netrc.

Usage:
    uv run exploration/plot_coordinates.py
    uv run exploration/plot_coordinates.py --collection no2
"""

import argparse
import sys

import earthaccess
import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np

from tempo_collections import add_collection_argument, resolve_concept_id

LAT_COLOR = "#4269d0"
LON_COLOR = "#efb118"


def describe_coordinate(name: str, values: np.ndarray) -> None:
    print(f"\n{name}: ndim={values.ndim}, shape={values.shape}, dtype={values.dtype}")
    if values.ndim != 1:
        print("  2D coordinate array -> curvilinear grid")
        return
    diffs = np.diff(values.astype("float64"))
    print(f"  range: {values.min():.6g} to {values.max():.6g}")
    print(f"  spacing: min={diffs.min():.10g}, mean={diffs.mean():.10g}, max={diffs.max():.10g}")
    # Coordinates are stored as float32, so adjacent diffs wobble by up to a few
    # ULPs of the coordinate magnitude (~2e-5 deg at 168 deg); treat spacing as
    # uniform if deviations stay within that representation error.
    rounding = 4 * np.finfo(values.dtype).eps * np.abs(values).max()
    deviation = np.abs(diffs - diffs.mean()).max()
    uniform = deviation <= rounding
    print(
        f"  max spacing deviation {deviation:.3g} vs {values.dtype} rounding bound "
        f"{rounding:.3g} -> uniform: {uniform}"
    )


def plot_coordinate(axes_column, name: str, values: np.ndarray, color: str) -> None:
    ax_values, ax_spacing = axes_column
    index = np.arange(values.size)
    ax_values.plot(index, values, color=color, linewidth=2)
    ax_values.set_title(f"{name} values")
    ax_values.set_xlabel("index")
    ax_values.set_ylabel("degrees")

    diffs = np.diff(values)
    ax_spacing.plot(index[1:], diffs, color=color, linewidth=2)
    ax_spacing.set_title(f"{name} spacing (Δ per step)")
    ax_spacing.set_xlabel("index")
    ax_spacing.set_ylabel("degrees")
    # A tight symmetric band around the mean makes any non-uniformity visible;
    # a perfectly regular grid renders as a flat line.
    spread = max(abs(diffs - diffs.mean()).max() * 3, abs(diffs.mean()) * 0.01)
    ax_spacing.set_ylim(diffs.mean() - spread, diffs.mean() + spread)
    ax_spacing.annotate(
        f"mean Δ = {diffs.mean():.6g}°",
        xy=(0.02, 0.94),
        xycoords="axes fraction",
        va="top",
        fontsize=9,
        color="#555555",
    )

    for ax in (ax_values, ax_spacing):
        ax.grid(True, linewidth=0.3, alpha=0.4)


def main() -> int:
    parser_cli = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser_cli.add_argument(
        "--plot-path",
        default="tempo_coordinates.png",
        help="Where to write the PNG of coordinate values and spacing",
    )
    add_collection_argument(parser_cli)
    args = parser_cli.parse_args()
    concept_id = resolve_concept_id(args)

    earthaccess.login(strategy="netrc")

    print(f"Finding the first granule of {concept_id}...")
    granules = earthaccess.search_data(concept_id=concept_id, count=1, sort_key="start_date")
    if not granules:
        raise RuntimeError(f"No granules found for {concept_id}")

    print("Opening granule over HTTPS and reading coordinate arrays...")
    import xarray as xr

    root = xr.open_dataset(earthaccess.open(granules)[0], engine="h5netcdf")
    lat = root["latitude"].values
    lon = root["longitude"].values

    describe_coordinate("latitude", lat)
    describe_coordinate("longitude", lon)

    if lat.ndim == 1 and lon.ndim == 1:
        print("\nBoth coordinates are 1D -> rectilinear grid (not curvilinear).")

    fig, axes = plt.subplots(2, 2, figsize=(12, 7), constrained_layout=True)
    plot_coordinate(axes[:, 0], "latitude", lat, LAT_COLOR)
    plot_coordinate(axes[:, 1], "longitude", lon, LON_COLOR)
    fig.suptitle(f"TEMPO L3 coordinate structure ({concept_id})")
    fig.savefig(args.plot_path, dpi=150)
    print(f"\nWrote {args.plot_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
