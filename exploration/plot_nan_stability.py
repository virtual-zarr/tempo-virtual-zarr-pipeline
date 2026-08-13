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
"""Check whether the NaN placement in a TEMPO L3 variable is constant across time steps.

Loads ``vertical_column`` from the first N granules of the selected collection
(HCHO by default, ``--collection no2`` for NO2) over Earthdata-authed HTTPS,
compares the NaN masks across scans, and renders a PNG (``--plot-path``) mapping
how many time steps each pixel is NaN in — separating the always-valid interior,
the always-NaN exterior, and the pixels that flip between scans.

Requires Earthdata Login credentials in ~/.netrc.

Usage:
    uv run exploration/plot_nan_stability.py
    uv run exploration/plot_nan_stability.py --n-granules 4 --collection no2
"""

import argparse
import sys

import earthaccess
import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
from tempo_collections import add_collection_argument, resolve_concept_id


def main() -> int:
    parser_cli = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser_cli.add_argument("--n-granules", type=int, default=3)
    parser_cli.add_argument(
        "--plot-path",
        default="tempo_nan_stability.png",
        help="Where to write the PNG map of NaN counts per pixel",
    )
    add_collection_argument(parser_cli)
    args = parser_cli.parse_args()
    concept_id = resolve_concept_id(args)

    earthaccess.login(strategy="netrc")

    print(f"Finding the first {args.n_granules} granules of {concept_id}...")
    granules = earthaccess.search_data(
        concept_id=concept_id, count=args.n_granules, sort_key="start_date"
    )

    masks, times = [], []
    var_name = None
    lat = lon = None
    for f in earthaccess.open(granules):
        root = xr.open_dataset(f, engine="h5netcdf")
        product = xr.open_dataset(f, engine="h5netcdf", group="product")
        if var_name is None:
            var_name = (
                "vertical_column"
                if "vertical_column" in product
                else next(iter(product.data_vars))
            )
            lat = root["latitude"].values
            lon = root["longitude"].values
        time_value = root["time"].values[0]
        print(f"  loading {var_name} at {time_value}...")
        mask = np.isnan(product[var_name].isel(time=0).values)
        masks.append(mask)
        times.append(time_value)

    if lat is None or lon is None:
        raise RuntimeError(f"No granules opened for {concept_id}")

    n = len(masks)
    count = np.sum(masks, axis=0).astype("int16")
    total = count.size

    print(f"\nNaN mask comparison for {var_name} across {n} scans:")
    for time_value, mask in zip(times, masks):
        print(f"  {time_value}: {mask.mean():.1%} NaN")
    identical = all(np.array_equal(masks[0], m) for m in masks[1:])
    always_nan = int((count == n).sum())
    never_nan = int((count == 0).sum())
    varying = total - always_nan - never_nan
    print(f"  masks identical across scans: {identical}")
    print(
        f"  always NaN: {always_nan / total:.1%}, "
        f"never NaN: {never_nan / total:.1%}, varying: {varying / total:.1%}"
    )

    print("\nRendering NaN-count map...")
    cmap = plt.get_cmap("Blues", n + 1)
    fig, ax = plt.subplots(figsize=(12, 6), constrained_layout=True)
    image = ax.imshow(
        count,
        origin="lower",
        extent=(lon.min(), lon.max(), lat.min(), lat.max()),
        cmap=cmap,
        vmin=-0.5,
        vmax=n + 0.5,
        interpolation="nearest",
    )
    ax.set_aspect(1 / np.cos(np.deg2rad(lat.mean())))
    ax.set_xlabel("longitude")
    ax.set_ylabel("latitude")
    time_range = " to ".join(
        np.datetime_as_string(np.asarray(t), unit="m") for t in (times[0], times[-1])
    )
    ax.set_title(f"Time steps where {var_name} is NaN (of {n} scans, {time_range} UTC)")
    fig.colorbar(image, ax=ax, ticks=range(n + 1), shrink=0.85, label="scans NaN")
    fig.savefig(args.plot_path, dpi=150)
    print(f"  wrote {args.plot_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
