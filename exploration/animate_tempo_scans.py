# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "earthaccess>=0.14",
#     "virtualizarr[hdf,icechunk]>=2.7",
#     "obspec-utils",
#     "obstore",
#     "xarray",
#     "matplotlib",
#     "imageio-ffmpeg",
# ]
# ///
"""Animate a day of TEMPO scans, read from a virtual Icechunk store.

Each hourly scan is revealed east→west (the way TEMPO's mirror steps
across North America) while a timeline underneath accumulates the
granules on the store's ``time`` axis. Every pixel is fetched from the
source granules on ASDC through the store's virtual references. Writes
a GIF plus a scrubbable MP4 sibling.

Defaults to the pipeline's S3 store for the collection:

    uv run exploration/animate_tempo_scans.py                   # tempo/hcho/v04
    uv run exploration/animate_tempo_scans.py --collection no2  # tempo/no2/v04

under s3://airquality-data-store-develop/. Run from in-region (us-west-2)
compute with AWS credentials for the store bucket — the temporary ASDC
credentials for the virtual chunk reads only work there.

A local store built by build_titiler_test_store.py works too (needs
EARTHDATA_TOKEN or ~/.netrc instead of AWS credentials):

    uv run exploration/animate_tempo_scans.py --store-dir stores/tempo-hcho-anim
"""

import argparse
import sys
import time
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
from animate_store_growth import save_animation
from build_s3_test_store import (
    asdc_s3_credentials,
    make_storage,
    open_readonly_with_credentials,
)
from build_titiler_test_store import open_readonly_with_token
from matplotlib.animation import FuncAnimation
from matplotlib.colors import Normalize
from tempo_collections import STORE_URI, VARIABLES, add_collection_argument
from tempo_virtual import BACKOFF_SECONDS, PARSE_ATTEMPTS, earthdata_token

DEFAULT_COARSEN = 10  # 2950 x 7750 -> 295 x 775, both divide evenly
DEFAULT_SUBSTEPS = 6  # reveal frames per scan
HOLD_FRAMES = 2  # full-scan frames after each sweep
NO_RETRIEVAL = "#ececec"  # same gray as tempo_plot.py


def pick_day(times: np.ndarray) -> np.ndarray:
    """Boolean mask for the most recent UTC day with at least 8 scans."""
    days = times.astype("datetime64[D]")
    for day in np.unique(days)[::-1]:
        if (days == day).sum() >= 8:
            return days == day
    return np.ones(len(times), dtype=bool)


def load_frames(
    ds: xr.Dataset, variables: list[str], mask: np.ndarray, coarsen: int
) -> xr.DataArray:
    """Quality-masked, block-averaged column values for the selected scans."""
    column, quality = (
        ds[variables[0]].isel(time=mask),
        ds[variables[1]].isel(time=mask),
    )
    frames = []
    for i in range(column.sizes["time"]):
        print(f"  loading scan {i + 1}/{column.sizes['time']} through virtual refs...")
        for attempt in range(PARSE_ATTEMPTS):
            try:
                scan = (
                    column.isel(time=i).load().where(quality.isel(time=i).load() == 0)
                )
                break
            except Exception as error:
                if attempt == PARSE_ATTEMPTS - 1:
                    raise
                delay = BACKOFF_SECONDS[min(attempt, len(BACKOFF_SECONDS) - 1)]
                print(f"    {type(error).__name__}, retrying in {delay}s")
                time.sleep(delay)
        frames.append(
            scan.coarsen(latitude=coarsen, longitude=coarsen).mean().astype("float32")
        )
    return xr.concat(frames, dim="time")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--store-dir",
        help="s3:// or local store (default: the pipeline store for the collection)",
    )
    parser.add_argument("--day", help="UTC day YYYY-MM-DD (default: busiest recent)")
    parser.add_argument("--coarsen", type=int, default=DEFAULT_COARSEN)
    parser.add_argument("--substeps", type=int, default=DEFAULT_SUBSTEPS)
    parser.add_argument("--fps", type=int, default=7)
    parser.add_argument("--dpi", type=int, default=100)
    parser.add_argument("--out", default=None, help="output GIF path")
    add_collection_argument(parser)
    args = parser.parse_args()
    variables = VARIABLES[args.collection]
    out_path = Path(args.out or f"tempo-{args.collection}-scan-day.gif")
    store = args.store_dir or STORE_URI.format(args.collection)

    print(f"Opening {store} read-only...")
    if store.startswith("s3://"):
        # Ambient AWS credentials for the store bucket, temporary ASDC
        # credentials for the virtual chunk container. The ASDC credentials
        # only read from in-region (us-west-2) compute, so run this on the
        # hub, not a laptop.
        bucket, _, prefix = store.removeprefix("s3://").partition("/")
        ds = open_readonly_with_credentials(
            make_storage(bucket, prefix.rstrip("/")), asdc_s3_credentials()
        )
    else:
        ds = open_readonly_with_token(Path(store), earthdata_token())
    times = ds["time"].values
    mask = (
        times.astype("datetime64[D]") == np.datetime64(args.day)
        if args.day
        else pick_day(times)
    )
    if not mask.any():
        sys.exit(f"no scans on {args.day} in the store")
    day_times = times[mask]
    day = str(day_times[0].astype("datetime64[D]"))
    print(f"Animating {mask.sum()} scans on {day}")

    # Loading a day of scans over throttled HTTPS takes minutes; cache the
    # coarsened frames so plotting tweaks re-render instantly.
    cache_dir = out_path.parent if store.startswith("s3://") else Path(store)
    cache = cache_dir / f"frames-{args.collection}-{day}-c{args.coarsen}.npz"
    if cache.exists():
        print(f"Using cached frames from {cache}")
        cached = np.load(cache)
        values, lat, lon = cached["values"], cached["lat"], cached["lon"]
    else:
        frames = load_frames(ds, variables, mask, args.coarsen)
        lat, lon = frames["latitude"].values, frames["longitude"].values
        values = frames.values  # (scan, lat, lon)
        np.savez_compressed(cache, values=values, lat=lat, lon=lon)
    finite = values[np.isfinite(values)]
    norm = Normalize(*np.percentile(finite, [2, 98]))
    cmap = plt.get_cmap("Blues").copy()
    cmap.set_bad(NO_RETRIEVAL)

    n_scans = len(day_times)
    per_scan = args.substeps + HOLD_FRAMES
    long_name = ds[variables[0]].attrs.get("long_name", variables[0])
    units = ds[variables[0]].attrs.get("units", "")

    fig, (ax, ax_tl) = plt.subplots(
        2,
        1,
        figsize=(9.6, 6.2),
        dpi=args.dpi,
        height_ratios=[5, 1],
        constrained_layout=True,
    )
    fig.patch.set_facecolor("white")
    extent = (lon[0], lon[-1], lat[0], lat[-1])
    image = ax.imshow(
        np.full(values.shape[1:], np.nan),
        origin="lower",
        extent=extent,
        cmap=cmap,
        norm=norm,
        interpolation="nearest",
    )
    ax.set_aspect(1 / np.cos(np.deg2rad(lat.mean())))
    ax.set_xlabel("longitude")
    ax.set_ylabel("latitude")
    sweep_line = ax.axvline(np.nan, color="#0b0b0b", linewidth=1, linestyle="--")
    title = ax.set_title("", loc="left", fontsize=12)
    fig.colorbar(image, ax=ax, shrink=0.85, label=f"{long_name} [{units}]")
    caption = (
        "One hourly scan = one netCDF-4 granule. The virtual Icechunk store maps "
        "every granule's chunks in place,\nserving the whole mission as a single "
        "time × latitude × longitude dataset."
    )
    # Reserve the bottom strip of the figure for the caption so constrained
    # layout doesn't run the timeline's xlabel into it.
    fig.get_layout_engine().set(rect=(0, 0.08, 1, 0.92))
    fig.text(0.02, 0.005, caption, fontsize=8.5, color="#52514e", va="bottom")

    # Timeline: each committed scan becomes a tick on the day's UTC hour axis.
    hours = (day_times - day_times[0].astype("datetime64[D]")) / np.timedelta64(1, "h")
    ax_tl.set_xlim(0, 24)
    ax_tl.set_ylim(0, 1)
    ax_tl.set_yticks([])
    ax_tl.set_xticks(range(0, 25, 3))
    ax_tl.set_xlabel(f"scan start, hours UTC on {day}", fontsize=9)
    for spine in ("top", "right", "left"):
        ax_tl.spines[spine].set_visible(False)
    tl_scatter = ax_tl.scatter([], [], marker="|", s=250, color="#2a78d6")
    tl_text = ax_tl.annotate(
        "",
        xy=(0.995, 0.72),
        xycoords="axes fraction",
        ha="right",
        fontsize=9,
        color="#0b0b0b",
    )

    def draw(frame: int):
        scan, step = divmod(frame, per_scan)
        data = values[scan].copy()
        if step < args.substeps:  # sweep east -> west (high lon index down)
            front = round(len(lon) * (1 - (step + 1) / args.substeps))
            data[:, :front] = np.nan
            sweep_line.set_xdata([lon[front] if front else lon[0]])
            sweep_line.set_visible(front > 0)
        else:
            sweep_line.set_visible(False)
        image.set_data(data)
        stamp = np.datetime_as_string(day_times[scan], unit="m")
        title.set_text(
            f"TEMPO {args.collection.upper()} L3 — scan {scan + 1}/{n_scans}, "
            f"{stamp} UTC (mirror steps east → west)"
        )
        committed = scan + (step >= args.substeps)
        tl_scatter.set_offsets(
            np.column_stack([hours[:committed], np.full(committed, 0.4)])
            if committed
            else np.empty((0, 2))
        )
        tl_text.set_text(f"len(time) = {committed}")
        return image, sweep_line, title, tl_scatter, tl_text

    total = n_scans * per_scan
    anim = FuncAnimation(fig, draw, frames=total, blit=False)
    print(f"Rendering {total} frames to {out_path}...")
    save_animation(anim, out_path, args.fps)
    return 0


if __name__ == "__main__":
    sys.exit(main())
