# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "earthaccess-auth[icechunk]>=0.4.0",
#     "icechunk",
#     "matplotlib",
#     "numpy",
#     "xarray",
#     "zarr",
# ]
# ///
"""Visually QA a published store against the imagery NASA serves to the public.

TEMPO's public data page (https://tempo.si.edu/data_for_public.html) sends
readers to Worldview, which draws the L3 products from GIBS. This script
renders one scan of the Source Cooperative store through GIBS's own
colormap and sets it beside the GIBS image of the same scan, on the same
pixel grid, with a third panel of the per-pixel difference in colormap bins.
The two should look the same; a shifted grid, a wrong scan, a scale or fill
problem, or a missing region shows up at a glance.

GIBS draws only the pixels that pass a quality screen. The store panel
applies the screen that reproduces it (``main_data_quality_flag`` <= 1,
``eff_cloud_fraction`` < 0.5 and ``solar_zenith_angle`` < 80, matched
empirically to within ~1% of pixels for both collections, on a midday and
an early-morning scan), so pixels only the store has (orange in the third
panel) or only GIBS has (black) should be scattered specks, not regions.
Thin vertical stripes of ±1 bin are GIBS resampling its tiles onto this
grid, not the store.

The store is read anonymously through Source Coop's proxy. Its chunks are
virtual references into ASDC's bucket, so reading them needs Earthdata
credentials (EARTHDATA_TOKEN, or EARTHDATA_USERNAME and EARTHDATA_PASSWORD)
and an in-region host such as the VEDA JupyterHub; S3 access to
asdc-prod-protected is refused from outside us-west-2.

Usage:
    uv run scripts/compare_to_gibs.py --collection no2
    uv run scripts/compare_to_gibs.py --collection hcho --time 2026-10-01T18:00
    uv run scripts/compare_to_gibs.py --collection no2 --bbox -125 24 -66 50
"""

from __future__ import annotations

import argparse
import io
import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from typing import Any

import numpy as np

GIBS = "https://gibs.earthdata.nasa.gov"
# collection -> (store variable, GIBS layer, GIBS colormap)
LAYERS = {
    "no2": (
        "vertical_column_troposphere",
        "TEMPO_L3_NO2_Vertical_Column_Troposphere",
        "TEMPO_NO2_Vertical_Column_Troposphere",
    ),
    "hcho": (
        "vertical_column",
        "TEMPO_L3_Formaldehyde_Vertical_Column",
        "TEMPO_HCHO_Vertical_Column",
    ),
}
# The store's time axis is GPS seconds, which xarray decodes as if they were
# UTC; GIBS and CMR are keyed by UTC scan start.
# ponytail: leap seconds fixed at 18 (true since 2017); add one if IERS ever does.
GPS_MINUS_UTC = np.timedelta64(18, "s")
NO_DATA, OFF_PALETTE = -1, -2


def screen(
    values: np.ndarray, flag: np.ndarray, cloud: np.ndarray, sza: np.ndarray
) -> np.ndarray:
    """``values`` with NaN wherever GIBS would not draw the pixel."""
    # ponytail: thresholds fitted to GIBS output, not documented by GIBS; refit if
    # the masks stop matching.
    return np.where((flag <= 1) & (cloud < 0.5) & (sza < 80), values, np.nan)


def fetch(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=300) as response:
        return bytes(response.read())


def parse_colormap(xml: bytes) -> tuple[np.ndarray, np.ndarray]:
    """Upper bin edges and their RGB colors from a GIBS v1.3 colormap.

    Entries read ``[lo,hi)``; the first is ``[0)`` (below zero). Values
    past the last edge take the last color.
    """
    edges, colors = [], []
    for entry in ET.fromstring(xml).iter("ColorMapEntry"):
        if entry.get("nodata") == "true":
            continue
        edges.append(float(entry.get("value", "").strip("[]()").split(",")[-1]))
        colors.append([int(c) for c in entry.get("rgb", "").split(",")])
    return np.array(edges), np.array(colors, dtype=np.uint8)


def value_bins(values: np.ndarray, edges: np.ndarray) -> np.ndarray:
    """Colormap bin of each value, NO_DATA where it is NaN."""
    bins = np.searchsorted(edges, values, side="right").clip(0, len(edges) - 1)
    return np.where(np.isnan(values), NO_DATA, bins)


def color_bins(rgba: np.ndarray, colors: np.ndarray) -> np.ndarray:
    """Colormap bin of each GIBS pixel: NO_DATA where transparent,
    OFF_PALETTE where its color is not in the colormap."""
    lookup = {tuple(c): i for i, c in enumerate(colors.tolist())}
    rgb = rgba[..., :3].reshape(-1, 3)
    bins = np.array([lookup.get(tuple(c), OFF_PALETTE) for c in rgb.tolist()])
    return np.where(rgba[..., 3].ravel() == 0, NO_DATA, bins).reshape(rgba.shape[:2])


def gibs_rgba(
    layer: str, time: np.datetime64, bbox: list[float], shape: tuple[int, int]
) -> np.ndarray:
    """GIBS's image of ``layer`` at ``time``, one pixel per store cell,
    south row first like the store."""
    import matplotlib.image

    query = urllib.parse.urlencode(
        {
            "SERVICE": "WMS",
            "VERSION": "1.1.1",  # lon,lat axis order for EPSG:4326
            "REQUEST": "GetMap",
            "LAYERS": layer,
            "STYLES": "",
            "SRS": "EPSG:4326",
            "BBOX": ",".join(f"{v:.6f}" for v in bbox),
            "WIDTH": shape[1],
            "HEIGHT": shape[0],
            "FORMAT": "image/png",
            "TRANSPARENT": "TRUE",
            "TIME": f"{np.datetime_as_string(time, unit='s')}Z",
        }
    )
    png = fetch(f"{GIBS}/wms/epsg4326/best/wms.cgi?{query}")
    image = matplotlib.image.imread(io.BytesIO(png), format="png")
    return np.flipud((image * 255).round().astype(np.uint8))


def open_store(collection: str) -> Any:
    import icechunk
    import xarray as xr
    from earthaccess_auth.adapters.icechunk import earthdata_containers_credentials

    storage = icechunk.s3_storage(
        bucket="pangeo",
        prefix=f"tempo-virtual-icechunk/tempo/{collection}/v04",
        endpoint_url="https://data.source.coop",
        region="us-west-2",
        anonymous=True,
        force_path_style=True,
    )
    repo = icechunk.Repository.open(storage)
    repo = repo.reopen(
        authorize_virtual_chunk_access=earthdata_containers_credentials(repo)
    )
    return xr.open_zarr(repo.readonly_session("main").store, consolidated=False)


def plot(
    path: str,
    title: str,
    gibs: np.ndarray,
    gibs_bin: np.ndarray,
    store_bin: np.ndarray,
    colors: np.ndarray,
    extent: list[float],
) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import ListedColormap

    both = (gibs_bin >= 0) & (store_bin >= 0)
    diff = np.ma.masked_where(~both, store_bin - gibs_bin)
    coverage = np.zeros(gibs_bin.shape + (4,))
    coverage[(store_bin >= 0) & (gibs_bin == NO_DATA)] = (0.95, 0.55, 0.1, 1)
    coverage[(store_bin == NO_DATA) & (gibs_bin != NO_DATA)] = (0, 0, 0, 1)

    fig, axes = plt.subplots(3, 1, figsize=(14, 17), constrained_layout=True)
    show: dict[str, Any] = {
        "origin": "lower",
        "extent": extent,
        "interpolation": "nearest",
    }
    axes[0].imshow(gibs, **show)
    axes[0].set_title("GIBS (what Worldview shows)")
    palette = ListedColormap(colors / 255)
    axes[1].imshow(
        np.ma.masked_less(store_bin, 0),
        cmap=palette,
        vmin=0,
        vmax=len(colors) - 1,
        **show,
    )
    axes[1].set_title("Source Coop store, GIBS colormap")
    image = axes[2].imshow(diff, cmap="RdBu_r", vmin=-5, vmax=5, **show)
    axes[2].imshow(coverage, **show)
    axes[2].set_title("store bin − GIBS bin   (orange: store only, black: GIBS only)")
    fig.colorbar(
        image, ax=axes[2], location="bottom", shrink=0.4, label="colormap bins"
    )
    for ax in axes:
        ax.set_facecolor("#dddddd")
        ax.set_aspect(1 / np.cos(np.deg2rad((extent[2] + extent[3]) / 2)))
    fig.suptitle(title)
    fig.savefig(path, dpi=110)
    plt.close(fig)


def compare(
    collection: str,
    values: np.ndarray,
    lat: np.ndarray,
    lon: np.ndarray,
    utc: np.datetime64,
    out: str,
) -> int:
    """Fetch GIBS for the scan in ``values`` (ascending lat, lon), plot, report."""
    _, layer, colormap = LAYERS[collection]
    edges, colors = parse_colormap(fetch(f"{GIBS}/colormaps/v1.3/{colormap}.xml"))
    half = abs(float(lat[1] - lat[0])) / 2
    bbox = [
        float(lon[0]) - half,
        float(lat[0]) - half,
        float(lon[-1]) + half,
        float(lat[-1]) + half,
    ]
    gibs = gibs_rgba(layer, utc, bbox, values.shape)
    gibs_bin = color_bins(gibs, colors)
    store_bin = value_bins(values, edges)

    both = (gibs_bin >= 0) & (store_bin >= 0)
    diff = np.abs(store_bin - gibs_bin)[both]
    counts = {
        "both": int(both.sum()),
        "store only": int(((store_bin >= 0) & (gibs_bin == NO_DATA)).sum()),
        "GIBS only": int(((store_bin == NO_DATA) & (gibs_bin != NO_DATA)).sum()),
        "GIBS off-palette": int((gibs_bin == OFF_PALETTE).sum()),
    }
    print(f"{collection} scan {utc}Z  {layer}")
    print("  pixels: " + ", ".join(f"{k} {v:,}" for k, v in counts.items()))
    if diff.size:
        same, near = np.mean(diff == 0), np.mean(diff <= 1)
        print(f"  same bin {same:.1%}, within 1 bin {near:.1%}")
    title = f"{collection.upper()} {LAYERS[collection][0]}  scan {utc}Z"
    plot(
        out,
        title,
        gibs,
        gibs_bin,
        store_bin,
        colors,
        [bbox[0], bbox[2], bbox[1], bbox[3]],
    )
    print(f"  wrote {out}")
    if not counts["both"]:
        print(
            "  no pixels in common: GIBS has nothing for this scan or box",
            file=sys.stderr,
        )
        return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--collection", choices=sorted(LAYERS), default="no2")
    parser.add_argument(
        "--time",
        help="UTC; the scan in progress then, as Worldview picks it (default: latest)",
    )
    parser.add_argument("--bbox", nargs=4, type=float, metavar=("W", "S", "E", "N"))
    parser.add_argument(
        "--out", help="PNG path (default: gibs-<collection>-<scan>.png)"
    )
    args = parser.parse_args()

    variable = LAYERS[args.collection][0]
    da = open_store(args.collection)[
        [variable, "main_data_quality_flag", "eff_cloud_fraction", "solar_zenith_angle"]
    ]
    if args.time:
        # Worldview shows the scan whose start precedes the time, not the nearest;
        # compare whole seconds, as GIBS keys scans (the axis has fractions).
        starts = (da.time.values - GPS_MINUS_UTC).astype("datetime64[s]")
        index = np.searchsorted(starts, np.datetime64(args.time, "s"), side="right")
        if index == 0:
            parser.error(f"--time {args.time} is before the first scan")
        da = da.isel(time=index - 1)
    else:
        da = da.isel(time=-1)
    if args.bbox:
        w, s, e, n = args.bbox
        da = da.sel(latitude=slice(s, n), longitude=slice(w, e))
    utc = (da.time.values - GPS_MINUS_UTC).astype("datetime64[s]")
    stamp = np.datetime_as_string(utc, unit="s").replace(":", "")
    out = args.out or f"gibs-{args.collection}-{stamp}.png"
    values = screen(
        da[variable].values,
        da.main_data_quality_flag.values,
        da.eff_cloud_fraction.values,
        da.solar_zenith_angle.values,
    )
    return compare(
        args.collection, values, da.latitude.values, da.longitude.values, utc, out
    )


if __name__ == "__main__":
    sys.exit(main())
