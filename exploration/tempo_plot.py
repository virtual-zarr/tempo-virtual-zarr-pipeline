"""Shared map-plotting helper for the exploration scripts.

Importable from sibling scripts because ``uv run exploration/<script>.py`` puts the
script's directory on ``sys.path``.
"""

from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import xarray as xr


def save_map_png(da: xr.DataArray, path: str | Path) -> Path:
    """Render a 2D (latitude, longitude) slice as a PNG map and return its path.

    Uses robust (2nd-98th percentile) color limits so retrieval noise and outliers
    don't wash out the map, a single-hue sequential colormap for the column
    magnitude, and neutral gray for pixels with no retrieval.
    """
    cmap = plt.get_cmap("Blues").copy()
    cmap.set_bad("#ececec")

    fig, ax = plt.subplots(figsize=(12, 6), constrained_layout=True)
    da.plot(ax=ax, cmap=cmap, robust=True, cbar_kwargs={"shrink": 0.85})

    # Approximate geographic aspect for an unprojected lat/lon grid.
    mid_lat = float(da["latitude"].mean())
    ax.set_aspect(1 / np.cos(np.deg2rad(mid_lat)))

    title = da.attrs.get("long_name", da.name)
    time_coord = da.coords.get("time")
    if time_coord is not None:
        title = f"{title}\n{np.datetime_as_string(time_coord.values, unit='m')} UTC"
    ax.set_title(title)

    path = Path(path)
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path
