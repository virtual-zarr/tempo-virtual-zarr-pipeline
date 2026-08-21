"""Tiny synthetic TEMPO L3 granules for fast, offline tests.

Mimics the real layout at a 4x6 grid: root ``time``/``latitude``/
``longitude`` dimension scales (contiguous, uncompressed), a per-scan 2-D
``weight`` (no time dimension — the promotion case), and the four data
groups with shuffle+deflate chunked 3-D variables. Root attributes carry
the shared TEMPO identity plus per-granule volatile ones, with
``time_coverage_start_since_epoch`` equal to ``/time[0]`` exactly, as in
the real files. Data values are deterministic functions of ``time_value``
so read-back can be asserted per scan.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import h5py
import numpy as np

if TYPE_CHECKING:
    from virtualizarr_processor.inventory import BackfillInventory

TINY_LAT = np.linspace(14.01, 72.99, 4, dtype="float32")
TINY_LON = np.linspace(-167.99, -13.01, 6, dtype="float32")

# The TEMPO epoch: 1980-01-06T00:00:00Z (GPS epoch).
TIME_UNITS = "seconds since 1980-01-06T00:00:00Z"

SHARED_ROOT_ATTRS: dict[str, Any] = {
    "Conventions": "CF-1.6, ACDD-1.3",
    "project": "TEMPO",
    "platform": "Intelsat 40e",
    "processing_level": "3",
    "time_reference": "1980-01-06T00:00:00Z",
}


def expected_vertical_column(time_value: float) -> np.ndarray:
    """The deterministic (1, 4, 6) payload written for ``time_value``."""
    base = float(time_value) % 1.0e5
    grid = base + 10.0 * np.arange(4)[:, None] + np.arange(6)[None, :]
    return grid[None, :, :].astype("float64")


def expected_weight(time_value: float, weight_scale: float) -> np.ndarray:
    grid = np.add.outer(np.arange(4), np.arange(6)) + 1.0
    return (weight_scale * (float(time_value) % 1.0e5) * grid).astype("float32")


def write_tempo_granule(
    path: Path,
    *,
    time_value: float,
    collection_shortname: str = "TEMPO_HCHO_L3",
    lat: np.ndarray = TINY_LAT,
    lon: np.ndarray = TINY_LON,
    attrs: dict[str, Any] | None = None,
    weight_scale: float = 1.0,
) -> Path:
    """Write one synthetic granule; returns ``path``."""
    ny, nx = lat.size, lon.size
    with h5py.File(path, "w") as f:
        f.attrs["collection_shortname"] = collection_shortname
        f.attrs["shortname"] = collection_shortname
        for key, value in SHARED_ROOT_ATTRS.items():
            f.attrs[key] = value
        # Volatile per-granule attributes, as in the real files.
        f.attrs["history"] = f"produced for test at {time_value}"
        f.attrs["time_coverage_start_since_epoch"] = np.array([time_value])
        f.attrs["geospatial_lat_min"] = np.array([float(lat[0])])
        f.attrs["geospatial_lat_max"] = np.array([float(lat[-1])])
        f.attrs["geospatial_lon_min"] = np.array([float(lon[0])])
        f.attrs["geospatial_lon_max"] = np.array([float(lon[-1])])
        for key, value in (attrs or {}).items():
            f.attrs[key] = value

        time_v = f.create_dataset("time", data=np.array([time_value], dtype="float64"))
        time_v.attrs["units"] = TIME_UNITS
        time_v.attrs["standard_name"] = "time"
        time_v.attrs["calendar"] = "gregorian"
        time_v.make_scale("time")

        lat_v = f.create_dataset("latitude", data=lat)
        lat_v.attrs["units"] = "degrees_north"
        lat_v.attrs["standard_name"] = "latitude"
        lat_v.make_scale("latitude")

        lon_v = f.create_dataset("longitude", data=lon)
        lon_v.attrs["units"] = "degrees_east"
        lon_v.attrs["standard_name"] = "longitude"
        lon_v.make_scale("longitude")

        def make_3d(
            group: h5py.Group,
            name: str,
            data: np.ndarray,
            fill: Any,
            var_attrs: dict[str, Any] | None = None,
        ) -> None:
            ds = group.create_dataset(
                name,
                data=data,
                chunks=(1, max(1, ny // 2), max(1, nx // 2)),
                shuffle=True,
                compression="gzip",
                compression_opts=1,
                fillvalue=fill,
            )
            ds.attrs["_FillValue"] = np.array([fill], dtype=data.dtype)
            ds.attrs["coordinates"] = "time latitude longitude"
            for key, value in (var_attrs or {}).items():
                ds.attrs[key] = value
            ds.dims[0].attach_scale(time_v)
            ds.dims[1].attach_scale(lat_v)
            ds.dims[2].attach_scale(lon_v)

        weight = f.create_dataset(
            "weight",
            data=expected_weight(time_value, weight_scale),
            chunks=(ny, nx),
            shuffle=True,
            compression="gzip",
            compression_opts=1,
            fillvalue=np.float32(9.96921e36),
        )
        weight.attrs["_FillValue"] = np.array([9.96921e36], dtype="float32")
        weight.attrs["units"] = "km^2"
        weight.dims[0].attach_scale(lat_v)
        weight.dims[1].attach_scale(lon_v)

        product = f.create_group("product")
        make_3d(
            product,
            "vertical_column",
            expected_vertical_column(time_value),
            -1.0e30,
            {"units": "molecules/cm^2", "long_name": "vertical column"},
        )
        make_3d(
            product,
            "main_data_quality_flag",
            np.zeros((1, ny, nx), dtype="int16"),
            np.int16(-9999),
            {"flag_meanings": "normal suspicious bad"},
        )
        geolocation = f.create_group("geolocation")
        make_3d(
            geolocation,
            "solar_zenith_angle",
            np.full((1, ny, nx), 30.0, dtype="float32"),
            np.float32(-1.0e30),
            {"units": "degrees"},
        )
        support = f.create_group("support_data")
        make_3d(
            support,
            "surface_pressure",
            np.full((1, ny, nx), 1000.0, dtype="float32"),
            np.float32(-1.0e30),
            {"units": "hPa"},
        )
    return path


TIME_BASE = 1471196538.0244286
TINY_TOML = """\
name = "tiny-hcho"
collection_shortname = "TEMPO_HCHO_L3"
concept_id = "C3685897141-LARC_CLOUD"
append_dim = "time"
time_units = "seconds since 1980-01-06T00:00:00Z"
flatten_groups = ["product", "geolocation", "support_data"]
promote_to_time = ["weight"]
drop_variables = []
extra_volatile_attributes = [
    "geospatial_lat_min",
    "geospatial_lat_max",
    "geospatial_lon_min",
    "geospatial_lon_max",
]
time_chunk_size = 8
template_file = "{template_file}"
coordinates_file = "{coordinates_file}"
"""


@dataclass
class TinyCollection:
    """A complete miniature collection: granules, config, artifacts, inventory."""

    config_path: Path
    granule_paths: list[Path]
    urls: list[str]
    inventory: "BackfillInventory"

    @property
    def times(self) -> list[float]:
        return [entry.time for entry in self.inventory.granules]


def build_tiny_collection(
    directory: Path, n: int = 3, spacing: float = 3600.0
) -> TinyCollection:
    """Build granules, a config TOML, generated artifacts, and an inventory."""
    from virtualizarr_processor.collection import load_collection
    from virtualizarr_processor.inventory import BackfillInventory, GranuleEntry
    from virtualizarr_processor.template import build_template

    directory.mkdir(parents=True, exist_ok=True)
    times = [TIME_BASE + i * spacing for i in range(n)]
    paths = [
        write_tempo_granule(
            directory / f"granule_{i}.nc", time_value=t, weight_scale=1.0 + i
        )
        for i, t in enumerate(times)
    ]

    config_path = directory / "tiny.toml"
    config_path.write_text(
        TINY_TOML.format(
            template_file=directory / "template.json",
            coordinates_file=directory / "coordinates.npz",
        )
    )
    config = load_collection(str(config_path))
    spec, coords = build_template(paths, config)
    (directory / "template.json").write_text(spec.model_dump_json(indent=1))
    with (directory / "coordinates.npz").open("wb") as f:
        np.savez(f, **coords)

    inventory = BackfillInventory(
        schema_id="tempo-backfill-inventory/1",
        collection="TEMPO_HCHO_L3",
        concept_id="C3685897141-LARC_CLOUD",
        time_units=TIME_UNITS,
        built_at="2026-08-20T00:00:00Z",
        granules=tuple(
            GranuleEntry(url=f"file://{path}", granule_ur=path.stem, time=t)
            for path, t in zip(paths, times)
        ),
    )
    return TinyCollection(
        config_path=config_path,
        granule_paths=paths,
        urls=[f"file://{p}" for p in paths],
        inventory=inventory,
    )
