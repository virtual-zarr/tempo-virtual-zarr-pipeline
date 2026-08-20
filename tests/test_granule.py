"""Tests for granule parsing and the flatten/promote transform."""

import pathlib

import h5py
import numpy as np
import pytest
import xarray as xr
from tempo_fixtures import TINY_LAT, TINY_LON, write_tempo_granule
from virtualizarr.manifests import ManifestArray
from virtualizarr_processor.collection import CollectionConfig, load_collection
from virtualizarr_processor.granule import (
    granule_time,
    make_registry,
    open_flat_granule,
)
from virtualizarr_processor.store_template import GranuleValidationError

TIME_0 = 1471196538.0244286

# The synthetic fixtures only carry a subset of the real groups' variables.
TINY_CONFIG_UPDATE = {
    "flatten_groups": ("product", "geolocation", "support_data"),
}


@pytest.fixture()
def tiny_config() -> CollectionConfig:
    return load_collection("hcho").model_copy(update=TINY_CONFIG_UPDATE)


def granule_url(directory: pathlib.Path, **kwargs: object) -> str:
    path = write_tempo_granule(directory / "granule.nc", **kwargs)  # type: ignore[arg-type]
    return f"file://{path}"


def test_flatten_and_promote(
    tempo_granule_dir: pathlib.Path, tiny_config: CollectionConfig
) -> None:
    vds = open_flat_granule(
        granule_url(tempo_granule_dir, time_value=TIME_0), tiny_config
    )
    assert set(vds.data_vars) == {
        "weight",
        "vertical_column",
        "main_data_quality_flag",
        "solar_zenith_angle",
        "surface_pressure",
    }
    for name, var in vds.data_vars.items():
        assert var.dims == ("time", "latitude", "longitude"), name
        assert isinstance(var.data, ManifestArray), f"{name} should stay virtual"
    assert set(vds.coords) == {"time", "latitude", "longitude"}
    assert vds.attrs["collection_shortname"] == "TEMPO_HCHO_L3"
    # Variable attrs survive the flatten.
    assert vds["vertical_column"].attrs["units"] == "molecules/cm^2"
    np.testing.assert_array_equal(vds["latitude"].values, TINY_LAT)
    np.testing.assert_array_equal(vds["longitude"].values, TINY_LON)


def test_missing_group_raises(
    tempo_granule_dir: pathlib.Path, tiny_config: CollectionConfig
) -> None:
    config = tiny_config.model_copy(
        update={"flatten_groups": ("product", "qa_statistics")}
    )
    with pytest.raises(GranuleValidationError, match="qa_statistics"):
        open_flat_granule(granule_url(tempo_granule_dir, time_value=TIME_0), config)


def test_name_collision_raises(
    tempo_granule_dir: pathlib.Path, tiny_config: CollectionConfig
) -> None:
    url = granule_url(tempo_granule_dir, time_value=TIME_0)
    with h5py.File(url.removeprefix("file://"), "a") as f:
        f["product"]["weight"] = np.zeros(3, dtype="float32")
    with pytest.raises(GranuleValidationError, match="weight"):
        open_flat_granule(url, tiny_config)


def test_unpromoted_time_invariant_variable_raises(
    tempo_granule_dir: pathlib.Path, tiny_config: CollectionConfig
) -> None:
    """A data variable without the time dim that the config does not promote
    would silently freeze one scan's values under concat — reject it."""
    url = granule_url(tempo_granule_dir, time_value=TIME_0)
    with h5py.File(url.removeprefix("file://"), "a") as f:
        ds = f["support_data"].create_dataset(
            "static_map", data=np.zeros((4, 6), dtype="float32")
        )
        ds.dims[0].attach_scale(f["latitude"])
        ds.dims[1].attach_scale(f["longitude"])
    with pytest.raises(GranuleValidationError, match="static_map"):
        open_flat_granule(url, tiny_config)


def test_drop_variables(
    tempo_granule_dir: pathlib.Path, tiny_config: CollectionConfig
) -> None:
    config = tiny_config.model_copy(update={"drop_variables": ("solar_zenith_angle",)})
    vds = open_flat_granule(granule_url(tempo_granule_dir, time_value=TIME_0), config)
    assert "solar_zenith_angle" not in vds


def test_granule_time_exact(
    tempo_granule_dir: pathlib.Path, tiny_config: CollectionConfig
) -> None:
    vds = open_flat_granule(
        granule_url(tempo_granule_dir, time_value=TIME_0), tiny_config
    )
    assert granule_time(vds) == TIME_0


def test_granule_time_epoch_attr_mismatch_raises(
    tempo_granule_dir: pathlib.Path, tiny_config: CollectionConfig
) -> None:
    url = granule_url(tempo_granule_dir, time_value=TIME_0)
    with h5py.File(url.removeprefix("file://"), "a") as f:
        f.attrs["time_coverage_start_since_epoch"] = np.array([TIME_0 + 1.0])
    vds = open_flat_granule(url, tiny_config)
    with pytest.raises(GranuleValidationError, match="time_coverage_start_since_epoch"):
        granule_time(vds)


def test_granule_time_missing_epoch_attr_raises(
    tempo_granule_dir: pathlib.Path,
) -> None:
    vds = xr.Dataset(
        {"foo": (("time",), np.zeros(1))}, coords={"time": ("time", [TIME_0])}
    )
    with pytest.raises(GranuleValidationError, match="time_coverage_start_since_epoch"):
        granule_time(vds)


def test_granule_time_requires_single_step() -> None:
    vds = xr.Dataset(
        {"foo": (("time",), np.zeros(2))},
        coords={"time": ("time", [TIME_0, TIME_0 + 1])},
        attrs={"time_coverage_start_since_epoch": np.array([TIME_0])},
    )
    with pytest.raises(GranuleValidationError, match="exactly one"):
        granule_time(vds)


def test_make_registry_schemes(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = make_registry("file:///tmp/x.nc")
    store, path = registry.resolve("file:///tmp/x.nc")
    assert path == "tmp/x.nc"
    monkeypatch.setenv("EARTHDATA_TOKEN", "token123")
    registry = make_registry("https://data.asdc.earthdata.nasa.gov/a/b.nc")
    store, path = registry.resolve("https://data.asdc.earthdata.nasa.gov/a/b.nc")
    assert path == "a/b.nc"
    monkeypatch.setenv("AWS_REGION", "us-west-2")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "x")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "y")
    registry = make_registry("s3://asdc-prod-protected/TEMPO/g.nc")
    store, path = registry.resolve("s3://asdc-prod-protected/TEMPO/g.nc")
    assert path == "TEMPO/g.nc"
    with pytest.raises(ValueError, match="scheme"):
        make_registry("ftp://nope/g.nc")


# --- Real-granule integration (skipped when the context data is absent) ---


def test_real_granules_flatten(real_data_dir: pathlib.Path) -> None:
    hcho_path = sorted(real_data_dir.glob("TEMPO_HCHO_L3_*.nc"))[0]
    no2_path = sorted(real_data_dir.glob("TEMPO_NO2_L3_*.nc"))[0]

    def expected_names(path: pathlib.Path, groups: tuple[str, ...]) -> set[str]:
        with h5py.File(path) as f:
            names = {"weight"}
            for group in groups:
                names |= set(f[group].keys())
        return names

    for name, path in [("hcho", hcho_path), ("no2", no2_path)]:
        config = load_collection(name)
        vds = open_flat_granule(f"file://{path}", config)
        assert set(vds.data_vars) == expected_names(path, config.flatten_groups)
        for var_name, var in vds.data_vars.items():
            assert var.dims == ("time", "latitude", "longitude"), var_name
        assert granule_time(vds) == float(vds["time"].values[0])
        assert vds.sizes["latitude"] == 2950
        assert vds.sizes["longitude"] == 7750
