"""Tests for the store-template generator."""

import pathlib

import numpy as np
import pytest
from tempo_fixtures import TINY_LAT, TINY_LON, write_tempo_granule
from virtualizarr_processor.collection import CollectionConfig, load_collection
from virtualizarr_processor.store_template import GranuleValidationError
from virtualizarr_processor.template import build_template

TIME_0 = 1471196538.0244286


@pytest.fixture()
def tiny_config() -> CollectionConfig:
    return load_collection("hcho").model_copy(
        update={
            "flatten_groups": ("product", "geolocation", "support_data"),
            "time_chunk_size": 7,
        }
    )


def granules(
    directory: pathlib.Path, n: int = 3, **kwargs: object
) -> list[pathlib.Path]:
    return [
        write_tempo_granule(
            directory / f"granule_{i}.nc",
            time_value=TIME_0 + 3600.0 * i,
            **kwargs,  # type: ignore[arg-type]
        )
        for i in range(n)
    ]


def test_template_structure(
    tempo_granule_dir: pathlib.Path, tiny_config: CollectionConfig
) -> None:
    spec, coords = build_template(granules(tempo_granule_dir), tiny_config)
    flat = spec.to_flat()
    for name in (
        "weight",
        "vertical_column",
        "main_data_quality_flag",
        "solar_zenith_angle",
        "surface_pressure",
    ):
        array = flat[f"/{name}"]
        assert array.dimension_names == ("time", "latitude", "longitude"), name
        assert array.shape == (1, 4, 6), name
    # The axis chunk override (a 1-granule store would chunk time (1,)).
    time_array = flat["/time"]
    assert tuple(time_array.chunk_grid["configuration"]["chunk_shape"]) == (7,)
    np.testing.assert_array_equal(coords["latitude"], TINY_LAT)
    np.testing.assert_array_equal(coords["longitude"], TINY_LON)


def test_template_attribute_policy(
    tempo_granule_dir: pathlib.Path, tiny_config: CollectionConfig
) -> None:
    spec, _ = build_template(granules(tempo_granule_dir), tiny_config)
    root_attrs = spec.to_flat()[""].attributes
    assert root_attrs["project"] == "TEMPO"  # shared attrs kept
    for volatile in (
        "history",
        "geospatial_lat_min",
        "time_coverage_start_since_epoch",
    ):
        assert volatile not in root_attrs, volatile
    # Variable attributes are declared too, including the fill value
    # readers need for masking; the copies xarray adds to the root group
    # and the coordinate arrays are not.
    variable_attrs = spec.to_flat()["/vertical_column"].attributes
    assert variable_attrs["units"] == "molecules/cm^2"
    assert "_FillValue" in variable_attrs
    assert "_FillValue" not in spec.to_flat()["/time"].attributes
    assert "coordinates" not in root_attrs


def test_divergent_shared_attribute_is_a_hard_error(
    tempo_granule_dir: pathlib.Path, tiny_config: CollectionConfig
) -> None:
    paths = granules(tempo_granule_dir, n=2)
    paths.append(
        write_tempo_granule(
            tempo_granule_dir / "divergent.nc",
            time_value=TIME_0 + 3600.0 * 5,
            attrs={"project": "NOT-TEMPO"},
        )
    )
    with pytest.raises(GranuleValidationError, match="project"):
        build_template(paths, tiny_config)


def test_divergent_grid_is_a_hard_error(
    tempo_granule_dir: pathlib.Path, tiny_config: CollectionConfig
) -> None:
    paths = granules(tempo_granule_dir, n=2)
    shifted = TINY_LAT + np.float32(0.01)
    paths.append(
        write_tempo_granule(
            tempo_granule_dir / "shifted.nc",
            time_value=TIME_0 + 3600.0 * 5,
            lat=shifted,
        )
    )
    with pytest.raises(GranuleValidationError, match="latitude"):
        build_template(paths, tiny_config)


def test_template_is_deterministic(
    tempo_granule_dir: pathlib.Path, tiny_config: CollectionConfig
) -> None:
    paths = granules(tempo_granule_dir)
    spec_a, _ = build_template(paths, tiny_config)
    spec_b, _ = build_template(paths, tiny_config)
    assert spec_a.model_dump_json(indent=1) == spec_b.model_dump_json(indent=1)
