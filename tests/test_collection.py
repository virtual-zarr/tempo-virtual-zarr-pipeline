"""Tests for the declarative per-collection configuration."""

import pytest
from virtualizarr_processor.collection import (
    CollectionConfig,
    load_collection,
    load_coordinates,
    load_template,
)
from virtualizarr_processor.store_template import TEMPO_L3_VOLATILE_ATTRIBUTES


def test_load_hcho() -> None:
    config = load_collection("hcho")
    assert config.name == "hcho"
    assert config.collection_shortname == "TEMPO_HCHO_L3"
    assert config.concept_id == "C3685897141-LARC_CLOUD"
    assert config.append_dim == "time"
    assert config.time_units == "seconds since 1980-01-06T00:00:00Z"
    assert config.flatten_groups == (
        "product",
        "geolocation",
        "qa_statistics",
        "support_data",
    )
    assert config.promote_to_time == ("weight",)
    assert config.drop_variables == ()


def test_load_no2() -> None:
    config = load_collection("no2")
    assert config.collection_shortname == "TEMPO_NO2_L3"
    assert config.concept_id == "C3685896708-LARC_CLOUD"


def test_unknown_collection_raises() -> None:
    with pytest.raises(ValueError, match="nope"):
        load_collection("nope")


def test_env_selection(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEMPO_COLLECTION", "no2")
    assert load_collection().name == "no2"


def test_no_env_no_name_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TEMPO_COLLECTION", raising=False)
    with pytest.raises(ValueError, match="TEMPO_COLLECTION"):
        load_collection()


def test_volatile_attributes_include_base_list_and_geospatial_bounds() -> None:
    config = load_collection("hcho")
    assert TEMPO_L3_VOLATILE_ATTRIBUTES <= config.volatile_attributes
    # The four per-granule geospatial bounds vary per scan (see
    # context/findings.txt DIFFERENCES) and must be volatile or every
    # granule but the reference would be rejected.
    assert {
        "geospatial_lat_min",
        "geospatial_lat_max",
        "geospatial_lon_min",
        "geospatial_lon_max",
    } <= config.volatile_attributes


def test_config_is_immutable() -> None:
    config = load_collection("hcho")
    with pytest.raises(Exception):
        config.name = "other"  # type: ignore[misc]


def _skip_unless_artifact(config: CollectionConfig, filename: str) -> None:
    from importlib.resources import files

    resource = files("virtualizarr_processor") / "collections" / filename
    if not resource.is_file():
        pytest.skip(f"template artifact {filename} not generated yet (Task 5)")


def test_load_template_returns_group_spec() -> None:
    config = load_collection("hcho")
    _skip_unless_artifact(config, config.template_file)
    spec = load_template(config)
    flat = spec.to_flat()
    # Full-granule spatial shape on the primary variable, with a time dim.
    array = flat["/vertical_column"]
    assert array.dimension_names == ("time", "latitude", "longitude")
    assert array.shape[1:] == (2950, 7750)


def test_load_coordinates_reference_grid() -> None:
    config = load_collection("hcho")
    _skip_unless_artifact(config, config.coordinates_file)
    coords = load_coordinates(config)
    assert coords["latitude"].shape == (2950,)
    assert coords["longitude"].shape == (7750,)
    assert str(coords["latitude"].dtype) == "float32"


def test_template_missing_file_names_it(tmp_path: object) -> None:
    config = load_collection("hcho").model_copy(
        update={"template_file": "does_not_exist.json"}
    )
    with pytest.raises(FileNotFoundError, match="does_not_exist.json"):
        load_template(config)


def test_coordinates_missing_file_names_it() -> None:
    config = load_collection("hcho").model_copy(
        update={"coordinates_file": "does_not_exist.npz"}
    )
    with pytest.raises(FileNotFoundError, match="does_not_exist.npz"):
        load_coordinates(config)


def test_configs_declare_distinct_templates() -> None:
    assert load_collection("hcho").template_file != load_collection("no2").template_file
    assert isinstance(load_collection("hcho"), CollectionConfig)


def test_time_chunk_size_declared() -> None:
    assert load_collection("hcho").time_chunk_size == 16384
    assert load_collection("no2").time_chunk_size == 16384
