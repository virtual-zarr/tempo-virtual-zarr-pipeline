"""Tests for virtualizarr_processor.store_template.

The template utilities declare an Icechunk/Zarr store schema once (as a
pydantic-zarr GroupSpec), materialize it as an empty (metadata-only) store,
and validate that an existing store conforms to it.
"""

import icechunk
import pytest
import zarr
from pydantic_zarr.v3 import ArraySpec, GroupSpec
from virtualizarr_processor.store_template import (
    StoreValidationError,
    create_empty_store,
    resize,
    validate_store,
)

TIME, Y, X = 4, 2, 3


def array_spec(
    shape: tuple[int, ...],
    chunks: tuple[int, ...],
    dtype: str,
    dims: tuple[str, ...],
    fill_value: object = 0,
) -> ArraySpec:
    return ArraySpec(
        attributes={},
        shape=shape,
        data_type=dtype,
        chunk_grid={"name": "regular", "configuration": {"chunk_shape": chunks}},
        chunk_key_encoding={"name": "default", "configuration": {"separator": "/"}},
        fill_value=fill_value,
        codecs=({"name": "bytes", "configuration": {"endian": "little"}},),
        dimension_names=dims,
    )


def template() -> GroupSpec:
    """A miniature TEMPO-shaped template: root coords plus a product group."""
    return GroupSpec.from_flat(
        {
            "": GroupSpec(attributes={"title": "test"}, members=None),
            "/time": array_spec((TIME,), (TIME,), "int64", ("time",)),
            "/latitude": array_spec((Y,), (Y,), "float32", ("latitude",)),
            "/product": GroupSpec(attributes={}, members=None),
            "/product/vertical_column": array_spec(
                (TIME, Y, X),
                (1, Y, X),
                "float64",
                ("time", "latitude", "longitude"),
                fill_value="NaN",
            ),
        }
    )


class TestCreateEmptyStore:
    def test_creates_full_hierarchy(self) -> None:
        store = zarr.storage.MemoryStore()

        root = create_empty_store(template(), store)

        assert root.attrs["title"] == "test"
        var = zarr.open_array(store, path="product/vertical_column")
        assert var.shape == (TIME, Y, X)
        assert var.chunks == (1, Y, X)
        assert var.dtype == "float64"
        assert var.metadata.dimension_names == ("time", "latitude", "longitude")

    def test_writes_no_chunk_data(self) -> None:
        store = zarr.storage.MemoryStore()

        create_empty_store(template(), store)

        for path in ("time", "latitude", "product/vertical_column"):
            assert zarr.open_array(store, path=path).nchunks_initialized == 0

    def test_existing_node_raises(self) -> None:
        store = zarr.storage.MemoryStore()
        zarr.create_group(store=store).create_array("time", shape=(1,), dtype="int32")

        with pytest.raises(Exception):
            create_empty_store(template(), store)

    def test_works_on_icechunk_and_survives_commit(
        self, backfill_repo: icechunk.Repository
    ) -> None:
        session = backfill_repo.writable_session("main")

        create_empty_store(template(), session.store, path="template")
        session.commit("template")

        readonly = backfill_repo.readonly_session("main")
        var = zarr.open_array(readonly.store, path="template/product/vertical_column")
        assert var.shape == (TIME, Y, X)


class TestValidateStore:
    def test_conforming_store_passes(self) -> None:
        store = zarr.storage.MemoryStore()
        create_empty_store(template(), store)

        validate_store(template(), zarr.open_group(store, mode="r"))

    def test_nan_fill_values_compare_equal(self) -> None:
        # NaN != NaN under ==; validation must not flag identical NaN fills.
        store = zarr.storage.MemoryStore()
        create_empty_store(template(), store)

        validate_store(template(), zarr.open_group(store, mode="r"))

    def test_missing_node_is_reported(self) -> None:
        store = zarr.storage.MemoryStore()
        incomplete = template()
        flat = {k: v for k, v in incomplete.to_flat().items() if k != "/latitude"}
        create_empty_store(GroupSpec.from_flat(flat), store)

        with pytest.raises(StoreValidationError, match="/latitude"):
            validate_store(template(), zarr.open_group(store, mode="r"))

    def test_extra_node_is_reported(self) -> None:
        store = zarr.storage.MemoryStore()
        create_empty_store(template(), store)
        zarr.open_group(store).create_array("extra", shape=(1,), dtype="int8")

        with pytest.raises(StoreValidationError, match="/extra"):
            validate_store(template(), zarr.open_group(store, mode="r"))

    def test_extra_node_tolerated_with_allow_extra(self) -> None:
        store = zarr.storage.MemoryStore()
        create_empty_store(template(), store)
        zarr.open_group(store).create_array("extra", shape=(1,), dtype="int8")

        validate_store(template(), zarr.open_group(store, mode="r"), allow_extra=True)

    def test_mismatched_field_names_path_and_field(self) -> None:
        store = zarr.storage.MemoryStore()
        flat = template().to_flat()
        flat["/time"] = array_spec((TIME,), (TIME,), "int32", ("time",))
        create_empty_store(GroupSpec.from_flat(flat), store)

        with pytest.raises(StoreValidationError) as excinfo:
            validate_store(template(), zarr.open_group(store, mode="r"))

        message = str(excinfo.value)
        assert "/time" in message
        assert "data_type" in message

    def test_validates_icechunk_committed_store(
        self, backfill_repo: icechunk.Repository
    ) -> None:
        session = backfill_repo.writable_session("main")
        create_empty_store(template(), session.store, path="template")
        session.commit("template")

        readonly = backfill_repo.readonly_session("main")
        validate_store(
            template(), zarr.open_group(readonly.store, path="template", mode="r")
        )


class TestResize:
    def test_resizes_named_dimension_everywhere(self) -> None:
        resized = resize(template(), {"time": 100})

        flat = resized.to_flat()
        time_spec = flat["/time"]
        var_spec = flat["/product/vertical_column"]
        assert isinstance(time_spec, ArraySpec)
        assert isinstance(var_spec, ArraySpec)
        assert time_spec.shape == (100,)
        assert var_spec.shape == (100, Y, X)

    def test_preserves_chunk_shapes_and_other_dims(self) -> None:
        resized = resize(template(), {"time": 100})

        flat = resized.to_flat()
        var_spec = flat["/product/vertical_column"]
        lat_spec = flat["/latitude"]
        assert isinstance(var_spec, ArraySpec)
        assert isinstance(lat_spec, ArraySpec)
        assert var_spec.chunk_grid["configuration"]["chunk_shape"] == (1, Y, X)
        assert lat_spec.shape == (Y,)

    def test_unknown_dimension_raises(self) -> None:
        with pytest.raises(ValueError, match="altitude"):
            resize(template(), {"altitude": 10})

    def test_original_spec_is_unchanged(self) -> None:
        spec = template()

        resize(spec, {"time": 100})

        time_spec = spec.to_flat()["/time"]
        assert isinstance(time_spec, ArraySpec)
        assert time_spec.shape == (TIME,)
