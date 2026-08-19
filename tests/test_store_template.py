"""Tests for virtualizarr_processor.store_template.

The template utilities declare an Icechunk/Zarr store schema once (as a
pydantic-zarr GroupSpec), materialize it as an empty (metadata-only) store,
and validate that an existing store conforms to it.
"""

import icechunk
import numpy as np
import pytest
import xarray as xr
import zarr
from pydantic_zarr.v3 import ArraySpec, GroupSpec
from virtualizarr_processor.store_template import (
    TEMPO_L3_VOLATILE_ATTRIBUTES,
    GranuleValidationError,
    StoreValidationError,
    create_empty_store,
    resize,
    strip_attributes,
    validate_granule,
    validate_store,
)

TIME, Y, X = 4, 2, 3


def array_spec(
    shape: tuple[int, ...],
    chunks: tuple[int, ...],
    dtype: str,
    dims: tuple[str, ...],
    fill_value: object = 0,
    attributes: dict | None = None,
) -> ArraySpec:
    return ArraySpec(
        attributes=attributes or {},
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


def granule_template() -> GroupSpec:
    """A flat template carrying shared attributes, as a real one would after
    strip_attributes() on a reference-granule spec."""
    return GroupSpec.from_flat(
        {
            "": GroupSpec(
                attributes={"title": "TEMPO test", "platform": "TEMPO"},
                members=None,
            ),
            "/latitude": array_spec((Y,), (Y,), "float32", ("latitude",)),
            "/vertical_column": array_spec(
                (TIME, Y, X),
                (1, Y, X),
                "float64",
                ("time", "latitude", "longitude"),
                attributes={"units": "molecules/cm^2"},
            ),
        }
    )


LATITUDE = np.arange(Y, dtype="float32")


def granule(
    *,
    root_attrs: dict | None = None,
    var_attrs: dict | None = None,
    latitude: np.ndarray = LATITUDE,
) -> xr.Dataset:
    return xr.Dataset(
        {
            "vertical_column": (
                ("time", "latitude", "longitude"),
                np.zeros((1, Y, X)),
                {"units": "molecules/cm^2"} if var_attrs is None else var_attrs,
            )
        },
        coords={"latitude": ("latitude", latitude)},
        attrs=(
            {"title": "TEMPO test", "platform": "TEMPO"}
            if root_attrs is None
            else root_attrs
        ),
    )


class TestStripAttributes:
    def test_removes_named_attributes_everywhere(self) -> None:
        flat = granule_template().to_flat()
        root = flat[""]
        var = flat["/vertical_column"]
        flat[""] = root.model_copy(
            update={"attributes": {**root.attributes, "history": "run 42"}}
        )
        flat["/vertical_column"] = var.model_copy(
            update={"attributes": {**var.attributes, "history": "run 42"}}
        )
        spec = GroupSpec.from_flat(flat)

        stripped = strip_attributes(spec, {"history"})

        flat_out = stripped.to_flat()
        assert flat_out[""].attributes == {
            "title": "TEMPO test",
            "platform": "TEMPO",
        }
        assert flat_out["/vertical_column"].attributes == {"units": "molecules/cm^2"}

    def test_original_spec_is_unchanged(self) -> None:
        spec = granule_template()

        strip_attributes(spec, {"title"})

        assert spec.to_flat()[""].attributes["title"] == "TEMPO test"

    def test_tempo_volatile_list_covers_profiled_attrs(self) -> None:
        assert "history" in TEMPO_L3_VOLATILE_ATTRIBUTES
        assert "time_coverage_start" in TEMPO_L3_VOLATILE_ATTRIBUTES


class TestValidateGranule:
    def test_conforming_granule_passes(self) -> None:
        validate_granule(
            granule_template(), granule(), coordinates={"latitude": LATITUDE}
        )

    def test_differing_spatial_coordinates_raise(self) -> None:
        shifted = granule(latitude=LATITUDE + np.float32(0.1))

        with pytest.raises(GranuleValidationError, match="latitude"):
            validate_granule(
                granule_template(), shifted, coordinates={"latitude": LATITUDE}
            )

    def test_missing_spatial_coordinate_raises(self) -> None:
        no_coord = granule().drop_vars("latitude")

        with pytest.raises(GranuleValidationError, match="latitude"):
            validate_granule(
                granule_template(), no_coord, coordinates={"latitude": LATITUDE}
            )

    def test_differing_expected_attribute_raises(self) -> None:
        wrong_units = granule(var_attrs={"units": "DU"})

        with pytest.raises(GranuleValidationError) as excinfo:
            validate_granule(granule_template(), wrong_units)

        message = str(excinfo.value)
        assert "/vertical_column" in message
        assert "units" in message

    def test_missing_expected_attribute_raises(self) -> None:
        missing = granule(root_attrs={"title": "TEMPO test"})

        with pytest.raises(GranuleValidationError, match="platform"):
            validate_granule(granule_template(), missing)

    def test_volatile_attribute_differences_are_ignored(self) -> None:
        noisy = granule(
            root_attrs={
                "title": "TEMPO test",
                "platform": "TEMPO",
                "history": "produced 2026-08-19",
            }
        )

        validate_granule(granule_template(), noisy, volatile={"history"})

    def test_ndarray_attribute_values_compare_by_content(self) -> None:
        flat = granule_template().to_flat()
        root = flat[""]
        flat[""] = root.model_copy(
            update={"attributes": {**root.attributes, "bounds": [-90.0, 90.0]}}
        )
        spec = GroupSpec.from_flat(flat)
        ok = granule(
            root_attrs={
                "title": "TEMPO test",
                "platform": "TEMPO",
                "bounds": np.array([-90.0, 90.0]),
            }
        )

        validate_granule(spec, ok)

        bad = granule(
            root_attrs={
                "title": "TEMPO test",
                "platform": "TEMPO",
                "bounds": np.array([-89.0, 90.0]),
            }
        )
        with pytest.raises(GranuleValidationError, match="bounds"):
            validate_granule(spec, bad)

    def test_unexpected_attribute_warns_and_passes(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        extra = granule(
            var_attrs={"units": "molecules/cm^2", "made_up_attr": "surprise"}
        )

        with caplog.at_level("WARNING", logger="virtualizarr_processor.store_template"):
            validate_granule(granule_template(), extra)

        assert any(
            "made_up_attr" in record.message and "/vertical_column" in record.message
            for record in caplog.records
        )

    def test_unexpected_attribute_records_otel_span_event(self) -> None:
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
            InMemorySpanExporter,
        )

        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        tracer = provider.get_tracer("test")
        extra = granule(
            var_attrs={"units": "molecules/cm^2", "made_up_attr": "surprise"}
        )

        with tracer.start_as_current_span("validate"):
            validate_granule(granule_template(), extra)

        (span,) = exporter.get_finished_spans()
        (event,) = span.events
        assert "unexpected" in event.name
        assert "made_up_attr" in event.attributes["attributes"]

    def test_datatree_group_attributes_are_checked(self) -> None:
        spec = GroupSpec.from_flat(
            {
                "": GroupSpec(attributes={}, members=None),
                "/product": GroupSpec(attributes={}, members=None),
                "/product/vertical_column": array_spec(
                    (TIME, Y, X),
                    (1, Y, X),
                    "float64",
                    ("time", "latitude", "longitude"),
                    attributes={"units": "molecules/cm^2"},
                ),
            }
        )
        tree = xr.DataTree.from_dict(
            {
                "/": xr.Dataset(),
                "/product": xr.Dataset(
                    {
                        "vertical_column": (
                            ("time", "latitude", "longitude"),
                            np.zeros((1, Y, X)),
                            {"units": "DU"},
                        )
                    }
                ),
            }
        )

        with pytest.raises(GranuleValidationError) as excinfo:
            validate_granule(spec, tree)

        assert "/product/vertical_column" in str(excinfo.value)

    def test_conforming_datatree_passes(self) -> None:
        spec = GroupSpec.from_flat(
            {
                "": GroupSpec(attributes={"title": "t"}, members=None),
                "/product": GroupSpec(attributes={}, members=None),
                "/product/vertical_column": array_spec(
                    (TIME, Y, X),
                    (1, Y, X),
                    "float64",
                    ("time", "latitude", "longitude"),
                    attributes={"units": "molecules/cm^2"},
                ),
            }
        )
        tree = xr.DataTree.from_dict(
            {
                "/": xr.Dataset(attrs={"title": "t"}),
                "/product": xr.Dataset(
                    {
                        "vertical_column": (
                            ("time", "latitude", "longitude"),
                            np.zeros((1, Y, X)),
                            {"units": "molecules/cm^2"},
                        )
                    }
                ),
            }
        )

        validate_granule(spec, tree)


class TestValidateStoreAttributePolicy:
    def test_extra_store_attribute_warns_instead_of_raising(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        store = zarr.storage.MemoryStore()
        create_empty_store(template(), store)
        group = zarr.open_group(store)
        group.attrs["made_up_attr"] = "surprise"

        with caplog.at_level("WARNING", logger="virtualizarr_processor.store_template"):
            validate_store(template(), zarr.open_group(store, mode="r"))

        assert any("made_up_attr" in record.message for record in caplog.records)

    def test_differing_expected_store_attribute_raises(self) -> None:
        store = zarr.storage.MemoryStore()
        create_empty_store(template(), store)
        group = zarr.open_group(store)
        group.attrs["title"] = "renamed"

        with pytest.raises(StoreValidationError, match="title"):
            validate_store(template(), zarr.open_group(store, mode="r"))

    def test_missing_expected_store_attribute_raises(self) -> None:
        store = zarr.storage.MemoryStore()
        create_empty_store(template(), store)
        group = zarr.open_group(store)
        del group.attrs["title"]

        with pytest.raises(StoreValidationError, match="title"):
            validate_store(template(), zarr.open_group(store, mode="r"))
