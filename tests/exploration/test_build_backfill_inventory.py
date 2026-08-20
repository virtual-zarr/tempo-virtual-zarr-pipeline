"""Tests for the pure inventory-building logic in build_backfill_inventory.

The CMR query, the per-granule header reads, and the S3 upload edges need
live network and stay untested, like the other exploration scripts;
everything between them is covered here via an injectable ``read_time``.
"""

import pathlib

import pydantic
import pytest
from build_backfill_inventory import (
    InventoryError,
    build_inventory,
    dedupe_republications,
    write_inventory,
)
from virtualizarr_processor.inventory import BackfillInventory

TIME_0 = 1471196538.0244286
FILE_TIMES = {
    # Exact in-file times deliberately differ from any filename timestamp.
    "s3://asdc-prod-protected/TEMPO/a.nc": TIME_0,
    "s3://asdc-prod-protected/TEMPO/b.nc": TIME_0 + 3600.0,
    "s3://asdc-prod-protected/TEMPO/c.nc": TIME_0 + 7200.0,
}


def read_time(url: str) -> float:
    return FILE_TIMES[url]


class StubGranule(dict):
    """Duck-types the earthaccess DataGranule surface the script consumes."""

    def __init__(self, name: str, revision: int = 1) -> None:
        super().__init__(
            {
                "meta": {
                    "concept-id": f"G-{name}-r{revision}",
                    "revision-id": revision,
                },
                "umm": {"GranuleUR": name},
            }
        )
        self._links = [
            f"s3://asdc-prod-protected/TEMPO/{name}.nc",
            f"s3://asdc-prod-protected/TEMPO/{name}.xml",
        ]

    def data_links(self, access: str = "external") -> list[str]:
        return self._links


def build(granules: list[StubGranule]) -> BackfillInventory:
    return build_inventory(
        granules,
        access="direct",
        read_time=read_time,
        collection_shortname="TEMPO_HCHO_L3",
        concept_id="C3685897141-LARC_CLOUD",
        workers=2,
    )


def test_orders_by_exact_file_time_not_input_order() -> None:
    inventory = build([StubGranule("c"), StubGranule("a"), StubGranule("b")])
    assert inventory.urls() == [
        "s3://asdc-prod-protected/TEMPO/a.nc",
        "s3://asdc-prod-protected/TEMPO/b.nc",
        "s3://asdc-prod-protected/TEMPO/c.nc",
    ]
    assert inventory.times().tolist() == [TIME_0, TIME_0 + 3600.0, TIME_0 + 7200.0]
    # Granule UR is the filename stem — the forward consumer's convention.
    assert [g.granule_ur for g in inventory.granules] == ["a", "b", "c"]


def test_dedupes_republications_keeping_newest_revision() -> None:
    old, new = StubGranule("a", revision=1), StubGranule("a", revision=3)
    assert dedupe_republications([old, new, StubGranule("b")]) == [
        new,
        StubGranule("b"),
    ]


def test_granule_without_nc_link_is_an_error() -> None:
    bad = StubGranule("a")
    bad._links = ["s3://bucket/only-metadata.xml"]
    with pytest.raises(InventoryError, match="G-a-r1"):
        build([bad])


def test_duplicate_file_time_is_an_error() -> None:
    """Two distinct granules with one in-file time cannot form an axis."""
    twin = StubGranule("twin")
    FILE_TIMES["s3://asdc-prod-protected/TEMPO/twin.nc"] = TIME_0
    try:
        with pytest.raises(pydantic.ValidationError, match="strictly increasing"):
            build([StubGranule("a"), twin])
    finally:
        del FILE_TIMES["s3://asdc-prod-protected/TEMPO/twin.nc"]


def test_empty_result_is_an_error() -> None:
    with pytest.raises(InventoryError, match="[Nn]o granules"):
        build([])


def test_written_inventory_round_trips_through_the_model(
    tmp_path: pathlib.Path,
) -> None:
    inventory = build([StubGranule("a"), StubGranule("b")])
    out = tmp_path / "inv" / "tempo.json"
    write_inventory(inventory, out)
    assert BackfillInventory.from_json(out.read_text()) == inventory
