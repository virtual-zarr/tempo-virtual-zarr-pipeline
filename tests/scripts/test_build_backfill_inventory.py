"""Tests for the pure inventory-building logic in build_backfill_inventory.

The CMR query, the per-granule header reads, and the S3 upload edges need
live network and stay untested, like the exploration scripts;
everything between them is covered here via an injectable ``read_time``.
"""

import pathlib
import sys
import types

import pydantic
import pytest
from build_backfill_inventory import (
    InventoryError,
    build_inventory,
    dedupe_republications,
    read_time_via_earthaccess,
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


def test_read_access_reads_via_other_flavor_but_records_access_urls() -> None:
    class DualLinkGranule(StubGranule):
        def data_links(self, access: str = "external") -> list[str]:
            if access == "direct":
                return self._links
            return [
                url.replace("s3://", "https://data.asdc.earthdata.nasa.gov/", 1)
                for url in self._links
            ]

    read_urls: list[str] = []

    def read_https_time(url: str) -> float:
        read_urls.append(url)
        return FILE_TIMES[
            url.replace("https://data.asdc.earthdata.nasa.gov/", "s3://", 1)
        ]

    inventory = build_inventory(
        [DualLinkGranule("a")],
        access="direct",
        read_access="external",
        read_time=read_https_time,
        collection_shortname="TEMPO_HCHO_L3",
        concept_id="C3685897141-LARC_CLOUD",
    )
    assert inventory.urls() == ["s3://asdc-prod-protected/TEMPO/a.nc"]
    assert read_urls == [
        "https://data.asdc.earthdata.nasa.gov/asdc-prod-protected/TEMPO/a.nc"
    ]


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


def test_not_in_region_error_fails_fast(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """earthaccess's in-region refusal is config, not transient: no retries."""
    calls = []

    def refuse(urls: list[str]) -> list[object]:
        calls.append(urls)
        raise ValueError(
            "We cannot open S3 links when we are not in-region, try using HTTPS links"
        )

    # The script imports earthaccess lazily inside the function; stub the
    # module so the test never touches the real package or the network.
    monkeypatch.setitem(sys.modules, "earthaccess", types.SimpleNamespace(open=refuse))
    monkeypatch.setitem(sys.modules, "h5py", types.SimpleNamespace())
    monkeypatch.setattr(
        "build_backfill_inventory.time_module.sleep",
        lambda _: pytest.fail("must not sleep/retry on the in-region error"),
    )

    with pytest.raises(ValueError, match="not in-region"):
        read_time_via_earthaccess("s3://asdc-prod-protected/TEMPO/a.nc")
    assert len(calls) == 1


def test_written_inventory_round_trips_through_the_model(
    tmp_path: pathlib.Path,
) -> None:
    inventory = build([StubGranule("a"), StubGranule("b")])
    out = tmp_path / "inv" / "tempo.json"
    write_inventory(inventory, out)
    assert BackfillInventory.from_json(out.read_text()) == inventory
