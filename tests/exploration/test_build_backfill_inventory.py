"""Tests for the pure inventory-building logic in build_backfill_inventory.

The CMR query and S3 upload edges need live network and stay untested, like
the other exploration scripts; everything between them is covered here.
"""

import json
import pathlib

import pytest
from build_backfill_inventory import InventoryError, build_inventory, write_inventory


class StubGranule(dict):
    """Duck-types the earthaccess DataGranule surface the script consumes
    (DataGranule subclasses dict; see exploration/tempo_dataset_info.py for
    the umm layout)."""

    def __init__(self, start: str, links: list[str]) -> None:
        super().__init__(
            {
                "meta": {"concept-id": f"G-{start}"},
                "umm": {
                    "TemporalExtent": {"RangeDateTime": {"BeginningDateTime": start}}
                },
            }
        )
        self._links = links

    def data_links(self, access: str = "external") -> list[str]:
        return self._links


def granule(start: str, name: str) -> StubGranule:
    return StubGranule(
        start,
        [
            f"s3://asdc-prod-protected/TEMPO/{name}.nc",
            f"s3://asdc-prod-protected/TEMPO/{name}.xml",
        ],
    )


class TestBuildInventory:
    def test_orders_keys_chronologically(self) -> None:
        granules = [
            granule("2024-01-02T12:00:00Z", "b"),
            granule("2024-01-01T12:00:00Z", "a"),
            granule("2024-01-03T12:00:00Z", "c"),
        ]

        keys = build_inventory(granules, access="direct")

        assert keys == [
            "s3://asdc-prod-protected/TEMPO/a.nc",
            "s3://asdc-prod-protected/TEMPO/b.nc",
            "s3://asdc-prod-protected/TEMPO/c.nc",
        ]

    def test_selects_only_nc_links(self) -> None:
        keys = build_inventory([granule("2024-01-01T00:00:00Z", "a")], access="direct")

        assert keys == ["s3://asdc-prod-protected/TEMPO/a.nc"]

    def test_granule_without_nc_link_is_an_error(self) -> None:
        bad = StubGranule("2024-01-01T00:00:00Z", ["s3://bucket/only-metadata.xml"])

        with pytest.raises(InventoryError, match="G-2024-01-01T00:00:00Z"):
            build_inventory([bad], access="direct")

    def test_duplicate_time_step_is_an_error(self) -> None:
        # Two granules on one time step would violate the pipeline's
        # disjoint-regions requirement, so this must fail loudly.
        granules = [
            granule("2024-01-01T00:00:00Z", "a"),
            granule("2024-01-01T00:00:00Z", "a-reprocessed"),
        ]

        with pytest.raises(InventoryError, match="2024-01-01T00:00:00Z"):
            build_inventory(granules, access="direct")

    def test_empty_result_is_an_error(self) -> None:
        with pytest.raises(InventoryError, match="[Nn]o granules"):
            build_inventory([], access="direct")


class TestWriteInventory:
    def test_writes_read_inventory_compatible_json(
        self, tmp_path: pathlib.Path
    ) -> None:
        out = tmp_path / "inv" / "tempo.json"

        write_inventory(["s3://b/a.nc", "s3://b/b.nc"], out)

        # backfill_handlers.inventory.read_inventory expects a bare JSON array.
        assert json.loads(out.read_text()) == ["s3://b/a.nc", "s3://b/b.nc"]
