"""The sidecar time cache: rebuilds only read new/republished granules."""

import pathlib

import build_backfill_inventory as bbi
import pytest


class Granule(dict):
    def __init__(self, name: str, revision: int = 1) -> None:
        super().__init__(
            {
                "meta": {"concept-id": f"G-{name}", "revision-id": revision},
                "umm": {"GranuleUR": name},
            }
        )

    def data_links(self, access: str = "external") -> list[str]:
        return [f"s3://b/TEMPO/{self['umm']['GranuleUR']}.nc"]


def build(granules, read_time, known_times):
    return bbi.build_inventory(
        granules,
        access="direct",
        read_time=read_time,
        collection_shortname="TEMPO_NO2_L3",
        concept_id="C1",
        known_times=known_times,
    )


def test_cached_times_skip_reads() -> None:
    reads: list[str] = []

    def read_time(url: str) -> float:
        reads.append(url)
        return 99.0

    cache = {
        "g0": {"time": 10.0, "revision": 1},
        "g1": {"time": 11.0, "revision": 1},
    }
    inventory = build([Granule("g0"), Granule("g1"), Granule("g2")], read_time, cache)
    assert reads == ["s3://b/TEMPO/g2.nc"]  # only the uncached granule
    times = {e.granule_ur: e.time for e in inventory.granules}
    assert times == {"g0": 10.0, "g1": 11.0, "g2": 99.0}


def test_republished_granule_is_reread() -> None:
    reads: list[str] = []

    def read_time(url: str) -> float:
        reads.append(url)
        return 42.0

    cache = {"g0": {"time": 10.0, "revision": 1}}
    inventory = build([Granule("g0", revision=2)], read_time, cache)
    assert reads == ["s3://b/TEMPO/g0.nc"]  # revision bumped -> reread
    assert inventory.granules[0].time == 42.0


def test_failures_among_uncached_reads_still_collected() -> None:
    def read_time(url: str) -> float:
        raise OSError("boom")

    cache = {"g0": {"time": 10.0, "revision": 1}}
    with pytest.raises(bbi.InventoryError, match="1 of 1"):
        build([Granule("g0"), Granule("g1")], read_time, cache)


def test_cache_roundtrip_local(tmp_path: pathlib.Path) -> None:
    uri = str(tmp_path / "inv.json")
    bbi.save_time_cache(uri, {"g0": {"time": 1.0, "revision": 3}})
    assert bbi.load_time_cache(uri) == {"g0": {"time": 1.0, "revision": 3}}


def test_missing_cache_is_empty(tmp_path: pathlib.Path) -> None:
    assert bbi.load_time_cache(str(tmp_path / "nope.json")) == {}
