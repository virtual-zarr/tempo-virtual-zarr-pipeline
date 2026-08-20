"""Tests for the typed backfill inventory."""

import numpy as np
import pydantic
import pytest
from virtualizarr_processor.inventory import BackfillInventory, GranuleEntry

EXACT_TIME = 1471196538.0244286  # /time[0] of the S009 reference granule


def entry(i: int, time: float | None = None, ur: str | None = None) -> dict:
    return {
        "url": f"s3://bucket/TEMPO_HCHO_L3_V04_fake_S{i:03d}.nc",
        "granule_ur": ur or f"TEMPO_HCHO_L3_V04_fake_S{i:03d}",
        "time": EXACT_TIME + i * 3600.0 if time is None else time,
    }


def doc(granules: list[dict]) -> dict:
    return {
        "schema": "tempo-backfill-inventory/1",
        "collection": "TEMPO_HCHO_L3",
        "concept_id": "C3685897141-LARC_CLOUD",
        "time_units": "seconds since 1980-01-06T00:00:00Z",
        "built_at": "2026-08-20T00:00:00Z",
        "granules": granules,
    }


def test_round_trip_preserves_exact_float64() -> None:
    inventory = BackfillInventory.model_validate(doc([entry(0), entry(1)]))
    again = BackfillInventory.from_json(inventory.to_json())
    assert again == inventory
    assert again.granules[0].time == EXACT_TIME  # bit-exact through JSON


def test_times_and_urls() -> None:
    inventory = BackfillInventory.model_validate(doc([entry(0), entry(1)]))
    times = inventory.times()
    assert times.dtype == np.float64
    assert times.tolist() == [EXACT_TIME, EXACT_TIME + 3600.0]
    assert inventory.urls() == [g.url for g in inventory.granules]


def test_rejects_empty() -> None:
    with pytest.raises(pydantic.ValidationError, match="at least one granule"):
        BackfillInventory.model_validate(doc([]))


def test_rejects_unsorted_times() -> None:
    with pytest.raises(pydantic.ValidationError, match="strictly increasing"):
        BackfillInventory.model_validate(doc([entry(1), entry(0)]))


def test_rejects_duplicate_time() -> None:
    with pytest.raises(pydantic.ValidationError, match="strictly increasing"):
        BackfillInventory.model_validate(doc([entry(0), entry(1, time=EXACT_TIME)]))


def test_rejects_duplicate_granule_ur() -> None:
    with pytest.raises(pydantic.ValidationError, match="duplicate granule_ur"):
        BackfillInventory.model_validate(
            doc([entry(0), entry(1, ur=entry(0)["granule_ur"])])
        )


def test_rejects_non_nc_url() -> None:
    bad = entry(0)
    bad["url"] = "s3://bucket/file.h5"
    with pytest.raises(pydantic.ValidationError, match=".nc"):
        BackfillInventory.model_validate(doc([bad]))


def test_rejects_wrong_schema_id() -> None:
    document = doc([entry(0)])
    document["schema"] = "something-else/9"
    with pytest.raises(pydantic.ValidationError):
        BackfillInventory.model_validate(document)


def test_entries_are_frozen() -> None:
    inventory = BackfillInventory.model_validate(doc([entry(0)]))
    with pytest.raises(pydantic.ValidationError):
        inventory.granules[0].time = 0.0  # type: ignore[misc]
    assert isinstance(inventory.granules[0], GranuleEntry)
