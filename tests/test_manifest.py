"""Tests for the store manifest and pending ledger (spec I4)."""

import pathlib
from collections.abc import Iterator

import boto3
import numpy as np
import pytest
from moto import mock_aws
from virtualizarr_processor.inventory import BackfillInventory, GranuleEntry
from virtualizarr_processor.manifest import PendingLedger, StoreManifest
from virtualizarr_processor.store_template import StoreValidationError

TIME_0 = 1471196538.0244286


@pytest.fixture()
def s3_bucket() -> Iterator[str]:
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="manifests")
        yield "manifests"


def entry(i: int) -> GranuleEntry:
    return GranuleEntry(
        url=f"s3://data/granule_{i}.nc",
        granule_ur=f"granule_{i}",
        time=TIME_0 + 3600.0 * i,
    )


def inventory(n: int = 3) -> BackfillInventory:
    return BackfillInventory(
        schema_id="tempo-backfill-inventory/1",
        collection="TEMPO_HCHO_L3",
        concept_id="C3685897141-LARC_CLOUD",
        time_units="seconds since 1980-01-06T00:00:00Z",
        built_at="2026-08-20T00:00:00Z",
        granules=tuple(entry(i) for i in range(n)),
    )


def test_store_manifest_file_round_trip(tmp_path: pathlib.Path) -> None:
    uri = str(tmp_path / "manifest.json")
    StoreManifest.write(uri, inventory())
    assert StoreManifest.read(uri) == inventory()


def test_store_manifest_s3_round_trip(s3_bucket: str) -> None:
    uri = f"s3://{s3_bucket}/store-manifest.json"
    StoreManifest.write(uri, inventory())
    assert StoreManifest.read(uri) == inventory()


def test_validate_against_axis_bit_exact() -> None:
    document = inventory()
    StoreManifest.validate_against_axis(document, document.times())

    with pytest.raises(StoreValidationError, match="axis"):
        StoreManifest.validate_against_axis(document, document.times() + 1e-6)
    with pytest.raises(StoreValidationError, match="axis"):
        StoreManifest.validate_against_axis(document, document.times()[:-1])
    with pytest.raises(StoreValidationError, match="axis"):
        StoreManifest.validate_against_axis(document, np.array([]))


def test_pending_ledger_absent_reads_empty(tmp_path: pathlib.Path) -> None:
    assert PendingLedger.read(str(tmp_path / "missing.json")) == ()


def test_pending_ledger_absent_s3_reads_empty(s3_bucket: str) -> None:
    assert PendingLedger.read(f"s3://{s3_bucket}/missing.json") == ()


def test_pending_ledger_append_dedupes(tmp_path: pathlib.Path) -> None:
    uri = str(tmp_path / "ledger.json")
    PendingLedger.append(uri, [entry(0), entry(1)])
    PendingLedger.append(uri, [entry(1), entry(2)])  # redelivery of 1
    assert PendingLedger.read(uri) == (entry(0), entry(1), entry(2))


def test_pending_ledger_remove(tmp_path: pathlib.Path) -> None:
    uri = str(tmp_path / "ledger.json")
    PendingLedger.append(uri, [entry(0), entry(1), entry(2)])
    PendingLedger.remove(uri, ["granule_0", "granule_2"])
    assert PendingLedger.read(uri) == (entry(1),)
