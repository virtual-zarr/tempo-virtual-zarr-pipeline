from types import SimpleNamespace

import boto3
import pytest
from backfill_handlers import inventory
from pydantic import ValidationError
from virtualizarr_processor.inventory import BackfillInventory


def test_read_inventory_returns_typed_document(
    tempo_pipeline: SimpleNamespace,
) -> None:
    result = inventory.read_inventory(tempo_pipeline.inventory_uri)
    assert isinstance(result, BackfillInventory)
    assert result.urls() == tempo_pipeline.urls
    assert result.times().tolist() == tempo_pipeline.times


def test_read_inventory_rejects_untyped_document(s3_bucket: str) -> None:
    boto3.client("s3", region_name="us-east-1").put_object(
        Bucket=s3_bucket, Key="bad.json", Body=b'["a.nc", "b.nc"]'
    )
    with pytest.raises(ValidationError):
        inventory.read_inventory(f"s3://{s3_bucket}/bad.json")


def test_write_then_read_manifest_round_trips(s3_bucket: str) -> None:
    uri = f"s3://{s3_bucket}/partitions/0.json"
    inventory.write_manifest(uri, ["k1", "k2"])
    assert inventory.read_manifest(uri) == ["k1", "k2"]
