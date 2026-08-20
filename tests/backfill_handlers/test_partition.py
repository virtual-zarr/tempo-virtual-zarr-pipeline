from types import SimpleNamespace
from unittest.mock import MagicMock

from backfill_handlers import inventory, partition

BUCKET = "test-backfill-bucket"


def test_partition_splits_inventory_into_manifests(
    tempo_pipeline: SimpleNamespace, lambda_context: MagicMock
) -> None:
    event = {
        "inventory_uri": tempo_pipeline.inventory_uri,
        "run_prefix": f"s3://{BUCKET}/run/",
        "partition_size": 2,
    }

    result = partition.handler(event, lambda_context)

    parts = result["partitions"]
    assert [p["partition_id"] for p in parts] == ["0", "1", "2"]
    assert inventory.read_manifest(parts[0]["manifest_uri"]) == tempo_pipeline.urls[:2]
    assert inventory.read_manifest(parts[2]["manifest_uri"]) == tempo_pipeline.urls[4:]
    # manifest_key is the S3 object key of the manifest (for the
    # Distributed Map ItemReader).
    assert parts[0]["manifest_key"] == "run/partitions/0.json"
    assert parts[0]["run_prefix"] == event["run_prefix"]
