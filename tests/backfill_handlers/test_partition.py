import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from backfill_handlers import inventory, partition

sys.path.insert(0, str(Path(__file__).parent.parent))
from tempo_fixtures import emf_blobs, emf_value  # noqa: E402

BUCKET = "test-backfill-bucket"


def test_partition_splits_inventory_into_manifests(
    tempo_pipeline: SimpleNamespace,
    lambda_context: MagicMock,
    capsys: pytest.CaptureFixture[str],
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
    # The dashboard's backfill-progress widget carries this total forward
    # (FILL/REPEAT) against a running sum of PartitionsDone.
    assert emf_value(emf_blobs(capsys.readouterr().out), "PartitionsTotal") == 3
