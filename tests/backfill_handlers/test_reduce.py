import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from backfill_handlers import fork, init, reduce, worker

sys.path.insert(0, str(Path(__file__).parent.parent))
from tempo_fixtures import emf_blobs, emf_value  # noqa: E402

BUCKET = "test-backfill-bucket"


def test_reduce_commits_all_worker_forks(
    tempo_pipeline: SimpleNamespace,
    lambda_context: MagicMock,
    capsys: pytest.CaptureFixture[str],
) -> None:
    init.handler({"inventory_uri": tempo_pipeline.inventory_uri}, lambda_context)
    fork_result = fork.handler(
        {
            "partition_id": "0",
            "manifest_uri": f"s3://{BUCKET}/run/partitions/0.json",
            "run_prefix": f"s3://{BUCKET}/run/",
        },
        lambda_context,
    )
    base = {
        "fork_in_uri": fork_result["fork_in_uri"],
        "forks_out_prefix": fork_result["forks_out_prefix"],
    }
    worker.handler({**base, "file_keys": tempo_pipeline.urls[:3]}, lambda_context)
    worker.handler({**base, "file_keys": tempo_pipeline.urls[3:]}, lambda_context)

    result = reduce.handler(
        {"partition_id": "0", "forks_out_prefix": fork_result["forks_out_prefix"]},
        lambda_context,
    )

    assert isinstance(result["tip"], str) and result["tip"]
    assert result["partition_id"] == "0"
    # One PartitionsDone per committed partition; SUM feeds the gauge.
    assert emf_value(emf_blobs(capsys.readouterr().out), "PartitionsDone") == 1
