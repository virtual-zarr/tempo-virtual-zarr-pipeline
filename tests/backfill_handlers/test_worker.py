from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from backfill_handlers import fork, fork_store, init, worker

BUCKET = "test-backfill-bucket"


def _forked(
    tempo_pipeline: SimpleNamespace, lambda_context: MagicMock
) -> dict[str, str]:
    init.handler({"inventory_uri": tempo_pipeline.inventory_uri}, lambda_context)
    return fork.handler(
        {
            "partition_id": "0",
            "manifest_uri": f"s3://{BUCKET}/run/partitions/0.json",
            "run_prefix": f"s3://{BUCKET}/run/",
        },
        lambda_context,
    )


def test_worker_writes_child_fork(
    tempo_pipeline: SimpleNamespace,
    lambda_context: MagicMock,
) -> None:
    fork_result = _forked(tempo_pipeline, lambda_context)
    event = {
        "fork_in_uri": fork_result["fork_in_uri"],
        "forks_out_prefix": fork_result["forks_out_prefix"],
        "file_keys": tempo_pipeline.urls[:3],
    }
    result = worker.handler(event, lambda_context)

    assert result["child_fork_uri"].startswith(fork_result["forks_out_prefix"])
    assert len(fork_store.load_fork(result["child_fork_uri"])) > 0
    assert len(fork_store.list_forks(fork_result["forks_out_prefix"])) == 1


def test_worker_raises_on_invalid_granule(
    tempo_pipeline: SimpleNamespace,
    lambda_context: MagicMock,
) -> None:
    """A granule that fails validation fails the whole batch (and with it the
    Step Functions run) instead of being skipped silently."""
    fork_result = _forked(tempo_pipeline, lambda_context)
    event = {
        "fork_in_uri": fork_result["fork_in_uri"],
        "forks_out_prefix": fork_result["forks_out_prefix"],
        "file_keys": ["file:///nowhere/missing_granule.nc"],
    }
    with pytest.raises(RuntimeError, match="missing_granule"):
        worker.handler(event, lambda_context)
