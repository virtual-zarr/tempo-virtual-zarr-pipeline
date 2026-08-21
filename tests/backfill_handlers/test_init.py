from types import SimpleNamespace
from unittest.mock import MagicMock

import numpy as np
import zarr
from backfill_handlers import init
from virtualizarr_processor.processor import Processor


def test_init_creates_backfill_branch_with_inventory_axis(
    tempo_pipeline: SimpleNamespace,
    lambda_context: MagicMock,
) -> None:
    result = init.handler(
        {"inventory_uri": tempo_pipeline.inventory_uri}, lambda_context
    )

    assert isinstance(result["base_snapshot"], str) and result["base_snapshot"]
    repo = Processor().open_backfill_repo()
    # The CAS expectation for the promote step: main's tip at branch time.
    assert result["branched_from"] == repo.lookup_branch("main")
    assert "backfill" in repo.list_branches()


def test_init_resets_leftover_branch_from_failed_run(
    tempo_pipeline: SimpleNamespace,
    lambda_context: MagicMock,
) -> None:
    """A failed run leaves the backfill branch behind; restarting the state
    machine must not require manual branch deletion."""
    first = init.handler(
        {"inventory_uri": tempo_pipeline.inventory_uri}, lambda_context
    )

    second = init.handler(
        {"inventory_uri": tempo_pipeline.inventory_uri}, lambda_context
    )

    assert second["branched_from"] == first["branched_from"]  # main untouched
    repo = Processor().open_backfill_repo()
    assert repo.lookup_branch("backfill") == second["base_snapshot"]
    group = zarr.open_group(repo.readonly_session("backfill").store, mode="r")
    np.testing.assert_array_equal(np.asarray(group["time"][:]), tempo_pipeline.times)
    group = zarr.open_group(repo.readonly_session("backfill").store, mode="r")
    np.testing.assert_array_equal(np.asarray(group["time"][:]), tempo_pipeline.times)
