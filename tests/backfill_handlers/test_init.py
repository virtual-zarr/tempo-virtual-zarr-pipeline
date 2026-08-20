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
    assert "backfill" in repo.list_branches()
    group = zarr.open_group(repo.readonly_session("backfill").store, mode="r")
    np.testing.assert_array_equal(np.asarray(group["time"][:]), tempo_pipeline.times)
