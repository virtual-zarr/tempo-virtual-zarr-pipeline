import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import numpy as np
import zarr
from backfill_handlers import fork, init, inventory, partition, promote, reduce, worker
from virtualizarr_processor.processor import Processor

sys.path.insert(0, str(Path(__file__).parent.parent))
from tempo_fixtures import expected_vertical_column, expected_weight  # noqa: E402

BUCKET = "test-backfill-bucket"


def test_full_backfill_chain(
    tempo_pipeline: SimpleNamespace,
    lambda_context: MagicMock,
) -> None:
    run_prefix = f"s3://{BUCKET}/run/"

    # partition -> init
    parts = partition.handler(
        {
            "inventory_uri": tempo_pipeline.inventory_uri,
            "run_prefix": run_prefix,
            "partition_size": 3,
        },
        lambda_context,
    )["partitions"]
    init_result = init.handler(
        {"inventory_uri": tempo_pipeline.inventory_uri}, lambda_context
    )

    # serial over partitions: fork -> workers (one per file) -> reduce
    for part in parts:
        fork_result = fork.handler(
            {
                "partition_id": part["partition_id"],
                "manifest_uri": part["manifest_uri"],
                "run_prefix": run_prefix,
            },
            lambda_context,
        )
        for file_key in inventory.read_manifest(part["manifest_uri"]):
            worker.handler(
                {
                    "fork_in_uri": fork_result["fork_in_uri"],
                    "forks_out_prefix": fork_result["forks_out_prefix"],
                    "file_keys": [file_key],
                },
                lambda_context,
            )
        reduce.handler(
            {
                "partition_id": part["partition_id"],
                "forks_out_prefix": fork_result["forks_out_prefix"],
            },
            lambda_context,
        )

    # promote (gate + CAS branch move) and verify every scan on main
    promote.handler(
        {
            "inventory_uri": tempo_pipeline.inventory_uri,
            "branched_from": init_result["branched_from"],
        },
        lambda_context,
    )
    repo = Processor().open_backfill_repo()
    group = zarr.open_group(repo.readonly_session("main").store, mode="r")
    np.testing.assert_array_equal(np.asarray(group["time"][:]), tempo_pipeline.times)
    for i, time_value in enumerate(tempo_pipeline.times):
        np.testing.assert_array_equal(
            np.asarray(group["vertical_column"][i]),
            expected_vertical_column(time_value)[0],
        )
        np.testing.assert_array_equal(
            np.asarray(group["weight"][i]),
            expected_weight(time_value, weight_scale=1.0 + i),
        )
    # Store attributes are the shared template ones only.
    assert group.attrs["project"] == "TEMPO"
    assert "history" not in group.attrs
