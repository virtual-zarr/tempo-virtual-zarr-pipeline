"""End-to-end tests for the re-sort job."""

import os
import pickle
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import numpy as np
import pydantic
import pytest
import zarr
from backfill_handlers import resort
from virtualizarr_processor import backfill
from virtualizarr_processor.manifest import PendingLedger, StoreManifest
from virtualizarr_processor.processor import Processor

sys.path.insert(0, str(Path(__file__).parent.parent))
from tempo_fixtures import (  # noqa: E402
    expected_vertical_column,
    expected_weight,
    write_tempo_granule,
)


def backfilled_processor(tempo_pipeline: SimpleNamespace) -> Processor:
    """Backfill the tiny collection onto main and write the store manifest."""
    tiny = tempo_pipeline.tiny
    processor = Processor()
    repo = processor.open_backfill_repo()
    processor.initialize_backfill_store(repo, tiny.inventory)
    shared = pickle.loads(backfill.create_fork(repo))
    children = []
    for url in tiny.urls:
        child = shared.fork()
        assert processor.process_backfill_file(url, child)
        children.append(pickle.dumps(child))
    backfill.merge_and_commit(repo, children, message="backfill")
    backfill.promote(repo)
    StoreManifest.write(os.environ["STORE_MANIFEST_URI"], tiny.inventory)
    return processor


def test_resort_with_empty_ledger_is_a_noop(
    tempo_pipeline: SimpleNamespace, lambda_context: MagicMock
) -> None:
    backfilled_processor(tempo_pipeline)
    result = resort.handler({}, lambda_context)
    assert result == {"resorted": False, "reason": "ledger empty"}


def test_resort_folds_pending_granules_in(
    tempo_pipeline: SimpleNamespace, lambda_context: MagicMock
) -> None:
    tiny = tempo_pipeline.tiny
    processor = backfilled_processor(tempo_pipeline)
    directory = tiny.granule_paths[0].parent

    # One deep historical insertion (between slots 0 and 1, shifting the
    # rest) and one trailing granule that arrived via the ledger.
    deep_time = tiny.times[0] + 1800.0
    tail_time = tiny.times[-1] + 3600.0
    deep = write_tempo_granule(
        directory / "deep.nc", time_value=deep_time, weight_scale=7.0
    )
    tail = write_tempo_granule(
        directory / "tail.nc", time_value=tail_time, weight_scale=8.0
    )
    from virtualizarr_processor.inventory import GranuleEntry

    PendingLedger.append(
        os.environ["PENDING_LEDGER_URI"],
        [
            GranuleEntry(url=f"file://{deep}", granule_ur="deep", time=deep_time),
            GranuleEntry(url=f"file://{tail}", granule_ur="tail", time=tail_time),
        ],
    )

    result = resort.handler({}, lambda_context)
    assert result["resorted"] is True
    assert result["inserted"] == 2
    assert result["first_shifted_index"] == 1  # slot 0 kept, rest rewritten

    repo = processor.open_backfill_repo()
    group = zarr.open_group(repo.readonly_session("main").store, mode="r")
    merged_times = [tiny.times[0], deep_time, *tiny.times[1:], tail_time]
    np.testing.assert_array_equal(np.asarray(group["time"][:]), merged_times)

    # Every slot — kept, shifted, and inserted — carries its granule's data.
    # build_tiny_collection writes granule i with weight_scale = 1.0 + i.
    scales = {time: 1.0 + i for i, time in enumerate(tiny.times)}
    scales[deep_time] = 7.0
    scales[tail_time] = 8.0
    for i, time_value in enumerate(merged_times):
        np.testing.assert_array_equal(
            np.asarray(group["vertical_column"][i]),
            expected_vertical_column(time_value)[0],
        )
        np.testing.assert_array_equal(
            np.asarray(group["weight"][i]),
            expected_weight(time_value, weight_scale=scales[time_value]),
        )

    # The ledger is drained and the manifest matches the new axis.
    assert PendingLedger.read(os.environ["PENDING_LEDGER_URI"]) == ()
    manifest = StoreManifest.read(os.environ["STORE_MANIFEST_URI"])
    StoreManifest.validate_against_axis(manifest, np.asarray(group["time"][:]))
    expected_urs = (
        ["granule_0", "deep"]
        + [f"granule_{i}" for i in range(1, len(tiny.times))]
        + ["tail"]
    )
    assert [entry.granule_ur for entry in manifest.granules] == expected_urs


def test_resort_collision_aborts_before_touching_branches(
    tempo_pipeline: SimpleNamespace, lambda_context: MagicMock
) -> None:
    tiny = tempo_pipeline.tiny
    processor = backfilled_processor(tempo_pipeline)
    from virtualizarr_processor.inventory import GranuleEntry

    # A pending granule claiming an occupied time step under another name.
    PendingLedger.append(
        os.environ["PENDING_LEDGER_URI"],
        [
            GranuleEntry(
                url="s3://x/imposter.nc", granule_ur="imposter", time=tiny.times[1]
            )
        ],
    )
    repo = processor.open_backfill_repo()
    main_before = repo.lookup_branch("main")

    with pytest.raises(pydantic.ValidationError, match="strictly increasing"):
        resort.handler({}, lambda_context)

    repo = processor.open_backfill_repo()
    assert repo.lookup_branch("main") == main_before
    assert "resort" not in repo.list_branches()
