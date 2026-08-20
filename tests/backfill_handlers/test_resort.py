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
    init = processor.initialize_backfill_store(repo, tiny.inventory)
    shared = pickle.loads(backfill.create_fork(repo))
    children = []
    for url in tiny.urls:
        child = shared.fork()
        assert processor.process_backfill_file(url, child)
        children.append(pickle.dumps(child))
    backfill.merge_and_commit(repo, children, message="backfill")
    backfill.promote(repo, expected_target_tip=init.branched_from)
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


def test_resort_relocates_without_rereading_ingested_sources(
    tempo_pipeline: SimpleNamespace, lambda_context: MagicMock
) -> None:
    """Shifted slots move as chunk references; their source files are never
    reopened. Proven by hiding every ingested source during the resort."""
    tiny = tempo_pipeline.tiny
    processor = backfilled_processor(tempo_pipeline)
    directory = tiny.granule_paths[0].parent

    deep_time = tiny.times[0] + 1800.0  # shifts every slot after the first
    deep = write_tempo_granule(
        directory / "deep.nc", time_value=deep_time, weight_scale=7.0
    )
    from virtualizarr_processor.inventory import GranuleEntry

    PendingLedger.append(
        os.environ["PENDING_LEDGER_URI"],
        [GranuleEntry(url=f"file://{deep}", granule_ur="deep", time=deep_time)],
    )

    hidden = [(path, path.with_suffix(".hidden")) for path in tiny.granule_paths]
    for path, away in hidden:
        path.rename(away)
    try:
        result = resort.handler({}, lambda_context)
    finally:
        for path, away in hidden:  # rename preserves mtime, so reads still work
            away.rename(path)

    assert result["resorted"] is True
    repo = processor.open_backfill_repo()
    group = zarr.open_group(repo.readonly_session("main").store, mode="r")
    merged_times = [tiny.times[0], deep_time, *tiny.times[1:]]
    np.testing.assert_array_equal(np.asarray(group["time"][:]), merged_times)
    scales = {time: 1.0 + i for i, time in enumerate(tiny.times)}
    scales[deep_time] = 7.0
    for i, time_value in enumerate(merged_times):
        np.testing.assert_array_equal(
            np.asarray(group["weight"][i]),
            expected_weight(time_value, weight_scale=scales[time_value]),
        )


def test_resort_folds_at_most_max_fold_per_run(
    tempo_pipeline: SimpleNamespace,
    lambda_context: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A capped run folds the earliest pending granules, promotes durable
    partial progress, and leaves the rest in the ledger for the next run."""
    tiny = tempo_pipeline.tiny
    processor = backfilled_processor(tempo_pipeline)
    directory = tiny.granule_paths[0].parent
    monkeypatch.setenv("RESORT_MAX_FOLD", "1")

    early_time = tiny.times[0] + 1800.0
    late_time = tiny.times[1] + 1800.0
    early = write_tempo_granule(
        directory / "early.nc", time_value=early_time, weight_scale=7.0
    )
    late = write_tempo_granule(
        directory / "late.nc", time_value=late_time, weight_scale=8.0
    )
    from virtualizarr_processor.inventory import GranuleEntry

    PendingLedger.append(
        os.environ["PENDING_LEDGER_URI"],
        [
            GranuleEntry(url=f"file://{late}", granule_ur="late", time=late_time),
            GranuleEntry(url=f"file://{early}", granule_ur="early", time=early_time),
        ],
    )

    first = resort.handler({}, lambda_context)
    assert first == {
        "resorted": True,
        "inserted": 1,
        "remaining": 1,
        "first_shifted_index": 1,
    }
    ledger = PendingLedger.read(os.environ["PENDING_LEDGER_URI"])
    assert [entry.granule_ur for entry in ledger] == ["late"]  # earliest folded

    second = resort.handler({}, lambda_context)
    assert second["resorted"] is True and second["remaining"] == 0
    assert PendingLedger.read(os.environ["PENDING_LEDGER_URI"]) == ()

    repo = processor.open_backfill_repo()
    group = zarr.open_group(repo.readonly_session("main").store, mode="r")
    merged_times = sorted([*tiny.times, early_time, late_time])
    np.testing.assert_array_equal(np.asarray(group["time"][:]), merged_times)
    manifest = StoreManifest.read(os.environ["STORE_MANIFEST_URI"])
    StoreManifest.validate_against_axis(manifest, np.asarray(group["time"][:]))


def test_resort_promote_refuses_when_main_moved_mid_run(
    tempo_pipeline: SimpleNamespace,
    lambda_context: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A commit landing on main while the re-sort runs fails the promote
    instead of being discarded."""
    tiny = tempo_pipeline.tiny
    processor = backfilled_processor(tempo_pipeline)
    directory = tiny.granule_paths[0].parent
    deep_time = tiny.times[0] + 1800.0
    deep = write_tempo_granule(directory / "deep.nc", time_value=deep_time)
    from virtualizarr_processor.inventory import GranuleEntry

    PendingLedger.append(
        os.environ["PENDING_LEDGER_URI"],
        [GranuleEntry(url=f"file://{deep}", granule_ur="deep", time=deep_time)],
    )

    manifest_before = StoreManifest.read(os.environ["STORE_MANIFEST_URI"])
    concurrent_tip = None
    original = Processor.validate_backfill_store

    def commit_then_validate(self: Processor, *args: object, **kw: object) -> None:
        nonlocal concurrent_tip
        if concurrent_tip is None:  # only once, mid-resort
            session = self.open_backfill_repo().writable_session("main")
            concurrent_tip = session.commit("concurrent append", allow_empty=True)
        return original(self, *args, **kw)  # type: ignore[arg-type]

    monkeypatch.setattr(Processor, "validate_backfill_store", commit_then_validate)

    import icechunk

    with pytest.raises(icechunk.IcechunkError):
        resort.handler({}, lambda_context)

    repo = processor.open_backfill_repo()
    assert repo.lookup_branch("main") == concurrent_tip
    # Nothing was consumed: the ledger and manifest are untouched, so the
    # next scheduled run retries against the new tip.
    ledger = PendingLedger.read(os.environ["PENDING_LEDGER_URI"])
    assert [entry.granule_ur for entry in ledger] == ["deep"]
    assert StoreManifest.read(os.environ["STORE_MANIFEST_URI"]) == manifest_before


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
