"""End-to-end tests for the re-sort job."""

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
from icechunk import Repository
from virtualizarr_processor import backfill
from virtualizarr_processor.inventory import GranuleEntry
from virtualizarr_processor.manifest import PendingLedger, StoreManifest
from virtualizarr_processor.processor import Processor
from virtualizarr_processor.resort import merge_pending

sys.path.insert(0, str(Path(__file__).parent.parent))
from tempo_fixtures import (  # noqa: E402
    expected_vertical_column,
    expected_weight,
    write_tempo_granule,
)


def backfilled_processor(tempo_pipeline: SimpleNamespace) -> Processor:
    """Backfill the tiny collection onto main (manifest+ledger ride along)."""
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
    return processor


def defer(repo: Repository, entries: list[GranuleEntry]) -> None:
    """Append entries to the pending ledger via a committed main session."""
    session = repo.writable_session("main")
    PendingLedger.append(session.store, entries)
    session.commit("defer granules")


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
    repo = processor.open_backfill_repo()
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
    defer(
        repo,
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
    main_store = repo.readonly_session("main").store
    assert PendingLedger.read(main_store) == ()
    manifest = StoreManifest.read(main_store)
    assert manifest is not None
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
    repo = processor.open_backfill_repo()
    directory = tiny.granule_paths[0].parent

    deep_time = tiny.times[0] + 1800.0  # shifts every slot after the first
    deep = write_tempo_granule(
        directory / "deep.nc", time_value=deep_time, weight_scale=7.0
    )
    defer(repo, [GranuleEntry(url=f"file://{deep}", granule_ur="deep", time=deep_time)])

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
    repo = processor.open_backfill_repo()
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
    defer(
        repo,
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
    repo = processor.open_backfill_repo()
    ledger = PendingLedger.read(repo.readonly_session("main").store)
    assert [entry.granule_ur for entry in ledger] == ["late"]  # earliest folded

    second = resort.handler({}, lambda_context)
    assert second["resorted"] is True and second["remaining"] == 0
    repo = processor.open_backfill_repo()
    assert PendingLedger.read(repo.readonly_session("main").store) == ()

    group = zarr.open_group(repo.readonly_session("main").store, mode="r")
    merged_times = sorted([*tiny.times, early_time, late_time])
    np.testing.assert_array_equal(np.asarray(group["time"][:]), merged_times)
    manifest = StoreManifest.read(repo.readonly_session("main").store)
    assert manifest is not None
    np.testing.assert_array_equal(np.asarray(manifest.times()), merged_times)


def test_resort_promote_refuses_when_main_moved_mid_run(
    tempo_pipeline: SimpleNamespace,
    lambda_context: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A commit landing on main while the re-sort runs fails the promote
    instead of being discarded."""
    tiny = tempo_pipeline.tiny
    processor = backfilled_processor(tempo_pipeline)
    repo = processor.open_backfill_repo()
    directory = tiny.granule_paths[0].parent
    deep_time = tiny.times[0] + 1800.0
    deep = write_tempo_granule(directory / "deep.nc", time_value=deep_time)
    defer(repo, [GranuleEntry(url=f"file://{deep}", granule_ur="deep", time=deep_time)])

    manifest_before = StoreManifest.read(repo.readonly_session("main").store)
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
    main_store = repo.readonly_session("main").store
    ledger = PendingLedger.read(main_store)
    assert [entry.granule_ur for entry in ledger] == ["deep"]
    assert StoreManifest.read(main_store) == manifest_before


def test_resort_concurrent_append_fails_the_cas(
    tempo_pipeline: SimpleNamespace,
    lambda_context: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An append landing on main after the resort pinned its snapshot must
    fail the promote CAS — never be silently erased (review finding #2)."""
    import icechunk
    from virtualizarr_processor import backfill as backfill_module

    tiny = tempo_pipeline.tiny
    processor = backfilled_processor(tempo_pipeline)
    repo = processor.open_backfill_repo()
    directory = tiny.granule_paths[0].parent
    deep_time = tiny.times[0] + 1800.0
    deep = write_tempo_granule(directory / "deep.nc", time_value=deep_time)
    defer(repo, [GranuleEntry(url=f"file://{deep}", granule_ur="deep", time=deep_time)])

    real_promote = backfill_module.promote

    def promote_after_concurrent_append(repo: Repository, **kwargs: object) -> str:
        # Simulate the consumer committing between the pin and the CAS.
        session = repo.writable_session("main")
        zarr.open_group(session.store, mode="a").attrs["raced"] = True
        session.commit("concurrent consumer commit")
        return real_promote(repo, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(resort.backfill, "promote", promote_after_concurrent_append)
    with pytest.raises(icechunk.IcechunkError):
        resort.handler({}, lambda_context)


def test_resort_promotes_own_fold_snapshot_despite_concurrent_resort_reinit(
    tempo_pipeline: SimpleNamespace,
    lambda_context: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two interleaved resort runs, A and B, both pinned to the same main
    tip: B re-initializes the shared "resort" branch (its own init-only
    commit, same axis/manifest shape as A's merge but with data slots at or
    after the shift still holding their *old* granules' chunk refs — the
    reindex hasn't run) after A already committed its correctly-relocated
    fold, but before A validates and promotes. A must validate and promote
    its own fold snapshot, not the branch tip B just reset — otherwise main
    ends up serving B's unrelocated data under A's axis (review finding
    C1)."""
    tiny = tempo_pipeline.tiny
    processor = backfilled_processor(tempo_pipeline)
    repo = processor.open_backfill_repo()
    directory = tiny.granule_paths[0].parent
    deep_time = tiny.times[0] + 1800.0
    deep = write_tempo_granule(
        directory / "deep.nc", time_value=deep_time, weight_scale=9.0
    )
    defer(repo, [GranuleEntry(url=f"file://{deep}", granule_ur="deep", time=deep_time)])

    tip_before = repo.lookup_branch("main")
    pinned = repo.readonly_session(snapshot_id=tip_before).store
    manifest_before = StoreManifest.read(pinned)
    assert manifest_before is not None
    pending_before = PendingLedger.read(pinned)
    b_merged = merge_pending(manifest_before, pending_before)  # same merge as A's

    original = Processor.validate_backfill_store
    reinit_done = False

    def validate_after_b_reinits_resort(
        self: Processor, *args: object, **kw: object
    ) -> None:
        nonlocal reinit_done
        if not reinit_done:  # only once, between A's commit and A's validate
            reinit_done = True
            b_repo = self.open_backfill_repo()
            self.initialize_resort_store(b_repo, b_merged, from_tip=tip_before)
        return original(self, *args, **kw)  # type: ignore[arg-type]

    monkeypatch.setattr(
        Processor, "validate_backfill_store", validate_after_b_reinits_resort
    )

    result = resort.handler({}, lambda_context)
    assert result["resorted"] is True

    repo = processor.open_backfill_repo()
    main_store = repo.readonly_session("main").store
    manifest = StoreManifest.read(main_store)
    assert manifest is not None
    assert manifest.granules[0].granule_ur == "granule_0"
    assert manifest.granules[1].granule_ur == "deep"  # A's own relocated fold
    assert PendingLedger.read(main_store) == ()  # A's own drain, not B's undrained one

    group = zarr.open_group(main_store, mode="r")
    np.testing.assert_array_equal(
        np.asarray(group["weight"][1]),
        expected_weight(deep_time, weight_scale=9.0),
    )


def test_resort_collision_aborts_before_touching_branches(
    tempo_pipeline: SimpleNamespace, lambda_context: MagicMock
) -> None:
    tiny = tempo_pipeline.tiny
    processor = backfilled_processor(tempo_pipeline)
    repo = processor.open_backfill_repo()

    # A pending granule claiming an occupied time step under another name.
    defer(
        repo,
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
