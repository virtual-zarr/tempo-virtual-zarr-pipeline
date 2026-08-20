import pathlib

import icechunk
import numpy as np
import pytest
import zarr
from stub_processor import Processor
from virtualizarr_processor import backfill


def test_backfill_repo_has_main_branch(backfill_repo: icechunk.Repository) -> None:
    assert "main" in backfill_repo.list_branches()


def test_initialize_backfill_store_creates_full_shape(
    backfill_repo: icechunk.Repository,
) -> None:
    processor = Processor()
    main_tip = backfill_repo.lookup_branch("main")
    init = processor.initialize_backfill_store(backfill_repo)

    assert isinstance(init.snapshot, str) and init.snapshot
    assert init.branched_from == main_tip
    assert "backfill" in backfill_repo.list_branches()
    session = backfill_repo.readonly_session("backfill")
    arr = zarr.open_group(session.store, mode="r")["foo"]
    assert arr.shape == (6, 2, 3)
    # chunk geometry + dtype are load-bearing for process_backfill_file's
    # chunk-key and byte-offset arithmetic; assert them here to catch a
    # geometry regression before the Task 3 round-trip.
    assert arr.chunks == (1, 2, 3)
    assert arr.dtype == np.dtype("int32")
    time_coord = zarr.open_group(session.store, mode="r")["time"]
    assert time_coord.shape == (6,)
    assert (np.asarray(time_coord[:]) == np.arange(6)).all()


def _worker(shared_fork_bytes: bytes, keys: list[str]) -> bytes:
    import pickle

    processor = Processor()
    child = pickle.loads(shared_fork_bytes).fork()
    for key in keys:
        assert processor.process_backfill_file(key, child)
    return pickle.dumps(child)


def test_full_backfill_round_trip(backfill_repo: icechunk.Repository) -> None:
    processor = Processor()
    init = processor.initialize_backfill_store(backfill_repo)

    shared = backfill.create_fork(backfill_repo)
    child_a = _worker(shared, ["0", "1", "2"])
    child_b = _worker(shared, ["3", "4", "5"])

    tip = backfill.merge_and_commit(
        backfill_repo, [child_a, child_b], message="backfill commit"
    )
    assert isinstance(tip, str) and tip

    arr = zarr.open_group(backfill_repo.readonly_session("backfill").store, mode="r")[
        "foo"
    ]
    expected = np.arange(6)[:, None, None]
    assert (np.asarray(arr[:]) == expected).all()

    backfill.promote(backfill_repo, expected_target_tip=init.branched_from)
    arr_main = zarr.open_group(backfill_repo.readonly_session("main").store, mode="r")[
        "foo"
    ]
    assert (np.asarray(arr_main[:]) == expected).all()


def test_promote_refuses_when_main_moved(backfill_repo: icechunk.Repository) -> None:
    """A commit landing on main mid-run fails the promote instead of being
    silently discarded (the compare-and-swap that guards H1)."""
    processor = Processor()
    init = processor.initialize_backfill_store(backfill_repo)
    child = _worker(backfill.create_fork(backfill_repo), ["0"])
    backfill.merge_and_commit(backfill_repo, [child], message="partial")

    # Meanwhile something else (e.g. the forward consumer) commits to main.
    session = backfill_repo.writable_session("main")
    zarr.create_group(store=session.store, path="concurrent", zarr_format=3)
    concurrent_tip = session.commit("concurrent append")

    with pytest.raises(icechunk.IcechunkError):
        backfill.promote(backfill_repo, expected_target_tip=init.branched_from)
    assert backfill_repo.lookup_branch("main") == concurrent_tip


def test_open_backfill_repo_local_filesystem(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("ICECHUNK_BUCKET", raising=False)
    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "repo"))
    processor = Processor()

    repo = processor.open_backfill_repo()

    assert isinstance(repo, icechunk.Repository)
    assert "main" in repo.list_branches()
    # main must have a resolvable tip so initialize_backfill_store can branch off it
    assert repo.lookup_branch("main")


def test_backfill_store_matches_declared_template(
    backfill_repo: icechunk.Repository,
) -> None:
    from stub_processor import BACKFILL_TEMPLATE
    from virtualizarr_processor.store_template import validate_store

    processor = Processor()
    processor.initialize_backfill_store(backfill_repo)

    group = zarr.open_group(backfill_repo.readonly_session("backfill").store, mode="r")
    # main's pre-existing nodes ride along on the backfill branch, so the
    # template constrains its own nodes without forbidding extras.
    validate_store(BACKFILL_TEMPLATE, group, allow_extra=True)


def test_process_backfill_file_warns_on_unexpected_granule_attrs(
    backfill_repo: icechunk.Repository,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    import pickle

    import xarray as xr

    processor = Processor()
    processor.initialize_backfill_store(backfill_repo)
    original = Processor._backfill_slice_vds

    def noisy(self: Processor, t: int) -> xr.Dataset:
        vds = original(self, t)
        vds["foo"].attrs["made_up_attr"] = "surprise"
        return vds

    monkeypatch.setattr(Processor, "_backfill_slice_vds", noisy)
    child = pickle.loads(backfill.create_fork(backfill_repo)).fork()

    with caplog.at_level("WARNING", logger="virtualizarr_processor.store_template"):
        processor.process_backfill_file("0", child)

    assert any("made_up_attr" in record.message for record in caplog.records)
