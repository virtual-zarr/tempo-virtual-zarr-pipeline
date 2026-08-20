"""End-to-end and failure-mode tests for the TEMPO processor."""

import pathlib
import pickle

import h5py
import numpy as np
import pytest
import zarr
from tempo_fixtures import (
    TIME_BASE,
    TINY_LAT,
    TinyCollection,
    build_tiny_collection,
    expected_vertical_column,
    expected_weight,
    write_tempo_granule,
)
from virtualizarr_processor import backfill
from virtualizarr_processor.inventory import BackfillInventory, GranuleEntry
from virtualizarr_processor.processor import Processor
from virtualizarr_processor.store_template import StoreValidationError
from virtualizarr_processor.typing import ProcessOutcome


@pytest.fixture()
def tiny(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> TinyCollection:
    collection = build_tiny_collection(tmp_path / "collection")
    monkeypatch.delenv("ICECHUNK_BUCKET", raising=False)
    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "repo"))
    monkeypatch.setenv("VIRTUAL_CHUNK_PREFIX", f"file://{tmp_path}/")
    monkeypatch.setenv("TEMPO_COLLECTION", str(collection.config_path))
    monkeypatch.setenv("STORE_MANIFEST_URI", str(tmp_path / "store-manifest.json"))
    monkeypatch.setenv("PENDING_LEDGER_URI", str(tmp_path / "pending-ledger.json"))
    return collection


def run_backfill(processor: Processor, tiny: TinyCollection) -> zarr.Group:
    """init -> one fork per granule -> merge -> gate -> promote; returns main."""
    repo = processor.open_backfill_repo()
    init = processor.initialize_backfill_store(repo, tiny.inventory)
    shared = pickle.loads(backfill.create_fork(repo))
    children = []
    for url in tiny.urls:
        child = shared.fork()
        assert processor.process_backfill_file(url, child)
        children.append(pickle.dumps(child))
    backfill.merge_and_commit(repo, children, message="partition 0")
    processor.validate_backfill_store(repo, tiny.inventory, branch="backfill")
    backfill.promote(repo, expected_target_tip=init.branched_from)
    return zarr.open_group(repo.readonly_session("main").store, mode="r")


def test_backfill_end_to_end(tiny: TinyCollection) -> None:
    processor = Processor()
    group = run_backfill(processor, tiny)

    np.testing.assert_array_equal(np.asarray(group["time"][:]), tiny.times)
    for i, time_value in enumerate(tiny.times):
        np.testing.assert_array_equal(
            np.asarray(group["vertical_column"][i]),
            expected_vertical_column(time_value)[0],
        )
        # weight varies per scan: promotion actually took effect.
        np.testing.assert_array_equal(
            np.asarray(group["weight"][i]),
            expected_weight(time_value, weight_scale=1.0 + i),
        )
    # The store carries only shared attributes, never per-granule ones.
    assert group.attrs["project"] == "TEMPO"
    for volatile in (
        "history",
        "geospatial_lat_min",
        "time_coverage_start_since_epoch",
    ):
        assert volatile not in group.attrs, volatile


def test_rejects_granule_on_wrong_grid(tiny: TinyCollection) -> None:
    processor = Processor()
    repo = processor.open_backfill_repo()
    processor.initialize_backfill_store(repo, tiny.inventory)
    bad = write_tempo_granule(
        tiny.granule_paths[0].parent / "wrong_grid.nc",
        time_value=tiny.times[1],
        lat=TINY_LAT + np.float32(0.01),
    )
    fork = pickle.loads(backfill.create_fork(repo)).fork()
    assert processor.process_backfill_file(f"file://{bad}", fork) is False


def test_rejects_granule_with_time_not_in_inventory(tiny: TinyCollection) -> None:
    processor = Processor()
    repo = processor.open_backfill_repo()
    processor.initialize_backfill_store(repo, tiny.inventory)
    stray = write_tempo_granule(
        tiny.granule_paths[0].parent / "stray.nc",
        time_value=TIME_BASE + 999.0,  # not a slot in the axis
    )
    fork = pickle.loads(backfill.create_fork(repo)).fork()
    assert processor.process_backfill_file(f"file://{stray}", fork) is False


def test_rejects_internally_inconsistent_granule(tiny: TinyCollection) -> None:
    processor = Processor()
    repo = processor.open_backfill_repo()
    processor.initialize_backfill_store(repo, tiny.inventory)
    path = tiny.granule_paths[0].parent / "inconsistent.nc"
    write_tempo_granule(path, time_value=tiny.times[0])
    with h5py.File(path, "a") as f:
        f.attrs["time_coverage_start_since_epoch"] = np.array([tiny.times[0] + 1.0])
    fork = pickle.loads(backfill.create_fork(repo)).fork()
    assert processor.process_backfill_file(f"file://{path}", fork) is False


def test_initialize_rejects_wrong_collection(tiny: TinyCollection) -> None:
    processor = Processor()
    repo = processor.open_backfill_repo()
    wrong = tiny.inventory.model_copy(update={"collection": "TEMPO_NO2_L3"})
    with pytest.raises(StoreValidationError, match="TEMPO_NO2_L3"):
        processor.initialize_backfill_store(repo, wrong)


def test_promote_gate_rejects_axis_inventory_mismatch(tiny: TinyCollection) -> None:
    processor = Processor()
    repo = processor.open_backfill_repo()
    processor.initialize_backfill_store(repo, tiny.inventory)
    extra = tiny.inventory.model_copy(
        update={
            "granules": tiny.inventory.granules
            + (
                GranuleEntry(
                    url="file:///nowhere/extra.nc",
                    granule_ur="extra",
                    time=tiny.times[-1] + 3600.0,
                ),
            )
        }
    )
    with pytest.raises(StoreValidationError):
        processor.validate_backfill_store(repo, extra, branch="backfill")


# --- Real-granule integration (skipped when the context data is absent) ---


def test_real_backfill_two_granules(
    real_data_dir: pathlib.Path,
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = sorted(real_data_dir.glob("TEMPO_HCHO_L3_*.nc"))[:2]
    entries = []
    for i, path in enumerate(paths):
        with h5py.File(path) as f:
            entries.append(
                GranuleEntry(
                    url=f"file://{path}",
                    granule_ur=f"{path.stem}",
                    time=float(f["time"][0]),
                )
            )
    inventory = BackfillInventory(
        schema_id="tempo-backfill-inventory/1",
        collection="TEMPO_HCHO_L3",
        concept_id="C3685897141-LARC_CLOUD",
        time_units="seconds since 1980-01-06T00:00:00Z",
        built_at="2026-08-20T00:00:00Z",
        granules=tuple(entries),
    )
    monkeypatch.delenv("ICECHUNK_BUCKET", raising=False)
    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "repo"))
    monkeypatch.setenv("VIRTUAL_CHUNK_PREFIX", f"file://{real_data_dir}/")
    monkeypatch.setenv("TEMPO_COLLECTION", "hcho")

    processor = Processor()
    repo = processor.open_backfill_repo()
    init = processor.initialize_backfill_store(repo, inventory)
    shared = pickle.loads(backfill.create_fork(repo))
    children = []
    for entry in entries:
        child = shared.fork()
        assert processor.process_backfill_file(entry.url, child)
        children.append(pickle.dumps(child))
    backfill.merge_and_commit(repo, children, message="real granules")
    processor.validate_backfill_store(repo, inventory, branch="backfill")
    backfill.promote(repo, expected_target_tip=init.branched_from)

    group = zarr.open_group(repo.readonly_session("main").store, mode="r")
    window = np.s_[1200:1205, 3200:3205]
    for i, path in enumerate(paths):
        with h5py.File(path) as f:
            expected = f["product/vertical_column"][0][window]
        np.testing.assert_array_equal(
            np.asarray(group["vertical_column"][i][window]), expected
        )


# --- Forward processing ---


def backfilled(tiny: TinyCollection) -> Processor:
    """A promoted store with its manifest, ready for forward processing."""
    import os

    from virtualizarr_processor.manifest import StoreManifest

    processor = Processor()
    run_backfill(processor, tiny)
    StoreManifest.write(os.environ["STORE_MANIFEST_URI"], tiny.inventory)
    return processor


def forward(processor: Processor, urls: list[str]) -> list[ProcessOutcome]:
    repo = processor.open_backfill_repo()
    session = processor.initialize_session(repo)
    outcomes = [processor.process_file(url, session) for url in urls]
    if any(o is ProcessOutcome.WRITTEN for o in outcomes):
        processor.commit_processed_files(session)
    return outcomes


def test_forward_appends_in_order(tiny: TinyCollection) -> None:
    import os

    from virtualizarr_processor.manifest import StoreManifest

    processor = backfilled(tiny)
    new_time = tiny.times[-1] + 3600.0
    new = write_tempo_granule(
        tiny.granule_paths[0].parent / "granule_new.nc",
        time_value=new_time,
        weight_scale=9.0,
    )
    assert forward(processor, [f"file://{new}"]) == [ProcessOutcome.WRITTEN]

    repo = processor.open_backfill_repo()
    group = zarr.open_group(repo.readonly_session("main").store, mode="r")
    axis = np.asarray(group["time"][:])
    np.testing.assert_array_equal(axis, tiny.times + [new_time])
    np.testing.assert_array_equal(
        np.asarray(group["vertical_column"][-1]),
        expected_vertical_column(new_time)[0],
    )
    manifest = StoreManifest.read(os.environ["STORE_MANIFEST_URI"])
    assert manifest.urls()[-1] == f"file://{new}"
    StoreManifest.validate_against_axis(manifest, axis)


def test_forward_redelivery_is_idempotent(tiny: TinyCollection) -> None:
    processor = backfilled(tiny)
    # The already-backfilled granule 1 is redelivered: same UR, same time.
    assert forward(processor, [tiny.urls[1]]) == [ProcessOutcome.WRITTEN]

    repo = processor.open_backfill_repo()
    group = zarr.open_group(repo.readonly_session("main").store, mode="r")
    assert np.asarray(group["time"][:]).size == len(tiny.times)  # no growth
    np.testing.assert_array_equal(
        np.asarray(group["vertical_column"][1]),
        expected_vertical_column(tiny.times[1])[0],
    )


def test_forward_rejects_conflicting_granule(tiny: TinyCollection) -> None:
    processor = backfilled(tiny)
    # A *different* granule (different filename => different UR) claiming
    # granule 0's time step.
    imposter = write_tempo_granule(
        tiny.granule_paths[0].parent / "imposter.nc", time_value=tiny.times[0]
    )
    assert forward(processor, [f"file://{imposter}"]) == [ProcessOutcome.REJECTED]

    repo = processor.open_backfill_repo()
    group = zarr.open_group(repo.readonly_session("main").store, mode="r")
    np.testing.assert_array_equal(np.asarray(group["time"][:]), tiny.times)


def test_forward_defers_out_of_order_granule(tiny: TinyCollection) -> None:
    import os

    from virtualizarr_processor.manifest import PendingLedger

    processor = backfilled(tiny)
    historical_time = tiny.times[0] + 1800.0  # between slots, not on the axis
    historical = write_tempo_granule(
        tiny.granule_paths[0].parent / "historical.nc", time_value=historical_time
    )
    assert forward(processor, [f"file://{historical}"]) == [ProcessOutcome.DEFERRED]

    ledger = PendingLedger.read(os.environ["PENDING_LEDGER_URI"])
    assert [entry.granule_ur for entry in ledger] == ["historical"]
    assert ledger[0].time == historical_time
    repo = processor.open_backfill_repo()
    group = zarr.open_group(repo.readonly_session("main").store, mode="r")
    np.testing.assert_array_equal(np.asarray(group["time"][:]), tiny.times)


def test_forward_republication_overwrites_in_place(tiny: TinyCollection) -> None:
    processor = backfilled(tiny)
    # The producer replaces granule 1's file in place: same name, same time,
    # different data (weight_scale changes the weight payload).
    write_tempo_granule(
        tiny.granule_paths[1], time_value=tiny.times[1], weight_scale=42.0
    )
    assert forward(processor, [tiny.urls[1]]) == [ProcessOutcome.WRITTEN]

    repo = processor.open_backfill_repo()
    group = zarr.open_group(repo.readonly_session("main").store, mode="r")
    np.testing.assert_array_equal(
        np.asarray(group["weight"][1]),
        expected_weight(tiny.times[1], weight_scale=42.0),
    )
