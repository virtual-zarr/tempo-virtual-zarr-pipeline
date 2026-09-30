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
from virtualizarr_processor.processor import PartialWriteError, Processor
from virtualizarr_processor.store_template import StoreValidationError
from virtualizarr_processor.typing import ProcessOutcome


@pytest.fixture()
def tiny(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> TinyCollection:
    collection = build_tiny_collection(tmp_path / "collection")
    monkeypatch.delenv("ICECHUNK_BUCKET", raising=False)
    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "repo"))
    monkeypatch.setenv("VIRTUAL_CHUNK_PREFIX", f"file://{tmp_path}/")
    monkeypatch.setenv("TEMPO_COLLECTION", str(collection.config_path))
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
    # Reading data back needs the container authorized; writing did not.
    reader = processor.open_backfill_repo(authorize_virtual_reads=True)
    return zarr.open_group(reader.readonly_session("main").store, mode="r")


def test_repo_opens_with_manifest_splitting(tiny: TinyCollection) -> None:
    # Unsplit manifests span the whole archive and are rewritten in memory
    # on every commit; a single-granule append OOMed the consumer once the
    # full backfill promoted. Every writer opens through this path.
    repo = Processor().open_backfill_repo()
    assert repo.config.manifest is not None
    assert repo.config.manifest.splitting is not None


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

    from virtualizarr_processor.manifest import PendingLedger, StoreManifest

    repo = processor.open_backfill_repo()
    manifest = StoreManifest.read(repo.readonly_session("main").store)
    assert manifest is not None
    assert manifest.granules == tiny.inventory.granules
    assert PendingLedger.read(repo.readonly_session("main").store) == ()


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


def test_promote_gate_rejects_manifest_array_mismatch(tiny: TinyCollection) -> None:
    import zarr

    processor = Processor()
    repo = processor.open_backfill_repo()
    processor.initialize_backfill_store(repo, tiny.inventory)
    session = repo.writable_session("backfill")
    zarr.open_array(session.store, path="granule_ur")[0] = "someone-else"
    session.commit("corrupt a manifest slot")
    with pytest.raises(StoreValidationError, match="granule_ur"):
        processor.validate_backfill_store(repo, tiny.inventory, branch="backfill")


def test_promote_gate_rejects_missing_chunk_references(tiny: TinyCollection) -> None:
    """Unwritten slots read as fill values and pass every metadata check;
    only counting the stored chunk references catches them."""
    processor = Processor()
    repo = processor.open_backfill_repo()
    processor.initialize_backfill_store(repo, tiny.inventory)

    # Freshly initialized: axis, coordinates, and manifest are all perfect,
    # yet no data variable holds a single reference.
    with pytest.raises(StoreValidationError, match="chunk references"):
        processor.validate_backfill_store(repo, tiny.inventory, branch="backfill")

    # All but the last granule written: still short, still rejected.
    shared = pickle.loads(backfill.create_fork(repo))
    children = []
    for url in tiny.urls[:-1]:
        child = shared.fork()
        assert processor.process_backfill_file(url, child)
        children.append(pickle.dumps(child))
    backfill.merge_and_commit(repo, children, message="all but one")
    with pytest.raises(StoreValidationError, match="chunk references"):
        processor.validate_backfill_store(repo, tiny.inventory, branch="backfill")

    # The last write completes coverage and the gate passes.
    child = pickle.loads(backfill.create_fork(repo)).fork()
    assert processor.process_backfill_file(tiny.urls[-1], child)
    backfill.merge_and_commit(repo, [pickle.dumps(child)], message="last one")
    processor.validate_backfill_store(repo, tiny.inventory, branch="backfill")


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

    reader = processor.open_backfill_repo(authorize_virtual_reads=True)
    group = zarr.open_group(reader.readonly_session("main").store, mode="r")
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
    processor = Processor()
    run_backfill(processor, tiny)
    return processor


def forward(processor: Processor, urls: list[str]) -> list[ProcessOutcome]:
    repo = processor.open_backfill_repo()
    session = processor.initialize_session(repo)
    outcomes = [processor.process_file(url, session) for url in urls]
    # DEFERRED writes the pending ledger through the session too, so it
    # needs a commit just like a write; an all-REJECTED batch leaves the
    # session untouched and skips committing (nothing changed to commit).
    if any(o is not ProcessOutcome.REJECTED for o in outcomes):
        processor.commit_processed_files(session)
    return outcomes


def test_forward_appends_in_order(tiny: TinyCollection) -> None:
    processor = backfilled(tiny)
    new_time = tiny.times[-1] + 3600.0
    new = write_tempo_granule(
        tiny.granule_paths[0].parent / "granule_new.nc",
        time_value=new_time,
        weight_scale=9.0,
    )
    assert forward(processor, [f"file://{new}"]) == [ProcessOutcome.APPENDED]

    repo = processor.open_backfill_repo(authorize_virtual_reads=True)
    group = zarr.open_group(repo.readonly_session("main").store, mode="r")
    axis = np.asarray(group["time"][:])
    np.testing.assert_array_equal(axis, tiny.times + [new_time])
    np.testing.assert_array_equal(
        np.asarray(group["vertical_column"][-1]),
        expected_vertical_column(new_time)[0],
    )
    from virtualizarr_processor.manifest import StoreManifest

    manifest = StoreManifest.read(repo.readonly_session("main").store)
    assert manifest is not None and manifest.urls()[-1] == f"file://{new}"


def test_forward_redelivery_is_idempotent(tiny: TinyCollection) -> None:
    processor = backfilled(tiny)
    # The already-backfilled granule 1 is redelivered: same UR, same time.
    assert forward(processor, [tiny.urls[1]]) == [ProcessOutcome.OVERWRITTEN]

    repo = processor.open_backfill_repo(authorize_virtual_reads=True)
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
    from virtualizarr_processor.manifest import PendingLedger

    processor = backfilled(tiny)
    historical_time = tiny.times[0] + 1800.0  # between slots, not on the axis
    historical = write_tempo_granule(
        tiny.granule_paths[0].parent / "historical.nc", time_value=historical_time
    )
    assert forward(processor, [f"file://{historical}"]) == [ProcessOutcome.DEFERRED]

    repo = processor.open_backfill_repo()
    ledger = PendingLedger.read(repo.readonly_session("main").store)
    assert [entry.granule_ur for entry in ledger] == ["historical"]
    assert ledger[0].time == historical_time
    group = zarr.open_group(repo.readonly_session("main").store, mode="r")
    np.testing.assert_array_equal(np.asarray(group["time"][:]), tiny.times)


def test_forward_all_deferred_batch_commits(tiny: TinyCollection) -> None:
    """A batch of only out-of-order granules must still commit (the ledger
    write is a session change), not raise NoChangesToCommitError."""
    from virtualizarr_processor.manifest import PendingLedger

    processor = backfilled(tiny)
    between = write_tempo_granule(
        tiny.granule_paths[0].parent / "between.nc",
        time_value=tiny.times[0] + 1800.0,
    )
    assert forward(processor, [f"file://{between}"]) == [ProcessOutcome.DEFERRED]
    repo = processor.open_backfill_repo()
    assert [
        e.granule_ur for e in PendingLedger.read(repo.readonly_session("main").store)
    ] == ["between"]


def test_forward_mid_write_failure_refuses_commit(
    tiny: TinyCollection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A granule failing mid-write taints the shared batch session: the
    commit must be refused so a sibling's success cannot persist the
    failed granule's partial writes."""
    processor = backfilled(tiny)
    new = write_tempo_granule(
        tiny.granule_paths[0].parent / "granule_new.nc",
        time_value=tiny.times[-1] + 3600.0,
    )

    def explode(*args: object, **kwargs: object) -> None:
        raise RuntimeError("chunk write failed mid-granule")

    monkeypatch.setattr(Processor, "_write_region", explode)
    repo = processor.open_backfill_repo()
    session = processor.initialize_session(repo)
    outcomes = [
        # Redelivery of granule 1 routes to the in-place overwrite, which
        # now fails mid-write; the sibling append succeeds.
        processor.process_file(tiny.urls[1], session),
        processor.process_file(f"file://{new}", session),
    ]
    assert outcomes == [ProcessOutcome.REJECTED, ProcessOutcome.APPENDED]
    with pytest.raises(PartialWriteError):
        processor.commit_processed_files(session)
    # Nothing from the batch reached main.
    group = zarr.open_group(repo.readonly_session("main").store, mode="r")
    np.testing.assert_array_equal(np.asarray(group["time"][:]), tiny.times)


def test_forward_rejects_republication_with_moved_timestamp(
    tiny: TinyCollection,
) -> None:
    """Same UR as an ingested granule, shifted time: reject loudly instead of
    poisoning the pending ledger with a UR the manifest already owns."""
    from virtualizarr_processor.manifest import PendingLedger

    processor = backfilled(tiny)
    moved = write_tempo_granule(
        tiny.granule_paths[0].parent / f"{tiny.granule_paths[1].stem}.nc",
        time_value=tiny.times[1] + 7.0,  # off-axis, before the end
    )
    assert forward(processor, [f"file://{moved}"]) == [ProcessOutcome.REJECTED]
    repo = processor.open_backfill_repo()
    assert PendingLedger.read(repo.readonly_session("main").store) == ()


def test_forward_rejects_republication_with_moved_timestamp_past_axis_end(
    tiny: TinyCollection,
) -> None:
    """Same UR as an ingested granule, time shifted PAST the axis end: this
    used to fall into the append branch (owned-UR check only guarded the
    out-of-order branch) and got appended, poisoning the manifest with a
    duplicate UR and failing every subsequent batch (review finding I1)."""
    from virtualizarr_processor.manifest import PendingLedger, StoreManifest

    processor = backfilled(tiny)
    moved = write_tempo_granule(
        tiny.granule_paths[1].parent / f"{tiny.granule_paths[1].stem}.nc",
        time_value=tiny.times[-1] + 3600.0,  # past the axis end
    )
    assert forward(processor, [f"file://{moved}"]) == [ProcessOutcome.REJECTED]
    repo = processor.open_backfill_repo()
    main_store = repo.readonly_session("main").store
    assert PendingLedger.read(main_store) == ()
    manifest = StoreManifest.read(main_store)
    assert manifest is not None
    assert [e.granule_ur for e in manifest.granules] == [
        f"granule_{i}" for i in range(len(tiny.times))
    ]


def test_forward_rejects_moved_timestamp_within_same_batch(
    tiny: TinyCollection,
) -> None:
    """The moved-timestamp gate must see the current batch's own writes too:
    a same-batch record sharing an earlier record's UR at a shifted,
    off-axis time must be rejected. Before the fix, both checks only read
    the granule_ur array (synced at commit time), so an in-batch UR was
    invisible and the second record slipped through as DEFERRED."""
    from virtualizarr_processor.manifest import PendingLedger, StoreManifest

    processor = backfilled(tiny)
    new_time = tiny.times[-1] + 3600.0
    first_dir = tiny.granule_paths[0].parent
    second_dir = first_dir / "resend"
    second_dir.mkdir()
    # Same basename in both directories => same derived granule UR.
    first = write_tempo_granule(first_dir / "batch_new.nc", time_value=new_time)
    second = write_tempo_granule(
        second_dir / "batch_new.nc",
        time_value=new_time - 7.0,  # off-axis, before it
    )
    assert forward(processor, [f"file://{first}", f"file://{second}"]) == [
        ProcessOutcome.APPENDED,
        ProcessOutcome.REJECTED,
    ]

    repo = processor.open_backfill_repo()
    assert PendingLedger.read(repo.readonly_session("main").store) == ()
    manifest = StoreManifest.read(repo.readonly_session("main").store)
    assert manifest is not None
    matches = [e for e in manifest.granules if e.granule_ur == "batch_new"]
    assert len(matches) == 1
    assert matches[0].time == new_time


def test_forward_republication_overwrites_in_place(tiny: TinyCollection) -> None:
    processor = backfilled(tiny)
    # The producer replaces granule 1's file in place: same name, same time,
    # different data (weight_scale changes the weight payload).
    write_tempo_granule(
        tiny.granule_paths[1], time_value=tiny.times[1], weight_scale=42.0
    )
    assert forward(processor, [tiny.urls[1]]) == [ProcessOutcome.OVERWRITTEN]

    repo = processor.open_backfill_repo(authorize_virtual_reads=True)
    group = zarr.open_group(repo.readonly_session("main").store, mode="r")
    np.testing.assert_array_equal(
        np.asarray(group["weight"][1]),
        expected_weight(tiny.times[1], weight_scale=42.0),
    )


def test_consumer_refuses_uninitialized_store(tiny: TinyCollection) -> None:
    """Bootstrapping the store is the backfill's (or the initialize
    Lambda's) job, never a side effect of consuming a message."""
    processor = Processor()
    with pytest.raises(StoreValidationError, match="not initialized"):
        processor.open_initialized_repo()

    run_backfill(processor, tiny)
    assert processor.open_initialized_repo() is not None


def test_open_backfill_repo_authorizes_container_only_for_readers(
    tiny: TinyCollection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Writers keep the container locked (they only write references);
    readers opt in and are authorized for exactly the configured prefix,
    else icechunk blocks every virtual chunk fetch with
    UnauthorizedVirtualChunkContainer."""
    import os

    import icechunk

    captured: dict = {}
    real = icechunk.Repository.open_or_create

    def spy(**kwargs):  # type: ignore[no-untyped-def]
        captured.update(kwargs)
        return real(**kwargs)

    monkeypatch.setattr(icechunk.Repository, "open_or_create", spy)
    processor = Processor()

    # file:// (the tiny collection's prefix)
    file_prefix = os.environ["VIRTUAL_CHUNK_PREFIX"]
    processor.open_backfill_repo()
    assert captured["authorize_virtual_chunk_access"] is None
    processor.open_backfill_repo(authorize_virtual_reads=True)
    file_auth = captured["authorize_virtual_chunk_access"]
    assert set(file_auth) == {file_prefix}
    assert isinstance(
        file_auth[file_prefix], icechunk.Credentials.LocalFileSystemAccess
    )

    # s3:// with EDL material configured
    monkeypatch.setenv("VIRTUAL_CHUNK_PREFIX", "s3://asdc-prod-protected/")
    monkeypatch.setenv("EARTHDATA_TOKEN", "tok")
    processor.open_backfill_repo()
    assert captured["authorize_virtual_chunk_access"] is None
    processor.open_backfill_repo(authorize_virtual_reads=True)
    s3_auth = captured["authorize_virtual_chunk_access"]
    assert set(s3_auth) == {"s3://asdc-prod-protected/"}
    assert isinstance(s3_auth["s3://asdc-prod-protected/"], icechunk.Credentials.S3)


# --- Unchanged-redelivery fast path (overwrite-on-change) ---


def stamps_on_main(processor: Processor) -> list[str]:
    from virtualizarr_processor.manifest import GranuleStamps

    repo = processor.open_backfill_repo()
    stamps = GranuleStamps.read(repo.readonly_session("main").store)
    assert stamps is not None
    return stamps


def test_forward_unchanged_redelivery_is_skipped(tiny: TinyCollection) -> None:
    """First redelivery self-heals the backfill's unknown stamp (slow path,
    OVERWRITTEN); the second finds an equal stamp and is consumed without a
    parse, a write, or a commit — the snapshot id must not move."""
    processor = backfilled(tiny)
    assert stamps_on_main(processor) == [""] * len(tiny.times)

    assert forward(processor, [tiny.urls[1]]) == [ProcessOutcome.OVERWRITTEN]
    stamps = stamps_on_main(processor)
    assert stamps[1] and [s for i, s in enumerate(stamps) if i != 1] == [""] * (
        len(tiny.times) - 1
    )

    repo = processor.open_backfill_repo()
    tip_before = repo.lookup_branch("main")
    assert forward(processor, [tiny.urls[1]]) == [ProcessOutcome.UNCHANGED]
    assert repo.lookup_branch("main") == tip_before  # no no-op snapshot
    # Freshness stays observable on commitless invocations.
    assert processor.axis_end == tiny.times[-1]


def test_forward_changed_source_takes_slow_path_and_restamps(
    tiny: TinyCollection,
) -> None:
    """A redelivery whose source object moved (different mtime => different
    stamp) falls through to today's overwrite branch and records the new
    stamp."""
    import os

    processor = backfilled(tiny)
    assert forward(processor, [tiny.urls[1]]) == [ProcessOutcome.OVERWRITTEN]
    first_stamp = stamps_on_main(processor)[1]

    path = tiny.granule_paths[1]
    stat = path.stat()
    os.utime(path, (stat.st_atime, stat.st_mtime + 10))
    assert forward(processor, [tiny.urls[1]]) == [ProcessOutcome.OVERWRITTEN]
    second_stamp = stamps_on_main(processor)[1]
    assert second_stamp and second_stamp != first_stamp


def test_forward_missing_stamp_array_disables_fast_path(
    tiny: TinyCollection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A store predating the granule_stamp array behaves byte-for-byte as
    before the fast path existed: every redelivery overwrites."""
    from virtualizarr_processor.manifest import GranuleStamps

    processor = backfilled(tiny)
    assert forward(processor, [tiny.urls[1]]) == [ProcessOutcome.OVERWRITTEN]

    monkeypatch.setattr(GranuleStamps, "read", staticmethod(lambda store: None))
    assert forward(processor, [tiny.urls[1]]) == [ProcessOutcome.OVERWRITTEN]


def test_forward_ledger_redelivery_unchanged_skips_ledger_rewrite(
    tiny: TinyCollection,
) -> None:
    """A deferred granule's redelivery with an equal stamp is UNCHANGED (no
    parse, no ledger rewrite, no commit); with a moved stamp it re-defers
    and the entry's stamp is replaced."""
    import os

    from virtualizarr_processor.manifest import PendingLedger

    processor = backfilled(tiny)
    historical = write_tempo_granule(
        tiny.granule_paths[0].parent / "historical.nc",
        time_value=tiny.times[0] + 1800.0,
    )
    assert forward(processor, [f"file://{historical}"]) == [ProcessOutcome.DEFERRED]
    repo = processor.open_backfill_repo()
    (entry,) = PendingLedger.read(repo.readonly_session("main").store)
    assert entry.stamp is not None

    tip_before = repo.lookup_branch("main")
    assert forward(processor, [f"file://{historical}"]) == [ProcessOutcome.UNCHANGED]
    assert repo.lookup_branch("main") == tip_before

    stat = historical.stat()
    os.utime(historical, (stat.st_atime, stat.st_mtime + 10))
    assert forward(processor, [f"file://{historical}"]) == [ProcessOutcome.DEFERRED]
    (replaced,) = PendingLedger.read(repo.readonly_session("main").store)
    assert replaced.stamp is not None and replaced.stamp != entry.stamp


def test_forward_mixed_batch_commits_and_stamps_the_append(
    tiny: TinyCollection,
) -> None:
    """UNCHANGED alongside a real write must not suppress the commit, and
    the appended slot's stamp is recorded with it."""
    processor = backfilled(tiny)
    assert forward(processor, [tiny.urls[1]]) == [ProcessOutcome.OVERWRITTEN]
    new = write_tempo_granule(
        tiny.granule_paths[0].parent / "granule_new.nc",
        time_value=tiny.times[-1] + 3600.0,
    )
    assert forward(processor, [tiny.urls[1], f"file://{new}"]) == [
        ProcessOutcome.UNCHANGED,
        ProcessOutcome.APPENDED,
    ]
    stamps = stamps_on_main(processor)
    assert len(stamps) == len(tiny.times) + 1
    assert stamps[-1]  # the append recorded its stamp
    repo = processor.open_backfill_repo()
    group = zarr.open_group(repo.readonly_session("main").store, mode="r")
    assert np.asarray(group["time"][:]).size == len(tiny.times) + 1


def test_forward_same_ur_twice_in_one_batch_resolves_against_batch_stamp(
    tiny: TinyCollection,
) -> None:
    """A UR redelivered twice within one batch: the first write records the
    batch-local stamp, the second occurrence must check it (not the
    committed array) and skip."""
    processor = backfilled(tiny)
    assert forward(processor, [tiny.urls[1], tiny.urls[1]]) == [
        ProcessOutcome.OVERWRITTEN,  # backfill stamp unknown: self-heal
        ProcessOutcome.UNCHANGED,  # equal batch-local stamp
    ]


def test_resort_fold_carries_stamps(tiny: TinyCollection) -> None:
    """Relocated slots keep their recorded stamp; the inserted slot takes
    its ledger entry's stamp."""
    from virtualizarr_processor.manifest import (
        GranuleStamps,
        PendingLedger,
        StoreManifest,
        stamp_value,
    )
    from virtualizarr_processor.resort import merge_pending

    processor = backfilled(tiny)
    # Record a stamp on slot 1, then defer an out-of-order granule.
    assert forward(processor, [tiny.urls[1]]) == [ProcessOutcome.OVERWRITTEN]
    historical = write_tempo_granule(
        tiny.granule_paths[0].parent / "historical.nc",
        time_value=tiny.times[0] + 1800.0,
    )
    assert forward(processor, [f"file://{historical}"]) == [ProcessOutcome.DEFERRED]

    repo = processor.open_backfill_repo()
    tip = repo.lookup_branch("main")
    pinned = repo.readonly_session(snapshot_id=tip).store
    slot1_stamp = GranuleStamps.read(pinned)
    assert slot1_stamp is not None
    manifest = StoreManifest.read(pinned)
    assert manifest is not None
    pending = PendingLedger.read(pinned)
    merged = merge_pending(manifest, pending)

    processor.initialize_resort_store(repo, merged, from_tip=tip)
    resorted = GranuleStamps.read(repo.readonly_session("resort").store)
    assert resorted is not None
    inserted_index = [e.granule_ur for e in merged.granules].index("historical")
    relocated_index = [e.granule_ur for e in merged.granules].index("granule_1")
    assert resorted[relocated_index] == slot1_stamp[1]  # kept through the fold
    assert pending[0].stamp is not None
    assert resorted[inserted_index] == stamp_value(pending[0].stamp)
