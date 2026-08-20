"""Tests for the store-manifest rebuild script."""

import os
import pathlib
import pickle
import sys
from datetime import datetime

import numpy as np
import pytest
import rebuild_manifest as rm
import verify_store as vs
import zarr
from tempo_fixtures import TinyCollection, build_tiny_collection, write_tempo_granule
from virtualizarr_processor import backfill
from virtualizarr_processor.inventory import GranuleEntry
from virtualizarr_processor.manifest import StoreManifest
from virtualizarr_processor.processor import Processor


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


def backfill_and_promote(tiny: TinyCollection) -> Processor:
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


def lookup_from(entries: list[GranuleEntry]) -> vs.CmrLookup:
    def lookup(when: datetime) -> tuple[str, str] | None:
        entry = min(entries, key=lambda e: abs(vs.axis_datetime(e.time) - when))
        return entry.url, entry.granule_ur

    return lookup


# --- rebuild_entries unit behavior ---


def _entry(time: float, ur: str) -> GranuleEntry:
    return GranuleEntry(url=f"s3://b/{ur}.nc", granule_ur=ur, time=time)


def test_sources_resolve_by_time_and_cmr_fills_gaps() -> None:
    axis = np.array([1.0, 2.0, 3.0])
    known = [_entry(1.0, "a"), _entry(2.0, "b")]
    entries, problems = rm.rebuild_entries(
        axis, [("manifest", known)], lookup_from([_entry(3.0, "c")])
    )
    assert problems == []
    assert [e.granule_ur for e in entries] == ["a", "b", "c"]


def test_conflicting_sources_are_reported() -> None:
    axis = np.array([1.0])
    entries, problems = rm.rebuild_entries(
        axis,
        [("manifest", [_entry(1.0, "a")]), ("ledger", [_entry(1.0, "imposter")])],
    )
    assert any("imposter" in line for line in problems)


def test_unresolved_slot_is_reported_offline() -> None:
    axis = np.array([1.0, 2.0])
    entries, problems = rm.rebuild_entries(axis, [("manifest", [_entry(1.0, "a")])])
    assert any("slot 1" in line for line in problems)


# --- main() against a real store ---


def run_main(monkeypatch: pytest.MonkeyPatch, *argv: str) -> int:
    monkeypatch.setattr(sys, "argv", ["rebuild_manifest.py", *argv])
    return rm.main()


def test_rebuilds_manifest_stale_after_commit(
    tiny: TinyCollection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The consumer crash window: an append committed but the manifest write
    was lost. Rebuild resolves the extra slot from CMR."""
    processor = backfill_and_promote(tiny)
    new_time = tiny.times[-1] + 3600.0
    new = write_tempo_granule(
        tiny.granule_paths[0].parent / "granule_new.nc", time_value=new_time
    )
    repo = processor.open_backfill_repo()
    session = processor.initialize_session(repo)
    processor.process_file(f"file://{new}", session)
    processor.commit_processed_files(session)
    # Simulate the crash: the stored manifest reverts to the pre-append one.
    StoreManifest.write(os.environ["STORE_MANIFEST_URI"], tiny.inventory)

    full = list(tiny.inventory.granules) + [
        GranuleEntry(url=f"file://{new}", granule_ur="granule_new", time=new_time)
    ]
    monkeypatch.setattr(vs, "cmr_lookup_for", lambda concept_id: lookup_from(full))

    assert run_main(monkeypatch, "--write") == 0

    rebuilt = StoreManifest.read(os.environ["STORE_MANIFEST_URI"])
    axis = np.asarray(
        zarr.open_array(repo.readonly_session("main").store, path="time")[:]
    )
    StoreManifest.validate_against_axis(rebuilt, axis)
    assert rebuilt.granules[-1].granule_ur == "granule_new"
    assert rebuilt.built_at == tiny.inventory.built_at  # watermark seed kept


def test_rebuilds_missing_manifest_from_inventory_offline(
    tiny: TinyCollection, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The promote crash window: main moved but no manifest was written."""
    backfill_and_promote(tiny)
    manifest_path = pathlib.Path(os.environ["STORE_MANIFEST_URI"])
    manifest_path.unlink()
    inventory_path = tmp_path / "inventory.json"
    inventory_path.write_text(tiny.inventory.to_json())

    code = run_main(
        monkeypatch, "--offline", "--inventory", str(inventory_path), "--write"
    )

    assert code == 0
    rebuilt = StoreManifest.read(os.environ["STORE_MANIFEST_URI"])
    assert rebuilt.granules == tiny.inventory.granules


def test_dry_run_leaves_manifest_alone_and_fails_on_gaps(
    tiny: TinyCollection, monkeypatch: pytest.MonkeyPatch
) -> None:
    backfill_and_promote(tiny)
    manifest_path = pathlib.Path(os.environ["STORE_MANIFEST_URI"])
    stale = tiny.inventory.model_copy(update={"granules": tiny.inventory.granules[:-1]})
    StoreManifest.write(os.environ["STORE_MANIFEST_URI"], stale)
    before = manifest_path.read_bytes()

    # Offline with a gap: non-zero exit, nothing written.
    assert run_main(monkeypatch, "--offline") == 1
    assert manifest_path.read_bytes() == before

    # Resolvable, but still a dry run: nothing written without --write.
    monkeypatch.setattr(
        vs,
        "cmr_lookup_for",
        lambda concept_id: lookup_from(list(tiny.inventory.granules)),
    )
    assert run_main(monkeypatch) == 0
    assert manifest_path.read_bytes() == before
