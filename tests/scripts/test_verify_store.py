"""Tests for the post-promote QA sampler."""

import os
import pathlib
import pickle

import pytest
from tempo_fixtures import TinyCollection, build_tiny_collection, write_tempo_granule
from verify_store import verify_store
from virtualizarr_processor import backfill
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


def test_clean_store_verifies(tiny: TinyCollection) -> None:
    processor = backfill_and_promote(tiny)
    repo = processor.open_backfill_repo()
    problems = verify_store(repo, tiny.inventory, samples=3, window=3)
    assert problems == []


def test_mutated_source_object_is_detected(tiny: TinyCollection) -> None:
    """Overwriting a source file after ingest must be detected, either as
    icechunk's checksum failure or as a value mismatch."""
    processor = backfill_and_promote(tiny)
    # The producer rewrites granule 1's file with different data after the
    # refs were written (a republication the pipeline never processed).
    write_tempo_granule(
        tiny.granule_paths[1], time_value=tiny.times[1], weight_scale=99.0
    )
    repo = processor.open_backfill_repo()
    problems = verify_store(repo, tiny.inventory, samples=3, window=3)
    assert any("granule_1" in line for line in problems)
