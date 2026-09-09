"""Test for the manifest rewrite migration script."""

import pathlib
import pickle
import sys

import numpy as np
import pytest
import rewrite_manifests
import zarr
from tempo_fixtures import (
    TinyCollection,
    build_tiny_collection,
    expected_vertical_column,
)
from virtualizarr_processor import backfill
from virtualizarr_processor.processor import Processor


@pytest.fixture()
def tiny(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> TinyCollection:
    collection = build_tiny_collection(tmp_path / "collection")
    monkeypatch.delenv("ICECHUNK_BUCKET", raising=False)
    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "repo"))
    monkeypatch.setenv("VIRTUAL_CHUNK_PREFIX", f"file://{tmp_path}/")
    monkeypatch.setenv("TEMPO_COLLECTION", str(collection.config_path))
    return collection


def test_rewrite_advances_tip_and_preserves_reads(
    tiny: TinyCollection, monkeypatch: pytest.MonkeyPatch
) -> None:
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
    before = repo.lookup_branch("main")

    monkeypatch.setattr(sys, "argv", ["rewrite_manifests.py"])
    rewrite_manifests.main()

    reader = Processor().open_backfill_repo(authorize_virtual_reads=True)
    assert reader.lookup_branch("main") != before
    group = zarr.open_group(reader.readonly_session("main").store, mode="r")
    np.testing.assert_array_equal(np.asarray(group["time"][:]), tiny.times)
    np.testing.assert_array_equal(
        np.asarray(group["vertical_column"][0]),
        expected_vertical_column(tiny.times[0])[0],
    )
