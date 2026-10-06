"""Tests for the relative-reference migration script."""

import os
import pathlib
import pickle
import shutil
import sys

import icechunk
import numpy as np
import pytest
import relativize_refs
import zarr
from tempo_fixtures import (
    TinyCollection,
    build_tiny_collection,
    expected_vertical_column,
)
from virtualizarr_processor import backfill
from virtualizarr_processor import processor as processor_module
from virtualizarr_processor.processor import Processor


@pytest.fixture()
def legacy(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> TinyCollection:
    """A promoted store holding absolute references, as built before naming."""
    collection = build_tiny_collection(tmp_path / "collection")
    monkeypatch.delenv("ICECHUNK_BUCKET", raising=False)
    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "repo"))
    monkeypatch.setenv("VIRTUAL_CHUNK_PREFIX", f"file://{tmp_path}/")
    monkeypatch.setenv("TEMPO_COLLECTION", str(collection.config_path))

    with pytest.MonkeyPatch.context() as legacy_writer:
        legacy_writer.setattr(
            processor_module, "relative_location", lambda url, prefix: url
        )
        processor = Processor()
        repo = processor.open_backfill_repo()
        init = processor.initialize_backfill_store(repo, collection.inventory)
        shared = pickle.loads(backfill.create_fork(repo))
        children = []
        for url in collection.urls:
            child = shared.fork()
            assert processor.process_backfill_file(url, child)
            children.append(pickle.dumps(child))
        backfill.merge_and_commit(repo, children, message="backfill")
        backfill.promote(repo, expected_target_tip=init.branched_from)
    return collection


def locations_under(moved: pathlib.Path) -> tuple[list[str], zarr.Group]:
    """Open the store with the container pointed at a copy of the granules.

    Absolute references ignore the container's prefix. So only relative
    references appear under ``moved``, and only they read back from it.
    """
    prefix = f"file://{moved}/"
    config = Processor().open_backfill_repo().config
    config.clear_virtual_chunk_containers()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            prefix, icechunk.local_filesystem_store(str(moved)), name="asdc"
        )
    )
    reader = icechunk.Repository.open(
        icechunk.local_filesystem_storage(os.environ["ICECHUNK_LOCAL_PATH"]),
        config=config,
        authorize_virtual_chunk_access=icechunk.containers_credentials(
            {prefix: icechunk.credentials.LocalFileSystemAccess}
        ),
    )
    session = reader.readonly_session("main")
    return session.all_virtual_chunk_locations(), zarr.open_group(
        session.store, mode="r"
    )


def test_rewrites_every_slot_relative_and_resumes(
    legacy: TinyCollection, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    moved = tmp_path / "moved"
    shutil.copytree(tmp_path / "collection", moved / "collection")
    locations, _ = locations_under(moved)
    assert locations
    assert not any(loc.startswith(f"file://{moved}/") for loc in locations)

    monkeypatch.setattr(
        sys, "argv", ["relativize_refs.py", "--batch", "2", "--workers", "2"]
    )
    assert relativize_refs.main() == 0

    repo = Processor().open_backfill_repo()
    tip = next(repo.ancestry(branch="main"))
    assert tip.message == "Relativize virtual refs in slots [2, 3) of 3"
    locations, group = locations_under(moved)
    assert all(loc.startswith(f"file://{moved}/") for loc in locations)
    for i, time_value in enumerate(legacy.times):
        np.testing.assert_array_equal(
            np.asarray(group["vertical_column"][i]),
            expected_vertical_column(time_value)[0],
        )
    persisted = icechunk.Repository.fetch_config(
        icechunk.local_filesystem_storage(os.environ["ICECHUNK_LOCAL_PATH"])
    )
    assert persisted is not None
    assert [c.name for c in persisted.virtual_chunk_containers.values()] == ["asdc"]

    # A rerun finds every slot done at the tip and commits nothing.
    assert relativize_refs.main() == 0
    assert next(repo.ancestry(branch="main")).id == tip.id


def test_resume_point_reads_the_tip_only(legacy: TinyCollection) -> None:
    repo = Processor().open_backfill_repo()
    assert relativize_refs.resume_point(repo, "main", 3) == 0
    session = repo.writable_session("main")
    zarr.open_group(session.store, mode="a").attrs["note"] = "batch"
    session.commit("Relativize virtual refs in slots [0, 2) of 3")
    assert relativize_refs.resume_point(repo, "main", 3) == 2
    # A different slot count means the axis changed underneath, so start over.
    assert relativize_refs.resume_point(repo, "main", 4) == 0
