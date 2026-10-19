"""Tests for the relative-reference migration script."""

import os
import pathlib
import shutil
import sys
import time
from datetime import datetime, timezone

import icechunk
import numpy as np
import pytest
import record_stamps
import relativize_refs
import zarr
from tempo_fixtures import TinyCollection, expected_vertical_column
from virtualizarr_processor.manifest import GranuleStamps, stamp_value
from virtualizarr_processor.processor import Processor

STALE = "checksum of the object owning the virtual chunk has changed"


@pytest.fixture()
def moved(
    legacy: TinyCollection, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> pathlib.Path:
    """The granules, stamped and then moved out from under their references.

    The rewrite must come from the store's own references. Nothing is left
    to parse at the old location.
    """
    monkeypatch.setattr(sys, "argv", ["record_stamps.py"])
    assert record_stamps.main() == 0
    target = tmp_path / "moved" / "collection"
    target.mkdir(parents=True)
    for path in legacy.granule_paths:
        shutil.move(path, target / path.name)
    return tmp_path / "moved"


def locations_under(moved: pathlib.Path) -> tuple[list[str], zarr.Group]:
    """Open the store with the container pointed at the moved granules.

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


def slot(group: zarr.Group, i: int) -> np.ndarray:
    return np.asarray(group["vertical_column"][i])


def test_refuses_a_store_with_unknown_stamps(
    legacy: TinyCollection, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = Processor().open_backfill_repo()
    tip = next(repo.ancestry(branch="main"))
    monkeypatch.setattr(sys, "argv", ["relativize_refs.py"])
    assert relativize_refs.main() == 1
    assert next(repo.ancestry(branch="main")).id == tip.id


def test_rewrites_every_slot_relative_from_the_store(
    legacy: TinyCollection, moved: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    locations, _ = locations_under(moved)
    assert locations
    assert not any(loc.startswith(f"file://{moved}/") for loc in locations)

    monkeypatch.setattr(sys, "argv", ["relativize_refs.py"])
    assert relativize_refs.main() == 0

    repo = Processor().open_backfill_repo()
    tip = next(repo.ancestry(branch="main"))
    assert tip.message == "Relativize virtual refs in 3 slots"
    locations, group = locations_under(moved)
    assert all(loc.startswith(f"file://{moved}/") for loc in locations)
    for i, time_value in enumerate(legacy.times):
        np.testing.assert_array_equal(
            slot(group, i), expected_vertical_column(time_value)[0]
        )
    persisted = icechunk.Repository.fetch_config(
        icechunk.local_filesystem_storage(os.environ["ICECHUNK_LOCAL_PATH"])
    )
    assert persisted is not None
    assert [c.name for c in persisted.virtual_chunk_containers.values()] == ["asdc"]

    # A rerun rewrites the same references again. That is harmless.
    assert relativize_refs.main() == 0
    _, group = locations_under(moved)
    np.testing.assert_array_equal(
        slot(group, 0), expected_vertical_column(legacy.times[0])[0]
    )


def test_writes_each_slot_with_its_recorded_stamp(
    legacy: TinyCollection, moved: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Slot 1 carries a stamp older than its source.
    repo = Processor().open_backfill_repo()
    session = repo.writable_session("main")
    stamps = GranuleStamps.read(session.store)
    assert stamps is not None
    stamps[1] = stamp_value(datetime(2000, 1, 1, tzinfo=timezone.utc))
    GranuleStamps.write(session.store, stamps)
    session.commit("age slot 1")

    monkeypatch.setattr(sys, "argv", ["relativize_refs.py"])
    assert relativize_refs.main() == 0

    _, group = locations_under(moved)
    with pytest.raises(Exception, match=STALE):
        slot(group, 1)
    slot(group, 0)
    slot(group, 2)
    assert GranuleStamps.read(group.store) == stamps

    # The recorded stamps are the sources' own times, so a source
    # overwritten later fails to read, as before the migration.
    future = time.time() + 86400
    os.utime(moved / "collection" / legacy.granule_paths[0].name, (future, future))
    _, group = locations_under(moved)
    with pytest.raises(Exception, match=STALE):
        slot(group, 0)
    slot(group, 2)
