"""Tests for the stamp-recording script."""

import os
import sys
import time
from datetime import datetime, timedelta, timezone

import pytest
import record_stamps
from tempo_fixtures import TinyCollection
from virtualizarr_processor.manifest import GranuleStamps, stamp_value
from virtualizarr_processor.processor import Processor


def source_stamp(path: os.PathLike[str]) -> str:
    modified = datetime.fromtimestamp(os.stat(path).st_mtime, tz=timezone.utc)
    return stamp_value(modified + timedelta(seconds=1))


def stamps() -> list[str]:
    repo = Processor().open_backfill_repo()
    read = GranuleStamps.read(repo.readonly_session("main").store)
    assert read is not None
    return read


def test_records_the_source_stamp_of_every_unknown_slot(
    legacy: TinyCollection, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = Processor().open_backfill_repo()
    session = repo.writable_session("main")
    recorded = GranuleStamps.read(session.store)
    assert recorded == ["", "", ""]
    recorded[1] = "2000-01-01T00:00:00+00:00"
    GranuleStamps.write(session.store, recorded)
    session.commit("stamp slot 1")

    monkeypatch.setattr(sys, "argv", ["record_stamps.py"])
    assert record_stamps.main() == 0

    # Unknown slots take their source's time. A recorded stamp is kept.
    assert stamps() == [
        source_stamp(legacy.granule_paths[0]),
        "2000-01-01T00:00:00+00:00",
        source_stamp(legacy.granule_paths[2]),
    ]
    tip = next(repo.ancestry(branch="main"))
    assert tip.message == "Record stamps for 2 slots"

    # Nothing unknown is left, so a rerun commits nothing.
    assert record_stamps.main() == 0
    assert next(repo.ancestry(branch="main")).id == tip.id


def test_leaves_a_slot_whose_source_changed_after_ingest(
    legacy: TinyCollection,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    future = time.time() + 86400
    os.utime(legacy.granule_paths[1], (future, future))

    monkeypatch.setattr(sys, "argv", ["record_stamps.py"])
    assert record_stamps.main() == 1

    assert stamps() == [
        source_stamp(legacy.granule_paths[0]),
        "",
        source_stamp(legacy.granule_paths[2]),
    ]
    assert "granule_1" in capsys.readouterr().err
