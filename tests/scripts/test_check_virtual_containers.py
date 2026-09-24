"""Tests for the virtual chunk container check.

Built on a real tiny store, since what the check inspects is the config
icechunk persists at repository creation and the failure it catches is that
config drifting away from the URLs the manifest references.
"""

import pathlib
import pickle
import sys

import check_virtual_containers
import icechunk
import pytest
from tempo_fixtures import TinyCollection, build_tiny_collection
from virtualizarr_processor import backfill
from virtualizarr_processor.processor import Processor


@pytest.fixture()
def tiny(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> TinyCollection:
    collection = build_tiny_collection(tmp_path / "collection")
    monkeypatch.delenv("ICECHUNK_BUCKET", raising=False)
    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "repo"))
    monkeypatch.setenv("VIRTUAL_CHUNK_PREFIX", f"file://{tmp_path}/")
    monkeypatch.setenv("TEMPO_COLLECTION", str(collection.config_path))

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


def run(monkeypatch: pytest.MonkeyPatch, *argv: str) -> int:
    monkeypatch.setattr(sys, "argv", ["check_virtual_containers.py", *argv])
    return check_virtual_containers.main()


def break_persisted_container(path: pathlib.Path) -> None:
    """Leave the store declaring a container that covers nothing it uses."""
    storage = icechunk.local_filesystem_storage(str(path))
    config = icechunk.Repository.fetch_config(storage)
    assert config is not None
    config.clear_virtual_chunk_containers()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            "file:///moved/", icechunk.local_filesystem_store("/moved")
        )
    )
    icechunk.Repository.open(storage=storage, config=config).save_config()


def test_uncovered_urls_reports_one_prefix_per_location() -> None:
    urls = [f"s3://asdc-prod-protected/TEMPO/x{i}.nc" for i in range(3)]
    covered = check_virtual_containers.uncovered_urls(
        {"s3://asdc-prod-protected/"}, urls
    )
    assert covered == []
    assert check_virtual_containers.uncovered_urls({"s3://other/"}, urls) == [
        "s3://asdc-prod-protected/TEMPO/"
    ]


def test_freshly_created_store_passes(
    tiny: TinyCollection, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert run(monkeypatch) == 0


def test_stale_container_fails_and_fix_repairs_it(
    tiny: TinyCollection, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    break_persisted_container(tmp_path / "repo")
    assert run(monkeypatch) == 1

    assert run(monkeypatch, "--fix") == 1  # non-zero: it changed something
    assert run(monkeypatch) == 0


def test_no_read_skips_the_chunk_fetch(
    tiny: TinyCollection, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    break_persisted_container(tmp_path / "repo")
    # The stale container is still reported; only the read is skipped.
    assert run(monkeypatch, "--no-read") == 1
    run(monkeypatch, "--fix")
    assert run(monkeypatch, "--no-read") == 0
