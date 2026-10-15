"""Path setup and fixtures shared by the script tests."""

import os
import sys

# scripts/ holds project utility scripts that are not on the default pytest
# path; add it so their pure helpers can be unit-tested (mirrors tests/exploration).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "scripts"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pathlib  # noqa: E402
import pickle  # noqa: E402

import pytest  # noqa: E402
from tempo_fixtures import TinyCollection, build_tiny_collection  # noqa: E402
from virtualizarr_processor import backfill  # noqa: E402
from virtualizarr_processor import processor as processor_module  # noqa: E402
from virtualizarr_processor.processor import Processor  # noqa: E402


@pytest.fixture()
def legacy(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> TinyCollection:
    """A promoted store holding absolute references, as built before naming.

    Every stamp is unknown, as after any backfill.
    """
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
