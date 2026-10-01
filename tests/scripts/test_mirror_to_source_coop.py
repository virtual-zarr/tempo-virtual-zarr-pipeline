"""Tests for the Source Cooperative tip publisher.

Both buckets live in one moto backend; the endpoint difference is only
client configuration. Pruning is Icechunk on a local directory, so the
source repository is built on disk and pushed into moto object by object.
"""

import io
import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import boto3
import icechunk
import mirror_to_source_coop as m
import numpy as np
import pytest
import zarr
from moto import mock_aws

SRC_BUCKET, DST_BUCKET = "air-quality", "pangeo"
SRC_PREFIX, DST_PREFIX = "tempo/no2/v04/", "tempo-virtual-icechunk/tempo/no2/v04/"


def build_repo(path: Path) -> str:
    """Three commits on main rewriting one native chunk, plus a stale branch.

    Returns the tip of main.
    """
    repo = icechunk.Repository.create(icechunk.local_filesystem_storage(str(path)))
    for i in range(3):
        session = repo.writable_session("main")
        if i == 0:
            array = zarr.create_array(
                session.store, name="x", shape=(1000,), chunks=(1000,), dtype="int64"
            )
        else:
            array = zarr.open_array(session.store, path="x")
        array[:] = np.arange(1000) + i  # 8 kB, well past the inline threshold
        snapshot = session.commit(f"commit {i}")
        if i == 0:
            repo.create_branch("backfill", snapshot)
    return snapshot


@pytest.fixture()
def tip(tmp_path: Path) -> str:
    """Build the source repository on disk; return the tip of main."""
    return build_repo(tmp_path / "source")


@contextmanager
def mock_s3(source: Path) -> Iterator[Any]:
    """Both buckets in one moto backend, with ``source``'s store files pushed."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-west-2")
        for bucket in (SRC_BUCKET, DST_BUCKET):
            client.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
            )
        for key in m.store_files(source):
            client.put_object(
                Bucket=SRC_BUCKET,
                Key=SRC_PREFIX + key,
                Body=(source / key).read_bytes(),
            )
        yield client


@pytest.fixture()
def s3(tmp_path: Path, tip: str) -> Iterator[Any]:
    with mock_s3(tmp_path / "source") as client:
        yield client


def run(client: Any, directory: Path, *, upload_copy: bool = True) -> Path:
    """The stages as ``main`` runs them; return the zip."""
    archive = directory.with_name(directory.name + ".zip")
    m.download(client, SRC_BUCKET, SRC_PREFIX, directory, workers=4)
    m.prune(directory)
    m.zip_store(directory, archive)
    if upload_copy:
        m.upload(client, directory, DST_BUCKET, DST_PREFIX, workers=4)
    return archive


def published(s3: Any) -> dict[str, int]:
    return m.object_sizes(s3, DST_BUCKET, DST_PREFIX)


def test_clients_ignore_a_configured_endpoint(monkeypatch: Any) -> None:
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:9000")
    monkeypatch.setenv("SOURCE_COOP_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("SOURCE_COOP_SECRET_ACCESS_KEY", "secret")

    for client in (m.source_client(), m.destination_client()):
        assert client.meta.endpoint_url == "https://s3.us-west-2.amazonaws.com"


def test_destination_credentials_are_required(monkeypatch: Any) -> None:
    # Without them boto3 would sign with the source's AWS credentials.
    monkeypatch.delenv("SOURCE_COOP_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("SOURCE_COOP_SECRET_ACCESS_KEY", raising=False)
    with pytest.raises(SystemExit):
        m.destination_client()

    monkeypatch.setenv("SOURCE_COOP_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("SOURCE_COOP_SECRET_ACCESS_KEY", "secret")

    client = m.destination_client()
    # Path-style, since the bucket name has dots.
    assert client.meta.config.s3["addressing_style"] == "path"
    assert client.meta.endpoint_url == "https://s3.us-west-2.amazonaws.com"


def test_store_files_skips_what_is_not_icechunks(tmp_path: Path) -> None:
    for name in (
        "repo",
        "config.yaml",
        "chunks/ABC",
        "chunks/.DS_Store",
        "chunks/ABC.s3transfer-tmp",  # a plain file, so it counts
        "manifests/DEF",
        ".DS_Store",
        "overwritten/repo.1",
        "notes.txt",
        "chunks/nested/XYZ",
    ):
        (tmp_path / name).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / name).write_bytes(b"x")

    assert set(m.store_files(tmp_path)) == {
        "repo",
        "config.yaml",
        "chunks/ABC",
        "chunks/ABC.s3transfer-tmp",
        "manifests/DEF",
    }


def test_publishes_only_the_tip_of_main(s3: Any, tip: str, tmp_path: Path) -> None:
    order: list[str] = []
    s3.meta.events.register(
        "provide-client-params.s3.PutObject",
        lambda params, **_: order.append(params["Key"]),
    )
    source_keys = m.object_sizes(s3, SRC_BUCKET, SRC_PREFIX)
    assert sum(k.startswith("chunks/") for k in source_keys) == 3

    archive = run(s3, tmp_path / "copy")

    keys = set(published(s3))
    assert sum(k.startswith("chunks/") for k in keys) == 1
    assert sum(k.startswith("snapshots/") for k in keys) == 2  # root + tip
    # Root, tip, and the two expired ancestors the tip still references.
    assert sum(k.startswith("transactions/") for k in keys) == 4
    assert all(k == "repo" or k.split("/")[0] in m.STORE_DIRS for k in keys)
    assert order[-1] == DST_PREFIX + "repo"

    # The zip is the same pruned store, readable after unzipping.
    assert archive == tmp_path / "copy.zip"
    unpacked = tmp_path / "unpacked"
    shutil.unpack_archive(archive, unpacked)
    assert m.store_files(unpacked).keys() == keys
    repo = icechunk.Repository.open(icechunk.local_filesystem_storage(str(unpacked)))
    assert repo.list_branches() == {"main"}
    assert repo.lookup_branch("main") == tip
    assert (
        repo.inspect_transaction_log(tip)["synthetic_composite"]["missing_tx_logs"]
        == []
    )
    array = zarr.open_array(repo.readonly_session("main").store, path="x")
    np.testing.assert_array_equal(array[:], np.arange(1000) + 2)


def test_rerun_uploads_only_the_repo_file(s3: Any, tmp_path: Path) -> None:
    run(s3, tmp_path / "copy")
    order: list[str] = []
    s3.meta.events.register(
        "provide-client-params.s3.PutObject",
        lambda params, **_: order.append(params["Key"]),
    )
    run(s3, tmp_path / "copy2")
    assert order == [DST_PREFIX + "repo"]


def test_failed_upload_leaves_the_old_tip(s3: Any, tmp_path: Path) -> None:
    run(s3, tmp_path / "copy")
    before = s3.get_object(Bucket=DST_BUCKET, Key=DST_PREFIX + "repo")["Body"].read()
    chunk = next(k for k in published(s3) if k.startswith("chunks/"))
    s3.delete_object(Bucket=DST_BUCKET, Key=DST_PREFIX + chunk)

    def fail_chunk_puts(params: Any, **_: Any) -> None:
        if params["Key"].startswith(DST_PREFIX + "chunks/"):
            raise RuntimeError("simulated upload failure")

    s3.meta.events.register("provide-client-params.s3.PutObject", fail_chunk_puts)
    with pytest.raises(Exception, match="simulated upload failure"):
        run(s3, tmp_path / "copy2")

    assert chunk not in published(s3)
    after = s3.get_object(Bucket=DST_BUCKET, Key=DST_PREFIX + "repo")["Body"].read()
    assert after == before


def test_refuses_a_non_empty_directory_or_an_existing_zip(
    s3: Any, tmp_path: Path
) -> None:
    (tmp_path / "copy").mkdir()
    (tmp_path / "copy" / "precious").write_text("do not delete")
    with pytest.raises(SystemExit, match="not empty"):
        run(s3, tmp_path / "copy")
    assert (tmp_path / "copy" / "precious").read_text() == "do not delete"

    (tmp_path / "other.zip").write_bytes(b"old")
    with pytest.raises(SystemExit, match="exists"):
        run(s3, tmp_path / "other")
    assert (tmp_path / "other.zip").read_bytes() == b"old"


def test_report_and_no_upload_write_nothing(s3: Any, tmp_path: Path) -> None:
    log = io.StringIO()
    m.report(s3, SRC_BUCKET, SRC_PREFIX, log=log)
    assert "the tip of main needs" in log.getvalue()
    assert not (tmp_path / "copy").exists()

    archive = run(s3, tmp_path / "copy", upload_copy=False)
    assert archive.exists()
    assert published(s3) == {}


def test_limit_downloads_a_partial_store(s3: Any, tmp_path: Path) -> None:
    m.download(s3, SRC_BUCKET, SRC_PREFIX, tmp_path / "copy", workers=4, limit=1)
    # repo, the tip and root snapshots fetched directly, plus one object.
    assert len(m.store_files(tmp_path / "copy")) == 4
    assert published(s3) == {}


def test_zip_store_refuses_a_directory_without_a_store(tmp_path: Path) -> None:
    (tmp_path / "empty").mkdir()
    with pytest.raises(SystemExit, match="no Icechunk store"):
        m.zip_store(tmp_path / "empty", tmp_path / "empty.zip")
    assert not (tmp_path / "empty.zip").exists()
