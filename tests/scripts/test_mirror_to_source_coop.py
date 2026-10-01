"""Tests for the Source Cooperative tip publisher.

Both buckets live in one moto backend; the endpoint difference is only
client configuration. Pruning is Icechunk on a local directory, so the
source repository is built on disk and pushed into moto object by object.
"""

import shutil
from pathlib import Path
from typing import Any

import boto3
import icechunk
import mirror_to_source_coop
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


@pytest.fixture()
def s3(tmp_path: Path, tip: str) -> Any:
    with mock_aws():
        client = boto3.client("s3", region_name="us-west-2")
        for bucket in (SRC_BUCKET, DST_BUCKET):
            client.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
            )
        source = tmp_path / "source"
        for key in mirror_to_source_coop.store_files(source):
            client.put_object(
                Bucket=SRC_BUCKET,
                Key=SRC_PREFIX + key,
                Body=(source / key).read_bytes(),
            )
        yield client


def run(client: Any, directory: Path, **kwargs: Any) -> Path | None:
    return mirror_to_source_coop.publish(
        client,
        client,
        source_bucket=SRC_BUCKET,
        source_prefix=SRC_PREFIX,
        destination_bucket=DST_BUCKET,
        destination_prefix=DST_PREFIX,
        directory=directory,
        workers=4,
        **kwargs,
    )


def published(s3: Any) -> dict[str, int]:
    return mirror_to_source_coop.object_sizes(s3, DST_BUCKET, DST_PREFIX)


def test_clients_ignore_a_configured_endpoint(monkeypatch: Any) -> None:
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:9000")
    monkeypatch.setenv("SOURCE_COOP_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("SOURCE_COOP_SECRET_ACCESS_KEY", "secret")

    for client in (
        mirror_to_source_coop.source_client(),
        mirror_to_source_coop.destination_client(),
    ):
        assert client.meta.endpoint_url == "https://s3.us-west-2.amazonaws.com"


def test_destination_credentials_are_required(monkeypatch: Any) -> None:
    # Without them boto3 would sign with the source's AWS credentials.
    monkeypatch.delenv("SOURCE_COOP_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("SOURCE_COOP_SECRET_ACCESS_KEY", raising=False)
    with pytest.raises(SystemExit):
        mirror_to_source_coop.destination_client()

    monkeypatch.setenv("SOURCE_COOP_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("SOURCE_COOP_SECRET_ACCESS_KEY", "secret")

    client = mirror_to_source_coop.destination_client()
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

    assert set(mirror_to_source_coop.store_files(tmp_path)) == {
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
    source_keys = mirror_to_source_coop.object_sizes(s3, SRC_BUCKET, SRC_PREFIX)
    assert sum(k.startswith("chunks/") for k in source_keys) == 3

    archive = run(s3, tmp_path / "copy")

    keys = set(published(s3))
    assert sum(k.startswith("chunks/") for k in keys) == 1
    assert sum(k.startswith("snapshots/") for k in keys) == 2  # root + tip
    # Root, tip, and the two expired ancestors the tip still references.
    assert sum(k.startswith("transactions/") for k in keys) == 4
    assert all(
        k == "repo" or k.split("/")[0] in mirror_to_source_coop.STORE_DIRS for k in keys
    )
    assert order[-1] == DST_PREFIX + "repo"

    # The zip is the same pruned store, readable after unzipping.
    assert archive is not None
    assert archive == (tmp_path / "copy.zip").resolve()
    unpacked = tmp_path / "unpacked"
    shutil.unpack_archive(archive, unpacked)
    assert mirror_to_source_coop.store_files(unpacked).keys() == keys
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
        run(s3, tmp_path / "copy", dry_run=True)
    assert (tmp_path / "copy" / "precious").read_text() == "do not delete"

    (tmp_path / "other.zip").write_bytes(b"old")
    with pytest.raises(SystemExit, match="exists"):
        run(s3, tmp_path / "other")
    assert (tmp_path / "other.zip").read_bytes() == b"old"


def test_dry_run_and_no_upload_write_nothing(s3: Any, tmp_path: Path) -> None:
    assert run(s3, tmp_path / "copy", dry_run=True) is None
    assert not (tmp_path / "copy").exists()

    archive = run(s3, tmp_path / "copy", upload_copy=False)
    assert archive is not None and archive.exists()
    assert published(s3) == {}


def test_limit_stops_after_a_partial_download(s3: Any, tmp_path: Path) -> None:
    assert run(s3, tmp_path / "copy", upload_copy=False, limit=1) is None
    # repo, the tip and root snapshots fetched directly, plus one object.
    assert len(mirror_to_source_coop.store_files(tmp_path / "copy")) == 4
    assert not (tmp_path / "copy.zip").exists()
    assert published(s3) == {}


def test_dry_run_reports_the_tip(s3: Any, tmp_path: Path) -> None:
    lines: list[str] = []

    class Log:
        def write(self, text: str) -> None:
            lines.append(text)

        def flush(self) -> None:
            pass

    run(s3, tmp_path / "copy", dry_run=True, log=Log())
    report = "".join(lines)
    assert "the tip of main needs" in report
    assert not (tmp_path / "copy").exists()
