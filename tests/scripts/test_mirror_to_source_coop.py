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
        for key in mirror_to_source_coop.file_sizes(source):
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


def test_publishes_only_the_tip_of_main(s3: Any, tip: str, tmp_path: Path) -> None:
    order: list[str] = []
    s3.meta.events.register(
        "provide-client-params.s3.PutObject",
        lambda params, **_: order.append(params["Key"]),
    )
    source_keys = mirror_to_source_coop.object_sizes(s3, SRC_BUCKET, SRC_PREFIX)
    assert sum(k.startswith("chunks/") for k in source_keys) == 3

    archive = run(s3, tmp_path / "copy")

    published = mirror_to_source_coop.object_sizes(s3, DST_BUCKET, DST_PREFIX)
    assert sum(k.startswith("chunks/") for k in published) == 1
    assert sum(k.startswith("snapshots/") for k in published) == 2  # root + tip
    assert not any(k.startswith("overwritten/") for k in published)
    assert order[-1] == DST_PREFIX + "repo"

    # The zip is the same pruned store, readable after unzipping.
    assert archive is not None
    unpacked = tmp_path / "unpacked"
    shutil.unpack_archive(archive, unpacked)
    repo = icechunk.Repository.open(icechunk.local_filesystem_storage(str(unpacked)))
    assert repo.list_branches() == {"main"}
    assert repo.lookup_branch("main") == tip
    array = zarr.open_array(repo.readonly_session("main").store, path="x")
    np.testing.assert_array_equal(array[:], np.arange(1000) + 2)


def test_rerun_uploads_only_the_repo_file(s3: Any, tmp_path: Path) -> None:
    run(s3, tmp_path / "copy")
    order: list[str] = []
    s3.meta.events.register(
        "provide-client-params.s3.PutObject",
        lambda params, **_: order.append(params["Key"]),
    )
    run(s3, tmp_path / "copy")
    assert order == [DST_PREFIX + "repo"]


def test_dry_run_and_no_upload_write_nothing(s3: Any, tmp_path: Path) -> None:
    assert run(s3, tmp_path / "copy", dry_run=True) is None
    assert not (tmp_path / "copy").exists()

    archive = run(s3, tmp_path / "copy", upload_copy=False)
    assert archive is not None and archive.exists()
    assert mirror_to_source_coop.object_sizes(s3, DST_BUCKET, DST_PREFIX) == {}
