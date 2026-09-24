"""Tests for the Source Cooperative snapshot publisher.

Both buckets live in one moto backend; the endpoint difference is boto3
configuration, not behaviour worth mocking. The ordering is what these
check: every immutable file must land before the repo file that names it.
"""

from typing import Any

import boto3
import mirror_to_source_coop
import pytest
from moto import mock_aws

SRC_BUCKET, DST_BUCKET = "air-quality", "pangeo"
SRC_PREFIX, DST_PREFIX = "tempo/no2/v04/", "tempo-virtual-icechunk/tempo/no2/v04/"
STORE = {
    "repo": b"repo-info pointing at snapshot-a",
    "config.yaml": b"manifest:\n  splitting: {}\n",
    "snapshots/snapshot-a": b"snapshot",
    "manifests/manifest-1": b"manifest",
    "transactions/txn-1": b"txn",
    "chunks/chunk-1": b"chunk",
}


@pytest.fixture()
def s3() -> Any:
    with mock_aws():
        client = boto3.client("s3", region_name="us-west-2")
        for bucket in (SRC_BUCKET, DST_BUCKET):
            client.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
            )
        for key, body in STORE.items():
            client.put_object(Bucket=SRC_BUCKET, Key=SRC_PREFIX + key, Body=body)
        yield client


def run(client: Any, **kwargs: Any) -> int:
    return mirror_to_source_coop.mirror(
        client,
        client,
        src_bucket=SRC_BUCKET,
        src_prefix=SRC_PREFIX,
        dst_bucket=DST_BUCKET,
        dst_prefix=DST_PREFIX,
        workers=4,
        **kwargs,
    )


def written_keys(client: Any) -> list[str]:
    """Destination keys, relative to the destination prefix."""
    listing = client.list_objects_v2(Bucket=DST_BUCKET, Prefix=DST_PREFIX)
    return [obj["Key"][len(DST_PREFIX) :] for obj in listing.get("Contents", [])]


def test_destination_credentials_are_required(monkeypatch: Any) -> None:
    # Left to the ambient chain, boto3 would sign Source Coop requests with
    # whatever AWS credentials the source read used and get AccessDenied.
    monkeypatch.delenv("SOURCE_COOP_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("SOURCE_COOP_SECRET_ACCESS_KEY", raising=False)
    with pytest.raises(SystemExit):
        mirror_to_source_coop.destination_client()

    monkeypatch.setenv("SOURCE_COOP_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("SOURCE_COOP_SECRET_ACCESS_KEY", "secret")
    client = mirror_to_source_coop.destination_client()
    # Path-style: the bucket is a path segment of data.source.coop, not a
    # subdomain of it.
    assert client.meta.config.s3["addressing_style"] == "path"
    assert client.meta.endpoint_url == mirror_to_source_coop.DEST_ENDPOINT


def test_publishes_whole_store_with_repo_info_last(s3: Any) -> None:
    order: list[str] = []
    s3.meta.events.register(
        "provide-client-params.s3.PutObject",
        lambda params, **_: order.append(params["Key"]),
    )

    assert run(s3) == 4
    assert sorted(written_keys(s3)) == sorted(STORE)
    for key in STORE:
        assert (
            s3.get_object(Bucket=DST_BUCKET, Key=DST_PREFIX + key)["Body"].read()
            == STORE[key]
        )
    # A reader that sees the published repo file can resolve every file it
    # names, because they all landed first.
    assert order[-1] == DST_PREFIX + "repo"


def test_second_run_copies_nothing(s3: Any) -> None:
    run(s3)
    assert run(s3) == 0


def test_new_source_files_are_copied_incrementally(s3: Any) -> None:
    run(s3)
    s3.put_object(
        Bucket=SRC_BUCKET, Key=SRC_PREFIX + "snapshots/snapshot-b", Body=b"newer"
    )
    s3.put_object(Bucket=SRC_BUCKET, Key=SRC_PREFIX + "repo", Body=b"now snapshot-b")

    assert run(s3) == 1
    assert (
        s3.get_object(Bucket=DST_BUCKET, Key=DST_PREFIX + "repo")["Body"].read()
        == b"now snapshot-b"
    )


def test_dry_run_writes_nothing(s3: Any) -> None:
    assert run(s3, dry_run=True) == 0
    assert written_keys(s3) == []
