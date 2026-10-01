"""Both buckets live in one moto backend; the endpoint difference is only
client configuration."""

import zipfile
from pathlib import Path
from typing import Any

import boto3
import mirror_to_source_coop as m
import pytest
from moto import mock_aws

OBJECTS = {"repo": b"r", "config.yaml": b"c", "chunks/A": b"aaa", "manifests/B": b"bb"}


def keys(client: Any, bucket: str, prefix: str) -> dict[str, bytes]:
    return {
        key: client.get_object(Bucket=bucket, Key=prefix + key)["Body"].read()
        for key in m.list_keys(client, bucket, prefix)
    }


def test_mirror_copies_every_object_and_a_zip(tmp_path: Path) -> None:
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-west-2")
        for bucket in ("air-quality", m.DEST_BUCKET):
            s3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
            )
        for key, body in OBJECTS.items():
            s3.put_object(Bucket="air-quality", Key="tempo/no2/v04/" + key, Body=body)

        m.mirror(s3, s3, "air-quality", "tempo/no2/v04", tmp_path / "v04.zip")

        dest = f"{m.DEST_ROOT}/tempo/no2/v04/"
        assert keys(s3, m.DEST_BUCKET, dest) == OBJECTS
        s3.download_file(
            m.DEST_BUCKET, dest.rstrip("/") + ".zip", str(tmp_path / "got.zip")
        )
        with zipfile.ZipFile(tmp_path / "got.zip") as zf:
            assert {info.filename: zf.read(info) for info in zf.infolist()} == OBJECTS
        assert {p.name for p in tmp_path.iterdir()} == {
            "v04.zip",
            "got.zip",
        }  # no store dir


def test_destination_credentials_are_required(monkeypatch: Any) -> None:
    monkeypatch.delenv("SOURCE_COOP_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("SOURCE_COOP_SECRET_ACCESS_KEY", raising=False)
    with pytest.raises(SystemExit):
        m.destination_client()

    monkeypatch.setenv("SOURCE_COOP_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("SOURCE_COOP_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:9000")  # ignored
    client = m.destination_client()
    assert client.meta.config.s3["addressing_style"] == "path"  # bucket name has dots
    assert client.meta.endpoint_url == "https://s3.us-west-2.amazonaws.com"
    assert m.source_client().meta.endpoint_url == "https://s3.us-west-2.amazonaws.com"
