import pathlib
from collections.abc import Iterator
from types import SimpleNamespace
from unittest.mock import MagicMock

import boto3
import pytest
from moto import mock_aws

BUCKET = "test-backfill-bucket"


@pytest.fixture()
def s3_bucket() -> Iterator[str]:
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield BUCKET


@pytest.fixture()
def lambda_context() -> MagicMock:
    """A stand-in Lambda context.

    powertools' @logger.inject_lambda_context reads context.function_name etc.
    at invocation, so handlers cannot be called with None; a MagicMock supplies
    any attribute (matching the existing tests/test_handler.py convention).
    """
    return MagicMock()


@pytest.fixture()
def tempo_pipeline(
    tmp_path: "pathlib.Path",
    monkeypatch: pytest.MonkeyPatch,
    s3_bucket: str,
) -> "SimpleNamespace":
    """A miniature TEMPO collection wired up for the handlers.

    Synthetic granules + generated template/coords + tiny config selected
    via $TEMPO_COLLECTION, a local-FS icechunk repo, a file:// virtual
    chunk container, and the typed inventory uploaded to moto S3.
    """
    import sys

    sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))
    from tempo_fixtures import build_tiny_collection

    tiny = build_tiny_collection(tmp_path / "collection", n=5)
    monkeypatch.delenv("ICECHUNK_BUCKET", raising=False)
    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "repo"))
    monkeypatch.setenv("VIRTUAL_CHUNK_PREFIX", f"file://{tmp_path}/")
    monkeypatch.setenv("TEMPO_COLLECTION", str(tiny.config_path))
    inventory_uri = f"s3://{s3_bucket}/inv.json"
    boto3.client("s3", region_name="us-east-1").put_object(
        Bucket=s3_bucket, Key="inv.json", Body=tiny.inventory.to_json().encode()
    )
    return SimpleNamespace(
        tiny=tiny,
        inventory_uri=inventory_uri,
        urls=tiny.urls,
        times=tiny.times,
    )
