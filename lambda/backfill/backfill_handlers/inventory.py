"""Read the S3 inventory file and read/write partition manifests (JSON key lists)."""

import json
from typing import cast

from virtualizarr_processor.inventory import BackfillInventory

from backfill_handlers.config import parse_s3_uri, s3_client


def read_inventory(uri: str) -> BackfillInventory:
    """Read and validate the typed inventory document from S3.

    Parse-time validation (non-empty, strictly increasing unique times,
    unique granule URs) runs again here on the consume side, so a
    hand-edited or corrupted inventory fails the run before any write.
    """
    bucket, key = parse_s3_uri(uri)
    body = s3_client().get_object(Bucket=bucket, Key=key)["Body"].read()
    return BackfillInventory.from_json(body)


def write_manifest(uri: str, keys: list[str]) -> None:
    """Write a partition manifest (JSON array of keys) to S3."""
    bucket, key = parse_s3_uri(uri)
    s3_client().put_object(Bucket=bucket, Key=key, Body=json.dumps(keys).encode())


def read_manifest(uri: str) -> list[str]:
    """Read a partition manifest (JSON array of keys) from S3."""
    bucket, key = parse_s3_uri(uri)
    body = s3_client().get_object(Bucket=bucket, Key=key)["Body"].read()
    return cast(list[str], json.loads(body))
