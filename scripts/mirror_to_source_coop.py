#!/usr/bin/env python3
"""Copy the Icechunk store to Source Cooperative, with a zip of it alongside.

Streams every object under the store prefix through this process: each
is read from the source, written to Source Coop under
``<DEST_ROOT>/<prefix>/`` and added to a local zip, which is then
uploaded as ``<DEST_ROOT>/<prefix>.zip``. The objects never touch disk;
the zip does, so free space of about the store's size is needed.
Nothing is compared, ordered or deleted; a rerun copies everything again
and overwrites what is there. Run it from the VEDA JupyterHub, in
us-west-2 with both buckets: docs/runbook-mirror-to-source-coop.md.

Source reads use your AWS credentials. Destination writes use the keys
Source Coop issued: SOURCE_COOP_ACCESS_KEY_ID, SOURCE_COOP_SECRET_ACCESS_KEY
and optionally SOURCE_COOP_SESSION_TOKEN. The store location comes from
ICECHUNK_BUCKET and S3_PREFIX/ICECHUNK_PREFIX.

Usage:
    uv run --env-file .env_no2 --env-file .env.local scripts/mirror_to_source_coop.py
"""

from __future__ import annotations

import argparse
import os
import sys
import threading
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from virtualizarr_processor.manifest import storage_prefix

DEST_BUCKET = "us-west-2.opendata.source.coop"
DEST_ROOT = "pangeo/tempo-virtual-icechunk"
REGION = "us-west-2"  # the store and Source Coop both live here
WORKERS = 16
# Ignore any AWS_ENDPOINT_URL in the environment, which would redirect both
# sides. botocore honors this from 1.29 on; the stubs do not list it yet.
NO_ENDPOINT_OVERRIDE = Config(ignore_configured_endpoint_urls=True)  # type: ignore[call-arg]


def source_client() -> Any:
    """The source store, read with your own AWS credentials."""
    return boto3.client("s3", region_name=REGION, config=NO_ENDPOINT_OVERRIDE)


def destination_client() -> Any:
    """Source Coop, written with the keys it issued.

    Required up front, since boto3 would otherwise sign with your AWS
    identity and get a bare AccessDenied. Path-style because the bucket
    name has dots.
    """
    key = os.environ.get("SOURCE_COOP_ACCESS_KEY_ID")
    secret = os.environ.get("SOURCE_COOP_SECRET_ACCESS_KEY")
    if not key or not secret:
        raise SystemExit(
            "set SOURCE_COOP_ACCESS_KEY_ID and SOURCE_COOP_SECRET_ACCESS_KEY "
            "to the credentials Source Coop issued for the repository; your "
            "own AWS credentials grant nothing there"
        )
    return boto3.client(
        "s3",
        region_name=REGION,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        aws_session_token=os.environ.get("SOURCE_COOP_SESSION_TOKEN"),
        config=NO_ENDPOINT_OVERRIDE.merge(Config(s3={"addressing_style": "path"})),
    )


def list_keys(client: Any, bucket: str, prefix: str) -> list[str]:
    """Every key under ``prefix`` (ending in ``/``), relative to it."""
    pages = client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix
    )
    return [
        obj["Key"][len(prefix) :] for page in pages for obj in page.get("Contents", [])
    ]


def mirror(
    source: Any, destination: Any, bucket: str, prefix: str, archive: Path
) -> list[str]:
    """Copy ``s3://bucket/prefix/`` and a zip of it to Source Coop; return the keys.

    ``prefix`` has no slashes at either end. Each object is held in memory
    between its GET and PUT, so WORKERS objects at a time; Icechunk's
    files are at most a few hundred MB (manifests), the rest KB.
    """
    keys = list_keys(source, bucket, f"{prefix}/")
    dest = f"{DEST_ROOT}/{prefix}/"
    print(
        f"copying {len(keys):,} objects to s3://{DEST_BUCKET}/{dest}", file=sys.stderr
    )
    lock = threading.Lock()  # zipfile is not thread-safe
    # Stored, not deflated: Icechunk's files are already compressed.
    with zipfile.ZipFile(archive, "w") as zf:

        def copy(key: str) -> None:
            body = source.get_object(Bucket=bucket, Key=f"{prefix}/{key}")[
                "Body"
            ].read()
            destination.put_object(Bucket=DEST_BUCKET, Key=dest + key, Body=body)
            with lock:
                zf.writestr(key, body)

        with ThreadPoolExecutor(WORKERS) as pool:
            list(pool.map(copy, keys))
    print(f"uploading {archive}", file=sys.stderr)
    destination.upload_file(str(archive), DEST_BUCKET, f"{DEST_ROOT}/{prefix}.zip")
    return keys


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--zip", type=Path, help="where to build the zip (default: stores/<prefix>.zip)"
    )
    args = parser.parse_args()

    bucket = os.environ.get("ICECHUNK_BUCKET")
    prefix = storage_prefix()
    if not bucket or not prefix:
        print(
            "ICECHUNK_BUCKET and S3_PREFIX/ICECHUNK_PREFIX must be set", file=sys.stderr
        )
        return 2
    prefix = prefix.strip("/")
    archive = args.zip or Path("stores") / f"{prefix}.zip"
    archive.parent.mkdir(parents=True, exist_ok=True)
    print(
        f"s3://{bucket}/{prefix}/ -> s3://{DEST_BUCKET}/{DEST_ROOT}/{prefix}/",
        file=sys.stderr,
    )
    keys = mirror(source_client(), destination_client(), bucket, prefix, archive)
    print(f"done: {len(keys):,} objects + s3://{DEST_BUCKET}/{DEST_ROOT}/{prefix}.zip")
    return 0


if __name__ == "__main__":
    sys.exit(main())
