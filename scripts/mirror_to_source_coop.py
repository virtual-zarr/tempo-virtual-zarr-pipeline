#!/usr/bin/env python3
"""Copy the Icechunk store to Source Cooperative, with a zip of it alongside.

Downloads every object under the store prefix to a local directory, zips
that directory, then uploads both to Source Coop: the objects under
``<DEST_ROOT>/<prefix>/`` and the zip as ``<DEST_ROOT>/<prefix>.zip``.
Nothing is compared, ordered or deleted; a rerun copies everything again
and overwrites what is there. Run it in us-west-2, where both buckets
live: docs/runbook-mirror-to-source-coop.md.

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


def download(source: Any, bucket: str, prefix: str, directory: Path) -> list[str]:
    """Fetch everything under ``prefix`` into ``directory``; return the keys."""
    keys = list_keys(source, bucket, prefix)
    print(f"downloading {len(keys):,} objects to {directory}", file=sys.stderr)

    def get(key: str) -> None:
        path = directory / key
        path.parent.mkdir(parents=True, exist_ok=True)
        source.download_file(bucket, prefix + key, str(path))

    with ThreadPoolExecutor(WORKERS) as pool:
        list(pool.map(get, keys))
    return keys


def zip_dir(directory: Path, keys: list[str], archive: Path) -> None:
    """Zip ``keys`` under ``directory`` into ``archive``, stored not deflated:
    Icechunk's files are already compressed."""
    print(f"zipping {archive}", file=sys.stderr)
    with zipfile.ZipFile(archive, "w") as zf:
        for key in sorted(keys):
            zf.write(directory / key, key)


def upload(destination: Any, files: dict[Path, str], bucket: str) -> None:
    """Send each local path to its key in ``bucket``."""
    print(f"uploading {len(files):,} objects to s3://{bucket}", file=sys.stderr)

    def put(item: tuple[Path, str]) -> None:
        destination.upload_file(str(item[0]), bucket, item[1])

    with ThreadPoolExecutor(WORKERS) as pool:
        list(pool.map(put, files.items()))


def mirror(
    source: Any, destination: Any, bucket: str, prefix: str, directory: Path
) -> dict[Path, str]:
    """Copy ``s3://bucket/prefix/`` and a zip of it to Source Coop.

    ``prefix`` has no slashes at either end. Returns what was uploaded,
    local path -> destination key.
    """
    archive = directory.with_name(directory.name + ".zip")
    keys = download(source, bucket, f"{prefix}/", directory)
    zip_dir(directory, keys, archive)
    files = {directory / key: f"{DEST_ROOT}/{prefix}/{key}" for key in keys}
    files[archive] = f"{DEST_ROOT}/{prefix}.zip"
    upload(destination, files, DEST_BUCKET)
    return files


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--dir",
        type=Path,
        help="local directory for the copy (default: stores/<prefix>)",
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
    directory = (args.dir or Path("stores") / prefix).resolve()
    print(
        f"s3://{bucket}/{prefix}/ -> s3://{DEST_BUCKET}/{DEST_ROOT}/{prefix}/",
        file=sys.stderr,
    )
    files = mirror(source_client(), destination_client(), bucket, prefix, directory)
    print(
        f"done: {len(files):,} objects, zip at s3://{DEST_BUCKET}/{DEST_ROOT}/{prefix}.zip"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
