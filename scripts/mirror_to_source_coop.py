#!/usr/bin/env python3
"""Mirror the Icechunk store to Source Cooperative.

Reads the repo file first (that pins the snapshot), copies the immutable
files the destination lacks, then writes the repo file last, so a reader
never sees a tip that names files still in flight. A crashed run leaves
the old tip in place and the next run catches up. Nothing is deleted.

Source reads use your AWS credentials. Destination writes use the keys
Source Coop issued: SOURCE_COOP_ACCESS_KEY_ID, SOURCE_COOP_SECRET_ACCESS_KEY
and optionally SOURCE_COOP_SESSION_TOKEN. The store location comes from
ICECHUNK_BUCKET and S3_PREFIX.

Usage (add --dry-run to only report):
    uv run --env-file .env_no2 --env-file .env.local scripts/mirror_to_source_coop.py
"""

from __future__ import annotations

import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from virtualizarr_processor.manifest import storage_prefix

IMMUTABLE_PREFIXES = ("snapshots", "manifests", "transactions", "chunks")
REPO_INFO_KEY = "repo"  # icechunk-format's REPO_INFO_FILE_PATH
CONFIG_KEY = "config.yaml"

# Source Coop's direct S3 bucket. data.source.coop is the other endpoint;
# set DEST_ENDPOINT to use it.
DEST_ENDPOINT: str | None = None
DEST_BUCKET = "us-west-2.opendata.source.coop"
DEST_ROOT = "pangeo/tempo-virtual-icechunk"
REGION = "us-west-2"  # the store and Source Coop both live source_keys
WORKERS = 16


def source_client() -> Any:
    """The source store, read with your own AWS credentials."""
    return boto3.client("s3", region_name=REGION)


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
            "own AWS credentials grant nothing destination_keys"
        )
    return boto3.client(
        "s3",
        endpoint_url=DEST_ENDPOINT,
        region_name=REGION,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        aws_session_token=os.environ.get("SOURCE_COOP_SESSION_TOKEN"),
        config=Config(s3={"addressing_style": "path"}),
    )


def relative_keys(client: Any, bucket: str, prefix: str) -> set[str]:
    """Keys under ``prefix`` (ending in ``/``), relative to it."""
    keys: set[str] = set()
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix
    ):
        keys |= {obj["Key"][len(prefix) :] for obj in page.get("Contents", [])}
    return keys


def mirror(
    source: Any,
    destination: Any,
    *,
    source_bucket: str,
    source_prefix: str,
    destination_bucket: str,
    destination_prefix: str,
    workers: int = 16,
    dry_run: bool = False,
    log: Any = sys.stderr,
) -> int:
    """Copy one snapshot; return how many objects were written.

    The prefixes are repository roots ending in ``/``.
    """
    pinned = source.get_object(Bucket=source_bucket, Key=source_prefix + REPO_INFO_KEY)[
        "Body"
    ].read()
    print(f"pinned {source_prefix}{REPO_INFO_KEY} ({len(pinned)} bytes)", file=log)

    # Nothing is deleted; files the source GC expires linger as orphans.
    todo: list[str] = []
    for area in IMMUTABLE_PREFIXES:
        source_keys = relative_keys(source, source_bucket, f"{source_prefix}{area}/")
        try:
            destination_keys = relative_keys(
                destination, destination_bucket, f"{destination_prefix}{area}/"
            )
        except ClientError as error:
            # Say which side failed; the destination keys cover one prefix.
            raise SystemExit(
                f"listing {destination_bucket}/{destination_prefix}{area}/ failed: "
                f"{error}. Check the SOURCE_COOP_* credentials cover that prefix."
            ) from error
        missing = sorted(source_keys - destination_keys)
        print(
            f"{area}: {len(source_keys)} source, {len(destination_keys)} destination, "
            f"{len(missing)} to copy",
            file=log,
        )
        todo += [f"{area}/{key}" for key in missing]

    if dry_run:
        print(f"dry run: would copy {len(todo)} objects", file=log)
        return 0

    def copy(key: str) -> None:
        body = source.get_object(Bucket=source_bucket, Key=source_prefix + key)["Body"]
        destination.upload_fileobj(body, destination_bucket, destination_prefix + key)

    if todo:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            # list() so a failed copy raises before the repo file is written.
            list(pool.map(copy, todo))

    # Small and mutable, so always refresh it. Optional; readers default
    # without it.
    try:
        config = source.get_object(
            Bucket=source_bucket, Key=source_prefix + CONFIG_KEY
        )["Body"].read()
    except source.exceptions.NoSuchKey:
        config = None
    if config is not None:
        destination.put_object(
            Bucket=destination_bucket, Key=destination_prefix + CONFIG_KEY, Body=config
        )

    destination.put_object(
        Bucket=destination_bucket, Key=destination_prefix + REPO_INFO_KEY, Body=pinned
    )
    written = destination.get_object(
        Bucket=destination_bucket, Key=destination_prefix + REPO_INFO_KEY
    )["Body"].read()
    if written != pinned:
        raise RuntimeError(
            f"published {destination_prefix}{REPO_INFO_KEY} reads back as "
            f"{len(written)} bytes, expected {len(pinned)}"
        )
    print(f"published {len(todo)} new objects + repo tip", file=log)
    return len(todo)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="report what would be copied and exit without writing",
    )
    args = parser.parse_args()

    source_bucket = os.environ.get("ICECHUNK_BUCKET")
    source_prefix = storage_prefix()
    if not source_bucket or not source_prefix:
        print(
            "ICECHUNK_BUCKET and S3_PREFIX/ICECHUNK_PREFIX must be set "
            "(local-filesystem stores have nothing to publish)",
            file=sys.stderr,
        )
        return 2

    # Nest the per-collection source prefix under DEST_ROOT.
    destination_prefix = f"{DEST_ROOT}/{source_prefix}".strip("/")

    print(
        f"s3://{source_bucket}/{source_prefix}/ -> s3://{DEST_BUCKET}/{destination_prefix}/",
        file=sys.stderr,
    )
    mirror(
        source_client(),
        destination_client(),
        source_bucket=source_bucket,
        source_prefix=f"{source_prefix.strip('/')}/",
        destination_bucket=DEST_BUCKET,
        destination_prefix=f"{destination_prefix}/",
        workers=WORKERS,
        dry_run=args.dry_run,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
