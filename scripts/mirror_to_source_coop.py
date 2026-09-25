#!/usr/bin/env python3
"""Mirror the Icechunk store to Source Cooperative.

The repo file is the store's only mutable object, so the copy order is
what makes this safe: read it first (pinning the snapshot to publish),
copy whatever immutable files the destination is missing, then write the
repo file last. A crashed run leaves the destination on its old repo
file and the next run catches up. Nothing is ever deleted.

Source reads use your own AWS credentials (``aws sso login``).
Destination writes use the keys Source Coop issued, from
SOURCE_COOP_ACCESS_KEY_ID / SOURCE_COOP_SECRET_ACCESS_KEY (and
SOURCE_COOP_SESSION_TOKEN if you have one). The store location comes
from the processor env vars ICECHUNK_BUCKET and S3_PREFIX.

Usage:
    uv run --env-file .env_no2 scripts/mirror_to_source_coop.py --dry-run
    uv run --env-file .env_no2 scripts/mirror_to_source_coop.py
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

# Source Coop's direct-S3 address: a real bucket in us-west-2, with the
# account and repository as the leading key segments. The data.source.coop
# endpoint is the other way in; set DEST_ENDPOINT to switch to it.
DEST_ENDPOINT: str | None = None
DEST_BUCKET = "us-west-2.opendata.source.coop"
DEST_ROOT = "pangeo/tempo-virtual-icechunk"
REGION = "us-west-2"  # both the Icechunk store and Source Coop live here
WORKERS = 16


def source_client() -> Any:
    """Reads the source store with your own AWS credentials (aws sso login)."""
    return boto3.client("s3", region_name=REGION)


def destination_client() -> Any:
    """Writes to Source Coop with the keys it issued (SOURCE_COOP_*).

    The keys are required up front: left to boto3's fallback, requests
    would be signed with your AWS identity and fail as a bare AccessDenied.
    Path-style addressing because the bucket name contains dots.
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
        endpoint_url=DEST_ENDPOINT,
        region_name=REGION,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        aws_session_token=os.environ.get("SOURCE_COOP_SESSION_TOKEN"),
        config=Config(s3={"addressing_style": "path"}),
    )


def relative_keys(client: Any, bucket: str, prefix: str) -> set[str]:
    """Every key under ``prefix`` (which must end in ``/``), relative to it."""
    keys: set[str] = set()
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix
    ):
        keys |= {obj["Key"][len(prefix) :] for obj in page.get("Contents", [])}
    return keys


def mirror(
    src: Any,
    dst: Any,
    *,
    src_bucket: str,
    src_prefix: str,
    dst_bucket: str,
    dst_prefix: str,
    workers: int = 16,
    dry_run: bool = False,
    log: Any = sys.stderr,
) -> int:
    """Copy one snapshot and return how many objects were written.

    ``src_prefix`` and ``dst_prefix`` are repository roots ending in ``/``.
    """
    pinned = src.get_object(Bucket=src_bucket, Key=src_prefix + REPO_INFO_KEY)[
        "Body"
    ].read()
    print(f"pinned {src_prefix}{REPO_INFO_KEY} ({len(pinned)} bytes)", file=log)

    # ponytail: no deletes, so files the source GC expires linger here as
    # orphans; add a delete pass after the repo PUT when the count matters.
    todo: list[str] = []
    for area in IMMUTABLE_PREFIXES:
        here = relative_keys(src, src_bucket, f"{src_prefix}{area}/")
        try:
            there = relative_keys(dst, dst_bucket, f"{dst_prefix}{area}/")
        except ClientError as error:
            # Which side failed is not obvious from the traceback, and the
            # destination credentials are scoped to one repository prefix.
            raise SystemExit(
                f"listing {dst_bucket}/{dst_prefix}{area}/ failed: {error}. "
                "Check the SOURCE_COOP_* credentials cover that prefix."
            ) from error
        missing = sorted(here - there)
        print(
            f"{area}: {len(here)} source, {len(there)} destination, "
            f"{len(missing)} to copy",
            file=log,
        )
        todo += [f"{area}/{key}" for key in missing]

    if dry_run:
        print(f"dry run: would copy {len(todo)} objects", file=log)
        return 0

    def copy(key: str) -> None:
        body = src.get_object(Bucket=src_bucket, Key=src_prefix + key)["Body"]
        dst.upload_fileobj(body, dst_bucket, dst_prefix + key)

    if todo:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            # list() so a failed copy raises before the repo file is written.
            list(pool.map(copy, todo))

    # Mutable and tiny, so refresh it rather than diffing. A store without
    # one is fine; readers fall back to the default repository config.
    try:
        config = src.get_object(Bucket=src_bucket, Key=src_prefix + CONFIG_KEY)[
            "Body"
        ].read()
    except src.exceptions.NoSuchKey:
        config = None
    if config is not None:
        dst.put_object(Bucket=dst_bucket, Key=dst_prefix + CONFIG_KEY, Body=config)

    dst.put_object(Bucket=dst_bucket, Key=dst_prefix + REPO_INFO_KEY, Body=pinned)
    written = dst.get_object(Bucket=dst_bucket, Key=dst_prefix + REPO_INFO_KEY)[
        "Body"
    ].read()
    if written != pinned:
        raise RuntimeError(
            f"published {dst_prefix}{REPO_INFO_KEY} reads back as "
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

    src_bucket = os.environ.get("ICECHUNK_BUCKET")
    src_prefix = storage_prefix()
    if not src_bucket or not src_prefix:
        print(
            "ICECHUNK_BUCKET and S3_PREFIX/ICECHUNK_PREFIX must be set "
            "(local-filesystem stores have nothing to publish)",
            file=sys.stderr,
        )
        return 2

    # The source prefix is unique per collection, so nesting it under
    # DEST_ROOT keeps collections apart without extra configuration.
    dst_prefix = f"{DEST_ROOT}/{src_prefix}".strip("/")

    print(
        f"s3://{src_bucket}/{src_prefix}/ -> s3://{DEST_BUCKET}/{dst_prefix}/",
        file=sys.stderr,
    )
    mirror(
        source_client(),
        destination_client(),
        src_bucket=src_bucket,
        src_prefix=f"{src_prefix.strip('/')}/",
        dst_bucket=DEST_BUCKET,
        dst_prefix=f"{dst_prefix}/",
        workers=WORKERS,
        dry_run=args.dry_run,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
