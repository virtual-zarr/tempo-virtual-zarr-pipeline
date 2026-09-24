#!/usr/bin/env python3
"""Copy a snapshot of the Icechunk store to Source Cooperative.

Order matters:

1. GET ``<prefix>/repo``, the repo-info file. It is the store's only
   mutable object and holds every branch and tag pointer, so reading it
   fixes which snapshot this run publishes.
2. Copy the keys the destination lacks under ``snapshots/``,
   ``manifests/``, ``transactions/`` and ``chunks/``. Those are immutable
   and uniquely named, so a missing key is the only difference possible
   and re-running after a crash is safe.
3. PUT the repo file read in step 1.

Step 2 runs after step 1, so everything the published repo file references
is in place before step 3 writes it. If a run dies partway the destination
keeps its previous repo file and the next run catches up. Source commits
made during a run are picked up by the next one.

Source Coop is a separate account behind its own endpoint, so objects are
streamed through this process instead of copied server-side. That is
affordable here because the store holds metadata, coordinates and
byte-range references (a few GB), not granule data. Readers still need
Earthdata Login to fetch the chunk bytes.

Nothing is ever deleted from the destination.

The source store comes from the processor's environment variables
($ICECHUNK_BUCKET, $S3_PREFIX/$ICECHUNK_PREFIX, $ICECHUNK_REGION).
Destination credentials come from $SOURCE_COOP_ACCESS_KEY_ID and
$SOURCE_COOP_SECRET_ACCESS_KEY, or the usual boto3 chain.

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
from virtualizarr_processor.manifest import storage_prefix

IMMUTABLE_PREFIXES = ("snapshots", "manifests", "transactions", "chunks")
REPO_INFO_KEY = "repo"  # icechunk-format's REPO_INFO_FILE_PATH
CONFIG_KEY = "config.yaml"

DEST_ENDPOINT = "https://data.source.coop"
DEST_REGION = "us-east-1"
DEST_BUCKET = "pangeo"
DEST_ROOT = "tempo-virtual-icechunk"


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
        there = relative_keys(dst, dst_bucket, f"{dst_prefix}{area}/")
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
        "--dest-prefix",
        help=f"destination repository root (default: {DEST_ROOT}/<source prefix>)",
    )
    parser.add_argument("--workers", type=int, default=16)
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
    dst_prefix = (args.dest_prefix or f"{DEST_ROOT}/{src_prefix}").strip("/")

    src = boto3.client("s3", region_name=os.environ.get("ICECHUNK_REGION"))
    dst = boto3.client(
        "s3",
        endpoint_url=DEST_ENDPOINT,
        region_name=DEST_REGION,
        aws_access_key_id=os.environ.get("SOURCE_COOP_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("SOURCE_COOP_SECRET_ACCESS_KEY"),
    )
    print(
        f"s3://{src_bucket}/{src_prefix}/ -> "
        f"{DEST_ENDPOINT}/{DEST_BUCKET}/{dst_prefix}/",
        file=sys.stderr,
    )
    mirror(
        src,
        dst,
        src_bucket=src_bucket,
        src_prefix=f"{src_prefix.strip('/')}/",
        dst_bucket=DEST_BUCKET,
        dst_prefix=f"{dst_prefix}/",
        workers=args.workers,
        dry_run=args.dry_run,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
