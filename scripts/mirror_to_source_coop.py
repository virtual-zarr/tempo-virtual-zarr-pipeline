#!/usr/bin/env python3
"""Publish the tip of the Icechunk store: a Source Cooperative mirror and a zip.

Downloads the store, prunes a local copy to the tip of ``main`` with
Icechunk's own expire and garbage-collect (so the source keeps its rollback
window and the pipeline keeps committing), zips the pruned copy, then
uploads it to Source Coop with ``repo`` last, so a reader never sees a tip
that names files still in flight. Nothing is deleted from either bucket;
objects earlier runs published linger as orphans.

Source reads use your AWS credentials. Destination writes use the keys
Source Coop issued: SOURCE_COOP_ACCESS_KEY_ID, SOURCE_COOP_SECRET_ACCESS_KEY
and optionally SOURCE_COOP_SESSION_TOKEN. The store location comes from
ICECHUNK_BUCKET and S3_PREFIX.

Usage (--dry-run only reports; --no-upload stops after the zip):
    uv run --env-file .env_no2 --env-file .env.local scripts/mirror_to_source_coop.py
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
import icechunk
from botocore.config import Config
from botocore.exceptions import ClientError
from virtualizarr_processor.manifest import storage_prefix

REPO_INFO_KEY = "repo"  # icechunk-format's REPO_INFO_FILE_PATH
BACKUPS = "overwritten/"  # repo-file backups: inspection only, stale once pruned

# Source Coop's direct S3 bucket. data.source.coop is the other endpoint;
# set DEST_ENDPOINT to use it.
DEST_ENDPOINT: str | None = None
DEST_BUCKET = "us-west-2.opendata.source.coop"
DEST_ROOT = "pangeo/tempo-virtual-icechunk"
REGION = "us-west-2"  # the store and Source Coop both live here
WORKERS = 16
# Ignore any AWS_ENDPOINT_URL in the environment, which would redirect both
# sides. botocore honors this from 1.29 on; the stubs do not list it yet.
NO_ENDPOINT_OVERRIDE = {"ignore_configured_endpoint_urls": True}


def source_client() -> Any:
    """The source store, read with your own AWS credentials."""
    return boto3.client(
        "s3",
        region_name=REGION,
        config=Config(**NO_ENDPOINT_OVERRIDE),
    )


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
        endpoint_url=DEST_ENDPOINT,
        region_name=REGION,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        aws_session_token=os.environ.get("SOURCE_COOP_SESSION_TOKEN"),
        config=Config(s3={"addressing_style": "path"}, **NO_ENDPOINT_OVERRIDE),
    )


def object_sizes(client: Any, bucket: str, prefix: str) -> dict[str, int]:
    """Objects under ``prefix`` (ending in ``/``): relative key -> size."""
    sizes: dict[str, int] = {}
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix
    ):
        sizes |= {
            obj["Key"][len(prefix) :]: obj["Size"] for obj in page.get("Contents", [])
        }
    return sizes


def file_sizes(directory: Path) -> dict[str, int]:
    """Files under ``directory``: relative posix path -> size."""
    return {
        path.relative_to(directory).as_posix(): path.stat().st_size
        for path in directory.rglob("*")
        if path.is_file()
    }


def download(
    source: Any, bucket: str, prefix: str, directory: Path, *, workers: int, log: Any
) -> int:
    """Fetch what ``directory`` lacks; return how many objects were fetched.

    ``repo`` comes first: it pins a snapshot whose files already exist, so a
    commit landing mid-download can't leave the copy naming files it never
    fetched. Sizes stand in for checksums; boto3 renames completed downloads
    into place, so a partial file never matches.
    """
    directory.mkdir(parents=True, exist_ok=True)
    source.download_file(bucket, prefix + REPO_INFO_KEY, str(directory / REPO_INFO_KEY))
    have = file_sizes(directory)
    todo = [
        key
        for key, size in object_sizes(source, bucket, prefix).items()
        if key != REPO_INFO_KEY
        and not key.startswith(BACKUPS)
        and have.get(key) != size
    ]

    def fetch(key: str) -> None:
        path = directory / key
        path.parent.mkdir(parents=True, exist_ok=True)
        source.download_file(bucket, prefix + key, str(path))

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(fetch, todo))
    print(f"downloaded {len(todo)} objects to {directory}", file=log)
    return len(todo)


def prune(directory: Path, *, log: Any) -> str:
    """Cut the local copy down to the tip of ``main``; return the tip's id."""
    repo = icechunk.Repository.open(icechunk.local_filesystem_storage(str(directory)))
    now = datetime.now(timezone.utc)
    repo.expire_snapshots(
        older_than=now, delete_expired_branches=True, delete_expired_tags=True
    )
    summary = repo.garbage_collect(delete_object_older_than=now)
    shutil.rmtree(directory / BACKUPS, ignore_errors=True)
    tip = repo.lookup_branch("main")
    print(f"pruned to main @ {tip}: {summary}", file=log)
    return tip


def upload(
    destination: Any,
    directory: Path,
    bucket: str,
    prefix: str,
    *,
    workers: int,
    log: Any,
) -> int:
    """Send what the destination lacks, ``repo`` last; return how many were sent."""
    try:
        have = object_sizes(destination, bucket, prefix)
    except ClientError as error:
        raise SystemExit(
            f"listing {bucket}/{prefix} failed: {error}. "
            "Check the SOURCE_COOP_* credentials cover that prefix."
        ) from error
    todo = sorted(
        key
        for key, size in file_sizes(directory).items()
        if key != REPO_INFO_KEY and have.get(key) != size
    )

    def put(key: str) -> None:
        destination.upload_file(str(directory / key), bucket, prefix + key)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        # list() so a failed upload raises before the repo file is written.
        list(pool.map(put, todo))
    put(REPO_INFO_KEY)
    print(
        f"published {len(todo)} objects + repo tip to s3://{bucket}/{prefix}", file=log
    )
    return len(todo) + 1


def publish(
    source: Any,
    destination: Any,
    *,
    source_bucket: str,
    source_prefix: str,
    destination_bucket: str,
    destination_prefix: str,
    directory: Path,
    workers: int = WORKERS,
    dry_run: bool = False,
    upload_copy: bool = True,
    log: Any = sys.stderr,
) -> Path | None:
    """Download, prune, zip, upload; return the zip's path (None on a dry run).

    The prefixes are repository roots ending in ``/``. ``directory`` keeps
    the full download so reruns fetch only what's new; the pruned copy lives
    beside it with a ``-tip`` suffix and is rebuilt every run.
    """
    if dry_run:
        sizes = object_sizes(source, source_bucket, source_prefix)
        print(
            f"dry run: {len(sizes)} objects, {sum(sizes.values()) / 1e9:.2f} GB at "
            f"s3://{source_bucket}/{source_prefix}; would download to {directory}",
            file=log,
        )
        return None

    download(source, source_bucket, source_prefix, directory, workers=workers, log=log)
    tip_dir = directory.with_name(directory.name + "-tip")
    shutil.rmtree(tip_dir, ignore_errors=True)
    shutil.copytree(directory, tip_dir)
    prune(tip_dir, log=log)
    archive = Path(shutil.make_archive(str(tip_dir), "zip", root_dir=tip_dir))
    print(f"zipped {archive} ({archive.stat().st_size / 1e6:.1f} MB)", file=log)
    if upload_copy:
        upload(
            destination,
            tip_dir,
            destination_bucket,
            destination_prefix,
            workers=workers,
            log=log,
        )
    return archive


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="report the source's size and exit without downloading",
    )
    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="stop after writing the zip; needs no Source Coop credentials",
    )
    parser.add_argument(
        "--dir",
        type=Path,
        help="where the full download lives (default: stores/<S3_PREFIX>)",
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
    source_prefix = source_prefix.strip("/")

    # Nest the per-collection source prefix under DEST_ROOT.
    destination_prefix = f"{DEST_ROOT}/{source_prefix}"
    upload_copy = not (args.dry_run or args.no_upload)

    print(
        f"s3://{source_bucket}/{source_prefix}/ -> s3://{DEST_BUCKET}/{destination_prefix}/",
        file=sys.stderr,
    )
    publish(
        source_client(),
        destination_client() if upload_copy else None,
        source_bucket=source_bucket,
        source_prefix=f"{source_prefix}/",
        destination_bucket=DEST_BUCKET,
        destination_prefix=f"{destination_prefix}/",
        directory=args.dir or Path("stores") / source_prefix,
        dry_run=args.dry_run,
        upload_copy=upload_copy,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
