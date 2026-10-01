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

Progress bars go to stderr. Off a terminal (CI, redirected output) they
redraw every 30 seconds instead of continuously.

Usage (--dry-run only reports; --no-upload stops after the zip; --limit N
fetches N objects as a trial and stops, pairing well with a scratch --dir):
    uv run --env-file .env_no2 --env-file .env.local scripts/mirror_to_source_coop.py
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import threading
import time
import zipfile
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
import icechunk
from botocore.config import Config
from botocore.exceptions import ClientError
from tqdm import tqdm
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

# Seconds between progress redraws, on and off a terminal.
TTY_INTERVAL = 0.1
LOG_INTERVAL = 30.0


# --------------------------------------------------------------------------
# Progress reporting
# --------------------------------------------------------------------------


def is_tty(log: Any) -> bool:
    return bool(getattr(log, "isatty", lambda: False)())


def human_bytes(n: float) -> str:
    return tqdm.format_sizeof(n, suffix="B", divisor=1000)


class TransferBar(tqdm):
    """A byte-counting tqdm bar with a files-done count in its postfix.

    ``add_bytes`` has the signature boto3 expects of a transfer ``Callback``
    and is called from s3transfer's threads, so updates take a lock (tqdm's
    own counter isn't guarded). Negative amounts, which s3transfer sends to
    rewind a retried chunk, are fine.

    The lock must not be named ``_lock``: tqdm keeps its display lock under
    that name and takes it inside ``update``, so shadowing it with a plain
    Lock deadlocks the first redraw.
    """

    def __init__(
        self, desc: str, *, total_bytes: int, total_files: int, log: Any
    ) -> None:
        super().__init__(
            desc=desc,
            total=total_bytes,
            unit="B",
            unit_scale=True,
            unit_divisor=1000,
            file=log,
            mininterval=TTY_INTERVAL if is_tty(log) else LOG_INTERVAL,
            dynamic_ncols=True,
            disable=total_files == 0,
        )
        self._counter_lock = threading.Lock()
        self._files = 0
        self._total_files = total_files
        self._show_files(refresh=False)

    def _show_files(self, *, refresh: bool) -> None:
        if not self.disable:
            self.set_postfix_str(
                f"{self._files:,}/{self._total_files:,} files", refresh=refresh
            )

    def add_bytes(self, amount: int) -> None:
        with self._counter_lock:
            self.update(amount)

    def file_done(self) -> None:
        with self._counter_lock:
            self._files += 1
            self._show_files(refresh=False)


@contextmanager
def stage(number: int, total: int, name: str, log: Any) -> Iterator[None]:
    """Announce a step and report how long it took."""
    print(f"[{number}/{total}] {name}", file=log, flush=True)
    start = time.monotonic()
    yield
    elapsed = tqdm.format_interval(time.monotonic() - start)
    print(f"[{number}/{total}] {name}: done in {elapsed}", file=log, flush=True)


# --------------------------------------------------------------------------
# S3 and filesystem helpers
# --------------------------------------------------------------------------


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


def object_sizes(
    client: Any, bucket: str, prefix: str, *, log: Any = None
) -> dict[str, int]:
    """Objects under ``prefix`` (ending in ``/``): relative key -> size.

    With ``log``, shows a running count, since listing a large store is
    itself slow (1,000 keys per request).
    """
    sizes: dict[str, int] = {}
    with tqdm(
        desc=f"listing s3://{bucket}/{prefix}",
        unit=" obj",
        unit_scale=True,
        file=log,
        mininterval=TTY_INTERVAL if is_tty(log) else LOG_INTERVAL,
        disable=log is None,
    ) as bar:
        for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=prefix
        ):
            contents = page.get("Contents", [])
            sizes |= {obj["Key"][len(prefix) :]: obj["Size"] for obj in contents}
            bar.update(len(contents))
    return sizes


def file_sizes(directory: Path) -> dict[str, int]:
    """Files under ``directory``: relative posix path -> size."""
    return {
        path.relative_to(directory).as_posix(): path.stat().st_size
        for path in directory.rglob("*")
        if path.is_file()
    }


# --------------------------------------------------------------------------
# Pipeline steps
# --------------------------------------------------------------------------


def download(
    source: Any,
    bucket: str,
    prefix: str,
    directory: Path,
    *,
    workers: int,
    log: Any,
    limit: int | None = None,
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
    remote = object_sizes(source, bucket, prefix, log=log)
    todo = {
        key: size
        for key, size in remote.items()
        if key != REPO_INFO_KEY
        and not key.startswith(BACKUPS)
        and have.get(key) != size
    }
    if limit is not None:
        print(f"trial: fetching {limit:,} of {len(todo):,} missing objects", file=log)
        todo = dict(list(todo.items())[:limit])
    print(
        f"{len(todo):,} objects to fetch ({human_bytes(sum(todo.values()))}); "
        f"{len(remote) - len(todo):,} already present or skipped",
        file=log,
        flush=True,
    )

    with TransferBar(
        "download",
        total_bytes=sum(todo.values()),
        total_files=len(todo),
        log=log,
    ) as bar:

        def fetch(key: str) -> None:
            path = directory / key
            path.parent.mkdir(parents=True, exist_ok=True)
            source.download_file(
                bucket, prefix + key, str(path), Callback=bar.add_bytes
            )
            bar.file_done()

        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(fetch, todo))
    print(f"downloaded {len(todo)} objects to {directory}", file=log)
    return len(todo)


def copy_tree(source: Path, destination: Path, *, log: Any) -> None:
    """``shutil.copytree`` with a progress bar; ``destination`` must not exist."""
    sizes = file_sizes(source)
    with TransferBar(
        "copy", total_bytes=sum(sizes.values()), total_files=len(sizes), log=log
    ) as bar:

        def copy(src: str, dst: str) -> Any:
            result = shutil.copy2(src, dst)
            bar.add_bytes(os.path.getsize(dst))
            bar.file_done()
            return result

        shutil.copytree(source, destination, copy_function=copy)


def prune(directory: Path, *, log: Any) -> str:
    """Cut the local copy down to the tip of ``main``; return the tip's id.

    Icechunk exposes no progress hooks here, so this step reports only
    its duration (via the enclosing stage).
    """
    repo = icechunk.Repository.open(icechunk.local_filesystem_storage(str(directory)))
    now = datetime.now(timezone.utc)
    print("expiring snapshots...", file=log, flush=True)
    repo.expire_snapshots(
        older_than=now, delete_expired_branches=True, delete_expired_tags=True
    )
    print("garbage-collecting...", file=log, flush=True)
    summary = repo.garbage_collect(delete_object_older_than=now)
    shutil.rmtree(directory / BACKUPS, ignore_errors=True)
    tip = repo.lookup_branch("main")
    print(f"pruned to main @ {tip}: {summary}", file=log)
    return tip


def zip_directory(directory: Path, *, log: Any) -> Path:
    """Zip ``directory`` to a sibling ``<name>.zip``; return its path.

    Equivalent to ``shutil.make_archive(str(directory), "zip",
    root_dir=directory)`` except that it reports progress and writes only
    file entries (no separate directory entries).
    """
    archive = directory.with_name(directory.name + ".zip")
    sizes = file_sizes(directory)
    with TransferBar(
        "zip", total_bytes=sum(sizes.values()), total_files=len(sizes), log=log
    ) as bar:
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for key in sorted(sizes):
                zf.write(directory / key, key)
                bar.add_bytes(sizes[key])
                bar.file_done()
    return archive


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
        have = object_sizes(destination, bucket, prefix, log=log)
    except ClientError as error:
        raise SystemExit(
            f"listing {bucket}/{prefix} failed: {error}. "
            "Check the SOURCE_COOP_* credentials cover that prefix."
        ) from error
    local = file_sizes(directory)
    todo = sorted(
        key
        for key, size in local.items()
        if key != REPO_INFO_KEY and have.get(key) != size
    )
    total_bytes = sum(local[key] for key in todo) + local[REPO_INFO_KEY]
    print(
        f"{len(todo):,} objects + repo tip to send ({human_bytes(total_bytes)})",
        file=log,
        flush=True,
    )

    with TransferBar(
        "upload", total_bytes=total_bytes, total_files=len(todo) + 1, log=log
    ) as bar:

        def put(key: str) -> None:
            destination.upload_file(
                str(directory / key), bucket, prefix + key, Callback=bar.add_bytes
            )
            bar.file_done()

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
    limit: int | None = None,
    log: Any = sys.stderr,
) -> Path | None:
    """Download, prune, zip, upload; return the zip, or None on a dry or trial run.

    The prefixes are repository roots ending in ``/``. ``directory`` keeps
    the full download so reruns fetch only what's new; the pruned copy lives
    beside it with a ``-tip`` suffix and is rebuilt every run.

    ``limit`` fetches at most that many objects and stops after the
    download: a partial store can't be pruned or published. The objects it
    fetches are complete, so a later full run into the same ``directory``
    skips them.
    """
    if dry_run:
        sizes = object_sizes(source, source_bucket, source_prefix)
        print(
            f"dry run: {len(sizes)} objects, {sum(sizes.values()) / 1e9:.2f} GB at "
            f"s3://{source_bucket}/{source_prefix}; would download to {directory}",
            file=log,
        )
        return None

    started = time.monotonic()
    total = 1 if limit is not None else 5 if upload_copy else 4

    with stage(1, total, "download", log):
        download(
            source,
            source_bucket,
            source_prefix,
            directory,
            workers=workers,
            log=log,
            limit=limit,
        )
    if limit is not None:
        elapsed = tqdm.format_interval(time.monotonic() - started)
        print(f"trial done in {elapsed}; stopping before prune", file=log)
        return None

    tip_dir = directory.with_name(directory.name + "-tip")
    with stage(2, total, f"copy to {tip_dir}", log):
        shutil.rmtree(tip_dir, ignore_errors=True)
        copy_tree(directory, tip_dir, log=log)

    with stage(3, total, "prune to tip of main", log):
        prune(tip_dir, log=log)

    with stage(4, total, "zip", log):
        archive = zip_directory(tip_dir, log=log)
        print(f"zipped {archive} ({archive.stat().st_size / 1e6:.1f} MB)", file=log)

    if upload_copy:
        with stage(5, total, "upload to Source Coop", log):
            upload(
                destination,
                tip_dir,
                destination_bucket,
                destination_prefix,
                workers=workers,
                log=log,
            )

    elapsed = tqdm.format_interval(time.monotonic() - started)
    print(f"all done in {elapsed}", file=log)
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
        "--limit",
        type=int,
        metavar="N",
        help="trial run: download at most N missing objects, then stop",
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
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be at least 1")
    upload_copy = not (args.dry_run or args.no_upload or args.limit is not None)

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
        limit=args.limit,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
