#!/usr/bin/env python3
"""Publish the tip of the Icechunk store to Source Cooperative, and zip it locally.

Four stages, listed one per line at the bottom of ``main``; comment out
the ones you don't need, since each reads what the one before left in the
directory. ``download`` fetches what the tip of ``main`` needs into an
empty directory (its snapshot, manifests, every transaction log and every
native chunk; the historical snapshots and manifests are most of the
source's size). ``prune`` cuts that directory down to the tip in place
with Icechunk's own expire and garbage-collect (the source keeps its
rollback window and the pipeline keeps committing). ``zip_store`` zips it;
the zip stays local and is not published anywhere. ``upload`` sends it to
Source Coop with ``repo`` last, so a reader never sees a tip that names
files still in flight. Only Icechunk's own files are zipped or uploaded.
Nothing is deleted from either bucket; objects earlier runs published
linger as orphans.

The script never deletes anything local: ``download`` refuses a non-empty
directory and ``zip_store`` an existing zip. After a crash, comment out the
stages that finished and rerun.

Source reads use your AWS credentials. Destination writes use the keys
Source Coop issued: SOURCE_COOP_ACCESS_KEY_ID, SOURCE_COOP_SECRET_ACCESS_KEY
and optionally SOURCE_COOP_SESSION_TOKEN, checked when ``upload`` starts.
The store location comes from ICECHUNK_BUCKET and S3_PREFIX/ICECHUNK_PREFIX.

Progress bars go to stderr. Off a terminal (CI, redirected output) they
redraw every 30 seconds instead of continuously.

Usage (--dry-run only reports what the tip needs; --limit N downloads at
most N objects as a trial, with the later stages commented out):
    uv run --env-file .env_no2 --env-file .env.local scripts/mirror_to_source_coop.py
"""

from __future__ import annotations

import argparse
import functools
import os
import sys
import tempfile
import threading
import time
import zipfile
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, ParamSpec, TypeVar

import boto3
import icechunk
from boto3.s3.transfer import S3Transfer, TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError
from tqdm import tqdm
from virtualizarr_processor.manifest import storage_prefix

# Icechunk's on-disk layout (icechunk-format's *_FILE_PATH constants). The
# directories are flat. ``overwritten/`` holds repo-file backups and is not
# part of the store.
REPO_INFO_KEY = "repo"
STORE_FILES = (REPO_INFO_KEY, "config.yaml")
STORE_DIRS = ("chunks", "manifests", "snapshots", "transactions")

# Source Coop's direct S3 bucket. data.source.coop is the other endpoint;
# set DEST_ENDPOINT to use it.
DEST_ENDPOINT: str | None = None
DEST_BUCKET = "us-west-2.opendata.source.coop"
DEST_ROOT = "pangeo/tempo-virtual-icechunk"
REGION = "us-west-2"  # the store and Source Coop both live here
WORKERS = 16
# Ignore any AWS_ENDPOINT_URL in the environment, which would redirect both
# sides. botocore honors this from 1.29 on; the stubs do not list it yet.
NO_ENDPOINT_OVERRIDE = Config(ignore_configured_endpoint_urls=True)  # type: ignore[call-arg]

# "Everything but the tips and the root" for expire and garbage-collect.
# Wall-clock ``now`` would leave a snapshot unexpired when the committing
# Lambda's clock ran ahead of ours, and the prune would then need snapshot
# files that were never downloaded.
FAR_FUTURE = datetime(9999, 1, 1, tzinfo=timezone.utc)

# Seconds between progress redraws, on and off a terminal.
TTY_INTERVAL = 0.1
LOG_INTERVAL = 30.0

P = ParamSpec("P")
R = TypeVar("R")


# --------------------------------------------------------------------------
# Progress reporting
# --------------------------------------------------------------------------


def is_tty(log: Any) -> bool:
    return bool(getattr(log, "isatty", lambda: False)())


def human_bytes(n: float) -> str:
    return str(tqdm.format_sizeof(n, suffix="B", divisor=1000))


class TransferBar(tqdm):  # type: ignore[type-arg]
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


def stage(step: Callable[P, R]) -> Callable[P, R]:
    """Announce a stage by its function name and report how long it took."""

    @functools.wraps(step)
    def run(*args: P.args, **kwargs: P.kwargs) -> R:
        log: Any = kwargs.get("log", sys.stderr)
        print(f"{step.__name__}...", file=log, flush=True)
        start = time.monotonic()
        result = step(*args, **kwargs)
        elapsed = tqdm.format_interval(time.monotonic() - start)
        print(f"{step.__name__}: done in {elapsed}", file=log, flush=True)
        return result

    return run


# --------------------------------------------------------------------------
# S3 and filesystem helpers
# --------------------------------------------------------------------------


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
        endpoint_url=DEST_ENDPOINT,
        region_name=REGION,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        aws_session_token=os.environ.get("SOURCE_COOP_SESSION_TOKEN"),
        config=NO_ENDPOINT_OVERRIDE.merge(Config(s3={"addressing_style": "path"})),
    )


def transfer(client: Any, workers: int) -> S3Transfer:
    """One transfer manager per client; ``download_file``/``upload_file`` on
    the client itself build and tear one down per object."""
    config = TransferConfig(max_concurrency=workers)
    return S3Transfer(client, config)  # type: ignore[arg-type]  # stub says botocore Config


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


def store_files(directory: Path) -> dict[str, int]:
    """Icechunk's own files under ``directory``: relative key -> size.

    Anything else (``.DS_Store``, ``overwritten/`` backups, s3transfer
    temporaries, stray files) is left out, so it is never zipped or
    uploaded.
    """
    paths = [directory / name for name in STORE_FILES]
    for name in STORE_DIRS:
        if (directory / name).is_dir():
            paths += (directory / name).iterdir()
    return {
        path.relative_to(directory).as_posix(): path.stat().st_size
        for path in paths
        if path.is_file() and not path.name.startswith(".")
    }


def fetch(source: Any, bucket: str, prefix: str, directory: Path, key: str) -> None:
    path = directory / key
    path.parent.mkdir(parents=True, exist_ok=True)
    source.download_file(bucket, prefix + key, str(path))


def tip_objects(
    source: Any, bucket: str, prefix: str, directory: Path, *, log: Any = None
) -> dict[str, int]:
    """What the tip of ``main`` needs, besides ``repo``: relative key -> size.

    Reads the ``repo`` file already in ``directory`` and fetches the tip and
    root snapshots there (the prune keeps both; Icechunk never expires the
    root), since listing a snapshot's manifests needs the file. Everything
    under ``transactions/`` and ``chunks/`` comes along and the prune drops
    what the tip doesn't reference: the expired ancestors' transaction logs
    stay referenced from the tip (``pruned_ancestor_tx_logs``), and no
    public API names a snapshot's native chunks. Both are small here; the
    data arrays are virtual and only the bookkeeping arrays are native.
    """
    repo = icechunk.Repository.open(icechunk.local_filesystem_storage(str(directory)))
    history = list(repo.ancestry(branch="main"))  # reads only the repo file
    sizes: dict[str, int] = {}
    for snapshot in {history[0].id, history[-1].id}:
        key = f"snapshots/{snapshot}"
        fetch(source, bucket, prefix, directory, key)
        sizes[key] = (directory / key).stat().st_size
        for manifest in repo.list_manifest_files(snapshot):
            sizes[f"manifests/{manifest.id}"] = manifest.size_bytes
    for kind in ("transactions", "chunks"):
        listed = object_sizes(source, bucket, f"{prefix}{kind}/", log=log)
        sizes |= {f"{kind}/{key}": size for key, size in listed.items()}
    return sizes


# --------------------------------------------------------------------------
# Stages
# --------------------------------------------------------------------------


def report(source: Any, bucket: str, prefix: str, *, log: Any = sys.stderr) -> None:
    """Dry run: say what the tip needs, from a scratch copy of ``repo``."""
    with tempfile.TemporaryDirectory() as scratch:
        fetch(source, bucket, prefix, Path(scratch), REPO_INFO_KEY)
        sizes = tip_objects(source, bucket, prefix, Path(scratch))
    print(
        f"the tip of main needs {len(sizes) + 1:,} objects "
        f"({human_bytes(sum(sizes.values()))}) from s3://{bucket}/{prefix}",
        file=log,
    )


@stage
def download(
    source: Any,
    bucket: str,
    prefix: str,
    directory: Path,
    *,
    workers: int = WORKERS,
    log: Any = sys.stderr,
    limit: int | None = None,
) -> int:
    """Fetch what the tip needs into the empty ``directory``; return how many.

    ``repo`` comes first: it pins a snapshot whose files already exist, so a
    commit landing mid-download can't leave the copy naming files it never
    fetched. boto3 renames completed downloads into place, so a crash
    leaves no partial file under a store key. ``limit`` fetches at most that
    many objects, for a trial; a partial store can't be pruned or published.
    """
    if directory.exists() and any(directory.iterdir()):
        raise SystemExit(f"{directory} is not empty; delete it or pass an empty --dir")
    directory.mkdir(parents=True, exist_ok=True)
    fetch(source, bucket, prefix, directory, REPO_INFO_KEY)
    remote = tip_objects(source, bucket, prefix, directory, log=log)
    todo = {k: size for k, size in remote.items() if not (directory / k).exists()}
    if limit is not None:
        print(f"trial: fetching {limit:,} of {len(todo):,} objects", file=log)
        todo = dict(list(todo.items())[:limit])
    print(
        f"{len(todo):,} objects to fetch ({human_bytes(sum(todo.values()))})",
        file=log,
        flush=True,
    )

    manager = transfer(source, workers)
    with TransferBar(
        "download",
        total_bytes=sum(todo.values()),
        total_files=len(todo),
        log=log,
    ) as bar:

        def get(key: str) -> None:
            path = directory / key
            path.parent.mkdir(parents=True, exist_ok=True)
            manager.download_file(
                bucket, prefix + key, str(path), callback=bar.add_bytes
            )
            bar.file_done()

        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(get, todo))
    print(f"downloaded {len(todo)} objects to {directory}", file=log)
    return len(todo)


@stage
def prune(directory: Path, *, log: Any = sys.stderr) -> str:
    """Cut the copy down to the tip of ``main`` in place; return the tip's id.

    Icechunk exposes no progress hooks here, so this stage reports only
    its duration.
    """
    repo = icechunk.Repository.open(icechunk.local_filesystem_storage(str(directory)))
    print("expiring snapshots...", file=log, flush=True)
    repo.expire_snapshots(
        older_than=FAR_FUTURE, delete_expired_branches=True, delete_expired_tags=True
    )
    print("garbage-collecting...", file=log, flush=True)
    # GC fetches the retained snapshots' manifests concurrently, 500 at a
    # time by default, and the local filesystem store opens each file
    # several times for range reads: past macOS's 256 open-file limit.
    summary = repo.garbage_collect(
        delete_object_older_than=FAR_FUTURE, max_concurrent_manifest_fetches=WORKERS
    )
    tip = repo.lookup_branch("main")
    print(f"pruned to main @ {tip}: {summary}", file=log)
    return tip


@stage
def zip_store(directory: Path, archive: Path, *, log: Any = sys.stderr) -> None:
    """Zip the store's files in ``directory`` to the new file ``archive``.

    Stored, not deflated: Icechunk's files are already compressed.
    """
    if archive.exists():
        raise SystemExit(f"{archive} exists; delete it or pass another --dir")
    sizes = store_files(directory)
    if REPO_INFO_KEY not in sizes:
        raise SystemExit(f"{directory} holds no Icechunk store (no repo file)")
    with TransferBar(
        "zip", total_bytes=sum(sizes.values()), total_files=len(sizes), log=log
    ) as bar:
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_STORED) as zf:
            for key in sorted(sizes):
                zf.write(directory / key, key)
                bar.add_bytes(sizes[key])
                bar.file_done()
    print(f"zipped {archive} ({human_bytes(archive.stat().st_size)})", file=log)


@stage
def upload(
    destination: Any,
    directory: Path,
    bucket: str,
    prefix: str,
    *,
    workers: int = WORKERS,
    log: Any = sys.stderr,
) -> int:
    """Send the store files the destination lacks, ``repo`` last; return how many."""
    try:
        have = object_sizes(destination, bucket, prefix, log=log)
    except ClientError as error:
        raise SystemExit(
            f"listing {bucket}/{prefix} failed: {error}. "
            "Check the SOURCE_COOP_* credentials cover that prefix."
        ) from error
    local = store_files(directory)
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

    manager = transfer(destination, workers)
    with TransferBar(
        "upload", total_bytes=total_bytes, total_files=len(todo) + 1, log=log
    ) as bar:

        def put(key: str) -> None:
            manager.upload_file(
                str(directory / key), bucket, prefix + key, callback=bar.add_bytes
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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="report what the tip needs and exit without downloading",
    )
    parser.add_argument(
        "--limit",
        type=int,
        metavar="N",
        help="trial: download at most N objects (comment out the later stages)",
    )
    parser.add_argument(
        "--dir",
        type=Path,
        help="empty or absent directory for the download (default: stores/<prefix>)",
    )
    args = parser.parse_args()
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be at least 1")

    bucket = os.environ.get("ICECHUNK_BUCKET")
    prefix = storage_prefix()
    if not bucket or not prefix:
        print(
            "ICECHUNK_BUCKET and S3_PREFIX/ICECHUNK_PREFIX must be set "
            "(local-filesystem stores have nothing to publish)",
            file=sys.stderr,
        )
        return 2
    prefix = prefix.strip("/")
    destination_prefix = f"{DEST_ROOT}/{prefix}/"  # per collection, under DEST_ROOT
    directory = (args.dir or Path("stores") / prefix).resolve()
    archive = directory.with_name(directory.name + ".zip")
    source = source_client()
    print(
        f"s3://{bucket}/{prefix}/ -> s3://{DEST_BUCKET}/{destination_prefix}",
        file=sys.stderr,
    )

    if args.dry_run:
        report(source, bucket, f"{prefix}/")
        return 0

    # The stages. Each reads what the one before left in `directory`, so
    # comment out the ones you don't need: everything after download for a
    # --limit trial, or the ones that finished when rerunning after a crash.
    download(source, bucket, f"{prefix}/", directory, limit=args.limit)
    prune(directory)
    zip_store(directory, archive)
    upload(destination_client(), directory, DEST_BUCKET, destination_prefix)
    return 0


if __name__ == "__main__":
    sys.exit(main())
