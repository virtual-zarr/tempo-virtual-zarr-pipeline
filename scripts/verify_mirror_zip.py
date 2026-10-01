#!/usr/bin/env python3
"""Check that a mirror zip is equivalent to the Icechunk store it came from.

Two layers, both reported, exit status 1 if either finds a difference:

1. Files. Every entry in the zip except ``repo`` must exist in the store
   under the same key with the same size and, unless ``--quick``, the same
   bytes. Icechunk's files are immutable and named by id, so this is the
   whole of the content check: manifests (the virtual references), native
   chunks, snapshots and transaction logs.

2. The store, as a reader sees it. The zip is unpacked and opened with
   Icechunk; its ``main`` must point at a snapshot the store's ``main``
   points at or descends from (the store moving on since the mirror is
   reported), and at that snapshot the Zarr metadata of every node and the
   set of virtual chunk locations must match. The zip's ancestry is the tip
   and the root, by design.

Reads the store with your AWS credentials, resolved by boto3 (so AWS_PROFILE
and SSO work); the store location comes from ICECHUNK_BUCKET and
S3_PREFIX/ICECHUNK_PREFIX, as for the mirror script. Reading metadata and
listing virtual locations needs no Earthdata access.

Usage:
    uv run --env-file .env_no2 scripts/verify_mirror_zip.py stores/tempo/no2/v04.zip
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
import tempfile
import zipfile
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import boto3
import icechunk
import zarr
from check_virtual_containers import store_credentials
from mirror_to_source_coop import (
    REGION,
    REPO_INFO_KEY,
    STORE_DIRS,
    WORKERS,
    object_sizes,
    source_client,
)
from tqdm import tqdm
from virtualizarr_processor.manifest import storage_prefix


def sha256(stream: Any) -> str:
    return hashlib.file_digest(stream, "sha256").hexdigest()


def compare_files(
    client: Any,
    bucket: str,
    prefix: str,
    archive: zipfile.ZipFile,
    *,
    quick: bool = False,
    workers: int = WORKERS,
    log: Any = None,
) -> list[str]:
    """Differences between the zip's entries and the store's objects."""
    entries = {
        info.filename: info.file_size
        for info in archive.infolist()
        if not info.is_dir() and info.filename != REPO_INFO_KEY
    }
    remote: dict[str, int] = {}
    for kind in STORE_DIRS:
        listed = object_sizes(client, bucket, f"{prefix}{kind}/", log=log)
        remote |= {f"{kind}/{key}": size for key, size in listed.items()}

    findings = []
    for key, size in sorted(entries.items()):
        if key not in remote:
            findings.append(f"{key}: in the zip, not in the store")
        elif remote[key] != size:
            findings.append(
                f"{key}: {size} bytes in the zip, {remote[key]} in the store"
            )
    if quick or findings:
        return findings

    def differs(key: str) -> str | None:
        body = client.get_object(Bucket=bucket, Key=prefix + key)["Body"]
        with archive.open(key) as zipped:
            return None if sha256(body) == sha256(zipped) else f"{key}: bytes differ"

    with ThreadPoolExecutor(max_workers=workers) as pool:
        results: Iterable[str | None] = pool.map(differs, sorted(entries))
        if log is not None:
            results = tqdm(results, total=len(entries), desc="comparing", file=log)
        return [finding for finding in results if finding]


def node_metadata(session: icechunk.Session) -> dict[str, Any]:
    root = zarr.open_group(session.store, mode="r")
    members = {"": root.metadata.to_dict()}
    for path, node in root.members(max_depth=None):
        members[path] = node.metadata.to_dict()
    return members


def compare_repos(store: icechunk.Repository, mirror: icechunk.Repository) -> list[str]:
    """Differences between the store and the unpacked zip as readers see them."""
    tip = mirror.lookup_branch("main")
    history = [info.id for info in store.ancestry(branch="main")]
    if tip not in history:
        return [f"the zip's main ({tip}) is not in the store's main history"]
    findings = []
    if behind := history.index(tip):
        findings.append(f"the store's main is {behind} commit(s) ahead of the zip")

    at_tip = store.readonly_session(snapshot_id=tip)
    mirrored = mirror.readonly_session("main")
    theirs, ours = node_metadata(at_tip), node_metadata(mirrored)
    for path in sorted(theirs.keys() | ours.keys()):
        if theirs.get(path) != ours.get(path):
            findings.append(f"{path or '/'}: zarr metadata differs")
    if set(at_tip.all_virtual_chunk_locations()) != set(
        mirrored.all_virtual_chunk_locations()
    ):
        findings.append("virtual chunk locations differ")
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("zip", type=Path, help="the mirror script's zip")
    parser.add_argument(
        "--quick", action="store_true", help="compare sizes only, not bytes"
    )
    args = parser.parse_args()

    bucket = os.environ.get("ICECHUNK_BUCKET")
    prefix = storage_prefix()
    if not bucket or not prefix:
        print(
            "ICECHUNK_BUCKET and S3_PREFIX/ICECHUNK_PREFIX must be set", file=sys.stderr
        )
        return 2
    prefix = prefix.strip("/") + "/"

    with zipfile.ZipFile(args.zip) as archive:
        print(f"files: {args.zip} vs s3://{bucket}/{prefix}", file=sys.stderr)
        findings = compare_files(
            source_client(), bucket, prefix, archive, quick=args.quick, log=sys.stderr
        )
        print("store: opening both", file=sys.stderr)
        with tempfile.TemporaryDirectory() as unpacked:
            archive.extractall(unpacked)
            store = icechunk.Repository.open(
                icechunk.s3_storage(
                    bucket=bucket,
                    prefix=prefix.rstrip("/"),
                    region=REGION,
                    **store_credentials(boto3.Session()),  # type: ignore[arg-type]
                )
            )
            mirror = icechunk.Repository.open(
                icechunk.local_filesystem_storage(unpacked)
            )
            findings += compare_repos(store, mirror)

    for finding in findings:
        print(finding)
    print("equivalent" if not findings else f"{len(findings)} difference(s)")
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
