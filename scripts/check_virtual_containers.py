#!/usr/bin/env python3
"""Check that a store declares virtual chunk containers its readers can use.

The store holds byte-range references into the DAAC's granule objects, not
the bytes themselves. To follow a reference, icechunk needs a virtual chunk
container whose url_prefix covers the referenced URL, and credentials
authorizing it.

The store's persisted container is written once, when the repository is
created, from whatever $VIRTUAL_CHUNK_PREFIX was set to then; opening it
later merges the writers' config in memory without saving it. So the
writers keep working after the prefix changes while the persisted copy goes
stale, and a reader who opens the store without supplying a config of their
own gets UnauthorizedVirtualChunkContainer on every chunk.

Three things are checked:

1. the persisted config declares at least one virtual chunk container;
2. every URL in the store manifest falls under one of them;
3. a chunk actually reads back with those containers authorized.

``--fix`` writes the expected container into the store's config. It needs
write access and adds no commit, since the config lives outside the
version history.

The store defaults to the processor's environment variables
($ICECHUNK_BUCKET, $S3_PREFIX/$ICECHUNK_PREFIX, $ICECHUNK_REGION).
``--bucket``/``--prefix`` and friends point it somewhere else, such as a
published copy. Reading chunks needs Earthdata credentials either way.

Usage:
    uv run --env-file .env_no2 scripts/check_virtual_containers.py
    uv run --env-file .env_no2 scripts/check_virtual_containers.py --fix
    uv run scripts/check_virtual_containers.py \
        --bucket pangeo --prefix tempo-virtual-icechunk/tempo/no2/v04 \
        --endpoint-url https://data.source.coop --region us-east-1
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Any, cast

import icechunk
import numpy as np
import zarr
from virtualizarr_processor.granule import icechunk_virtual_credentials
from virtualizarr_processor.manifest import (
    MANIFEST_ARRAYS,
    StoreManifest,
    storage_prefix,
)
from virtualizarr_processor.processor import DEFAULT_VIRTUAL_CHUNK_PREFIX


def uncovered_urls(declared: set[str], urls: list[str]) -> list[str]:
    """The distinct URL prefixes in ``urls`` that no container covers.

    Reported by bucket rather than per granule: a store references tens of
    thousands of URLs, and they fail as a group or not at all.
    """
    missing = {
        url.rsplit("/", 1)[0] + "/"
        for url in urls
        if not any(url.startswith(prefix) for prefix in declared)
    }
    return sorted(missing)


def credentials_for(prefix: str) -> Any:
    """Credentials authorizing reads from a container's url_prefix."""
    if prefix.startswith("file://"):
        return icechunk.credentials.LocalFileSystemAccess
    if prefix.startswith("s3://"):
        return icechunk_virtual_credentials(prefix.removeprefix("s3://").split("/")[0])
    raise ValueError(f"unsupported container prefix {prefix!r}")


def chunk_store_for(prefix: str) -> Any:
    """The object store a container at ``prefix`` reads through."""
    if prefix.startswith("file://"):
        return icechunk.local_filesystem_store(prefix.removeprefix("file://"))
    if prefix.startswith("s3://"):
        return icechunk.s3_store(
            region=os.environ.get("VIRTUAL_CHUNK_REGION", "us-west-2")
        )
    raise ValueError(f"unsupported container prefix {prefix!r}")


def read_one_chunk(repo: icechunk.Repository) -> str:
    """Read a single value through a virtual reference; describe what it read.

    The value itself is not checked: TEMPO fields are mostly fill, so a NaN
    here is ordinary. What is being tested is that the read returns at all
    rather than raising on an unauthorized or missing container.
    """
    session = repo.readonly_session("main")
    group = zarr.open_group(session.store, mode="r")
    arrays = [
        (name, cast(zarr.Array, group[name]))
        for name in group.array_keys()
        if name not in MANIFEST_ARRAYS
    ]
    # 3-D picks a data variable: the coordinates are all 1-D.
    candidates = [(name, array) for name, array in arrays if array.ndim == 3]
    if not candidates:
        raise RuntimeError("store has no 3-D data variable to read")
    name, array = candidates[0]
    index = tuple(size // 2 for size in array.shape)
    value = np.asarray(array[index])
    return f"{name}{list(index)} = {value}"


def open_storage(args: argparse.Namespace) -> icechunk.Storage:
    bucket = args.bucket or os.environ.get("ICECHUNK_BUCKET")
    prefix = args.prefix or storage_prefix()
    local = os.environ.get("ICECHUNK_LOCAL_PATH")
    if not bucket and local:
        print(f"store:        {local}", file=sys.stderr)
        return icechunk.local_filesystem_storage(local)
    if not bucket or not prefix:
        raise SystemExit(
            "pass --bucket/--prefix, or set ICECHUNK_BUCKET and "
            "S3_PREFIX/ICECHUNK_PREFIX, or ICECHUNK_LOCAL_PATH"
        )
    print(f"store:        s3://{bucket}/{prefix}", file=sys.stderr)
    return icechunk.s3_storage(
        bucket=bucket,
        prefix=prefix,
        region=args.region or os.environ.get("ICECHUNK_REGION"),
        endpoint_url=args.endpoint_url,
        anonymous=args.anonymous or None,
        from_env=None if args.anonymous else True,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bucket")
    parser.add_argument("--prefix")
    parser.add_argument("--region")
    parser.add_argument("--endpoint-url", help="for S3-compatible stores")
    parser.add_argument(
        "--anonymous", action="store_true", help="read the store without credentials"
    )
    parser.add_argument(
        "--fix",
        action="store_true",
        help="persist the expected container into the store's config",
    )
    parser.add_argument(
        "--no-read", action="store_true", help="skip the chunk read (check 3)"
    )
    args = parser.parse_args()

    storage = open_storage(args)
    expected = os.environ.get("VIRTUAL_CHUNK_PREFIX", DEFAULT_VIRTUAL_CHUNK_PREFIX)

    config = icechunk.Repository.fetch_config(storage)
    if config is None:
        print("FAIL: store has no config.yaml", file=sys.stderr)
        return 1
    declared = set(config.virtual_chunk_containers or {})
    print(f"containers:   {sorted(declared) or 'none'}", file=sys.stderr)

    problems: list[str] = []
    if not declared:
        problems.append(
            f"no virtual chunk container declared; readers cannot follow any "
            f"reference (expected {expected!r})"
        )

    repo = icechunk.Repository.open(
        storage=storage,
        config=config,
        authorize_virtual_chunk_access=icechunk.containers_credentials(
            {prefix: credentials_for(prefix) for prefix in declared}
        ),
    )
    manifest = StoreManifest.read(repo.readonly_session("main").store)
    if manifest is None:
        print("FAIL: store carries no manifest", file=sys.stderr)
        return 1
    urls = [entry.url for entry in manifest.granules]
    print(f"references:   {len(urls)} granule urls", file=sys.stderr)
    problems += [
        f"{prefix} is referenced by the manifest but no container covers it"
        for prefix in uncovered_urls(declared, urls)
    ]

    if args.fix and problems:
        config.set_virtual_chunk_container(
            icechunk.VirtualChunkContainer(expected, chunk_store_for(expected))
        )
        icechunk.Repository.open(storage=storage, config=config).save_config()
        print(
            f"fixed:        declared {expected!r}; re-run to confirm",
            file=sys.stderr,
        )
        return 1

    if not problems and not args.no_read:
        try:
            print(f"read:         {read_one_chunk(repo)}", file=sys.stderr)
        except Exception as error:
            problems.append(f"reading a chunk failed: {type(error).__name__}: {error}")

    if problems:
        print(f"FAIL: {len(problems)} problems", file=sys.stderr)
        for line in problems:
            print(f"  {line}", file=sys.stderr)
        return 1
    print("OK: containers cover every reference", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
