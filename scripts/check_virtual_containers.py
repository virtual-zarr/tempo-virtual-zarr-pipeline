#!/usr/bin/env python3
"""Check that a store declares virtual chunk containers its readers can use.

The store holds byte-range references into the DAAC's granule objects, not
the bytes themselves. References are relative to a container named
``asdc``, as ``vcc://asdc/<key>``. Stores from before this change are
migrated by scripts/relativize_refs.py. To follow a reference, icechunk
needs a container with that name. Its url_prefix must cover the granule
URLs, and the container must be authorized with credentials.

The store's persisted container is written once, when the repository is
created, from whatever $VIRTUAL_CHUNK_PREFIX was set to then; opening it
later merges the writers' config in memory without saving it. So the
writers keep working after the prefix changes while the persisted copy goes
stale, and a reader who opens the store without supplying a config of their
own gets UnauthorizedVirtualChunkContainer on every chunk.

Four things are checked:

1. the persisted config declares at least one virtual chunk container;
2. one of them is named ``asdc``, the name relative references use;
3. every URL in the store manifest falls under one of the containers;
4. a chunk actually reads back with those containers authorized.

``--fix`` writes the expected container into the store's config. It needs
write access and adds no commit, since the config lives outside the
version history.

The store defaults to the processor's environment variables
($ICECHUNK_BUCKET, $S3_PREFIX/$ICECHUNK_PREFIX, $ICECHUNK_REGION).
``--bucket``/``--prefix`` and friends point it somewhere else, such as a
published copy. Store credentials come from boto3 (so --profile and
AWS_PROFILE work); reading chunks needs Earthdata credentials either way.

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

import boto3
import icechunk
import numpy as np
import zarr
from earthaccess_auth.adapters.icechunk import earthdata_containers_credentials
from virtualizarr_processor.manifest import (
    MANIFEST_ARRAYS,
    StoreManifest,
    storage_prefix,
)
from virtualizarr_processor.processor import (
    VIRTUAL_CHUNK_CONTAINER,
    virtual_chunk_prefix,
)


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


def authorize(repo: icechunk.Repository) -> icechunk.Repository:
    """Reopen ``repo`` with its declared containers authorized, as a reader would.

    Containers in Earthdata buckets get Earthdata credentials. Local ones
    (test stores) need none. Any other container stays unauthorized, so a
    reader's failure to read it shows up here too.
    """
    authorized = earthdata_containers_credentials(repo)
    containers = repo.config.virtual_chunk_containers or {}
    authorized |= icechunk.containers_credentials(
        {
            prefix: icechunk.credentials.LocalFileSystemAccess
            for prefix in containers
            if prefix.startswith("file://")
        }
    )
    return repo.reopen(authorize_virtual_chunk_access=authorized)


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


def resolve_region(explicit: str | None, session_region: str | None) -> str:
    """The store's region, preferring the deployment's own variable.

    Left unset, icechunk asks EC2's instance metadata service for a default
    and fails on anything that is not an EC2 instance, several frames deep
    in a dispatch error that never mentions the region.
    """
    for value in (explicit, os.environ.get("ICECHUNK_REGION"), session_region):
        if value:
            return value
    raise SystemExit(
        "no region: pass --region, or set ICECHUNK_REGION or AWS_REGION, "
        "or give the profile one"
    )


def store_credentials(session: boto3.Session) -> dict[str, str | None]:
    """Static S3 credentials for the store, resolved by boto3.

    icechunk's own from_env path defers to the AWS SDK for Rust, which
    reports finding nothing as a dispatch failure from whichever call
    needed the credentials. boto3 walks the same chain (environment, shared
    credentials file, SSO, container and instance roles) and says so
    plainly. They are resolved once and passed as static values, so a run
    outliving a short-lived session token would have to be repeated.
    """
    credentials = session.get_credentials()
    if credentials is None:
        raise SystemExit(
            "no AWS credentials: pass --profile, or set AWS_PROFILE or "
            "AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY"
        )
    frozen = credentials.get_frozen_credentials()
    return {
        "access_key_id": frozen.access_key,
        "secret_access_key": frozen.secret_key,
        "session_token": frozen.token,
    }


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
    session = boto3.Session(profile_name=args.profile)
    region = resolve_region(args.region, session.region_name)
    credentials = {} if args.anonymous else store_credentials(session)
    print(f"store:        s3://{bucket}/{prefix} ({region})", file=sys.stderr)
    return icechunk.s3_storage(
        bucket=bucket,
        prefix=prefix,
        region=region,
        endpoint_url=args.endpoint_url,
        anonymous=args.anonymous or None,
        **credentials,  # type: ignore[arg-type]
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bucket")
    parser.add_argument("--prefix")
    parser.add_argument("--region")
    parser.add_argument("--profile", help="AWS profile to read the store with")
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
    expected = virtual_chunk_prefix()

    config = icechunk.Repository.fetch_config(storage)
    if config is None:
        print("FAIL: store has no config.yaml", file=sys.stderr)
        return 1
    containers = config.virtual_chunk_containers or {}
    declared = set(containers)
    print(f"containers:   {sorted(declared) or 'none'}", file=sys.stderr)

    problems: list[str] = []
    if not declared:
        problems.append(
            f"no virtual chunk container declared; readers cannot follow any "
            f"reference (expected {expected!r})"
        )
    elif all(c.name != VIRTUAL_CHUNK_CONTAINER for c in containers.values()):
        problems.append(
            f"no container is named {VIRTUAL_CHUNK_CONTAINER!r}; relative "
            f"references (vcc://{VIRTUAL_CHUNK_CONTAINER}/...) cannot resolve"
        )

    repo = authorize(icechunk.Repository.open(storage=storage, config=config))
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
            icechunk.VirtualChunkContainer(
                expected, chunk_store_for(expected), name=VIRTUAL_CHUNK_CONTAINER
            )
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
