# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "earthaccess>=0.14",
#     "boto3>=1.34.0",
#     "h5py",
#     "virtualizarr-processor",
# ]
#
# [tool.uv.sources]
# virtualizarr-processor = { path = "../lambda/virtualizarr-processor" }
# ///
"""Build the typed backfill inventory for the selected TEMPO L3 collection.

Produces the ``tempo-backfill-inventory/1`` JSON document the backfill
pipeline consumes: one entry per granule with its ``.nc`` data link, its
granule UR (the filename stem, the convention the forward consumer relies
on), and the granule's exact in-file ``/time[0]`` value.

The exact times matter. TEMPO's in-file scan time differs from the CMR
and filename timestamps (...T174200Z has /time = 17:42:18.02), and the
store's time axis is built from these values at Init. There is no
metadata-only source for them, so this script opens every granule's
header (a few KB each) with bounded concurrency and backoff.

Republished granules (same UR, new revision) are deduped keeping the
newest revision. The pydantic model validates the result before anything
is written.

Requires Earthdata credentials (``~/.netrc`` or ``$EARTHDATA_TOKEN``) for
the per-granule reads; run in us-west-2 with ``--access direct`` for the
production inventory.

Usage:
    uv run scripts/build_backfill_inventory.py
    uv run scripts/build_backfill_inventory.py --collection no2
    uv run scripts/build_backfill_inventory.py --start 2024-01-01 --end 2024-02-01
    uv run scripts/build_backfill_inventory.py --max-count 100 \
        --s3-uri s3://my-bucket/inventory/tempo-hcho-test.json
"""

import argparse
import sys
import time as time_module
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from virtualizarr_processor.collection import load_collection
from virtualizarr_processor.inventory import SCHEMA_ID, BackfillInventory, GranuleEntry

TIME_UNITS = "seconds since 1980-01-06T00:00:00Z"
READ_ATTEMPTS = 4
BACKOFF_SECONDS = (10, 30, 60)


class InventoryError(Exception):
    """The granule set cannot form a valid backfill inventory."""


def _revision(granule: Any) -> int:
    return int(granule["meta"].get("revision-id", 0))


def data_link(granule: Any, access: str) -> str:
    links = [u for u in granule.data_links(access=access) if u.endswith(".nc")]
    if not links:
        raise InventoryError(
            f"No .nc data link for granule {granule['meta']['concept-id']}"
        )
    return str(links[0])


def dedupe_republications(granules: list[Any]) -> list[Any]:
    """Keep only the newest revision of each granule UR."""
    newest: dict[str, Any] = {}
    for granule in granules:
        ur = str(granule["umm"].get("GranuleUR", granule["meta"]["concept-id"]))
        if ur not in newest or _revision(granule) > _revision(newest[ur]):
            newest[ur] = granule
    return list(newest.values())


def build_inventory(
    granules: list[Any],
    *,
    access: str,
    read_time: Callable[[str], float],
    collection_shortname: str,
    concept_id: str,
    workers: int = 4,
) -> BackfillInventory:
    """Build the validated typed inventory for ``granules``.

    ``read_time(url) -> float`` supplies each granule's exact in-file
    time; it is injectable so the logic can be tested offline. Raises
    ``InventoryError`` or ``pydantic.ValidationError`` for any set that
    cannot form a valid axis.
    """
    if not granules:
        raise InventoryError("No granules matched the query")
    deduped = dedupe_republications(granules)
    urls = [data_link(granule, access) for granule in deduped]

    with ThreadPoolExecutor(max_workers=workers) as pool:
        times = list(pool.map(read_time, urls))

    entries = sorted(
        (
            GranuleEntry(
                url=url,
                granule_ur=url.rsplit("/", 1)[-1].removesuffix(".nc"),
                time=time_value,
            )
            for url, time_value in zip(urls, times)
        ),
        key=lambda entry: entry.time,
    )
    return BackfillInventory(
        schema=SCHEMA_ID,  # type: ignore[call-arg]
        collection=collection_shortname,
        concept_id=concept_id,
        time_units=TIME_UNITS,
        built_at=datetime.now(timezone.utc).isoformat(),
        granules=tuple(entries),
    )


def read_time_via_earthaccess(url: str) -> float:
    """Read the granule's exact /time[0] from its header."""
    import earthaccess
    import h5py

    for attempt in range(READ_ATTEMPTS):
        try:
            [f] = earthaccess.open([url])
            with h5py.File(f) as h5:
                return float(h5["time"][0])
        except Exception as error:
            if attempt == READ_ATTEMPTS - 1:
                raise
            delay = BACKOFF_SECONDS[min(attempt, len(BACKOFF_SECONDS) - 1)]
            print(
                f"  {url.rsplit('/', 1)[-1]}: {type(error).__name__} "
                f"(attempt {attempt + 1}), retrying in {delay}s",
                file=sys.stderr,
            )
            time_module.sleep(delay)
    raise AssertionError("unreachable")


def write_inventory(inventory: BackfillInventory, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(inventory.to_json() + "\n")


def search_granules(concept_id: str, start: str | None, end: str | None) -> list[Any]:
    """All CMR granules for the collection, optionally windowed in time."""
    import earthaccess  # deferred so the pure helpers are testable offline

    kwargs: dict[str, Any] = {"concept_id": concept_id, "count": -1}
    if start or end:
        kwargs["temporal"] = (start, end)
    return list(earthaccess.search_data(**kwargs))


def upload(path: Path, s3_uri: str) -> None:
    import boto3  # deferred so the pure helpers are testable offline

    bucket, _, key = s3_uri.removeprefix("s3://").partition("/")
    if not bucket or not key:
        raise InventoryError(f"--s3-uri must look like s3://bucket/key: {s3_uri}")
    boto3.client("s3").upload_file(str(path), bucket, key)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--access",
        choices=["direct", "external"],
        default="direct",
        help=(
            "Data link flavor: 'direct' for in-region s3:// keys (default; "
            "what the deployed backfill workers read), 'external' for "
            "EDL-authed HTTPS URLs"
        ),
    )
    parser.add_argument("--start", help="Temporal window start (ISO date)")
    parser.add_argument("--end", help="Temporal window end (ISO date)")
    parser.add_argument(
        "--max-count",
        type=int,
        help="Keep only the N most recent granules (for test runs)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Concurrent per-granule header reads (keep low over HTTPS)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help=("Output path (default inventories/tempo-<collection>-inventory.json)"),
    )
    parser.add_argument(
        "--s3-uri",
        help="Also upload the inventory to this s3://bucket/key location",
    )
    parser.add_argument(
        "--collection",
        choices=["hcho", "no2"],
        default="hcho",
        help="TEMPO L3 collection to target (default hcho)",
    )
    parser.add_argument(
        "--concept-id",
        help="Explicit CMR collection concept ID (overrides --collection)",
    )
    args = parser.parse_args()
    concept_id = args.concept_id or load_collection(args.collection).concept_id
    output = Path(args.output or f"inventories/tempo-{args.collection}-inventory.json")

    import earthaccess

    # "all" tries $EARTHDATA_TOKEN / $EARTHDATA_USERNAME+PASSWORD first,
    # then ~/.netrc — matching the docstring's promise.
    earthaccess.login()

    print(f"Searching CMR for all granules of {concept_id}...", file=sys.stderr)
    granules = search_granules(concept_id, args.start, args.end)
    print(f"  {len(granules)} granules returned", file=sys.stderr)
    if args.max_count:
        granules = sorted(
            granules,
            key=lambda g: str(
                g["umm"]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"]
            ),
        )[-args.max_count :]

    shortname = str(granules[0]["umm"]["CollectionReference"]["ShortName"])
    print(
        f"Reading exact /time from {len(granules)} granule headers "
        f"({args.workers} workers)...",
        file=sys.stderr,
    )
    inventory = build_inventory(
        granules,
        access=args.access,
        read_time=read_time_via_earthaccess,
        collection_shortname=shortname,
        concept_id=concept_id,
        workers=args.workers,
    )

    write_inventory(inventory, output)
    print(f"Wrote {len(inventory.granules)} granules to {output}", file=sys.stderr)
    print(f"  first: {inventory.granules[0].url}", file=sys.stderr)
    print(f"  last:  {inventory.granules[-1].url}", file=sys.stderr)

    if args.s3_uri:
        upload(output, args.s3_uri)
        print(f"Uploaded to {args.s3_uri}", file=sys.stderr)
        print(
            f"Start the run with: scripts/start_backfill.sh <name> {args.s3_uri}",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
