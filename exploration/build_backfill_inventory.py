# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "earthaccess>=0.14",
#     "boto3>=1.34.0",
# ]
# ///
"""Build the backfill inventory file for the selected TEMPO L3 collection.

Queries CMR for every granule of the collection (HCHO by default,
``--collection no2`` for NO2, optionally windowed with ``--start``/``--end``),
takes one ``.nc`` data link per granule, and writes them in chronological
order as a bare JSON array — exactly the format
``backfill_handlers.inventory.read_inventory`` parses. Upload the file (or
pass ``--s3-uri`` to have this script upload it) and hand its URI to
``scripts/start_backfill.sh``.

The backfill pipeline requires each inventory entry to map to a distinct
region of the store (merge is last-writer-wins), so the script fails loudly
if two granules claim the same time step or a granule has no ``.nc`` link.

Only CMR metadata is read, so no Earthdata credentials are needed;
``--s3-uri`` uses the ambient AWS credentials.

Usage:
    uv run exploration/build_backfill_inventory.py
    uv run exploration/build_backfill_inventory.py --collection no2
    uv run exploration/build_backfill_inventory.py --start 2024-01-01 --end 2024-02-01
    uv run exploration/build_backfill_inventory.py --max-count 100 \
        --s3-uri s3://my-bucket/inventory/tempo-hcho-test.json
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from tempo_collections import add_collection_argument, resolve_concept_id


class InventoryError(Exception):
    """The granule set cannot form a valid backfill inventory."""


def _temporal_start(granule: Any) -> str:
    return str(granule["umm"]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"])


def build_inventory(granules: list[Any], access: str) -> list[str]:
    """Ordered, validated inventory keys for ``granules``.

    Returns one ``.nc`` data link per granule, sorted by temporal start so
    partitions stay time-contiguous. Raises :class:`InventoryError` for an
    empty granule set, a granule without a ``.nc`` link, or two granules on
    the same time step (which would break the pipeline's disjoint-regions
    requirement).
    """
    if not granules:
        raise InventoryError("No granules matched the query")

    entries: list[tuple[str, str]] = []
    for granule in granules:
        links = [u for u in granule.data_links(access=access) if u.endswith(".nc")]
        if not links:
            raise InventoryError(
                f"No .nc data link for granule {granule['meta']['concept-id']}"
            )
        entries.append((_temporal_start(granule), links[0]))

    entries.sort()
    duplicates = {
        start
        for (start, _), (next_start, _) in zip(entries, entries[1:])
        if start == next_start
    }
    if duplicates:
        raise InventoryError(
            "Multiple granules share a time step (the backfill requires "
            f"disjoint regions): {sorted(duplicates)}"
        )
    return [key for _, key in entries]


def write_inventory(keys: list[str], path: Path) -> None:
    """Write ``keys`` as the bare JSON array ``read_inventory`` expects."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(keys, indent=1) + "\n")


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
        "--output",
        default=None,
        help=("Output path (default inventories/tempo-<collection>-inventory.json)"),
    )
    parser.add_argument(
        "--s3-uri",
        help="Also upload the inventory to this s3://bucket/key location",
    )
    add_collection_argument(parser)
    args = parser.parse_args()
    concept_id = resolve_concept_id(args)
    output = Path(args.output or f"inventories/tempo-{args.collection}-inventory.json")

    print(f"Searching CMR for all granules of {concept_id}...", file=sys.stderr)
    granules = search_granules(concept_id, args.start, args.end)
    print(f"  {len(granules)} granules returned", file=sys.stderr)

    keys = build_inventory(granules, access=args.access)
    if args.max_count:
        keys = keys[-args.max_count :]

    write_inventory(keys, output)
    print(f"Wrote {len(keys)} keys to {output}", file=sys.stderr)
    print(f"  first: {keys[0]}", file=sys.stderr)
    print(f"  last:  {keys[-1]}", file=sys.stderr)

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
