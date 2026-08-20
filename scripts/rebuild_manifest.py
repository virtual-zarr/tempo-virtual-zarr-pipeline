#!/usr/bin/env python3
"""Reconstruct the store manifest from the store's own time axis.

The store manifest is derivable state: the axis fixes each slot's exact
time, and the granule that owns each time is recorded in the old
manifest, the pending ledger, a backfill inventory, or CMR. An Icechunk
commit and its manifest write are two separate writes, so a crash between
them leaves the pair divergent — redeliveries get rejected and the
re-sort job fails its axis check until the manifest is repaired. This
script is that repair: one command instead of hand-editing JSON in S3.

Each axis slot is resolved by exact time match against the existing
manifest, the pending ledger, and ``--inventory`` (in that order; two
sources disagreeing on the granule UR for one time is an error), and any
still-unresolved slot is looked up in CMR by nearest temporal match
(disable with ``--offline``). The result is validated through the
inventory model and against the axis before anything is written.

Dry-run by default: prints the rebuilt manifest's divergence from the
stored one and exits non-zero if slots could not be resolved. Pass
``--write`` to replace the stored manifest. Run ``verify_store.py``
afterwards to confirm the repaired manifest against the sources.

Uses the same environment variables as the processor Lambdas
(ICECHUNK_BUCKET or ICECHUNK_LOCAL_PATH, TEMPO_COLLECTION,
STORE_MANIFEST_URI, PENDING_LEDGER_URI).

Usage:
    uv run scripts/rebuild_manifest.py
    uv run scripts/rebuild_manifest.py --inventory s3://bucket/inventory.json
    uv run scripts/rebuild_manifest.py --offline --write
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from typing import Callable, Iterable, Optional

import numpy as np
import zarr
from virtualizarr_processor.inventory import SCHEMA_ID, BackfillInventory, GranuleEntry
from virtualizarr_processor.manifest import PendingLedger, StoreManifest

# verify_store.CmrLookup: when -> (url, granule_ur) of the nearest granule.
CmrLookup = Callable[[datetime], Optional[tuple[str, str]]]


def rebuild_entries(
    axis: np.ndarray,
    sources: list[tuple[str, Iterable[GranuleEntry]]],
    cmr_lookup: CmrLookup | None = None,
) -> tuple[list[GranuleEntry], list[str]]:
    """Resolve one granule per axis slot; returns (entries, problems).

    ``sources`` are consulted by exact time match in the given order; a
    later source disagreeing on the granule UR for a time is a problem.
    ``cmr_lookup`` (a ``verify_store.CmrLookup``) fills slots no source
    covers. ``entries`` is complete only when ``problems`` is empty.
    """
    import verify_store  # type: ignore[import-not-found]  # sibling script

    by_time: dict[float, GranuleEntry] = {}
    origin: dict[float, str] = {}
    problems: list[str] = []
    for name, entries in sources:
        for candidate in entries:
            known = by_time.get(candidate.time)
            if known is None:
                by_time[candidate.time] = candidate
                origin[candidate.time] = name
            elif known.granule_ur != candidate.granule_ur:
                problems.append(
                    f"time {candidate.time!r}: {origin[candidate.time]} says "
                    f"{known.granule_ur}, {name} says {candidate.granule_ur}"
                )

    resolved: list[GranuleEntry] = []
    for index, time_value in enumerate(float(t) for t in axis):
        entry: GranuleEntry | None = by_time.get(time_value)
        if entry is None and cmr_lookup is not None:
            found = cmr_lookup(verify_store.axis_datetime(time_value))
            if found is not None:
                url, granule_ur = found
                entry = GranuleEntry(url=url, granule_ur=granule_ur, time=time_value)
        if entry is None:
            problems.append(
                f"slot {index} (time {time_value!r}): no source knows this granule"
            )
        else:
            resolved.append(entry)
    return resolved, problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument(
        "--inventory",
        help="a backfill inventory (s3:// or path) as an extra slot source",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="do not consult CMR for unresolved slots",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="replace the stored manifest (default: dry-run)",
    )
    args = parser.parse_args()

    import verify_store  # type: ignore[import-not-found]  # sibling script
    from virtualizarr_processor.processor import Processor

    processor = Processor()
    repo = processor.open_backfill_repo()
    axis = np.asarray(
        zarr.open_array(repo.readonly_session("main").store, path="time")[:]
    )

    manifest_uri = os.environ["STORE_MANIFEST_URI"]
    try:
        old_manifest: BackfillInventory | None = StoreManifest.read(manifest_uri)
    except FileNotFoundError:
        old_manifest = None
    sources: list[tuple[str, Iterable[GranuleEntry]]] = []
    if old_manifest is not None:
        sources.append(("manifest", old_manifest.granules))
    ledger_uri = os.environ.get("PENDING_LEDGER_URI")
    if ledger_uri:
        sources.append(("ledger", PendingLedger.read(ledger_uri)))
    inventory = None
    if args.inventory:
        inventory = StoreManifest.read(args.inventory)
        sources.append(("inventory", inventory.granules))

    lookup = (
        None
        if args.offline
        else verify_store.cmr_lookup_for(processor.config.concept_id)
    )
    entries, problems = rebuild_entries(axis, sources, lookup)
    if problems:
        print(f"FAIL: {len(problems)} unresolved slots/conflicts", file=sys.stderr)
        for line in problems:
            print(f"  {line}", file=sys.stderr)
        return 1

    # built_at feeds the CMR poller's initial watermark; keep the earliest
    # known build time rather than pretending the store is brand new.
    built_at = (
        old_manifest.built_at
        if old_manifest is not None
        else (
            inventory.built_at
            if inventory is not None
            else datetime.now(timezone.utc).isoformat()
        )
    )
    rebuilt = BackfillInventory(
        schema_id=SCHEMA_ID,  # type: ignore[call-arg]  # pydantic alias
        collection=processor.config.collection_shortname,
        concept_id=processor.config.concept_id,
        time_units=processor.config.time_units,
        built_at=built_at,
        granules=tuple(entries),
    )
    StoreManifest.validate_against_axis(rebuilt, axis)

    old = list(old_manifest.granules) if old_manifest is not None else []
    changed = sum(
        1 for i, e in enumerate(entries) if i >= len(old) or old[i] != e
    ) + max(0, len(old) - len(entries))
    print(
        f"Rebuilt manifest: {len(entries)} slots, {changed} differ from the "
        f"stored manifest ({len(old)} slots)",
        file=sys.stderr,
    )
    if args.write:
        StoreManifest.write(manifest_uri, rebuilt)
        print(f"Wrote {manifest_uri}", file=sys.stderr)
        print("Run verify_store.py to confirm the repair.", file=sys.stderr)
    else:
        print("Dry run; pass --write to replace the stored manifest.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
