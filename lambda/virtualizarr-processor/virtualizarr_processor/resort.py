"""Pure helpers for the scheduled re-sort job (design spec §5).

The job folds the pending ledger (out-of-order arrivals) into the store:
merge the store manifest with the pending entries into one sorted
inventory — whose model validators reject any collision loudly — find the
first shifted axis position, rewrite the axis on a ``resort`` branch, and
re-ingest every granule from that position on.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

from virtualizarr_processor.inventory import BackfillInventory, GranuleEntry


def merge_pending(
    manifest: BackfillInventory, pending: Iterable[GranuleEntry]
) -> BackfillInventory:
    """The sorted union of the store manifest and the pending entries.

    Re-validated through the inventory model, so duplicate times or
    granule URs abort the job before it touches any branch.
    """
    entries = sorted([*manifest.granules, *pending], key=lambda entry: entry.time)
    return BackfillInventory.model_validate(
        manifest.model_dump(by_alias=True)
        | {
            "granules": [entry.model_dump() for entry in entries],
            "built_at": datetime.now(timezone.utc).isoformat(),
        }
    )


def first_shifted_index(manifest: BackfillInventory, merged: BackfillInventory) -> int:
    """The first axis position whose granule changed between the inventories.

    Every slot from here on must be rewritten: inserted granules occupy new
    positions and existing granules after the earliest insertion shift by
    the number of insertions before them.
    """
    for index, (old, new) in enumerate(zip(manifest.granules, merged.granules)):
        if old != new:
            return index
    return len(manifest.granules)
