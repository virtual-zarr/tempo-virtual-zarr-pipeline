"""The store manifest and pending ledger, kept inside the store itself.

The store manifest is the store's current inventory: the same
``BackfillInventory`` document, now stored as two vlen-string arrays
(``granule_ur``/``granule_url``) on the append dimension plus scalar
metadata in the root attribute ``tempo_store``, with the time values read
straight from the store's own ``time`` axis. The pending ledger holds
granules that arrived out of order and are waiting for the scheduled
re-sort job, kept in the root attribute ``pending_ledger`` and deduped by
granule UR so at-least-once SQS delivery is harmless. Both are written
through the same session as the data they describe, so they commit
atomically with it and cannot drift from it or race a concurrent writer.
"""

from __future__ import annotations

import os
from collections.abc import Iterable

import numpy as np
import zarr
from zarr.abc.store import Store

from virtualizarr_processor.inventory import BackfillInventory, GranuleEntry

# The manifest's storage representation inside the store itself: two
# vlen-string arrays on the append dimension, plus two root attributes.
MANIFEST_ARRAYS: tuple[str, str] = ("granule_ur", "granule_url")
STORE_META_ATTRIBUTE = "tempo_store"
PENDING_LEDGER_ATTRIBUTE = "pending_ledger"
PIPELINE_STATE_ATTRIBUTES: frozenset[str] = frozenset(
    {STORE_META_ATTRIBUTE, PENDING_LEDGER_ATTRIBUTE}
)


def storage_prefix() -> str | None:
    """The repository's S3 key prefix: $S3_PREFIX and $ICECHUNK_PREFIX joined.

    Deployed Lambdas receive the combined value as $ICECHUNK_PREFIX; local
    runs with a per-collection env file carry the two parts, joined here
    exactly as the CDK stack joins them.
    """
    return (
        "/".join(
            part.strip("/")
            for part in (os.environ.get("S3_PREFIX"), os.environ.get("ICECHUNK_PREFIX"))
            if part and part.strip("/")
        )
        or None
    )


class StoreManifest:
    """The store's typed inventory, stored in the store itself.

    ``granule_ur``/``granule_url`` are vlen-string arrays on the append
    dimension (template-declared), the scalars live in the root attribute
    ``tempo_store``, and the time values are the store's own axis — so the
    manifest is committed atomically with the data it describes and cannot
    drift from it.
    """

    @staticmethod
    def read(store: Store) -> BackfillInventory | None:
        """Reconstruct the inventory, or None if the store carries none.

        Runs the full ``BackfillInventory`` validation (strictly increasing
        times, no duplicate URs), so a corrupted store fails loudly here.
        """
        group = zarr.open_group(store, mode="r")
        meta = group.attrs.get(STORE_META_ATTRIBUTE)
        axis = np.asarray(zarr.open_array(store, path="time")[:])
        if meta is None or not axis.size:
            return None
        urs = zarr.open_array(store, path=MANIFEST_ARRAYS[0])[:]
        urls = zarr.open_array(store, path=MANIFEST_ARRAYS[1])[:]
        return BackfillInventory.model_validate(
            dict(meta)
            | {
                "granules": [
                    {"url": str(url), "granule_ur": str(ur), "time": float(t)}
                    for url, ur, t in zip(urls, urs, axis, strict=True)
                ]
            }
        )

    @staticmethod
    def write(store: Store, inventory: BackfillInventory) -> None:
        """Write the arrays and meta attribute (does not touch the axis)."""
        n = len(inventory.granules)
        columns = {
            MANIFEST_ARRAYS[0]: [e.granule_ur for e in inventory.granules],
            MANIFEST_ARRAYS[1]: [e.url for e in inventory.granules],
        }
        for name, values in columns.items():
            array = zarr.open_array(store, path=name)
            array.resize((n,))
            array[:] = np.array(values, dtype=object)
        group = zarr.open_group(store, mode="a")
        group.attrs[STORE_META_ATTRIBUTE] = inventory.model_dump(
            by_alias=True, exclude={"granules"}
        )


class PendingLedger:
    """Out-of-order arrivals awaiting the re-sort job, deduped by granule UR.

    Stored as the root attribute ``pending_ledger``, so updates commit
    atomically with the batch that produced them; concurrent writers
    surface as icechunk commit conflicts instead of lost updates.
    """

    @staticmethod
    def read(store: Store) -> tuple[GranuleEntry, ...]:
        raw = zarr.open_group(store, mode="r").attrs.get(PENDING_LEDGER_ATTRIBUTE, [])
        return tuple(GranuleEntry.model_validate(item) for item in raw)

    @staticmethod
    def write(store: Store, entries: Iterable[GranuleEntry]) -> None:
        group = zarr.open_group(store, mode="a")
        group.attrs[PENDING_LEDGER_ATTRIBUTE] = [e.model_dump() for e in entries]

    @classmethod
    def append(cls, store: Store, entries: Iterable[GranuleEntry]) -> None:
        existing = list(cls.read(store))
        seen = {entry.granule_ur for entry in existing}
        for entry in entries:
            if entry.granule_ur not in seen:
                existing.append(entry)
                seen.add(entry.granule_ur)
        cls.write(store, existing)
