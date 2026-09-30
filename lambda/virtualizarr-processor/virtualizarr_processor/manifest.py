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
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from typing import cast

import numpy as np
import zarr
from zarr.abc.store import Store

from virtualizarr_processor.inventory import BackfillInventory, GranuleEntry

# The manifest's storage representation inside the store itself: two
# vlen-string arrays on the append dimension, plus two root attributes.
MANIFEST_ARRAYS: tuple[str, str] = ("granule_ur", "granule_url")
# A third bookkeeping column, kept out of MANIFEST_ARRAYS because that
# tuple is indexed positionally and feeds BackfillInventory
# reconstruction, which has no stamps. Each slot holds
# the exact ``last_updated_at`` stamp its references were written with
# (see :func:`stamp_value`), or "" when unknown. Stamps are opaque strings
# compared for equality only, so a future switch to ETag stamps is a
# value change, not a schema change.
STAMP_ARRAY = "granule_stamp"
STORE_META_ATTRIBUTE = "tempo_store"
PENDING_LEDGER_ATTRIBUTE = "pending_ledger"
PIPELINE_STATE_ATTRIBUTES: frozenset[str] = frozenset(
    {STORE_META_ATTRIBUTE, PENDING_LEDGER_ATTRIBUTE}
)


# The time axis stores seconds since this epoch. The collections' TOML
# declares the same epoch in their time_units; tests/test_manifest.py pins
# the two equal so they cannot silently drift.
TEMPO_EPOCH = datetime(1980, 1, 6, tzinfo=timezone.utc)


def axis_end_lag(axis_end: float) -> float:
    """Seconds between now and the store's last time slot — the freshness SLI
    behind the AxisEndLag metric and its staleness alarm."""
    return (datetime.now(timezone.utc) - TEMPO_EPOCH).total_seconds() - axis_end


def stamp_value(stamp: datetime) -> str:
    """The storage form of a ``last_updated_at`` stamp, ISO-8601 UTC."""
    return stamp.astimezone(timezone.utc).isoformat()


class GranuleStamps:
    """The per-slot ``last_updated_at`` stamps, stored next to the manifest.

    A store predating the array reads as ``None``, every stamp unknown, so
    the consumer's unchanged-redelivery fast path disables itself and
    behaves exactly like the pre-stamp pipeline. Unknown ("") stamps
    self-heal when the slot's next redelivery takes the slow path once and
    records its stamp.
    """

    @staticmethod
    def read(store: Store) -> list[str] | None:
        """All stamps in axis order, or None if the store has no array."""
        if STAMP_ARRAY not in zarr.open_group(store, mode="r"):
            return None
        return [str(v) for v in np.asarray(zarr.open_array(store, path=STAMP_ARRAY)[:])]

    @staticmethod
    def write(store: Store, stamps: Sequence[str]) -> None:
        array = zarr.open_array(store, path=STAMP_ARRAY)
        array.resize((len(stamps),))
        if stamps:
            array[:] = np.array(stamps, dtype=object)

    @classmethod
    def initialize(
        cls, store: Store, *, size: int, chunk: int, append_dim: str
    ) -> None:
        """Create the array if missing, and leave every stamp "" (unknown).

        Init paths rewrite every slot they describe, so any previously
        recorded stamps no longer match what will be written.
        """
        if STAMP_ARRAY in zarr.open_group(store, mode="r"):
            cls.write(store, [""] * size)
            return
        zarr.create_array(
            store,
            name=STAMP_ARRAY,
            shape=(size,),
            chunks=(chunk,),
            dtype="str",
            dimension_names=(append_dim,),
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
        meta_map = cast(Mapping[str, object], meta)
        urs = np.asarray(zarr.open_array(store, path=MANIFEST_ARRAYS[0])[:])
        urls = np.asarray(zarr.open_array(store, path=MANIFEST_ARRAYS[1])[:])
        return BackfillInventory.model_validate(
            dict(meta_map)
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
        return tuple(
            GranuleEntry.model_validate(item) for item in cast(Sequence[object], raw)
        )

    @staticmethod
    def depth(store: Store) -> int:
        """Entry count from the raw attribute. The attribute is still read
        and parsed wholesale, but skipping :meth:`read`'s per-entry pydantic
        validation keeps the consumer's PendingLedgerDepth metric cheap as
        the ledger grows (the exact condition the metric exists to detect)."""
        raw = zarr.open_group(store, mode="r").attrs.get(PENDING_LEDGER_ATTRIBUTE, [])
        return len(cast(Sequence[object], raw))

    @staticmethod
    def write(store: Store, entries: Iterable[GranuleEntry]) -> None:
        group = zarr.open_group(store, mode="a")
        # The optional stamp is a datetime and zarr attrs are JSON, hence
        # mode="json".
        group.attrs[PENDING_LEDGER_ATTRIBUTE] = [
            e.model_dump(mode="json") for e in entries
        ]

    @classmethod
    def append(cls, store: Store, entries: Iterable[GranuleEntry]) -> None:
        """Append new entries; a redelivered UR replaces its stale entry
        in place (last delivery wins) rather than being dropped, so a
        republication with a corrected time or url can't leave a stale
        entry to crash-loop the resort fold."""
        existing = list(cls.read(store))
        by_ur = {entry.granule_ur: i for i, entry in enumerate(existing)}
        for entry in entries:
            index = by_ur.get(entry.granule_ur)
            if index is None:
                by_ur[entry.granule_ur] = len(existing)
                existing.append(entry)
            else:
                existing[index] = entry
        cls.write(store, existing)
