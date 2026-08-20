"""The typed backfill inventory: the file enumerating granules to backfill.

The inventory is a JSON document produced by
``exploration/build_backfill_inventory.py`` and consumed by the backfill
Init and Partition steps. Each entry carries the granule's **exact**
``/time[0]`` value as read from the file (equal to the granule's
``time_coverage_start_since_epoch`` attribute) — not the CMR/filename
timestamp, which differs from it. Init builds the store's time axis from
these values; workers then align each granule against that axis with
``to_icechunk(region="auto")``, so a granule whose in-file time is not in
the inventory fails loudly instead of landing in the wrong slot.

Validation here is what makes the fork/merge backfill safe: merge is
last-writer-wins, so two granules on one time step would corrupt the store
silently. The model rejects that (and unsorted or empty inventories) at
parse time, on both the build and the consume side.
"""

from __future__ import annotations

from collections import Counter
from typing import Literal

import numpy as np
from pydantic import BaseModel, Field, field_validator, model_validator

SCHEMA_ID = "tempo-backfill-inventory/1"


class GranuleEntry(BaseModel, frozen=True):
    """One granule to backfill: where it is and where it goes on the axis."""

    url: str
    granule_ur: str
    time: float  # exact float64 /time[0], seconds since the TEMPO epoch

    @field_validator("url")
    @classmethod
    def _url_is_netcdf(cls, value: str) -> str:
        if not value.endswith(".nc"):
            raise ValueError(f"granule url must end in .nc: {value!r}")
        return value


class BackfillInventory(BaseModel, frozen=True):
    """A validated, chronologically ordered enumeration of granules."""

    schema_id: Literal["tempo-backfill-inventory/1"] = Field(alias="schema")
    collection: str
    concept_id: str
    time_units: str
    built_at: str
    granules: tuple[GranuleEntry, ...]

    model_config = {"populate_by_name": True}

    @model_validator(mode="after")
    def _granules_form_a_valid_axis(self) -> BackfillInventory:
        if not self.granules:
            raise ValueError("inventory must contain at least one granule")
        times = [g.time for g in self.granules]
        if any(b <= a for a, b in zip(times, times[1:])):
            raise ValueError(
                "granule times must be strictly increasing (each granule "
                "must own a distinct time step; merge is last-writer-wins)"
            )
        counts = Counter(g.granule_ur for g in self.granules)
        duplicates = {ur for ur, n in counts.items() if n > 1}
        if duplicates:
            raise ValueError(f"duplicate granule_ur entries: {sorted(duplicates)}")
        return self

    @classmethod
    def from_json(cls, data: bytes | str) -> BackfillInventory:
        return cls.model_validate_json(data)

    def to_json(self) -> str:
        return self.model_dump_json(by_alias=True, indent=1)

    def times(self) -> np.ndarray:
        """The store's time axis values, in order."""
        return np.array([g.time for g in self.granules], dtype=np.float64)

    def urls(self) -> list[str]:
        """The file keys in axis order (what partition manifests contain)."""
        return [g.url for g in self.granules]
