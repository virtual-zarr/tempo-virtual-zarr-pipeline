"""Build a collection's store template from reference granules.

Used by ``scripts/generate_template.py`` to produce the committed
artifacts, and by tests to build tiny templates from synthetic granules.

The template is generated **through the actual ingest path**: each granule
is virtualized and transformed by ``open_flat_granule``, the first one is
written to a throwaway in-memory Icechunk store with ``to_icechunk`` (so
the captured dtypes/chunks/codecs are exactly what backfill workers will
region-write into), and the spec is read back with ``GroupSpec.from_zarr``
and stripped of volatile and write-artifact attributes. Every remaining
granule is then validated against the candidate template: an attribute
that varies and is not declared volatile, or a coordinate grid that
differs, is a hard error — divergence must be classified explicitly in the
collection config, never absorbed silently.
"""

from __future__ import annotations

from pathlib import Path

import icechunk
import numpy as np
import zarr
from pydantic_zarr.v3 import GroupSpec

from virtualizarr_processor.collection import CollectionConfig
from virtualizarr_processor.granule import open_flat_granule
from virtualizarr_processor.store_template import (
    WRITE_ARTIFACT_ATTRIBUTES,
    AnyGroupSpec,
    strip_attributes,
    validate_granule,
)


def build_template(
    paths: list[Path], config: CollectionConfig
) -> tuple[AnyGroupSpec, dict[str, np.ndarray]]:
    """The collection's store template and reference coordinates.

    Raises :class:`GranuleValidationError` when the granules disagree on
    anything not declared volatile.
    """
    if not paths:
        raise ValueError("at least one reference granule is required")
    granules = [open_flat_granule(f"file://{path.resolve()}", config) for path in paths]

    reference = granules[0]
    coordinates = {
        "latitude": np.asarray(reference["latitude"].values),
        "longitude": np.asarray(reference["longitude"].values),
    }

    # Write the reference granule through the real write path, then read the
    # schema back, so the template is exactly what to_icechunk produces.
    repo = icechunk.Repository.create(storage=icechunk.in_memory_storage())
    session = repo.writable_session("main")
    reference.vz.to_icechunk(session.store, validate_containers=False)
    spec: AnyGroupSpec = GroupSpec.from_zarr(zarr.open_group(session.store, mode="r"))
    spec = strip_attributes(
        spec, config.volatile_attributes | WRITE_ARTIFACT_ATTRIBUTES
    )

    # Override the axis chunking: a one-granule store chunks time (1,), which
    # after resize would mean one tiny chunk per scan.
    flat = spec.to_flat()
    flat["/time"] = flat["/time"].model_copy(
        update={
            "chunk_grid": {
                "name": "regular",
                "configuration": {"chunk_shape": (config.time_chunk_size,)},
            }
        }
    )
    spec = GroupSpec.from_flat(flat)

    for granule in granules[1:]:
        validate_granule(
            spec,
            granule,
            coordinates=coordinates,
            volatile=config.volatile_attributes | WRITE_ARTIFACT_ATTRIBUTES,
        )
    return spec, coordinates
