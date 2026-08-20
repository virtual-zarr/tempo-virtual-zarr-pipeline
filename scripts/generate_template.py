#!/usr/bin/env python3
"""Generate the committed per-collection store templates and coordinates.

Builds each collection's declarative store template (pydantic-zarr
``GroupSpec`` JSON) and the shared reference coordinate arrays (npz) from
local reference granules, and writes them into
``virtualizarr_processor/collections/``.

The template is generated **through the actual ingest path**: each granule
is virtualized and transformed by ``open_flat_granule``, the first one is
written to a throwaway in-memory Icechunk store with ``to_icechunk`` (so
the captured dtypes/chunks/codecs are exactly what backfill workers will
region-write into), and the spec is read back with ``GroupSpec.from_zarr``
and stripped of per-granule volatile attributes. Every remaining granule
is then validated against the candidate template: an attribute that varies
and is not declared volatile, or a coordinate grid that differs, is a hard
error — divergence must be classified explicitly in the collection config,
never absorbed silently.

Usage:
    uv run scripts/generate_template.py            # both collections
    uv run scripts/generate_template.py --collection hcho
    uv run scripts/generate_template.py --data-dir /workspace/context/data
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import h5py
import icechunk
import numpy as np
import zarr
from virtualizarr_processor.collection import CollectionConfig, load_collection
from virtualizarr_processor.granule import open_flat_granule
from virtualizarr_processor.store_template import (
    WRITE_ARTIFACT_ATTRIBUTES,
    AnyGroupSpec,
    strip_attributes,
    validate_granule,
)

try:
    from pydantic_zarr.v3 import GroupSpec
except ImportError:  # pragma: no cover
    raise SystemExit("pydantic-zarr is required; run via `uv run`")

PACKAGE_COLLECTIONS_DIR = (
    Path(__file__).parent.parent
    / "lambda"
    / "virtualizarr-processor"
    / "virtualizarr_processor"
    / "collections"
)
DEFAULT_DATA_DIR = Path("/workspace/context/data")


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
    spec = GroupSpec.from_zarr(zarr.open_group(session.store, mode="r"))
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


def collection_of(path: Path) -> str:
    with h5py.File(path) as f:
        raw = f.attrs["collection_shortname"]
        value = raw[0] if isinstance(raw, np.ndarray) else raw
        return value.decode() if isinstance(value, bytes) else str(value)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--collection", choices=["hcho", "no2"], action="append")
    parser.add_argument("--output-dir", type=Path, default=PACKAGE_COLLECTIONS_DIR)
    args = parser.parse_args()

    names = args.collection or ["hcho", "no2"]
    reference_coords: dict[str, np.ndarray] | None = None
    coordinates_file = None
    for name in names:
        config = load_collection(name)
        paths = sorted(
            path
            for path in args.data_dir.glob("*.nc")
            if collection_of(path) == config.collection_shortname
        )
        if not paths:
            raise SystemExit(
                f"No granules of {config.collection_shortname} in {args.data_dir}"
            )
        print(f"{config.collection_shortname}: {len(paths)} reference granules")
        spec, coords = build_template(paths, config)

        if reference_coords is None:
            reference_coords = coords
            coordinates_file = config.coordinates_file
        else:
            # The collections share one committed grid; verify, don't assume.
            for axis, values in coords.items():
                if not np.array_equal(reference_coords[axis], values):
                    raise SystemExit(
                        f"{axis} grid differs between collections; cannot "
                        "share a coordinates artifact"
                    )
            if config.coordinates_file != coordinates_file:
                raise SystemExit("collections declare different coordinates_file names")

        template_path = args.output_dir / config.template_file
        template_path.write_text(spec.model_dump_json(indent=1) + "\n")
        arrays = len(spec.to_flat()) - 1
        print(f"  wrote {template_path} ({arrays} nodes)")

    assert reference_coords is not None and coordinates_file is not None
    coords_path = args.output_dir / coordinates_file
    with coords_path.open("wb") as f:
        np.savez(f, **reference_coords)
    print(f"  wrote {coords_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
