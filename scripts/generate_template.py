#!/usr/bin/env python3
"""Generate the committed per-collection store templates and coordinates.

Builds each collection's store template (pydantic-zarr ``GroupSpec`` JSON)
and the shared reference coordinate arrays (npz) from local reference
granules, via ``virtualizarr_processor.template.build_template``, and
writes them into ``virtualizarr_processor/collections/``.

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
import numpy as np
from virtualizarr_processor.collection import load_collection
from virtualizarr_processor.template import build_template

PACKAGE_COLLECTIONS_DIR = (
    Path(__file__).parent.parent
    / "lambda"
    / "virtualizarr-processor"
    / "virtualizarr_processor"
    / "collections"
)
DEFAULT_DATA_DIR = Path("/workspace/context/data")


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
