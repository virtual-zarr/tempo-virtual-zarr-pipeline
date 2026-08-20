"""Declarative per-collection configuration.

Each supported collection is described by a TOML file in
``virtualizarr_processor/collections/`` and two generated artifacts next to
it: the pydantic-zarr store template (JSON) and the reference coordinate
arrays (npz), both produced by ``scripts/generate_template.py``. The
deployed instance selects its collection with ``$TEMPO_COLLECTION`` (one
Icechunk repository per collection).
"""

from __future__ import annotations

import os
import tomllib
from importlib.resources import files
from importlib.resources.abc import Traversable
from pathlib import Path

import numpy as np
from pydantic import BaseModel
from pydantic_zarr.v3 import GroupSpec

from virtualizarr_processor.store_template import (
    TEMPO_L3_VOLATILE_ATTRIBUTES,
    AnyGroupSpec,
)


class CollectionConfig(BaseModel, frozen=True):
    """A TEMPO collection as the pipeline sees it, loaded from TOML."""

    name: str
    collection_shortname: str
    concept_id: str
    append_dim: str
    time_units: str
    flatten_groups: tuple[str, ...]
    promote_to_time: tuple[str, ...]
    drop_variables: tuple[str, ...]
    volatile_attributes: frozenset[str]
    # Chunk size for the native time axis. A template generated from a
    # single-granule store would otherwise chunk time as (1,), which after
    # resize means one tiny chunk per scan for every reader to fetch.
    time_chunk_size: int
    template_file: str
    coordinates_file: str


def _resource(filename: str) -> Traversable:
    return files("virtualizarr_processor") / "collections" / filename


def load_collection(name: str | None = None) -> CollectionConfig:
    """Load a collection config by name, or from ``$TEMPO_COLLECTION``.

    A value ending in ``.toml`` is read as a filesystem path instead of a
    packaged collection name (used by tests and ad-hoc deployments; such a
    config typically names its template/coordinates artifacts by absolute
    path too).
    """
    if name is None:
        name = os.environ.get("TEMPO_COLLECTION")
        if not name:
            raise ValueError("No collection name given and TEMPO_COLLECTION is not set")
    if name.endswith(".toml"):
        data = tomllib.loads(Path(name).read_text())
    else:
        resource = _resource(f"{name}.toml")
        if not resource.is_file():
            raise ValueError(f"Unknown collection {name!r}: no {name}.toml packaged")
        data = tomllib.loads(resource.read_text())
    extra = data.pop("extra_volatile_attributes", [])
    return CollectionConfig(
        volatile_attributes=TEMPO_L3_VOLATILE_ATTRIBUTES | frozenset(extra), **data
    )


def _artifact(filename: str, kind: str) -> Traversable:
    """A packaged artifact, or a filesystem one when named by absolute path."""
    resource: Traversable = (
        Path(filename) if Path(filename).is_absolute() else _resource(filename)
    )
    if not resource.is_file():
        raise FileNotFoundError(
            f"{kind} artifact {filename} is not available; "
            "run scripts/generate_template.py"
        )
    return resource


def load_template(config: CollectionConfig) -> AnyGroupSpec:
    """The collection's committed store template (single-granule shape)."""
    return GroupSpec.model_validate_json(
        _artifact(config.template_file, "Template").read_text()
    )


def load_coordinates(config: CollectionConfig) -> dict[str, np.ndarray]:
    """The committed reference coordinate arrays (bit-exact grid)."""
    with _artifact(config.coordinates_file, "Coordinates").open("rb") as f:
        with np.load(f) as data:
            return {name: data[name] for name in data.files}
