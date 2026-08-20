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
    template_file: str
    coordinates_file: str


def _resource(filename: str) -> Traversable:
    return files("virtualizarr_processor") / "collections" / filename


def load_collection(name: str | None = None) -> CollectionConfig:
    """Load a collection config by name, or from ``$TEMPO_COLLECTION``."""
    if name is None:
        name = os.environ.get("TEMPO_COLLECTION")
        if not name:
            raise ValueError("No collection name given and TEMPO_COLLECTION is not set")
    resource = _resource(f"{name}.toml")
    if not resource.is_file():
        raise ValueError(f"Unknown collection {name!r}: no {name}.toml packaged")
    data = tomllib.loads(resource.read_text())
    extra = data.pop("extra_volatile_attributes", [])
    return CollectionConfig(
        volatile_attributes=TEMPO_L3_VOLATILE_ATTRIBUTES | frozenset(extra), **data
    )


def load_template(config: CollectionConfig) -> AnyGroupSpec:
    """The collection's committed store template (single-granule shape)."""
    resource = _resource(config.template_file)
    if not resource.is_file():
        raise FileNotFoundError(
            f"Template artifact {config.template_file} is not packaged; "
            "run scripts/generate_template.py"
        )
    return GroupSpec.model_validate_json(resource.read_text())


def load_coordinates(config: CollectionConfig) -> dict[str, np.ndarray]:
    """The committed reference coordinate arrays (bit-exact grid)."""
    resource = _resource(config.coordinates_file)
    if not resource.is_file():
        raise FileNotFoundError(
            f"Coordinates artifact {config.coordinates_file} is not packaged; "
            "run scripts/generate_template.py"
        )
    with resource.open("rb") as f:
        with np.load(f) as data:
            return {name: data[name] for name in data.files}
