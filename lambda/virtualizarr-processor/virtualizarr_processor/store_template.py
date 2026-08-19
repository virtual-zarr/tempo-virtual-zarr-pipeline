"""Declarative Icechunk/Zarr store templates.

A store schema is declared once as a ``pydantic_zarr.v3.GroupSpec`` and can
then be:

- materialized as an empty store with :func:`create_empty_store` (metadata
  only — no chunk data is written, which is exactly what the backfill
  pipeline needs before workers region-write virtual references),
- derived at full shape from a single-granule spec with :func:`resize`,
- checked against an actual store with :func:`validate_store`, which reports
  every missing node, unexpected node, and mismatched metadata field.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

import zarr
from pydantic_zarr.v3 import ArraySpec, GroupSpec
from zarr.abc.store import Store

AnyGroupSpec = GroupSpec[Any, Any]

# Zarr v3 JSON spells non-finite floats as strings; a spec author may use
# either representation, and zarr itself is inconsistent across versions.
_NON_FINITE = {"NaN": math.nan, "Infinity": math.inf, "-Infinity": -math.inf}


class StoreValidationError(Exception):
    """A store does not conform to its template.

    ``differences`` holds one human-readable line per divergence.
    """

    def __init__(self, differences: list[str]) -> None:
        self.differences = differences
        super().__init__("Store does not match template:\n" + "\n".join(differences))


def create_empty_store(
    spec: AnyGroupSpec, store: Store, *, path: str = ""
) -> zarr.Group:
    """Materialize ``spec`` as a metadata-only hierarchy in ``store``.

    Only group/array metadata documents are written — no chunk data — so the
    result is an empty store ready for region writes. Groups declared by the
    template may already exist (their non-template members are left alone,
    which lets a template land on a branch that inherited nodes from
    ``main``), but a pre-existing node at any template array path is an
    error.
    """
    for subpath, node in sorted(spec.to_flat().items()):
        node_path = f"{path.rstrip('/')}{subpath}".lstrip("/")
        existing = _maybe_node(store, node_path)
        if isinstance(node, ArraySpec):
            if existing is not None:
                raise ValueError(
                    f"cannot create template array at {subpath or '/'}: "
                    "a node already exists there"
                )
            node.to_zarr(store, node_path)
        elif isinstance(existing, zarr.Array):
            raise ValueError(
                f"cannot create template group at {subpath or '/'}: "
                "an array already exists there"
            )
        elif existing is None:
            group = zarr.create_group(store=store, path=node_path, zarr_format=3)
            group.attrs.put(node.model_dump()["attributes"])
        elif node.attributes:
            existing.attrs.put(node.model_dump()["attributes"])
    return zarr.open_group(store, path=path, mode="r")


def _maybe_node(store: Store, path: str) -> zarr.Array | zarr.Group | None:
    try:
        return zarr.open(store=store, path=path, mode="r", zarr_format=3)
    except FileNotFoundError:
        return None


def resize(spec: AnyGroupSpec, sizes: Mapping[str, int]) -> AnyGroupSpec:
    """Return a copy of ``spec`` with named dimensions resized.

    Every array whose ``dimension_names`` include a key of ``sizes`` gets
    that axis set to the new size; chunk shapes and all other metadata are
    preserved. This turns a spec captured from a single-granule store into a
    full-shape template (e.g. ``resize(spec, {"time": n_granules})``).

    Raises ``ValueError`` if a named dimension appears on no array.
    """
    unseen = set(sizes)
    flat: dict[str, Any] = {}
    for path, node in spec.to_flat().items():
        if isinstance(node, ArraySpec) and node.dimension_names is not None:
            shape = tuple(
                sizes.get(dim, extent) if dim is not None else extent
                for dim, extent in zip(node.dimension_names, node.shape)
            )
            unseen -= set(node.dimension_names)
            node = node.model_copy(update={"shape": shape})
        flat[path] = node
    if unseen:
        raise ValueError(
            f"Dimension(s) {sorted(unseen)} appear on no array in the template"
        )
    return GroupSpec.from_flat(flat)


def validate_store(
    spec: AnyGroupSpec, group: zarr.Group, *, allow_extra: bool = False
) -> None:
    """Check that ``group`` conforms to the template ``spec``.

    Raises :class:`StoreValidationError` listing every missing node,
    unexpected node, and mismatched metadata field (NaN fill values compare
    equal). Returns ``None`` when the store matches. With ``allow_extra``
    the store may contain nodes the template does not declare — useful when
    the template landed on a branch that inherited other nodes.
    """
    expected = {p: node.model_dump() for p, node in spec.to_flat().items()}
    found = {
        p: node.model_dump() for p, node in GroupSpec.from_zarr(group).to_flat().items()
    }

    differences = [
        f"{p or '/'}: missing from store" for p in sorted(expected - found.keys())
    ]
    if not allow_extra:
        differences += [
            f"{p or '/'}: unexpected node in store"
            for p in sorted(found - expected.keys())
        ]
    for path in sorted(expected.keys() & found.keys()):
        for field in sorted(expected[path].keys() | found[path].keys()):
            want = expected[path].get(field)
            got = found[path].get(field)
            if not _values_equal(want, got):
                differences.append(
                    f"{path or '/'}: {field} expected {want!r}, found {got!r}"
                )
    if differences:
        raise StoreValidationError(differences)


def _values_equal(a: object, b: object) -> bool:
    """Structural equality that treats non-finite floats as equal to both
    their float and their zarr-v3 string spelling."""
    a = _NON_FINITE.get(a, a) if isinstance(a, str) else a
    b = _NON_FINITE.get(b, b) if isinstance(b, str) else b
    if isinstance(a, float) and isinstance(b, float):
        return (math.isnan(a) and math.isnan(b)) or a == b
    if isinstance(a, Mapping) and isinstance(b, Mapping):
        return a.keys() == b.keys() and all(
            _values_equal(v, b[k]) for k, v in a.items()
        )
    if isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
        return len(a) == len(b) and all(map(_values_equal, a, b))
    return bool(a == b)
