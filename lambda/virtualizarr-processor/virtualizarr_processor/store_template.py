"""Declarative Icechunk/Zarr store templates.

A store schema is declared once as a ``pydantic_zarr.v3.GroupSpec`` —
structure plus the attributes shared by every granule (strip the per-granule
volatile ones with :func:`strip_attributes`) — and can then be:

- materialized as an empty store with :func:`create_empty_store` (metadata
  only — no chunk data is written, which is exactly what the backfill
  pipeline needs before workers region-write virtual references),
- derived at full shape from a single-granule spec with :func:`resize`,
- checked against an actual store with :func:`validate_store`, which reports
  every missing node, unexpected node, and mismatched metadata field,
- checked against each input granule with :func:`validate_granule`, which
  raises when spatial coordinates or expected shared attributes differ.

Both validators raise on divergence from what the template declares, but
attributes the template does not mention only produce a warning — emitted on
this module's logger and as an event on the current OpenTelemetry span (a
no-op unless an OTEL SDK is configured, e.g. by a Lambda OTEL layer).
"""

from __future__ import annotations

import logging
import math
from collections.abc import Collection, Iterable, Mapping
from typing import Any

import numpy as np
import xarray as xr
import zarr
from opentelemetry import trace
from pydantic_zarr.v3 import ArraySpec, GroupSpec
from zarr.abc.store import Store

logger = logging.getLogger(__name__)

AnyGroupSpec = GroupSpec[Any, Any]

# Zarr v3 JSON spells non-finite floats as strings; a spec author may use
# either representation, and zarr itself is inconsistent across versions.
_NON_FINITE = {"NaN": math.nan, "Infinity": math.inf, "-Infinity": -math.inf}

# Attributes that vary per TEMPO L3 granule and carry no shared structural
# information, established by profiling recent granules of both collections
# (see the granule comparison findings in the README / context/compare.py).
# Strip these when deriving a shared-attribute template from a reference
# granule, and pass them to validate_granule so they are neither required
# nor warned about.
TEMPO_L3_VOLATILE_ATTRIBUTES: frozenset[str] = frozenset(
    {
        "history",
        "scan_num",
        "time_coverage_start",
        "time_coverage_end",
        "time_coverage_start_since_epoch",
        "time_coverage_end_since_epoch",
        "production_date_time",
        "begin_date",
        "begin_time",
        "end_date",
        "end_time",
        "local_granule_id",
        "input_files",
        "geospatial_bounds",
        "day_of_year",
        "coremetadata",
        "REFERENCE_LIST",
        "DIMENSION_LIST",
    }
)


# Attributes that xarray's write path adds on the store side (a root
# "coordinates" listing; _FillValue re-emitted from decode encoding on
# loadable variables). Granules never carry them in .attrs, so templates
# strip them and granule validation treats them as expected extras.
# Array-level fill values are still checked via the zarr metadata
# ``fill_value`` field.
WRITE_ARTIFACT_ATTRIBUTES: frozenset[str] = frozenset({"coordinates", "_FillValue"})


class StoreValidationError(Exception):
    """A store does not conform to its template.

    ``differences`` holds one human-readable line per divergence.
    """

    header = "Store does not match template:"

    def __init__(self, differences: list[str]) -> None:
        self.differences = differences
        super().__init__(self.header + "\n" + "\n".join(differences))


class GranuleValidationError(StoreValidationError):
    """An input granule does not conform to the store template."""

    header = "Granule does not match template:"


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


def strip_attributes(spec: AnyGroupSpec, names: Collection[str]) -> AnyGroupSpec:
    """Return a copy of ``spec`` with the named attributes removed everywhere.

    Use with a per-granule volatile list (e.g.
    :data:`TEMPO_L3_VOLATILE_ATTRIBUTES`) to turn a reference-granule spec
    into a template that declares only the attributes shared by every
    granule.
    """
    flat: dict[str, Any] = {}
    for path, node in spec.to_flat().items():
        attributes = {k: v for k, v in dict(node.attributes).items() if k not in names}
        flat[path] = node.model_copy(update={"attributes": attributes})
    return GroupSpec.from_flat(flat)


def validate_store(
    spec: AnyGroupSpec, group: zarr.Group, *, allow_extra: bool = False
) -> None:
    """Check that ``group`` conforms to the template ``spec``.

    Raises :class:`StoreValidationError` listing every missing node,
    unexpected node, and mismatched metadata field (NaN fill values compare
    equal). Attributes follow the shared policy: every attribute the
    template declares must be present and equal (raise), while attributes it
    does not declare only produce a warning. Returns ``None`` when the store
    matches. With ``allow_extra`` the store may contain nodes the template
    does not declare — useful when the template landed on a branch that
    inherited other nodes.
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
            if field == "attributes":
                continue
            want = expected[path].get(field)
            got = found[path].get(field)
            if not _values_equal(want, got):
                differences.append(
                    f"{path or '/'}: {field} expected {want!r}, found {got!r}"
                )
        differences += _attribute_differences(
            path,
            expected[path].get("attributes") or {},
            found[path].get("attributes") or {},
            volatile=(),
            where="store",
        )
    if differences:
        raise StoreValidationError(differences)


def validate_granule(
    spec: AnyGroupSpec,
    granule: xr.Dataset | xr.DataTree,
    *,
    coordinates: Mapping[str, Any] | None = None,
    volatile: Collection[str] = (),
) -> None:
    """Check that an input granule conforms to the store template ``spec``.

    Raises :class:`GranuleValidationError` when any coordinate named in
    ``coordinates`` is missing from the granule or differs from the given
    reference values, or when an attribute the template declares (on a node
    the granule carries) is missing or differs. Attribute names in
    ``volatile`` (e.g. :data:`TEMPO_L3_VOLATILE_ATTRIBUTES`) are exempt per
    granule. Attributes the template does not declare only produce a
    warning — a granule is never rejected for carrying extras.
    """
    differences: list[str] = []
    for name, reference in (coordinates or {}).items():
        actual = _coordinate_values(granule, name)
        if actual is None:
            differences.append(f"coordinate {name!r}: missing from granule")
        elif not np.array_equal(np.asarray(reference), actual):
            differences.append(
                f"coordinate {name!r}: values differ from the template reference"
            )

    granule_attrs = _granule_attributes(granule)
    for path, node in spec.to_flat().items():
        if path not in granule_attrs:
            continue
        differences += _attribute_differences(
            path,
            dict(node.attributes),
            granule_attrs[path],
            volatile=volatile,
            where="granule",
        )
    if differences:
        raise GranuleValidationError(differences)


def _attribute_differences(
    path: str,
    expected: Mapping[str, Any],
    actual: Mapping[str, Any],
    *,
    volatile: Collection[str],
    where: str,
) -> list[str]:
    """Shared attribute policy: expected must match (differences returned),
    unexpected only warn."""
    differences = []
    for key, want in expected.items():
        if key in volatile:
            continue
        if key not in actual:
            differences.append(f"{path or '/'}: attribute {key!r} missing from {where}")
        elif not _values_equal(want, actual[key]):
            differences.append(
                f"{path or '/'}: attribute {key!r} expected {want!r}, "
                f"found {actual[key]!r}"
            )
    unexpected = set(actual) - set(expected) - set(volatile)
    if unexpected:
        _warn_unexpected_attributes(path, unexpected)
    return differences


def _warn_unexpected_attributes(path: str, names: Iterable[str]) -> None:
    """Warn on this module's logger and the current OTEL span (no-op span
    unless an OpenTelemetry SDK is configured)."""
    names_sorted = sorted(names)
    logger.warning(
        "Unexpected attributes at %s: %s", path or "/", ", ".join(names_sorted)
    )
    trace.get_current_span().add_event(
        "store_template.unexpected_attributes",
        {"path": path or "/", "attributes": names_sorted},
    )


def _coordinate_values(
    granule: xr.Dataset | xr.DataTree, name: str
) -> np.ndarray | None:
    dataset = (
        granule.to_dataset(inherit=False)
        if isinstance(granule, xr.DataTree)
        else granule
    )
    if name not in dataset.variables:
        return None
    return np.asarray(dataset.variables[name].values)


def _granule_attributes(
    granule: xr.Dataset | xr.DataTree,
) -> dict[str, Mapping[str, Any]]:
    """Flatten a granule to {template-style path: attributes} for its root,
    groups, and variables."""
    if isinstance(granule, xr.Dataset):
        nodes: dict[str, Mapping[str, Any]] = {"": granule.attrs}
        for name, variable in granule.variables.items():
            nodes[f"/{name}"] = variable.attrs
        return nodes
    nodes = {}
    for node in granule.subtree:
        base = "" if node.path == "/" else node.path
        dataset = node.to_dataset(inherit=False)
        nodes[base] = dataset.attrs
        for name, variable in dataset.variables.items():
            nodes[f"{base}/{name}"] = variable.attrs
    return nodes


def _values_equal(a: object, b: object) -> bool:
    """Structural equality that treats non-finite floats as equal to both
    their float and their zarr-v3 string spelling."""
    a = _NON_FINITE.get(a, a) if isinstance(a, str) else a
    b = _NON_FINITE.get(b, b) if isinstance(b, str) else b
    if isinstance(a, float) and isinstance(b, float):
        return (math.isnan(a) and math.isnan(b)) or a == b
    if isinstance(a, np.ndarray) or isinstance(b, np.ndarray):
        return bool(np.array_equal(np.asarray(a), np.asarray(b)))
    if isinstance(a, Mapping) and isinstance(b, Mapping):
        return a.keys() == b.keys() and all(
            _values_equal(v, b[k]) for k, v in a.items()
        )
    if isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
        return len(a) == len(b) and all(map(_values_equal, a, b))
    return bool(a == b)
