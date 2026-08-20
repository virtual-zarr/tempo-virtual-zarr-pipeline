"""Parse one TEMPO L3 granule into the flat virtual dataset the store uses.

``open_flat_granule`` runs the whole ingest transform declared by the
collection config: virtualize with VirtualiZarr's ``HDFParser``, flatten
the configured groups' variables to the root group (the layout proven with
titiler-multidim; name collisions are an error), drop configured
variables, and promote per-scan variables stored without a time dimension
(``weight``) so concatenation cannot silently freeze one scan's values.
Any data variable that still lacks the append dimension afterwards is a
hard error for the same reason.

``granule_time`` returns the granule's exact axis value and cross-checks
it against the file's own ``time_coverage_start_since_epoch`` attribute
(bit-equal in real TEMPO files) so an internally inconsistent file cannot
define its own position on the axis.
"""

from __future__ import annotations

import os
from urllib.parse import urlparse

import numpy as np
import xarray as xr
from obspec_utils.registry import ObjectStoreRegistry
from obstore.store import ClientConfig, HTTPStore, LocalStore, from_url
from virtualizarr.parsers.hdf import HDFParser

from virtualizarr_processor.collection import CollectionConfig
from virtualizarr_processor.store_template import GranuleValidationError

EPOCH_ATTRIBUTE = "time_coverage_start_since_epoch"


def make_registry(url: str) -> ObjectStoreRegistry:
    """An object-store registry able to resolve ``url``.

    ``file://`` for local granules (tests, the template generator),
    ``s3://`` for in-region production access (credentials from the AWS
    environment), and ``https://`` with an ``$EARTHDATA_TOKEN`` bearer
    header for EDL-authed access.
    """
    parsed = urlparse(url)
    if parsed.scheme == "file":
        return ObjectStoreRegistry({"file://": LocalStore()})
    if parsed.scheme == "s3":
        base = f"s3://{parsed.netloc}"
        return ObjectStoreRegistry({base: from_url(base)})
    if parsed.scheme == "https":
        base = f"https://{parsed.netloc}"
        token = os.environ.get("EARTHDATA_TOKEN")
        client_options: ClientConfig | None = (
            {"default_headers": {"Authorization": f"Bearer {token}"}} if token else None
        )
        return ObjectStoreRegistry(
            {base: HTTPStore.from_url(base, client_options=client_options)}
        )
    raise ValueError(
        f"Unsupported url scheme {parsed.scheme!r} for {url!r}; "
        "expected file://, s3://, or https://"
    )


def open_flat_granule(
    url: str,
    config: CollectionConfig,
    registry: ObjectStoreRegistry | None = None,
) -> xr.Dataset:
    """Virtualize ``url`` and apply the config's flatten/drop/promote."""
    registry = registry or make_registry(url)
    # decode_times=False: the pipeline is raw float64 seconds end to end —
    # the store axis, the inventory, and region alignment all use the
    # files' exact values, so nothing may round-trip through datetime64.
    tree = HDFParser()(url=url, registry=registry).to_virtual_datatree(
        decode_times=False
    )

    root = tree.to_dataset(inherit=False)
    missing = [g for g in config.flatten_groups if g not in tree.children]
    if missing:
        raise GranuleValidationError(
            [f"group {name!r} missing from granule" for name in missing]
        )

    variables: dict[str, xr.Variable] = {
        str(name): array.variable for name, array in root.data_vars.items()
    }
    collisions = []
    for group in config.flatten_groups:
        for name, array in (
            tree.children[group].to_dataset(inherit=False).data_vars.items()
        ):
            if str(name) in variables or name in root.coords:
                collisions.append(
                    f"variable {name!r} in group {group!r} collides with "
                    "another variable when flattened to the root group"
                )
            else:
                variables[str(name)] = array.variable
    if collisions:
        raise GranuleValidationError(collisions)

    flat = xr.Dataset(variables, coords=root.coords, attrs=dict(tree.attrs))

    unknown = set(config.drop_variables) - set(map(str, flat.data_vars))
    if unknown:
        raise GranuleValidationError(
            [
                f"drop_variables entry {name!r} not in granule"
                for name in sorted(unknown)
            ]
        )
    flat = flat.drop_vars(list(config.drop_variables))

    for name in config.promote_to_time:
        if name not in flat.data_vars:
            raise GranuleValidationError(
                [f"promote_to_time variable {name!r} missing from granule"]
            )
        variable = flat[name].variable
        if config.append_dim in variable.dims:
            continue
        # np.expand_dims dispatches to ManifestArray, so virtual stays virtual.
        flat[name] = xr.Variable(
            (config.append_dim, *variable.dims),
            np.expand_dims(variable.data, axis=0),
            attrs=variable.attrs,
            encoding=variable.encoding,
        )

    stuck = [
        str(name)
        for name, array in flat.data_vars.items()
        if config.append_dim not in array.dims
    ]
    if stuck:
        raise GranuleValidationError(
            [
                f"variable {name!r} has no {config.append_dim!r} dimension and "
                "is not promoted or dropped; concatenation would silently keep "
                "a single scan's values — add it to promote_to_time or "
                "drop_variables"
                for name in stuck
            ]
        )
    return flat


def granule_time(vds: xr.Dataset) -> float:
    """The granule's exact time-axis value, integrity-checked (spec V3)."""
    if "time" not in vds.variables:
        raise GranuleValidationError(["granule has no 'time' variable"])
    values = np.asarray(vds["time"].values)
    if values.shape != (1,):
        raise GranuleValidationError(
            [f"granule must carry exactly one time step, found shape {values.shape}"]
        )
    time_value = float(values[0])
    attr = vds.attrs.get(EPOCH_ATTRIBUTE)
    if attr is None:
        raise GranuleValidationError(
            [f"root attribute {EPOCH_ATTRIBUTE!r} missing from granule"]
        )
    attr_value = float(np.asarray(attr).ravel()[0])
    if attr_value != time_value:
        raise GranuleValidationError(
            [
                f"root attribute {EPOCH_ATTRIBUTE!r} ({attr_value!r}) does not "
                f"equal /time[0] ({time_value!r}); refusing an internally "
                "inconsistent granule"
            ]
        )
    return time_value
