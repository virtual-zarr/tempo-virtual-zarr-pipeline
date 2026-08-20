"""Parse one TEMPO L3 granule into the flat virtual dataset the store uses.

``open_flat_granule`` applies the transform declared by the collection
config: virtualize with VirtualiZarr's ``HDFParser``, flatten the
configured groups' variables to the root group, drop configured
variables, and promote per-scan variables stored without a time dimension
(``weight``). A data variable that still lacks the append dimension
afterwards is an error, because concatenating it would silently keep a
single scan's values.

``granule_time`` returns the granule's exact axis value, cross-checked
against the file's ``time_coverage_start_since_epoch`` attribute so an
internally inconsistent file cannot define its own axis position.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from functools import lru_cache
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import numpy as np
import obstore
import xarray as xr
from obspec_utils.registry import ObjectStoreRegistry
from obstore.store import HTTPStore, LocalStore, S3Store, from_url

if TYPE_CHECKING:  # ClientConfig is a stub-only TypedDict in obstore
    from obstore.auth.earthdata import NasaEarthdataCredentialProvider
    from obstore.store import ClientConfig
from virtualizarr.parsers.hdf import HDFParser

from virtualizarr_processor.collection import CollectionConfig
from virtualizarr_processor.store_template import GranuleValidationError

EPOCH_ATTRIBUTE = "time_coverage_start_since_epoch"

# Buckets whose reads are authorized with temporary credentials from the
# DAAC's ``s3credentials`` endpoint (the flow readers of the virtual store
# use too). $EARTHDATA_S3_CREDENTIALS_ENDPOINT overrides for other buckets.
S3_CREDENTIALS_ENDPOINTS = {
    "asdc-prod-protected": "https://data.asdc.earthdata.nasa.gov/s3credentials",
    "asdc2-prod-protected": "https://data.asdc.earthdata.nasa.gov/s3credentials",
}


@lru_cache(maxsize=1)
def _earthdata_auth() -> str | tuple[str, str] | None:
    """Resolve Earthdata Login material, or None to use ambient IAM.

    Sources, in order: ``$EARTHDATA_TOKEN``, ``$EARTHDATA_USERNAME`` and
    ``$EARTHDATA_PASSWORD``, then the Secrets Manager secret at
    ``$EARTHDATA_SECRET_ARN`` (JSON with ``token`` or
    ``username``+``password``, or a plain token string).
    """
    token = os.environ.get("EARTHDATA_TOKEN")
    if token:
        return token
    username = os.environ.get("EARTHDATA_USERNAME")
    password = os.environ.get("EARTHDATA_PASSWORD")
    if username and password:
        return (username, password)
    arn = os.environ.get("EARTHDATA_SECRET_ARN")
    if not arn:
        return None
    import boto3  # deferred: provided by the Lambda runtime / dev deps

    secret = boto3.client("secretsmanager").get_secret_value(SecretId=arn)[
        "SecretString"
    ]
    try:
        data = json.loads(secret)
    except ValueError:
        return str(secret)  # a plain token string
    if not isinstance(data, dict):
        return str(secret)
    if data.get("token"):
        return str(data["token"])
    return (str(data["username"]), str(data["password"]))


@lru_cache(maxsize=None)
def _s3_credential_provider(bucket: str) -> "NasaEarthdataCredentialProvider | None":
    """An EDL-refreshing credential provider for ``bucket``, or None.

    None when no EDL material is configured or the bucket has no known
    ``s3credentials`` endpoint; such reads use ambient AWS credentials.
    Cached so a warm Lambda exchanges credentials once per expiry window.
    """
    auth = _earthdata_auth()
    if auth is None:
        return None
    endpoint = os.environ.get(
        "EARTHDATA_S3_CREDENTIALS_ENDPOINT"
    ) or S3_CREDENTIALS_ENDPOINTS.get(bucket)
    if endpoint is None:
        return None
    from obstore.auth.earthdata import NasaEarthdataCredentialProvider

    return NasaEarthdataCredentialProvider(endpoint, auth=auth)


def make_registry(url: str) -> ObjectStoreRegistry:
    """Build an object-store registry that can resolve ``url``.

    Supports ``file://`` (tests, the template generator); ``s3://`` with
    temporary Earthdata credentials when EDL material is configured (see
    :func:`_earthdata_auth`), otherwise ambient AWS credentials; and
    ``https://`` with an EDL bearer-token header (token material only).
    """
    parsed = urlparse(url)
    if parsed.scheme == "file":
        return ObjectStoreRegistry({"file://": LocalStore()})
    if parsed.scheme == "s3":
        base = f"s3://{parsed.netloc}"
        provider = _s3_credential_provider(parsed.netloc)
        if provider is not None:
            return ObjectStoreRegistry(
                {base: S3Store.from_url(base, credential_provider=provider)}
            )
        return ObjectStoreRegistry({base: from_url(base)})
    if parsed.scheme == "https":
        base = f"https://{parsed.netloc}"
        auth = _earthdata_auth()
        client_options: ClientConfig | None = (
            {"default_headers": {"Authorization": f"Bearer {auth}"}}
            if isinstance(auth, str)
            else None
        )
        return ObjectStoreRegistry(
            {base: HTTPStore.from_url(base, client_options=client_options)}
        )
    raise ValueError(
        f"Unsupported url scheme {parsed.scheme!r} for {url!r}; "
        "expected file://, s3://, or https://"
    )


def source_last_modified(
    url: str, registry: ObjectStoreRegistry | None = None
) -> datetime:
    """Return the source object's last-modified time as observed now.

    Used as the ``last_updated_at`` checksum on the object's virtual
    references, so a later overwrite of the object makes reads of the
    stale references fail. Using the object's own modification time keeps
    the stamp independent of the worker's clock.
    """
    registry = registry or make_registry(url)
    store, path = registry.resolve(url)
    return obstore.head(store, path)["last_modified"]


def open_flat_granule(
    url: str,
    config: CollectionConfig,
    registry: ObjectStoreRegistry | None = None,
) -> xr.Dataset:
    """Virtualize ``url`` and apply the config's flatten/drop/promote."""
    registry = registry or make_registry(url)
    # decode_times=False: the pipeline works in raw float64 seconds
    # throughout, so time values must not round-trip through datetime64.
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
    """Return the granule's exact time-axis value after integrity checks."""
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
                f"equal /time[0] ({time_value!r}); the granule is internally "
                "inconsistent"
            ]
        )
    return time_value
