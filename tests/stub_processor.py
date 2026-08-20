"""Synthetic reference processor (the template sample), kept as a test stub.

Exercises the generic backfill mechanics (fork/merge, region writes,
templates) without TEMPO parsing; moved out of the shipped package when the
TEMPO processor replaced it.
"""

import logging
import os
import tempfile
from copy import Error
from datetime import datetime
from itertools import islice
from typing import cast

import icechunk
import numpy as np
import obstore
import xarray as xr
import zarr
from icechunk import ForkSession, Repository, Session
from pydantic_zarr.v3 import ArraySpec, DefaultChunkKeyEncoding, GroupSpec
from virtualizarr.manifests import ChunkManifest, ManifestArray
from virtualizarr_processor.store_template import (
    create_empty_store,
    validate_granule,
    validate_store,
)
from virtualizarr_processor.typing import BranchInit
from zarr.codecs import BytesCodec
from zarr.core.dtype import parse_data_type
from zarr.core.metadata import ArrayV3Metadata

logger = logging.getLogger(__name__)

CHUNK_DIR = os.path.realpath(tempfile.gettempdir())
CHUNK_DIRECTORY_URL_PREFIX = f"file://{CHUNK_DIR}/"

# Backfill synthetic dataset: N time steps, each a (Y, X) int32 chunk.
BACKFILL_N, BACKFILL_Y, BACKFILL_X = 6, 2, 3
BACKFILL_DTYPE = np.dtype("int32")


def _template_array(
    shape: tuple[int, ...],
    chunks: tuple[int, ...],
    dtype: str,
    dims: tuple[str, ...],
) -> ArraySpec:
    """An uncompressed little-endian array spec, matching what the synthetic
    backfill worker expects when it decodes raw chunk bytes."""
    return ArraySpec(
        attributes={},
        shape=shape,
        data_type=dtype,
        chunk_grid={"name": "regular", "configuration": {"chunk_shape": chunks}},
        chunk_key_encoding=DefaultChunkKeyEncoding(
            name="default", configuration={"separator": "/"}
        ),
        fill_value=0,
        codecs=({"name": "bytes", "configuration": {"endian": "little"}},),
        dimension_names=dims,
    )


# The full-shape schema of the synthetic backfill store. Declared once so the
# store can be created empty (metadata only) and later checked against it.
BACKFILL_TEMPLATE: GroupSpec = GroupSpec.from_flat(
    {
        "": GroupSpec(attributes={}, members=None),
        "/foo": _template_array(
            (BACKFILL_N, BACKFILL_Y, BACKFILL_X),
            (1, BACKFILL_Y, BACKFILL_X),
            str(BACKFILL_DTYPE),
            ("time", "y", "x"),
        ),
        "/time": _template_array((BACKFILL_N,), (BACKFILL_N,), "int64", ("time",)),
    }
)


def synthetic_vds(date: str) -> xr.Dataset:
    filepath = f"{CHUNK_DIR}/data_chunk"
    store = obstore.store.LocalStore()
    arr = np.repeat([[1, 2]], 3, axis=1)
    shape = arr.shape
    dtype = arr.dtype
    buf = arr.tobytes()
    obstore.put(
        store,
        filepath,
        buf,
    )
    manifest = ChunkManifest(
        {"0.0": {"path": filepath, "offset": 0, "length": len(buf)}}
    )
    zdtype = parse_data_type(dtype, zarr_format=3)
    metadata = ArrayV3Metadata(
        shape=shape,
        data_type=zdtype,
        chunk_grid={
            "name": "regular",
            "configuration": {"chunk_shape": shape},
        },
        chunk_key_encoding={"name": "default"},
        fill_value=zdtype.default_scalar(),
        codecs=[BytesCodec()],
        attributes={},
        dimension_names=("y", "x"),
        storage_transformers=None,
    )
    ma = ManifestArray(
        chunkmanifest=manifest,
        metadata=metadata,
    )
    foo = xr.Variable(data=ma, dims=["y", "x"], encoding={"scale_factor": 2})
    vds = xr.Dataset(
        {"foo": foo},
        coords={
            "time": ("time", [np.datetime64(date)])  # Single time point
        },
    )
    return vds


class Processor:
    def initialize_repo(self) -> Repository:
        chunk_store = icechunk.local_filesystem_store(CHUNK_DIR)
        storage = icechunk.in_memory_storage()
        config = icechunk.RepositoryConfig.default()
        config.set_virtual_chunk_container(
            icechunk.VirtualChunkContainer(CHUNK_DIRECTORY_URL_PREFIX, chunk_store)
        )
        repo = icechunk.Repository.open_or_create(
            storage=storage,
            config=config,
            authorize_virtual_chunk_access={
                CHUNK_DIRECTORY_URL_PREFIX: icechunk.credentials.LocalFileSystemAccess
            },
        )
        # Get only up to 2 commits to check if the repository is new
        history = list(islice(repo.ancestry(branch="main"), 2))
        if len(history) == 1:
            session = repo.writable_session("main")
            vds = synthetic_vds("2024-01-01")
            vds.vz.to_icechunk(session.store, validate_containers=False)
            session.commit(message="Initialization")
        return repo

    def initialize_session(self, repo: Repository) -> Session:
        session = repo.writable_session("main")
        return session

    def process_file(self, file_key: str, session: Session) -> bool:
        result = False
        try:
            vds = synthetic_vds(file_key)
            # Reject granules whose expected shared attributes differ from
            # the template (raises); merely-unexpected attributes only warn.
            # A real processor also passes coordinates= with the reference
            # spatial grid so mis-gridded granules are rejected.
            validate_granule(BACKFILL_TEMPLATE, vds)
            vds.vz.to_icechunk(
                session.store, append_dim="time", validate_containers=False
            )
            result = True
        except Error:
            result = False
        return result

    def commit_processed_files(self, session: Session) -> str:
        snapshot = session.commit(message=f"Append to {session.snapshot_id}")
        return str(snapshot)

    def initialize_backfill_store(self, repo: Repository) -> BranchInit:
        main_tip = repo.lookup_branch("main")
        # Reset a leftover branch from a failed run, per the protocol.
        if "backfill" in repo.list_branches():
            repo.reset_branch("backfill", main_tip)
        else:
            repo.create_branch("backfill", main_tip)
        session = repo.writable_session("backfill")
        create_empty_store(BACKFILL_TEMPLATE, session.store)
        time_coord = zarr.open_array(session.store, path="time")
        time_coord[:] = np.arange(BACKFILL_N)
        # allow_extra: the branch also carries whatever `main` already held.
        validate_store(
            BACKFILL_TEMPLATE,
            zarr.open_group(session.store, mode="r"),
            allow_extra=True,
        )
        snapshot = cast(str, session.commit("Initialize backfill shape"))
        return BranchInit(snapshot=snapshot, branched_from=main_tip)

    def open_backfill_repo(self) -> Repository:
        # Reference impl storage config, read from the environment:
        #   ICECHUNK_BUCKET  - if set, use S3 storage (Lambda); IAM creds via from_env
        #   ICECHUNK_PREFIX  - S3 key prefix (optional)
        #   ICECHUNK_REGION  - S3 region (optional)
        #   ICECHUNK_LOCAL_PATH - filesystem repo path when no bucket (tests)
        chunk_store = icechunk.local_filesystem_store(CHUNK_DIR)
        bucket = os.environ.get("ICECHUNK_BUCKET")
        if bucket:
            storage = icechunk.s3_storage(
                bucket=bucket,
                prefix=os.environ.get("ICECHUNK_PREFIX"),
                region=os.environ.get("ICECHUNK_REGION"),
                from_env=True,
            )
        else:
            storage = icechunk.local_filesystem_storage(
                os.environ["ICECHUNK_LOCAL_PATH"]
            )
        config = icechunk.RepositoryConfig.default()
        config.set_virtual_chunk_container(
            icechunk.VirtualChunkContainer(CHUNK_DIRECTORY_URL_PREFIX, chunk_store)
        )
        return icechunk.Repository.open_or_create(
            storage=storage,
            config=config,
            authorize_virtual_chunk_access={
                CHUNK_DIRECTORY_URL_PREFIX: icechunk.credentials.LocalFileSystemAccess
            },
        )

    def _backfill_slice_vds(self, t: int) -> xr.Dataset:
        """A one-time-step virtual dataset for backfill index t, carrying the
        matching `time` coordinate so to_icechunk(region="auto") can place it."""
        buf = np.full((1, BACKFILL_Y, BACKFILL_X), t, dtype=BACKFILL_DTYPE).tobytes()
        # Synthetic reference only: each slice writes its own local source chunk.
        # These accumulate under CHUNK_DIR; a real Processor references existing
        # source files (e.g. in S3) and does not create per-slice temp files.
        filepath = f"{CHUNK_DIR}/backfill_slice_{t}"
        obstore.put(obstore.store.LocalStore(), filepath, buf)
        manifest = ChunkManifest(
            {"0.0.0": {"path": filepath, "offset": 0, "length": len(buf)}}
        )
        zdtype = parse_data_type(BACKFILL_DTYPE, zarr_format=3)
        metadata = ArrayV3Metadata(
            shape=(1, BACKFILL_Y, BACKFILL_X),
            data_type=zdtype,
            chunk_grid={
                "name": "regular",
                "configuration": {"chunk_shape": (1, BACKFILL_Y, BACKFILL_X)},
            },
            chunk_key_encoding={"name": "default"},
            fill_value=zdtype.default_scalar(),
            codecs=[BytesCodec()],
            attributes={},
            dimension_names=("time", "y", "x"),
            storage_transformers=None,
        )
        ma = ManifestArray(chunkmanifest=manifest, metadata=metadata)
        return xr.Dataset(
            {"foo": xr.Variable(("time", "y", "x"), ma)},
            coords={"time": ("time", [t])},
        )

    def process_backfill_file(self, file_key: str, fork: ForkSession) -> bool:
        try:
            # Synthetic keys are the integer time index as a string ("0".."5").
            # A real processor parses the source file for its own coordinate.
            t = int(file_key)
            vds = self._backfill_slice_vds(t)
            # Reject granules whose expected shared attributes differ from
            # the template; the except below turns the raise into a logged
            # rejection (False). A real processor also passes coordinates=
            # with the reference spatial grid.
            validate_granule(BACKFILL_TEMPLATE, vds)
            vds.vz.to_icechunk(fork.store, region="auto", validate_containers=False)
            return True
        except Exception:
            # Catch parse/region errors and I/O failures from to_icechunk, but log
            # the real cause first — otherwise the worker only reports a generic
            # "process_backfill_file failed" and the underlying error is lost.
            # A real (network-reading) processor should also retry the granule read
            # here with backoff, since transient object-store / auth throttling under
            # a large backfill's concurrency is otherwise fatal. logger.exception
            # includes the traceback.
            logger.exception("process_backfill_file failed for %s", file_key)
            return False

    def garbage_collect(self, expiry_time: datetime) -> icechunk.GCSummary:
        repo = self.initialize_repo()
        repo.expire_snapshots(older_than=expiry_time)
        gcs = repo.garbage_collect(delete_object_older_than=expiry_time)
        return gcs
