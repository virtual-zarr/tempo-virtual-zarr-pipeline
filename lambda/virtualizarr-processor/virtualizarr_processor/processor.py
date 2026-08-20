"""The TEMPO L3 VirtualizarrProcessor.

The deployed instance selects its collection via ``$TEMPO_COLLECTION``
and builds the store from the committed artifacts (template JSON and
reference coordinates). Before anything is written, each granule must
match the template's shared attributes, carry the bit-identical reference
grid, agree with its own epoch attribute, and match exactly one slot on
the store's time axis. Virtual references are stamped with
``last_updated_at`` so that reads fail if a source object is overwritten
after ingest.

Environment variables:

- ``ICECHUNK_BUCKET`` / ``ICECHUNK_PREFIX`` / ``ICECHUNK_REGION``: S3
  repository storage, or ``ICECHUNK_LOCAL_PATH`` for a local repository
  in tests.
- ``VIRTUAL_CHUNK_PREFIX``: virtual chunk container prefix (default
  ``s3://asdc-prod-protected/``; tests use ``file:///...``). S3
  containers are registered without credentials; readers authorize with
  temporary ASDC credentials at open time.
- ``VIRTUAL_CHUNK_REGION``: region of the S3 container (default
  ``us-west-2``).

The store manifest and pending ledger live inside the store itself (see
``manifest.py``), so they commit atomically with the data they describe.
"""

from __future__ import annotations

import logging
import os
import time as time_module
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from typing import cast

import icechunk
import numpy as np
import xarray as xr
import zarr
from icechunk import ForkSession, Repository, Session
from pydantic_zarr.v3 import ArraySpec

from virtualizarr_processor import manifest as manifest_module
from virtualizarr_processor.collection import (
    CollectionConfig,
    load_collection,
    load_coordinates,
    load_template,
)
from virtualizarr_processor.granule import (
    granule_time,
    open_flat_granule,
    source_last_modified,
)
from virtualizarr_processor.inventory import BackfillInventory, GranuleEntry
from virtualizarr_processor.manifest import (
    MANIFEST_ARRAYS,
    PIPELINE_STATE_ATTRIBUTES,
    STORE_META_ATTRIBUTE,
    PendingLedger,
    StoreManifest,
)
from virtualizarr_processor.resort import first_shifted_index
from virtualizarr_processor.store_template import (
    GranuleValidationError,
    StoreValidationError,
    create_empty_store,
    resize,
    validate_granule,
    validate_store,
)
from virtualizarr_processor.typing import BranchInit, ProcessOutcome

logger = logging.getLogger(__name__)

DEFAULT_VIRTUAL_CHUNK_PREFIX = "s3://asdc-prod-protected/"
PARSE_ATTEMPTS = 3
PARSE_BACKOFF_SECONDS = (5, 15)


def _granule_ur(file_key: str) -> str:
    """Derive the granule UR from the file key.

    TEMPO filenames are the granule UR plus ``.nc``; the inventory builder
    follows the same convention.
    """
    return file_key.rsplit("/", 1)[-1].removesuffix(".nc")


class Processor:
    """TEMPO implementation of the VirtualizarrProcessor protocol."""

    def __init__(self, config: CollectionConfig | None = None) -> None:
        self.config = config or load_collection()
        self.template = load_template(self.config)
        self.coordinates = load_coordinates(self.config)
        # Forward-processing batch state, reset per session.
        self._appended: list[GranuleEntry] = []
        self._replaced: dict[int, GranuleEntry] = {}

    # -- repository ---------------------------------------------------------

    def open_backfill_repo(self) -> Repository:
        """Open or create the repository per the environment contract above."""
        bucket = os.environ.get("ICECHUNK_BUCKET")
        if bucket:
            storage = icechunk.s3_storage(
                bucket=bucket,
                prefix=manifest_module.storage_prefix(),
                region=os.environ.get("ICECHUNK_REGION"),
                from_env=True,
            )
        else:
            storage = icechunk.local_filesystem_storage(
                os.environ["ICECHUNK_LOCAL_PATH"]
            )

        prefix = os.environ.get("VIRTUAL_CHUNK_PREFIX", DEFAULT_VIRTUAL_CHUNK_PREFIX)
        config = icechunk.RepositoryConfig.default()
        authorize: dict[str, object] | None = None
        chunk_store: (
            icechunk.ObjectStoreConfig.LocalFileSystem
            | icechunk.ObjectStoreConfig.S3Compatible
            | icechunk.ObjectStoreConfig.S3
        )
        if prefix.startswith("file://"):
            chunk_store = icechunk.local_filesystem_store(
                prefix.removeprefix("file://")
            )
            authorize = {prefix: icechunk.credentials.LocalFileSystemAccess}
        elif prefix.startswith("s3://"):
            # Credential-less container: writing refs needs no chunk access;
            # readers authorize with temporary ASDC credentials at open time.
            chunk_store = icechunk.s3_store(
                region=os.environ.get("VIRTUAL_CHUNK_REGION", "us-west-2")
            )
        else:
            raise ValueError(f"Unsupported VIRTUAL_CHUNK_PREFIX {prefix!r}")
        config.set_virtual_chunk_container(
            icechunk.VirtualChunkContainer(prefix, chunk_store)
        )
        return icechunk.Repository.open_or_create(
            storage=storage,
            config=config,
            authorize_virtual_chunk_access=authorize,  # type: ignore[arg-type]
        )

    # -- backfill -----------------------------------------------------------

    def initialize_backfill_store(
        self, repo: Repository, inventory: BackfillInventory
    ) -> BranchInit:
        """Create the full-shape store on a clean ``backfill`` branch."""
        if inventory.collection != self.config.collection_shortname:
            raise StoreValidationError(
                [
                    f"inventory is for {inventory.collection!r}, this deployment "
                    f"processes {self.config.collection_shortname!r}"
                ]
            )
        template = resize(
            self.template, {self.config.append_dim: len(inventory.granules)}
        )
        main_tip = repo.lookup_branch("main")
        # Reset a leftover branch from a failed run so it can be restarted.
        # Concurrent backfill runs are not supported.
        if "backfill" in repo.list_branches():
            repo.reset_branch("backfill", main_tip)
        else:
            repo.create_branch("backfill", main_tip)
        session = repo.writable_session("backfill")
        create_empty_store(template, session.store)

        # The native coordinates, written once: the axis from the inventory's
        # exact per-granule times, the grid from the committed artifact.
        zarr.open_array(session.store, path="time")[:] = inventory.times()
        for axis, values in self.coordinates.items():
            zarr.open_array(session.store, path=axis)[:] = values

        # The manifest and an empty ledger ride the same branch, so the
        # promote lands data and state in one atomic reset.
        StoreManifest.write(session.store, inventory)
        PendingLedger.write(session.store, ())

        # allow_extra: the branch also carries whatever `main` already held.
        validate_store(
            template,
            zarr.open_group(session.store, mode="r"),
            allow_extra=True,
            volatile=PIPELINE_STATE_ATTRIBUTES,
        )
        snapshot = session.commit(
            f"Initialize backfill store: {len(inventory.granules)} granules "
            f"of {inventory.collection}"
        )
        return BranchInit(snapshot=snapshot, branched_from=main_tip)

    def process_backfill_file(self, file_key: str, fork: ForkSession) -> bool:
        """Validate one granule and region-write it into the fork (no commit)."""
        try:
            vds, stamp = self._parse_and_validate(file_key)
            index = self._axis_index(fork.store, float(vds["time"].values[0]))
            self._write_region(vds, fork.store, index, stamp)
            return True
        except Exception:
            # Log the real cause; the worker handler turns False into a raise
            # so the Step Functions run fails before promote.
            logger.exception("process_backfill_file failed for %s", file_key)
            return False

    def initialize_resort_store(
        self, repo: Repository, merged: BackfillInventory
    ) -> BranchInit:
        """Prepare the ``resort`` branch for the re-sort job.

        Branch (or reset) ``resort`` off the current ``main`` tip, resize
        every array carrying the append dimension to the merged inventory's
        length, rewrite the time axis, validate, and commit. Slots at or
        after the first shifted index hold stale references until the job
        relocates them (:meth:`reindex_resort_slots`) and writes the
        inserted granules, which it must do before promoting.
        """
        if merged.collection != self.config.collection_shortname:
            raise StoreValidationError(
                [
                    f"merged inventory is for {merged.collection!r}, this "
                    f"deployment processes {self.config.collection_shortname!r}"
                ]
            )
        tip = repo.lookup_branch("main")
        if "resort" in repo.list_branches():
            repo.reset_branch("resort", tip)
        else:
            repo.create_branch("resort", tip)
        session = repo.writable_session("resort")
        template = resize(self.template, {self.config.append_dim: len(merged.granules)})
        for path, node in template.to_flat().items():
            if (
                isinstance(node, ArraySpec)
                and node.dimension_names
                and self.config.append_dim in node.dimension_names
            ):
                array = zarr.open_array(session.store, path=path.lstrip("/"))
                array.resize(node.shape)
        zarr.open_array(session.store, path="time")[:] = merged.times()
        validate_store(
            template,
            zarr.open_group(session.store, mode="r"),
            allow_extra=True,
            volatile=PIPELINE_STATE_ATTRIBUTES,
        )
        snapshot = session.commit(f"Resort: axis of {len(merged.granules)} granules")
        return BranchInit(snapshot=snapshot, branched_from=tip)

    def process_resort_file(self, file_key: str, session: Session) -> bool:
        """Rewrite one granule's slot on the resort branch without committing."""
        # Same as the backfill worker path; only the session type differs,
        # and only the .store attribute is used.
        return self.process_backfill_file(file_key, session)  # type: ignore[arg-type]

    def reindex_resort_slots(
        self,
        session: Session,
        manifest: BackfillInventory,
        merged: BackfillInventory,
    ) -> int:
        """Move already-ingested slots to their merged positions, metadata-only.

        Relocates each shifted slot's chunk references with icechunk's
        ``reindex_array``; payloads (including ``last_updated_at`` checksums)
        move untouched, so no granule is re-read from source. Slots for
        inserted granules are cleared and must be written afterwards with
        :meth:`process_resort_file`.

        Call on the ``resort`` session after :meth:`initialize_resort_store`
        has resized the arrays and rewritten the axis. Returns the number of
        slots moved.
        """
        shift = first_shifted_index(manifest, merged)
        merged_position = {e.granule_ur: i for i, e in enumerate(merged.granules)}
        # old slot -> new slot for every already-ingested granule that moves.
        forward_map = {
            old: merged_position[entry.granule_ur]
            for old, entry in enumerate(manifest.granules)
            if old >= shift
        }
        backward_map = {new: old for old, new in forward_map.items()}
        if not forward_map:
            return 0

        for path, node in self.template.to_flat().items():
            if not isinstance(node, ArraySpec) or not node.dimension_names:
                continue
            if self.config.append_dim not in node.dimension_names:
                continue
            if path.lstrip("/") == self.config.append_dim:
                continue  # the axis itself is rewritten wholesale at init
            axis_pos = node.dimension_names.index(self.config.append_dim)
            chunk_shape = node.chunk_grid["configuration"]["chunk_shape"]
            if chunk_shape[axis_pos] != 1:
                # A chunk spanning several slots cannot be relocated per slot.
                raise StoreValidationError(
                    [
                        f"array {path!r} chunks {chunk_shape[axis_pos]} slots per "
                        f"chunk along {self.config.append_dim!r}; the re-sort "
                        "job can only relocate arrays chunked one slot per chunk"
                    ]
                )

            def forward(coords: Iterable[int]) -> list[int] | None:
                coords = list(coords)
                new_slot = forward_map.get(coords[axis_pos])
                if new_slot is None:  # unshifted slot: leave in place
                    return None
                coords[axis_pos] = new_slot
                return coords

            def backward(coords: Iterable[int]) -> list[int] | None:
                coords = list(coords)
                slot = coords[axis_pos]
                if slot < shift:
                    return coords  # identity: the slot keeps its chunk
                old_slot = backward_map.get(slot)
                if old_slot is None:  # an inserted granule's slot: clear it
                    return None
                coords[axis_pos] = old_slot
                return coords

            session.reindex_array(f"/{path.lstrip('/')}", forward, backward)
        return len(forward_map)

    def validate_backfill_store(
        self, repo: Repository, inventory: BackfillInventory, *, branch: str
    ) -> None:
        """Check a finished branch against template, inventory, and grid.

        This is the gate that runs before promoting to ``main``.
        """
        session = repo.readonly_session(branch)
        group = zarr.open_group(session.store, mode="r")
        template = resize(
            self.template, {self.config.append_dim: len(inventory.granules)}
        )
        validate_store(
            template, group, allow_extra=True, volatile=PIPELINE_STATE_ATTRIBUTES
        )
        differences = []
        axis = np.asarray(zarr.open_array(session.store, path="time")[:])
        if not np.array_equal(axis, inventory.times()):
            differences.append("store time axis differs from the inventory")
        if np.any(np.diff(axis) <= 0):
            differences.append("store time axis is not strictly increasing")
        for name, values in self.coordinates.items():
            actual = np.asarray(zarr.open_array(session.store, path=name)[:])
            if not np.array_equal(actual, values):
                differences.append(
                    f"store {name} differs from the reference coordinates"
                )
        urs = np.asarray(zarr.open_array(session.store, path=MANIFEST_ARRAYS[0])[:])
        if [str(v) for v in urs] != [e.granule_ur for e in inventory.granules]:
            differences.append("store granule_ur array differs from the inventory")
        urls = np.asarray(zarr.open_array(session.store, path=MANIFEST_ARRAYS[1])[:])
        if [str(v) for v in urls] != [e.url for e in inventory.granules]:
            differences.append("store granule_url array differs from the inventory")
        if differences:
            raise StoreValidationError(differences)

    def _write_region(
        self, vds: xr.Dataset, store: object, index: int, stamp: datetime
    ) -> None:
        """Write one validated granule's references into axis slot ``index``.

        The region is passed as an explicit slice rather than
        ``region="auto"``: xarray's auto-detection CF-decodes the store axis
        and compares in datetime64 space, while this pipeline works in raw
        seconds throughout. Dropping ``time`` leaves the axis written at
        init untouched, and clearing the granule attributes keeps store
        attributes template-only (differing group-attribute updates from
        parallel forks would also fail the merge).
        """
        vds = vds.drop_vars("time")
        vds.attrs = {}
        vds.vz.to_icechunk(
            store,  # type: ignore[arg-type]
            region={self.config.append_dim: slice(index, index + 1)},
            validate_containers=False,
            last_updated_at=stamp,
        )

    def _axis_index(self, store: object, time_value: float) -> int:
        """Return the unique axis slot whose value equals ``time_value``."""
        axis = np.asarray(zarr.open_array(store, path="time")[:])  # type: ignore[arg-type]
        matches = np.nonzero(axis == time_value)[0]
        if matches.size != 1:
            raise GranuleValidationError(
                [
                    f"granule time {time_value!r} matches {matches.size} slots "
                    "in the store axis; expected exactly one (is the granule in "
                    "the inventory?)"
                ]
            )
        return int(matches[0])

    # -- shared parsing/validation ------------------------------------------

    def _parse_and_validate(self, file_key: str) -> tuple[xr.Dataset, datetime]:
        """Parse and validate one granule.

        Returns the dataset ready for writing (reference coordinates are
        validated, then dropped, since they are written once at init) and
        the ``last_updated_at`` stamp for its references. The stamp is the
        source object's observed modification time plus a one-second
        precision margin, so it does not depend on the worker's clock.
        """
        stamp = source_last_modified(file_key) + timedelta(seconds=1)
        vds = self._open_with_retry(file_key)
        granule_time(vds)
        validate_granule(
            self.template,
            vds,
            coordinates=self.coordinates,
            volatile=self.config.volatile_attributes,
        )
        return vds.drop_vars(list(self.coordinates)), stamp

    def _open_with_retry(self, file_key: str) -> xr.Dataset:
        """Parse the granule, retrying transient errors with backoff.

        Validation errors are not retried. The retries keep object-store
        throttling under backfill concurrency from failing a whole batch.
        """
        for attempt in range(PARSE_ATTEMPTS):
            try:
                return open_flat_granule(file_key, self.config)
            except GranuleValidationError:
                raise
            except Exception:
                if attempt == PARSE_ATTEMPTS - 1:
                    raise
                delay = PARSE_BACKOFF_SECONDS[
                    min(attempt, len(PARSE_BACKOFF_SECONDS) - 1)
                ]
                logger.warning(
                    "parse attempt %d for %s failed; retrying in %ds",
                    attempt + 1,
                    file_key,
                    delay,
                    exc_info=True,
                )
                time_module.sleep(delay)
        raise AssertionError("unreachable")

    # -- forward processing --------------------------------------------------

    def open_initialized_repo(self) -> Repository:
        """Open the repository, raising if the store is not initialized.

        Used by the forward consumer, which must never create the store as
        a side effect; that is the backfill Init step's job (or the
        initialize Lambda's, in forward-only deployments).
        """
        repo = self.open_backfill_repo()
        try:
            group = zarr.open_group(repo.readonly_session("main").store, mode="r")
            initialized = "time" in group
        except FileNotFoundError:  # no root group: a brand-new repository
            initialized = False
        if not initialized:
            raise StoreValidationError(
                [
                    "store is not initialized on `main`; run the backfill or "
                    "the initialize Lambda before consuming granules"
                ]
            )
        return repo

    def initialize_repo(self) -> Repository:
        """Open the repository, creating an empty templated store if new."""
        repo = self.open_backfill_repo()
        session = repo.writable_session("main")
        group = zarr.open_group(session.store, mode="a")
        if "time" not in group:
            template = resize(self.template, {self.config.append_dim: 0})
            create_empty_store(template, session.store)
            for axis, values in self.coordinates.items():
                zarr.open_array(session.store, path=axis)[:] = values
            root = zarr.open_group(session.store, mode="a")
            root.attrs[STORE_META_ATTRIBUTE] = {
                "schema": "tempo-backfill-inventory/1",
                "collection": self.config.collection_shortname,
                "concept_id": self.config.concept_id,
                "time_units": self.config.time_units,
                "built_at": datetime.now(timezone.utc).isoformat(),
            }
            PendingLedger.write(session.store, ())
            validate_store(
                template,
                zarr.open_group(session.store, mode="r"),
                allow_extra=True,
                volatile=PIPELINE_STATE_ATTRIBUTES,
            )
            session.commit("Initialize empty templated store")
        return repo

    def initialize_session(self, repo: Repository) -> Session:
        self._appended = []
        self._replaced = {}
        return repo.writable_session("main")

    def process_file(self, file_key: str, session: Session) -> ProcessOutcome:
        """Validate one granule and route it: append, overwrite, defer, or reject."""
        try:
            vds, stamp = self._parse_and_validate(file_key)
        except Exception:
            logger.exception("process_file: validation failed for %s", file_key)
            return ProcessOutcome.REJECTED
        try:
            time_value = float(np.asarray(vds["time"].values)[0])
            entry = GranuleEntry(
                url=file_key, granule_ur=_granule_ur(file_key), time=time_value
            )
            axis = np.asarray(zarr.open_array(session.store, path="time")[:])
            occupied = np.nonzero(axis == time_value)[0]

            if occupied.size == 1:
                index = int(occupied[0])
                known_ur = str(zarr.open_array(session.store, path="granule_ur")[index])
                if known_ur != entry.granule_ur:
                    # A different granule claiming an occupied time step is a
                    # data inconsistency; never overwrite.
                    logger.error(
                        "process_file: slot %d (time %r) belongs to %s, "
                        "refusing to overwrite it with %s",
                        index,
                        time_value,
                        known_ur,
                        entry.granule_ur,
                    )
                    return ProcessOutcome.REJECTED
                # Republication of a known scan, or an at-least-once
                # redelivery: refresh the slot's refs and stamps in place.
                self._write_region(vds, session.store, index, stamp)
                self._replaced[index] = entry
                return ProcessOutcome.WRITTEN

            if not axis.size or time_value > float(axis[-1]):
                vds.attrs = {}  # store attributes come from the template only
                vds.vz.to_icechunk(
                    session.store,
                    append_dim=self.config.append_dim,
                    validate_containers=False,
                    last_updated_at=stamp,
                )
                self._appended.append(entry)
                return ProcessOutcome.WRITTEN

            # Out of order: appending would break axis monotonicity.
            owned = {
                str(v)
                for v in np.asarray(
                    zarr.open_array(session.store, path="granule_ur")[:]
                )
            }
            if entry.granule_ur in owned:
                # Same granule, shifted time: folding it in would give the
                # manifest a duplicate UR and wedge every future re-sort.
                # Reject to the DLQ for operator review instead.
                logger.error(
                    "process_file: %s already owns a slot but its time %r no "
                    "longer matches the axis; rejecting republication with a "
                    "moved timestamp",
                    entry.granule_ur,
                    time_value,
                )
                return ProcessOutcome.REJECTED
            # Record it for the scheduled re-sort job; the ledger update is
            # part of this session and commits with the batch.
            PendingLedger.append(session.store, [entry])
            logger.info(
                "process_file: deferred out-of-order granule %s (time %r) "
                "to the pending ledger",
                entry.granule_ur,
                time_value,
            )
            return ProcessOutcome.DEFERRED
        except Exception:
            logger.exception("process_file failed for %s", file_key)
            return ProcessOutcome.REJECTED

    def commit_processed_files(self, session: Session) -> str:
        """Update the manifest arrays for the batch's writes, then commit.

        The manifest is re-read (running the full inventory validation)
        before the commit, so duplicate URs or a non-monotonic axis fail
        the batch rather than committing state the store cannot describe.
        An all-DEFERRED batch commits too: the ledger attribute update is
        itself a session change.
        """
        if self._appended or self._replaced:
            axis_size = zarr.open_array(session.store, path="time").shape[0]
            ur_array = zarr.open_array(session.store, path="granule_ur")
            url_array = zarr.open_array(session.store, path="granule_url")
            ur_array.resize((axis_size,))
            url_array.resize((axis_size,))
            start = axis_size - len(self._appended)
            for offset, entry in enumerate(self._appended):
                ur_array[start + offset] = entry.granule_ur
                url_array[start + offset] = entry.url
            for index, entry in self._replaced.items():
                ur_array[index] = entry.granule_ur
                url_array[index] = entry.url
            if StoreManifest.read(session.store) is None:
                raise StoreValidationError(
                    [
                        "store carries no manifest metadata; run the backfill "
                        "or the initialize Lambda first"
                    ]
                )
        snapshot = cast(str, session.commit(f"Append to {session.snapshot_id}"))
        self._appended = []
        self._replaced = {}
        return snapshot

    def garbage_collect(self, expiry_time: datetime) -> icechunk.GCSummary:
        repo = self.open_backfill_repo()
        repo.expire_snapshots(older_than=expiry_time)
        return repo.garbage_collect(delete_object_older_than=expiry_time)
