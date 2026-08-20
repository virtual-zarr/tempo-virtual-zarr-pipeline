"""The TEMPO L3 VirtualizarrProcessor.

Config-driven (``virtualizarr_processor.collection``): the deployed
instance selects its collection via ``$TEMPO_COLLECTION`` and creates /
validates the store from the committed declarative artifacts (template
JSON + reference coordinates). Every granule insertion runs the layered
validation from the design spec (docs/superpowers/specs/
2026-08-20-tempo-inventory-and-processor-design.md): shared-attribute
template check, bit-exact reference grid, in-file time integrity, and
exact-match axis slot lookup — a granule that fails any check is
rejected loudly and nothing is written for it. All virtual references are
stamped with ``last_updated_at`` so a source object that is overwritten
after ingest fails reads instead of returning bytes from a changed file.

Repository/storage environment contract (Lambda and tests):

- ``ICECHUNK_BUCKET`` / ``ICECHUNK_PREFIX`` / ``ICECHUNK_REGION`` — S3
  repo storage (IAM creds from the environment); or
  ``ICECHUNK_LOCAL_PATH`` — local filesystem repo (tests).
- ``VIRTUAL_CHUNK_PREFIX`` — url prefix of the virtual chunk container
  (default ``s3://asdc-prod-protected/``; tests use ``file:///...``).
  S3 containers are registered credential-less: readers authorize with
  temporary ASDC credentials at open time.
- ``VIRTUAL_CHUNK_REGION`` — region of the s3 container (default
  ``us-west-2``).
"""

from __future__ import annotations

import logging
import os
import time as time_module
from datetime import datetime, timedelta, timezone
from typing import cast

import icechunk
import numpy as np
import xarray as xr
import zarr
from icechunk import ForkSession, Repository, Session
from pydantic_zarr.v3 import ArraySpec

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
from virtualizarr_processor.manifest import PendingLedger, StoreManifest
from virtualizarr_processor.store_template import (
    WRITE_ARTIFACT_ATTRIBUTES,
    GranuleValidationError,
    StoreValidationError,
    create_empty_store,
    resize,
    validate_granule,
    validate_store,
)
from virtualizarr_processor.typing import ProcessOutcome

logger = logging.getLogger(__name__)

DEFAULT_VIRTUAL_CHUNK_PREFIX = "s3://asdc-prod-protected/"
PARSE_ATTEMPTS = 3
PARSE_BACKOFF_SECONDS = (5, 15)


def _store_manifest_uri() -> str:
    return os.environ["STORE_MANIFEST_URI"]


def _pending_ledger_uri() -> str:
    return os.environ["PENDING_LEDGER_URI"]


def _granule_ur(file_key: str) -> str:
    """The granule UR as derived from the file key (TEMPO filenames are the
    granule UR plus ``.nc``; the inventory builder keeps this consistent)."""
    return file_key.rsplit("/", 1)[-1].removesuffix(".nc")


class Processor:
    def __init__(self, config: CollectionConfig | None = None) -> None:
        self.config = config or load_collection()
        self.template = load_template(self.config)
        self.coordinates = load_coordinates(self.config)
        # Forward-processing batch state (spec §5); reset per session.
        self._appended: list[GranuleEntry] = []
        self._replaced: dict[int, GranuleEntry] = {}

    # -- repository ---------------------------------------------------------

    def open_backfill_repo(self) -> Repository:
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
    ) -> str:
        """Create the full-shape store on a clean `backfill` branch (spec §2)."""
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
        repo.create_branch("backfill", repo.lookup_branch("main"))
        session = repo.writable_session("backfill")
        create_empty_store(template, session.store)

        # The native coordinates, written once: the axis from the inventory's
        # exact per-granule times, the grid from the committed artifact.
        zarr.open_array(session.store, path="time")[:] = inventory.times()
        for axis, values in self.coordinates.items():
            zarr.open_array(session.store, path=axis)[:] = values

        # allow_extra: the branch also carries whatever `main` already held.
        validate_store(
            template, zarr.open_group(session.store, mode="r"), allow_extra=True
        )
        return cast(
            str,
            session.commit(
                f"Initialize backfill store: {len(inventory.granules)} granules "
                f"of {inventory.collection}"
            ),
        )

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
    ) -> str:
        """Prepare the `resort` branch for the re-sort job (spec §5).

        Branches (or resets) `resort` off the current `main` tip, resizes
        every array carrying the append dimension to the merged inventory's
        length, rewrites the time axis to the merged values, validates, and
        commits the clean base the rewrite workers build on. Slots at or
        after the first shifted index hold stale references until the job
        rewrites all of them; promote only happens after it has.
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
            template, zarr.open_group(session.store, mode="r"), allow_extra=True
        )
        return cast(
            str,
            session.commit(f"Resort: axis of {len(merged.granules)} granules"),
        )

    def process_resort_file(self, file_key: str, session: Session) -> bool:
        """Rewrite one granule's slot on the resort branch (no commit)."""
        # Identical to the backfill worker path; only the session type
        # differs, and just the .store attribute is used.
        return self.process_backfill_file(file_key, session)  # type: ignore[arg-type]

    def validate_backfill_store(
        self, repo: Repository, inventory: BackfillInventory, *, branch: str
    ) -> None:
        """The promote gate (spec §4): template, axis, and grid, bit-exact."""
        session = repo.readonly_session(branch)
        group = zarr.open_group(session.store, mode="r")
        template = resize(
            self.template, {self.config.append_dim: len(inventory.granules)}
        )
        validate_store(template, group, allow_extra=True)
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
        if differences:
            raise StoreValidationError(differences)

    def _write_region(
        self, vds: xr.Dataset, store: object, index: int, stamp: datetime
    ) -> None:
        """Write one validated granule's refs into axis slot ``index``.

        The slot was located by exact raw-float match and the region is
        passed explicitly; region="auto" would CF-decode the store axis and
        compare in datetime64 space — a precision seam this pipeline forbids
        — and rewrite the shared native time chunk from every worker.
        Dropping `time` leaves the init-written axis untouched. Root attrs
        come solely from the template at init; per-granule attrs must not
        land in the store, and differing group-attr updates from parallel
        forks make the merge conflict.
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
        """The unique axis slot whose value equals ``time_value`` bit-exactly."""
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
        """Parse + validate one granule; returns the writable vds (reference
        coordinates validated then dropped — they are native, written once at
        init) and the `last_updated_at` stamp for its refs.

        The stamp anchors to the source object's own observed mtime (plus a
        one-second checksum-precision margin, matching virtualizarr's own
        default margin) so overwritten objects fail reads without depending
        on the worker's clock."""
        stamp = source_last_modified(file_key) + timedelta(seconds=1)
        vds = self._open_with_retry(file_key)
        granule_time(vds)
        validate_granule(
            self.template,
            vds,
            coordinates=self.coordinates,
            volatile=self.config.volatile_attributes | WRITE_ARTIFACT_ATTRIBUTES,
        )
        return vds.drop_vars(list(self.coordinates)), stamp

    def _open_with_retry(self, file_key: str) -> xr.Dataset:
        """Parse with backoff on transient (non-validation) errors; object-store
        throttling under backfill concurrency must not fail the whole batch."""
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

    def initialize_repo(self) -> Repository:
        """Open the repo; create an empty (time=0) templated store if new."""
        repo = self.open_backfill_repo()
        session = repo.writable_session("main")
        group = zarr.open_group(session.store, mode="a")
        if "time" not in group:
            template = resize(self.template, {self.config.append_dim: 0})
            create_empty_store(template, session.store)
            for axis, values in self.coordinates.items():
                zarr.open_array(session.store, path=axis)[:] = values
            validate_store(
                template, zarr.open_group(session.store, mode="r"), allow_extra=True
            )
            session.commit("Initialize empty templated store")
        return repo

    def initialize_session(self, repo: Repository) -> Session:
        self._appended = []
        self._replaced = {}
        return repo.writable_session("main")

    def process_file(self, file_key: str, session: Session) -> ProcessOutcome:
        """Route one granule per the design spec §5 table."""
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
                known = self._manifest_entry_at(index, axis)
                if known.granule_ur != entry.granule_ur:
                    # Two distinct granules claiming one time step is a data
                    # inconsistency to investigate, never to write.
                    logger.error(
                        "process_file: slot %d (time %r) belongs to %s, "
                        "refusing to overwrite it with %s",
                        index,
                        time_value,
                        known.granule_ur,
                        entry.granule_ur,
                    )
                    return ProcessOutcome.REJECTED
                # Republication of a known scan, or an at-least-once
                # redelivery: refresh the slot's refs and stamps in place.
                self._write_region(vds, session.store, index, stamp)
                self._replaced[index] = entry
                return ProcessOutcome.WRITTEN

            if not axis.size or time_value > float(axis[-1]):
                vds.attrs = {}  # store attrs come solely from the template
                vds.vz.to_icechunk(
                    session.store,
                    append_dim=self.config.append_dim,
                    validate_containers=False,
                    last_updated_at=stamp,
                )
                self._appended.append(entry)
                return ProcessOutcome.WRITTEN

            # Out of order (historical drip-feed or an adjacent-scan swap):
            # not appendable without breaking axis monotonicity. Record it
            # for the scheduled re-sort job and consume the message.
            PendingLedger.append(_pending_ledger_uri(), [entry])
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
        """Commit the batch, then update the store manifest to match.

        The updated manifest is validated bit-exactly against the session's
        (about-to-be-committed) axis *before* committing, so a manifest/axis
        divergence fails the whole batch loudly instead of committing state
        the manifest cannot describe.
        """
        entries = list(self._manifest_entries())
        for index, entry in self._replaced.items():
            entries[index] = entry
        entries += self._appended
        axis = np.asarray(zarr.open_array(session.store, path="time")[:])
        manifest = None
        if entries:
            manifest = BackfillInventory(
                schema="tempo-backfill-inventory/1",  # type: ignore[call-arg]
                collection=self.config.collection_shortname,
                concept_id=self.config.concept_id,
                time_units=self.config.time_units,
                built_at=datetime.now(timezone.utc).isoformat(),
                granules=tuple(entries),
            )
            StoreManifest.validate_against_axis(manifest, axis)
        snapshot = cast(str, session.commit(f"Append to {session.snapshot_id}"))
        if manifest is not None and (self._appended or self._replaced):
            StoreManifest.write(_store_manifest_uri(), manifest)
        self._appended = []
        self._replaced = {}
        return snapshot

    def _manifest_entries(self) -> tuple[GranuleEntry, ...]:
        """The store manifest's granules; absent manifest = empty store only."""
        try:
            manifest = StoreManifest.read(_store_manifest_uri())
        except FileNotFoundError:
            return ()
        if manifest.collection != self.config.collection_shortname:
            raise StoreValidationError(
                [
                    f"store manifest is for {manifest.collection!r}, this "
                    f"deployment processes {self.config.collection_shortname!r}"
                ]
            )
        return manifest.granules

    def _manifest_entry_at(self, index: int, axis: np.ndarray) -> GranuleEntry:
        """The manifest entry owning axis slot ``index``, trust-checked."""
        entries = self._manifest_entries()
        if len(entries) != axis.size or entries[index].time != float(axis[index]):
            raise StoreValidationError(
                [
                    "store manifest does not describe the store axis "
                    f"(slot {index}); refusing to route against it"
                ]
            )
        return entries[index]

    def garbage_collect(self, expiry_time: datetime) -> icechunk.GCSummary:
        repo = self.open_backfill_repo()
        repo.expire_snapshots(older_than=expiry_time)
        return repo.garbage_collect(delete_object_older_than=expiry_time)
