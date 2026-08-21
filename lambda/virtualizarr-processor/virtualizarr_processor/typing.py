from __future__ import annotations

import enum
from datetime import datetime
from typing import TYPE_CHECKING, NamedTuple, Protocol, runtime_checkable

import icechunk
from icechunk import ForkSession, Repository, Session

if TYPE_CHECKING:
    from virtualizarr_processor.inventory import BackfillInventory


class BranchInit(NamedTuple):
    """Result of creating a work branch off the promote target.

    ``branched_from`` is :func:`virtualizarr_processor.backfill.promote`'s
    compare-and-swap expectation: if the target moved while the work branch
    was built, the promote fails instead of discarding that commit.
    """

    snapshot: str  # the init commit on the work branch
    branched_from: str  # the promote target's tip at branch time


class ProcessOutcome(enum.Enum):
    """What became of one forward-processed granule."""

    WRITTEN = "written"  # appended, or republication overwritten in place
    DEFERRED = "deferred"  # out of order: recorded in the pending ledger
    REJECTED = "rejected"  # validation failure — SQS retry, then DLQ


@runtime_checkable
class VirtualizarrProcessor(Protocol):
    def initialize_repo(self) -> Repository:
        """
        Open the repository, creating an empty templated store if new.

        The store must carry an append dimension for forward processing.

        Returns
        -------
        Repository
            An Icechunk Repository.
        """
        ...

    def initialize_session(self, repo: Repository) -> Session:
        """
        Initialize an Icechunk writable Session.

        Parameters
        ----------
            repo: An Icechunk Repository.
        Returns
        -------
        Session
            An Icechunk writable Session.
        """
        ...

    def process_file(self, file_key: str, session: Session) -> ProcessOutcome:
        """
        Parse, validate, and route one granule:
        append when its time is after the axis end; overwrite in place when
        its time already owns a slot and the granule UR matches the store
        manifest (republication / redelivery); defer to the pending ledger
        when it arrived out of order; reject on any validation failure or
        when a different granule claims an occupied slot.

        Parameters
        ----------
            file_key: The full key path to the source file.
            session: The Icechunk writable Session to use for adding the file.
        Returns
        -------
        ProcessOutcome
            WRITTEN, DEFERRED, or REJECTED.
        """
        ...

    def commit_processed_files(self, session: Session) -> str:
        """
        Commit the updates made by calls to process_file.

        Parameters
        ----------
            session: The Icechunk writable Session used with process_file.
        Returns
        -------
        str
            A snapshot id of the append commit.
        """
        ...

    def initialize_backfill_store(
        self, repo: Repository, inventory: "BackfillInventory"
    ) -> BranchInit:
        """
        Create the `backfill` branch off the current `main` tip, build the
        full-shape store (metadata plus native coordinates), and commit.

        The store is declared at full extent up front because backfill
        writes disjoint regions rather than appending. The time axis is
        written from the inventory's exact per-granule values, and workers
        match each granule against it, so a granule missing from the
        inventory is rejected rather than misplaced. The session must have
        no uncommitted changes after this returns, so that forks share the
        committed branch-tip snapshot as their base.

        A `backfill` branch left behind by a failed run is reset so the run
        can be restarted. Concurrent backfill runs are not supported.

        Parameters
        ----------
            repo: An Icechunk Repository (durable storage; not in-memory).
            inventory: The validated backfill inventory for this collection.
        Returns
        -------
        BranchInit
            The base snapshot id of the committed full-shape store and the
            `main` tip it was branched from.
        """
        ...

    def open_backfill_repo(self) -> Repository:
        """
        Open (or create) the durable backfill repository.

        Storage is chosen by the implementation (S3 in a deployed Lambda,
        local filesystem in tests) but must be durable and shared: a pickled
        ForkSession cannot resolve its base snapshot from in-memory storage.
        open_or_create semantics guarantee a `main` branch to branch off.

        Returns
        -------
        Repository
            An Icechunk Repository backed by durable storage.
        """
        ...

    def process_backfill_file(self, file_key: str, fork: ForkSession | Session) -> bool:
        """
        Write a per-file virtual dataset into the fork's store with an
        explicit region write, locating the target slot by exact match of
        the granule's time value against the store axis. Must NOT commit.

        Parameters
        ----------
            file_key: The full key path to the source file.
            fork: An Icechunk ForkSession to write references into.
        Returns
        -------
        bool
            True if the file was successfully processed.
        """
        ...

    def garbage_collect(self, expiry_time: datetime) -> icechunk.GCSummary:
        """
        Run Icechunk snapshot expiry and garbage collection.

        Parameters
        ----------
            expiry_time: Remove snapshots older than this time.
        Returns
        -------
        GCSummary
        """
        ...
