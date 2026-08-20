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

    ``branched_from`` is the target branch's tip at branch time — the
    compare-and-swap expectation :func:`virtualizarr_processor.backfill.promote`
    requires, so a commit that lands on the target while the work branch is
    built fails the promote loudly instead of being silently discarded.
    """

    snapshot: str  # the init commit on the work branch
    branched_from: str  # the promote target's tip when the branch was created


class ProcessOutcome(enum.Enum):
    """What became of one forward-processed granule."""

    WRITTEN = "written"  # appended, or republication overwritten in place
    DEFERRED = "deferred"  # out of order: recorded in the pending ledger
    REJECTED = "rejected"  # validation failure — SQS retry, then DLQ


@runtime_checkable
class VirtualizarrProcessor(Protocol):
    def initialize_repo(self) -> Repository:
        """
        Initialize an Icechunk Store with the necessary structure and return
        a Repository handle.

        This store should have a dimension that can be used with an append function.

        Parameters
        ----------

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
        Commits the updates made by one or multiple calls to process_file

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
        Create the `backfill` branch off the current `main` tip and build the
        full-shape store (metadata plus native coordinates), commit, and
        return the base snapshot id together with the `main` tip the branch
        was created from (the promote CAS expectation).

        The store is declared at its full extent up front because backfill
        writes disjoint regions rather than appending. The time axis is
        written from the inventory's exact per-granule values, read from
        the files at inventory build time. Workers match each granule
        against that axis, so a granule missing from the inventory is
        rejected rather than misplaced. The session must have no
        uncommitted changes after this returns, so that forks taken from a
        fresh session share the committed branch-tip snapshot as their
        base.

        The `backfill` branch must not already exist. This method is intended
        to be called exactly once per backfill run.

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

        Storage is chosen by the implementation (e.g. S3 in a deployed Lambda,
        local filesystem in tests). Must use durable, shared storage — a pickled
        ForkSession cannot resolve its base snapshot from in-memory storage.
        Uses open_or_create semantics so the `main` branch exists for
        initialize_backfill_store to branch off.

        Returns
        -------
        Repository
            An Icechunk Repository backed by durable storage.
        """
        ...

    def process_backfill_file(self, file_key: str, fork: ForkSession) -> bool:
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
        Run Icechunk garbage collection and snapshot removal.

        Parameters
        ----------
            repo: And Icechunk Repository.
            expiry_time: Remove snapshots older than this time.
        Returns
        -------
        GCSummary
        """
        ...

    # def cron_processing(self, store: IcechunkStore) -> str:
    # """
    # Variable level operations that need to be run periodically and then
    # released as a tag.

    # Parameters
    # ----------
    # store: And Icechunk store.
    # Returns
    # -------
    # str
    # """
    # ...
