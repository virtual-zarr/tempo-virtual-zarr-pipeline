from __future__ import annotations

import enum
from typing import NamedTuple


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
