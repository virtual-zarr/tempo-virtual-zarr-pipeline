"""Handler: fold the pending ledger into the store (the re-sort job).

Merges the store manifest with the pending ledger into one sorted
inventory (collisions abort loudly before any branch is touched), rewrites
the axis on a ``resort`` branch, re-ingests every granule from the first
shifted slot on, validates, fast-forwards ``main``, updates the store
manifest, and clears the folded ledger entries.

This inline implementation re-ingests the shifted suffix serially in one
Lambda invocation, which covers the routine cases (adjacent-scan swaps
shift only the tail; a daily drip-feed batch shifts more but stays
bounded).
"""

# ponytail: serial suffix rewrite in one invocation; if a deep resort ever
# exceeds the Lambda time limit, run the backfill state machine over the
# suffix on the resort branch instead (same worker path, distributed map).

import os
from typing import Any

import numpy as np
import zarr
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from virtualizarr_processor import backfill
from virtualizarr_processor.manifest import PendingLedger, StoreManifest
from virtualizarr_processor.processor import Processor
from virtualizarr_processor.resort import first_shifted_index, merge_pending

logger = Logger()
tracer = Tracer()


@logger.inject_lambda_context()
@tracer.capture_lambda_handler
def handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:
    manifest_uri = os.environ["STORE_MANIFEST_URI"]
    ledger_uri = os.environ["PENDING_LEDGER_URI"]

    pending = PendingLedger.read(ledger_uri)
    if not pending:
        logger.info("Pending ledger is empty; nothing to resort")
        return {"resorted": False, "reason": "ledger empty"}

    processor = Processor()
    repo = processor.open_backfill_repo()
    manifest = StoreManifest.read(manifest_uri)

    axis = np.asarray(
        zarr.open_array(repo.readonly_session("main").store, path="time")[:]
    )
    # Trust boundary: the manifest must describe the store exactly before
    # it is used to relocate existing granules.
    StoreManifest.validate_against_axis(manifest, axis)

    merged = merge_pending(manifest, pending)  # collisions raise here
    shift_index = first_shifted_index(manifest, merged)
    logger.info(
        "Resorting",
        extra={
            "pending": len(pending),
            "first_shifted_index": shift_index,
            "rewrites": len(merged.granules) - shift_index,
        },
    )

    processor.initialize_resort_store(repo, merged)
    session = repo.writable_session("resort")
    for entry in merged.granules[shift_index:]:
        if not processor.process_resort_file(entry.url, session):
            raise RuntimeError(f"resort rewrite failed for {entry.url}")
    session.commit(f"Resort: rewrite slots {shift_index}..{len(merged.granules) - 1}")

    processor.validate_backfill_store(repo, merged, branch="resort")
    backfill.promote(repo, source="resort")
    StoreManifest.write(manifest_uri, merged)
    PendingLedger.remove(ledger_uri, [entry.granule_ur for entry in pending])
    logger.info("Resort promoted to main")
    return {
        "resorted": True,
        "inserted": len(pending),
        "first_shifted_index": shift_index,
    }
