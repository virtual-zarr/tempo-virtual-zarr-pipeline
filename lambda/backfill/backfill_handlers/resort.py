"""Handler: fold the pending ledger into the store (the re-sort job).

Merges the store manifest with the pending ledger into one sorted
inventory (collisions abort before any branch is touched), rewrites the
axis on a ``resort`` branch, relocates already-ingested slots with a
metadata-only chunk reindex, parses only the inserted granules,
validates, promotes ``main`` by compare-and-swap, updates the manifest,
and clears the folded ledger entries.

One run folds at most ``$RESORT_MAX_FOLD`` pending granules (earliest
first); the rest stay in the ledger for the next run. Each promoted run
is durable partial progress, and deep insertions stay cheap because the
shifted suffix moves as references, not re-parsed data.
"""

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

DEFAULT_MAX_FOLD = 500


@logger.inject_lambda_context()
@tracer.capture_lambda_handler
def handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:
    manifest_uri = os.environ["STORE_MANIFEST_URI"]
    ledger_uri = os.environ["PENDING_LEDGER_URI"]
    max_fold = int(os.environ.get("RESORT_MAX_FOLD", DEFAULT_MAX_FOLD))

    pending = PendingLedger.read(ledger_uri)
    if not pending:
        logger.info("Pending ledger is empty; nothing to resort")
        return {"resorted": False, "reason": "ledger empty"}
    fold = sorted(pending, key=lambda entry: entry.time)[:max_fold]

    processor = Processor()
    repo = processor.open_backfill_repo()
    manifest = StoreManifest.read(manifest_uri)

    axis = np.asarray(
        zarr.open_array(repo.readonly_session("main").store, path="time")[:]
    )
    # The manifest is used to relocate existing granules, so it must
    # describe the store exactly.
    StoreManifest.validate_against_axis(manifest, axis)

    merged = merge_pending(manifest, fold)  # collisions raise here
    shift_index = first_shifted_index(manifest, merged)
    logger.info(
        "Resorting",
        extra={
            "pending": len(pending),
            "folding": len(fold),
            "first_shifted_index": shift_index,
            "relocations": len(manifest.granules) - shift_index,
        },
    )

    init_result = processor.initialize_resort_store(repo, merged)
    session = repo.writable_session("resort")
    # Already-ingested slots move as chunk references; only the inserted
    # granules are parsed from source.
    processor.reindex_resort_slots(session, manifest, merged)
    fold_urs = {entry.granule_ur for entry in fold}
    for entry in merged.granules:
        if entry.granule_ur not in fold_urs:
            continue
        if not processor.process_resort_file(entry.url, session):
            raise RuntimeError(f"resort insert failed for {entry.url}")
    session.commit(
        f"Resort: insert {len(fold)} granules, "
        f"relocate slots {shift_index}..{len(merged.granules) - 1}"
    )

    processor.validate_backfill_store(repo, merged, branch="resort")
    backfill.promote(
        repo, source="resort", expected_target_tip=init_result.branched_from
    )
    StoreManifest.write(manifest_uri, merged)
    PendingLedger.remove(ledger_uri, fold_urs)
    remaining = len(pending) - len(fold)
    if remaining:
        logger.info(
            "Resort promoted to main; ledger not yet drained",
            extra={"remaining": remaining},
        )
    else:
        logger.info("Resort promoted to main")
    return {
        "resorted": True,
        "inserted": len(fold),
        "remaining": remaining,
        "first_shifted_index": shift_index,
    }
