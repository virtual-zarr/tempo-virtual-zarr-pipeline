"""Handler: fold the pending ledger into the store (the re-sort job).

Pins ``main``'s tip first; every read (ledger, manifest, axis) comes from
that one snapshot, so a concurrent append landing mid-run moves the tip
out from under the promote's compare-and-swap instead of being silently
erased. Merges the pinned manifest with the pending ledger into one
sorted inventory (collisions abort before any branch is touched),
rewrites the axis on a ``resort`` branch, relocates already-ingested
slots with a metadata-only chunk reindex, parses only the inserted
granules, drains the folded ledger entries into the same fold commit,
validates, and promotes ``main`` by compare-and-swap against the pinned
tip. Nothing writes after the promote.

One run folds at most ``$RESORT_MAX_FOLD`` pending granules (earliest
first); the rest stay in the ledger for the next run. Each promoted run
is durable partial progress, and deep insertions stay cheap because the
shifted suffix moves as references, not re-parsed data.
"""

import os
from typing import Any

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
    max_fold = int(os.environ.get("RESORT_MAX_FOLD", DEFAULT_MAX_FOLD))

    processor = Processor()
    repo = processor.open_backfill_repo()
    # Pin main's tip FIRST; every read below comes from this snapshot, and
    # the promote CAS targets it, so an append landing mid-run fails the
    # CAS instead of being erased.
    tip = repo.lookup_branch("main")
    pinned = repo.readonly_session(snapshot_id=tip).store

    pending = PendingLedger.read(pinned)
    if not pending:
        logger.info("Pending ledger is empty; nothing to resort")
        return {"resorted": False, "reason": "ledger empty"}
    fold = sorted(pending, key=lambda entry: entry.time)[:max_fold]

    manifest = StoreManifest.read(pinned)
    if manifest is None:
        raise RuntimeError("store carries no manifest; is it initialized?")

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

    processor.initialize_resort_store(repo, merged, from_tip=tip)
    session = repo.writable_session("resort")
    # Already-ingested slots move as chunk references; only the inserted
    # granules are parsed from source.
    processor.reindex_resort_slots(session, manifest, merged)
    fold_urs = {entry.granule_ur for entry in fold}
    for entry in merged.granules:
        if entry.granule_ur not in fold_urs:
            continue
        if not processor.process_backfill_file(entry.url, session):
            raise RuntimeError(f"resort insert failed for {entry.url}")
    # Drain the folded entries inside the same commit that folds them.
    PendingLedger.write(
        session.store, [e for e in pending if e.granule_ur not in fold_urs]
    )
    # Capture this run's own commit and validate/promote *that* snapshot,
    # never the branch tip: a concurrent resort run can reset the "resort"
    # branch between this commit and the promote below (e.g. re-initializing
    # it for its own fold), and a tip lookup at that point would pick up the
    # other run's snapshot instead of this run's — validating and promoting
    # a store that was never actually folded or relocated.
    fold_snapshot = session.commit(
        f"Resort: insert {len(fold)} granules, "
        f"relocate slots {shift_index}..{len(merged.granules) - 1}"
    )

    processor.validate_backfill_store(
        repo, merged, branch="resort", snapshot_id=fold_snapshot
    )
    backfill.promote(
        repo, source="resort", source_snapshot=fold_snapshot, expected_target_tip=tip
    )
    remaining = len(pending) - len(fold)
    logger.info("Resort promoted to main", extra={"remaining": remaining})
    return {
        "resorted": True,
        "inserted": len(fold),
        "remaining": remaining,
        "first_shifted_index": shift_index,
    }
