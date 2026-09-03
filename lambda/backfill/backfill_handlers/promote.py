"""Handler: validate the finished backfill store, then promote main.

The `backfill` tip is looked up exactly once and pinned: the gate
validates that snapshot and the branch move promotes that snapshot, so a
concurrent run's Init resetting the branch mid-promote can never swap a
different (validated-looking but unfilled) snapshot in between the two —
the same pin the resort job uses.

The gate runs first: the store must match the resized template, the time
axis must equal the inventory's values bit-exactly and be strictly
increasing, the native lat/lon chunks must equal the committed reference
grid, the manifest arrays must equal the inventory, and every data array
must hold a chunk reference for every chunk of its grid (an unwritten
slot reads as fill values and passes every metadata check). A gate
failure raises and leaves `main` untouched. The manifest and an empty
pending ledger were already committed on the `backfill` branch by the
Init step, so the move is the entire promote: compare-and-swap against
``branched_from`` (the `main` tip the Init step branched from), so a
commit that landed on `main` mid-run fails the promote instead of being
discarded. Only branch cleanup runs after the CAS, and it cannot fail
the execution.
"""

from typing import Any

from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from virtualizarr_processor import backfill
from virtualizarr_processor.processor import Processor

from backfill_handlers import inventory

logger = Logger()
tracer = Tracer()


@logger.inject_lambda_context()
@tracer.capture_lambda_handler
def handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:
    processor = Processor()
    repo = processor.open_backfill_repo()
    backfill_inventory = inventory.read_inventory(event["inventory_uri"])
    # Pin the tip once: validate this snapshot, promote this snapshot.
    tip = repo.lookup_branch("backfill")
    processor.validate_backfill_store(
        repo, backfill_inventory, branch="backfill", snapshot_id=tip
    )
    backfill.promote(
        repo, source_snapshot=tip, expected_target_tip=event["branched_from"]
    )
    # main now points at the promoted snapshot; the work branch has served
    # its purpose. Deleting it keeps init's branch-exists check meaningful
    # (finding #8): existing == live or failed, never "finished". A failed
    # delete must not fail an execution whose promote already landed - the
    # next init will just ask for force. Only delete it if it still points
    # at the snapshot this run pinned and promoted: a concurrent run's
    # forced Init may have already reset "backfill" out from under us (the
    # same TOCTOU the pinned snapshot above guards for validate/promote),
    # and that run's fresh branch must not vanish underneath it.
    try:
        if repo.lookup_branch("backfill") == tip:
            repo.delete_branch("backfill")
    except Exception:
        logger.warning("promote succeeded but deleting 'backfill' failed")
    logger.info("Promoted main to backfill tip", extra={"snapshot": tip})
    return {"promoted": True}
