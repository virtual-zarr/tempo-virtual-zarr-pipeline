"""Handler: validate the finished backfill store, then promote main.

The gate runs first: the store must match the resized template, the time
axis must equal the inventory's values bit-exactly and be strictly
increasing, the native lat/lon chunks must equal the committed reference
grid, and the manifest arrays must equal the inventory. A gate failure
raises and leaves `main` untouched. The manifest and an empty pending
ledger were already committed on the `backfill` branch by the Init step,
so the move is the entire promote: compare-and-swap against
``branched_from`` (the `main` tip the Init step branched from), so a
commit that landed on `main` mid-run fails the promote instead of being
discarded. Nothing runs after the CAS.
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
    processor.validate_backfill_store(repo, backfill_inventory, branch="backfill")
    backfill.promote(repo, expected_target_tip=event["branched_from"])
    logger.info("Promoted main to backfill tip")
    return {"promoted": True}
