"""Handler: validate the finished backfill store, then fast-forward main.

The promote gate (spec §4) runs before the fast-forward: the store must
match the resized template, the time axis must equal the inventory's
values bit-exactly and be strictly increasing, and the native lat/lon
chunks must equal the committed reference grid. A gate failure raises and
leaves `main` untouched.
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
    backfill.promote(repo)
    logger.info("Promoted main to backfill tip")
    return {"promoted": True}
