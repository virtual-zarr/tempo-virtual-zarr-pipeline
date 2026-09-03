"""Handler: create the full-shape backfill store and commit (clean base)."""

from typing import Any

from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
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
    init_result = processor.initialize_backfill_store(
        repo, backfill_inventory, force=event.get("force") is True
    )
    logger.info(
        "Initialized backfill store",
        extra={
            "base_snapshot": init_result.snapshot,
            "branched_from": init_result.branched_from,
        },
    )
    # The promote step uses branched_from as its compare-and-swap
    # expectation for `main`.
    return {
        "base_snapshot": init_result.snapshot,
        "branched_from": init_result.branched_from,
    }
