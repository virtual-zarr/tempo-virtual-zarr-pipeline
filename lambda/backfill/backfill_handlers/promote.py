"""Handler: validate the finished backfill store, then promote main.

The promote gate runs before the branch move: the store must
match the resized template, the time axis must equal the inventory's
values bit-exactly and be strictly increasing, and the native lat/lon
chunks must equal the committed reference grid. A gate failure raises and
leaves `main` untouched. The move itself is compare-and-swap against the
`main` tip the Init step branched from (``branched_from`` in the event),
so a commit that landed on `main` during the run fails the promote loudly
instead of being discarded.
"""

import os
from typing import Any

from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from virtualizarr_processor import backfill
from virtualizarr_processor.manifest import StoreManifest
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
    manifest_uri = os.environ.get("STORE_MANIFEST_URI")
    if manifest_uri:
        # The backfill inventory becomes the store's living manifest, which
        # forward processing and the re-sort job maintain from here on.
        StoreManifest.write(manifest_uri, backfill_inventory)
        logger.info("Wrote store manifest", extra={"uri": manifest_uri})
    else:
        logger.warning("STORE_MANIFEST_URI not set; store manifest not written")
    return {"promoted": True}
