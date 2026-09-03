"""Handler: write one file-batch's virtual refs into a child fork on S3."""

import hashlib
import pickle
from typing import Any

from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from virtualizarr_processor.processor import Processor

from backfill_handlers import fork_store

logger = Logger()
tracer = Tracer()


@logger.inject_lambda_context()
@tracer.capture_lambda_handler
def handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:
    processor = Processor()
    shared = pickle.loads(fork_store.load_fork(event["fork_in_uri"]))
    child = shared.fork()
    for file_key in event["file_keys"]:
        if not processor.process_backfill_file(file_key, child):
            logger.error("Failed to process file", extra={"file_key": file_key})
            raise RuntimeError(f"process_backfill_file failed for {file_key}")

    # Deterministic, batch-keyed name: an SFN retry that re-runs this batch
    # overwrites its own fork instead of leaving a stale sibling for reduce
    # to merge last-writer-wins (review finding #9).
    batch_key = hashlib.sha256("\n".join(event["file_keys"]).encode()).hexdigest()
    child_fork_uri = f"{event['forks_out_prefix']}{batch_key}.pkl"
    fork_store.save_fork(child_fork_uri, pickle.dumps(child))
    logger.info("Wrote child fork", extra={"child_fork_uri": child_fork_uri})
    return {"child_fork_uri": child_fork_uri}
