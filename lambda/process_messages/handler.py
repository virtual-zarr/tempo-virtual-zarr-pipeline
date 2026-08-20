"""SQS consumer: parse, validate, and route granules into the store.

Messages come from the CMR poller (``{"url": "s3://..."}``) or, if an SNS
subscription is ever wired up, from S3 object-created notifications. Each
batch is pre-sorted by filename timestamp so adjacent scans arriving
together append in order. A REJECTED granule fails its record (SQS retry,
then DLQ); DEFERRED (out-of-order, recorded in the pending ledger) and
WRITTEN are both successful consumption.
"""

import json
from typing import Any, Dict, Optional

from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
)
from aws_lambda_powertools.utilities.batch.types import PartialItemFailureResponse
from aws_lambda_powertools.utilities.data_classes import SQSEvent, SQSRecord
from aws_lambda_powertools.utilities.typing import LambdaContext
from icechunk import Session
from virtualizarr_processor.processor import Processor
from virtualizarr_processor.typing import ProcessOutcome

logger = Logger()
tracer = Tracer()
batch_processor = BatchProcessor(event_type=EventType.SQS)


def granule_url(message: Dict[str, Any]) -> Optional[str]:
    """Extract the granule's s3:// url from either supported message shape."""
    if "url" in message:  # CMR poller message
        return str(message["url"])
    record = message.get("Records", [{}])[0].get("s3", {})
    bucket = record.get("bucket", {}).get("name")
    key = record.get("object", {}).get("key")
    if bucket and key:
        return f"s3://{bucket}/{key}"
    return None


def record_url(record: Dict[str, Any]) -> str:
    """Extract the url from a raw SQS record for batch sorting, best effort."""
    try:
        message = json.loads(record["body"])
        if "Message" in message:  # SNS envelope
            message = json.loads(message["Message"])
        return granule_url(message) or ""
    except Exception:
        return ""


@tracer.capture_method
def process_notification(
    message: Dict[str, Any],
    session: Session,
    processor: Processor,
) -> None:
    url = granule_url(message)
    if not url:
        logger.warning("Message carries no granule url; skipping", extra=message)
        return
    outcome = processor.process_file(file_key=url, session=session)
    logger.info("Processed granule", extra={"url": url, "outcome": outcome.value})
    if outcome is ProcessOutcome.REJECTED:
        raise RuntimeError(f"granule rejected: {url}")


@logger.inject_lambda_context()
@tracer.capture_lambda_handler
def handler(event: Any, context: LambdaContext) -> PartialItemFailureResponse:
    """Process a batch of granule notifications from the SQS queue."""
    sqs_event = SQSEvent(event)
    # Sort by filename (which encodes the nominal scan time) so adjacent
    # scans that arrived swapped within one batch still append in order.
    records = sorted(sqs_event.raw_event["Records"], key=record_url)
    virtualizarr_processor = Processor()
    # Refuses an uninitialized store: bootstrapping is the backfill's (or the
    # initialize Lambda's) job, never a side effect of consuming a message.
    repo = virtualizarr_processor.open_initialized_repo()
    session = virtualizarr_processor.initialize_session(repo=repo)

    @tracer.capture_method
    def record_handler(record: SQSRecord) -> None:
        try:
            message = json.loads(record.body)
            if "Message" in message:  # SNS envelope
                message = json.loads(message["Message"])
            process_notification(
                message=message,
                session=session,
                processor=virtualizarr_processor,
            )
        except Exception as e:
            logger.error(
                f"Error processing record: {str(e)}",
                extra={"message_id": record.message_id},
            )
            raise

    with batch_processor(records=records, handler=record_handler) as batch:
        batch.process()
    # Now attempt the commit (also updates the store manifest):
    try:
        snapshot_id = virtualizarr_processor.commit_processed_files(session=session)
        logger.info(f"Committed to {snapshot_id}")
    except Exception:
        # All records retry, including DEFERRED ones whose pending-ledger
        # write already persisted — safe only because PendingLedger.append
        # dedupes by granule UR on redelivery.
        logger.exception("Commit failed, marking all records as failed")
        return {
            "batchItemFailures": [
                {"itemIdentifier": record["messageId"]} for record in records
            ]
        }

    # Commit succeeded — return normal partial failure response
    # (only individually-failed records retry)
    return batch_processor.response()
