import os
from datetime import datetime, timedelta, timezone

from aws_lambda_powertools import Logger
from virtualizarr_processor.processor import Processor

logger = Logger()

DEFAULT_EXPIRY_DAYS = 30


def handler() -> None:
    # Exceptions must propagate: this runs as a Batch job, and a swallowed
    # failure would exit 0, defeating the job's retry policy and any
    # monitoring.
    virtualizarr_processor = Processor()
    expiry_days = int(os.environ.get("GC_EXPIRY_DAYS", DEFAULT_EXPIRY_DAYS))
    expiry_time = datetime.now(timezone.utc) - timedelta(days=expiry_days)
    summary = virtualizarr_processor.garbage_collect(expiry_time=expiry_time)
    logger.info(
        "Icechunk garbage collected",
        extra={"expiry_time": expiry_time.isoformat(), "summary": str(summary)},
    )
