from typing import Any

from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from virtualizarr_processor.processor import Processor

logger = Logger()
tracer = Tracer()


@logger.inject_lambda_context()
@tracer.capture_lambda_handler
def handler(event: Any, context: LambdaContext) -> None:
    # Exceptions must propagate: this runs as a CloudFormation custom
    # resource, and a swallowed failure would report a successful deploy
    # with an uninitialized store.
    virtualizarr_processor = Processor()
    virtualizarr_processor.initialize_repo()
    logger.info("Icechunk initialized")
