import os
from typing import Any, Literal

from pydantic import model_validator
from pydantic_settings import BaseSettings

print("STAGE from env:", os.getenv("STAGE"))


def include_trailing_slash(value: Any) -> Any:
    """Make sure the value includes a trailing slash if str"""
    if isinstance(value, str):
        return value.rstrip("/") + "/"
    return value


class StackSettings(BaseSettings):
    PROJECT_NAME: str = "virtualizarr-data-pipelines"
    STACK_NAME: str = "virtualizarr-data-pipelines"
    STAGE: Literal["dev", "prod"]
    # Optional: when blank, app.py falls back to CDK_DEFAULT_ACCOUNT (the account
    # of the active AWS credentials) so synth/deploy still resolve an environment.
    ACCOUNT_ID: str | None = None
    # Default to the source data's region: asdc-prod-protected is in
    # us-west-2, and deploying elsewhere makes every worker read a
    # cross-region transfer (the largest avoidable backfill cost).
    ACCOUNT_REGION: str = "us-west-2"
    ICECHUNK_BUCKET_NAME: str = "icechunk-outuput"
    ICECHUNK_BUCKET: str | None = None
    # Key prefix for this dataset's repo. Icechunk >=2.1.0 refuses to CREATE a
    # repo at an empty prefix (bucket root), so this must be non-empty to bootstrap
    # a new store. Passed into the Lambda env as ICECHUNK_PREFIX.
    ICECHUNK_PREFIX: str | None = None
    DATA_BUCKET_NAME: str | None = None
    PROJECT: str = "virtualizarr-data-pipelines"
    SNS_TOPIC: str | None = None
    MAX_CONCURRENCY: int = 50
    SQS_BATCH_SIZE: int = 10

    # ARN of the Secrets Manager secret holding Earthdata Login material —
    # JSON with "token" or "username"+"password". The processor exchanges it
    # for temporary S3 credentials at the DAAC's s3credentials endpoint when
    # reading protected source granules. When unset, no secret resource or
    # IAM grant is created and reads rely on the Lambda role's ambient IAM
    # access to the source bucket.
    EARTHDATA_SECRET_ARN: str | None = None

    # Freguency in days to run garbage collection.
    GARBAGE_COLLECTION_FREQUENCY: int | None = None

    # TEMPO collection this instance processes ("hcho" or "no2"); reaches every
    # processor Lambda as $TEMPO_COLLECTION. One repo per collection.
    TEMPO_COLLECTION: str | None = None
    # Virtual chunk container prefix (defaults inside the processor to
    # s3://asdc-prod-protected/).
    VIRTUAL_CHUNK_PREFIX: str | None = None
    # Forward-processing state artifacts. When unset they default
    # to s3://<icechunk bucket>/<prefix>state/<name>.json, derived in the stack.
    STORE_MANIFEST_URI: str | None = None
    PENDING_LEDGER_URI: str | None = None
    POLL_WATERMARK_URI: str | None = None
    # Scheduled forward-processing jobs; deployed only when the forward queue is
    # enabled. None disables the individual schedule.
    RESORT_SCHEDULE_HOURS: int | None = 24
    POLL_SCHEDULE_MINUTES: int | None = 30
    # Max pending granules one re-sort run parses and inserts; the rest stay
    # in the ledger for the next run. Relocating already-ingested slots is
    # metadata-only and not bounded by this.
    RESORT_MAX_FOLD: int = 500

    VPC_ID: str | None = None
    # AWS Batch cluster reference to SSM parameter describing the AMI _or_ the AMI ID
    # If using SSM to resolve the AMI ID, prefix with `resolve:ssm`.
    # MCP_AMI_ID: str = "resolve:ssm:/mcp/amis/aml2023-ecs"
    AMI_ID: str = (
        "resolve:ssm:/aws/service/ecs/optimized-ami/amazon-linux-2/recommended/image_id"
    )

    # Cluster scaling max
    BATCH_MAX_VCPU: int = 10

    # Backfill (partitioned fork/merge) pipeline
    BACKFILL_ENABLED: bool = False
    BACKFILL_PARTITION_SIZE: int = 500
    BACKFILL_MAX_ITEMS_PER_BATCH: int = 10
    BACKFILL_MAX_CONCURRENCY: int = 50

    # Forward SQS consumer. `None` resolves in the validator below:
    #   backfill enabled  -> default disabled (bootstrap via backfill, enable later)
    #   backfill disabled -> default enabled  (normal forward-only deployment)
    FORWARD_QUEUE_ENABLED: bool | None = None

    @model_validator(mode="after")
    def _resolve_forward_queue_enabled(self) -> "StackSettings":
        if self.FORWARD_QUEUE_ENABLED is None:
            self.FORWARD_QUEUE_ENABLED = not self.BACKFILL_ENABLED
        return self
