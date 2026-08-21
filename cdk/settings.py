from typing import Literal

from pydantic import model_validator
from pydantic_settings import BaseSettings


class StackSettings(BaseSettings):
    PROJECT_NAME: str = "virtualizarr-data-pipelines"
    STACK_NAME: str = "virtualizarr-data-pipelines"
    STAGE: Literal["dev", "prod"]
    # Optional: when blank, app.py falls back to CDK_DEFAULT_ACCOUNT (the account
    # of the active AWS credentials) so synth/deploy still resolve an environment.
    ACCOUNT_ID: str | None = None
    # The source data's region: asdc-prod-protected is in us-west-2, and
    # deploying elsewhere makes every worker read a cross-region transfer.
    ACCOUNT_REGION: str = "us-west-2"
    ICECHUNK_BUCKET_NAME: str = "icechunk-output"
    # Optional existing bucket for the Icechunk store and per-run backfill
    # artifacts. It must be in the stack's region; deployment checks its
    # actual region and fails otherwise.
    ICECHUNK_BUCKET: str | None = None
    # Common key prefix for every output written by this deployment. Backfill
    # artifacts are placed directly below it; ICECHUNK_PREFIX is relative to it.
    S3_PREFIX: str | None = None
    # Dataset-specific suffix for the Icechunk repo. Icechunk >=2.1.0 refuses to
    # create a repo at the bucket root, so S3_PREFIX or ICECHUNK_PREFIX must be
    # non-empty to bootstrap a new store. Passed into Lambda as the combined path.
    ICECHUNK_PREFIX: str | None = None
    # Key prefix in the Icechunk bucket where backfill inventories are uploaded
    # (see README: s3://<bucket>/<S3_PREFIX>/inventory/). The backfill partition
    # Lambda is granted read on this prefix only.
    INVENTORY_PREFIX: str | None = None
    DATA_BUCKET_NAME: str | None = None
    PROJECT: str = "virtualizarr-data-pipelines"
    SNS_TOPIC: str | None = None
    MAX_CONCURRENCY: int = 50
    SQS_BATCH_SIZE: int = 10

    # Secrets Manager secret holding Earthdata Login material (JSON with
    # "token" or "username"+"password"), exchanged for temporary S3
    # credentials when reading protected source granules. When unset, reads
    # rely on the Lambda role's ambient IAM access to the source bucket.
    EARTHDATA_SECRET_ARN: str | None = None

    # Frequency in days to run garbage collection (requires VPC_ID).
    GARBAGE_COLLECTION_FREQUENCY: int | None = None
    # Snapshots older than this are expired by a GC run. Also the rollback
    # window: a bad promote can only be reset to a snapshot younger than this.
    GC_EXPIRY_DAYS: int = 30

    # Email for CloudWatch alarm notifications (DLQ depth, scheduled-job
    # failures). Without it the alarms are console-only.
    ALARM_EMAIL: str | None = None

    # TEMPO collection this instance processes ("hcho" or "no2"); reaches every
    # processor Lambda as $TEMPO_COLLECTION. One repo per collection.
    TEMPO_COLLECTION: str | None = None
    # Virtual chunk container prefix (defaults inside the processor to
    # s3://asdc-prod-protected/).
    VIRTUAL_CHUNK_PREFIX: str | None = None
    # Forward-processing watermark artifact. When unset it defaults
    # to s3://<icechunk bucket>/<prefix>state/<name>.json, derived in the stack.
    POLL_WATERMARK_URI: str | None = None
    # First poll's starting point when no watermark exists yet (typically the
    # backfill inventory's build time). Unset falls back to a fixed lookback.
    POLL_START_ISO: str | None = None
    # Scheduled forward-processing jobs; deployed only when the forward queue is
    # enabled. None disables the individual schedule.
    RESORT_SCHEDULE_HOURS: int | None = 24
    POLL_SCHEDULE_MINUTES: int | None = 30
    # Max pending granules one re-sort run parses and inserts; the rest
    # stay in the ledger for the next run. Slot relocation is metadata-only
    # and not bounded by this.
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

    @property
    def s3_key_prefix(self) -> str | None:
        """Return the normalized global S3 key prefix."""
        return self.S3_PREFIX.strip("/") if self.S3_PREFIX else None

    @property
    def icechunk_storage_prefix(self) -> str | None:
        """Return the global and dataset-specific prefixes as one S3 key prefix."""
        return (
            "/".join(
                prefix.strip("/")
                for prefix in (self.S3_PREFIX, self.ICECHUNK_PREFIX)
                if prefix and prefix.strip("/")
            )
            or None
        )

    @property
    def inventory_prefix(self) -> str:
        """Key prefix the backfill partition Lambda may read inventories from."""
        if self.INVENTORY_PREFIX:
            return self.INVENTORY_PREFIX.strip("/")
        return "/".join(p for p in (self.s3_key_prefix, "inventory") if p)

    @model_validator(mode="after")
    def _validate_prefixes(self) -> "StackSettings":
        """Keep the Icechunk prefix relative to the global output prefix."""
        icechunk_prefix = (self.ICECHUNK_PREFIX or "").strip("/")
        if self.s3_key_prefix and (
            icechunk_prefix == self.s3_key_prefix
            or icechunk_prefix.startswith(f"{self.s3_key_prefix}/")
        ):
            raise ValueError("ICECHUNK_PREFIX must be relative to S3_PREFIX")
        return self

    @model_validator(mode="after")
    def _resolve_forward_queue_enabled(self) -> "StackSettings":
        if self.FORWARD_QUEUE_ENABLED is None:
            self.FORWARD_QUEUE_ENABLED = not self.BACKFILL_ENABLED
        return self
