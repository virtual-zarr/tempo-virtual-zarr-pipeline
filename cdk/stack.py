import textwrap
from pathlib import Path
from typing import Any

from aws_cdk import (
    CfnOutput,
    CustomResource,
    Duration,
    RemovalPolicy,
    Stack,
    Tags,
)
from aws_cdk import (
    aws_cloudwatch as cloudwatch,
)
from aws_cdk import (
    aws_cloudwatch_actions as cloudwatch_actions,
)
from aws_cdk import (
    aws_ec2 as ec2,
)
from aws_cdk import (
    aws_ecr_assets as ecr_assets,
)
from aws_cdk import (
    aws_events as events,
)
from aws_cdk import (
    aws_events_targets as targets,
)
from aws_cdk import (
    aws_iam as iam,
)
from aws_cdk import (
    aws_lambda as _lambda,
)
from aws_cdk import (
    aws_lambda_event_sources as lambda_event_sources,
)
from aws_cdk import (
    aws_s3 as s3,
)
from aws_cdk import (
    aws_secretsmanager as secretsmanager,
)
from aws_cdk import (
    aws_sns as sns,
)
from aws_cdk import (
    aws_sns_subscriptions as subscriptions,
)
from aws_cdk import (
    aws_sqs as sqs,
)
from aws_cdk import custom_resources as cr
from constructs import Construct
from settings import StackSettings  # type: ignore[import-not-found]
from stack_constructs import BackfillPipeline, BatchInfra, BatchJob, function_log_group


def _concept_id(collection_name: str) -> str:
    """The CMR concept id from the collection's declarative TOML."""
    import tomllib

    path = (
        Path(__file__).parent.parent
        / "lambda"
        / "virtualizarr-processor"
        / "virtualizarr_processor"
        / "collections"
        / f"{collection_name}.toml"
    )
    return str(tomllib.loads(path.read_text())["concept_id"])


class VirtualizarrSqsStack(Stack):
    def __init__(
        self: Any,
        scope: Construct,
        construct_id: str,
        settings: StackSettings,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        Tags.of(self).add("Project", settings.PROJECT)

        self.dlq = sqs.Queue(
            self,
            f"{settings.STACK_NAME}-Dlq",
            queue_name=f"{settings.STACK_NAME}-Dlq",
            retention_period=Duration.days(14),
        )

        self.queue = sqs.Queue(
            self,
            f"{settings.STACK_NAME}-queue",
            queue_name=f"{settings.STACK_NAME}-queue",
            visibility_timeout=Duration.seconds(1800),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=20,
                queue=self.dlq,
            ),
        )
        # Backfill run artifacts (fork pickles, partition manifests) live
        # under <S3_PREFIX>/backfill/<execution>/ and are per-run scratch.
        s3_prefix = settings.s3_key_prefix
        run_artifact_prefix = f"{s3_prefix}/backfill/" if s3_prefix else "backfill/"

        # Failure states here are fail-safe but silent (rejected granules,
        # failing scheduled jobs); alarms make them visible.
        self.alarm_topic: sns.Topic | None = None
        if settings.ALARM_EMAIL:
            self.alarm_topic = sns.Topic(self, "AlarmTopic")
            self.alarm_topic.add_subscription(
                subscriptions.EmailSubscription(settings.ALARM_EMAIL)
            )
        self._alarm(
            "DlqMessagesAlarm",
            self.dlq.metric_approximate_number_of_messages_visible(
                period=Duration.minutes(5), statistic="Maximum"
            ),
            "Granules were rejected to the dead-letter queue",
        )

        if settings.ICECHUNK_BUCKET:
            self.icechunk_bucket = s3.Bucket.from_bucket_name(
                self,
                f"{settings.STACK_NAME}-bucket",
                bucket_name=settings.ICECHUNK_BUCKET,
            )
            self._validate_bucket_region(settings)
        else:
            dev = settings.STAGE == "dev"
            self.icechunk_bucket = s3.Bucket(
                self,
                f"{settings.STACK_NAME}-bucket",
                bucket_name=settings.ICECHUNK_BUCKET_NAME,
                # Expire run artifacts so repeated runs do not accumulate.
                lifecycle_rules=[
                    s3.LifecycleRule(
                        prefix=run_artifact_prefix, expiration=Duration.days(30)
                    )
                ],
                # dev stores are disposable: `cdk destroy` empties and deletes
                # the bucket. prod keeps the default RETAIN so the store
                # outlives the stack.
                removal_policy=RemovalPolicy.DESTROY if dev else RemovalPolicy.RETAIN,
                auto_delete_objects=dev,
            )

        CfnOutput(
            self,
            "IcechunkBucketName",
            value=self.icechunk_bucket.bucket_name,
            description="Icechunk store bucket. Upload the backfill inventory here "
            "(the partition Lambda has read access to this bucket).",
        )

        # Forward-processing state artifacts live next to the repo unless
        # overridden.
        storage_prefix = settings.icechunk_storage_prefix
        state_prefix = (
            f"s3://{self.icechunk_bucket.bucket_name}/"
            f"{storage_prefix + '/' if storage_prefix else ''}state/"
        )
        self.poll_watermark_uri = (
            settings.POLL_WATERMARK_URI or f"{state_prefix}cmr-watermark.json"
        )

        # Shared processor env: resolved by virtualizarr_processor at runtime to
        # open the icechunk store (ICECHUNK_BUCKET set => S3) and to read protected
        # granules via Earthdata (EARTHDATA_SECRET_ARN).
        self.processor_env = {
            "ICECHUNK_BUCKET": self.icechunk_bucket.bucket_name,
            "ICECHUNK_REGION": settings.ACCOUNT_REGION,
        }
        if settings.TEMPO_COLLECTION:
            self.processor_env["TEMPO_COLLECTION"] = settings.TEMPO_COLLECTION
        if settings.VIRTUAL_CHUNK_PREFIX:
            self.processor_env["VIRTUAL_CHUNK_PREFIX"] = settings.VIRTUAL_CHUNK_PREFIX
        if storage_prefix:
            self.processor_env["ICECHUNK_PREFIX"] = storage_prefix
        if settings.EARTHDATA_SECRET_ARN:
            self.processor_env["EARTHDATA_SECRET_ARN"] = settings.EARTHDATA_SECRET_ARN

        self.earthdata_secret = (
            secretsmanager.Secret.from_secret_complete_arn(
                self, "EarthdataSecret", settings.EARTHDATA_SECRET_ARN
            )
            if settings.EARTHDATA_SECRET_ARN
            else None
        )

        if settings.SNS_TOPIC:
            self.sns_topic = sns.Topic.from_topic_arn(
                self,
                f"{settings.STACK_NAME}-sns-topic",
                topic_arn=settings.SNS_TOPIC,
            )

            self.sns_topic.add_subscription(
                subscriptions.SqsSubscription(
                    self.queue,
                    raw_message_delivery=True,
                )
            )

        self.process_messages_lambda = _lambda.DockerImageFunction(
            self,
            f"{settings.STACK_NAME}-process_messages_lambda",
            log_group=function_log_group(self, "process-messages-logs"),
            code=_lambda.DockerImageCode.from_image_asset(
                directory="lambda",
                file="process_messages/Dockerfile",
                platform=ecr_assets.Platform.LINUX_AMD64,  # or LINUX_AMD64
            ),
            architecture=_lambda.Architecture.X86_64,
            timeout=Duration.minutes(5),
            memory_size=2048,
            environment=dict(self.processor_env),
            # Single-writer: concurrent consumers conflict on the append
            # resize and the store-manifest update, and SQS max_concurrency
            # cannot go below 2.
            reserved_concurrent_executions=1,
        )

        self._alarm(
            "ConsumerErrorsAlarm",
            self.process_messages_lambda.metric_errors(period=Duration.minutes(5)),
            "The forward-processing consumer failed",
        )

        self.queue.grant_consume_messages(self.process_messages_lambda)
        if self.earthdata_secret is not None:
            self.earthdata_secret.grant_read(self.process_messages_lambda)

        # The consumer reads source granules; without a bucket name the
        # policy below would target the literal bucket "None".
        if settings.FORWARD_QUEUE_ENABLED and not settings.DATA_BUCKET_NAME:
            raise ValueError(
                "DATA_BUCKET_NAME must be set when the forward queue is "
                "enabled; the consumer reads source granules from it"
            )
        if settings.DATA_BUCKET_NAME:
            self.process_messages_lambda.add_to_role_policy(
                iam.PolicyStatement(
                    actions=[
                        "s3:GetObject",
                        "s3:ListBucket",
                    ],
                    resources=[
                        f"arn:aws:s3:::{settings.DATA_BUCKET_NAME}/*",
                        f"arn:aws:s3:::{settings.DATA_BUCKET_NAME}",
                    ],
                )
            )

        self.icechunk_bucket.grant_read_write(self.process_messages_lambda)

        self.process_messages_lambda.add_event_source(
            lambda_event_sources.SqsEventSource(
                self.queue,
                batch_size=settings.SQS_BATCH_SIZE,
                report_batch_item_failures=True,
                max_concurrency=settings.MAX_CONCURRENCY,
                enabled=settings.FORWARD_QUEUE_ENABLED,
            )
        )

        if settings.FORWARD_QUEUE_ENABLED:
            self._forward_ops(settings)

        # When backfill is enabled, initialize_backfill_store (the Step Functions
        # Init step) is the sole store bootstrap. Skipping the deploy-time seed
        # avoids a create_array("foo", ...) collision on `main`.
        if not settings.BACKFILL_ENABLED:
            self.initialize_icechunk_lambda = _lambda.DockerImageFunction(
                self,
                f"{settings.STACK_NAME}-initialize-icechunk-lambda",
                log_group=function_log_group(self, "initialize-icechunk-logs"),
                code=_lambda.DockerImageCode.from_image_asset(
                    directory="lambda",
                    file="initialize/Dockerfile",
                    platform=ecr_assets.Platform.LINUX_AMD64,  # or LINUX_AMD64
                ),
                architecture=_lambda.Architecture.X86_64,
                timeout=Duration.minutes(5),
                memory_size=2048,
                environment=dict(self.processor_env),
            )

            self.icechunk_bucket.grant_read_write(self.initialize_icechunk_lambda)
            if self.earthdata_secret is not None:
                self.earthdata_secret.grant_read(self.initialize_icechunk_lambda)

            if settings.ICECHUNK_BUCKET:
                # Trigger it once on first deploy
                self.trigger = cr.AwsCustomResource(
                    self,
                    "TriggerOnce",
                    on_create=cr.AwsSdkCall(
                        service="Lambda",
                        action="invoke",
                        parameters={
                            "FunctionName": (
                                self.initialize_icechunk_lambda.function_name
                            ),
                            "InvocationType": "Event",
                        },
                        physical_resource_id=cr.PhysicalResourceId.of(
                            "trigger-once-id"
                        ),
                    ),
                    policy=cr.AwsCustomResourcePolicy.from_sdk_calls(
                        resources=[self.initialize_icechunk_lambda.function_arn]
                    ),
                )

                self.trigger.node.add_dependency(self.initialize_icechunk_lambda)
            else:
                self.custom_resource_provider = cr.Provider(
                    self,
                    "S3BucketCustomResourceProvider",
                    on_event_handler=self.initialize_icechunk_lambda,
                )

                self.bucket_custom_resource = CustomResource(
                    self,
                    "S3BucketCustomResource",
                    service_token=self.custom_resource_provider.service_token,
                    properties={
                        "BucketName": self.icechunk_bucket.bucket_name,
                    },
                )

                self.bucket_custom_resource.node.add_dependency(self.icechunk_bucket)

        if settings.GARBAGE_COLLECTION_FREQUENCY:
            if not settings.VPC_ID:
                raise ValueError(
                    "VPC_ID must be set when GARBAGE_COLLECTION_FREQUENCY is "
                    "set; the GC Batch cluster runs inside a VPC"
                )
            self.vpc = ec2.Vpc.from_lookup(self, "VPC", vpc_id=settings.VPC_ID)

            self.gc_image_asset = ecr_assets.DockerImageAsset(
                self,
                "GCImage",
                directory="lambda",
                file="garbage_collect/Dockerfile",
                platform=ecr_assets.Platform.LINUX_AMD64,
            )

            self.batch_infra = BatchInfra(
                self,
                "Batch-Infra",
                max_vcpu=settings.BATCH_MAX_VCPU,
                ami_id=settings.AMI_ID,
                vpc=self.vpc,
                stage=settings.STAGE,
                stack_name=settings.STACK_NAME,
            )

            self.gc_job = BatchJob(
                self,
                "GC-Job",
                vcpu=2,
                image_asset=self.gc_image_asset,
                memory_mb=2000,
                retry_attempts=1,
                environment=dict(
                    self.processor_env,
                    GC_EXPIRY_DAYS=str(settings.GC_EXPIRY_DAYS),
                ),
            )
            self.icechunk_bucket.grant_read_write(self.gc_job.role)
            if self.earthdata_secret is not None:
                self.earthdata_secret.grant_read(self.gc_job.role)

            self.cron_rule = events.Rule(
                self,
                "GarbageCollectionSchedule",
                schedule=events.Schedule.rate(
                    Duration.days(settings.GARBAGE_COLLECTION_FREQUENCY)
                ),
            )

            self.cron_rule.add_target(
                targets.BatchJob(
                    job_queue_arn=self.batch_infra.queue.job_queue_arn,
                    job_queue_scope=self.batch_infra.queue,
                    job_definition_arn=self.gc_job.job_def.job_definition_arn,
                    job_definition_scope=self.gc_job.job_def,
                    job_name="garbage-collection",
                )
            )

        self._build_backfill(settings)

    def _forward_ops(self, settings: StackSettings) -> None:
        """The scheduled forward-processing jobs: the re-sort job
        that folds the pending ledger in, and the CMR poller that feeds the
        queue (ASDC publishes no SNS topic)."""
        if settings.RESORT_SCHEDULE_HOURS:
            resort_env = dict(self.processor_env)
            resort_env["RESORT_MAX_FOLD"] = str(settings.RESORT_MAX_FOLD)
            self.resort_lambda = _lambda.DockerImageFunction(
                self,
                f"{settings.STACK_NAME}-resort-lambda",
                log_group=function_log_group(self, "resort-logs"),
                code=_lambda.DockerImageCode.from_image_asset(
                    directory="lambda",
                    file="backfill/Dockerfile",
                    platform=ecr_assets.Platform.LINUX_AMD64,
                    cmd=["backfill_handlers.resort.handler"],
                ),
                architecture=_lambda.Architecture.X86_64,
                timeout=Duration.minutes(15),
                # A deep resort's chunk-reference relocation builds the whole
                # shifted suffix's manifest updates in memory.
                memory_size=4096,
                environment=resort_env,
            )
            self.icechunk_bucket.grant_read_write(self.resort_lambda)
            if self.earthdata_secret is not None:
                self.earthdata_secret.grant_read(self.resort_lambda)
            if settings.DATA_BUCKET_NAME:
                # The re-sort re-virtualizes shifted granules from source.
                self.resort_lambda.add_to_role_policy(
                    iam.PolicyStatement(
                        actions=["s3:GetObject", "s3:ListBucket"],
                        resources=[
                            f"arn:aws:s3:::{settings.DATA_BUCKET_NAME}/*",
                            f"arn:aws:s3:::{settings.DATA_BUCKET_NAME}",
                        ],
                    )
                )
            events.Rule(
                self,
                "ResortSchedule",
                schedule=events.Schedule.rate(
                    Duration.hours(settings.RESORT_SCHEDULE_HOURS)
                ),
                targets=[targets.LambdaFunction(self.resort_lambda)],
            )
            # A failing re-sort otherwise just lets the pending ledger grow.
            self._alarm(
                "ResortErrorsAlarm",
                self.resort_lambda.metric_errors(period=Duration.hours(1)),
                "The scheduled re-sort job failed",
            )

        if settings.POLL_SCHEDULE_MINUTES:
            poller_env = {
                "QUEUE_URL": self.queue.queue_url,
                "POLL_WATERMARK_URI": self.poll_watermark_uri,
            }
            if settings.POLL_START_ISO:
                poller_env["POLL_START_ISO"] = settings.POLL_START_ISO
            if settings.TEMPO_COLLECTION:
                # Resolved at synth from the collection's declarative TOML so
                # the lightweight poller image needs no processor package.
                poller_env["CONCEPT_ID"] = _concept_id(settings.TEMPO_COLLECTION)
            self.cmr_poller_lambda = _lambda.DockerImageFunction(
                self,
                f"{settings.STACK_NAME}-cmr-poller-lambda",
                log_group=function_log_group(self, "cmr-poller-logs"),
                code=_lambda.DockerImageCode.from_image_asset(
                    directory="lambda",
                    file="cmr_poller/Dockerfile",
                    platform=ecr_assets.Platform.LINUX_AMD64,
                ),
                architecture=_lambda.Architecture.X86_64,
                timeout=Duration.minutes(10),
                memory_size=512,
                environment=poller_env,
            )
            self.queue.grant_send_messages(self.cmr_poller_lambda)
            # The watermark lives in the icechunk bucket's state prefix.
            self.icechunk_bucket.grant_read_write(self.cmr_poller_lambda)
            events.Rule(
                self,
                "CmrPollSchedule",
                schedule=events.Schedule.rate(
                    Duration.minutes(settings.POLL_SCHEDULE_MINUTES)
                ),
                targets=[targets.LambdaFunction(self.cmr_poller_lambda)],
            )
            # A failing poller silently stops feeding the queue.
            self._alarm(
                "PollerErrorsAlarm",
                self.cmr_poller_lambda.metric_errors(period=Duration.hours(1)),
                "The scheduled CMR poller failed",
            )

    def _build_backfill(self, settings: StackSettings) -> None:
        if settings.BACKFILL_ENABLED:
            if settings.DATA_BUCKET_NAME is None:
                raise ValueError(
                    "DATA_BUCKET_NAME must be set when BACKFILL_ENABLED is true; "
                    "the backfill workers need read access to the source bucket"
                )
            self.backfill_pipeline = BackfillPipeline(
                self,
                "BackfillPipeline",
                icechunk_bucket=self.icechunk_bucket,
                icechunk_prefix=settings.icechunk_storage_prefix,
                s3_prefix=settings.s3_key_prefix,
                data_bucket_name=settings.DATA_BUCKET_NAME,
                partition_size=settings.BACKFILL_PARTITION_SIZE,
                max_items_per_batch=settings.BACKFILL_MAX_ITEMS_PER_BATCH,
                max_concurrency=settings.BACKFILL_MAX_CONCURRENCY,
                earthdata_secret_arn=settings.EARTHDATA_SECRET_ARN,
                extra_env={
                    key: self.processor_env[key]
                    for key in (
                        "TEMPO_COLLECTION",
                        "VIRTUAL_CHUNK_PREFIX",
                    )
                    if key in self.processor_env
                },
            )

            CfnOutput(
                self,
                "BackfillStateMachineArn",
                value=self.backfill_pipeline.state_machine.state_machine_arn,
                description="Start a backfill with: aws stepfunctions start-execution "
                '--state-machine-arn <this> --input \'{"inventory_uri": "s3://..."}\'',
            )

    def _alarm(
        self, construct_id: str, metric: cloudwatch.IMetric, description: str
    ) -> cloudwatch.Alarm:
        """An "anything above zero" alarm, wired to the alarm topic if any."""
        alarm = cloudwatch.Alarm(
            self,
            construct_id,
            metric=metric,
            threshold=0,
            comparison_operator=(cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD),
            evaluation_periods=1,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
            alarm_description=description,
        )
        if self.alarm_topic is not None:
            alarm.add_alarm_action(cloudwatch_actions.SnsAction(self.alarm_topic))
        return alarm

    def _validate_bucket_region(self, settings: StackSettings) -> None:
        """Fail the deploy if the existing Icechunk bucket is in another region.

        An out-of-region bucket would silently make every store read and
        write a cross-region transfer.
        """
        validator = _lambda.Function(
            self,
            "ValidateIcechunkBucketRegionFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="index.handler",
            timeout=Duration.seconds(30),
            log_group=function_log_group(self, "validate-bucket-region-logs"),
            code=_lambda.Code.from_inline(
                textwrap.dedent(
                    """\
                    import boto3


                    def handler(event, _context):
                        if event["RequestType"] == "Delete":
                            return {
                                "PhysicalResourceId": event["PhysicalResourceId"]
                            }

                        bucket = event["ResourceProperties"]["BucketName"]
                        expected = event["ResourceProperties"]["ExpectedRegion"]
                        location = boto3.client("s3").get_bucket_location(
                            Bucket=bucket
                        )["LocationConstraint"]
                        actual = {None: "us-east-1", "EU": "eu-west-1"}.get(
                            location, location
                        )
                        if actual != expected:
                            raise ValueError(
                                f"Icechunk bucket {bucket!r} is in {actual!r}; "
                                f"expected {expected!r}"
                            )
                        return {"PhysicalResourceId": f"{bucket}:{actual}"}
                    """
                )
            ),
        )
        validator.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:GetBucketLocation"],
                resources=[self.icechunk_bucket.bucket_arn],
            )
        )
        provider = cr.Provider(
            self,
            "ValidateIcechunkBucketRegionProvider",
            on_event_handler=validator,
        )
        self.icechunk_bucket_region_validator = CustomResource(
            self,
            "ValidateIcechunkBucketRegion",
            service_token=provider.service_token,
            properties={
                "BucketName": self.icechunk_bucket.bucket_name,
                "ExpectedRegion": settings.ACCOUNT_REGION,
            },
        )
