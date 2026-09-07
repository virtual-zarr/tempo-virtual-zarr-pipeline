import textwrap
from pathlib import Path
from typing import Any, cast

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
    aws_codebuild as codebuild,
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
from stack_constructs import (
    BackfillPipeline,
    BatchInfra,
    BatchJob,
    function_log_group,
    grant_prefixed_read_write,
)

# CDK-side twin of virtualizarr_processor.metrics.NAMESPACE (this package
# cannot import the Lambda code); tests/cdk/test_dashboard.py pins them equal.
METRIC_NAMESPACE = "TempoPipeline"


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

        # Dashboard widgets and alarms accumulate at each component's
        # construction site (so setting-gated components gate their own
        # widgets) and are assembled by _dashboard() at the end.
        self._widgets: list[cloudwatch.IWidget] = []
        self._alarms: list[cloudwatch.Alarm] = []
        # Custom-metric identity shared with the (deferred) emission side:
        # namespace TempoPipeline, dimensions Collection and Stage.
        self._metric_dimensions = {
            key: value
            for key, value in {
                "Collection": settings.TEMPO_COLLECTION,
                "Stage": settings.STAGE,
            }.items()
            if value
        }

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

        # Dashboard top-line tiles: is the store fresh, is the queue moving,
        # is anything dead-lettered, how far behind is the pending ledger.
        for title, metric in (
            ("Store freshness", self._custom_metric("AxisEndLag")),
            (
                "Queue oldest message age",
                self.queue.metric_approximate_age_of_oldest_message(
                    period=Duration.minutes(5), statistic="Maximum"
                ),
            ),
            (
                "DLQ depth",
                self.dlq.metric_approximate_number_of_messages_visible(
                    period=Duration.minutes(5), statistic="Maximum"
                ),
            ),
            ("Pending ledger depth", self._custom_metric("PendingLedgerDepth")),
        ):
            self._widgets.append(
                # One metric per tile: a second metric silently drops the
                # sparkline, which is what makes trends readable here.
                cloudwatch.SingleValueWidget(
                    title=title, metrics=[metric], sparkline=True, width=6, height=4
                )
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
            description="Icechunk bucket for backfill inventory under INVENTORY_PREFIX "
            "(default {S3_PREFIX}/inventory/). Partition Lambda has read-only access.",
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
            # Metric dimension for the TempoPipeline custom metrics.
            "STAGE": settings.STAGE,
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

        # Held for the dashboard's rejected-granules log-query widget.
        self.process_messages_log_group = function_log_group(
            self, "process-messages-logs"
        )
        self.process_messages_lambda = _lambda.DockerImageFunction(
            self,
            f"{settings.STACK_NAME}-process_messages_lambda",
            log_group=self.process_messages_log_group,
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

        # Dashboard forward-processing section (queue and consumer are
        # unconditional; the poller and re-sort widgets are appended inside
        # their setting-gated blocks and disappear with them).
        self._widgets.append(
            cloudwatch.TextWidget(markdown="## Forward processing", width=24, height=1)
        )
        self._widgets.append(
            cloudwatch.GraphWidget(
                title="Granule routing",
                stacked=True,
                width=12,
                height=6,
                left=[
                    self._custom_metric(
                        "GranulesRouted",
                        statistic="Sum",
                        period=Duration.minutes(30),
                        extra_dimensions={"Route": route},
                    )
                    for route in ("APPENDED", "OVERWRITTEN", "REJECTED", "PENDING")
                ],
            )
        )
        self._widgets.append(
            cloudwatch.GraphWidget(
                title="Consumer duration",
                width=12,
                height=6,
                left=[
                    self.process_messages_lambda.metric_duration(statistic=statistic)
                    for statistic in ("p50", "p95", "Maximum")
                ],
                right=[
                    self.process_messages_lambda.metric_throttles(statistic="Sum"),
                    self._custom_metric("CommitFailures", statistic="Sum"),
                ],
                # The 5-min function timeout is what kills an invocation; the
                # 1800 s SQS visibility timeout is only the redelivery bound.
                left_annotations=[
                    cloudwatch.HorizontalAnnotation(
                        value=300000,
                        label="Lambda timeout (5 min)",
                        color=cloudwatch.Color.RED,
                    )
                ],
            )
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

        grant_prefixed_read_write(
            self.process_messages_lambda,
            self.icechunk_bucket,
            [settings.icechunk_storage_prefix],
        )

        self.process_messages_lambda.add_event_source(
            lambda_event_sources.SqsEventSource(
                self.queue,
                batch_size=settings.SQS_BATCH_SIZE,
                report_batch_item_failures=True,
                # No max_concurrency: Lambda rejects a mapping whose maximum
                # exceeds the function's reserved concurrency (1, above), and
                # the setting's floor is 2. Excess pollers are throttled and
                # the event source scales itself down.
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

            grant_prefixed_read_write(
                self.initialize_icechunk_lambda,
                self.icechunk_bucket,
                [settings.icechunk_storage_prefix],
            )
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
            grant_prefixed_read_write(
                self.gc_job.role,
                self.icechunk_bucket,
                [settings.icechunk_storage_prefix],
            )
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
        self._build_inventory_project(settings)
        self._dashboard(settings)

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
                # Single-writer, same reason as the consumer: two concurrent
                # resort runs race to reset/promote the shared "resort"
                # branch, and the schedule alone doesn't rule out overlap
                # (a slow run plus a manual invoke, or async redelivery).
                reserved_concurrent_executions=1,
            )
            grant_prefixed_read_write(
                self.resort_lambda,
                self.icechunk_bucket,
                [settings.icechunk_storage_prefix],
            )
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
            self._widgets.append(
                cloudwatch.GraphWidget(
                    title="Re-sort",
                    width=12,
                    height=6,
                    left=[
                        self._custom_metric("FoldedGranules", statistic="Sum"),
                        self._custom_metric("PromoteFailures", statistic="Sum"),
                    ],
                    # A run killed by the Lambda timeout mid-fold emits no
                    # error metric (the pending ledger just grows silently);
                    # duration trending toward the 15-min line is the only
                    # early warning for that failure mode.
                    right=[self.resort_lambda.metric_duration(statistic="Maximum")],
                    left_annotations=[
                        cloudwatch.HorizontalAnnotation(
                            value=settings.RESORT_MAX_FOLD, label="RESORT_MAX_FOLD"
                        )
                    ],
                    right_annotations=[
                        cloudwatch.HorizontalAnnotation(
                            value=900000, label="Lambda timeout (15 min)"
                        )
                    ],
                )
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
            # The watermark lives in the icechunk bucket's state prefix,
            # under the store prefix.
            grant_prefixed_read_write(
                self.cmr_poller_lambda,
                self.icechunk_bucket,
                [settings.icechunk_storage_prefix],
            )
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
            self._widgets.append(
                cloudwatch.GraphWidget(
                    title="Poller",
                    width=12,
                    height=6,
                    left=[
                        self.cmr_poller_lambda.metric_invocations(statistic="Sum"),
                        self.cmr_poller_lambda.metric_errors(statistic="Sum"),
                    ],
                )
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
                inventory_prefix=settings.inventory_prefix,
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
                        # Metric dimension for the TempoPipeline metrics the
                        # partition/reduce/promote handlers emit.
                        "STAGE",
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

            # Dashboard backfill section, rendered only when the backfill
            # pipeline is deployed.
            state_machine = self.backfill_pipeline.state_machine
            self._widgets.append(
                cloudwatch.TextWidget(markdown="## Backfill", width=24, height=1)
            )
            self._widgets.append(
                cloudwatch.GraphWidget(
                    title="Backfill executions",
                    width=12,
                    height=6,
                    left=[
                        state_machine.metric_started(statistic="Sum"),
                        state_machine.metric_succeeded(statistic="Sum"),
                        state_machine.metric_failed(statistic="Sum"),
                    ],
                    right=[state_machine.metric_time(statistic="Maximum")],
                )
            )
            self._widgets.append(
                # Cumulative progress: RUNNING_SUM accumulates the per-bin
                # completion counts and FILL(REPEAT) carries the one-shot
                # PartitionsTotal point forward, so the two lines converge
                # as the backfill lands. (Per-bin division cannot work:
                # PartitionsTotal exists in exactly one 5-minute bin.) A
                # window spanning two backfill runs mixes their sums — fine
                # for its purpose of watching one run.
                cloudwatch.GraphWidget(
                    title="Backfill progress",
                    width=6,
                    height=6,
                    left=[
                        cloudwatch.MathExpression(
                            expression="RUNNING_SUM([done])",
                            label="partitions done",
                            using_metrics={
                                "done": self._custom_metric(
                                    "PartitionsDone", statistic="Sum"
                                )
                            },
                        ),
                        cloudwatch.MathExpression(
                            expression="FILL(total, REPEAT)",
                            label="partitions total",
                            using_metrics={
                                "total": self._custom_metric("PartitionsTotal")
                            },
                        ),
                    ],
                )
            )
            self._widgets.append(
                cloudwatch.GraphWidget(
                    title="Backfill worker failures",
                    width=6,
                    height=6,
                    left=[
                        self.backfill_pipeline.functions["worker"].metric_errors(
                            statistic="Sum"
                        )
                    ],
                )
            )

    def _build_inventory_project(self, settings: StackSettings) -> None:
        """CodeBuild project for reproducible in-region inventory builds.

        The DAAC's temporary S3 credentials only work from us-west-2, so
        ``build_backfill_inventory.py --access direct`` fails on a laptop.
        ``scripts/run_codebuild.sh`` uploads ``git archive HEAD`` as
        the project's source zip and starts a build, so every run is pinned
        by a commit plus the in-repo buildspec. Costs nothing while idle.

        The same project also runs ``scripts/verify_store.py`` when started
        with ``run_codebuild.sh -V`` (a ``--buildspec-override`` to
        ``scripts/verify_buildspec.yml``), which is why it carries the
        processor env and read access to the store prefix.
        """
        collection = settings.TEMPO_COLLECTION or "hcho"
        env = {
            "COLLECTION": codebuild.BuildEnvironmentVariable(value=collection),
            "S3_URI": codebuild.BuildEnvironmentVariable(
                value=f"s3://{self.icechunk_bucket.bucket_name}/"
                f"{settings.inventory_prefix}/{collection}.json"
            ),
            # Trial cap; override per build (empty = full inventory).
            "MAX_COUNT": codebuild.BuildEnvironmentVariable(value=""),
            # Extra flags for verify runs (scripts/verify_buildspec.yml via
            # --buildspec-override); empty for inventory builds.
            "VERIFY_ARGS": codebuild.BuildEnvironmentVariable(value=""),
        }
        # Verify runs open the store with the same env contract as the
        # Lambdas. setdefault keeps the Secrets-Manager EARTHDATA_TOKEN
        # entry (added below) authoritative over any plaintext collision.
        for key, value in self.processor_env.items():
            env.setdefault(key, codebuild.BuildEnvironmentVariable(value=value))
        if settings.EARTHDATA_SECRET_ARN:
            env["EARTHDATA_TOKEN"] = codebuild.BuildEnvironmentVariable(
                type=codebuild.BuildEnvironmentVariableType.SECRETS_MANAGER,
                value=f"{settings.EARTHDATA_SECRET_ARN}:EARTHDATA_TOKEN",
            )
        self.inventory_build = codebuild.Project(
            self,
            "InventoryBuild",
            source=codebuild.Source.s3(
                bucket=self.icechunk_bucket,
                path=f"{settings.inventory_prefix}/source.zip",
            ),
            build_spec=codebuild.BuildSpec.from_source_filename(
                "scripts/inventory_buildspec.yml"
            ),
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.AMAZON_LINUX_2023_5,
                compute_type=codebuild.ComputeType.SMALL,
            ),
            environment_variables=env,
            # The full ~13.6k-granule header sweep far exceeds the 1 h
            # CodeBuild default (and Lambda's 15 min ceiling).
            timeout=Duration.hours(8),
        )
        self.icechunk_bucket.grant_put(
            self.inventory_build, f"{settings.inventory_prefix}/*"
        )
        # Verify runs read the store; nothing in this project ever writes it.
        self.icechunk_bucket.grant_read(
            self.inventory_build,
            f"{settings.icechunk_storage_prefix}/*"
            if settings.icechunk_storage_prefix
            else "*",
        )
        # Verify runs publish CompletenessDelta via put_metric_data
        # (CodeBuild logs are not EMF-parsed). PutMetricData cannot be
        # resource-scoped; the namespace condition is the scoping.
        self.inventory_build.add_to_role_policy(
            iam.PolicyStatement(
                actions=["cloudwatch:PutMetricData"],
                resources=["*"],
                conditions={
                    "StringEquals": {"cloudwatch:namespace": METRIC_NAMESPACE}
                },
            )
        )
        if self.earthdata_secret is not None:
            self.earthdata_secret.grant_read(self.inventory_build)

        CfnOutput(
            self,
            "InventoryBuildProject",
            value=self.inventory_build.project_name,
            description="CodeBuild project for in-region inventory builds; "
            "start one with scripts/run_codebuild.sh",
        )

    def _alarm(
        self,
        construct_id: str,
        metric: cloudwatch.IMetric,
        description: str,
        *,
        threshold: float = 0,
        evaluation_periods: int = 1,
        treat_missing_data: cloudwatch.TreatMissingData = (
            cloudwatch.TreatMissingData.NOT_BREACHING
        ),
    ) -> cloudwatch.Alarm:
        """An alarm wired to the alarm topic (if any) and registered on the
        dashboard's alarm strip. Defaults give "anything above zero"."""
        alarm = cloudwatch.Alarm(
            self,
            construct_id,
            metric=metric,
            threshold=threshold,
            comparison_operator=(cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD),
            evaluation_periods=evaluation_periods,
            treat_missing_data=treat_missing_data,
            alarm_description=description,
        )
        if self.alarm_topic is not None:
            alarm.add_alarm_action(cloudwatch_actions.SnsAction(self.alarm_topic))
        self._alarms.append(alarm)
        return alarm

    def _custom_metric(
        self,
        metric_name: str,
        *,
        statistic: str = "Maximum",
        period: Duration | None = None,
        extra_dimensions: dict[str, str] | None = None,
    ) -> cloudwatch.Metric:
        """A custom metric the pipeline's handlers emit as CloudWatch EMF.

        Names and the {Collection, Stage} dimension set must match the
        emission side exactly (virtualizarr_processor.metrics.emit_metric,
        the shared helper every Lambda handler and verify_store.py use) —
        a mismatched name or an extra dimension is a different CloudWatch
        series, and the widget or alarm querying it shows nothing.
        """
        return cloudwatch.Metric(
            namespace=METRIC_NAMESPACE,
            metric_name=metric_name,
            dimensions_map={**self._metric_dimensions, **(extra_dimensions or {})},
            statistic=statistic,
            period=period or Duration.minutes(5),
        )

    def _dashboard(self, settings: StackSettings) -> None:
        """The data-quality widgets, the staleness alarm, and the dashboard
        assembled from every widget and alarm the components accumulated."""
        # Store freshness: AxisEndLag is seconds between now and the last
        # time-axis slot, emitted by the consumer after each commit and by
        # the re-sort after each promote. Missing data breaches because a
        # silently-dead poller or a re-sort killed by its timeout emits no
        # error metric — the series going quiet is the only signal. But the
        # emitters are event-driven and TEMPO is daylight-only, so the
        # series legitimately goes quiet overnight: only a full day of
        # consecutive missing-or-stale hours alarms. A backfill-only stack
        # has no freshness contract, so no alarm at all.
        if settings.FORWARD_QUEUE_ENABLED:
            self._alarm(
                "AxisEndLagAlarm",
                self._custom_metric("AxisEndLag", period=Duration.hours(1)),
                "The store's time axis is more than 24 h stale, "
                "or its freshness metric stopped arriving for 24 h",
                threshold=86400,
                evaluation_periods=24,
                treat_missing_data=cloudwatch.TreatMissingData.BREACHING,
            )

        # Dashboard data-quality section.
        self._widgets.append(
            cloudwatch.TextWidget(markdown="## Data quality", width=24, height=1)
        )
        self._widgets.append(
            # A sawtooth, not a continuous series: one point per verify run,
            # via put_metric_data (CodeBuild logs are not EMF-parsed).
            cloudwatch.GraphWidget(
                title="CMR-vs-store delta",
                width=8,
                height=6,
                left=[self._custom_metric("CompletenessDelta")],
            )
        )
        self._widgets.append(
            # The consumer's powertools Logger writes one structured JSON
            # line per granule ("Processed granule", with url and outcome
            # fields); this surfaces the rejected ones — the reason is in
            # the adjacent log lines.
            cloudwatch.LogQueryWidget(
                title="Rejected granules",
                width=16,
                height=6,
                log_group_names=[self.process_messages_log_group.log_group_name],
                view=cloudwatch.LogQueryVisualizationType.TABLE,
                query_lines=[
                    # Two shapes: a clean rejection logs "Processed granule"
                    # with outcome/url; a granule that *raises* mid-process
                    # logs only record_handler's error line with message_id.
                    # Both redeliver to the DLQ, so the runbook table shows
                    # both — coalesce gives whichever identifier the line has.
                    # Exclude the deliberate re-raise for standard rejections
                    # because its granule already appears via the outcome branch.
                    "fields @timestamp, coalesce(url, message_id) as granule, outcome",
                    "filter outcome = 'rejected'"
                    " or (message like 'Error processing record'"
                    " and message not like 'granule rejected')",
                    "sort @timestamp desc",
                    "limit 50",
                ],
            )
        )

        # Row wraps at the 24-column grid width, so the accumulated widgets
        # lay out band by band; the alarm strip renders first. The cast works
        # around this aws-cdk-lib version's Row stubs missing two IWidget
        # protocol members (warnings/warnings_v2); Row is an IWidget at runtime.
        row = cast(
            cloudwatch.IWidget,
            cloudwatch.Row(
                cloudwatch.AlarmStatusWidget(
                    alarms=list(self._alarms), width=24, height=2
                ),
                *self._widgets,
            ),
        )
        dashboard = cloudwatch.Dashboard(
            self,
            "Dashboard",
            dashboard_name=settings.STACK_NAME,
            widgets=[[row]],
        )
        CfnOutput(
            self,
            "DashboardUrl",
            value=(
                f"https://{self.region}.console.aws.amazon.com/cloudwatch/home"
                f"?region={self.region}#dashboards/dashboard/{dashboard.dashboard_name}"
            ),
            description="The stack's CloudWatch dashboard",
        )

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
