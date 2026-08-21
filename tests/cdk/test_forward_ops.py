"""CDK assertions for the forward-processing operational pieces."""

import aws_cdk as cdk
from aws_cdk.assertions import Match, Template
from settings import StackSettings
from stack import VirtualizarrSqsStack


def _template(**overrides: object) -> Template:
    kwargs: dict = dict(
        STAGE="dev",
        ACCOUNT_ID="111111111111",
        ICECHUNK_BUCKET_NAME="ice-test",
        ICECHUNK_PREFIX="tempo-hcho/",
        DATA_BUCKET_NAME="data-test",
        TEMPO_COLLECTION="hcho",
        BACKFILL_ENABLED=False,
    )
    kwargs.update(overrides)
    settings = StackSettings(**kwargs)
    app = cdk.App()
    stack = VirtualizarrSqsStack(
        app,
        settings.STACK_NAME,
        settings=settings,
        env={"account": settings.ACCOUNT_ID, "region": settings.ACCOUNT_REGION},
    )
    return Template.from_stack(stack)


def test_consumer_is_single_writer() -> None:
    """Concurrent appends conflict on the array resize, and SQS
    max_concurrency cannot go below 2, so the consumer runs at reserved
    concurrency 1."""
    _template().has_resource_properties(
        "AWS::Lambda::Function",
        Match.object_like({"ReservedConcurrentExecutions": 1}),
    )


def test_forward_state_env_reaches_lambdas() -> None:
    template = _template()
    template.has_resource_properties(
        "AWS::Lambda::Function",
        Match.object_like(
            {
                "Environment": {
                    "Variables": Match.object_like(
                        {
                            "TEMPO_COLLECTION": "hcho",
                        }
                    )
                },
                "ReservedConcurrentExecutions": 1,
            }
        ),
    )


def test_resort_and_poller_are_scheduled() -> None:
    template = _template()
    # One rule for the daily resort, one for the CMR poller.
    template.resource_count_is("AWS::Events::Rule", 2)
    template.has_resource_properties(
        "AWS::Events::Rule",
        Match.object_like({"ScheduleExpression": "rate(1 day)"}),
    )
    template.has_resource_properties(
        "AWS::Events::Rule",
        Match.object_like({"ScheduleExpression": "rate(30 minutes)"}),
    )


def test_backfill_only_deployment_has_no_forward_jobs() -> None:
    template = _template(BACKFILL_ENABLED=True)
    template.resource_count_is("AWS::Events::Rule", 0)


def test_existing_bucket_gets_a_region_check() -> None:
    """An out-of-region bucket must fail the deploy, not silently incur
    cross-region transfer on every store read and write."""
    template = _template(ICECHUNK_BUCKET="existing-ice")
    template.has_resource_properties(
        "AWS::CloudFormation::CustomResource",
        Match.object_like(
            {"BucketName": "existing-ice", "ExpectedRegion": "us-west-2"}
        ),
    )


def test_created_bucket_has_no_region_check() -> None:
    template = _template()
    resources = template.to_json()["Resources"].values()
    assert not any("ExpectedRegion" in str(r.get("Properties", {})) for r in resources)


def test_s3_prefix_scopes_state_env_and_run_artifact_lifecycle() -> None:
    template = _template(S3_PREFIX="tempo")
    # The processor sees the combined prefix and state artifacts below it.
    template.has_resource_properties(
        "AWS::Lambda::Function",
        Match.object_like(
            {
                "Environment": {
                    "Variables": Match.object_like(
                        {"ICECHUNK_PREFIX": "tempo/tempo-hcho"}
                    )
                }
            }
        ),
    )
    # The state URI embeds the bucket ref, so match the serialized template.
    assert "tempo/tempo-hcho/state/cmr-watermark.json" in str(template.to_json())
    # The run-artifact lifecycle rule follows the scoped run prefix.
    template.has_resource_properties(
        "AWS::S3::Bucket",
        Match.object_like(
            {
                "LifecycleConfiguration": {
                    "Rules": [Match.object_like({"Prefix": "tempo/backfill/"})]
                }
            }
        ),
    )


def test_alarms_cover_dlq_consumer_and_scheduled_jobs() -> None:
    """Failure states are fail-safe but silent; alarms make them visible."""
    # Forward deployment: DLQ depth, consumer, re-sort, and poller errors.
    _template().resource_count_is("AWS::CloudWatch::Alarm", 4)
    # Backfill-only deployment: no scheduled forward jobs to watch.
    _template(BACKFILL_ENABLED=True).resource_count_is("AWS::CloudWatch::Alarm", 2)


def test_alarm_email_wires_an_sns_topic() -> None:
    _template().resource_count_is("AWS::SNS::Topic", 0)
    with_email = _template(ALARM_EMAIL="ops@example.com")
    with_email.resource_count_is("AWS::SNS::Topic", 1)
    with_email.has_resource_properties(
        "AWS::SNS::Subscription",
        Match.object_like({"Protocol": "email", "Endpoint": "ops@example.com"}),
    )


def test_forward_queue_requires_a_data_bucket() -> None:
    import pytest

    with pytest.raises(ValueError, match="DATA_BUCKET_NAME"):
        _template(DATA_BUCKET_NAME=None)


def test_gc_requires_a_vpc() -> None:
    import pytest

    with pytest.raises(ValueError, match="VPC_ID"):
        _template(GARBAGE_COLLECTION_FREQUENCY=2)
