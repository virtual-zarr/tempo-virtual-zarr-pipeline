"""CDK assertions for the forward-processing operational pieces (spec §5)."""

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
                            "STORE_MANIFEST_URI": Match.any_value(),
                            "PENDING_LEDGER_URI": Match.any_value(),
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
