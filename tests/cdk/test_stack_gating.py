import aws_cdk as cdk
import pytest
from aws_cdk.assertions import Template
from settings import StackSettings
from stack import VirtualizarrSqsStack


def _template(
    *, backfill: bool, forward: bool | None = None, stage: str = "dev"
) -> Template:
    kwargs = dict(
        STAGE=stage,
        ACCOUNT_ID="111111111111",
        ICECHUNK_BUCKET_NAME="ice-test",
        DATA_BUCKET_NAME="data-test",
        BACKFILL_ENABLED=backfill,
    )
    if forward is not None:
        kwargs["FORWARD_QUEUE_ENABLED"] = forward
    settings = StackSettings(**kwargs)
    app = cdk.App()
    stack = VirtualizarrSqsStack(
        app,
        settings.STACK_NAME,
        settings=settings,
        env={"account": settings.ACCOUNT_ID, "region": settings.ACCOUNT_REGION},
    )
    return Template.from_stack(stack)


def _synth(enabled: bool) -> Template:
    return _template(backfill=enabled)


def test_backfill_disabled_creates_no_state_machine() -> None:
    _synth(False).resource_count_is("AWS::StepFunctions::StateMachine", 0)


def test_backfill_enabled_creates_state_machine() -> None:
    _synth(True).resource_count_is("AWS::StepFunctions::StateMachine", 1)


def test_forward_queue_enabled_when_backfill_off() -> None:
    t = _template(backfill=False)
    t.resource_count_is("AWS::Lambda::EventSourceMapping", 1)
    t.has_resource_properties("AWS::Lambda::EventSourceMapping", {"Enabled": True})


def test_forward_queue_disabled_when_backfill_on() -> None:
    t = _template(backfill=True)
    t.resource_count_is("AWS::Lambda::EventSourceMapping", 1)
    t.has_resource_properties("AWS::Lambda::EventSourceMapping", {"Enabled": False})


def test_forward_queue_explicit_enable_with_backfill_on() -> None:
    t = _template(backfill=True, forward=True)
    t.resource_count_is("AWS::Lambda::EventSourceMapping", 1)
    t.has_resource_properties("AWS::Lambda::EventSourceMapping", {"Enabled": True})


def _resource_ids(template: Template) -> str:
    return " ".join(template.to_json()["Resources"].keys()).lower()


def test_backfill_disabled_creates_initialize_lambda() -> None:
    assert "initializeicechunk" in _resource_ids(_template(backfill=False))


def test_backfill_enabled_skips_initialize_lambda() -> None:
    assert "initializeicechunk" not in _resource_ids(_template(backfill=True))


def test_backfill_enabled_requires_data_bucket_name() -> None:
    settings = StackSettings(
        STAGE="dev",
        ACCOUNT_ID="111111111111",
        ICECHUNK_BUCKET_NAME="ice-test",
        BACKFILL_ENABLED=True,
    )
    app = cdk.App()
    with pytest.raises(ValueError, match="DATA_BUCKET_NAME"):
        VirtualizarrSqsStack(
            app,
            settings.STACK_NAME,
            settings=settings,
            env={"account": settings.ACCOUNT_ID, "region": settings.ACCOUNT_REGION},
        )


def test_dev_bucket_is_disposable() -> None:
    """`cdk destroy` of a dev stack must fully remove the created bucket."""
    t = _template(backfill=False, stage="dev")
    t.has_resource(
        "AWS::S3::Bucket",
        {"DeletionPolicy": "Delete", "UpdateReplacePolicy": "Delete"},
    )
    # auto_delete_objects wires the emptying custom resource.
    assert "autodeleteobjects" in _resource_ids(_template(backfill=False))


def test_prod_bucket_is_retained() -> None:
    t = _template(backfill=False, stage="prod")
    t.has_resource("AWS::S3::Bucket", {"DeletionPolicy": "Retain"})


def test_bucket_expires_backfill_run_artifacts() -> None:
    """Fork pickles and partition manifests under backfill/<execution>/ are
    per-run debris; a lifecycle rule keeps repeated runs from accumulating."""
    from aws_cdk.assertions import Match

    _template(backfill=True).has_resource_properties(
        "AWS::S3::Bucket",
        Match.object_like(
            {
                "LifecycleConfiguration": {
                    "Rules": [
                        Match.object_like(
                            {"Prefix": "backfill/", "ExpirationInDays": 30}
                        )
                    ]
                }
            }
        ),
    )


def test_lambdas_use_stack_owned_log_groups() -> None:
    """Implicit /aws/lambda/* log groups are never-expire and survive destroy;
    every function must own a bounded, stack-deleted log group instead."""
    t = _template(backfill=True, forward=True)
    tmpl = t.to_json()["Resources"]
    # The pipeline's own functions are all Docker images; CDK framework
    # helpers (e.g. the bucket auto-delete handler) are zip-based.
    functions = [
        r
        for r in tmpl.values()
        if r["Type"] == "AWS::Lambda::Function"
        and r["Properties"].get("PackageType") == "Image"
    ]
    log_groups = [r for r in tmpl.values() if r["Type"] == "AWS::Logs::LogGroup"]
    assert len(functions) == 9  # consumer, resort, poller + 6 backfill handlers
    assert len(log_groups) >= len(functions)
    for group in log_groups:
        assert group["Properties"]["RetentionInDays"] == 30
        assert group["DeletionPolicy"] == "Delete"
    for fn in functions:
        assert "LogGroup" in str(fn["Properties"].get("LoggingConfig", {}))
