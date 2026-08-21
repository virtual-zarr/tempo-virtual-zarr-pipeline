"""Every role's Icechunk-bucket access is scoped to this stack's key prefixes.

The HCHO and NO2 stacks share one bucket, separated only by per-collection
prefixes; a bucket-wide write grant would let either stack corrupt the
other's store.
"""

import aws_cdk as cdk
import pytest
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk.assertions import Template
from conftest import actions_of, iam_statements, resources_of
from settings import StackSettings
from stack import VirtualizarrSqsStack

STORE_OBJECTS = "arn:<REF>:s3:::ice-test/tempo/hcho/v04/*"
BUCKET_ARN = "arn:<REF>:s3:::ice-test"
BUCKET_WIDE = "arn:<REF>:s3:::ice-test/*"
WRITE_ACTIONS = [
    "s3:GetObject",
    "s3:PutObject",
    "s3:DeleteObject",
    "s3:AbortMultipartUpload",
]


def test_grant_helper_scopes_and_falls_back() -> None:
    from stack_constructs import grant_prefixed_read_write

    app = cdk.App()
    stack = cdk.Stack(
        app, "T", env=cdk.Environment(account="111111111111", region="us-east-1")
    )
    bucket = s3.Bucket.from_bucket_name(stack, "B", "ice-test")
    scoped = iam.Role(
        stack, "ScopedRole", assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
    )
    wide = iam.Role(
        stack, "WideRole", assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
    )
    grant_prefixed_read_write(scoped, bucket, ["tempo/hcho/v04", None, ""])
    grant_prefixed_read_write(wide, bucket, [None])
    template = Template.from_stack(stack)

    scoped_stmts = list(iam_statements(template, "scopedrole"))
    assert any(
        actions_of(s) == WRITE_ACTIONS and resources_of(s) == [STORE_OBJECTS]
        for s in scoped_stmts
    )
    assert any(
        actions_of(s) == ["s3:ListBucket"]
        and resources_of(s) == [BUCKET_ARN]
        and s["Condition"] == {"StringLike": {"s3:prefix": ["tempo/hcho/v04/*"]}}
        for s in scoped_stmts
    )
    assert not any(BUCKET_WIDE in resources_of(s) for s in scoped_stmts)

    wide_stmts = list(iam_statements(template, "widerole"))
    assert any(
        BUCKET_WIDE in resources_of(s) and "s3:DeleteObject*" in actions_of(s)
        for s in wide_stmts
    )


def _stack_template(**overrides: object) -> Template:
    kwargs: dict[str, object] = dict(
        STAGE="dev",
        ACCOUNT_ID="111111111111",
        ICECHUNK_BUCKET="ice-test",
        DATA_BUCKET_NAME="data-test",
        TEMPO_COLLECTION="hcho",
        S3_PREFIX="tempo/hcho",
        ICECHUNK_PREFIX="v04",
        INVENTORY_PREFIX=None,
    )
    kwargs.update(overrides)
    settings = StackSettings(**{k: v for k, v in kwargs.items() if v is not None})
    app = cdk.App()
    stack = VirtualizarrSqsStack(
        app,
        settings.STACK_NAME,
        settings=settings,
        env={"account": settings.ACCOUNT_ID, "region": settings.ACCOUNT_REGION},
    )
    return Template.from_stack(stack)


@pytest.mark.parametrize(
    "marker",
    [
        "processmessageslambda",
        "initializeicechunk",
        "resortlambda",
        "cmrpollerlambda",
    ],
)
def test_stack_lambdas_scoped_to_store_prefix(marker: str) -> None:
    stmts = list(iam_statements(_stack_template(), marker))
    writes = [s for s in stmts if "s3:PutObject" in actions_of(s)]
    assert writes and all(resources_of(s) == [STORE_OBJECTS] for s in writes)
    assert any(
        "s3:ListBucket" in actions_of(s)
        and s.get("Condition") == {"StringLike": {"s3:prefix": ["tempo/hcho/v04/*"]}}
        for s in stmts
    )


@pytest.mark.parametrize("backfill", [False, True])
def test_no_bucket_wide_writes_remain_when_prefix_set(backfill: bool) -> None:
    template = _stack_template(BACKFILL_ENABLED=backfill)
    for stmt in iam_statements(template):
        if any(a.startswith(("s3:Put", "s3:Delete")) for a in actions_of(stmt)):
            assert BUCKET_WIDE not in resources_of(stmt)


def test_backfill_partition_gets_inventory_grant_from_settings() -> None:
    template = _stack_template(BACKFILL_ENABLED=True)
    stmts = list(iam_statements(template, "partitionfn"))
    assert any(
        resources_of(s) == ["arn:<REF>:s3:::ice-test/tempo/hcho/inventory/*"]
        and actions_of(s) == ["s3:GetObject"]
        for s in stmts
    )


def test_no_prefix_keeps_bucket_wide_grant() -> None:
    template = _stack_template(S3_PREFIX=None, ICECHUNK_PREFIX=None)
    stmts = list(iam_statements(template, "processmessageslambda"))
    assert any(
        BUCKET_WIDE in resources_of(s) and "s3:DeleteObject*" in actions_of(s)
        for s in stmts
    )


def test_gc_role_scoped_to_store_prefix() -> None:
    template = _stack_template(GARBAGE_COLLECTION_FREQUENCY=2, VPC_ID="vpc-12345")
    stmts = list(iam_statements(template, "gcjobtaskrole"))
    writes = [s for s in stmts if "s3:PutObject" in actions_of(s)]
    assert writes and all(resources_of(s) == [STORE_OBJECTS] for s in writes)
