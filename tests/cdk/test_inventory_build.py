"""CDK assertions for the in-region inventory CodeBuild project."""

import aws_cdk as cdk
from aws_cdk.assertions import Match, Template
from settings import StackSettings
from stack import VirtualizarrSqsStack


def _template(**overrides: object) -> Template:
    kwargs: dict = dict(
        STAGE="dev",
        ACCOUNT_ID="111111111111",
        ICECHUNK_BUCKET_NAME="ice-test",
        S3_PREFIX="tempo/hcho",
        ICECHUNK_PREFIX="v04",
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


def test_inventory_build_project() -> None:
    """The project builds from the inventory-prefix source zip with the
    in-repo buildspec, and outlives CodeBuild's 1 h default timeout (the
    full header sweep takes hours)."""
    template = _template()
    template.resource_count_is("AWS::CodeBuild::Project", 1)
    template.has_resource_properties(
        "AWS::CodeBuild::Project",
        Match.object_like(
            {
                "TimeoutInMinutes": 480,
                "Source": Match.object_like(
                    {
                        "Type": "S3",
                        "BuildSpec": "scripts/inventory_buildspec.yml",
                    }
                ),
            }
        ),
    )


def test_inventory_build_reads_earthdata_secret() -> None:
    """With EARTHDATA_SECRET_ARN set, the token is injected from Secrets
    Manager — required in accounts with no bucket-policy grant on the
    source bucket."""
    arn = "arn:aws:secretsmanager:us-west-2:111111111111:secret:tempo-earthdata-abc123"
    _template(EARTHDATA_SECRET_ARN=arn).has_resource_properties(
        "AWS::CodeBuild::Project",
        Match.object_like(
            {
                "Environment": Match.object_like(
                    {
                        "EnvironmentVariables": Match.array_with(
                            [
                                {
                                    "Name": "EARTHDATA_TOKEN",
                                    "Type": "SECRETS_MANAGER",
                                    "Value": f"{arn}:token",
                                }
                            ]
                        )
                    }
                )
            }
        ),
    )
