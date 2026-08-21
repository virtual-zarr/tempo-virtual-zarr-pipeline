"""Shared IAM grant helpers for the Icechunk bucket."""

from collections.abc import Sequence

from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3


def grant_prefixed_read_write(
    grantee: iam.IGrantable,
    bucket: s3.IBucket,
    prefixes: Sequence[str | None],
) -> None:
    """Grant object read/write and listing on ``bucket`` under ``prefixes``.

    The HCHO and NO2 stacks share this bucket, one key prefix each; scoping
    the grants keeps one stack's roles from writing the other stack's store.
    With no usable prefix the pre-existing bucket-wide grant is kept as the
    fallback.
    """
    keys = [p.strip("/") for p in prefixes if p and p.strip("/")]
    if not keys:
        bucket.grant_read_write(grantee)
        return
    principal = grantee.grant_principal
    principal.add_to_principal_policy(
        iam.PolicyStatement(
            actions=[
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:AbortMultipartUpload",
            ],
            resources=[bucket.arn_for_objects(f"{key}/*") for key in keys],
        )
    )
    principal.add_to_principal_policy(
        iam.PolicyStatement(
            actions=["s3:ListBucket"],
            resources=[bucket.bucket_arn],
            conditions={"StringLike": {"s3:prefix": [f"{key}/*" for key in keys]}},
        )
    )
