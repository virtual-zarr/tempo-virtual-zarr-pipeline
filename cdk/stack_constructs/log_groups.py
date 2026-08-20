"""Stack-owned Lambda log groups.

Lambda's implicitly created ``/aws/lambda/<fn>`` log groups are
never-expire and survive ``cdk destroy``; giving every function an
explicit log group bounds retention and makes teardown complete.
"""

from aws_cdk import RemovalPolicy
from aws_cdk import aws_logs as logs
from constructs import Construct


def function_log_group(scope: Construct, construct_id: str) -> logs.LogGroup:
    return logs.LogGroup(
        scope,
        construct_id,
        retention=logs.RetentionDays.ONE_MONTH,
        removal_policy=RemovalPolicy.DESTROY,
    )
