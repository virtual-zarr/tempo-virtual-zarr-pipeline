"""EMF emission for the TempoPipeline custom metrics.

The dimension set must stay exactly {Collection, Stage} plus any explicit
extras (e.g. Route): anything else is a different CloudWatch series,
invisible to the dashboard widgets and alarms that query these metrics.
"""

import os

from aws_lambda_powertools.metrics import MetricUnit, single_metric


def emit_metric(
    name: str,
    value: float,
    unit: MetricUnit = MetricUnit.Count,
    **extra_dimensions: str,
) -> None:
    """One EMF blob (a JSON line on stdout, parsed by Lambda's log pipeline)."""
    with single_metric(
        name=name,
        unit=unit,
        value=value,
        namespace="TempoPipeline",
        default_dimensions={
            key: val
            for key, val in (
                ("Collection", os.environ.get("TEMPO_COLLECTION")),
                ("Stage", os.environ.get("STAGE")),
                *extra_dimensions.items(),
            )
            if val
        },
    ):
        pass
