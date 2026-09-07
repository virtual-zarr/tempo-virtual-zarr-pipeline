"""EMF emission for the TempoPipeline custom metrics.

Single home for the metric identity the dashboard queries (see
_custom_metric in cdk/stack.py): namespace TempoPipeline, dimensions
exactly {Collection, Stage} (falsy values dropped) plus explicit extras
(e.g. Route). Emitters and dashboard must agree, or the emission lands in
a different CloudWatch series and the widget/alarm silently shows nothing.

aws-lambda-powertools is imported lazily: every Lambda package that calls
emit_metric ships it, while scripts/verify_store.py imports only NAMESPACE
and metric_dimensions (its transport is put_metric_data — CodeBuild logs
are not EMF-parsed).
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aws_lambda_powertools.metrics import MetricUnit

NAMESPACE = "TempoPipeline"


def metric_dimensions(**extra_dimensions: str) -> dict[str, str]:
    """The exact dimension set the dashboard queries, unset values dropped."""
    return {
        key: value
        for key, value in (
            ("Collection", os.environ.get("TEMPO_COLLECTION")),
            ("Stage", os.environ.get("STAGE")),
            *extra_dimensions.items(),
        )
        if value
    }


def emit_metric(
    name: str,
    value: float,
    unit: MetricUnit | None = None,
    **extra_dimensions: str,
) -> None:
    """One EMF blob (a JSON line on stdout, parsed by Lambda's log pipeline)."""
    from aws_lambda_powertools.metrics import MetricUnit, single_metric

    with single_metric(
        name=name,
        unit=unit or MetricUnit.Count,
        value=value,
        namespace=NAMESPACE,
        default_dimensions=metric_dimensions(**extra_dimensions),
    ):
        pass
