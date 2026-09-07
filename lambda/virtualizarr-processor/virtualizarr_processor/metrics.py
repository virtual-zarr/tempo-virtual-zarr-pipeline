"""EMF emission for the TempoPipeline custom metrics.

Single home for the metric identity the dashboard queries (see
_custom_metric in cdk/stack.py, which imports NAMESPACE and DIMENSION_ENV
from here): namespace TempoPipeline, dimensions exactly {Collection,
Stage} (falsy values dropped) plus explicit extras (e.g. Route). Emitters
and dashboard must agree, or the emission lands in a different CloudWatch
series and the widget/alarm silently shows nothing.

Emission is best-effort by design (see README): a metric must never fail
the batch, run, or verify it describes, so emit_metric logs and swallows
its own failures instead of every call site wrapping it.

aws-lambda-powertools is imported lazily: every Lambda package that calls
emit_metric ships it, while cdk/stack.py and scripts/verify_store.py
import only the identity constants (verify_store's transport is
put_metric_data — CodeBuild logs are not EMF-parsed).
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aws_lambda_powertools.metrics import MetricUnit

logger = logging.getLogger(__name__)

NAMESPACE = "TempoPipeline"

# Dimension name -> the settings field / env var that carries its value.
# Also drives the dashboard's dimension set, the Lambdas' env plumbing,
# and the backfill env allowlist (cdk/stack.py), so they cannot drift.
DIMENSION_ENV = {"Collection": "TEMPO_COLLECTION", "Stage": "STAGE"}


def metric_dimensions(**extra_dimensions: str) -> dict[str, str]:
    """The exact dimension set the dashboard queries, unset values dropped."""
    return {
        key: value
        for key, value in (
            *((name, os.environ.get(env)) for name, env in DIMENSION_ENV.items()),
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
    """One EMF blob (a JSON line on stdout, parsed by Lambda's log pipeline).

    Best-effort: an emission failure is logged and swallowed, never raised.
    """
    try:
        from aws_lambda_powertools.metrics import MetricUnit, single_metric

        with single_metric(
            name=name,
            unit=unit or MetricUnit.Count,
            value=value,
            namespace=NAMESPACE,
            default_dimensions=metric_dimensions(**extra_dimensions),
        ):
            pass
    except Exception:
        logger.warning("Skipping %s emission", name, exc_info=True)
