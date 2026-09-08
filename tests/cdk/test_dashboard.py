"""CDK assertions for the in-stack CloudWatch dashboard."""

import json
import re
from pathlib import Path
from typing import Any

import aws_cdk as cdk
import pytest
from aws_cdk.assertions import Match, Template
from conftest import resolve_joins
from settings import StackSettings
from stack import METRIC_NAMESPACE, VirtualizarrSqsStack
from virtualizarr_processor.metrics import metric_dimensions

REPO = Path(__file__).resolve().parents[2]

# Sources whose emit_metric()/MetricName calls define the emitted names.
EMITTER_SOURCES = [
    REPO / "lambda/process_messages/handler.py",
    REPO / "lambda/backfill/backfill_handlers/partition.py",
    REPO / "lambda/backfill/backfill_handlers/reduce.py",
    REPO / "lambda/backfill/backfill_handlers/resort.py",
    REPO / "scripts/verify_store.py",
]
EMIT_CALL = re.compile(r'emit_metric\(\s*"(\w+)"|"MetricName":\s*"(\w+)"')


def _template(*, backfill: bool = False, forward: bool | None = None) -> Template:
    kwargs: dict = dict(
        STAGE="dev",
        ACCOUNT_ID="111111111111",
        STACK_NAME="tempo-hcho-dev",
        ICECHUNK_BUCKET_NAME="ice-test",
        DATA_BUCKET_NAME="data-test",
        TEMPO_COLLECTION="hcho",
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


def _widget_titles(template: Template) -> list[str]:
    """Titles (and text-header markdown) of every dashboard widget."""
    (dashboard,) = [
        r
        for r in template.to_json()["Resources"].values()
        if r["Type"] == "AWS::CloudWatch::Dashboard"
    ]
    body: dict[str, Any] = json.loads(
        resolve_joins(dashboard["Properties"]["DashboardBody"])
    )
    return [
        w["properties"].get("title") or w["properties"].get("markdown", "")
        for w in body["widgets"]
    ]


def _widget(template: Template, title: str) -> dict[str, Any]:
    (dashboard,) = [
        r
        for r in template.to_json()["Resources"].values()
        if r["Type"] == "AWS::CloudWatch::Dashboard"
    ]
    body = json.loads(resolve_joins(dashboard["Properties"]["DashboardBody"]))
    (widget,) = [w for w in body["widgets"] if w["properties"].get("title") == title]
    return widget


def test_dashboard_created() -> None:
    _template().resource_count_is("AWS::CloudWatch::Dashboard", 1)


def test_dashboard_name_is_stack_qualified() -> None:
    """hcho and no2 deploy the same code; the name must not collide."""
    _template().has_resource_properties(
        "AWS::CloudWatch::Dashboard", {"DashboardName": "tempo-hcho-dev"}
    )


def test_backfill_widgets_omitted_when_disabled() -> None:
    assert not any("Backfill" in t for t in _widget_titles(_template(backfill=False)))


def test_backfill_widgets_present_when_enabled() -> None:
    assert any("Backfill" in t for t in _widget_titles(_template(backfill=True)))


def test_axis_end_lag_alarm_requires_a_full_day_of_missing_or_stale() -> None:
    """TEMPO is daylight-only: the event-driven emitters legitimately go
    quiet overnight, so one missing hour must not page. Missing data still
    breaches (a dead poller or silently-killed re-sort stops the series),
    but only 24 consecutive breaching-or-missing hours alarm."""
    _template().has_resource_properties(
        "AWS::CloudWatch::Alarm",
        Match.object_like(
            {
                "MetricName": "AxisEndLag",
                "TreatMissingData": "breaching",
                "EvaluationPeriods": 24,
                "Threshold": 86400,
            }
        ),
    )


def test_axis_end_lag_alarm_omitted_without_forward_processing() -> None:
    """A backfill-only stack has no freshness contract; without this gate
    the alarm would be in ALARM permanently."""
    template = _template(backfill=True, forward=False)
    alarms = template.find_resources(
        "AWS::CloudWatch::Alarm", {"Properties": {"MetricName": "AxisEndLag"}}
    )
    assert not alarms


def test_backfill_progress_is_cumulative() -> None:
    """Per-bin Sum/Max math cannot show cumulative progress (PartitionsTotal
    exists in exactly one 5-minute bin); the widget must accumulate
    completions and carry the total forward."""
    widget = _widget(_template(backfill=True), "Backfill progress")
    expressions = [
        m[0]["expression"]
        for m in widget["properties"]["metrics"]
        if isinstance(m[0], dict) and "expression" in m[0]
    ]
    assert any("RUNNING_SUM" in e for e in expressions)
    assert any("FILL" in e and "REPEAT" in e for e in expressions)


def test_rejected_granules_query_filters_structured_fields_and_dedups() -> None:
    """The query must key on structured log fields only (message prose gets
    reworded; tests/test_handler.py pins the emitting side) and aggregate
    per granule — every redelivery logs again, up to the DLQ's 20, so raw
    rows would fill the 50-row cap with duplicates of a few bad granules."""
    widget = _widget(_template(), "Rejected granules")
    query = widget["properties"]["query"]
    assert "outcome in ['rejected', 'errored']" in query
    assert "stats latest(@timestamp)" in query
    assert "by coalesce(url, message_id) as granule" in query
    # No free-text message matching: rewording a log line must not be able
    # to silently break the runbook table.
    assert "message" not in query.replace("message_id", "")


def test_codebuild_may_put_tempo_pipeline_metrics_only() -> None:
    """verify_store.py emits CompletenessDelta via put_metric_data from the
    inventory CodeBuild project; without this grant the call fails silently
    (best-effort catch) and the CMR-vs-store widget is permanently blank.
    PutMetricData cannot be resource-scoped; the namespace condition is the
    least-privilege scoping."""
    _template().has_resource_properties(
        "AWS::IAM::Policy",
        Match.object_like(
            {
                "PolicyDocument": Match.object_like(
                    {
                        "Statement": Match.array_with(
                            [
                                Match.object_like(
                                    {
                                        "Action": "cloudwatch:PutMetricData",
                                        "Condition": {
                                            "StringEquals": {
                                                "cloudwatch:namespace": "TempoPipeline"
                                            }
                                        },
                                    }
                                )
                            ]
                        )
                    }
                )
            }
        ),
    )


def _dashboard_metrics(template: Template) -> list[list]:
    """Every TempoPipeline metric definition in the dashboard body (flattened
    from graph widgets), e.g.
    ["TempoPipeline", "AxisEndLag", "Collection", "hcho", "Stage", "dev", {...}].

    The backfill widget's MathExpressions (RUNNING_SUM/FILL) render their
    using_metrics as separate top-level entries in the widget's metrics
    array rather than nested under the expression entry, so no unwrapping
    is needed: the expression entries themselves are excluded below because
    their first element is a dict, not the namespace string.
    """
    (dashboard,) = [
        r
        for r in template.to_json()["Resources"].values()
        if r["Type"] == "AWS::CloudWatch::Dashboard"
    ]
    body = json.loads(resolve_joins(dashboard["Properties"]["DashboardBody"]))
    return [
        metric
        for widget in body["widgets"]
        for metric in widget["properties"].get("metrics", [])
        if isinstance(metric, list) and metric and metric[0] == METRIC_NAMESPACE
    ]


def test_dashboard_dimensions_match_emitters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A mismatched dimension set is a different CloudWatch series: the
    widget or alarm querying it silently shows nothing. The emitters build
    dimensions from env; the dashboard from settings — same values in,
    identical sets out."""
    monkeypatch.setenv("TEMPO_COLLECTION", "hcho")
    monkeypatch.setenv("STAGE", "dev")
    expected = metric_dimensions()  # {"Collection": "hcho", "Stage": "dev"}
    metrics = _dashboard_metrics(_template(backfill=True))
    assert metrics, "no TempoPipeline metrics found in the dashboard body"
    for definition in metrics:
        # ["Ns", "Name", dimName, dimValue, ..., {options}]
        end = (
            len(definition) - 1 if isinstance(definition[-1], dict) else len(definition)
        )
        pairs = definition[2:end]
        dims = dict(zip(pairs[::2], pairs[1::2]))
        dims.pop("Route", None)  # explicit extra, emitted per-call
        assert dims == expected, f"{definition[1]}: {dims} != {expected}"


def test_dashboard_queries_only_emitted_metric_names() -> None:
    """Every name the dashboard queries must have an emitter in the source
    tree, or the widget can never show data."""
    emitted = {
        group
        for source in EMITTER_SOURCES
        for match in EMIT_CALL.finditer(source.read_text())
        for group in match.groups()
        if group
    }
    queried = {
        definition[1] for definition in _dashboard_metrics(_template(backfill=True))
    }
    assert queried and queried <= emitted, queried - emitted
