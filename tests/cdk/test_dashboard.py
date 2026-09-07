"""CDK assertions for the in-stack CloudWatch dashboard."""

import json
from typing import Any

import aws_cdk as cdk
from aws_cdk.assertions import Match, Template
from conftest import resolve_joins
from settings import StackSettings
from stack import VirtualizarrSqsStack


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
