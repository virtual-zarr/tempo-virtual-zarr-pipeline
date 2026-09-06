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


def test_axis_end_lag_alarm_breaches_on_missing_data() -> None:
    """No data *is* the failure: a dead poller or silently-failing re-sort
    stops the metric, and NOT_BREACHING (the _alarm default) would hide it."""
    _template().has_resource_properties(
        "AWS::CloudWatch::Alarm",
        Match.object_like(
            {"MetricName": "AxisEndLag", "TreatMissingData": "breaching"}
        ),
    )
