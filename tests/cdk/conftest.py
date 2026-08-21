import os
import sys

# cdk/ holds top-level modules (settings, stack, stack_constructs) that are not on
# the default pytest path; add it so the CDK tests can import them.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "cdk"))

from collections.abc import Iterator
from typing import Any

from aws_cdk.assertions import Template


def resolve_joins(value: Any) -> Any:
    """Render Fn::Join nodes to strings; non-string parts become "<REF>".

    Mirrors the flattening idiom already used by _state_machine_asl in
    test_backfill_pipeline.py, applied to a whole template dict.
    """
    if isinstance(value, dict):
        if set(value) == {"Fn::Join"}:
            sep, parts = value["Fn::Join"]
            resolved = [resolve_joins(p) for p in parts]
            return sep.join(p if isinstance(p, str) else "<REF>" for p in resolved)
        return {k: resolve_joins(v) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve_joins(v) for v in value]
    return value


def iam_statements(
    template: Template, role_marker: str | None = None
) -> Iterator[dict[str, Any]]:
    """Yield rendered statements from AWS::IAM::Policy resources, optionally
    only from policies whose logical id contains role_marker (case-insensitive)."""
    for logical_id, res in template.to_json()["Resources"].items():
        if res["Type"] != "AWS::IAM::Policy":
            continue
        if role_marker and role_marker.lower() not in logical_id.lower():
            continue
        yield from resolve_joins(res["Properties"]["PolicyDocument"]["Statement"])


def actions_of(stmt: dict[str, Any]) -> list[str]:
    action = stmt.get("Action", [])
    return action if isinstance(action, list) else [action]


def resources_of(stmt: dict[str, Any]) -> list[str]:
    resource = stmt.get("Resource", [])
    return resource if isinstance(resource, list) else [resource]
