"""The shared metric identity in virtualizarr_processor.metrics."""

import pytest
from virtualizarr_processor.metrics import NAMESPACE, metric_dimensions


def test_namespace() -> None:
    assert NAMESPACE == "TempoPipeline"


def test_metric_dimensions_drops_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TEMPO_COLLECTION", raising=False)
    monkeypatch.setenv("STAGE", "dev")
    assert metric_dimensions() == {"Stage": "dev"}


def test_metric_dimensions_with_extras(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEMPO_COLLECTION", "hcho")
    monkeypatch.setenv("STAGE", "dev")
    assert metric_dimensions(Route="APPENDED") == {
        "Collection": "hcho",
        "Stage": "dev",
        "Route": "APPENDED",
    }
