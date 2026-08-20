"""The initialize and garbage-collect entry points must fail loudly.

A swallowed exception here reported a successful deploy with an
uninitialized store, and a GC Batch job exiting 0 on failure defeated
both its retry policy and any monitoring.
"""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lambda"))

from garbage_collect import handler as gc_handler  # noqa: E402
from initialize import handler as init_handler  # noqa: E402


def test_initialize_handler_propagates_failures() -> None:
    with patch("initialize.handler.Processor", side_effect=RuntimeError("boom")):
        with pytest.raises(RuntimeError, match="boom"):
            init_handler.handler({}, MagicMock())


def test_gc_handler_propagates_failures() -> None:
    with patch("garbage_collect.handler.Processor", side_effect=RuntimeError("boom")):
        with pytest.raises(RuntimeError, match="boom"):
            gc_handler.handler()


def test_gc_handler_honors_configured_expiry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GC_EXPIRY_DAYS", "7")
    with patch("garbage_collect.handler.Processor") as MockProcessor:
        gc_handler.handler()
    expiry = MockProcessor.return_value.garbage_collect.call_args.kwargs["expiry_time"]
    age = datetime.now(timezone.utc) - expiry
    assert timedelta(days=6, hours=23) < age < timedelta(days=7, hours=1)
