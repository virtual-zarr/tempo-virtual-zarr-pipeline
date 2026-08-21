"""Offline tests for the CMR poller feeder."""

import json
import pathlib
import sys
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import boto3
import pytest
from moto import mock_aws

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lambda"))

from cmr_poller import handler as poller  # noqa: E402


def granule_item(name: str, with_s3: bool = True) -> dict:
    urls = [{"Type": "GET DATA", "URL": f"https://host/{name}.nc"}]
    if with_s3:
        urls.append(
            {
                "Type": "GET DATA VIA DIRECT ACCESS",
                "URL": f"s3://asdc-prod-protected/TEMPO/{name}.nc",
            }
        )
    return {"meta": {"concept-id": name}, "umm": {"RelatedUrls": urls}}


def test_direct_s3_url_extraction() -> None:
    assert (
        poller.direct_s3_url(granule_item("g1")["umm"])
        == "s3://asdc-prod-protected/TEMPO/g1.nc"
    )
    assert poller.direct_s3_url(granule_item("g2", with_s3=False)["umm"]) is None


def test_search_granules_pages_until_exhausted() -> None:
    pages = [
        ([granule_item("a"), granule_item("b")], "after-1"),
        ([granule_item("c")], "after-2"),
        ([], None),
    ]
    calls: list[str | None] = []

    def fetch(
        concept_id: str, since: str, search_after: str | None
    ) -> tuple[list[dict], str | None]:
        calls.append(search_after)
        return pages[len(calls) - 1]

    items = poller.search_granules("C123", "2026-08-01T00:00:00+00:00", fetch)
    assert [item["meta"]["concept-id"] for item in items] == ["a", "b", "c"]
    assert calls == [None, "after-1", "after-2"]


def test_watermark_round_trip(tmp_path: pathlib.Path) -> None:
    uri = str(tmp_path / "watermark.json")
    assert poller.read_watermark(uri) is None
    value = datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc)
    poller.write_watermark(uri, value)
    assert poller.read_watermark(uri) == value


@pytest.fixture()
def sqs_queue() -> Iterator[str]:
    with mock_aws():
        client = boto3.client("sqs", region_name="us-east-1")
        yield client.create_queue(QueueName="test-queue")["QueueUrl"]


def test_handler_enqueues_and_advances_watermark(
    sqs_queue: str,
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    watermark_uri = str(tmp_path / "watermark.json")
    monkeypatch.setenv("CONCEPT_ID", "C3685897141-LARC_CLOUD")
    monkeypatch.setenv("QUEUE_URL", sqs_queue)
    monkeypatch.setenv("POLL_WATERMARK_URI", watermark_uri)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    since_seen: list[str] = []

    def fetch(
        concept_id: str, since: str, search_after: str | None
    ) -> tuple[list[dict], str | None]:
        assert concept_id == "C3685897141-LARC_CLOUD"
        since_seen.append(since)
        return [granule_item("g1"), granule_item("g2", with_s3=False)], None

    monkeypatch.setattr(poller, "_http_fetch", fetch)

    result = poller.handler({}, MagicMock())

    assert result["enqueued"] == 1  # only the granule with a direct s3 link
    messages = boto3.client("sqs", region_name="us-east-1").receive_message(
        QueueUrl=sqs_queue, MaxNumberOfMessages=10
    )["Messages"]
    assert [json.loads(m["Body"]) for m in messages] == [
        {"url": "s3://asdc-prod-protected/TEMPO/g1.nc"}
    ]

    # First run: since = now - default lookback - overlap (9 days back).
    first_since = datetime.fromisoformat(since_seen[0])
    assert (datetime.now(timezone.utc) - first_since).days >= 8

    # Second run: since derives from the persisted watermark minus overlap.
    watermark = poller.read_watermark(watermark_uri)
    assert watermark is not None
    poller.handler({}, MagicMock())
    second_since = datetime.fromisoformat(since_seen[1])
    assert second_since == watermark - poller.OVERLAP


def test_initial_watermark_uses_poll_start_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POLL_START_ISO", "2026-08-01T00:00:00+00:00")
    now = datetime(2026, 8, 20, tzinfo=timezone.utc)
    assert poller.initial_watermark(now) == datetime(
        2026, 8, 1, tzinfo=timezone.utc
    )


def test_initial_watermark_falls_back_to_lookback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("POLL_START_ISO", raising=False)
    now = datetime(2026, 8, 20, tzinfo=timezone.utc)
    assert poller.initial_watermark(now) == now - poller.DEFAULT_LOOKBACK


class _FlakySqs:
    """send_message_batch stub that fails one entry for the first N calls."""

    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.calls: list[list[str]] = []

    def send_message_batch(self, QueueUrl: str, Entries: list[dict]) -> dict:
        self.calls.append([e["Id"] for e in Entries])
        if len(self.calls) <= self.failures:
            return {"Failed": [{"Id": Entries[0]["Id"], "Code": "InternalError"}]}
        return {}


def test_enqueue_retries_partial_batch_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """send_message_batch is not all-or-nothing; failed entries are retried."""
    stub = _FlakySqs(failures=1)
    monkeypatch.setattr(boto3, "client", lambda service: stub)

    sent = poller.enqueue("queue-url", [f"s3://b/{i}.nc" for i in range(3)])

    assert sent == 3
    assert stub.calls == [["0", "1", "2"], ["0"]]  # only the failed entry retried


def test_enqueue_raises_when_failures_persist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(boto3, "client", lambda service: _FlakySqs(failures=2))
    with pytest.raises(RuntimeError, match="failed to enqueue"):
        poller.enqueue("queue-url", ["s3://b/0.nc"])


def test_watermark_not_advanced_when_enqueue_fails(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed enqueue must leave the watermark alone so the next poll
    re-covers the same window instead of dropping the granules for good."""
    watermark_uri = str(tmp_path / "watermark.json")
    monkeypatch.setenv("CONCEPT_ID", "C")
    monkeypatch.setenv("QUEUE_URL", "queue-url")
    monkeypatch.setenv("POLL_WATERMARK_URI", watermark_uri)
    monkeypatch.setattr(poller, "_http_fetch", lambda *a: ([granule_item("g1")], None))
    monkeypatch.setattr(boto3, "client", lambda service: _FlakySqs(failures=2))

    with pytest.raises(RuntimeError):
        poller.handler({}, MagicMock())

    assert poller.read_watermark(watermark_uri) is None
