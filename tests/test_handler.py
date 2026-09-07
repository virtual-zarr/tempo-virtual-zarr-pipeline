import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from aws_lambda_powertools.utilities.batch.exceptions import BatchProcessingError
from virtualizarr_processor.typing import ProcessOutcome

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lambda"))

from process_messages.handler import handler

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tempo_fixtures import emf_blobs, emf_value  # noqa: E402


def make_sqs_event(
    urls: list[str] | None = None,
    s3_keys: list[str] | None = None,
    bucket: str = "test-bucket",
    receive_count: str = "1",
) -> dict:
    """An SQS event with poller (`{"url": ...}`) or S3-notification bodies."""
    bodies = [{"url": url} for url in urls or []]
    bodies += [
        {"Records": [{"s3": {"bucket": {"name": bucket}, "object": {"key": key}}}]}
        for key in s3_keys or []
    ]
    records = []
    for i, body in enumerate(bodies):
        records.append(
            {
                "messageId": f"msg-{i:03d}",
                "receiptHandle": f"receipt-{i}",
                "body": json.dumps(body),
                "attributes": {
                    "ApproximateReceiveCount": receive_count,
                    "SentTimestamp": "1717600000000",
                    "ApproximateFirstReceiveTimestamp": "1717600000000",
                },
                "messageAttributes": {},
                "md5OfBody": "abc",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-1:123456789:test-queue",
                "awsRegion": "us-east-1",
            }
        )
    return {"Records": records}


@patch("process_messages.handler.Processor")
def test_handler_processes_all_records_sorted(MockProcessor: MagicMock) -> None:
    mock_processor = MockProcessor.return_value
    mock_session = MagicMock()
    mock_processor.open_initialized_repo.return_value = MagicMock()
    mock_processor.initialize_session.return_value = mock_session
    mock_processor.process_file.return_value = ProcessOutcome.APPENDED
    mock_processor.commit_processed_files.return_value = "snapshot-123"

    # Deliberately out of order: the handler sorts by filename so adjacent
    # scans arriving swapped within one batch still append in order.
    event = make_sqs_event(
        urls=[
            "s3://data/TEMPO_HCHO_L3_V04_20260819T184200Z_S010.nc",
            "s3://data/TEMPO_HCHO_L3_V04_20260819T174200Z_S009.nc",
        ]
    )

    response = handler(event, MagicMock())

    assert response["batchItemFailures"] == []
    calls = mock_processor.process_file.call_args_list
    assert [c.kwargs["file_key"].rsplit("/", 1)[-1][:40] for c in calls] == [
        "TEMPO_HCHO_L3_V04_20260819T174200Z_S009.",
        "TEMPO_HCHO_L3_V04_20260819T184200Z_S010.",
    ]
    mock_processor.commit_processed_files.assert_called_once_with(session=mock_session)


@patch("process_messages.handler.Processor")
def test_handler_accepts_s3_notification_shape(MockProcessor: MagicMock) -> None:
    mock_processor = MockProcessor.return_value
    mock_processor.open_initialized_repo.return_value = MagicMock()
    mock_processor.initialize_session.return_value = MagicMock()
    mock_processor.process_file.return_value = ProcessOutcome.APPENDED
    mock_processor.commit_processed_files.return_value = "snapshot-123"

    event = make_sqs_event(s3_keys=["TEMPO/granule.nc"], bucket="asdc-prod-protected")
    response = handler(event, MagicMock())

    assert response["batchItemFailures"] == []
    call = mock_processor.process_file.call_args_list[0]
    assert call.kwargs["file_key"] == "s3://asdc-prod-protected/TEMPO/granule.nc"


@patch("process_messages.handler.Processor")
def test_handler_deferred_is_successful_consumption(MockProcessor: MagicMock) -> None:
    mock_processor = MockProcessor.return_value
    mock_processor.open_initialized_repo.return_value = MagicMock()
    mock_processor.initialize_session.return_value = MagicMock()
    mock_processor.process_file.return_value = ProcessOutcome.DEFERRED
    mock_processor.commit_processed_files.return_value = "snapshot-123"

    event = make_sqs_event(urls=["s3://data/old_granule.nc"])
    response = handler(event, MagicMock())

    assert response["batchItemFailures"] == []


@patch("process_messages.handler.Processor")
def test_handler_raises_when_entire_batch_fails(MockProcessor: MagicMock) -> None:
    """If all records are rejected, BatchProcessor raises BatchProcessingError."""
    mock_processor = MockProcessor.return_value
    mock_processor.open_initialized_repo.return_value = MagicMock()
    mock_processor.initialize_session.return_value = MagicMock()
    mock_processor.process_file.return_value = ProcessOutcome.REJECTED

    event = make_sqs_event(urls=["s3://data/bad.nc"])

    with pytest.raises(BatchProcessingError):
        handler(event, MagicMock())


@patch("process_messages.handler.Processor")
def test_handler_partial_failure(MockProcessor: MagicMock) -> None:
    """If some records are rejected, only those appear in batchItemFailures."""
    mock_processor = MockProcessor.return_value
    mock_processor.open_initialized_repo.return_value = MagicMock()
    mock_processor.initialize_session.return_value = MagicMock()
    mock_processor.process_file.side_effect = [
        ProcessOutcome.APPENDED,
        ProcessOutcome.REJECTED,
    ]
    mock_processor.commit_processed_files.return_value = "snapshot-123"

    event = make_sqs_event(urls=["s3://data/a_good.nc", "s3://data/b_bad.nc"])
    response = handler(event, MagicMock())

    failed_ids = [item["itemIdentifier"] for item in response["batchItemFailures"]]
    assert "msg-001" in failed_ids
    assert "msg-000" not in failed_ids


@patch("process_messages.handler.Processor")
def test_handler_fails_all_on_commit_error(MockProcessor: MagicMock) -> None:
    """If commit fails, all records should be marked as failed."""
    mock_processor = MockProcessor.return_value
    mock_processor.open_initialized_repo.return_value = MagicMock()
    mock_processor.initialize_session.return_value = MagicMock()
    mock_processor.process_file.return_value = ProcessOutcome.APPENDED
    mock_processor.commit_processed_files.side_effect = Exception("Commit failed")

    event = make_sqs_event(urls=["s3://data/a.nc", "s3://data/b.nc"])
    response = handler(event, MagicMock())

    failed_ids = [item["itemIdentifier"] for item in response["batchItemFailures"]]
    assert "msg-000" in failed_ids
    assert "msg-001" in failed_ids


def _hour_ago_store() -> Any:
    """A real store whose time axis ends one hour ago."""
    import zarr
    from virtualizarr_processor.manifest import TEMPO_EPOCH

    store = zarr.storage.MemoryStore()
    axis = zarr.open_group(store, mode="w").create_array(
        "time", shape=(1,), dtype="float64"
    )
    axis[:] = [(datetime.now(timezone.utc) - TEMPO_EPOCH).total_seconds() - 3600.0]
    return store


@patch("process_messages.handler.Processor")
def test_handler_emits_axis_end_lag_after_commit(
    MockProcessor: MagicMock, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    monkeypatch.setenv("TEMPO_COLLECTION", "hcho")
    monkeypatch.setenv("STAGE", "dev")
    mock_processor = MockProcessor.return_value
    mock_processor.open_initialized_repo.return_value = MagicMock()
    mock_session = MagicMock()
    mock_processor.initialize_session.return_value = mock_session
    mock_processor.process_file.return_value = ProcessOutcome.APPENDED
    mock_processor.commit_processed_files.return_value = "snapshot-123"
    mock_session.store = _hour_ago_store()

    handler(make_sqs_event(urls=["s3://data/a.nc"]), MagicMock())

    blobs = emf_blobs(capsys.readouterr().out)
    (blob,) = [b for b in blobs if "AxisEndLag" in b]
    (spec,) = blob["_aws"]["CloudWatchMetrics"]
    assert spec["Namespace"] == "TempoPipeline"
    # Exactly these dimensions: anything extra is a different CloudWatch
    # series, and the dashboard and AxisEndLag alarm would see nothing.
    assert spec["Dimensions"] == [["Collection", "Stage"]]
    assert spec["Metrics"][0]["Name"] == "AxisEndLag"
    assert spec["Metrics"][0]["Unit"] == "Seconds"
    assert blob["Collection"] == "hcho"
    assert blob["Stage"] == "dev"
    (lag,) = blob["AxisEndLag"]
    assert 3590 < lag < 3900


@patch("process_messages.handler.Processor")
def test_handler_emits_routing_and_ledger_metrics(
    MockProcessor: MagicMock, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    monkeypatch.setenv("TEMPO_COLLECTION", "hcho")
    monkeypatch.setenv("STAGE", "dev")
    mock_processor = MockProcessor.return_value
    mock_processor.open_initialized_repo.return_value = MagicMock()
    mock_session = MagicMock()
    mock_processor.initialize_session.return_value = mock_session
    mock_processor.process_file.side_effect = [
        ProcessOutcome.APPENDED,
        ProcessOutcome.APPENDED,
        ProcessOutcome.OVERWRITTEN,
        ProcessOutcome.DEFERRED,
        ProcessOutcome.REJECTED,
    ]
    mock_processor.commit_processed_files.return_value = "snapshot-123"
    mock_session.store = _hour_ago_store()

    handler(make_sqs_event(urls=[f"s3://data/{i}.nc" for i in range(5)]), MagicMock())

    blobs = emf_blobs(capsys.readouterr().out)
    assert emf_value(blobs, "GranulesRouted", Route="APPENDED") == 2
    assert emf_value(blobs, "GranulesRouted", Route="OVERWRITTEN") == 1
    # DEFERRED surfaces as the dashboard's PENDING route.
    assert emf_value(blobs, "GranulesRouted", Route="PENDING") == 1
    assert emf_value(blobs, "GranulesRouted", Route="REJECTED") == 1
    # The test store carries no ledger attribute: depth 0.
    assert emf_value(blobs, "PendingLedgerDepth") == 0
    (routed,) = [b for b in blobs if b.get("Route") == "APPENDED"]
    (spec,) = routed["_aws"]["CloudWatchMetrics"]
    assert sorted(spec["Dimensions"][0]) == ["Collection", "Route", "Stage"]


@patch("process_messages.handler.Processor")
def test_handler_emits_commit_failure_metric(
    MockProcessor: MagicMock, capsys: Any
) -> None:
    """A failed commit is otherwise invisible (the invocation still succeeds);
    CommitFailures is its only signal besides queue redelivery.
    """
    mock_processor = MockProcessor.return_value
    mock_processor.open_initialized_repo.return_value = MagicMock()
    mock_session = MagicMock()
    mock_session.store = _hour_ago_store()
    mock_processor.initialize_session.return_value = mock_session
    mock_processor.process_file.return_value = ProcessOutcome.APPENDED
    mock_processor.commit_processed_files.side_effect = Exception("CAS conflict")

    response = handler(make_sqs_event(urls=["s3://data/a.nc"]), MagicMock())

    assert response["batchItemFailures"]  # all records retried
    blobs = emf_blobs(capsys.readouterr().out)
    assert emf_value(blobs, "CommitFailures") == 1
    # Nothing was persisted: no routing counts, no freshness point.
    assert emf_value(blobs, "GranulesRouted", Route="APPENDED") is None
    assert emf_value(blobs, "AxisEndLag") is None


@patch("process_messages.handler.Processor")
def test_rejected_counted_only_on_first_receipt(
    MockProcessor: MagicMock, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    """A rejected granule redelivers up to the DLQ's maxReceiveCount (20);
    counting every attempt would inflate GranulesRouted{REJECTED} 20x per
    bad granule.

    Uses two granules (not one) so the batch is a partial, not total,
    failure: a single-record all-REJECTED batch makes BatchProcessor raise
    BatchProcessingError before the commit/metrics path ever runs (see
    test_handler_raises_when_entire_batch_fails), which would defeat this
    test regardless of the fix under test.
    """
    monkeypatch.setenv("TEMPO_COLLECTION", "hcho")
    monkeypatch.setenv("STAGE", "dev")
    mock_processor = MockProcessor.return_value
    mock_processor.open_initialized_repo.return_value = MagicMock()
    mock_session = MagicMock()
    mock_session.store = _hour_ago_store()
    mock_processor.initialize_session.return_value = mock_session
    mock_processor.process_file.side_effect = [
        ProcessOutcome.REJECTED,
        ProcessOutcome.APPENDED,
    ]
    mock_processor.commit_processed_files.return_value = "snapshot-123"

    response = handler(
        make_sqs_event(urls=["s3://data/a.nc", "s3://data/b.nc"], receive_count="2"),
        MagicMock(),
    )

    assert response["batchItemFailures"]  # still fails the rejected record
    blobs = emf_blobs(capsys.readouterr().out)
    assert emf_value(blobs, "GranulesRouted", Route="REJECTED") is None
    assert emf_value(blobs, "GranulesRouted", Route="APPENDED") == 1


@patch("process_messages.handler.Processor")
def test_axis_end_lag_read_failure_does_not_fail_batch(
    MockProcessor: MagicMock, capsys: Any
) -> None:
    """The store-reading metrics are best-effort: a committed batch must
    still succeed when the session store cannot be read."""
    mock_processor = MockProcessor.return_value
    mock_processor.open_initialized_repo.return_value = MagicMock()
    mock_session = MagicMock()
    mock_session.store = object()  # not a zarr store; store reads raise
    mock_processor.initialize_session.return_value = mock_session
    mock_processor.process_file.return_value = ProcessOutcome.APPENDED
    mock_processor.commit_processed_files.return_value = "snapshot-123"

    response = handler(make_sqs_event(urls=["s3://data/a.nc"]), MagicMock())

    assert response["batchItemFailures"] == []
    blobs = emf_blobs(capsys.readouterr().out)
    # Routing needs no store access and still emits.
    assert emf_value(blobs, "GranulesRouted", Route="APPENDED") == 1
    assert emf_value(blobs, "PendingLedgerDepth") is None
    assert emf_value(blobs, "AxisEndLag") is None
