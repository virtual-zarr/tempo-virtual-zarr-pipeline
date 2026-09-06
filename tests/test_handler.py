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


def make_sqs_event(
    urls: list[str] | None = None,
    s3_keys: list[str] | None = None,
    bucket: str = "test-bucket",
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
                    "ApproximateReceiveCount": "1",
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
    mock_processor.process_file.return_value = ProcessOutcome.WRITTEN
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
    mock_processor.process_file.return_value = ProcessOutcome.WRITTEN
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
        ProcessOutcome.WRITTEN,
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
    mock_processor.process_file.return_value = ProcessOutcome.WRITTEN
    mock_processor.commit_processed_files.side_effect = Exception("Commit failed")

    event = make_sqs_event(urls=["s3://data/a.nc", "s3://data/b.nc"])
    response = handler(event, MagicMock())

    failed_ids = [item["itemIdentifier"] for item in response["batchItemFailures"]]
    assert "msg-000" in failed_ids
    assert "msg-001" in failed_ids


def _emf_blobs(captured: str) -> list[dict]:
    """EMF metric blobs among the captured stdout lines (logs included)."""
    blobs = []
    for line in captured.splitlines():
        try:
            blob = json.loads(line)
        except ValueError:
            continue
        if isinstance(blob, dict) and "_aws" in blob:
            blobs.append(blob)
    return blobs


@patch("process_messages.handler.Processor")
def test_handler_emits_axis_end_lag_after_commit(
    MockProcessor: MagicMock, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    import zarr
    from virtualizarr_processor.manifest import TEMPO_EPOCH

    monkeypatch.setenv("TEMPO_COLLECTION", "hcho")
    monkeypatch.setenv("STAGE", "dev")
    mock_processor = MockProcessor.return_value
    mock_processor.open_initialized_repo.return_value = MagicMock()
    mock_session = MagicMock()
    mock_processor.initialize_session.return_value = mock_session
    mock_processor.process_file.return_value = ProcessOutcome.WRITTEN
    mock_processor.commit_processed_files.return_value = "snapshot-123"
    # A real store whose time axis ends one hour ago.
    store = zarr.storage.MemoryStore()
    axis = zarr.open_group(store, mode="w").create_array(
        "time", shape=(1,), dtype="float64"
    )
    hour_ago = (datetime.now(timezone.utc) - TEMPO_EPOCH).total_seconds() - 3600.0
    axis[:] = [hour_ago]
    mock_session.store = store

    handler(make_sqs_event(urls=["s3://data/a.nc"]), MagicMock())

    (blob,) = _emf_blobs(capsys.readouterr().out)
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
def test_axis_end_lag_read_failure_does_not_fail_batch(
    MockProcessor: MagicMock, capsys: Any
) -> None:
    """The freshness metric is best-effort: a committed batch must still
    succeed when the time axis cannot be read."""
    mock_processor = MockProcessor.return_value
    mock_processor.open_initialized_repo.return_value = MagicMock()
    mock_session = MagicMock()
    mock_session.store = object()  # not a zarr store; the axis read raises
    mock_processor.initialize_session.return_value = mock_session
    mock_processor.process_file.return_value = ProcessOutcome.WRITTEN
    mock_processor.commit_processed_files.return_value = "snapshot-123"

    response = handler(make_sqs_event(urls=["s3://data/a.nc"]), MagicMock())

    assert response["batchItemFailures"] == []
    assert _emf_blobs(capsys.readouterr().out) == []
