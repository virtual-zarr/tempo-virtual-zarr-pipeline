import json
import sys
from pathlib import Path
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
