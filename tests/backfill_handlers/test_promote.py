from types import SimpleNamespace
from unittest.mock import MagicMock

import boto3
import pytest
from backfill_handlers import init, promote
from virtualizarr_processor.processor import Processor
from virtualizarr_processor.store_template import StoreValidationError

BUCKET = "test-backfill-bucket"


def test_promote_gates_then_moves_main_to_backfill_tip(
    tempo_pipeline: SimpleNamespace,
    lambda_context: MagicMock,
) -> None:
    init.handler({"inventory_uri": tempo_pipeline.inventory_uri}, lambda_context)

    result = promote.handler(
        {"inventory_uri": tempo_pipeline.inventory_uri}, lambda_context
    )

    assert result["promoted"] is True
    repo = Processor().open_backfill_repo()
    assert repo.lookup_branch("main") == repo.lookup_branch("backfill")


def test_promote_gate_failure_leaves_main_untouched(
    tempo_pipeline: SimpleNamespace,
    lambda_context: MagicMock,
) -> None:
    init.handler({"inventory_uri": tempo_pipeline.inventory_uri}, lambda_context)

    # An inventory that disagrees with the store axis: one granule extra.
    wrong = tempo_pipeline.tiny.inventory.model_copy(
        update={"granules": tempo_pipeline.tiny.inventory.granules[:-1]}
    )
    boto3.client("s3", region_name="us-east-1").put_object(
        Bucket=BUCKET, Key="wrong.json", Body=wrong.to_json().encode()
    )
    repo = Processor().open_backfill_repo()
    main_before = repo.lookup_branch("main")

    with pytest.raises(StoreValidationError):
        promote.handler({"inventory_uri": f"s3://{BUCKET}/wrong.json"}, lambda_context)

    repo = Processor().open_backfill_repo()
    assert repo.lookup_branch("main") == main_before
