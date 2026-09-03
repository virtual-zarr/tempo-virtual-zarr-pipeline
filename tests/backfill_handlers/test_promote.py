from types import SimpleNamespace
from unittest.mock import MagicMock

import boto3
import pytest
from backfill_handlers import init, promote
from virtualizarr_processor.processor import Processor
from virtualizarr_processor.store_template import StoreValidationError

BUCKET = "test-backfill-bucket"


def run_workers(tempo_pipeline: SimpleNamespace) -> None:
    """Fill every slot of the initialized backfill branch, fork/merge style."""
    import pickle

    from virtualizarr_processor import backfill

    processor = Processor()
    repo = processor.open_backfill_repo()
    shared = pickle.loads(backfill.create_fork(repo))
    children = []
    for url in tempo_pipeline.urls:
        child = shared.fork()
        assert processor.process_backfill_file(url, child)
        children.append(pickle.dumps(child))
    backfill.merge_and_commit(repo, children, message="workers")


def test_promote_gates_then_moves_main_to_backfill_tip(
    tempo_pipeline: SimpleNamespace,
    lambda_context: MagicMock,
) -> None:
    init_result = init.handler(
        {"inventory_uri": tempo_pipeline.inventory_uri}, lambda_context
    )
    run_workers(tempo_pipeline)
    repo = Processor().open_backfill_repo()
    expected_tip = repo.lookup_branch("backfill")

    result = promote.handler(
        {
            "inventory_uri": tempo_pipeline.inventory_uri,
            "branched_from": init_result["branched_from"],
        },
        lambda_context,
    )

    assert result["promoted"] is True
    repo = Processor().open_backfill_repo()
    assert repo.lookup_branch("main") == expected_tip


def test_promote_rejects_unfilled_store(
    tempo_pipeline: SimpleNamespace,
    lambda_context: MagicMock,
) -> None:
    """An initialized store with no worker writes has a perfect axis,
    manifest, and template — and every data slot reading as fill values.
    The gate must count chunk references, not just metadata."""
    init_result = init.handler(
        {"inventory_uri": tempo_pipeline.inventory_uri}, lambda_context
    )
    repo = Processor().open_backfill_repo()
    main_before = repo.lookup_branch("main")

    with pytest.raises(StoreValidationError, match="chunk references"):
        promote.handler(
            {
                "inventory_uri": tempo_pipeline.inventory_uri,
                "branched_from": init_result["branched_from"],
            },
            lambda_context,
        )

    repo = Processor().open_backfill_repo()
    assert repo.lookup_branch("main") == main_before


def test_promote_pins_the_snapshot_it_validated(
    tempo_pipeline: SimpleNamespace,
    lambda_context: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A concurrent run's Init resets the `backfill` branch while this
    run's promote is between its validation and its branch move (the
    resort TOCTOU, backfill flavor). The promote must validate and move
    `main` to one pinned snapshot — its own finished one — never to a
    fresh lookup of the tip the other run just reset."""
    init_result = init.handler(
        {"inventory_uri": tempo_pipeline.inventory_uri}, lambda_context
    )
    run_workers(tempo_pipeline)
    finished_tip = Processor().open_backfill_repo().lookup_branch("backfill")

    original = Processor.validate_backfill_store
    reset_done = False

    def validate_after_b_resets_backfill(
        self: Processor, *args: object, **kw: object
    ) -> None:
        nonlocal reset_done
        if not reset_done:  # only once: run B's Init, mid-promote of run A
            reset_done = True
            b_repo = self.open_backfill_repo()
            self.initialize_backfill_store(
                b_repo, tempo_pipeline.tiny.inventory, force=True
            )
        return original(self, *args, **kw)  # type: ignore[arg-type]

    monkeypatch.setattr(
        Processor, "validate_backfill_store", validate_after_b_resets_backfill
    )

    result = promote.handler(
        {
            "inventory_uri": tempo_pipeline.inventory_uri,
            "branched_from": init_result["branched_from"],
        },
        lambda_context,
    )

    assert result["promoted"] is True
    repo = Processor().open_backfill_repo()
    assert repo.lookup_branch("main") == finished_tip
    assert repo.lookup_branch("backfill") != finished_tip  # B's reset stands


def test_promote_gate_failure_leaves_main_untouched(
    tempo_pipeline: SimpleNamespace,
    lambda_context: MagicMock,
) -> None:
    init_result = init.handler(
        {"inventory_uri": tempo_pipeline.inventory_uri}, lambda_context
    )

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
        promote.handler(
            {
                "inventory_uri": f"s3://{BUCKET}/wrong.json",
                "branched_from": init_result["branched_from"],
            },
            lambda_context,
        )

    repo = Processor().open_backfill_repo()
    assert repo.lookup_branch("main") == main_before


def test_promote_deletes_backfill_branch(
    tempo_pipeline: SimpleNamespace,
    lambda_context: MagicMock,
) -> None:
    """After promote, the branch is gone, so the next run's init never
    needs force (branch-exists then always means live or failed)."""
    init_result = init.handler(
        {"inventory_uri": tempo_pipeline.inventory_uri}, lambda_context
    )
    run_workers(tempo_pipeline)
    promote.handler(
        {
            "inventory_uri": tempo_pipeline.inventory_uri,
            "branched_from": init_result["branched_from"],
        },
        lambda_context,
    )

    repo = Processor().open_backfill_repo()
    assert "backfill" not in repo.list_branches()
    # A fresh run's init no longer needs force: the guard only ever
    # blocked on branch existence, and the branch is gone, so init takes
    # the create_branch path. (A second full backfill of the very same
    # granules against an already-populated `main` is a separate, out-of-
    # scope limit: create_empty_store correctly refuses to re-template
    # over data that is already there, so this fails inside the store
    # write rather than on BackfillBranchExistsError.)
    with pytest.raises(ValueError, match="already exists"):
        init.handler({"inventory_uri": tempo_pipeline.inventory_uri}, lambda_context)
