"""Tests for the store verification script."""

import pathlib
import pickle
from datetime import datetime

import pytest
import verify_store as vs
from tempo_fixtures import TinyCollection, build_tiny_collection, write_tempo_granule
from virtualizarr_processor import backfill
from virtualizarr_processor.inventory import BackfillInventory
from virtualizarr_processor.processor import Processor


@pytest.fixture()
def tiny(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> TinyCollection:
    collection = build_tiny_collection(tmp_path / "collection")
    monkeypatch.delenv("ICECHUNK_BUCKET", raising=False)
    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "repo"))
    monkeypatch.setenv("VIRTUAL_CHUNK_PREFIX", f"file://{tmp_path}/")
    monkeypatch.setenv("TEMPO_COLLECTION", str(collection.config_path))
    return collection


def backfill_and_promote(tiny: TinyCollection) -> Processor:
    processor = Processor()
    repo = processor.open_backfill_repo()
    init = processor.initialize_backfill_store(repo, tiny.inventory)
    shared = pickle.loads(backfill.create_fork(repo))
    children = []
    for url in tiny.urls:
        child = shared.fork()
        assert processor.process_backfill_file(url, child)
        children.append(pickle.dumps(child))
    backfill.merge_and_commit(repo, children, message="backfill")
    backfill.promote(repo, expected_target_tip=init.branched_from)
    return processor


def lookup_from(inventory: BackfillInventory) -> vs.CmrLookup:
    """A fake CMR lookup that answers with the inventory's own granules."""

    def lookup(when: datetime) -> tuple[str, str] | None:
        entry = min(
            inventory.granules,
            key=lambda e: abs(vs.axis_datetime(e.time) - when),
        )
        return entry.url, entry.granule_ur

    return lookup


# --- offline (manifest-driven) mode ---


def test_clean_store_verifies_offline(tiny: TinyCollection) -> None:
    processor = backfill_and_promote(tiny)
    repo = processor.open_backfill_repo()
    assert verify(repo, tiny.inventory) == []


def test_mutated_source_object_is_detected(tiny: TinyCollection) -> None:
    """Overwriting a source file after ingest must be detected, either as
    icechunk's checksum failure or as a value mismatch."""
    processor = backfill_and_promote(tiny)
    write_tempo_granule(
        tiny.granule_paths[1], time_value=tiny.times[1], weight_scale=99.0
    )
    repo = processor.open_backfill_repo()
    problems = verify(repo, tiny.inventory)
    assert any("granule_1" in line for line in problems)


# --- CMR (independent) mode ---


def verify(repo: object, manifest: BackfillInventory, **kwargs: object) -> list[str]:
    return vs.verify_store(repo, manifest, samples=3, window=3, **kwargs)  # type: ignore[arg-type]


def test_clean_store_verifies_against_cmr(tiny: TinyCollection) -> None:
    processor = backfill_and_promote(tiny)
    repo = processor.open_backfill_repo()
    problems = verify(repo, tiny.inventory, cmr_lookup=lookup_from(tiny.inventory))
    assert problems == []


def test_superseded_revision_is_detected_only_via_cmr(tiny: TinyCollection) -> None:
    """A republication under a new key leaves the old object intact: the
    manifest-driven check passes, the CMR-driven check must not."""
    processor = backfill_and_promote(tiny)
    revised = write_tempo_granule(
        tiny.granule_paths[0].parent / "revised_1.nc",
        time_value=tiny.times[1],
        weight_scale=55.0,
    )

    def lookup(when: datetime) -> tuple[str, str] | None:
        if when == vs.axis_datetime(tiny.times[1]):
            return f"file://{revised}", "revised_1"
        return lookup_from(tiny.inventory)(when)

    repo = processor.open_backfill_repo()
    assert verify(repo, tiny.inventory) == []  # offline mode cannot see it
    problems = verify(repo, tiny.inventory, cmr_lookup=lookup)
    assert any("differs from CMR's current url" in line for line in problems)
    assert any("weight" in line and "raw bytes differ" in line for line in problems)


def test_missing_cmr_granule_is_reported(tiny: TinyCollection) -> None:
    processor = backfill_and_promote(tiny)

    def lookup(when: datetime) -> tuple[str, str] | None:
        if when == vs.axis_datetime(tiny.times[0]):
            return None
        return lookup_from(tiny.inventory)(when)

    repo = processor.open_backfill_repo()
    problems = verify(repo, tiny.inventory, cmr_lookup=lookup)
    assert any("CMR has no granule near" in line for line in problems)


# --- completeness ---


def fake_search(urs: list[str]):  # type: ignore[no-untyped-def]
    def search(params: dict, search_after: str | None = None):  # type: ignore[no-untyped-def]
        if search_after:
            return [], None
        return [{"umm": {"GranuleUR": ur}} for ur in urs], "page-2"

    return search


def test_completeness_clean(tiny: TinyCollection) -> None:
    urs = [entry.granule_ur for entry in tiny.inventory.granules]
    assert (
        vs.verify_completeness("C1", tiny.inventory, set(), search=fake_search(urs))
        == []
    )


def test_completeness_reports_granule_missing_from_store(
    tiny: TinyCollection,
) -> None:
    urs = [entry.granule_ur for entry in tiny.inventory.granules] + ["brand_new"]
    problems = vs.verify_completeness(
        "C1", tiny.inventory, set(), search=fake_search(urs)
    )
    assert problems == [
        "completeness: brand_new exists in CMR but is neither in the store "
        "manifest nor the pending ledger"
    ]
    # A granule waiting in the pending ledger is not a finding.
    assert (
        vs.verify_completeness(
            "C1", tiny.inventory, {"brand_new"}, search=fake_search(urs)
        )
        == []
    )


def test_completeness_reports_granule_gone_from_cmr(tiny: TinyCollection) -> None:
    urs = [entry.granule_ur for entry in tiny.inventory.granules][:-1]
    problems = vs.verify_completeness(
        "C1", tiny.inventory, set(), search=fake_search(urs)
    )
    assert len(problems) == 1 and "CMR no longer lists it" in problems[0]


def test_fill_values_decode_to_nan_and_verify_clean(tiny: TinyCollection) -> None:
    """A store built from the template must mask fills on decoded reads;
    the decoded comparison fails if the _FillValue attribute goes missing."""
    import h5py
    import numpy as np

    with h5py.File(tiny.granule_paths[0], "a") as f:
        data = f["product/vertical_column"][:]
        data[0, 0, 0] = -1.0e30
        f["product/vertical_column"][...] = data
    processor = backfill_and_promote(tiny)
    repo = processor.open_backfill_repo()
    problems = vs.verify_store(
        repo, tiny.inventory, samples=len(tiny.urls), window=6, seed=0
    )
    assert problems == []

    import xarray as xr

    session = repo.readonly_session("main")
    decoded = xr.open_dataset(session.store, engine="zarr", consolidated=False)
    assert np.isnan(decoded["vertical_column"].isel(time=0).values[0, 0])
