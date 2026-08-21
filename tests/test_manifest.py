"""StoreManifest / PendingLedger round-trips against an icechunk store."""

import icechunk
import pytest
import zarr
import zarr.abc.store
from virtualizarr_processor.inventory import BackfillInventory, GranuleEntry
from virtualizarr_processor.manifest import (
    MANIFEST_ARRAYS,
    PendingLedger,
    StoreManifest,
)


def entry(i: int) -> GranuleEntry:
    return GranuleEntry(url=f"s3://b/g{i}.nc", granule_ur=f"G{i}", time=float(i))


def inventory(n: int) -> BackfillInventory:
    return BackfillInventory(
        schema="tempo-backfill-inventory/1",
        collection="TEMPO_HCHO_L3",
        concept_id="C1",
        time_units="seconds since 1980-01-06",
        built_at="2026-08-20T00:00:00Z",
        granules=tuple(entry(i) for i in range(n)),
    )


@pytest.fixture()
def store() -> zarr.abc.store.Store:
    """A minimal store: time axis + manifest arrays, as the template creates."""
    repo = icechunk.Repository.create(storage=icechunk.in_memory_storage())
    session = repo.writable_session("main")
    zarr.create_array(
        session.store,
        name="time",
        shape=(0,),
        chunks=(8,),
        dtype="float64",
        dimension_names=("time",),
    )
    for name in MANIFEST_ARRAYS:
        zarr.create_array(
            session.store,
            name=name,
            shape=(0,),
            chunks=(8,),
            dtype="str",
            dimension_names=("time",),
        )
    return session.store


def test_manifest_round_trip(store: zarr.abc.store.Store) -> None:
    inv = inventory(3)
    zarr.open_array(store, path="time").resize((3,))
    zarr.open_array(store, path="time")[:] = inv.times()
    StoreManifest.write(store, inv)
    assert StoreManifest.read(store) == inv


def test_manifest_read_none_before_write(store: zarr.abc.store.Store) -> None:
    assert StoreManifest.read(store) is None


def test_manifest_read_runs_inventory_validators(
    store: zarr.abc.store.Store,
) -> None:
    inv = inventory(2)
    zarr.open_array(store, path="time").resize((2,))
    zarr.open_array(store, path="time")[:] = inv.times()
    StoreManifest.write(store, inv)
    # Corrupt: duplicate UR directly in the array.
    zarr.open_array(store, path="granule_ur")[1] = "G0"
    with pytest.raises(ValueError, match="duplicate granule_ur"):
        StoreManifest.read(store)


def test_pending_ledger_round_trip_and_dedupe(store: zarr.abc.store.Store) -> None:
    assert PendingLedger.read(store) == ()
    PendingLedger.append(store, [entry(1)])
    PendingLedger.append(store, [entry(1), entry(2)])  # redelivery of 1
    assert [e.granule_ur for e in PendingLedger.read(store)] == ["G1", "G2"]
    PendingLedger.write(store, [entry(2)])
    assert [e.granule_ur for e in PendingLedger.read(store)] == ["G2"]


def test_pending_ledger_append_replaces_stale_entry_on_redelivery(
    store: zarr.abc.store.Store,
) -> None:
    """A republished pending granule with a corrected time/url must replace
    the stale entry, not be dropped by it — an unreplaced stale entry
    crash-loops the resort fold forever (review finding I2)."""
    PendingLedger.append(store, [entry(1)])
    corrected = GranuleEntry(url="s3://b/g1-corrected.nc", granule_ur="G1", time=99.0)
    PendingLedger.append(store, [corrected])
    ledger = PendingLedger.read(store)
    assert len(ledger) == 1
    assert ledger[0] == corrected


def test_state_rides_the_commit() -> None:
    """Manifest + ledger written through a session survive commit, and an
    attrs-only change is a committable session change (no empty commit)."""
    repo = icechunk.Repository.create(storage=icechunk.in_memory_storage())
    session = repo.writable_session("main")
    zarr.create_array(
        session.store,
        name="time",
        shape=(1,),
        chunks=(8,),
        dtype="float64",
        dimension_names=("time",),
    )
    for name in MANIFEST_ARRAYS:
        zarr.create_array(
            session.store,
            name=name,
            shape=(1,),
            chunks=(8,),
            dtype="str",
            dimension_names=("time",),
        )
    inv = inventory(1)
    zarr.open_array(session.store, path="time")[:] = inv.times()
    StoreManifest.write(session.store, inv)
    session.commit("init")

    deferral = repo.writable_session("main")
    PendingLedger.append(deferral.store, [entry(7)])  # attrs-only change
    deferral.commit("defer")  # must not raise NoChangesToCommitError

    readonly = repo.readonly_session("main")
    assert StoreManifest.read(readonly.store) == inv
    assert [e.granule_ur for e in PendingLedger.read(readonly.store)] == ["G7"]


def test_storage_prefix_combines_like_the_cdk_stack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from virtualizarr_processor.manifest import storage_prefix

    monkeypatch.delenv("S3_PREFIX", raising=False)
    monkeypatch.delenv("ICECHUNK_PREFIX", raising=False)
    assert storage_prefix() is None

    monkeypatch.setenv("ICECHUNK_PREFIX", "hcho-v04/")
    assert storage_prefix() == "hcho-v04"

    monkeypatch.setenv("S3_PREFIX", "/tempo/")
    assert storage_prefix() == "tempo/hcho-v04"
