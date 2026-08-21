"""The measurement core of scripts/measure_publish_order.py, offline."""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from measure_publish_order import Granule, measure, parse_granule

SWAP_WINDOW = timedelta(hours=24)


def granule(published_min: int, scan_min: int, revision_id: int = 1) -> Granule:
    base = datetime(2026, 8, 19, tzinfo=timezone.utc)
    return Granule(
        published=base + timedelta(minutes=published_min),
        revision_id=revision_id,
        scan_start=base + timedelta(minutes=scan_min),
    )


def test_in_order_publications_have_no_inversions() -> None:
    report = measure([granule(0, 0), granule(10, 60), granule(20, 120)], SWAP_WINDOW)
    assert report.inversions == 0
    assert report.inversion_pct == 0.0


def test_swapped_adjacent_pair_counts_as_adjacent_swap() -> None:
    # scans S010 then S009 published in reverse: one inversion, within window
    report = measure([granule(0, 60), granule(10, 0)], SWAP_WINDOW)
    assert report.inversions == 1 and report.adjacent_swaps == 1
    assert report.historical_arrivals == 0
    assert report.inversion_pct == 100.0


def test_old_archive_granule_counts_as_historical_arrival() -> None:
    # a years-old granule drip-fed between two fresh scans
    old = granule(10, -1_000_000)
    report = measure([granule(0, 0), old, granule(20, 60)], SWAP_WINDOW)
    assert report.inversions == 1 and report.historical_arrivals == 1
    assert report.adjacent_swaps == 0


def test_publication_order_is_derived_by_sorting_not_input_order() -> None:
    # same granules, shuffled input: identical report
    granules = [granule(20, 120), granule(0, 60), granule(10, 0)]
    report = measure(granules, SWAP_WINDOW)
    assert report.inversions == 1 and report.adjacent_swaps == 1


def test_republished_means_revised_beyond_the_sample_baseline() -> None:
    # ASDC revises every granule once on ingest, so a uniform revision-id 2
    # is the baseline, not evidence of republication
    uniform = measure([granule(0, 0, 2), granule(10, 60, 2)], SWAP_WINDOW)
    assert uniform.republished == 0
    mixed = measure([granule(0, 0, 2), granule(10, 60, 3)], SWAP_WINDOW)
    assert mixed.republished == 1
    assert mixed.republished_pct == 50.0


def test_parse_granule_reads_cmr_umm_item() -> None:
    item = {
        "meta": {"revision-date": "2026-08-19T18:00:05.123Z", "revision-id": "2"},
        "umm": {
            "TemporalExtent": {
                "RangeDateTime": {"BeginningDateTime": "2026-08-19T17:42:00Z"}
            }
        },
    }
    parsed = parse_granule(item)
    assert parsed.revision_id == 2
    assert parsed.scan_start == datetime(2026, 8, 19, 17, 42, tzinfo=timezone.utc)
    assert parsed.published.tzinfo is not None
