"""The measurement core of scripts/measure_publish_order.py, offline."""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from measure_publish_order import Granule, measure, measure_lag, parse_granule

SWAP_WINDOW = timedelta(hours=24)
BASE = datetime(2026, 8, 19, tzinfo=timezone.utc)


def granule(
    published_min: int,
    scan_min: int,
    revision_id: int = 1,
    production_min: int | None = None,
) -> Granule:
    return Granule(
        published=BASE + timedelta(minutes=published_min),
        revision_id=revision_id,
        scan_start=BASE + timedelta(minutes=scan_min),
        production=(
            BASE + timedelta(minutes=production_min)
            if production_min is not None
            else None
        ),
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
            },
            "DataGranule": {"ProductionDateTime": "2026-08-19T19:30:00Z"},
        },
    }
    parsed = parse_granule(item)
    assert parsed.revision_id == 2
    assert parsed.scan_start == datetime(2026, 8, 19, 17, 42, tzinfo=timezone.utc)
    assert parsed.published.tzinfo is not None
    assert parsed.production == datetime(2026, 8, 19, 19, 30, tzinfo=timezone.utc)


def test_parse_granule_tolerates_missing_production_date() -> None:
    item = {
        "meta": {"revision-date": "2026-08-19T18:00:05Z", "revision-id": "2"},
        "umm": {
            "TemporalExtent": {
                "RangeDateTime": {"BeginningDateTime": "2026-08-19T17:42:00Z"}
            }
        },
    }
    assert parse_granule(item).production is None


def test_lag_is_measured_scan_to_publication_with_production_split() -> None:
    # scan at t=0, produced at t=120, published at t=180: 3 h lag, 2 h + 1 h
    report = measure_lag([granule(180, 0, production_min=120)], window_start=BASE)
    assert report.fresh == 1
    assert report.median == timedelta(hours=3) == report.p90
    assert report.median_production == timedelta(hours=2)
    assert report.median_delivery == timedelta(hours=1)


def test_lag_excludes_historical_arrivals_and_republications() -> None:
    fresh = granule(180, 0, revision_id=2)
    historical = granule(200, -1_000_000, revision_id=2)  # archive drip-feed
    republished = granule(600, 10, revision_id=3)  # revision-date = redelivery
    report = measure_lag([fresh, historical, republished], window_start=BASE)
    assert report.fresh == 1
    assert report.median == timedelta(hours=3)
    # production split degrades to n/a when no fresh granule carries the field
    assert report.median_production is None and report.median_delivery is None


def test_lag_report_empty_when_window_holds_no_fresh_scans() -> None:
    report = measure_lag([granule(200, -1_000_000)], window_start=BASE)
    assert report.fresh == 0 and report.median is None
