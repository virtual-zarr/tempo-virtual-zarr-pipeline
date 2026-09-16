"""Measure TEMPO publication behavior: out-of-order rate and production lag.

Re-derives the design doc's "fact 8" (~7% of adjacent pairs swapped,
~0.4% republished) instead of leaving it folklore: fetches the granules
whose ``revision_date`` falls in a recent window, orders them by
publication time, and counts adjacent pairs whose scan times are
inverted. Inversions are split into adjacent swaps (scan times within
--swap-window hours) and historical arrivals (the V04 archive is still
being drip-fed backwards), so the two phenomena aren't conflated.

Caveat: CMR exposes only the *latest* revision's date, so for the rare
republished granule (revision-id > 1) publication time is the redelivery
time, not first publication. At the observed 0.4% republication rate
this cannot move the headline number materially.

Usage:
    uv run scripts/measure_publish_order.py --collection hcho --days 30
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

CMR_GRANULES_URL = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"
PAGE_SIZE = 2000
CONCEPT_IDS = {
    "hcho": "C3685897141-LARC_CLOUD",
    "no2": "C3685896708-LARC_CLOUD",
}


@dataclass(frozen=True)
class Granule:
    published: datetime  # meta revision-date (latest revision)
    revision_id: int
    scan_start: datetime  # umm BeginningDateTime
    production: datetime | None = None  # umm DataGranule.ProductionDateTime


@dataclass(frozen=True)
class Report:
    total: int
    pairs: int
    inversions: int
    adjacent_swaps: int
    historical_arrivals: int
    republished: int

    @property
    def inversion_pct(self) -> float:
        return 100.0 * self.inversions / self.pairs if self.pairs else 0.0

    @property
    def republished_pct(self) -> float:
        return 100.0 * self.republished / self.total if self.total else 0.0


def measure(granules: list[Granule], swap_window: timedelta) -> Report:
    """Count scan-time inversions between publication-order neighbours."""
    ordered = sorted(granules, key=lambda g: g.published)
    inversions = swaps = historical = 0
    for earlier, later in zip(ordered, ordered[1:]):
        if later.scan_start < earlier.scan_start:
            inversions += 1
            if earlier.scan_start - later.scan_start <= swap_window:
                swaps += 1
            else:
                historical += 1
    # ASDC's ingest revises every granule once on arrival (all granules sit
    # at revision 2), so "republished" means revised beyond the sample's
    # baseline revision, not simply revision-id > 1.
    baseline = min((g.revision_id for g in ordered), default=1)
    return Report(
        total=len(ordered),
        pairs=max(len(ordered) - 1, 0),
        inversions=inversions,
        adjacent_swaps=swaps,
        historical_arrivals=historical,
        republished=sum(g.revision_id > baseline for g in ordered),
    )


@dataclass(frozen=True)
class LagReport:
    fresh: int
    median: timedelta | None  # scan start -> CMR publication
    p90: timedelta | None
    median_production: timedelta | None  # scan start -> ProductionDateTime
    median_delivery: timedelta | None  # ProductionDateTime -> CMR publication


def _percentile(deltas: list[timedelta], q: float) -> timedelta:
    ordered = sorted(deltas)
    return ordered[min(int(q * len(ordered)), len(ordered) - 1)]


def measure_lag(granules: list[Granule], window_start: datetime) -> LagReport:
    """Publication lag of fresh scans: scan start -> CMR revision date.

    Only scans from inside the lookback window count — a historical
    arrival would report its multi-year archive delay as "lag". Granules
    revised beyond the sample's baseline revision are excluded too: CMR
    keeps only the latest revision's date, which measures the redelivery,
    not first publication. Where ``ProductionDateTime`` is present the
    median is split into processing (scan -> production) and delivery
    (production -> publication).
    """
    baseline = min((g.revision_id for g in granules), default=1)
    fresh = [
        g
        for g in granules
        if g.scan_start >= window_start and g.revision_id == baseline
    ]
    if not fresh:
        return LagReport(0, None, None, None, None)
    lags = [g.published - g.scan_start for g in fresh]
    processing = [
        g.production - g.scan_start for g in fresh if g.production is not None
    ]
    delivery = [g.published - g.production for g in fresh if g.production is not None]
    return LagReport(
        fresh=len(fresh),
        median=_percentile(lags, 0.5),
        p90=_percentile(lags, 0.9),
        median_production=_percentile(processing, 0.5) if processing else None,
        median_delivery=_percentile(delivery, 0.5) if delivery else None,
    )


def _hours(delta: timedelta | None) -> str:
    return "n/a" if delta is None else f"{delta.total_seconds() / 3600:.1f} h"


def parse_granule(item: dict[str, Any]) -> Granule:
    meta = item["meta"]
    begin = item["umm"]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"]
    production = item["umm"].get("DataGranule", {}).get("ProductionDateTime")
    return Granule(
        published=_parse_iso(meta["revision-date"]),
        revision_id=int(meta["revision-id"]),
        scan_start=_parse_iso(begin),
        production=_parse_iso(production) if production else None,
    )


def _parse_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def fetch_granules(concept_id: str, since_iso: str, limit: int | None) -> list[Granule]:
    """Page the CMR search API, same shape of query as the poller Lambda."""
    granules: list[Granule] = []
    search_after: str | None = None
    while True:
        params = urllib.parse.urlencode(
            {
                "collection_concept_id": concept_id,
                "revision_date": f"{since_iso},",
                "page_size": PAGE_SIZE,
            }
        )
        request = urllib.request.Request(f"{CMR_GRANULES_URL}?{params}")
        if search_after:
            request.add_header("CMR-Search-After", search_after)
        with urllib.request.urlopen(request, timeout=60) as response:
            payload = json.loads(response.read())
            search_after = response.headers.get("CMR-Search-After")
        items = payload.get("items", [])
        granules.extend(parse_granule(item) for item in items)
        if not items or not search_after or (limit and len(granules) >= limit):
            break
    return granules[:limit] if limit else granules


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--collection", choices=sorted(CONCEPT_IDS), default="hcho")
    parser.add_argument("--concept-id", help="override the collection concept id")
    parser.add_argument("--days", type=int, default=30, help="revision-date lookback")
    parser.add_argument("--limit", type=int, help="cap the number of granules")
    parser.add_argument(
        "--swap-window",
        type=float,
        default=24.0,
        help="hours separating an adjacent swap from a historical arrival",
    )
    args = parser.parse_args()

    concept_id = args.concept_id or CONCEPT_IDS[args.collection]
    since = datetime.now(timezone.utc) - timedelta(days=args.days)
    granules = fetch_granules(concept_id, since.isoformat(), args.limit)
    if not granules:
        print(f"no granules for {concept_id} in the last {args.days} days")
        return 1

    report = measure(granules, timedelta(hours=args.swap_window))
    print(f"{concept_id}: {report.total} granules published in {args.days} days")
    print(
        f"  out of scan-time order: {report.inversions}/{report.pairs} "
        f"adjacent pairs ({report.inversion_pct:.1f}%)"
    )
    print(f"    adjacent swaps (< {args.swap_window:g} h): {report.adjacent_swaps}")
    print(f"    historical arrivals: {report.historical_arrivals}")
    print(
        f"  republished (revised beyond ingest baseline): {report.republished} "
        f"({report.republished_pct:.1f}%)"
    )
    lag = measure_lag(granules, since)
    if lag.fresh:
        print(
            f"  production lag, scan start -> CMR publication "
            f"({lag.fresh} fresh scans): median {_hours(lag.median)}, "
            f"p90 {_hours(lag.p90)}"
        )
        if lag.median_production is not None:
            processing, delivery = (
                _hours(lag.median_production),
                _hours(lag.median_delivery),
            )
            print(f"    scan -> ProductionDateTime: median {processing}")
            print(f"    ProductionDateTime -> publication: median {delivery}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
