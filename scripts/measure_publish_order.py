"""Measure how TEMPO granules arrive in CMR: publication order and production lag.

NASA's data center (ASDC) publishes each TEMPO scan to the CMR catalog some
hours after the scan. This script fetches the granules published in the last
``--days`` days and reports, from CMR metadata alone (no credentials):

- how often consecutive publications are out of scan-time order, the reason
  the pipeline has a re-sort. An inversion is an *adjacent swap* when the two
  scans are within ``--swap-window`` hours of each other and a *historical
  arrival* when an old archive granule is back-filled between fresh ones;
- how many granules were republished (revised after they first appeared);
- production lag: how long after a scan starts its granule is in CMR, split
  into processing (scan start -> ``ProductionDateTime``) and delivery
  (``ProductionDateTime`` -> CMR ``revision-date``).

Usage:
    uv run scripts/measure_publish_order.py --collection hcho --days 30

Two things to know when reading the numbers:

- ASDC's ingest revises every granule once on arrival, so a whole sample sits
  at revision 2. "Republished" therefore means revised beyond the sample's
  lowest revision, not simply revision > 1.
- CMR keeps only the latest revision's date, so a republished granule's
  "publication" is its redelivery. The lag figures skip those granules, and
  any scan from before the window (its "lag" would be an archive delay).
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
    published: datetime  # CMR meta.revision-date (of the latest revision)
    revision_id: int
    scan_start: datetime  # UMM BeginningDateTime
    production: datetime | None = None  # UMM DataGranule.ProductionDateTime


@dataclass(frozen=True)
class Report:
    total: int
    pairs: int  # adjacent pairs in publication order
    inversions: int  # pairs whose scan times are in the opposite order
    adjacent_swaps: int
    historical_arrivals: int
    republished: int

    @property
    def inversion_pct(self) -> float:
        return 100.0 * self.inversions / self.pairs if self.pairs else 0.0

    @property
    def republished_pct(self) -> float:
        return 100.0 * self.republished / self.total if self.total else 0.0


@dataclass(frozen=True)
class LagReport:
    fresh: int  # scans from inside the window, not republished
    median: timedelta | None  # scan start -> CMR publication
    p90: timedelta | None
    median_processing: timedelta | None  # scan start -> ProductionDateTime
    median_delivery: timedelta | None  # ProductionDateTime -> CMR publication


def baseline_revision(granules: list[Granule]) -> int:
    """The revision every granule has after ingest; above it means republished."""
    return min((g.revision_id for g in granules), default=1)


def measure(granules: list[Granule], swap_window: timedelta) -> Report:
    """Count scan-time inversions between publication-order neighbours."""
    ordered = sorted(granules, key=lambda g: g.published)
    swaps = historical = 0
    for earlier, later in zip(ordered, ordered[1:]):
        if later.scan_start < earlier.scan_start:
            if earlier.scan_start - later.scan_start <= swap_window:
                swaps += 1
            else:
                historical += 1
    baseline = baseline_revision(granules)
    return Report(
        total=len(ordered),
        pairs=max(len(ordered) - 1, 0),
        inversions=swaps + historical,
        adjacent_swaps=swaps,
        historical_arrivals=historical,
        republished=sum(g.revision_id > baseline for g in ordered),
    )


def measure_lag(granules: list[Granule], window_start: datetime) -> LagReport:
    """Lag from scan start to CMR publication, for fresh, never-republished scans."""
    baseline = baseline_revision(granules)
    fresh = [
        g
        for g in granules
        if g.scan_start >= window_start and g.revision_id == baseline
    ]
    lags = [g.published - g.scan_start for g in fresh]
    processing = [g.production - g.scan_start for g in fresh if g.production]
    delivery = [g.published - g.production for g in fresh if g.production]
    return LagReport(
        fresh=len(fresh),
        median=percentile(lags, 0.5),
        p90=percentile(lags, 0.9),
        median_processing=percentile(processing, 0.5),
        median_delivery=percentile(delivery, 0.5),
    )


def percentile(deltas: list[timedelta], q: float) -> timedelta | None:
    """Nearest-rank percentile; None for an empty list."""
    if not deltas:
        return None
    ordered = sorted(deltas)
    return ordered[min(int(q * len(ordered)), len(ordered) - 1)]


def parse_granule(item: dict[str, Any]) -> Granule:
    """Build a Granule from one item of a CMR ``umm_json`` search response."""
    meta, umm = item["meta"], item["umm"]
    production = umm.get("DataGranule", {}).get("ProductionDateTime")
    return Granule(
        published=datetime.fromisoformat(meta["revision-date"]),
        revision_id=int(meta["revision-id"]),
        scan_start=datetime.fromisoformat(
            umm["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"]
        ),
        production=datetime.fromisoformat(production) if production else None,
    )


def fetch_granules(
    concept_id: str, since: datetime, limit: int | None
) -> list[Granule]:
    """Page through CMR for granules revised since ``since`` (the poller's query)."""
    params = urllib.parse.urlencode(
        {
            "collection_concept_id": concept_id,
            "revision_date": f"{since.isoformat()},",
            "page_size": PAGE_SIZE,
        }
    )
    granules: list[Granule] = []
    search_after = None  # CMR's cursor for the next page
    while True:
        request = urllib.request.Request(f"{CMR_GRANULES_URL}?{params}")
        if search_after:
            request.add_header("CMR-Search-After", search_after)
        with urllib.request.urlopen(request, timeout=60) as response:
            items = json.loads(response.read()).get("items", [])
            search_after = response.headers.get("CMR-Search-After")
        granules.extend(parse_granule(item) for item in items)
        if not items or not search_after or (limit and len(granules) >= limit):
            return granules[:limit]


def hours(delta: timedelta | None) -> str:
    return "n/a" if delta is None else f"{delta.total_seconds() / 3600:.1f} h"


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
    granules = fetch_granules(concept_id, since, args.limit)
    if not granules:
        print(f"no granules for {concept_id} in the last {args.days} days")
        return 1

    order = measure(granules, timedelta(hours=args.swap_window))
    lag = measure_lag(granules, since)
    print(f"{concept_id}: {order.total} granules published in {args.days} days")
    print(
        f"  out of scan-time order: {order.inversions}/{order.pairs} "
        f"adjacent pairs ({order.inversion_pct:.1f}%)"
    )
    print(f"    adjacent swaps (< {args.swap_window:g} h): {order.adjacent_swaps}")
    print(f"    historical arrivals: {order.historical_arrivals}")
    print(
        f"  republished (revised beyond ingest baseline): {order.republished} "
        f"({order.republished_pct:.1f}%)"
    )
    if lag.fresh:
        print(
            f"  production lag, scan start -> CMR publication ({lag.fresh} fresh "
            f"scans): median {hours(lag.median)}, p90 {hours(lag.p90)}"
        )
        processing, delivery = hours(lag.median_processing), hours(lag.median_delivery)
        print(f"    scan -> ProductionDateTime: median {processing}")
        print(f"    ProductionDateTime -> publication: median {delivery}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
