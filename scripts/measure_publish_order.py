"""Measure how often CMR publishes TEMPO granules out of scan-time order.

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


def parse_granule(item: dict[str, Any]) -> Granule:
    meta = item["meta"]
    begin = item["umm"]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"]
    return Granule(
        published=_parse_iso(meta["revision-date"]),
        revision_id=int(meta["revision-id"]),
        scan_start=_parse_iso(begin),
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
    return 0


if __name__ == "__main__":
    sys.exit(main())
