"""Scheduled CMR poller: the feeder for the forward-processing queue.

ASDC publishes no SNS topic for ``asdc-prod-protected``, so this Lambda
polls CMR instead (design spec §5 "Feeding the queue"): every granule of
the collection whose ``revision_date`` advanced past a persisted watermark
(minus a 24 h overlap window) is enqueued as ``{"url": "s3://.../*.nc"}``.
Revision-date polling captures new scans, republications, and the
historical drip-feed alike, and duplicate enqueues are harmless — the
consumer's routing is idempotent — so the watermark needs no exactness.

Deliberately lightweight: stdlib HTTP against CMR's public search API
(no Earthdata credentials needed for metadata) plus boto3.
"""

from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from aws_lambda_powertools import Logger

logger = Logger()

CMR_GRANULES_URL = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"
PAGE_SIZE = 2000
OVERLAP = timedelta(hours=24)
DEFAULT_LOOKBACK = timedelta(days=8)

# (concept_id, since_iso, search_after) -> (items, next_search_after)
FetchPage = Callable[[str, str, Optional[str]], tuple[list[dict], Optional[str]]]


def direct_s3_url(umm: dict[str, Any]) -> str | None:
    """The granule's direct in-region s3:// data link, if it has one."""
    for related in umm.get("RelatedUrls", []):
        url = related.get("URL", "")
        if (
            related.get("Type") == "GET DATA VIA DIRECT ACCESS"
            and url.startswith("s3://")
            and url.endswith(".nc")
        ):
            return str(url)
    return None


def _http_fetch(
    concept_id: str, since_iso: str, search_after: str | None
) -> tuple[list[dict], str | None]:
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
        return payload.get("items", []), response.headers.get("CMR-Search-After")


def search_granules(
    concept_id: str, since_iso: str, fetch: FetchPage | None = None
) -> list[dict]:
    fetch = fetch or _http_fetch  # resolved at call time (tests monkeypatch it)
    items: list[dict] = []
    search_after: str | None = None
    while True:
        page, search_after = fetch(concept_id, since_iso, search_after)
        items.extend(page)
        if not page or not search_after:
            return items


def read_watermark(uri: str) -> datetime | None:
    data = _read_bytes(uri)
    if data is None:
        return None
    return datetime.fromisoformat(json.loads(data)["revision_date"])


def write_watermark(uri: str, value: datetime) -> None:
    _write_bytes(uri, json.dumps({"revision_date": value.isoformat()}).encode())


def _read_bytes(uri: str) -> bytes | None:
    if uri.startswith("s3://"):
        import boto3

        bucket, _, key = uri.removeprefix("s3://").partition("/")
        client = boto3.client("s3")
        try:
            return bytes(client.get_object(Bucket=bucket, Key=key)["Body"].read())
        except client.exceptions.NoSuchKey:
            return None
    path = Path(uri)
    return path.read_bytes() if path.exists() else None


def _write_bytes(uri: str, data: bytes) -> None:
    if uri.startswith("s3://"):
        import boto3

        bucket, _, key = uri.removeprefix("s3://").partition("/")
        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=data)
    else:
        Path(uri).parent.mkdir(parents=True, exist_ok=True)
        Path(uri).write_bytes(data)


def enqueue(queue_url: str, urls: list[str]) -> int:
    import boto3

    client = boto3.client("sqs")
    sent = 0
    for start in range(0, len(urls), 10):
        batch = urls[start : start + 10]
        client.send_message_batch(
            QueueUrl=queue_url,
            Entries=[
                {"Id": str(i), "MessageBody": json.dumps({"url": url})}
                for i, url in enumerate(batch)
            ],
        )
        sent += len(batch)
    return sent


@logger.inject_lambda_context()
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    concept_id = os.environ["CONCEPT_ID"]
    queue_url = os.environ["QUEUE_URL"]
    watermark_uri = os.environ["POLL_WATERMARK_URI"]

    started = datetime.now(timezone.utc)
    watermark = read_watermark(watermark_uri) or (started - DEFAULT_LOOKBACK)
    since = watermark - OVERLAP

    items = search_granules(concept_id, since.isoformat())
    urls = []
    for item in items:
        url = direct_s3_url(item.get("umm", {}))
        if url:
            urls.append(url)
        else:
            logger.warning(
                "Granule without a direct s3 .nc link",
                extra={"meta": item.get("meta", {})},
            )
    sent = enqueue(queue_url, urls)
    # The watermark is the poll start time; the overlap window plus the
    # consumer's idempotent routing absorb any boundary imprecision.
    write_watermark(watermark_uri, started)
    logger.info(
        "Poll complete",
        extra={"granules": len(items), "enqueued": sent, "since": since.isoformat()},
    )
    return {"enqueued": sent, "since": since.isoformat()}
