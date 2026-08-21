"""Scheduled CMR poller: the feeder for the forward-processing queue.

ASDC publishes no SNS topic for ``asdc-prod-protected``, so this Lambda
polls CMR instead. Every granule of the collection whose ``revision_date``
advanced past a persisted watermark (minus a 24 h overlap window) is
enqueued as ``{"url": "s3://.../*.nc"}``. Revision-date polling captures
new scans, republications, and historical arrivals alike. Duplicate
enqueues are harmless because the consumer's routing is idempotent, so
the watermark does not need to be exact.

Kept lightweight on purpose: stdlib HTTP against CMR's public search API
(metadata needs no Earthdata credentials) plus boto3.
"""

from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Optional

if TYPE_CHECKING:
    from mypy_boto3_sqs.type_defs import SendMessageBatchRequestEntryTypeDef

from aws_lambda_powertools import Logger

logger = Logger()

CMR_GRANULES_URL = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"
PAGE_SIZE = 2000
OVERLAP = timedelta(hours=24)
DEFAULT_LOOKBACK = timedelta(days=8)

# (concept_id, since_iso, search_after) -> (items, next_search_after)
FetchPage = Callable[[str, str, Optional[str]], tuple[list[dict], Optional[str]]]


def direct_s3_url(umm: dict[str, Any]) -> str | None:
    """Return the granule's direct in-region s3:// data link, if any."""
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


def initial_watermark(now: datetime) -> datetime:
    """Choose the starting point for a first poll (no watermark yet).

    ``$POLL_START_ISO`` (typically the backfill inventory's build time,
    covering everything published while the backfill ran) wins; otherwise
    a fixed lookback. The overlap window and the consumer's idempotent
    routing absorb any imprecision.
    """
    start = os.environ.get("POLL_START_ISO")
    if start:
        return datetime.fromisoformat(start)
    return now - DEFAULT_LOOKBACK


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
    """Enqueue every url, or raise.

    ``send_message_batch`` is not all-or-nothing: entries can fail while
    the call succeeds. Failed entries are retried once; if any still fail,
    raise so the watermark is not advanced and the next poll re-covers the
    window. A dropped entry would otherwise be lost once its revision date
    fell behind the watermark's overlap.
    """
    import boto3

    client = boto3.client("sqs")
    sent = 0
    for start in range(0, len(urls), 10):
        batch = urls[start : start + 10]
        entries: "list[SendMessageBatchRequestEntryTypeDef]" = [
            {"Id": str(i), "MessageBody": json.dumps({"url": url})}
            for i, url in enumerate(batch)
        ]
        for attempt in range(2):
            failed = client.send_message_batch(QueueUrl=queue_url, Entries=entries).get(
                "Failed", []
            )
            if not failed:
                break
            failed_ids = {item["Id"] for item in failed}
            entries = [entry for entry in entries if entry["Id"] in failed_ids]
        if failed:
            raise RuntimeError(
                f"{len(failed)} messages failed to enqueue after a retry "
                f"(first: {failed[0]}); leaving the watermark unadvanced"
            )
        sent += len(batch)
    return sent


@logger.inject_lambda_context()
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    concept_id = os.environ["CONCEPT_ID"]
    queue_url = os.environ["QUEUE_URL"]
    watermark_uri = os.environ["POLL_WATERMARK_URI"]

    started = datetime.now(timezone.utc)
    watermark = read_watermark(watermark_uri) or initial_watermark(started)
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
