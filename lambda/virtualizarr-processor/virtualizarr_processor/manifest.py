"""The store manifest and pending ledger (design spec invariant I4).

The **store manifest** is the store's living inventory — the same
``BackfillInventory`` document, kept in S3 next to the repo. Backfill's
promote writes it (equal to the backfill inventory); the forward consumer
and the re-sort job update it. It is what maps each time slice back to
its source granule, which the re-sort job and the QA sampler both need.
``validate_against_axis`` is the trust boundary: every consumer checks it
bit-exactly against the store's actual axis before relying on it.

The **pending ledger** holds granules that arrived out of order (the
historical drip-feed, adjacent-scan swaps) and are waiting for the
scheduled re-sort job to fold them in. Appends dedupe by granule UR, so
at-least-once SQS delivery is harmless.

URIs may be ``s3://bucket/key`` (boto3, imported lazily) or plain
filesystem paths (tests, local runs). Both artifacts assume the
single-writer discipline the deployment enforces (consumer at reserved
concurrency 1; re-sort serialized with the consumer).
"""

from __future__ import annotations

import json
from collections.abc import Collection, Iterable
from pathlib import Path
from typing import Any

import numpy as np

from virtualizarr_processor.inventory import BackfillInventory, GranuleEntry
from virtualizarr_processor.store_template import StoreValidationError


def _is_s3(uri: str) -> bool:
    return uri.startswith("s3://")


def _split(uri: str) -> tuple[str, str]:
    bucket, _, key = uri.removeprefix("s3://").partition("/")
    return bucket, key


def _s3_client() -> Any:
    import boto3  # deferred: provided by the Lambda runtime / dev deps

    return boto3.client("s3")


def _read_bytes(uri: str) -> bytes | None:
    """The object's bytes, or None if it does not exist."""
    if _is_s3(uri):
        client = _s3_client()
        bucket, key = _split(uri)
        try:
            return bytes(client.get_object(Bucket=bucket, Key=key)["Body"].read())
        except client.exceptions.NoSuchKey:
            return None
    path = Path(uri)
    return path.read_bytes() if path.exists() else None


def _write_bytes(uri: str, data: bytes) -> None:
    if _is_s3(uri):
        bucket, key = _split(uri)
        _s3_client().put_object(Bucket=bucket, Key=key, Body=data)
    else:
        Path(uri).parent.mkdir(parents=True, exist_ok=True)
        Path(uri).write_bytes(data)


class StoreManifest:
    """The store's living typed inventory at a URI."""

    @classmethod
    def read(cls, uri: str) -> BackfillInventory:
        data = _read_bytes(uri)
        if data is None:
            raise FileNotFoundError(f"store manifest not found at {uri}")
        return BackfillInventory.from_json(data)

    @staticmethod
    def write(uri: str, inventory: BackfillInventory) -> None:
        _write_bytes(uri, inventory.to_json().encode())

    @staticmethod
    def validate_against_axis(inventory: BackfillInventory, axis: np.ndarray) -> None:
        """Bit-exact agreement between the manifest and the store's axis."""
        expected = inventory.times()
        actual = np.asarray(axis, dtype=np.float64)
        if actual.shape != expected.shape or not np.array_equal(actual, expected):
            raise StoreValidationError(
                [
                    f"store manifest ({expected.shape[0]} granules) does not "
                    f"match the store time axis ({actual.shape[0]} steps) "
                    "bit-exactly; refusing to trust the manifest"
                ]
            )


class PendingLedger:
    """Out-of-order arrivals awaiting the re-sort job, deduped by granule UR."""

    @classmethod
    def read(cls, uri: str) -> tuple[GranuleEntry, ...]:
        data = _read_bytes(uri)
        if data is None:
            return ()
        return tuple(GranuleEntry.model_validate(item) for item in json.loads(data))

    @staticmethod
    def _write(uri: str, entries: Iterable[GranuleEntry]) -> None:
        payload = json.dumps(
            [entry.model_dump() for entry in entries], indent=1
        ).encode()
        _write_bytes(uri, payload)

    @classmethod
    def append(cls, uri: str, entries: Iterable[GranuleEntry]) -> None:
        existing = list(cls.read(uri))
        seen = {entry.granule_ur for entry in existing}
        for entry in entries:
            if entry.granule_ur not in seen:
                existing.append(entry)
                seen.add(entry.granule_ur)
        cls._write(uri, existing)

    @classmethod
    def remove(cls, uri: str, granule_urs: Collection[str]) -> None:
        remaining = [e for e in cls.read(uri) if e.granule_ur not in granule_urs]
        cls._write(uri, remaining)
