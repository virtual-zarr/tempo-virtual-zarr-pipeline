"""The store manifest and pending ledger.

The store manifest is the store's current inventory: the same
``BackfillInventory`` document, kept in S3 next to the repository. The
backfill promote writes it, and the forward consumer and re-sort job keep
it updated. It maps each time slice back to its source granule, which the
re-sort job and the verification script need. Consumers must call
``validate_against_axis`` before relying on it.

The pending ledger holds granules that arrived out of order and are
waiting for the scheduled re-sort job. Appends dedupe by granule UR, so
at-least-once SQS delivery is harmless.

URIs may be ``s3://bucket/key`` or plain filesystem paths. Both artifacts
rely on the deployment's single-writer setup (consumer at reserved
concurrency 1); a re-sort racing the consumer is caught by the promote's
compare-and-swap before either artifact is rewritten.
"""

from __future__ import annotations

import json
import os
from collections.abc import Collection, Iterable
from pathlib import Path
from typing import Any

import numpy as np

from virtualizarr_processor.inventory import BackfillInventory, GranuleEntry
from virtualizarr_processor.store_template import StoreValidationError


def storage_prefix() -> str | None:
    """The repository's S3 key prefix: $S3_PREFIX and $ICECHUNK_PREFIX joined.

    Deployed Lambdas receive the combined value as $ICECHUNK_PREFIX; local
    runs with a per-collection env file carry the two parts, joined here
    exactly as the CDK stack joins them.
    """
    return (
        "/".join(
            part.strip("/")
            for part in (os.environ.get("S3_PREFIX"), os.environ.get("ICECHUNK_PREFIX"))
            if part and part.strip("/")
        )
        or None
    )


def default_state_uri(filename: str) -> str:
    """The deployment-default URI for a state artifact next to the repo.

    Mirrors the CDK stack's ``s3://<bucket>/<prefix>/state/<filename>``
    derivation so local script runs resolve the same artifacts the Lambdas
    were deployed with. Explicit ``*_URI`` variables win at the call sites.
    """
    bucket = os.environ.get("ICECHUNK_BUCKET")
    if not bucket:
        raise ValueError(
            f"cannot derive the default URI for {filename}: ICECHUNK_BUCKET is "
            "not set (set the explicit *_URI variable instead)"
        )
    prefix = storage_prefix()
    return f"s3://{bucket}/{prefix + '/' if prefix else ''}state/{filename}"


def _is_s3(uri: str) -> bool:
    return uri.startswith("s3://")


def _split(uri: str) -> tuple[str, str]:
    bucket, _, key = uri.removeprefix("s3://").partition("/")
    return bucket, key


def _s3_client() -> Any:
    import boto3  # deferred: provided by the Lambda runtime / dev deps

    return boto3.client("s3")


def _read_bytes(uri: str) -> bytes | None:
    """Return the object's bytes, or None if it does not exist."""
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
    """Read and write the store's typed inventory at a URI."""

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
        """Check that the manifest matches the store's time axis exactly."""
        expected = inventory.times()
        actual = np.asarray(axis, dtype=np.float64)
        if actual.shape != expected.shape or not np.array_equal(actual, expected):
            raise StoreValidationError(
                [
                    f"store manifest ({expected.shape[0]} granules) does not "
                    f"match the store time axis ({actual.shape[0]} steps) "
                    "exactly; the manifest does not describe this store"
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
