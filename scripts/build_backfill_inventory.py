# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "earthaccess>=0.14",
#     "boto3>=1.34.0",
#     "h5py",
#     "obstore",
#     "obspec-utils",
#     "virtualizarr-processor",
# ]
#
# [tool.uv.sources]
# virtualizarr-processor = { path = "../lambda/virtualizarr-processor" }
# ///
"""Build the typed backfill inventory for the selected TEMPO L3 collection.

Produces the ``tempo-backfill-inventory/1`` JSON document the backfill
pipeline consumes: one entry per granule with its ``.nc`` data link, its
granule UR (the filename stem, the convention the forward consumer relies
on), and the granule's exact in-file ``/time[0]`` value.

The exact times matter. TEMPO's in-file scan time differs from the CMR
and filename timestamps (...T174200Z has /time = 17:42:18.02), and the
store's time axis is built from these values at Init. There is no
metadata-only source for them, so this script opens every granule's
header (a few KB each) with bounded concurrency and backoff.

Republished granules (same UR, new revision) are deduped keeping the
newest revision. The pydantic model validates the result before anything
is written.

Every run also reports a profile: per-granule read latencies, phase wall
times, and an RSS timeline (written under ``--profile-dir``, summary to
stderr) — the numbers that size the CodeBuild container and the
workers/timeout budget for a full ~17k-granule sweep.

Built for one environment: the stack's us-west-2 CodeBuild project,
which sets ``$EARTHDATA_TOKEN`` (required for the per-granule reads) and
passes ``--access direct --read-access external``.

Usage:
    uv run scripts/build_backfill_inventory.py
    uv run scripts/build_backfill_inventory.py --collection no2
    uv run scripts/build_backfill_inventory.py --start 2024-01-01 --end 2024-02-01
    uv run scripts/build_backfill_inventory.py --max-count 100 \
        --s3-uri s3://my-bucket/inventory/tempo-hcho-test.json
"""

import argparse
import csv
import functools
import os
import resource
import statistics
import sys
import threading
import time as time_module
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from virtualizarr_processor.collection import load_collection
from virtualizarr_processor.inventory import SCHEMA_ID, BackfillInventory, GranuleEntry

TIME_UNITS = "seconds since 1980-01-06T00:00:00Z"
READ_ATTEMPTS = 4
BACKOFF_SECONDS = (10, 30, 60)
PROGRESS_EVERY = 250
# Error-message substrings marking a permanent failure (expired/rejected
# token): retrying cannot help, fail fast instead of burning ~100s of
# backoff per granule.
PERMANENT_ERROR_MARKERS = ("401", "Unauthorized", "403", "Forbidden")


class InventoryError(Exception):
    """The granule set cannot form a valid backfill inventory."""


def _revision(granule: Any) -> int:
    return int(granule["meta"].get("revision-id", 0))


def data_link(granule: Any, access: str) -> str:
    links = [u for u in granule.data_links(access=access) if u.endswith(".nc")]
    if not links:
        raise InventoryError(
            f"No .nc data link for granule {granule['meta']['concept-id']}"
        )
    return str(links[0])


def dedupe_republications(granules: list[Any]) -> list[Any]:
    """Keep only the newest revision of each granule UR."""
    newest: dict[str, Any] = {}
    for granule in granules:
        ur = str(granule["umm"].get("GranuleUR", granule["meta"]["concept-id"]))
        if ur not in newest or _revision(granule) > _revision(newest[ur]):
            newest[ur] = granule
    return list(newest.values())


def instrumented_reader(
    read_time: Callable[[str], float],
    total: int,
    latencies: list[tuple[str, float]],
) -> Callable[[str], float]:
    """Wrap ``read_time`` with a progress line every PROGRESS_EVERY
    completions and per-granule latency capture (retries included)."""
    lock = threading.Lock()
    done = 0

    def read(url: str) -> float:
        nonlocal done
        start = time_module.monotonic()
        try:
            return read_time(url)
        finally:
            with lock:
                done += 1
                n = done
                latencies.append(
                    (url.rsplit("/", 1)[-1], round(time_module.monotonic() - start, 3))
                )
            # Early ticks distinguish "warming up" from "stalled" within
            # the first minute; a 250-granule first tick can be minutes
            # away and reads as a hang (observed 2026-09-09).
            if n in (10, 50) or n % PROGRESS_EVERY == 0 or n == total:
                # flush: CodeBuild streams the log; Python buffers stderr pipes
                print(f"  {n}/{total} headers read", file=sys.stderr, flush=True)

    return read


def build_inventory(
    granules: list[Any],
    *,
    access: str,
    read_access: str | None = None,
    read_time: Callable[[str], float],
    collection_shortname: str,
    concept_id: str,
    workers: int = 4,
    known_times: dict[str, dict] | None = None,
) -> BackfillInventory:
    """Build the validated typed inventory for ``granules``.

    ``read_time(url) -> float`` supplies each granule's exact in-file
    time; it is injectable so the logic can be tested offline.
    ``known_times`` is the previous run's sidecar cache
    (``{granule_ur: {"time": float, "revision": int}}``); a granule whose
    CMR revision matches its cached entry is not re-read — its /time
    cannot have changed. Raises ``InventoryError`` or
    ``pydantic.ValidationError`` for any set that cannot form a valid
    axis.
    """
    if not granules:
        raise InventoryError("No granules matched the query")
    deduped = dedupe_republications(granules)
    urls = [data_link(granule, access) for granule in deduped]
    # Recorded link flavor and read transport are independent; both are
    # normally the same direct s3:// links now that reads go over S3.
    read_urls = (
        urls
        if read_access in (None, access)
        else [data_link(granule, read_access) for granule in deduped]
    )

    known_times = known_times or {}
    cached: dict[int, float] = {}
    to_read: list[tuple[int, str]] = []
    for i, granule in enumerate(deduped):
        ur = str(granule["umm"].get("GranuleUR", granule["meta"]["concept-id"]))
        entry = known_times.get(ur)
        if entry is not None and entry.get("revision") == _revision(granule):
            cached[i] = float(entry["time"])
        else:
            to_read.append((i, read_urls[i]))
    if cached:
        print(
            f"  {len(cached)} of {len(deduped)} times reused from the "
            "previous inventory's sidecar cache",
            file=sys.stderr,
        )

    # Collect failures instead of letting the first one abort the pool:
    # on a ~17k-granule sweep, one bad granule at hour 6 must not discard
    # every completed read — finish the sweep, then report all failures.
    failures: list[tuple[str, str]] = []

    def read_or_record(item: tuple[int, str]) -> tuple[int, float]:
        index, url = item
        try:
            return index, read_time(url)
        except Exception as error:
            failures.append((url, f"{type(error).__name__}: {error}"))
            return index, float("nan")

    with ThreadPoolExecutor(max_workers=workers) as pool:
        fresh = dict(pool.map(read_or_record, to_read))
    if failures:
        preview = "; ".join(
            f"{u.rsplit('/', 1)[-1]} ({e})" for u, e in failures[:3]
        )
        raise InventoryError(
            f"{len(failures)} of {len(to_read)} granule reads failed — "
            f"first: {preview}"
        )
    times = [cached[i] if i in cached else fresh[i] for i in range(len(deduped))]

    entries = sorted(
        (
            GranuleEntry(
                url=url,
                granule_ur=url.rsplit("/", 1)[-1].removesuffix(".nc"),
                time=time_value,
            )
            for url, time_value in zip(urls, times)
        ),
        key=lambda entry: entry.time,
    )
    return BackfillInventory(
        schema=SCHEMA_ID,  # type: ignore[call-arg]
        collection=collection_shortname,
        concept_id=concept_id,
        time_units=TIME_UNITS,
        built_at=datetime.now(timezone.utc).isoformat(),
        granules=tuple(entries),
    )


def _with_retries(read_once: Callable[[str], float], url: str) -> float:
    """Run ``read_once(url)`` with the shared backoff/retry policy."""
    for attempt in range(READ_ATTEMPTS):
        try:
            return read_once(url)
        except Exception as error:
            if any(marker in str(error) for marker in PERMANENT_ERROR_MARKERS):
                raise
            if attempt == READ_ATTEMPTS - 1:
                raise
            delay = BACKOFF_SECONDS[min(attempt, len(BACKOFF_SECONDS) - 1)]
            print(
                f"  {url.rsplit('/', 1)[-1]}: {type(error).__name__} "
                f"(attempt {attempt + 1}), retrying in {delay}s",
                file=sys.stderr,
            )
            time_module.sleep(delay)
    raise AssertionError("unreachable")


def _s3_store_cls() -> Any:
    """Indirection so tests can substitute S3Store without obstore."""
    from obstore.store import S3Store

    return S3Store


def _https_store_cls() -> Any:
    """Indirection so tests can substitute HTTPStore without obstore."""
    from obstore.store import HTTPStore

    return HTTPStore


@functools.lru_cache(maxsize=4)
def _store_for_s3(bucket: str) -> Any:
    # Reuses the exact credential flow the deployed workers use
    # (granule.make_registry): EDL token -> temporary DAAC S3 creds,
    # auto-refreshed by the provider. Direct in-region range GETs skip
    # the TEA auth-redirect round trips that dominate HTTPS reads.
    from virtualizarr_processor import granule

    return _s3_store_cls().from_url(
        f"s3://{bucket}", credential_provider=granule.s3_credential_provider(bucket)
    )


@functools.lru_cache(maxsize=4)
def _store_for_https(host: str) -> Any:
    return _https_store_cls().from_url(
        f"https://{host}",
        client_options={
            "default_headers": {
                "Authorization": f"Bearer {os.environ['EARTHDATA_TOKEN']}"
            }
        },
    )


def _store_and_path(url: str) -> tuple[Any, str]:
    """Resolve a granule url to (obstore store, in-store path)."""
    if url.startswith("s3://"):
        bucket, _, path = url.removeprefix("s3://").partition("/")
        return _store_for_s3(bucket), path
    if url.startswith("https://"):
        host, _, path = url.removeprefix("https://").partition("/")
        return _store_for_https(host), path
    raise InventoryError(f"unsupported url scheme for header read: {url}")


def _read_once_block(url: str) -> float:
    """Read /time[0] through a 256 KB block cache.

    h5py touches ~2.3 KB in ~6 scattered regions of a ~900 MB granule
    (measured 2026-09-09 on TEMPO_NO2_L3_V04 S001); fsspec's 5 MB
    readahead turned that into ~37 MB transferred per granule. Small
    blocks keep it to ~1.5 MB in the same ~6 requests — deliberately
    small because the access pattern is scattered tiny metadata reads,
    not streaming.
    """
    import h5py
    from obspec_utils.readers import BlockStoreReader

    store, path = _store_and_path(url)
    reader = BlockStoreReader(store, path, block_size=256 * 1024, max_cached_blocks=64)
    with h5py.File(reader) as h5:
        return float(h5["time"][0])


def read_granule_time(url: str) -> float:
    """Read the granule's exact /time[0] from its header."""
    return _with_retries(_read_once_block, url)


def write_inventory(inventory: BackfillInventory, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(inventory.to_json() + "\n")


def search_granules(concept_id: str, start: str | None, end: str | None) -> list[Any]:
    """All CMR granules for the collection, optionally windowed in time."""
    import earthaccess  # deferred so the pure helpers are testable offline

    kwargs: dict[str, Any] = {"concept_id": concept_id, "count": -1}
    if start or end:
        kwargs["temporal"] = (start, end)
    return list(earthaccess.search_data(**kwargs))


def _cache_uri(s3_uri_or_path: str) -> str:
    return f"{s3_uri_or_path}.times.json"


def load_time_cache(s3_uri_or_path: str) -> dict[str, dict]:
    """Previous run's ``{granule_ur: {time, revision}}`` sidecar, or {}.

    A granule's /time never changes for a given CMR revision, so a
    rebuild only needs to read granules that are new or republished —
    the cache makes full rebuilds incremental.
    """
    import json

    uri = _cache_uri(s3_uri_or_path)
    try:
        if uri.startswith("s3://"):
            import boto3

            bucket, _, key = uri.removeprefix("s3://").partition("/")
            body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"]
            return dict(json.loads(body.read()))
        return dict(json.loads(Path(uri).read_text()))
    except Exception:
        return {}  # first run, missing sidecar, or unreadable: full sweep


def save_time_cache(s3_uri_or_path: str, cache: dict[str, dict]) -> None:
    import json

    uri = _cache_uri(s3_uri_or_path)
    data = json.dumps(cache).encode()
    if uri.startswith("s3://"):
        import boto3

        bucket, _, key = uri.removeprefix("s3://").partition("/")
        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=data)
    else:
        Path(uri).write_bytes(data)


def upload(path: Path, s3_uri: str) -> None:
    import boto3  # deferred so the pure helpers are testable offline

    bucket, _, key = s3_uri.removeprefix("s3://").partition("/")
    if not bucket or not key:
        raise InventoryError(f"--s3-uri must look like s3://bucket/key: {s3_uri}")
    boto3.client("s3").upload_file(str(path), bucket, key)


# -- profiling ---------------------------------------------------------------


def _rss_mb() -> float | None:
    """Resident set size, or None where /proc doesn't exist (macOS)."""
    try:
        with open("/proc/self/statm") as f:
            return int(f.read().split()[1]) * resource.getpagesize() / 1e6
    except OSError:
        return None


def _sample_memory(
    rows: list[tuple[float, float]], t0: float, stop: threading.Event
) -> None:
    while not stop.wait(5):
        rss = _rss_mb()
        if rss is not None:
            rows.append((round(time_module.monotonic() - t0, 1), round(rss, 1)))


def write_profile(
    out: Path,
    phases: list[tuple[str, float]],
    latencies: list[tuple[str, float]],
    memory: list[tuple[float, float]],
) -> None:
    """Write read_latencies.csv, memory_timeline.csv, and summary.txt
    (summary also to stderr, so it survives in the CodeBuild log even
    when the container's files don't)."""
    out.mkdir(parents=True, exist_ok=True)
    with (out / "read_latencies.csv").open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["granule", "seconds"])
        writer.writerows(latencies)
    with (out / "memory_timeline.csv").open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["elapsed_s", "rss_mb"])
        writer.writerows(memory)

    lines = ["== Phases =="]
    lines += [f"  {name:<20} {seconds:>10.1f} s" for name, seconds in phases]
    if latencies:
        values = sorted(seconds for _, seconds in latencies)
        big = len(values) >= 100
        q = statistics.quantiles(values, n=100) if big else []
        lines += [
            "",
            f"== Read latencies ({len(values)} granules, retries included) ==",
            f"  min/median/mean: {values[0]:.2f} / "
            f"{statistics.median(values):.2f} / {statistics.mean(values):.2f} s",
            f"  p90/p99/max:     {q[89] if big else values[-1]:.2f} / "
            f"{q[98] if big else values[-1]:.2f} / {values[-1]:.2f} s",
            "  slowest 5:",
        ]
        lines += [
            f"    {seconds:>8.2f} s  {name}"
            for name, seconds in sorted(latencies, key=lambda r: -r[1])[:5]
        ]
    # ru_maxrss is KB on Linux but bytes on macOS
    divisor = 1e3 if sys.platform == "linux" else 1e6
    peak_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / divisor
    lines += ["", "== Memory =="]
    if memory:
        lines += [
            f"  start/end sampled RSS: {memory[0][1]:.0f} / {memory[-1][1]:.0f} MB"
        ]
    lines += [f"  peak RSS:              {peak_mb:.0f} MB"]
    (out / "summary.txt").write_text("\n".join(lines) + "\n")
    print("\n".join(lines), file=sys.stderr)
    print(f"Profile written to {out}/", file=sys.stderr)


def _self_test() -> None:
    """Offline check of the profile-report path (no network, no creds)."""
    import tempfile

    with tempfile.TemporaryDirectory() as d:
        out = Path(d) / "profile"
        write_profile(
            out,
            phases=[("CMR search", 1.0), ("read+sort+validate", 2.0)],
            latencies=[(f"g{i}", 0.1 * i) for i in range(1, 201)],
            memory=[(0.0, 100.0), (5.0, 120.0)],
        )
        summary = (out / "summary.txt").read_text()
        assert "p90/p99/max" in summary and "peak RSS" in summary, summary
        assert len((out / "read_latencies.csv").read_text().splitlines()) == 201
    print("self-test ok", file=sys.stderr)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--access",
        choices=["direct", "external"],
        default="direct",
        help=(
            "Data link flavor: 'direct' for in-region s3:// keys (default; "
            "what the deployed backfill workers read), 'external' for "
            "EDL-authed HTTPS URLs"
        ),
    )
    parser.add_argument(
        "--read-access",
        choices=["direct", "external"],
        help=(
            "Link flavor used only to read /time headers (defaults to "
            "--access); lets CodeBuild record direct s3:// links while "
            "reading over HTTPS"
        ),
    )
    parser.add_argument("--start", help="Temporal window start (ISO date)")
    parser.add_argument("--end", help="Temporal window end (ISO date)")
    parser.add_argument(
        "--max-count",
        type=int,
        help="Keep only the N most recent granules (for test runs)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Concurrent per-granule header reads (keep low over HTTPS)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help=("Output path (default inventories/tempo-<collection>-inventory.json)"),
    )
    parser.add_argument(
        "--s3-uri",
        help="Also upload the inventory to this s3://bucket/key location",
    )
    parser.add_argument(
        "--collection",
        choices=["hcho", "no2"],
        default="hcho",
        help="TEMPO L3 collection to target (default hcho)",
    )
    parser.add_argument(
        "--concept-id",
        help="Explicit CMR collection concept ID (overrides --collection)",
    )
    parser.add_argument(
        "--profile-dir",
        default="profile-out",
        help="Directory for the run profile (latencies, RSS timeline, summary)",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Check the profile-reporting path offline and exit",
    )
    args = parser.parse_args()
    if args.self_test:
        _self_test()
        return 0
    concept_id = args.concept_id or load_collection(args.collection).concept_id
    output = Path(args.output or f"inventories/tempo-{args.collection}-inventory.json")

    # CMR search needs no auth; the per-granule reads need the token.
    if not os.environ.get("EARTHDATA_TOKEN"):
        raise InventoryError(
            "EARTHDATA_TOKEN is not set (CodeBuild wires it; needed for "
            "the s3credentials exchange and https reads)"
        )

    t0 = time_module.monotonic()
    phases: list[tuple[str, float]] = []
    latencies: list[tuple[str, float]] = []
    first_rss = _rss_mb()
    memory: list[tuple[float, float]] = (
        [(0.0, round(first_rss, 1))] if first_rss is not None else []
    )
    stop = threading.Event()
    threading.Thread(
        target=_sample_memory, args=(memory, t0, stop), daemon=True
    ).start()

    try:
        print(f"Searching CMR for all granules of {concept_id}...", file=sys.stderr)
        phase_start = time_module.monotonic()
        granules = search_granules(concept_id, args.start, args.end)
        phases.append(("CMR search", time_module.monotonic() - phase_start))
        print(f"  {len(granules)} granules returned", file=sys.stderr)
        if args.max_count:
            granules = sorted(
                granules,
                key=lambda g: str(
                    g["umm"]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"]
                ),
            )[-args.max_count :]

        if not granules:
            raise InventoryError("No granules matched the query")
        shortname = str(granules[0]["umm"]["CollectionReference"]["ShortName"])
        # UR -> CMR revision, for the sidecar cache written after upload.
        revisions = {
            str(g["umm"].get("GranuleUR", g["meta"]["concept-id"])): _revision(g)
            for g in granules
        }
        known_times = load_time_cache(args.s3_uri) if args.s3_uri else {}
        print(
            f"Reading exact /time from {len(granules)} granule headers "
            f"({args.workers} workers)...",
            file=sys.stderr,
        )
        phase_start = time_module.monotonic()
        inventory = build_inventory(
            granules,
            access=args.access,
            read_access=args.read_access,
            # The progress denominator counts all granules; cache hits
            # make it finish early — the "reused" line explains the gap.
            read_time=instrumented_reader(
                read_granule_time, len(granules), latencies
            ),
            collection_shortname=shortname,
            concept_id=concept_id,
            workers=args.workers,
            known_times=known_times,
        )
        phases.append(("read+sort+validate", time_module.monotonic() - phase_start))

        write_inventory(inventory, output)
        print(f"Wrote {len(inventory.granules)} granules to {output}", file=sys.stderr)
        print(f"  first: {inventory.granules[0].url}", file=sys.stderr)
        print(f"  last:  {inventory.granules[-1].url}", file=sys.stderr)

        if args.s3_uri:
            upload(output, args.s3_uri)
            print(f"Uploaded to {args.s3_uri}", file=sys.stderr)
            save_time_cache(
                args.s3_uri,
                {
                    e.granule_ur: {
                        "time": e.time,
                        "revision": revisions.get(e.granule_ur, 0),
                    }
                    for e in inventory.granules
                },
            )
            print(
                f"Start the run with: scripts/start_backfill.sh <name> {args.s3_uri}",
                file=sys.stderr,
            )
        return 0
    finally:
        # Written on failure too: a partial profile is exactly what you
        # want after an OOM kill's survivor run or a mid-sweep crash.
        stop.set()
        write_profile(Path(args.profile_dir), phases, latencies, memory)


if __name__ == "__main__":
    sys.exit(main())
