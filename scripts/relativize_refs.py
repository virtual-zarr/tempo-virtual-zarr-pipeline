#!/usr/bin/env python3
"""One-time migration that makes a store's virtual references relative to its container.

Stores built before the container was named hold absolute references,
such as ``s3://asdc-prod-protected/TEMPO/...``. Icechunk resolves them
only through a container with that exact prefix, so the store can be read
only from us-west-2 over S3. The processor now writes
``vcc://asdc/TEMPO/...`` (see ``relative_location``). A reader resolves
that through the prefix of its own container named ``asdc``, so one store
serves S3 in us-west-2 and HTTPS everywhere else.

Icechunk has no API to read a reference back and rewrite it. So this
script parses every granule in the store manifest from source again, as
a backfill worker does, and writes the references into the slot the
granule already has. It goes through the slots in manifest order, in
batches. Each batch is one commit on the branch. A rerun continues after
the last batch named in the commit message at the branch tip. Rewriting
a slot a second time is harmless. At the end the script saves the named
container into the store's config.

The source reads need the DAAC's temporary credentials, which only work
in us-west-2. Run the script in the stack's CodeBuild project with
``scripts/run_codebuild.sh -R``, with forward processing paused. See
docs/reference/runbook-relativize-virtual-refs.md.

Uses the same environment variables as the processor Lambdas:

    uv run --env-file .env_no2 --env-file .env.local scripts/relativize_refs.py
"""

from __future__ import annotations

import argparse
import multiprocessing
import os
import pickle
import re
import sys
from concurrent.futures import ProcessPoolExecutor
from functools import lru_cache
from itertools import repeat

import icechunk
from virtualizarr_processor import backfill
from virtualizarr_processor.manifest import StoreManifest
from virtualizarr_processor.processor import Processor

MESSAGE = "Relativize virtual refs in slots [{start}, {stop}) of {total}"
MESSAGE_PATTERN = re.compile(
    r"Relativize virtual refs in slots \[\d+, (\d+)\) of (\d+)$"
)


@lru_cache(maxsize=1)
def _processor() -> Processor:
    """One Processor per worker process. It loads the template once."""
    return Processor()


def rewrite_granule(shared: bytes, url: str) -> bytes:
    """Parse ``url`` again and write its references into a child of ``shared``."""
    child = pickle.loads(shared).fork()
    if not _processor().process_backfill_file(url, child):
        raise RuntimeError(f"rewrite failed for {url}; see the logged exception")
    return pickle.dumps(child)


def resume_point(repo: icechunk.Repository, branch: str, total: int) -> int:
    """Return the slot after the last batch committed at the tip, or 0.

    Only the tip commit is read. With forward processing paused, every
    commit after the first batch comes from this script. If the tip commit
    came from something else, for example a resumed consumer or a re-sort
    that moved slots, the script starts over. That only costs time.
    """
    tip = next(repo.ancestry(branch=branch))
    match = MESSAGE_PATTERN.search(tip.message)
    if match and int(match.group(2)) == total:
        return int(match.group(1))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--branch", default="main")
    parser.add_argument(
        "--batch", type=int, default=100, help="slots per commit (default: 100)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=os.cpu_count(),
        help="granules parsed in parallel (default: the CPU count)",
    )
    args = parser.parse_args()

    repo = Processor().open_backfill_repo()
    manifest = StoreManifest.read(repo.readonly_session(args.branch).store)
    if manifest is None:
        print("FAIL: store carries no manifest", file=sys.stderr)
        return 1
    granules = manifest.granules
    total = len(granules)
    start = resume_point(repo, args.branch, total)
    print(f"{total} slots on {args.branch!r}; starting at {start}", file=sys.stderr)

    # Use spawn, not fork. The parent holds icechunk's async runtime. A
    # forked child would get a copy of it without its threads.
    context = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(args.workers, mp_context=context) as pool:
        for batch_start in range(start, total, args.batch):
            batch = granules[batch_start : batch_start + args.batch]
            stop = batch_start + len(batch)
            shared = backfill.create_fork(repo, branch=args.branch)
            children = pool.map(rewrite_granule, repeat(shared), [e.url for e in batch])
            snapshot = backfill.merge_and_commit(
                repo,
                children,
                branch=args.branch,
                message=MESSAGE.format(start=batch_start, stop=stop, total=total),
            )
            print(
                f"committed slots [{batch_start}, {stop}): {snapshot}", file=sys.stderr
            )

    repo.save_config()
    print(f"OK: {total} slots relative; named container persisted", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
