#!/usr/bin/env python3
"""Record the ``last_updated_at`` stamp of every slot the store does not know.

The backfill writes references with a stamp, the source's last-modified
time plus one second, but does not record it in the store's stamp array
(see ``GranuleStamps``). The consumer records stamps for the slots it
writes. So after a backfill most stamps are unknown, and the consumer's
unchanged-redelivery fast path stays off for those slots.

Icechunk does not read a stamp back. This script asks the source for it
again: one HEAD request per unknown slot, no parse. That reproduces the
original stamp exactly if the source has not changed since the slot was
written. A source whose last-modified time is later than the commit that
wrote the slot has changed since. Its references are stale. The script
leaves that stamp unknown, names the granule, and exits 1. Redeliver the
granule, then run the script again.

Every other unknown stamp goes into the stamp array in one commit. The
relative-reference migration (``relativize_refs.py``) needs every stamp
known, so run this first.

The HEAD requests need the DAAC's temporary credentials, which work only
in us-west-2. Run the script in the stack's CodeBuild project with
``scripts/run_codebuild.sh -R``. Uses the same environment variables as
the processor Lambdas:

    uv run --env-file .env_no2 --env-file .env.local scripts/record_stamps.py
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta

import icechunk
from virtualizarr_processor.granule import make_registry, source_last_modified
from virtualizarr_processor.manifest import (
    MANIFEST_ARRAYS,
    STAMP_ARRAY,
    GranuleStamps,
    StoreManifest,
    stamp_value,
)
from virtualizarr_processor.processor import Processor


def slot_write_times(
    repo: icechunk.Repository, branch: str, slots: set[int], skip: set[str]
) -> dict[int, datetime]:
    """The time of the last commit that wrote references into each slot.

    Walks the branch from the tip, one diff per commit, until every slot
    in ``slots`` has a time. Arrays in ``skip`` are bookkeeping along the
    same axis, not references. The append dimension is the first axis of
    every reference array.
    """
    times: dict[int, datetime] = {}
    commits = list(repo.ancestry(branch=branch))
    for newer, older in zip(commits, commits[1:]):
        if slots <= times.keys():
            break
        diff = repo.diff(from_snapshot_id=older.id, to_snapshot_id=newer.id)
        for path, coords in diff.updated_chunks.items():
            if path.lstrip("/") in skip:
                continue
            for coord in coords:
                times.setdefault(coord[0], newer.written_at)
    return times


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--branch", default="main")
    args = parser.parse_args()

    processor = Processor()
    repo = processor.open_backfill_repo()
    session = repo.writable_session(args.branch)
    manifest = StoreManifest.read(session.store)
    stamps = GranuleStamps.read(session.store)
    if manifest is None or stamps is None:
        print("FAIL: store carries no manifest or no stamp array", file=sys.stderr)
        return 1
    unknown = [i for i, stamp in enumerate(stamps) if not stamp]
    print(f"{len(unknown)} of {len(stamps)} stamps unknown", file=sys.stderr)
    if not unknown:
        print("OK: nothing to record", file=sys.stderr)
        return 0

    skip = {
        processor.config.append_dim,
        *processor.coordinates,
        *MANIFEST_ARRAYS,
        STAMP_ARRAY,
    }
    written = slot_write_times(repo, args.branch, set(unknown), skip)
    registry = make_registry(manifest.granules[unknown[0]].url)
    changed = []
    for i in unknown:
        entry = manifest.granules[i]
        modified = source_last_modified(entry.url, registry)
        if i not in written or modified > written[i]:
            changed.append(entry.granule_ur)
            continue
        stamps[i] = stamp_value(modified + timedelta(seconds=1))
    recorded = len(unknown) - len(changed)
    if recorded:
        GranuleStamps.write(session.store, stamps)
        snapshot = session.commit(f"Record stamps for {recorded} slots")
        print(f"recorded {recorded} stamps in {snapshot}", file=sys.stderr)
    if changed:
        print(
            f"FAIL: {len(changed)} sources changed after their slot was written; "
            f"redeliver them: {' '.join(changed)}",
            file=sys.stderr,
        )
        return 1
    print("OK: every stamp known", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
