#!/usr/bin/env python3
"""One-time migration that makes a store's virtual references relative to its container.

Stores built before the container was named hold absolute references,
such as ``s3://asdc-prod-protected/TEMPO/...``. Icechunk resolves them
only through a container with that exact prefix, so the store can be read
only from us-west-2 over S3. The processor now writes
``vcc://asdc/TEMPO/...`` (see ``relative_location``). A reader resolves
that through the prefix of its own container named ``asdc``, so one store
serves S3 in us-west-2 and HTTPS everywhere else.

The script reads every reference back through VirtualiZarr's
``IcechunkParser``, rewrites the location, and writes it into the slot it
came from. Nothing is read from the source bucket, so the script runs from
anywhere with write access to the store. One commit holds the whole
rewrite. A stopped run leaves nothing behind. Run it again.

Icechunk stores a ``last_updated_at`` stamp with each reference but does
not read it back. Each slot is written with the stamp the store recorded
for it (see ``GranuleStamps``). The script refuses a store with unknown
stamps. Run ``record_stamps.py`` first.

Uses the same environment variables as the processor Lambdas:

    uv run --env-file .env_no2 --env-file .env.local scripts/relativize_refs.py
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime

from obspec_utils.registry import ObjectStoreRegistry
from virtualizarr.parsers import IcechunkParser
from virtualizarr_processor.manifest import MANIFEST_ARRAYS, STAMP_ARRAY, GranuleStamps
from virtualizarr_processor.processor import (
    Processor,
    relative_location,
    virtual_chunk_prefix,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--branch", default="main")
    args = parser.parse_args()

    processor = Processor()
    repo = processor.open_backfill_repo()
    session = repo.writable_session(args.branch)
    stamps = GranuleStamps.read(session.store) or []
    unknown = sum(1 for stamp in stamps if not stamp)
    if not stamps or unknown:
        print(
            f"FAIL: {unknown or 'all'} stamps unknown; run record_stamps.py first",
            file=sys.stderr,
        )
        return 1

    dim = processor.config.append_dim
    # The axes, the manifest and the stamps are native arrays. The parser
    # would hand them back as references to the store's own chunk files.
    skip = [dim, *processor.coordinates, *MANIFEST_ARRAYS, STAMP_ARRAY]
    vds = (
        IcechunkParser(skip_variables=skip)
        .parse_session(session, ObjectStoreRegistry(), native_chunks_prefix="native://")
        .to_virtual_dataset()
    )
    vds.attrs = {}
    total = vds.sizes[dim]
    if total != len(stamps):
        print(f"FAIL: {total} slots but {len(stamps)} stamps", file=sys.stderr)
        return 1
    prefix = virtual_chunk_prefix()
    print(f"{total} slots on {args.branch!r}", file=sys.stderr)

    for i, stamp in enumerate(stamps):
        region = slice(i, i + 1)
        vds.isel({dim: region}).vz.rename_paths(
            lambda url: relative_location(url, prefix)
        ).vz.to_icechunk(
            session.store,
            region={dim: region},
            validate_containers=False,
            last_updated_at=datetime.fromisoformat(stamp),
        )
    snapshot = session.commit(f"Relativize virtual refs in {total} slots")
    repo.save_config()
    print(
        f"OK: {total} slots relative in {snapshot}; container persisted",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
