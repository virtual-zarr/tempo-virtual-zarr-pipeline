#!/usr/bin/env python3
"""One-time migration: re-split an existing store's chunk manifests.

Stores backfilled before manifest splitting was configured hold one
whole-archive manifest per array, which every later commit rewrites in
memory — the failure that OOMed the forward consumer after the
2026-09-09 NO2 backfill promoted. This script opens the repo with the
processor's splitting config (500 append-dim slots per split, see
``Processor.open_backfill_repo``) and rewrites all manifests once, so
subsequent appends only rewrite the tail split.

Run it BEFORE deploying the consumer against a store whose backfill ran
without splitting: the first append against monolithic manifests
performs this same rewrite inside the 2 GB Lambda and dies. Run from
any machine with a few GB of free RAM and write access to the icechunk
bucket — only icechunk metadata is touched, never DAAC source objects,
so the us-west-2 region lock does not apply.

Rerunning is harmless but pointless: each run adds one commit rewriting
the already-split manifests.

Uses the same environment variables as the processor Lambdas:

    uv run --env-file .env_no2 --env-file .env.local scripts/rewrite_manifests.py
"""

from __future__ import annotations

import argparse

from virtualizarr_processor.processor import Processor


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--branch",
        default="main",
        help="branch whose tip gets the rewrite commit (default: main)",
    )
    args = parser.parse_args()

    repo = Processor().open_backfill_repo()
    snapshot = repo.rewrite_manifests(
        "Split chunk manifests along the append dimension",
        branch=args.branch,
    )
    print(f"Rewrote manifests on {args.branch!r}: new tip {snapshot}")


if __name__ == "__main__":
    main()
