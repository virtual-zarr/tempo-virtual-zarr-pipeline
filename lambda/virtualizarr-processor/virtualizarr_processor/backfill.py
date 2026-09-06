"""Generic Icechunk fork/merge/promote helpers for backfill.

These operations are processor-independent, so they live here rather than
on the Processor. The fork/merge mechanics are covered by
tests/backfill_mechanics against the pinned icechunk (>=2.1): a fresh
writable session can merge forks created by an earlier (now-discarded)
session and commit, as long as the fork's base is a committed branch-tip
snapshot.
"""

import pickle

from icechunk import Repository


def create_fork(repo: Repository, *, branch: str = "backfill") -> bytes:
    """Open a fresh writable session on `branch` and return a pickled fork.

    The session is clean (init already committed), so the fork's base is the
    committed branch-tip snapshot. The single returned artifact is distributed
    to all workers, which each call fork() to make their own child.
    """
    session = repo.writable_session(branch)
    return pickle.dumps(session.fork())


def merge_and_commit(
    repo: Repository,
    child_fork_bytes: list[bytes],
    *,
    branch: str = "backfill",
    message: str,
) -> str:
    """Open a fresh writable session, merge all child forks, and commit once.

    Returns the new tip snapshot id.
    """
    session = repo.writable_session(branch)
    forks = [pickle.loads(b) for b in child_fork_bytes]
    session.merge(*forks)
    return session.commit(message)


def promote(
    repo: Repository,
    *,
    source: str = "backfill",
    source_snapshot: str | None = None,
    target: str = "main",
    expected_target_tip: str,
) -> None:
    """Move `target` to the tip of `source`, compare-and-swap style.

    `expected_target_tip` is the `target` tip the `source` branch was
    created from (BranchInit.branched_from). If `target` has moved since,
    for example because the consumer committed an append mid-run, the reset
    raises instead of discarding that commit; the run is retried against
    the new tip.

    By default the promoted snapshot is looked up as `source`'s branch tip
    at call time — fine when nothing else can reset that branch mid-run.
    When a concurrent run of the same job could reset `source` between this
    run's own commit and its promote (the resort job, run twice), pass the
    exact `source_snapshot` this run committed instead: the CAS then either
    promotes this run's own commit or fails, never a stranger's.
    """
    promoted = (
        source_snapshot if source_snapshot is not None else repo.lookup_branch(source)
    )
    if repo.lookup_branch(target) == promoted:
        # Already promoted: a retried run (crash after a successful CAS,
        # Step Functions retry, manual re-invoke) converges instead of
        # failing the CAS because `target` has moved.
        return
    repo.reset_branch(target, promoted, from_snapshot_id=expected_target_tip)
