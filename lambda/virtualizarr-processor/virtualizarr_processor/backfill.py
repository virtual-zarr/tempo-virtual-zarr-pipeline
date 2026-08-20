"""Generic Icechunk fork/merge/promote helpers for backfill.

These operations are identical for every VirtualizarrProcessor implementation,
so they live here rather than on the Protocol. The fork/merge mechanics are
covered by tests/backfill_mechanics against the pinned icechunk (>=2.1): a
fresh writable session can merge forks created by an earlier (now-discarded)
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
    target: str = "main",
    expected_target_tip: str,
) -> None:
    """Move `target` to the tip of `source`, compare-and-swap style.

    `expected_target_tip` is the `target` tip the `source` branch was
    created from (BranchInit.branched_from). If `target` has moved since,
    for example because the consumer committed an append mid-run, the reset
    raises instead of discarding that commit; the run is retried against
    the new tip.
    """
    repo.reset_branch(
        target, repo.lookup_branch(source), from_snapshot_id=expected_target_tip
    )
