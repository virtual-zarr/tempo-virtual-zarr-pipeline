"""Tracked env files must not hold values for semi-sensitive keys.

Runs the same check as the env-hygiene pre-commit hook, plus a fixture
proving the hook actually rejects a violation.
"""

import subprocess
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "scripts" / "check_env_hygiene.sh"


def run_check(*files: Path) -> int:
    return subprocess.run(
        [str(SCRIPT), *map(str, files)], capture_output=True
    ).returncode


def test_tracked_env_files_are_clean() -> None:
    tracked = subprocess.run(
        ["git", "ls-files", ".env*"], cwd=REPO, capture_output=True, text=True
    ).stdout.split()
    assert tracked, "expected tracked .env* files"
    assert run_check(*(REPO / f for f in tracked)) == 0


def test_semi_sensitive_value_is_rejected(tmp_path: Path) -> None:
    bad = tmp_path / ".env_bad"
    bad.write_text("STACK_NAME=ok\nACCOUNT_ID=123456789012\n")
    assert run_check(bad) == 1


def test_blank_semi_sensitive_key_is_allowed(tmp_path: Path) -> None:
    sample = tmp_path / ".env.local.sample"
    sample.write_text("ACCOUNT_ID=\nEARTHDATA_SECRET_ARN=\n")
    assert run_check(sample) == 0
