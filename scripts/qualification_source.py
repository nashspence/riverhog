"""Git checkout preconditions for operation qualification.

Commands must exclusively own their checkout while running. These boundary
checks detect dirty or mismatched source, not transient edits restored between
checks. Git-ignored caches and generated outputs do not dirty the checkout.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def checkout_state() -> dict[str, object]:
    def git(*args: str) -> str:
        return subprocess.run(
            ["git", *args],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    return {
        "head": git("rev-parse", "--verify", "HEAD"),
        "clean": not git("status", "--porcelain=v1", "--untracked-files=all"),
    }


def matches_source(state: object, source_sha: str) -> bool:
    return (
        isinstance(state, dict) and state.get("head") == source_sha and state.get("clean") is True
    )
