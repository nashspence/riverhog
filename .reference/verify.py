#!/usr/bin/env python3
"""Apply the payload patch in a new detached worktree and run focused source tests.

Dependencies must already be available in the selected interpreter. No package
installation, existing-worktree change, commit, push or provider access occurs.
The new worktree is retained for inspection, including after a failing test.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

TESTS = [
    "tests/unit/test_archive_payload_domains.py",
    ".reference/tests",
    "packages/riverhog-archive-contracts/tests",
    "packages/riverhog-canonical-json/tests",
    "tests/unit/test_pack_volume.py",
    "some-implementations/riverhog/recovery/tests",
    "some-implementations/riverhog/storage/filesystem/tests",
    "some-implementations/riverhog/storage/s3-support/tests",
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worktree", required=True, type=Path)
    parser.add_argument("--python", type=Path, default=Path(sys.executable))
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    destination = args.worktree.expanduser().absolute()
    python = args.python.expanduser().resolve(strict=True)
    if destination.exists() or destination.is_symlink():
        parser.error("worktree destination must not already exist")
    if destination == root or destination.is_relative_to(root):
        parser.error("worktree destination must be outside the reference checkout")
    if subprocess.check_output(["git", "status", "--porcelain"], cwd=root).strip():
        parser.error("commit or preserve local changes before verifying the reference")
    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    subprocess.run(
        ["git", "-c", "core.hooksPath=/dev/null", "worktree", "add", "--detach", str(destination), sha],
        cwd=root, check=True,
    )
    patch = destination / ".reference/patches/01-payload-read-domains.patch"
    subprocess.run(["git", "apply", "--check", str(patch)], cwd=destination, check=True)
    subprocess.run(["git", "apply", str(patch)], cwd=destination, check=True)
    roots = [str(destination)] + [
        str(path) for path in sorted(destination.rglob("src"))
        if path.is_dir() and ".venv" not in path.parts and ".git" not in path.parts
        and (path.parent / "pyproject.toml").is_file()
    ]
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(roots + [env.get("PYTHONPATH", "")])
    print(f"Reference: {sha}\nPrepared worktree: {destination}", flush=True)
    result = subprocess.run([str(python), "-m", "pytest", "-q", *TESTS], cwd=destination, env=env)
    print(f"Worktree retained for inspection: {destination}", flush=True)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
