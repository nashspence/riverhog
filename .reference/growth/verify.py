#!/usr/bin/env python3
"""Replay #958 growth reference patches on the exact audited main checkpoint.

Requires preinstalled dependencies and local Git objects. Creates only a new
worktree and temporary test/evidence state. Does not install packages, access
providers, change existing worktrees, commit, push, or apply policy patch 02.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

BASE = "e0036f80410a83cc5eaa9d63c17650ffae72e2bd"
PATCHES = (
    "patches/01-payload-read-domains.patch",
    "growth/03-recovery-growth.patch",
    "growth/04-materialization-cleanup.patch",
)
TESTS = (
    "tests/unit/test_archive_payload_domains.py",
    ".reference/tests",
    "packages/riverhog-archive-contracts/tests",
    "packages/riverhog-canonical-json/tests",
    "tests/unit/test_growth_declarations.py",
    "tests/unit/test_private_growth_discovery.py",
    "tests/unit/test_pack_volume.py",
    "tests/unit/test_provenance_archive_read.py",
    "some-implementations/riverhog/recovery/tests",
    "some-implementations/riverhog/storage/filesystem/tests",
    "some-implementations/riverhog/storage/s3-support/tests",
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worktree", required=True, type=Path)
    parser.add_argument("--python", type=Path, default=Path(sys.executable))
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    reference = root / ".reference"
    destination = args.worktree.expanduser().absolute()
    python = args.python.expanduser().resolve(strict=True)
    if destination.exists() or destination.is_symlink():
        parser.error("destination must not exist")
    if destination == root or destination.is_relative_to(root):
        parser.error("destination must be outside the reference checkout")
    if subprocess.check_output(["git", "status", "--porcelain"], cwd=root).strip():
        parser.error("reference checkout must be clean before replay")
    manifest = json.loads((reference / "MANIFEST.json").read_text())
    for name, expected in manifest["sha256"].items():
        path = reference / name
        if not path.is_relative_to(reference) or ".." in Path(name).parts:
            parser.error("unsafe reference manifest path")
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            parser.error(f"reference artifact changed: {name}")
    subprocess.run(["git", "cat-file", "-e", BASE + "^{commit}"], cwd=root, check=True)
    subprocess.run(
        ["git", "-c", "core.hooksPath=/dev/null", "worktree", "add", "--detach",
         str(destination), BASE], cwd=root, check=True,
    )
    for name in PATCHES:
        path = reference / name
        subprocess.run(["git", "apply", "--check", str(path)], cwd=destination, check=True)
        subprocess.run(["git", "apply", str(path)], cwd=destination, check=True)
    # The old exact captures are consumed as reader oracles, not regenerated.
    for name in ("fixtures", "tests"):
        shutil.copytree(reference / name, destination / ".reference" / name,
                        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    roots = [str(destination)] + [
        str(path) for path in sorted(destination.rglob("src"))
        if path.is_dir() and (path.parent / "pyproject.toml").is_file()
        and ".git" not in path.parts and ".venv" not in path.parts
    ]
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(roots + [environment.get("PYTHONPATH", "")])
    print(f"Audited base: {BASE}\nPrepared worktree: {destination}", flush=True)
    completed = subprocess.run(
        [str(python), "-m", "pytest", "-q", *TESTS],
        cwd=destination, env=environment,
    )
    print(f"Worktree retained: {destination}", flush=True)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
