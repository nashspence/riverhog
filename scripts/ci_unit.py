"""Exhaustive unit ownership from the canonical Make roots and actual pytest collection."""

from __future__ import annotations

import argparse
import contextlib
import io
import os
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

import pytest
from riverhog_canonical_json import canonical_json_bytes

SHARDS = ("release", "contract", "unit")


def owners(nodeid: str) -> tuple[str, ...]:
    module = nodeid.partition("::")[0]
    if module.startswith("tests/unit/test_release") or module in {
        "tests/unit/test_operation_qualification.py",
        "tests/unit/test_installation_publication.py",
        "tests/unit/test_workflow_evidence.py",
        "tests/unit/test_qualification_source.py",
    }:
        return ("release",)
    if module.startswith(("tests/unit/test_contract", "tests/unit/test_documentation")):
        return ("contract",)
    return ("unit",)


def partition(nodeids: Sequence[str]) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {name: [] for name in SHARDS}
    seen: set[str] = set()
    for nodeid in nodeids:
        if nodeid in seen:
            raise ValueError(f"duplicate canonical unit collection: {nodeid}")
        seen.add(nodeid)
        owned = owners(nodeid)
        if len(owned) != 1 or owned[0] not in result:
            raise ValueError(f"unit item must have exactly one shard owner: {nodeid}")
        result[owned[0]].append(nodeid)
    return result


class Collection:
    def __init__(self) -> None:
        self.nodeids: list[str] = []

    def pytest_collection_finish(self, session: pytest.Session) -> None:
        self.nodeids = [item.nodeid for item in session.items]


def collect(roots: Sequence[str]) -> list[str]:
    collected = Collection()
    diagnostics = io.StringIO()
    with contextlib.redirect_stdout(diagnostics), contextlib.redirect_stderr(diagnostics):
        status = pytest.main(["--collect-only", "-q", *roots], plugins=[collected])
    if status != 0:
        raise RuntimeError(diagnostics.getvalue())
    return collected.nodeids


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("check", "run", "inventory"))
    parser.add_argument("--shard", choices=SHARDS)
    parser.add_argument("--output", type=Path)
    parser.add_argument("roots", nargs="+")
    raw = list(sys.argv[1:] if argv is None else argv)
    separator = raw.index("--") if "--" in raw else len(raw)
    args = parser.parse_args(raw[:separator])
    pytest_args = raw[separator + 1 :]
    try:
        inventory = partition(collect(args.roots))
        if any(not items for items in inventory.values()):
            raise ValueError("the complete canonical unit definition must populate every shard")
    except (RuntimeError, ValueError) as exc:
        parser.exit(1, f"unit ownership failed: {exc}\n")
    record = {
        "format": "riverhog-ci-unit-inventory/v1",
        "roots": args.roots,
        "shards": inventory,
    }
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_bytes(canonical_json_bytes(record))
    if args.command == "inventory":
        print(canonical_json_bytes(record).decode())
    else:
        print(
            "Canonical unit ownership: " + ", ".join(f"{k}={len(v)}" for k, v in inventory.items())
        )
    if args.command != "run":
        return 0
    if args.shard is None:
        parser.error("run requires --shard")
    modules = sorted({nodeid.partition("::")[0] for nodeid in inventory[args.shard]})
    return subprocess.run(
        [sys.executable, "-m", "pytest", "-q", *pytest_args, *modules],
        env={**os.environ, "RIVERHOG_CI_LANE": f"unit-{args.shard}"},
        check=False,
    ).returncode


if __name__ == "__main__":
    sys.exit(main())
