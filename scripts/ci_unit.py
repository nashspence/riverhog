"""Exhaustive unit ownership from the canonical Make roots and actual pytest collection."""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from riverhog_canonical_json import canonical_json_bytes

SHARDS = ("release", "contract", "unit")
SHARD_PROFILES = {
    "release": (2, "loadscope"),
    "contract": (2, "worksteal"),
    "unit": (4, "loadscope"),
}


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


def partition(
    nodeids: Sequence[str], *, contract_modules: frozenset[str] = frozenset()
) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {name: [] for name in SHARDS}
    seen: set[str] = set()
    for nodeid in nodeids:
        if nodeid in seen:
            raise ValueError(f"duplicate canonical unit collection: {nodeid}")
        seen.add(nodeid)
        owned = owners(nodeid)
        if owned == ("unit",) and nodeid.partition("::")[0] in contract_modules:
            owned = ("contract",)
        if len(owned) != 1 or owned[0] not in result:
            raise ValueError(f"unit item must have exactly one shard owner: {nodeid}")
        result[owned[0]].append(nodeid)
    return result


class Collection:
    def __init__(self) -> None:
        self.nodeids: list[str] = []
        self.contract_modules: set[str] = set()

    def pytest_collection_finish(self, session: pytest.Session) -> None:
        self.nodeids = [item.nodeid for item in session.items]
        self.contract_modules = {
            item.nodeid.partition("::")[0]
            for item in session.items
            if "generated_contract_closure" in getattr(item, "fixturenames", ())
        }


def collect(roots: Sequence[str]) -> Collection:
    collected = Collection()
    diagnostics = io.StringIO()
    with contextlib.redirect_stdout(diagnostics), contextlib.redirect_stderr(diagnostics):
        status = pytest.main(["--collect-only", "-q", *roots], plugins=[collected])
    if status != 0:
        raise RuntimeError(diagnostics.getvalue())
    return collected


@pytest.hookimpl(tryfirst=True)
def pytest_collection_finish(session: pytest.Session) -> None:
    expected_path = os.environ.get("RIVERHOG_CI_UNIT_EXPECTED_FILE")
    if expected_path is None:
        return
    expected = json.loads(Path(expected_path).read_bytes())["nodeids"]
    observed = [item.nodeid for item in session.items]
    if (
        len(expected) != len(set(expected))
        or len(observed) != len(expected)
        or set(observed) != set(expected)
    ):
        missing = sorted(set(expected) - set(observed))
        extra = sorted(set(observed) - set(expected))
        session.config.hook.pytest_collectreport(
            report=pytest.CollectReport(
                nodeid="canonical shard membership",
                outcome="failed",
                longrepr=(
                    "shard collection differs from canonical ownership: "
                    f"expected={len(expected)}, observed={len(observed)}, "
                    f"missing={len(missing)} {missing[:5]}, extra={len(extra)} {extra[:5]}"
                ),
                result=[],
            )
        )
        session.items.clear()


class ExecutedShard:
    def __init__(self, expected: set[str]) -> None:
        self.expected = expected
        self.observed: set[str] = set()

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        if report.when == "call" or (report.when == "setup" and report.skipped):
            self.observed.add(report.nodeid)

    def pytest_sessionfinish(self, session: pytest.Session, exitstatus: int) -> None:
        if exitstatus == 0 and self.observed != self.expected:
            session.exitstatus = pytest.ExitCode.TESTS_FAILED
            terminal = session.config.pluginmanager.get_plugin("terminalreporter")
            if terminal is not None:
                terminal.write_line(
                    "shard execution differs from canonical ownership: "
                    f"expected={len(self.expected)}, observed={len(self.observed)}"
                )


def pytest_configure(config: pytest.Config) -> None:
    expected_path = os.environ.get("RIVERHOG_CI_UNIT_EXPECTED_FILE")
    if expected_path is not None and not hasattr(config, "workerinput"):
        expected = json.loads(Path(expected_path).read_bytes())["nodeids"]
        config.pluginmanager.register(ExecutedShard(set(expected)), "riverhog-executed-shard")


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
        collected = collect(args.roots)
        inventory = partition(
            collected.nodeids, contract_modules=frozenset(collected.contract_modules)
        )
        if any(not items for items in inventory.values()):
            raise ValueError("the complete canonical unit definition must populate every shard")
    except (RuntimeError, ValueError) as exc:
        parser.exit(1, f"unit ownership failed: {exc}\n")
    record = {
        "format": "riverhog-ci-unit-inventory/v1",
        "roots": args.roots,
        "shards": inventory,
        "contract_fixture_modules": sorted(collected.contract_modules),
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
    workers, distribution = SHARD_PROFILES[args.shard]
    with TemporaryDirectory(prefix="riverhog-unit-shard-") as temporary:
        expected_path = Path(temporary) / "expected.json"
        expected_path.write_bytes(canonical_json_bytes({"nodeids": inventory[args.shard]}))
        return subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "-q",
                "-p",
                "scripts.ci_unit",
                "-n",
                str(workers),
                f"--dist={distribution}",
                *pytest_args,
                *modules,
            ],
            env={
                **os.environ,
                "RIVERHOG_CI_LANE": f"unit-{args.shard}",
                "RIVERHOG_CI_UNIT_EXPECTED_FILE": str(expected_path),
            },
            check=False,
        ).returncode


if __name__ == "__main__":
    sys.exit(main())
