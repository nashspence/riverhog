"""Bind executed owner-test witnesses to source, work and actual pytest outcomes."""

from __future__ import annotations

import ast
import hashlib
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from riverhog_canonical_json import require_canonical_json


@dataclass(frozen=True)
class TestWitness:
    """An execution association; it does not create a normative growth declaration."""

    id: str
    node_id: str
    gates: tuple[str, ...]
    source_sha256: str


def discover_test_witnesses(root: Path) -> tuple[TestWitness, ...]:
    result: list[TestWitness] = []
    for directory in ("packages", "some-implementations", "riverhog", "tests/unit"):
        for path in sorted((root / directory).rglob("test_*.py")):
            raw = path.read_bytes()
            if b"growth_witness" not in raw:
                continue
            tree = ast.parse(raw, filename=str(path))
            for node in tree.body:
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                for decorator in node.decorator_list:
                    if not isinstance(decorator, ast.Call):
                        continue
                    if ast.unparse(decorator.func) != "pytest.mark.growth_witness":
                        continue
                    if len(decorator.args) != 1 or len(decorator.keywords) != 1:
                        raise ValueError("growth witness requires one identity and gate binding")
                    identity = ast.literal_eval(decorator.args[0])
                    keyword = decorator.keywords[0]
                    gates = ast.literal_eval(keyword.value)
                    if (
                        not isinstance(identity, str)
                        or not identity
                        or keyword.arg != "gates"
                        or not isinstance(gates, tuple)
                        or not gates
                        or any(not isinstance(gate, str) or not gate for gate in gates)
                    ):
                        raise ValueError("invalid source-owned growth witness association")
                    result.append(
                        TestWitness(
                            id=identity,
                            node_id=path.relative_to(root).as_posix() + "::" + node.name,
                            gates=gates,
                            source_sha256=hashlib.sha256(raw).hexdigest(),
                        )
                    )
    if len({witness.id for witness in result}) != len(result):
        raise ValueError("growth witness execution identities must be unique")
    return tuple(result)


def collect_test_witnesses(
    paths: Sequence[Path], *, root: Path, expected: Sequence[str], source_sha: str, run_id: str
) -> dict[str, str]:
    """Verify actual assertion execution and source-bound primitive observations.

    Test results support their explicitly owned test scope. Product conformance
    still requires an independently declared obligation and its Audit binding.
    """
    declarations = {witness.id: witness for witness in discover_test_witnesses(root)}
    if set(expected) - declarations.keys():
        raise ValueError("required growth witness has no source owner")
    records: list[dict[str, Any]] = []
    for path in paths:
        row = require_canonical_json(path.read_bytes())
        if (
            not isinstance(row, dict)
            or row.get("format") != "riverhog-ci-pytest-timing/v2"
            or row.get("run_id") != run_id
        ):
            raise ValueError("invalid or stale growth execution artifact")
        records.append(row)
    sessions: dict[str, list[dict[str, Any]]] = {}
    for row in records:
        identity = row.get("session_id")
        if not isinstance(identity, str) or not identity:
            raise ValueError("missing pytest session identity")
        sessions.setdefault(identity, []).append(row)
    selected = []
    for rows in sessions.values():
        workers = [row for row in rows if row.get("worker") != "controller"]
        selected.extend(workers or rows)
    observed: dict[str, str] = {}
    for record in selected:
        if record.get("source_sha") != source_sha or record.get("source_clean") is not True:
            raise ValueError("growth artifact is not bound to the required clean source")
        if record.get("source_end") != {"source_sha": source_sha, "source_clean": True}:
            raise ValueError("growth artifact source changed during execution")
        bindings, observations, tests = (
            record.get("growth_witnesses"),
            record.get("growth_observations"),
            record.get("tests"),
        )
        if not all(isinstance(value, dict) for value in (bindings, observations, tests)):
            raise ValueError("missing executed growth witness data")
        assert (
            isinstance(bindings, dict)
            and isinstance(observations, dict)
            and isinstance(tests, dict)
        )
        for identity in expected:
            witness = declarations[identity]
            binding = bindings.get(witness.node_id)
            report = tests.get(witness.node_id)
            if report is None:
                continue
            if identity in observed or binding != {
                "id": identity,
                "source_sha256": witness.source_sha256,
            }:
                raise ValueError("duplicate, stale or mismatched owner growth witness")
            if not isinstance(report, dict):
                raise ValueError("invalid or expected-failure witness assertion results")
            phases: dict[str, object] = {}
            for phase, value in report.items():
                if not isinstance(value, dict) or value.get("expected_failure") is not False:
                    raise ValueError("invalid or expected-failure witness assertion results")
                phases[phase] = value.get("outcome")
            if phases != {"setup": "passed", "call": "passed", "teardown": "passed"}:
                raise ValueError("required growth witness did not completely execute")
            metrics = observations.get(witness.node_id)
            if (
                not isinstance(metrics, dict)
                or not metrics
                or any(
                    not isinstance(key, str) or type(value) is not int or value < 0
                    for key, value in metrics.items()
                )
            ):
                raise ValueError("missing or malformed actual growth observations")
            observed[identity] = "pass"
    if set(observed) != set(expected):
        raise ValueError("missing required owner growth witness execution")
    return observed
