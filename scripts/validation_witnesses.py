"""Bind executed owner-test witnesses to source, work and actual pytest outcomes."""

from __future__ import annotations

import ast
import hashlib
import inspect
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from riverhog_canonical_json import canonical_json_bytes, require_canonical_json


@dataclass(frozen=True)
class TestWitness:
    """An execution association; it does not create a normative growth declaration."""

    id: str
    node_id: str
    gates: tuple[str, ...]
    source_sha256: str
    operation: str | None = None


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
                    if ast.unparse(decorator) == "pytest.mark.growth_witness":
                        raise ValueError("growth witness requires one identity and gate binding")
                    if not isinstance(decorator, ast.Call):
                        continue
                    if ast.unparse(decorator.func) != "pytest.mark.growth_witness":
                        continue
                    if len(decorator.args) != 1 or len(decorator.keywords) not in (1, 2):
                        raise ValueError("growth witness requires one identity and gate binding")
                    identity = ast.literal_eval(decorator.args[0])
                    keywords = {kw.arg: ast.literal_eval(kw.value) for kw in decorator.keywords}
                    if set(keywords) not in ({"gates"}, {"gates", "operation"}):
                        raise ValueError("growth witness has unknown source binding fields")
                    gates = keywords["gates"]
                    operation = keywords.get("operation")
                    if (
                        not isinstance(identity, str)
                        or not identity
                        or not isinstance(gates, tuple)
                        or not gates
                        or (
                            operation is not None
                            and (not isinstance(operation, str) or not operation)
                        )
                        or any(not isinstance(gate, str) or not gate for gate in gates)
                    ):
                        raise ValueError("invalid source-owned growth witness association")
                    result.append(
                        TestWitness(
                            id=identity,
                            node_id=path.relative_to(root).as_posix() + "::" + node.name,
                            gates=gates,
                            source_sha256=hashlib.sha256(raw).hexdigest(),
                            operation=operation,
                        )
                    )
    if len({witness.id for witness in result}) != len(result):
        raise ValueError("growth witness execution identities must be unique")
    return tuple(result)


def operation_projection(
    subject: str, *, root: Path | None = None
) -> tuple[dict[str, object], dict[str, str]]:
    """Resolve the normative owner from trusted source, never from an artifact."""
    from riverhog_growth_contracts import growth_binding, project_bindings

    from scripts.growth_contract import resolve_operation

    binding = growth_binding(resolve_operation(subject))
    projection = project_bindings((binding,))
    pending, seen = [binding], set()
    while pending:
        current = pending.pop()
        if current.declaration.id in seen:
            continue
        seen.add(current.declaration.id)
        if root is not None and not Path(
            inspect.getfile(inspect.unwrap(current.operation))
        ).resolve().is_relative_to(root.resolve()):
            raise ValueError("growth operation source is outside the required checkout")
        pending.extend(growth_binding(operation) for _, operation in current.delegates)
    return projection, {
        "id": binding.declaration.id,
        "sha256": hashlib.sha256(canonical_json_bytes(binding.project())).hexdigest(),
        "projection_sha256": hashlib.sha256(canonical_json_bytes(projection)).hexdigest(),
    }


def validate_growth_execution(
    projection: Mapping[str, Any], evidence: object, *, source: Mapping[str, object]
) -> None:
    """Check a retained execution record against standalone normative data and source.

    The collecting gate verifies artifact custody and source-owned test identity;
    this reader preserves their exact association without importing source code.
    """
    from riverhog_growth_contracts import validate_execution_samples

    source_sha = source.get("source_sha")
    fields = {
        "format",
        "source",
        "source_end",
        "run_id",
        "session_id",
        "node_id",
        "owner_sha256",
        "declaration",
        "outcomes",
        "samples",
    }
    if not isinstance(evidence, Mapping) or set(evidence) != fields:
        raise ValueError("growth execution record has missing or unknown evidence fields")
    if (
        evidence["format"] != "riverhog-growth-execution/v1"
        or source.get("source_clean") is not True
        or not isinstance(source_sha, str)
        or not re.fullmatch(r"[0-9a-f]{40}", source_sha)
        or evidence["source"] != source
        or evidence["source_end"] != source
        or any(
            not isinstance(evidence[key], str) or not evidence[key]
            for key in ("run_id", "session_id", "node_id")
        )
        or not isinstance(evidence["owner_sha256"], str)
        or not re.fullmatch(r"[0-9a-f]{64}", evidence["owner_sha256"])
    ):
        raise ValueError("growth execution is not bound to the required clean source and owner")
    binding = evidence["declaration"]
    if not isinstance(binding, Mapping) or set(binding) != {"id", "sha256", "projection_sha256"}:
        raise ValueError("growth execution has no exact normative binding")
    declarations = projection["declarations"]
    identity = binding["id"]
    if not isinstance(identity, str) or identity not in declarations:
        raise ValueError("growth execution has no declared operation owner")
    selected, pending = {}, [identity]
    while pending:
        child = pending.pop()
        if child in selected:
            continue
        if child not in declarations:
            raise ValueError("growth execution has an unresolved delegate declaration")
        selected[child] = declarations[child]
        pending.extend(row["declaration"] for row in declarations[child]["delegates"].values())
    owned_projection = {**projection, "declarations": selected}
    if binding != {
        "id": identity,
        "sha256": hashlib.sha256(canonical_json_bytes(declarations[identity])).hexdigest(),
        "projection_sha256": hashlib.sha256(canonical_json_bytes(owned_projection)).hexdigest(),
    }:
        raise ValueError("growth execution normative declaration or reachable composition is stale")
    outcomes = evidence["outcomes"]
    if (
        not isinstance(outcomes, Mapping)
        or set(outcomes) != {"setup", "call", "teardown"}
        or any(
            not isinstance(row, Mapping)
            or set(row) != {"seconds", "outcome", "expected_failure"}
            or type(row["seconds"]) not in (int, float)
            or not math.isfinite(row["seconds"])
            or row["seconds"] < 0
            or row.get("outcome") != "passed"
            or row.get("expected_failure") is not False
            for row in outcomes.values()
        )
    ):
        raise ValueError("growth execution has incomplete or failed assertion phases")
    validate_execution_samples(owned_projection, identity, evidence["samples"])


def collect_test_witnesses(
    paths: Sequence[Path],
    *,
    root: Path,
    expected: Sequence[str],
    source_sha: str,
    run_id: str,
    evidence: list[dict[str, object]] | None = None,
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
            or row.get("format") != "riverhog-ci-pytest-timing/v3"
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
            required_binding: dict[str, object] = {
                "id": identity,
                "source_sha256": witness.source_sha256,
            }
            projection = None
            if witness.operation is not None:
                projection, declaration = operation_projection(witness.operation, root=root)
                required_binding["declaration"] = declaration
            if identity in observed or binding != required_binding:
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
            if projection is not None:
                samples = record.get("growth_samples")
                if not isinstance(samples, dict):
                    raise ValueError("missing declared growth execution samples")
                execution: dict[str, object] = {
                    "format": "riverhog-growth-execution/v1",
                    "source": {"source_sha": source_sha, "source_clean": True},
                    "source_end": record["source_end"],
                    "run_id": run_id,
                    "session_id": record["session_id"],
                    "node_id": witness.node_id,
                    "owner_sha256": witness.source_sha256,
                    "declaration": declaration,
                    "outcomes": report,
                    "samples": samples.get(witness.node_id),
                }
                validate_growth_execution(
                    projection, execution, source={"source_sha": source_sha, "source_clean": True}
                )
                if evidence is not None:
                    evidence.append(execution)
                observed[identity] = "pass"
                continue
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
