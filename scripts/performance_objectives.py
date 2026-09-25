#!/usr/bin/env python3
"""Noncontractual performance accounting and comparable measured goodput samples."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import math
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "qualification/performance/README.md"
SAMPLE_FORMAT = "riverhog-performance-sample/v1"
CONTEXT_FORMAT = "riverhog-performance-comparison-context/v1"
GOODPUT_FRACTION = 0.90
NETWORK_SCENARIOS = frozenset(
    {
        "riverhog-ingress",
        "stove0-derived-publication",
        "stove0-input-read",
        "riverhog-retrieval",
        "archive-upload",
        "archive-retrieval",
        "archive-replication",
    }
)
WORKLOADS = frozenset({"large-file", "many-small-files", "resume"})
_ID = re.compile(r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_BOUNDARY = re.compile(r"[a-z][a-z0-9-]{0,95}\Z")


class PerformanceError(ValueError):
    """A performance definition or measurement cannot support its claimed meaning."""


@dataclass(frozen=True)
class Objective:
    id: str
    metric: str
    unit: str
    workload: str
    useful_work_numerator: str
    timed_interval: str
    reference_measurement: str
    comparability: str
    measurement_source: str
    enforcement_owner: str
    disposition: str
    minimum_reference_fraction: float


@dataclass(frozen=True)
class AccountingNote:
    id: str
    meaning: str
    source: str


# One reviewed source of report-only candidates. Their presence creates no release gate.
OBJECTIVES = (
    Objective(
        id="transfer-goodput",
        metric="newly completed useful payload per end-to-end wall time",
        unit="bytes/second",
        workload="Named network scenario and exact declared workload fingerprint",
        useful_work_numerator=(
            "Verified newly completed logical payload bytes; exclude resume work already "
            "complete and retransmitted bytes"
        ),
        timed_interval="Command start through verified completion, including setup and teardown",
        reference_measurement="Another measured, completed run of the same operation path",
        comparability=(
            "Same scenario, workload, byte count/domain, completion boundary, path, "
            "environment, cache state, concurrency, and comparison session"
        ),
        measurement_source="scripts/transfer_profile.py::main",
        enforcement_owner="scripts/transfer_profile.py::main",
        disposition="report-only candidate",
        minimum_reference_fraction=GOODPUT_FRACTION,
    ),
    Objective(
        id="storage-upload-goodput",
        metric="completed stored-object payload per upload wall time",
        unit="bytes/second",
        workload="One synthetic object through a sequential public storage-adapter path",
        useful_work_numerator="Exact completed stored-object payload bytes",
        timed_interval="Begin-write request through validated complete-write response",
        reference_measurement="Another measured upload with matching declared context",
        comparability=(
            "Same payload size, segment maximum, path, environment, cache state, "
            "concurrency, and comparison session; compare upload with upload only"
        ),
        measurement_source="tests/harness/storage_adapter_goodput_probe.py::run",
        enforcement_owner="tests/harness/storage_adapter_goodput_probe.py::run",
        disposition="report-only candidate",
        minimum_reference_fraction=GOODPUT_FRACTION,
    ),
    Objective(
        id="storage-read-goodput",
        metric="verified stored-object payload per read wall time",
        unit="bytes/second",
        workload="Read after writing one synthetic object through the public storage adapter",
        useful_work_numerator="Exact read and hash-verified stored-object payload bytes",
        timed_interval="Open exact revision through complete read, hash, and stream close",
        reference_measurement="Another measured read with matching declared context",
        comparability=(
            "Same payload size, segment maximum, path, environment, cache state, "
            "concurrency, and comparison session; compare read with read only"
        ),
        measurement_source="tests/harness/storage_adapter_goodput_probe.py::run",
        enforcement_owner="tests/harness/storage_adapter_goodput_probe.py::run",
        disposition="report-only candidate",
        minimum_reference_fraction=GOODPUT_FRACTION,
    ),
)

OBSERVATIONS = (
    AccountingNote(
        "recovery-transfer-rate",
        "Recovery command bytes, items, and elapsed time have no accepted numerical target.",
        "scripts/transfer_profile.py::main",
    ),
    AccountingNote(
        "operation-cold-cli-startup",
        "Cold CLI startup samples have no accepted latency target.",
        "scripts/operation_qualification.py::_cold_cli_timings",
    ),
    AccountingNote(
        "operation-local-api-client-wall",
        "Local API and client timing samples have no accepted per-operation speed target.",
        "scripts/operation_qualification.py::_load_operation_timings",
    ),
    AccountingNote(
        "provider-qualification-elapsed",
        "Provider qualification start, phase, and completion timestamps are observations.",
        "scripts/provider_qualification.py::evidence_from_checkpoint",
    ),
    AccountingNote(
        "stove0-scale",
        "Scale-smoke elapsed time, CPU, memory, and size observations have no rate target.",
        "scripts/test_compose_smoke.sh",
    ),
    AccountingNote(
        "transfer-phase-telemetry",
        "Segment and phase totals are diagnostics, not unique end-to-end goodput.",
        "riverhog/src/riverhog_core/throughput.py",
    ),
)

INDEPENDENT_BOUNDS = (
    AccountingNote(
        "database-regression-guards",
        "Fixture-local memory, plan-work, and latency envelopes remain enforced by "
        "database qualification.",
        "scripts/database_qualification.py",
    ),
    AccountingNote(
        "gogurt-listener-regression-guards",
        "Fixture-local traced-allocation ceilings remain with the listener tests.",
        "some-implementations/gogurt/application/tests/test_listener.py",
    ),
    AccountingNote(
        "extent-and-correctness-bounds",
        "Contract extents and correctness/resource checks retain their independent authorities.",
        "scripts/extent_contract.py",
    ),
)


def _source_exists(root: Path, reference: str) -> bool:
    path_text, separator, symbol = reference.partition("::")
    relative = Path(path_text)
    if relative.is_absolute() or ".." in relative.parts or relative.as_posix() != path_text:
        return False
    path = root / relative
    if not path.is_file() or path.is_symlink():
        return False
    if not separator:
        return True
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (OSError, UnicodeError, SyntaxError):
        return False
    return (
        sum(
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == symbol
            for node in tree.body
        )
        == 1
    )


def validate_registry(root: Path = ROOT) -> None:
    records = (*OBJECTIVES, *OBSERVATIONS, *INDEPENDENT_BOUNDS)
    identities = [item.id for item in records]
    if len(identities) != len(set(identities)) or any(not _ID.fullmatch(x) for x in identities):
        raise PerformanceError("performance accounting IDs must be unique and stable")
    for item in OBJECTIVES:
        fields = asdict(item)
        if any(not value for key, value in fields.items() if key != "minimum_reference_fraction"):
            raise PerformanceError(f"incomplete performance objective: {item.id}")
        if item.disposition != "report-only candidate":
            raise PerformanceError(f"objective cannot create an enforcement decision: {item.id}")
        if not math.isfinite(item.minimum_reference_fraction) or not (
            0 < item.minimum_reference_fraction <= 1
        ):
            raise PerformanceError(f"invalid objective fraction: {item.id}")
        if not _source_exists(root, item.measurement_source) or not _source_exists(
            root, item.enforcement_owner
        ):
            raise PerformanceError(f"stale objective executable source: {item.id}")
    for note in (*OBSERVATIONS, *INDEPENDENT_BOUNDS):
        if not note.meaning or not _source_exists(root, note.source):
            raise PerformanceError(f"stale accounting source: {note.id}")


def definition_sha256() -> str:
    payload = json.dumps(
        [asdict(item) for item in OBJECTIVES], sort_keys=True, separators=(",", ":")
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def render() -> str:
    validate_registry()
    lines = [
        "# Performance accounting — noncontractual",
        "",
        "Generated from [performance_objectives.py](../../scripts/performance_objectives.py); "
        "edit that source, not this page.",
        "",
        "These engineering objectives and observations do not create, extend, interpret, "
        "override, or relax an external contract. A recorded target does not create a "
        "release gate. No result or passing run is claimed here.",
        "",
        "A goodput comparison uses newly completed useful bytes divided by its complete "
        "operation interval. A reference is another measured sample on the same declared "
        "path and workload; nominal link capacity and raw transport rate are not references. "
        "The checker can compare declarations, but cannot independently attest that an "
        "operator's context fingerprints describe physical conditions truthfully. Missing "
        "or incompatible evidence is not evaluated, never passed. Absolute rates accompany "
        "every ratio. The 0.90 fraction remains a report-only candidate pending workload, "
        "baseline, and release interpretation.",
        "",
        "Comparison context JSON has `format = riverhog-performance-comparison-context/v1`, "
        "a shared UUID `comparison_id`, SHA-256 fingerprints for `workload_sha256`, "
        "`environment_sha256`, and `path_sha256`, plus `byte_domain`, "
        "`completion_boundary`, `cache_state`, and positive `concurrency`. These are "
        "declarations to review, not independently verified physical facts. The reference "
        "is a prior v2 result from the same producer and comparison session.",
        "",
        "The transfer profiler passes `RIVERHOG_PERFORMANCE_RUN_ID` and "
        "`RIVERHOG_PERFORMANCE_RECEIPT` to its command. A command that verifies actual "
        "completion may write a JSON receipt with `format = riverhog-performance-completion/v1`, "
        "that exact `run_id`, exact `completed_bytes` and `completed_items`, and "
        "`verified = true`. Exit success alone leaves completion unverified and cannot "
        "meet an objective. The storage probe verifies the exact readback itself.",
        "",
        "## Report-only candidate objectives",
        "",
    ]
    for item in OBJECTIVES:
        path = item.measurement_source.partition("::")[0]
        lines.extend(
            [
                f"### {item.id}",
                "",
                f"Metric: {item.metric} (`{item.unit}`).",
                "",
                f"Workload: {item.workload}.",
                "",
                f"Useful-work numerator: {item.useful_work_numerator}.",
                "",
                f"Timed interval: {item.timed_interval}.",
                "",
                f"Reference: {item.reference_measurement}.",
                "",
                f"Comparable when: {item.comparability}.",
                "",
                f"Candidate ratio: `candidate/reference >= {item.minimum_reference_fraction:.2f}`. "
                f"Enforcement: {item.disposition}, owned by `{item.enforcement_owner}`.",
                "",
                f"Executable source: [`{item.measurement_source}`](../../{path}).",
                "",
            ]
        )
    for heading, records in (
        ("Observations without objectives", OBSERVATIONS),
        ("Independently owned bounds and guards", INDEPENDENT_BOUNDS),
    ):
        lines.extend([f"## {heading}", ""])
        for note in records:
            path = note.source.partition("::")[0]
            lines.extend(
                [f"- **{note.id}**: {note.meaning} [Source: `{note.source}`](../../{path})."]
            )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _positive_number(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PerformanceError(f"{label} must be a positive finite number")
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise PerformanceError(f"{label} must be a positive finite number")
    return result


def _positive_int(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise PerformanceError(f"{label} must be a positive integer")
    return value


def _uuid(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise PerformanceError(f"{label} must be a UUID")
    try:
        parsed = UUID(value)
    except ValueError as exc:
        raise PerformanceError(f"{label} must be a UUID") from exc
    if str(parsed) != value:
        raise PerformanceError(f"{label} must be a canonical UUID")
    return value


_CONTEXT_FIELDS = {
    "format",
    "comparison_id",
    "workload_sha256",
    "environment_sha256",
    "path_sha256",
    "byte_domain",
    "completion_boundary",
    "cache_state",
    "concurrency",
}


def validate_context(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping) or set(value) != _CONTEXT_FIELDS:
        raise PerformanceError("comparison context has unexpected fields")
    if value["format"] != CONTEXT_FORMAT:
        raise PerformanceError("comparison context format is unsupported")
    _uuid(value["comparison_id"], "comparison_id")
    for key in ("workload_sha256", "environment_sha256", "path_sha256"):
        item = value[key]
        if not isinstance(item, str) or not _SHA256.fullmatch(item):
            raise PerformanceError(f"{key} must be a SHA-256 fingerprint")
    if value["byte_domain"] not in {"logical-payload", "stored-payload"}:
        raise PerformanceError("comparison byte domain is unsupported")
    if value["cache_state"] not in {"cold", "warm", "read-after-write", "mixed-declared"}:
        raise PerformanceError("comparison cache state is unsupported")
    boundary = value["completion_boundary"]
    if not isinstance(boundary, str) or not _BOUNDARY.fullmatch(boundary):
        raise PerformanceError("completion boundary must be a safe semantic identifier")
    _positive_int(value["concurrency"], "concurrency")
    return dict(value)


def _objective(identity: str | None, scenario: str, workload: str) -> Objective | None:
    if identity is None:
        if scenario != "a-riverhog-recovery-tool" or workload not in WORKLOADS:
            raise PerformanceError("observation is outside the recorded recovery scope")
        return None
    found = next((item for item in OBJECTIVES if item.id == identity), None)
    if found is None:
        raise PerformanceError(f"unknown goodput objective: {identity}")
    if identity == "transfer-goodput":
        valid = scenario in NETWORK_SCENARIOS and workload in WORKLOADS
    else:
        valid = (
            re.fullmatch(r"storage-adapter-sequential-segment-[1-9][0-9]*", scenario) is not None
            and workload == "synthetic-one-object-read-after-write"
        )
    if not valid:
        raise PerformanceError(f"sample is outside objective scope: {identity}")
    return found


def _validate_byte_domain(identity: str | None, context: Mapping[str, object] | None) -> None:
    if identity is None or context is None:
        return
    expected = "logical-payload" if identity == "transfer-goodput" else "stored-payload"
    if context["byte_domain"] != expected:
        raise PerformanceError(f"sample byte domain is incompatible with {identity}")


_SAMPLE_FIELDS = {
    "format",
    "objective_id",
    "definition_sha256",
    "run_id",
    "scenario",
    "workload",
    "context",
    "completed_bytes",
    "elapsed_seconds",
    "bytes_per_second",
    "completion_verified",
}


def sample(
    *,
    objective_id: str | None,
    scenario: str,
    workload: str,
    context: Mapping[str, object] | None,
    completed_bytes: int,
    elapsed_seconds: float,
    completion_verified: bool,
    run_id: str,
) -> dict[str, object]:
    _objective(objective_id, scenario, workload)
    count = _positive_int(completed_bytes, "completed_bytes")
    seconds = _positive_number(elapsed_seconds, "elapsed_seconds")
    if type(completion_verified) is not bool:
        raise PerformanceError("completion_verified must be boolean")
    _uuid(run_id, "run_id")
    checked_context = validate_context(context) if context is not None else None
    _validate_byte_domain(objective_id, checked_context)
    rate = _positive_number(count / seconds, "bytes_per_second")
    return {
        "format": SAMPLE_FORMAT,
        "objective_id": objective_id,
        "definition_sha256": definition_sha256(),
        "run_id": run_id,
        "scenario": scenario,
        "workload": workload,
        "context": checked_context,
        "completed_bytes": count,
        "elapsed_seconds": seconds,
        "bytes_per_second": rate,
        "completion_verified": completion_verified,
    }


def validate_sample(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or set(value) != _SAMPLE_FIELDS:
        raise PerformanceError("performance sample has unexpected fields")
    if value["format"] != SAMPLE_FORMAT:
        raise PerformanceError("performance sample format is unsupported")
    identity = value["objective_id"]
    scenario = value["scenario"]
    workload = value["workload"]
    if (
        (identity is not None and not isinstance(identity, str))
        or not isinstance(scenario, str)
        or not isinstance(workload, str)
    ):
        raise PerformanceError("performance sample scope is invalid")
    _objective(identity, scenario, workload)
    digest = value["definition_sha256"]
    if not isinstance(digest, str) or not _SHA256.fullmatch(digest):
        raise PerformanceError("performance definition identity is invalid")
    _uuid(value["run_id"], "run_id")
    count = _positive_int(value["completed_bytes"], "completed_bytes")
    seconds = _positive_number(value["elapsed_seconds"], "elapsed_seconds")
    rate = _positive_number(value["bytes_per_second"], "bytes_per_second")
    if not math.isclose(rate, count / seconds, rel_tol=1e-12):
        raise PerformanceError("performance sample rate disagrees with bytes and interval")
    checked_context = validate_context(value["context"]) if value["context"] is not None else None
    _validate_byte_domain(identity, checked_context)
    if type(value["completion_verified"]) is not bool:
        raise PerformanceError("performance completion flag is invalid")
    return value


def evaluate_sample(
    candidate: Mapping[str, object], reference: Mapping[str, object] | None
) -> dict[str, object]:
    validate_sample(candidate)
    identity = candidate["objective_id"]
    objective = _objective(
        identity if isinstance(identity, str) else None,
        str(candidate["scenario"]),
        str(candidate["workload"]),
    )
    result: dict[str, object] = {
        "objective_id": identity,
        "status": "not-evaluated",
        "reason": "no-objective" if objective is None else "no-measured-reference",
        "candidate_bytes_per_second": candidate["bytes_per_second"],
        "reference_bytes_per_second": None,
        "reference_ratio": None,
        "minimum_reference_fraction": (
            objective.minimum_reference_fraction if objective is not None else None
        ),
    }
    if objective is None or reference is None:
        return result
    validate_sample(reference)
    result["reference_bytes_per_second"] = reference["bytes_per_second"]
    if candidate["run_id"] == reference["run_id"]:
        result["reason"] = "self-comparison"
        return result
    for row in (candidate, reference):
        if row["definition_sha256"] != definition_sha256():
            result["reason"] = "definition-changed"
            return result
        if row["completion_verified"] is not True:
            result["reason"] = "completion-unverified"
            return result
        if row["context"] is None:
            result["reason"] = "comparison-context-missing"
            return result
    for field in ("objective_id", "scenario", "workload", "completed_bytes", "context"):
        if candidate[field] != reference[field]:
            result["reason"] = f"incomparable-{field.replace('_', '-')}"
            return result
    ratio = _positive_number(candidate["bytes_per_second"], "candidate rate") / _positive_number(
        reference["bytes_per_second"], "reference rate"
    )
    if not math.isfinite(ratio):
        raise PerformanceError("goodput ratio is not finite")
    result["reference_ratio"] = ratio
    result["status"] = "met" if ratio >= objective.minimum_reference_fraction else "missed"
    result["reason"] = "matched-measured-reference"
    return result


def _unique_pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise PerformanceError(f"duplicate measurement key: {key}")
        result[key] = value
    return result


def _nonfinite(token: str) -> None:
    raise PerformanceError(f"nonfinite measurement number: {token}")


def read_json(path: Path) -> dict[str, Any]:
    try:
        result = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_unique_pairs,
            parse_constant=_nonfinite,
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise PerformanceError("measurement JSON is unavailable or invalid") from exc
    if not isinstance(result, dict):
        raise PerformanceError("measurement JSON must be an object")
    return result


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("check", "update"))
    args = parser.parse_args(argv)
    try:
        expected = render()
        if OUTPUT.is_symlink():
            raise PerformanceError("generated performance page cannot be a symlink")
        if args.command == "update":
            OUTPUT.parent.mkdir(parents=True, exist_ok=True)
            OUTPUT.write_text(expected, encoding="utf-8")
        elif not OUTPUT.is_file() or OUTPUT.read_text(encoding="utf-8") != expected:
            raise PerformanceError(
                "performance view is stale; run make performance-objectives-update"
            )
    except (PerformanceError, OSError, UnicodeError) as exc:
        print(f"performance accounting check failed: {exc}", file=sys.stderr)
        return 2
    print(f"performance accounting {args.command}: {len(OBJECTIVES)} report-only candidates")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
