#!/usr/bin/env python3
"""Non-contractual performance objectives, bookkeeping, and result comparisons.

No application or contract generator imports this module. Existing benchmark/test
callers decide whether a miss is diagnostic or fails their existing check.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import math
import re
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

ROOT = Path(__file__).resolve().parents[1]
PAGE = Path("qualification/performance/README.md")
SCHEMA = "riverhog-performance-objectives/v1"
RESULT_SCHEMA = "riverhog-performance-sample/v1"
CONTEXT_SCHEMA = "riverhog-performance-context/v1"
NOTICE = (
    'Non-contractual engineering objectives and observations only. These records '
        'do not '
    'create, extend, interpret, override, or relax any external contract. A '
        'recorded target '
    "is not automatically a release gate. Related checks retain their independently "
    'defined role. No passing execution or external guarantee is implied by this '
        'page.'
)

# Retained existing tolerances, not theoretical hardware capabilities. A target
# change is an explicit source edit. Do not duplicate these values in consumers.
BUDGETS: dict[str, dict[str, float | int]] = {
    "goodput": {"minimum_reference_fraction": 0.90},
    "page-stream-peak": {"maximum": 32 * 1024 * 1024},
    "http-peak": {"maximum": 64 * 1024 * 1024},
    "plan-work": {"indexed_factor": 4, "linear_factor": 1.5, "slack_rows": 1000},
    "plan-latency": {"minimum_factor": 8, "growth_factor": 4, "slack_ms": 100},
    "memory-growth": {"factor": 4, "slack_bytes": 8 * 1024 * 1024},
    "stream-latency": {"minimum_factor": 4, "growth_factor": 1.5, "slack_ms_per_row": 0.5},
    "listener-observation": {"exclusive_maximum": 4 * 1024 * 1024},
    "listener-runnable": {"exclusive_maximum": 1024 * 1024},
    "plan-temp-io": {"maximum": 0},
}

# These are benchmark dimensions, not assertions that all combinations have run.
SCENARIO_OPERATIONS: dict[str, frozenset[str]] = {
    "riverhog-ingress": frozenset({"pack_upload_open", "pack_write_segment", "raw_upload_open",
            "raw_write_segment"}),
    "stove0-derived-publication": frozenset({"pack_upload_open", "pack_write_segment",
            "raw_upload_open", "raw_write_segment"}),
    "stove0-input-read": frozenset({"pack_retrieval_member", "pack_retrieval_range",
            "raw_retrieval_part"}),
    "riverhog-retrieval": frozenset({"pack_retrieval_member", "pack_retrieval_range",
            "raw_retrieval_part"}),
    "reference-recovery": frozenset(),
    "archive-upload": frozenset({"pack_upload_open", "pack_write_segment", "raw_upload_open",
            "raw_write_segment"}),
    "archive-retrieval": frozenset({"pack_retrieval_member", "pack_retrieval_range",
            "raw_retrieval_part", "retrieval_cache_hydration"}),
    "archive-replication": frozenset({"archive_copy_segment", "archive_copy_object"}),
}
WORKLOADS = ("large-file", "many-small-files", "resume")
NETWORK_SCENARIOS = frozenset(SCENARIO_OPERATIONS) - {"reference-recovery"}


@dataclass(frozen=True)
class Objective:
    id: str
    metric: str
    unit: str
    rule: str
    budget: str
    scope: str
    existing_use: str
    sources: tuple[str, ...]


_DB = "scripts/database_qualification.py"
_LISTENER = "reference/gogurt/application/tests/test_listener.py"
OBJECTIVES = (
    Objective(
        id='transfer-goodput',
        metric='newly completed useful payload / end-to-end wall time',
        unit='bytes/s',
        rule='reference-ratio',
        budget='goodput',
        scope=(
                'Seven network scenario labels x three workload labels in '
                'SCENARIO_OPERATIONS/WORKLOADS. Compare only matched, verified completion '
                'measurements in one declared comparison session. Counts exclude '
                'already-completed resume work and retransmitted copies. The reference is a '
                'measured comparable completion path, not line rate, an asserted capacity, or '
                'an automatic previous-release baseline.'
        ),
        existing_use='report-only',
        sources=(
            'scripts/transfer_profile.py::main',
        ),
    ),
    Objective(
        id='storage-upload-goodput',
        metric='completed stored-object payload / begin-through-complete wall time',
        unit='bytes/s',
        rule='reference-ratio',
        budget='goodput',
        scope=(
                'One bounded synthetic object through the public adapter. Includes admission, '
                'all segment writes, segment traversal and validated completion. Post-upload '
                'readback verifies the payload but is outside the upload timer. Compare to '
                'upload only; exclude setup and cleanup equally on both sides.'
        ),
        existing_use='report-only',
        sources=(
            'tests/harness/storage_adapter_goodput_probe.py::run',
        ),
    ),
    Objective(
        id='storage-read-goodput',
        metric='verified stored-object payload / read-and-hash wall time',
        unit='bytes/s',
        rule='reference-ratio',
        budget='goodput',
        scope=(
                'Read the just-written exact revision, including hashing and stream close. '
                'This is a read-after-write workload, not an uncached retrieval promise. '
                'Compare to read only; no shared upload/read baseline.'
        ),
        existing_use='report-only',
        sources=(
            'tests/harness/storage_adapter_goodput_probe.py::run',
        ),
    ),
    Objective(
        id='database-page-stream-peak',
        metric='peak traced Python allocations during SQL stream',
        unit='bytes',
        rule='maximum',
        budget='page-stream-peak',
        scope=(
                'Current PostgreSQL selector fixture, 100-row fetch chunks, measured at 4096 '
                'and 65536 relation rows. tracemalloc is not RSS or database-server memory.'
        ),
        existing_use='existing-check',
        sources=(
            'scripts/database_qualification.py::_measure_page_stream',
        ),
    ),
    Objective(
        id='database-http-peak',
        metric='peak traced Python allocations during official-client traversal',
        unit='bytes',
        rule='maximum',
        budget='http-peak',
        scope=(
                'Current bounded-page, catalog-sync and inventory fixture paths in '
                '_measure_http_path. Process-wide traced Python allocations, not per-request '
                'RSS or a deployment memory allowance.'
        ),
        existing_use='existing-check',
        sources=(
            'scripts/database_qualification.py::_measure_http_path',
        ),
    ),
    Objective(
        id='database-indexed-work-growth',
        metric='sum of plan Actual Rows x Actual Loops',
        unit='plan-node-rows',
        rule='indexed-work',
        budget='plan-work',
        scope=(
                'Two current cardinalities for cases declaring expected_indexes. Node-row sums '
                'count work across plan levels; they are not unique output rows. Two fixture '
                'points are a regression check, not an asymptotic proof.'
        ),
        existing_use='existing-check',
        sources=(
            'scripts/database_qualification.py::_compare_cardinalities',
        ),
    ),
    Objective(
        id='database-unindexed-work-growth',
        metric='sum of plan Actual Rows x Actual Loops',
        unit='plan-node-rows',
        rule='linear-work',
        budget='plan-work',
        scope=(
                'Two current cardinalities for cases without expected_indexes. Uses actual '
                'relation-row growth; same caveats as the indexed-work check.'
        ),
        existing_use='existing-check',
        sources=(
            'scripts/database_qualification.py::_compare_cardinalities',
        ),
    ),
    Objective(
        id='database-plan-latency-growth',
        metric='PostgreSQL EXPLAIN ANALYZE execution time',
        unit='milliseconds',
        rule='plan-latency',
        budget='plan-latency',
        scope=(
                'Paired cardinalities, identical plan case. Retained broad '
                'runner/cache-sensitive regression envelope; not a user-facing latency target.'
        ),
        existing_use='existing-check',
        sources=(
            'scripts/database_qualification.py::_compare_cardinalities',
        ),
    ),
    Objective(
        id='database-stream-memory-growth',
        metric='peak traced Python allocations during SQL stream',
        unit='bytes',
        rule='memory-growth',
        budget='memory-growth',
        scope=(
                'Paired cardinalities, identical statement case. Relative envelope supplements '
                'the absolute stream ceiling.'
        ),
        existing_use='existing-check',
        sources=(
            'scripts/database_qualification.py::_compare_cardinalities',
        ),
    ),
    Objective(
        id='database-stream-latency-growth',
        metric='full stream wall time / max(returned rows, 1)',
        unit='milliseconds/row',
        rule='stream-latency',
        budget='stream-latency',
        scope=(
                'Paired stream cases; denominator max(returned rows, 1). Empty results '
                'represent empty-query cost rather than per-row observations. Full-stream wall '
                'time includes cursor/transaction cleanup, not time to first byte or end-user '
                'request latency.'
        ),
        existing_use='existing-check',
        sources=(
            'scripts/database_qualification.py::_compare_cardinalities',
        ),
    ),
    Objective(
        id='database-http-memory-growth',
        metric='peak traced Python allocations during client traversal',
        unit='bytes',
        rule='memory-growth',
        budget='memory-growth',
        scope=(
                'Paired cardinalities for each of the three named HTTP traversal paths; no '
                'averaging between paths.'
        ),
        existing_use='existing-check',
        sources=(
            'scripts/database_qualification.py::_compare_cardinalities',
        ),
    ),
    Objective(
        id='database-plan-temp-io',
        metric='sum of reported plan-node temporary reads and writes',
        unit='blocks',
        rule='maximum',
        budget='plan-temp-io',
        scope=(
                'Current bounded selector-plan fixtures should not spill to temporary storage. '
                'The node sum is used only to test zero, not to estimate unique physical I/O.'
        ),
        existing_use='existing-check',
        sources=(
            'scripts/database_qualification.py::_measure_plan',
        ),
    ),
    Objective(
        id='listener-observation-peak',
        metric='peak traced Python allocations during observe',
        unit='bytes',
        rule='exclusive-maximum',
        budget='listener-observation',
        scope=(
                'One current unmarked mount after seeding 50000 historical mounts; seeding is '
                'outside tracing. The ceiling is fixture-local and does not by itself prove '
                'history-independent complexity.'
        ),
        existing_use='existing-check',
        sources=(
            (
                'reference/gogurt/application/tests/test_listener.py::test_listener_observation'
                '_memory_depends_on_current_mounts_not_history'
            ),
        ),
    ),
    Objective(
        id='listener-runnable-peak',
        metric='peak traced Python allocations during runnable selection',
        unit='bytes',
        rule='exclusive-maximum',
        budget='listener-runnable',
        scope=(
                'Select three runnable dispatches from a fixture of 10000 queued rows. Seeding '
                'is outside tracing.'
        ),
        existing_use='existing-check',
        sources=(
            (
                'reference/gogurt/application/tests/test_listener.py::test_listener_runnable_se'
                'lection_is_bounded_by_available_custody'
            ),
        ),
    ),
)

# Accounting dispositions keep measurements and correctness/extent controls from
# being mistaken for missing numerical targets. They never create thresholds.
DISPOSITIONS = (
    ("observation-only", "reference-recovery", "scripts/transfer_profile.py::main",
     'Three workload labels; record completed bytes, items and elapsed time. No '
         'existing rate target; do not manufacture one.'),
    ("observation-only", "process-cold-cli-startup",
        "scripts/operation_qualification.py::_cold_cli_timings",
     'Three fresh Python processes per entrypoint by default; --help elapsed '
         'min/median/max and output identity. '
     "Not a cold OS filesystem cache; no existing latency ceiling."),
    ("observation-only", "local-api-and-client-timings",
        "scripts/operation_qualification.py::_load_operation_timings",
     'Successful local-fixture server/client wall-time summaries joined to the '
         'executable operation matrix. '
     'Coverage/positive-sample requirements are not speed targets; no invented '
         'per-endpoint budget.'),
    ("observation-only", "stove0-scale", "scripts/test_compose_smoke.sh",
     'Completion elapsed time, file counts, database/document bytes, target CPU '
         'and cgroup peak memory. '
     '128 files is a fixture size, not a capacity guarantee. Cgroup counters '
         'cover their actual lifetime, not necessarily just the workload.'),
    ("observation-only", "transfer-phase-telemetry", "riverhog/src/riverhog_core/throughput.py",
     'Phase timers, bytes and bottleneck labels are diagnostics. Summed '
         'phase/segment work can overlap or count retries; '
     'never use those sums as unique end-to-end goodput or exclusive '
         'elapsed-time attribution.'),
    ("observation-only", "database-first-row-and-planning-time", _DB,
     'Planning time, first-item time and absolute traversal durations are '
         'retained observations; only the named objectives above carry bounds.'),
    ("not-an-objective", "extent-and-structural-correctness", "scripts/extent_contract.py",
     'Bounded pages, no truncation, exact results, row/query counts, '
         'continuation, natural-plan shape and cleanup assertions '
     'remain with their present owners. This inventory does not weaken them or '
         'reclassify contracts.'),
    ("not-an-objective", "runtime-capacity-and-estimates",
        "riverhog/src/riverhog_core/throughput.py",
     'Concurrency/chunk/memory settings, quotas and retrieval estimated latency '
         'configure operation. '
     "They are neither measured capacity baselines nor performance commitments."),
    ("not-an-objective", "harness-deadlines-and-cleanup", "scripts/test_compose_smoke.sh",
     'Wait/timeout limits bound the test harness. Empty temporary workspace '
         'after completion is cleanup correctness, '
     "not a zero-working-memory objective."),
)


class PerformanceError(ValueError):
    """Invalid bookkeeping or incomparable performance input."""


def _number(value: object, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PerformanceError("measurement must be a finite number")
    number = float(value)
    if not math.isfinite(number) or number < 0 or (positive and number == 0):
        raise PerformanceError(
            "measurement must be finite and nonnegative (positive for rates/times)"
        )
    return number


def _integer(value: object, *, positive: bool = False) -> int:
    if type(value) is not int or value < 0 or (positive and value == 0):
        raise PerformanceError("count must be a nonnegative integer (positive when required)")
    return value


def objective(identity: str) -> Objective:
    for item in OBJECTIVES:
        if item.id == identity:
            return item
    raise PerformanceError(f"unknown performance objective: {identity}")


def limit(identity: str, *, low: float | None = None, growth: float | None = None) -> float:
    item = objective(identity)
    p = BUDGETS[item.budget]
    if item.rule == "reference-ratio":
        return float(p["minimum_reference_fraction"])
    if item.rule == "maximum":
        return float(p["maximum"])
    if item.rule == "exclusive-maximum":
        return float(p["exclusive_maximum"])
    baseline = _number(low)
    if item.rule == "memory-growth":
        return baseline * p["factor"] + p["slack_bytes"]
    if item.rule == "indexed-work":
        return (baseline + 1) * p["indexed_factor"] + p["slack_rows"] - 1
    multiplier = _number(growth, positive=True)
    if multiplier <= 1:
        raise PerformanceError("cardinality growth must be greater than one")
    if item.rule == "linear-work":
        return (baseline + 1) * multiplier * p["linear_factor"] + p["slack_rows"] - 1
    if item.rule == "plan-latency":
        return baseline * max(p["minimum_factor"], multiplier * p["growth_factor"]) + p["slack_ms"]
    if item.rule == "stream-latency":
        return baseline * max(p["minimum_factor"],
            multiplier * p["growth_factor"]) + p["slack_ms_per_row"]
    raise PerformanceError(f"unknown rule: {item.rule}")


def within(identity: str, value: float, *, low: float | None = None,
    growth: float | None = None) -> bool:
    observed = _number(value)
    bound = limit(identity, low=low, growth=growth)
    rule = objective(identity).rule
    if rule == "reference-ratio":
        raise PerformanceError(
            "goodput requires evaluate_sample, not an unqualified scalar comparison"
        )
    return observed < bound if rule == "exclusive-maximum" else observed <= bound



def comparison(identity: str, value: float, *, low: float | None = None,
               growth: float | None = None, case: str | None = None) -> dict[str, Any]:
    (
        "Record the exact retained check, without changing its caller's failure "
        'policy.'
    )
    return {"objective": identity, "case": case, "observed": _number(value),
            "limit": limit(identity, low=low, growth=growth),
            "status": "met" if within(identity, value, low=low, growth=growth) else "missed"}

def definition_sha256() -> str:
    # Includes evaluator and benchmark-dimension semantics, not only target scalars.
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def _pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise PerformanceError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def read_json(path: Path) -> dict[str, Any]:
    def reject(token: str) -> None:
        raise PerformanceError(f"nonfinite JSON number: {token}")
    try:
        result = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_pairs,
            parse_constant=reject)
    except (OSError, json.JSONDecodeError) as exc:
        raise PerformanceError("unable to read valid measurement JSON") from exc
    if not isinstance(result, dict):
        raise PerformanceError("measurement document must be an object")
    return result


_CONTEXT_KEYS = {"schema", "comparison_id", "workload_sha256", "environment_sha256", "path_sha256",
                 "byte_domain", "completion_boundary", "cache_state", "concurrency"}


def validate_context(value: Mapping[str, Any]) -> dict[str, Any]:
    if set(value) != _CONTEXT_KEYS or value.get("schema") != CONTEXT_SCHEMA:
        raise PerformanceError("comparison context fields or schema do not match")
    try:
        UUID(str(value["comparison_id"]))
    except ValueError as exc:
        raise PerformanceError(
            "comparison_id must identify one measurement session with a UUID"
        ) from exc
    for key in ("workload_sha256", "environment_sha256", "path_sha256"):
        if not isinstance(value[key], str) or re.fullmatch(r"[0-9a-f]{64}", value[key]) is None:
            raise PerformanceError(f"{key} must be an exact SHA256 fingerprint")
    if value["byte_domain"] not in ("logical-payload", "stored-payload"):
        raise PerformanceError("unknown byte domain")
    if value["cache_state"] not in ("cold", "warm", "read-after-write", "mixed-declared"):
        raise PerformanceError("unknown cache state")
    boundary = value["completion_boundary"]
    if not isinstance(boundary, str) or re.fullmatch(r"[a-z][a-z0-9-]{0,95}", boundary) is None:
        raise PerformanceError("completion boundary must be a safe semantic identifier")
    _integer(value["concurrency"], positive=True)
    return dict(value)


def source_identity(root: Path = ROOT) -> dict[str, Any]:
    try:
        head = subprocess.run(["git", "-C", str(root), "rev-parse", "HEAD"], check=True,
            capture_output=True, text=True)
        state = subprocess.run(["git", "-C", str(root), "status", "--porcelain",
                "--untracked-files=all"], check=True, capture_output=True, text=True)
        sha = head.stdout.strip()
        if re.fullmatch(r"[0-9a-f]{40}", sha) is None:
            raise ValueError("invalid SHA")
        return {"sha": sha, "clean": not state.stdout.strip()}
    except (OSError, subprocess.SubprocessError, ValueError):
        return {"sha": None, "clean": False}



def _validate_scope(identity: str | None, scenario: str, workload: str) -> None:
    if identity == "transfer-goodput":
        valid = scenario in NETWORK_SCENARIOS and workload in WORKLOADS
    elif identity in {"storage-upload-goodput", "storage-read-goodput"}:
        valid = (re.fullmatch(r"storage-adapter-sequential-segment-[1-9][0-9]*",
                scenario) is not None
                 and workload == "synthetic-one-object-read-after-write")
    else:
        valid = identity is None and scenario == "reference-recovery" and workload in WORKLOADS
    if not valid:
        raise PerformanceError("sample does not belong to a registered benchmark scope")

def sample(*, identity: str | None, scenario: str, workload: str, completed_bytes: int,
           elapsed_seconds: float, verified: bool, context: Mapping[str, Any] | None,
           source_start: Mapping[str, Any], source_finish: Mapping[str, Any],
    run_id: str) -> dict[str, Any]:
    if identity is not None and objective(identity).rule != "reference-ratio":
        raise PerformanceError("sample objective is not a goodput objective")
    _validate_scope(identity, scenario, workload)
    count = _integer(completed_bytes, positive=True)
    seconds = _number(elapsed_seconds, positive=True)
    if type(verified) is not bool:
        raise PerformanceError("verification flag must be boolean")
    UUID(run_id)
    return {"schema": RESULT_SCHEMA, "objective": identity,
        "definition_sha256": definition_sha256(),
            "run_id": run_id, "scenario": scenario, "workload": workload,
            "context": validate_context(context) if context is not None else None,
            "source_start": dict(source_start), "source_finish": dict(source_finish),
            "completed_bytes": count, "elapsed_seconds": seconds,
            "bytes_per_second": count / seconds, "completion_verified": verified}


def _validate_sample(value: Mapping[str, Any]) -> None:
    keys = {"schema", "objective", "definition_sha256", "run_id", "scenario", "workload", "context",
            "source_start", "source_finish", "completed_bytes", "elapsed_seconds",
        "bytes_per_second", "completion_verified"}
    if set(value) != keys or value.get("schema") != RESULT_SCHEMA:
        raise PerformanceError("sample fields or schema do not match")
    _validate_scope(value["objective"], value["scenario"], value["workload"])
    count = _integer(value["completed_bytes"], positive=True)
    seconds = _number(value["elapsed_seconds"], positive=True)
    rate = _number(value["bytes_per_second"], positive=True)
    if not math.isclose(rate, count / seconds, rel_tol=1e-12):
        raise PerformanceError("sample rate disagrees with its numerator and duration")
    try:
        UUID(str(value["run_id"]))
    except ValueError as exc:
        raise PerformanceError("invalid sample identity") from exc
    if type(value["completion_verified"]) is not bool:
        raise PerformanceError("invalid verification flag")
    if value["context"] is not None:
        validate_context(value["context"])
    for boundary in ("source_start", "source_finish"):
        source = value[boundary]
        if not isinstance(source, dict) or set(source) != {"sha",
            "clean"} or type(source["clean"]) is not bool:
            raise PerformanceError("invalid source identity")
        if source["sha"] is not None and (not isinstance(source["sha"], str) 
            or re.fullmatch(r"[0-9a-f]{40}", source["sha"]) is None):
            raise PerformanceError("invalid source SHA")


def evaluate_sample(candidate: Mapping[str, Any], reference: Mapping[str,
        Any] | None) -> dict[str, Any]:
    _validate_sample(candidate)
    identity = candidate["objective"]
    result: dict[str, Any] = {"objective": identity, "status": "not_evaluated",
        "reason": "no_target",
                              "reference_ratio": None, "minimum_reference_fraction": None}
    if identity is None:
        return result
    if objective(identity).rule != "reference-ratio":
        raise PerformanceError("not a goodput objective")
    result["minimum_reference_fraction"] = limit(identity)
    if reference is None:
        result["reason"] = "no_measured_reference"
        return result
    _validate_sample(reference)
    for row in (candidate, reference):
        if row["definition_sha256"] != definition_sha256():
            result["reason"] = "definition_changed"
            return result
        if row["completion_verified"] is not True:
            result["reason"] = "completion_unverified"
            return result
        if (
            row["source_start"] != row["source_finish"]
            or row["source_start"]["clean"] is not True
