"""Shared, report-only measurements for transfer and storage profilers."""

from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from uuid import UUID

SAMPLE_FORMAT = "riverhog-performance-sample/v2"
CONTEXT_FORMAT = "riverhog-performance-comparison-context/v1"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_BOUNDARY = re.compile(r"[a-z][a-z0-9-]{0,95}\Z")
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
_SAMPLE_FIELDS = {
    "format",
    "run_id",
    "scenario",
    "workload",
    "context",
    "completed_bytes",
    "elapsed_seconds",
    "bytes_per_second",
    "completion_verified",
}


class PerformanceError(ValueError):
    """A measurement cannot support its claimed meaning."""


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


def sample(
    *,
    scenario: str,
    workload: str,
    context: Mapping[str, object] | None,
    completed_bytes: int,
    elapsed_seconds: float,
    completion_verified: bool,
    run_id: str,
) -> dict[str, object]:
    if not scenario or not workload:
        raise PerformanceError("measurement scenario and workload are required")
    count = _positive_int(completed_bytes, "completed_bytes")
    seconds = _positive_number(elapsed_seconds, "elapsed_seconds")
    if type(completion_verified) is not bool:
        raise PerformanceError("completion_verified must be boolean")
    _uuid(run_id, "run_id")
    checked_context = validate_context(context) if context is not None else None
    rate = _positive_number(count / seconds, "bytes_per_second")
    return {
        "format": SAMPLE_FORMAT,
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
    if not isinstance(value["scenario"], str) or not value["scenario"]:
        raise PerformanceError("performance sample scenario is invalid")
    if not isinstance(value["workload"], str) or not value["workload"]:
        raise PerformanceError("performance sample workload is invalid")
    _uuid(value["run_id"], "run_id")
    count = _positive_int(value["completed_bytes"], "completed_bytes")
    seconds = _positive_number(value["elapsed_seconds"], "elapsed_seconds")
    rate = _positive_number(value["bytes_per_second"], "bytes_per_second")
    if not math.isclose(rate, count / seconds, rel_tol=1e-12):
        raise PerformanceError("performance sample rate disagrees with bytes and interval")
    if value["context"] is not None:
        validate_context(value["context"])
    if type(value["completion_verified"]) is not bool:
        raise PerformanceError("performance completion flag is invalid")
    return value


def compare_sample(
    candidate: Mapping[str, object],
    reference: Mapping[str, object] | None,
    *,
    target_ratio: float | None = None,
    unavailable_reason: str = "no-measured-reference",
) -> dict[str, object]:
    """Report comparable rates; never decide whether a workflow may proceed."""

    validate_sample(candidate)
    target = _positive_number(target_ratio, "target_ratio") if target_ratio is not None else None
    result: dict[str, object] = {
        "status": "not-compared",
        "reason": unavailable_reason,
        "report_only": True,
        "candidate_bytes_per_second": candidate["bytes_per_second"],
        "reference_bytes_per_second": None,
        "reference_ratio": None,
        "target_ratio": target,
    }
    if reference is None:
        return result
    try:
        validate_sample(reference)
    except PerformanceError:
        result["reason"] = "invalid-measured-reference"
        return result
    result["reference_bytes_per_second"] = reference["bytes_per_second"]
    if candidate["run_id"] == reference["run_id"]:
        result["reason"] = "self-comparison"
        return result
    for row in (candidate, reference):
        if row["completion_verified"] is not True:
            result["reason"] = "completion-unverified"
            return result
        if row["context"] is None:
            result["reason"] = "comparison-context-missing"
            return result
    for field in ("scenario", "workload", "completed_bytes", "context"):
        if candidate[field] != reference[field]:
            result["reason"] = f"incomparable-{field.replace('_', '-')}"
            return result
    ratio = _positive_number(candidate["bytes_per_second"], "candidate rate") / _positive_number(
        reference["bytes_per_second"], "reference rate"
    )
    if not math.isfinite(ratio):
        raise PerformanceError("goodput ratio is not finite")
    result["reference_ratio"] = ratio
    result["status"] = "compared" if target is None else "met" if ratio >= target else "missed"
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
