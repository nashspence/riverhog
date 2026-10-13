"""Validation authority and advisory observations have separate data and result paths."""

from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime


def _seconds(value: object, *, positive: bool) -> int | float:
    if type(value) not in (int, float):
        raise ValueError("seconds must be finite numeric measurement data")
    assert isinstance(value, (int, float))
    try:
        valid = math.isfinite(value) and (value > 0 if positive else value >= 0)
    except OverflowError:
        valid = False
    if not valid:
        raise ValueError("seconds must be finite and in the measurement domain")
    return value


def validation_path_identity(value: str) -> str:
    if re.fullmatch(r"[a-zA-Z0-9_-][a-zA-Z0-9_.-]{0,127}", value) is None:
        raise ValueError("invalid validation path identity")
    return value


def runner_platform(image: str) -> str:
    for prefix, platform in (("ubuntu-", "linux"), ("macos-", "darwin"), ("windows-", "win32")):
        if image.startswith(prefix):
            return platform
    raise ValueError("runner condition has no observed platform adapter")


def validate_measurement_context(row: Mapping[str, object]) -> None:
    for field in ("started_at", "finished_at"):
        value = row[field]
        if not isinstance(value, str) or datetime.fromisoformat(value).utcoffset() is None:
            raise ValueError("execution timestamps require an explicit timezone")
    start, finish = row["started_at"], row["finished_at"]
    assert isinstance(start, str) and isinstance(finish, str)
    if datetime.fromisoformat(finish) < datetime.fromisoformat(start):
        raise ValueError("execution finished before it started")
    _seconds(row["elapsed_seconds"], positive=False)


def validate_phase_costs(row: Mapping[str, object]) -> None:
    validate_measurement_context(row)
    phases = row["phase_costs"]
    if not isinstance(phases, dict) or set(phases) != {
        "preparation",
        "command-including-owner-cleanup",
        "evidence-collection",
    }:
        raise ValueError("required phase costs are incomplete")
    costs = [_seconds(value, positive=False) for value in phases.values()]
    if not math.isclose(
        sum(costs), _seconds(row["elapsed_seconds"], positive=False), rel_tol=1e-9, abs_tol=1e-9
    ):
        raise ValueError("phase costs contradict measured invocation wall time")


@dataclass(frozen=True)
class ValidationWork:
    """Required work; an optional speed goal never enters its required definition."""

    name: str
    command: tuple[str, ...]
    target_seconds: float | None = None
    boundary: str = "command-including-owned-preparation-and-cleanup"
    conditions: tuple[tuple[str, str], ...] = ()
    growth_witnesses: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if (
            not isinstance(self.name, str)
            or not self.name
            or not self.command
            or any(not isinstance(item, str) or not item for item in self.command)
        ):
            raise ValueError("validation work requires a name and a command")
        if self.target_seconds is not None:
            _seconds(self.target_seconds, positive=True)
        if (
            not isinstance(self.boundary, str)
            or not self.boundary
            or (len(dict(self.conditions)) != len(self.conditions))
        ):
            raise ValueError("validation work requires an exact boundary and unique conditions")
        if any(not key or not isinstance(value, str) for key, value in self.conditions):
            raise ValueError("invalid measured-work conditions")
        if len(set(self.growth_witnesses)) != len(self.growth_witnesses) or any(
            not isinstance(item, str) or not item for item in self.growth_witnesses
        ):
            raise ValueError("growth witness bindings must be nonempty and unique")

    def definition(self) -> dict[str, object]:
        """Only executable work and independently owned obligations bind coverage."""
        return {
            "name": self.name,
            "command": list(self.command),
            "boundary": self.boundary,
            "conditions": dict(self.conditions),
            "growth_witnesses": list(self.growth_witnesses),
        }

    def advisory_target(self) -> dict[str, object] | None:
        if self.target_seconds is None:
            return None
        return {
            "target_seconds": self.target_seconds,
            "boundary": self.boundary,
            "conditions": dict(self.conditions),
        }


def observe_performance(
    definition: Mapping[str, object],
    *,
    elapsed: object,
    exit_status: object,
    source_clean: object,
    target: object = None,
) -> dict[str, object]:
    """An unavailable comparison is neutral; malformed supplied data is an error."""
    measured = _seconds(elapsed, positive=False)
    if type(exit_status) is not int or type(source_clean) is not bool:
        raise ValueError("invalid validation completion state")
    seconds = None
    if target is not None:
        if not isinstance(target, dict) or set(target) != {
            "target_seconds",
            "boundary",
            "conditions",
        }:
            raise ValueError("malformed advisory target observation")
        seconds = _seconds(target["target_seconds"], positive=True)
        if not isinstance(target["boundary"], str) or not target["boundary"]:
            raise ValueError("invalid advisory measurement boundary")
        conditions = target["conditions"]
        if not isinstance(conditions, dict) or any(
            not isinstance(k, str) or not isinstance(v, str) for k, v in conditions.items()
        ):
            raise ValueError("invalid advisory comparison conditions")
    reason = "completed-command"
    if exit_status:
        reason = "command-failed"
    elif not source_clean:
        reason = "source-unverified"
    elif target is None:
        reason = "no-target"
    elif (
        target["boundary"] != definition["boundary"]
        or target["conditions"] != definition["conditions"]
    ):
        reason = "conditions-not-comparable"
    status = "not-compared"
    if reason == "completed-command":
        assert seconds is not None
        status = "met" if measured < seconds else "missed"
    return {
        "report_only": True,
        "target_seconds": seconds,
        "status": status,
        "reason": reason,
    }


def validate_evidence_records(
    records: Sequence[object],
    *,
    expected: Sequence[ValidationWork],
    source_sha: str,
    run_id: str,
) -> dict[str, object]:
    """Validate required evidence and propagate contract outcomes, never target outcomes.

    Growth results enter after the owner adapter has verified its actual artifacts.
    This checks expected outcome coverage, not the truth of an arbitrary claimed bound.
    """
    if re.fullmatch(r"[0-9a-f]{40}", source_sha) is None or not run_id:
        raise ValueError("exact expected source and run identity are required")
    definitions = {work.name: work.definition() for work in expected}
    if not expected or len(definitions) != len(expected):
        raise ValueError("expected workloads must be nonempty and unique")
    required_fields = {
        "format",
        "source_sha",
        "source_clean",
        "source_end",
        "run_id",
        "definition",
        "state",
        "elapsed_seconds",
        "phase_costs",
        "exit_status",
        "growth_results",
        "started_at",
        "finished_at",
        "started_monotonic_seconds",
        "observed_conditions",
    }
    advisory_fields = {"advisory_target", "profiling_target"}
    seen: set[str] = set()
    comparisons: dict[str, object] = {}
    functional: dict[str, int] = {}
    growth: dict[str, dict[str, str]] = {}
    for row in records:
        if (
            not isinstance(row, dict)
            or not required_fields <= row.keys()
            or (row.keys() - required_fields - advisory_fields)
            or row.get("format") != "riverhog-ci-validation-evidence/v2"
        ):
            raise ValueError("invalid validation evidence format")
        if row["source_sha"] != source_sha or row["run_id"] != run_id:
            raise ValueError("validation evidence source or run differs")
        definition = row["definition"]
        if not isinstance(definition, dict):
            raise ValueError("missing executable workload definition")
        name = definition.get("name")
        if not isinstance(name, str) or name not in definitions or name in seen:
            raise ValueError("unexpected or duplicate validation workload")
        if definition != definitions[name]:
            raise ValueError("required workload, boundary, conditions or growth bindings differ")
        if row["state"] != "completed":
            raise ValueError("validation execution is incomplete")
        validate_phase_costs(row)
        _seconds(row["started_monotonic_seconds"], positive=False)
        conditions = row["observed_conditions"]
        if (
            not isinstance(conditions, dict)
            or set(conditions) != {"platform", "architecture", "python"}
            or any(not isinstance(value, str) or not value for value in conditions.values())
        ):
            raise ValueError("invalid observed execution conditions")
        runner = definition["conditions"].get("runner_image")
        if runner is not None and conditions["platform"] != runner_platform(runner):
            raise ValueError("observed platform contradicts the required work conditions")
        if row["source_clean"] is not True or row["source_end"] != {
            "source_sha": source_sha,
            "source_clean": True,
        }:
            raise ValueError("required validation source is unverified or changed")
        observation = observe_performance(
            definition,
            elapsed=row["elapsed_seconds"],
            exit_status=row["exit_status"],
            source_clean=row["source_clean"],
            target=row.get("advisory_target"),
        )
        # A supplied observation must be truthful. Its outcome is never a gate.
        if "profiling_target" in row and (
            not isinstance(row["profiling_target"], dict)
            or row["profiling_target"].get("report_only") is not True
            or row["profiling_target"] != observation
        ):
            raise ValueError("supplied advisory observation contradicts measured evidence")
        outcomes = row["growth_results"]
        if not isinstance(outcomes, dict) or set(outcomes) != set(definition["growth_witnesses"]):
            raise ValueError("missing or unexpected required growth evidence")
        if any(value not in ("pass", "fail") for value in outcomes.values()):
            raise ValueError("invalid or incomplete required growth result")
        seen.add(name)
        functional[name] = row["exit_status"]
        growth[name] = dict(outcomes)
        comparisons[name] = observation
    if seen != definitions.keys():
        missing = ", ".join(sorted(definitions.keys() - seen))
        raise ValueError(f"missing validation evidence: {missing}")
    contract_failed = any(functional.values()) or any(
        outcome == "fail" for outcomes in growth.values() for outcome in outcomes.values()
    )
    return {
        "evidence_complete": True,
        "functional_exit_status": functional,
        "growth_results": growth,
        "qualification_exit_status": 1 if contract_failed else 0,
        "comparisons": comparisons,
    }
