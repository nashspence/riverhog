"""Descriptive three-arm comparison, after the real evidence owners validate inputs.

This reference has no executor, admission, signature, resource receiving or CLI.
An asserted identity/receipt is not proof of its origin. The maintained producer,
source/artifact validators and original resource owners must establish those facts.
All timings are integer nanoseconds; unknown counters remain None, never zero.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields
from fractions import Fraction
from typing import TypedDict, cast
from uuid import UUID

from scripts import performance_measurement as existing

ARMS = ("legacy", "restart", "adaptive")
DEFAULT_POLICY = {
    "recovery_mode": "adaptive",
    "replay_target_seconds": 120,
    "max_optional_overhead_fraction": 0.125,
    "max_optional_checkpoints": None,
    "retained_checkpoint_generations": 2,
}


@dataclass(frozen=True)
class Counters:
    """Different quantities, not additive components of an elapsed interval."""

    worker_cpu_ns: int | None = None
    worker_wait_ns: int | None = None
    sql_attempts: int | None = None
    commit_attempts: int | None = None
    commit_returns: int | None = None
    sync_attempts: int | None = None
    sync_returns: int | None = None
    checkpoint_bytes: int | None = None
    metadata_bytes: int | None = None
    optional_store_initializations: int | None = None
    optional_attempts: int | None = None
    optional_successes: int | None = None
    optional_lifecycle_ns: int | None = None
    optional_setup_ns: int | None = None
    optional_failed_ns: int | None = None
    optional_cleanup_ns: int | None = None
    policy_instrumentation_ns: int | None = None
    restore_validation_ns: int | None = None
    replayed_useful_ns: int | None = None
    repeated_network_bytes: int | None = None
    paid_effect_attempts: int | None = None


@dataclass(frozen=True)
class Observation:
    arm: str
    block_id: str
    order: int
    sample: Mapping[str, object]
    source_sha: str
    source_clean: bool
    artifact_sha256: str
    executor_sha256: str
    instrumentation_sha256: str
    conditions_sha256: str
    clock_origin_id: str
    input_kind: str
    result_identity_sha256: str
    receipt_sha256: str
    policy: Mapping[str, object] | None
    result_ns: int
    received_ns: int | None
    counters: Counters
    interruption_sha256: str | None = None


class Difference(TypedDict):
    signed_delta_ns: int
    clipped_d_ns: int
    d_over_adaptive: dict[str, int]
    target_at_most_one_eighth: bool | None
    equivalent_restart_over_seven: bool | None


def _integer(value: object, name: str, minimum: int = 0) -> None:
    if type(value) is not int or value < minimum:
        raise existing.PerformanceError(f"{name} requires an exact integer >= {minimum}")


def _digest(value: object, name: str, width: int = 64) -> None:
    if not isinstance(value, str) or re.fullmatch(rf"[0-9a-f]{{{width}}}", value) is None:
        raise existing.PerformanceError(f"{name} has invalid digest")


def _fraction(value: Fraction) -> dict[str, int]:
    return {"numerator": value.numerator, "denominator": value.denominator}


def _validate(row: Observation) -> None:
    if row.arm not in ARMS:
        raise existing.PerformanceError("unknown arm")
    try:
        canonical = str(UUID(row.block_id))
    except (ValueError, TypeError, AttributeError) as error:
        raise existing.PerformanceError("invalid block identity") from error
    if canonical != row.block_id:
        raise existing.PerformanceError("noncanonical block identity")
    _integer(row.order, "order")
    if row.order >= len(ARMS):
        raise existing.PerformanceError("order outside one three-arm block")
    existing.validate_sample(row.sample)
    if row.clock_origin_id != row.sample["run_id"]:
        raise existing.PerformanceError(
            "milestones must use this original parent's monotonic origin"
        )
    if row.sample["completion_verified"] is not True:
        raise existing.PerformanceError("unverified result cannot be a successful observation")
    if row.source_clean is not True:
        raise existing.PerformanceError("qualified comparison requires clean exact source")
    _digest(row.source_sha, "source_sha", 40)
    for name in (
        "artifact_sha256",
        "executor_sha256",
        "instrumentation_sha256",
        "conditions_sha256",
        "result_identity_sha256",
        "receipt_sha256",
    ):
        _digest(getattr(row, name), name)
    if row.interruption_sha256 is not None:
        _digest(row.interruption_sha256, "interruption_sha256")
    if row.input_kind not in {"captured-immutable", "fresh-publication"}:
        raise existing.PerformanceError("unknown input identity contract")
    _integer(row.result_ns, "result_ns", 1)
    if row.sample["elapsed_seconds"] != row.result_ns / 1_000_000_000:
        raise existing.PerformanceError("sample interval differs from durable-result interval")
    if row.received_ns is not None:
        _integer(row.received_ns, "received_ns", row.result_ns)
    if row.arm == "legacy":
        if row.policy is not None:
            raise existing.PerformanceError("legacy is not a redesigned policy mode")
    else:
        expected = {**DEFAULT_POLICY, "recovery_mode": row.arm}
        if not isinstance(row.policy, Mapping) or set(row.policy) != set(expected):
            raise existing.PerformanceError("all five effective policy settings are required")
        if any(
            type(row.policy[key]) is not type(value) or row.policy[key] != value
            for key, value in expected.items()
        ):
            raise existing.PerformanceError("effective policy differs from the selected defaults")
    if type(row.counters) is not Counters:
        raise existing.PerformanceError("typed counter receipt is required")
    for field in fields(Counters):
        value = getattr(row.counters, field.name)
        if value is not None:
            _integer(value, field.name)
    for returned, attempted in (
        ("commit_returns", "commit_attempts"),
        ("sync_returns", "sync_attempts"),
        ("optional_successes", "optional_attempts"),
    ):
        left, right = getattr(row.counters, returned), getattr(row.counters, attempted)
        if left is not None and right is not None and left > right:
            raise existing.PerformanceError(f"{returned} exceeds {attempted}")
    if row.arm == "restart" and any(
        getattr(row.counters, name) not in (None, 0)
        for name in ("optional_store_initializations", "optional_attempts", "optional_successes")
    ):
        raise existing.PerformanceError("restart observation performed optional recovery work")


def _difference(restart_ns: int, adaptive_ns: int, *, normal: bool) -> Difference:
    signed = adaptive_ns - restart_ns
    clipped = max(0, signed)
    return {
        "signed_delta_ns": signed,
        "clipped_d_ns": clipped,
        "d_over_adaptive": _fraction(Fraction(clipped, adaptive_ns)),
        "target_at_most_one_eighth": clipped * 8 <= adaptive_ns if normal else None,
        "equivalent_restart_over_seven": clipped * 7 <= restart_ns if normal else None,
    }


def compare_block(rows: Sequence[Observation]) -> dict[str, object]:
    """Compare one matched block; missing/mismatched work never becomes a pass.

    Invalid supplied evidence raises PerformanceError. A still-open original tail
    can produce a result-boundary observation, but never a full-lifecycle result.
    Neither a timing miss nor a target match decides correctness or policy admission.
    """
    by_arm: dict[str, Observation] = {}
    for row in rows:
        _validate(row)
        if row.arm in by_arm:
            raise existing.PerformanceError("duplicate arm in one block")
        by_arm[row.arm] = row
    if len({row.block_id for row in rows}) > 1 or len({row.order for row in rows}) != len(rows):
        raise existing.PerformanceError("observations do not describe one uniquely ordered block")
    run_ids = [row.sample["run_id"] for row in rows]
    result: dict[str, object] = {
        "status": "not-compared",
        "report_only": True,
        "reason": "missing-arm",
        "missing_arms": [arm for arm in ARMS if arm not in by_arm],
        "run_ids": run_ids,
    }
    if result["missing_arms"]:
        return result
    legacy, restart, adaptive = (by_arm[arm] for arm in ARMS)
    if len(set(run_ids)) != 3 or len({row.receipt_sha256 for row in rows}) != 3:
        raise existing.PerformanceError("one execution or receipt reused as several arms")
    if {row.order for row in rows} != {0, 1, 2}:
        raise existing.PerformanceError("arm execution order is not a permutation")
    for name in (
        "block_id",
        "instrumentation_sha256",
        "conditions_sha256",
        "input_kind",
        "interruption_sha256",
    ):
        if any(getattr(row, name) != getattr(legacy, name) for row in rows):
            result["reason"] = f"incomparable-{name}"
            return result
    for row in (legacy, adaptive):
        comparison = existing.compare_sample(row.sample, restart.sample)
        if comparison["status"] != "compared":
            result["reason"] = comparison["reason"]
            return result
    if any(
        getattr(restart, name) != getattr(adaptive, name)
        for name in ("source_sha", "artifact_sha256", "executor_sha256")
    ):
        result["reason"] = "redesigned-executors-differ"
        return result
    if legacy.source_sha == restart.source_sha or legacy.executor_sha256 == restart.executor_sha256:
        result["reason"] = "legacy-is-not-a-preserved-different-executor"
        return result
    if (
        legacy.input_kind == "captured-immutable"
        and len({row.result_identity_sha256 for row in rows}) != 1
    ):
        raise existing.PerformanceError("captured immutable input produced conflicting results")
    normal = legacy.interruption_sha256 is None
    result.update(
        status="compared",
        reason="matched-owned-observations",
        block_id=legacy.block_id,
        kind="normal" if normal else "interrupted",
        order=[row.arm for row in sorted(rows, key=lambda row: row.order)],
        durable_result=_difference(restart.result_ns, adaptive.result_ns, normal=normal),
        legacy_to_restart_signed_ns=restart.result_ns - legacy.result_ns,
        received=None,
        tail_ns={row.arm: None for row in rows},
    )
    received = {row.arm: row.received_ns for row in rows if row.received_ns is not None}
    if len(received) == len(ARMS):
        result["received"] = _difference(received["restart"], received["adaptive"], normal=normal)
        result["tail_ns"] = {row.arm: received[row.arm] - row.result_ns for row in rows}
    else:
        result["receiving_comparison_reason"] = "original-receiving-incomplete"
    result["counter_differences"] = {
        field.name: (
            None
            if getattr(adaptive.counters, field.name) is None
            or getattr(restart.counters, field.name) is None
            else getattr(adaptive.counters, field.name) - getattr(restart.counters, field.name)
        )
        for field in fields(Counters)
    }
    result["restart_optional_zero_observed"] = all(
        getattr(restart.counters, name) == 0
        for name in ("optional_store_initializations", "optional_attempts", "optional_successes")
    )
    return result


def summarize_blocks(blocks: Sequence[Sequence[Observation]]) -> dict[str, object]:
    """Keep every block, including adverse/missing ones; descriptive spread only."""
    reports = [compare_block(block) for block in blocks]
    seen_runs = [row.sample["run_id"] for block in blocks for row in block]
    if len(seen_runs) != len(set(seen_runs)):
        raise existing.PerformanceError("same execution repeated across blocks")
    complete = [report for report in reports if report["status"] == "compared"]
    if len({report["block_id"] for report in complete}) != len(complete):
        raise existing.PerformanceError("duplicate paired block")
    # Full matching across repetitions is still required; don't pool unrelated cells.
    fingerprints = {
        (
            row.sample["scenario"],
            row.sample["workload"],
            row.sample["completed_bytes"],
            (
                tuple(sorted(existing.validate_context(row.sample["context"]).items()))
                if row.sample["context"] is not None
                else ()
            ),
            row.conditions_sha256,
            row.instrumentation_sha256,
            row.interruption_sha256,
            row.input_kind,
        )
        for block in blocks
        for row in block
    }
    artifact_drift = any(
        len(
            {
                (row.source_sha, row.artifact_sha256, row.executor_sha256)
                for block in blocks
                for row in block
                if row.arm == arm
            }
        )
        > 1
        for arm in ARMS
    )
    if len(fingerprints) > 1 or artifact_drift:
        return {
            "status": "not-compared",
            "report_only": True,
            "reason": "mixed-conditions-or-artifacts",
            "blocks": reports,
        }
    summary: dict[str, object] = {
        "status": "compared" if complete else "not-compared",
        "report_only": True,
        "blocks": reports,
        "paired_observations": len(complete),
        "omitted_or_unmatched_blocks": len(reports) - len(complete),
        "all_declared_blocks_compared": bool(reports) and len(reports) == len(complete),
        "uncertainty": "descriptive-paired-spread-no-confidence-interval",
    }
    for boundary in ("durable_result", "received"):
        # These values came only from this module's _difference after compare_block
        # validated the observations; this is not a cast of unvalidated wire input.
        values = [
            cast(Difference, report[boundary])
            for report in complete
            if report[boundary] is not None
        ]
        if not values:
            summary[boundary] = None
            continue
        signed = sorted(value["signed_delta_ns"] for value in values)
        median = Fraction(signed[(len(signed) - 1) // 2] + signed[len(signed) // 2], 2)
        mean = Fraction(sum(signed), len(signed))
        summary[boundary] = {
            "paired_observations": len(signed),
            "signed_delta_range_ns": [signed[0], signed[-1]],
            "signed_delta_median_ns": _fraction(median),
            "signed_delta_mean_ns": _fraction(mean),
            "mean_of_pairwise_clipped_d_ns": _fraction(
                Fraction(sum(value["clipped_d_ns"] for value in values), len(values))
            ),
            "clipped_mean_signed_delta_ns": _fraction(max(Fraction(0), mean)),
            "observed_target_misses": sum(
                value["target_at_most_one_eighth"] is False for value in values
            ),
            "repeated_miss_needs_owner_disposition": len(values) >= 3
            and all(value["target_at_most_one_eighth"] is False for value in values),
        }
    return summary
