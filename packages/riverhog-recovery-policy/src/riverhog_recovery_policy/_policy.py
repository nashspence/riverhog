"""Decide optional recovery admission from an owner's bounded accounting.

The workflow owner supplies conservative elapsed-work and cost estimates.
Admission proposes a reservation; it neither persists a checkpoint nor grants
authority to adopt state, perform effects, or release resources.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, fields
from fractions import Fraction
from typing import Literal, Self, TypedDict, cast, get_args

RecoveryMode = Literal["adaptive", "restart"]
DecisionReason = Literal[
    "completion-ready",
    "mandatory-anchor-ready",
    "restart-mode",
    "inconsistent-candidate",
    "storage-not-admitted",
    "checkpoint-in-flight",
    "count-limit",
    "unreconciled-accounting",
    "unknown-saved-replay",
    "unknown-checkpoint-cost",
    "near-completion",
    "below-replay-target",
    "candidate-too-expensive",
    "workflow-budget-exhausted",
    "checkpoint-admitted",
]


def _finite(value: object, name: str, *, nonnegative: bool = True) -> float:
    if type(value) is not int and type(value) is not float:
        raise ValueError(f"{name} requires a finite number")
    try:
        result = float(value)
    except OverflowError as exc:
        raise ValueError(f"{name} requires a finite number") from exc
    if not math.isfinite(result) or nonnegative and result < 0:
        raise ValueError(f"{name} requires a finite {'nonnegative ' if nonnegative else ''}number")
    return result


def _count(value: object, name: str, *, positive: bool = False) -> int:
    if type(value) is not int or value < (1 if positive else 0):
        raise ValueError(f"{name} requires a {'positive' if positive else 'nonnegative'} integer")
    return value


def _flag(value: object, name: str) -> None:
    if type(value) is not bool:
        raise ValueError(f"{name} requires a boolean")


class _PolicyInput(TypedDict, total=False):
    recovery_mode: RecoveryMode
    replay_target_seconds: float
    max_optional_overhead_fraction: float
    max_optional_checkpoints: int | None
    retained_checkpoint_generations: int


@dataclass(frozen=True, slots=True, kw_only=True)
class RecoveryPolicy:
    """Optional recovery settings, fixed by an execution's configuration owner."""

    recovery_mode: RecoveryMode = "adaptive"
    replay_target_seconds: float = 120.0
    max_optional_overhead_fraction: float = 0.125
    max_optional_checkpoints: int | None = None
    retained_checkpoint_generations: int = 2

    def __post_init__(self) -> None:
        if type(self.recovery_mode) is not str or self.recovery_mode not in ("adaptive", "restart"):
            raise ValueError("recovery_mode requires adaptive or restart")
        target = _finite(self.replay_target_seconds, "replay_target_seconds")
        if target <= 0:
            raise ValueError("replay_target_seconds must be positive")
        fraction = _finite(self.max_optional_overhead_fraction, "max_optional_overhead_fraction")
        if fraction >= 1:
            raise ValueError("max_optional_overhead_fraction must be less than one")
        if self.max_optional_checkpoints is not None:
            _count(self.max_optional_checkpoints, "max_optional_checkpoints")
        _count(
            self.retained_checkpoint_generations, "retained_checkpoint_generations", positive=True
        )
        object.__setattr__(self, "replay_target_seconds", target)
        object.__setattr__(self, "max_optional_overhead_fraction", fraction)

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> Self:
        """Validate the original operator values before any parser coercion."""
        if not isinstance(value, Mapping):
            raise ValueError("recovery policy requires a mapping")
        if any(type(key) is not str for key in value):
            raise ValueError("recovery policy keys must be strings")
        names = {item.name for item in fields(cls) if item.init}
        unknown = set(value) - names
        if unknown:
            raise ValueError(f"unknown recovery policy fields: {sorted(unknown)}")
        return cls(**cast(_PolicyInput, dict(value)))

    def to_mapping(self) -> dict[str, str | float | int | None]:
        """Return only the effective operational policy, without runtime authority."""
        return {
            "recovery_mode": self.recovery_mode,
            "replay_target_seconds": self.replay_target_seconds,
            "max_optional_overhead_fraction": self.max_optional_overhead_fraction,
            "max_optional_checkpoints": self.max_optional_checkpoints,
            "retained_checkpoint_generations": self.retained_checkpoint_generations,
        }


@dataclass(frozen=True, slots=True, kw_only=True)
class RecoveryAccounting:
    """One logical workflow's credited work, actual costs and held reservations.

    Useful elapsed credit excludes replay, optional work and unrelated waiting;
    concurrent children do not each contribute their full overlapping duration.
    Failed candidates remain charged. An owner with uncertain crash accounting
    keeps reconciled false until its known obligations are conservatively received.
    """

    useful_elapsed_seconds: float = 0.0
    charged_optional_seconds: float = 0.0
    reserved_optional_seconds: float = 0.0
    successful_checkpoints: int = 0
    reconciled: bool = True

    def __post_init__(self) -> None:
        for name in (
            "useful_elapsed_seconds",
            "charged_optional_seconds",
            "reserved_optional_seconds",
        ):
            object.__setattr__(self, name, _finite(getattr(self, name), name))
        _count(self.successful_checkpoints, "successful_checkpoints")
        _flag(self.reconciled, "reconciled")


@dataclass(frozen=True, slots=True, kw_only=True)
class CheckpointCandidate:
    """A consistent owner's recovery proposal with finite, provenance-owned estimates.

    saved_replay_seconds is net useful recomputation saved after candidate load
    and revalidation. None means unknown. checkpoint_cost_seconds includes every
    incremental creation/publication/lifecycle cost, including a genuine zero.
    The owner retains the estimate's source and observation conditions.
    """

    saved_replay_seconds: float | None
    checkpoint_cost_seconds: float | None
    remaining_useful_seconds: float | None = None
    consistent: bool = True
    storage_admitted: bool = True
    checkpoint_in_flight: bool = False
    completion_ready: bool = False
    mandatory_anchor_ready: bool = False

    def __post_init__(self) -> None:
        for name in ("saved_replay_seconds", "checkpoint_cost_seconds", "remaining_useful_seconds"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(
                    self, name, _finite(value, name, nonnegative=name != "saved_replay_seconds")
                )
        for name in (
            "consistent",
            "storage_admitted",
            "checkpoint_in_flight",
            "completion_ready",
            "mandatory_anchor_ready",
        ):
            _flag(getattr(self, name), name)


@dataclass(frozen=True, slots=True)
class CheckpointDecision:
    """A bounded reason and proposed cost reservation for the current owner snapshot."""

    reason: DecisionReason
    reserved_seconds: float = 0.0

    def __post_init__(self) -> None:
        if type(self.reason) is not str or self.reason not in get_args(DecisionReason):
            raise ValueError("checkpoint decision requires a known admission reason")
        reservation = _finite(self.reserved_seconds, "reserved_seconds")
        if self.reason != "checkpoint-admitted" and reservation != 0:
            raise ValueError("a suppressed candidate cannot reserve optional cost")
        object.__setattr__(self, "reserved_seconds", reservation)

    @property
    def admitted(self) -> bool:
        return self.reason == "checkpoint-admitted"


def net_saved_replay_seconds(
    *, restart_from_frontier_seconds: float, restart_from_candidate_seconds: float
) -> float:
    """Subtract candidate load/revalidation/replay from the existing restart cost."""

    frontier = _finite(restart_from_frontier_seconds, "restart_from_frontier_seconds")
    candidate = _finite(restart_from_candidate_seconds, "restart_from_candidate_seconds")
    return frontier - candidate


def decide_checkpoint(
    policy: RecoveryPolicy,
    accounting: RecoveryAccounting,
    candidate: CheckpointCandidate,
) -> CheckpointDecision:
    """Require all three admission inequalities before proposing optional work.

    Exact arithmetic over the admitted finite scalar estimates prevents overflow
    or rounding from creating recovery credits. It does not make the estimates
    exact observations or promise a physical elapsed-time ratio.
    """

    if not isinstance(policy, RecoveryPolicy):
        raise TypeError("checkpoint admission requires its recovery policy")
    if not isinstance(accounting, RecoveryAccounting):
        raise TypeError("checkpoint admission requires its logical workflow accounting")
    if not isinstance(candidate, CheckpointCandidate):
        raise TypeError("checkpoint admission requires its owner candidate")
    if candidate.completion_ready:
        return CheckpointDecision("completion-ready")
    if candidate.mandatory_anchor_ready:
        return CheckpointDecision("mandatory-anchor-ready")
    if policy.recovery_mode == "restart":
        return CheckpointDecision("restart-mode")
    if not candidate.consistent:
        return CheckpointDecision("inconsistent-candidate")
    if not candidate.storage_admitted:
        return CheckpointDecision("storage-not-admitted")
    if candidate.checkpoint_in_flight:
        return CheckpointDecision("checkpoint-in-flight")
    if (
        policy.max_optional_checkpoints is not None
        and accounting.successful_checkpoints >= policy.max_optional_checkpoints
    ):
        return CheckpointDecision("count-limit")
    if not accounting.reconciled:
        return CheckpointDecision("unreconciled-accounting")
    saved = candidate.saved_replay_seconds
    cost = candidate.checkpoint_cost_seconds
    if saved is None:
        return CheckpointDecision("unknown-saved-replay")
    if cost is None:
        return CheckpointDecision("unknown-checkpoint-cost")
    if (
        candidate.remaining_useful_seconds is not None
        and candidate.remaining_useful_seconds <= cost
    ):
        return CheckpointDecision("near-completion")
    if saved < policy.replay_target_seconds:
        return CheckpointDecision("below-replay-target")
    beta = Fraction(policy.max_optional_overhead_fraction)
    complement = 1 - beta
    if Fraction(cost) * complement > beta * Fraction(saved):
        return CheckpointDecision("candidate-too-expensive")
    obligation = (
        Fraction(accounting.charged_optional_seconds)
        + Fraction(accounting.reserved_optional_seconds)
        + Fraction(cost)
    )
    if obligation * complement > beta * Fraction(accounting.useful_elapsed_seconds):
        return CheckpointDecision("workflow-budget-exhausted")
    return CheckpointDecision("checkpoint-admitted", cost)


__all__ = [
    "RecoveryMode",
    "DecisionReason",
    "RecoveryPolicy",
    "RecoveryAccounting",
    "CheckpointCandidate",
    "CheckpointDecision",
    "net_saved_replay_seconds",
    "decide_checkpoint",
]
