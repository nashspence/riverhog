"""Decide optional recovery admission from an owner's bounded accounting.

The workflow owner supplies conservative elapsed-work and cost estimates.
Admission proposes a reservation; it neither persists a checkpoint nor grants
authority to adopt state, perform effects, or release resources.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, field, fields
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
    "unknown-restore-cost",
    "unknown-checkpoint-cost",
    "near-completion",
    "below-replay-target",
    "candidate-too-expensive",
    "overrun-debt",
    "workflow-budget-exhausted",
    "checkpoint-admitted",
]


def _ratio(value: object, name: str) -> tuple[int, int]:
    if type(value) is int:
        return value, 1
    if type(value) is float and math.isfinite(value):
        return value.as_integer_ratio()
    raise ValueError(f"{name} requires an explicit finite number")


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
    target_ns: int = field(init=False, repr=False)
    _fraction_numerator: int = field(init=False, repr=False)
    _fraction_complement: int = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if type(self.recovery_mode) is not str or self.recovery_mode not in ("adaptive", "restart"):
            raise ValueError("recovery_mode requires adaptive or restart")
        numerator, denominator = _ratio(self.replay_target_seconds, "replay_target_seconds")
        if numerator <= 0:
            raise ValueError("replay_target_seconds must be positive")
        fraction, divisor = _ratio(
            self.max_optional_overhead_fraction, "max_optional_overhead_fraction"
        )
        if not 0 <= fraction < divisor:
            raise ValueError("max_optional_overhead_fraction must be nonnegative and less than one")
        if self.max_optional_checkpoints is not None:
            _count(self.max_optional_checkpoints, "max_optional_checkpoints")
        _count(
            self.retained_checkpoint_generations, "retained_checkpoint_generations", positive=True
        )
        object.__setattr__(
            self, "target_ns", (numerator * 1_000_000_000 + denominator - 1) // denominator
        )
        object.__setattr__(self, "_fraction_numerator", fraction)
        object.__setattr__(self, "_fraction_complement", divisor - fraction)

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

    def _affords(self, cost_ns: int, useful_ns: int) -> bool:
        return cost_ns * self._fraction_complement <= useful_ns * self._fraction_numerator


@dataclass(frozen=True, slots=True, kw_only=True)
class RecoveryAccounting:
    """One logical workflow's credited work, actual costs and held reservations.

    Useful elapsed credit excludes replay, optional work and unrelated waiting;
    concurrent children do not each contribute their full overlapping duration.
    Failed candidates remain charged. An owner with uncertain crash accounting
    keeps reconciled false until its known obligations are conservatively received.
    Held count slots cover possible publications, separately from actual successes.
    """

    useful_ns: int = 0
    charged_ns: int = 0
    reserved_ns: int = 0
    successful_checkpoints: int = 0
    held_count_slots: int = 0
    reconciled: bool = True

    def __post_init__(self) -> None:
        for name in (
            "useful_ns",
            "charged_ns",
            "reserved_ns",
            "successful_checkpoints",
            "held_count_slots",
        ):
            _count(getattr(self, name), name)
        _flag(self.reconciled, "reconciled")


@dataclass(frozen=True, slots=True, kw_only=True)
class CheckpointCandidate:
    """An owner's consistent candidate, with explicit nanosecond estimate provenance.

    replay_ns is gross useful replay avoided; restore_ns includes candidate load
    and revalidation. cost_ns covers all incremental creation/publication/lifecycle
    work. None means unknown, including its required nonempty provenance label.
    Estimates are rounded by their owner: costs upward, useful credit downward.
    """

    replay_ns: int | None
    restore_ns: int | None
    cost_ns: int | None
    remaining_ns: int | None
    replay_provenance: str
    restore_provenance: str
    cost_provenance: str
    remaining_provenance: str
    consistent: bool = True
    storage_admitted: bool = True
    checkpoint_in_flight: bool = False
    completion_ready: bool = False
    mandatory_anchor_ready: bool = False

    def __post_init__(self) -> None:
        for name in ("replay", "restore", "cost", "remaining"):
            value = getattr(self, f"{name}_ns")
            if value is not None:
                _count(value, f"{name}_ns")
            provenance = getattr(self, f"{name}_provenance")
            if type(provenance) is not str or not provenance.strip():
                raise ValueError(f"{name} requires nonempty estimate or unknown provenance")
        for name in (
            "consistent",
            "storage_admitted",
            "checkpoint_in_flight",
            "completion_ready",
            "mandatory_anchor_ready",
        ):
            _flag(getattr(self, name), name)

    @property
    def net_saved_ns(self) -> int | None:
        if self.replay_ns is None or self.restore_ns is None:
            return None
        return net_saved_replay_ns(
            restart_from_frontier_ns=self.replay_ns,
            restart_from_candidate_ns=self.restore_ns,
        )


@dataclass(frozen=True, slots=True)
class CheckpointDecision:
    """A bounded reason and proposed reservation for the current owner snapshot."""

    reason: DecisionReason
    reserved_ns: int = 0
    net_saved_ns: int | None = None

    def __post_init__(self) -> None:
        if type(self.reason) is not str or self.reason not in get_args(DecisionReason):
            raise ValueError("checkpoint decision requires a known admission reason")
        _count(self.reserved_ns, "reserved_ns")
        if self.net_saved_ns is not None:
            _count(self.net_saved_ns, "net_saved_ns")
        if self.reason != "checkpoint-admitted" and self.reserved_ns != 0:
            raise ValueError("a suppressed candidate cannot reserve optional cost")

    @property
    def admitted(self) -> bool:
        return self.reason == "checkpoint-admitted"


def net_saved_replay_ns(*, restart_from_frontier_ns: int, restart_from_candidate_ns: int) -> int:
    """Subtract candidate load/revalidation/replay without granting negative credit."""
    frontier = _count(restart_from_frontier_ns, "restart_from_frontier_ns")
    candidate = _count(restart_from_candidate_ns, "restart_from_candidate_ns")
    return max(0, frontier - candidate)


def decide_checkpoint(
    policy: RecoveryPolicy,
    accounting: RecoveryAccounting,
    candidate: CheckpointCandidate,
) -> CheckpointDecision:
    """Require all three exact integer inequalities before proposing optional work.

    This arithmetic prevents scalar overflow and roundoff from creating credit.
    Estimates and physical elapsed-time ratios remain observations, not guarantees.
    """
    if not isinstance(policy, RecoveryPolicy):
        raise TypeError("checkpoint admission requires its recovery policy")
    if not isinstance(accounting, RecoveryAccounting):
        raise TypeError("checkpoint admission requires its logical workflow accounting")
    if not isinstance(candidate, CheckpointCandidate):
        raise TypeError("checkpoint admission requires its owner candidate")
    saved = candidate.net_saved_ns

    def suppressed(reason: DecisionReason) -> CheckpointDecision:
        return CheckpointDecision(reason, net_saved_ns=saved)

    if candidate.completion_ready:
        return suppressed("completion-ready")
    if candidate.mandatory_anchor_ready:
        return suppressed("mandatory-anchor-ready")
    if policy.recovery_mode == "restart":
        return suppressed("restart-mode")
    if not candidate.consistent:
        return suppressed("inconsistent-candidate")
    if (
        policy.max_optional_checkpoints is not None
        and accounting.successful_checkpoints + accounting.held_count_slots
        >= policy.max_optional_checkpoints
    ):
        return suppressed("count-limit")
    if not candidate.storage_admitted:
        return suppressed("storage-not-admitted")
    if candidate.checkpoint_in_flight:
        return suppressed("checkpoint-in-flight")
    cost = candidate.cost_ns
    if cost is None:
        return suppressed("unknown-checkpoint-cost")
    if candidate.remaining_ns is not None and candidate.remaining_ns <= cost:
        return suppressed("near-completion")
    if candidate.replay_ns is None:
        return suppressed("unknown-saved-replay")
    if candidate.restore_ns is None:
        return suppressed("unknown-restore-cost")
    if not accounting.reconciled:
        return suppressed("unreconciled-accounting")
    assert saved is not None
    if saved < policy.target_ns:
        return suppressed("below-replay-target")
    if not policy._affords(cost, saved):
        return suppressed("candidate-too-expensive")
    obligation = accounting.charged_ns + accounting.reserved_ns
    if not policy._affords(obligation, accounting.useful_ns):
        return suppressed("overrun-debt")
    if not policy._affords(obligation + cost, accounting.useful_ns):
        return suppressed("workflow-budget-exhausted")
    return CheckpointDecision("checkpoint-admitted", cost, saved)


__all__ = [
    "RecoveryMode",
    "DecisionReason",
    "RecoveryPolicy",
    "RecoveryAccounting",
    "CheckpointCandidate",
    "CheckpointDecision",
    "net_saved_replay_ns",
    "decide_checkpoint",
]
