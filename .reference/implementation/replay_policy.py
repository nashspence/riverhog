"""Persistence-independent replay-policy proposal; no decision grants authority.

Owners supply admitted estimates and conservative accounting, allocate actual grants,
establish consistency/current authorization, publish generations, and receive resources.
All elapsed inputs are integer nanoseconds. Unknown estimates are represented by None.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TypedDict, cast

NANOSECONDS_PER_SECOND = 1_000_000_000


class _PolicyInput(TypedDict, total=False):
    recovery_mode: str
    replay_target_seconds: int | float
    max_optional_overhead_fraction: int | float
    max_optional_checkpoints: int | None
    retained_checkpoint_generations: int


def _integer(name: str, value: object, *, minimum: int = 0) -> None:
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer, not a boolean or coerced value")
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")


def _ratio(name: str, value: object) -> tuple[int, int]:
    if type(value) is int:
        return value, 1
    if type(value) is not float:
        raise TypeError(f"{name} must be an explicit int or float")
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite")
    return value.as_integer_ratio()


def _flag(name: str, value: object) -> None:
    if type(value) is not bool:
        raise TypeError(f"{name} must be a boolean")


def _estimate(name: str, value: object, provenance: object) -> None:
    if value is not None:
        _integer(name, value)
    if type(provenance) is not str or not provenance.strip():
        raise ValueError(f"{name} requires nonempty estimate/unknown provenance")


@dataclass(frozen=True, slots=True)
class Policy:
    recovery_mode: str = "adaptive"
    replay_target_seconds: int | float = 120
    max_optional_overhead_fraction: int | float = 0.125
    max_optional_checkpoints: int | None = None
    retained_checkpoint_generations: int = 2
    target_ns: int = field(init=False)
    fraction_numerator: int = field(init=False)
    fraction_complement: int = field(init=False)

    def __post_init__(self) -> None:
        if type(self.recovery_mode) is not str or self.recovery_mode not in {"adaptive", "restart"}:
            raise ValueError("recovery_mode must be adaptive or restart")
        numerator, denominator = _ratio("replay_target_seconds", self.replay_target_seconds)
        if numerator <= 0:
            raise ValueError("replay_target_seconds must be positive")
        # Ceil once, using exact rational arithmetic even for enormous finite floats.
        target_ns = (numerator * NANOSECONDS_PER_SECOND + denominator - 1) // denominator
        fraction, divisor = _ratio(
            "max_optional_overhead_fraction", self.max_optional_overhead_fraction
        )
        if not 0 <= fraction < divisor:
            raise ValueError("max_optional_overhead_fraction must satisfy 0 <= beta < 1")
        if self.max_optional_checkpoints is not None:
            _integer("max_optional_checkpoints", self.max_optional_checkpoints)
        _integer("retained_checkpoint_generations", self.retained_checkpoint_generations, minimum=1)
        object.__setattr__(self, "target_ns", target_ns)
        object.__setattr__(self, "fraction_numerator", fraction)
        object.__setattr__(self, "fraction_complement", divisor - fraction)

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> Policy:
        if not isinstance(value, Mapping):
            raise TypeError("policy must be a mapping")
        if any(type(key) is not str for key in value):
            raise TypeError("policy keys must be strings")
        names = {
            "recovery_mode",
            "replay_target_seconds",
            "max_optional_overhead_fraction",
            "max_optional_checkpoints",
            "retained_checkpoint_generations",
        }
        unknown = set(value) - names
        if unknown:
            raise ValueError(f"unknown policy keys: {sorted(unknown)}")
        # Construction checks every raw value before a Policy can escape.
        return cls(**cast(_PolicyInput, value))

    def to_mapping(self) -> dict[str, str | int | float | None]:
        return {
            "recovery_mode": self.recovery_mode,
            "replay_target_seconds": self.replay_target_seconds,
            "max_optional_overhead_fraction": self.max_optional_overhead_fraction,
            "max_optional_checkpoints": self.max_optional_checkpoints,
            "retained_checkpoint_generations": self.retained_checkpoint_generations,
        }

    def _affords(self, cost_ns: int, useful_ns: int) -> bool:
        """Exact C <= beta/(1-beta) * useful; no floating division or rounding."""
        return cost_ns * self.fraction_complement <= useful_ns * self.fraction_numerator


@dataclass(frozen=True, slots=True)
class Candidate:
    replay_ns: int | None
    restore_ns: int | None
    cost_ns: int | None
    remaining_ns: int | None
    replay_provenance: str
    restore_provenance: str
    cost_provenance: str
    remaining_provenance: str
    safe_consistent: bool
    storage_admitted: bool
    completion_ready: bool
    mandatory_anchor_ready: bool
    conflicting_domain_checkpoint: bool

    def __post_init__(self) -> None:
        for name in ("replay", "restore", "cost", "remaining"):
            _estimate(name, getattr(self, f"{name}_ns"), getattr(self, f"{name}_provenance"))
        for name in (
            "safe_consistent",
            "storage_admitted",
            "completion_ready",
            "mandatory_anchor_ready",
            "conflicting_domain_checkpoint",
        ):
            _flag(name, getattr(self, name))

    @property
    def net_saved_ns(self) -> int | None:
        if self.replay_ns is None or self.restore_ns is None:
            return None
        return max(0, self.replay_ns - self.restore_ns)


@dataclass(frozen=True, slots=True)
class Budget:
    useful_ns: int = 0
    charged_ns: int = 0
    reserved_ns: int = 0
    successful_checkpoints: int = 0
    held_count_slots: int = 0
    accounting_uncertain: bool = False

    def __post_init__(self) -> None:
        for name in (
            "useful_ns",
            "charged_ns",
            "reserved_ns",
            "successful_checkpoints",
            "held_count_slots",
        ):
            _integer(name, getattr(self, name))
        _flag("accounting_uncertain", self.accounting_uncertain)

    @property
    def optional_liability_ns(self) -> int:
        return self.charged_ns + self.reserved_ns


class Reason(StrEnum):
    COMPLETE = "complete"
    MANDATORY_ANCHOR = "mandatory_anchor"
    RESTART_MODE = "restart_mode"
    UNSAFE_CANDIDATE = "unsafe_candidate"
    COUNT_LIMIT = "count_limit"
    STORAGE_REFUSED = "storage_refused"
    DOMAIN_CONFLICT = "domain_conflict"
    UNKNOWN_COST = "unknown_cost"
    UNKNOWN_REPLAY = "unknown_replay"
    UNKNOWN_RESTORE = "unknown_restore"
    ACCOUNTING_UNCERTAIN = "accounting_uncertain"
    NEAR_COMPLETION = "near_completion"
    BELOW_TARGET = "below_target"
    CANDIDATE_OVERHEAD = "candidate_overhead"
    OVERRUN_DEBT = "overrun_debt"
    WORKFLOW_OVERHEAD = "workflow_overhead"
    ADMIT = "admit"


@dataclass(frozen=True, slots=True)
class ReservationProposal:
    cost_ns: int
    count_slots: int = 1

    def __post_init__(self) -> None:
        _integer("cost_ns", self.cost_ns)
        _integer("count_slots", self.count_slots, minimum=1)
        if self.count_slots != 1:
            raise ValueError("one candidate proposes exactly one optional generation slot")


@dataclass(frozen=True, slots=True)
class Decision:
    reason: Reason
    net_saved_ns: int | None
    reservation: ReservationProposal | None = None

    @property
    def should_checkpoint(self) -> bool:
        return self.reservation is not None


def decide(policy: Policy, candidate: Candidate, budget: Budget) -> Decision:
    """Return a bounded proposal; the workflow owner must allocate and fence it.

    Suppression priority is deterministic. Completion and mandatory work win first,
    then eligibility/known cost, near completion, other estimates/accounting, target, candidate
    efficiency, existing debt, and aggregate budget. The owner owns retention; it
    never deletes a required generation merely because presentation retention is met.
    """
    saved = candidate.net_saved_ns

    def suppressed(reason: Reason) -> Decision:
        return Decision(reason, saved)

    if candidate.completion_ready:
        return suppressed(Reason.COMPLETE)
    if candidate.mandatory_anchor_ready:
        return suppressed(Reason.MANDATORY_ANCHOR)
    if policy.recovery_mode == "restart":
        return suppressed(Reason.RESTART_MODE)
    if not candidate.safe_consistent:
        return suppressed(Reason.UNSAFE_CANDIDATE)
    limit = policy.max_optional_checkpoints
    if limit is not None and budget.successful_checkpoints + budget.held_count_slots >= limit:
        return suppressed(Reason.COUNT_LIMIT)
    if not candidate.storage_admitted:
        return suppressed(Reason.STORAGE_REFUSED)
    if candidate.conflicting_domain_checkpoint:
        return suppressed(Reason.DOMAIN_CONFLICT)
    if candidate.cost_ns is None:
        return suppressed(Reason.UNKNOWN_COST)
    cost = candidate.cost_ns
    if candidate.remaining_ns is not None and candidate.remaining_ns <= cost:
        return suppressed(Reason.NEAR_COMPLETION)
    if candidate.replay_ns is None:
        return suppressed(Reason.UNKNOWN_REPLAY)
    if candidate.restore_ns is None:
        return suppressed(Reason.UNKNOWN_RESTORE)
    if budget.accounting_uncertain:
        return suppressed(Reason.ACCOUNTING_UNCERTAIN)
    assert saved is not None
    if saved < policy.target_ns:
        return suppressed(Reason.BELOW_TARGET)
    if not policy._affords(cost, saved):
        return suppressed(Reason.CANDIDATE_OVERHEAD)
    if not policy._affords(budget.optional_liability_ns, budget.useful_ns):
        return suppressed(Reason.OVERRUN_DEBT)
    if not policy._affords(budget.optional_liability_ns + cost, budget.useful_ns):
        return suppressed(Reason.WORKFLOW_OVERHEAD)
    return Decision(Reason.ADMIT, saved, ReservationProposal(cost))
