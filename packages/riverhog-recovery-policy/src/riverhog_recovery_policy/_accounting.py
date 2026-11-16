"""Bounded in-memory accounting retained by one logical workflow owner."""

from __future__ import annotations

import math
import sys
from dataclasses import dataclass, replace
from fractions import Fraction
from threading import RLock

from ._policy import (
    CheckpointCandidate,
    CheckpointDecision,
    RecoveryAccounting,
    RecoveryPolicy,
    _count,
    _finite,
    _flag,
    decide_checkpoint,
)


def _scalar(value: Fraction, *, upward: bool) -> tuple[float, bool]:
    """Round charges up and credit down; saturation never makes cost spendable."""
    maximum = sys.float_info.max
    if value > Fraction(maximum):
        return maximum, True
    result = float(value)
    exact = Fraction(result)
    if upward and exact < value:
        result = math.nextafter(result, math.inf)
    elif not upward and exact > value:
        result = math.nextafter(result, -math.inf)
    return result, False


@dataclass(frozen=True, slots=True)
class CheckpointCharge:
    """Actual received optional cost, distinct from its admitted estimate."""

    reserved_seconds: float
    actual_seconds: float
    overrun: bool


class RecoveryBudget:
    """One owner's shared optional allowance; domains cannot create useful credit.

    Only the root owner records completed useful intervals on one monotonic
    execution clock. It excludes replay, optional work and waiting before
    reporting those intervals. Overlap is credited at most once; out-of-order
    intervals may conservatively lose credit. This is not CPU utilization.

    State remains in memory. The existing owner persists accounting() alongside
    its required anchors or checkpoint publication. Every restored summary needs
    explicit outcome/cost reconciliation, even if it records no held reservation:
    unrecorded attempts after that anchor may still have spent optional work.
    """

    def __init__(
        self,
        policy: RecoveryPolicy,
        *,
        active_checkpoint_limit: int,
        recovered: RecoveryAccounting | None = None,
    ) -> None:
        if not isinstance(policy, RecoveryPolicy):
            raise TypeError("workflow accounting requires its fixed recovery policy")
        _count(active_checkpoint_limit, "active_checkpoint_limit", positive=True)
        accounting = RecoveryAccounting() if recovered is None else recovered
        if not isinstance(accounting, RecoveryAccounting):
            raise TypeError("workflow accounting requires its owner summary")
        self._policy = policy
        self._limit = active_checkpoint_limit
        self._useful = Fraction(accounting.useful_elapsed_seconds)
        self._charged = Fraction(accounting.charged_optional_seconds)
        self._recovered_reservation = Fraction(accounting.reserved_optional_seconds)
        self._held = self._recovered_reservation
        self._successful = accounting.successful_checkpoints
        self._reconciled = recovered is None
        self._credit_cursor: float | None = None
        self._pending: set[CheckpointReservation] = set()
        self._lock = RLock()

    @property
    def policy(self) -> RecoveryPolicy:
        return self._policy

    def domain(self) -> RecoveryDomain:
        """Share this root without allocating another percentage or clock."""
        return RecoveryDomain(self)

    def record_completed_interval(
        self, start: float, finish: float, *, repeated_replay: bool = False
    ) -> None:
        """Credit only actual completed useful work selected by the root owner.

        Replayed work advances the clock watermark but earns no new credit.
        Gaps earn nothing. Child scopes have no corresponding credit method.
        No interval history is retained or scanned.
        """
        start = _finite(start, "start")
        finish = _finite(finish, "finish")
        _flag(repeated_replay, "repeated_replay")
        if finish < start:
            raise ValueError("completed work interval ends before it starts")
        with self._lock:
            cursor = start if self._credit_cursor is None else max(start, self._credit_cursor)
            if not repeated_replay and finish > cursor:
                self._useful += Fraction(finish) - Fraction(cursor)
            self._credit_cursor = (
                finish if self._credit_cursor is None else max(finish, self._credit_cursor)
            )

    def _accounting(self) -> RecoveryAccounting:
        useful, _ = _scalar(self._useful, upward=False)
        charged, charged_saturated = _scalar(self._charged, upward=True)
        reserved, reserved_saturated = _scalar(self._held, upward=True)
        return RecoveryAccounting(
            useful_elapsed_seconds=useful,
            charged_optional_seconds=charged,
            reserved_optional_seconds=reserved,
            successful_checkpoints=self._successful,
            reconciled=self._reconciled and not charged_saturated and not reserved_saturated,
        )

    def accounting(self) -> RecoveryAccounting:
        """Return a conservative bounded summary, with no I/O or checkpoint."""
        with self._lock:
            return self._accounting()

    @property
    def pending_reservations(self) -> tuple[CheckpointReservation, ...]:
        """Retain original unresolved grants, bounded by active_checkpoint_limit."""
        with self._lock:
            return tuple(self._pending)

    def reconcile_recovered(
        self, *, actual_seconds: float, additional_published_checkpoints: int
    ) -> None:
        """Receive the owner's known original outcomes since its saved anchor.

        The domain owner must establish actual costs, publication and receiving.
        This method proves none of them. Existing charged costs/counts survive;
        lost advisory credit and repeated replay are never restored as new work.
        """
        actual = _finite(actual_seconds, "actual_seconds")
        _count(additional_published_checkpoints, "additional_published_checkpoints")
        with self._lock:
            if self._reconciled or self._pending:
                raise RuntimeError("reconciliation requires an unresolved recovered root")
            self._charged += Fraction(actual)
            self._held -= self._recovered_reservation
            self._recovered_reservation = Fraction(0)
            self._successful += additional_published_checkpoints
            self._reconciled = True


class RecoveryDomain:
    """A child view of the same root budget, with one conflicting save at a time."""

    def __init__(self, owner: RecoveryBudget) -> None:
        if not isinstance(owner, RecoveryBudget):
            raise TypeError("recovery domain requires its original workflow budget")
        self._owner = owner
        self._pending: CheckpointReservation | None = None

    @property
    def policy(self) -> RecoveryPolicy:
        return self._owner.policy

    @property
    def pending_reservation(self) -> CheckpointReservation | None:
        with self._owner._lock:
            return self._pending

    def reserve(
        self, candidate: CheckpointCandidate
    ) -> tuple[CheckpointDecision, CheckpointReservation | None]:
        """Atomically decide and hold root allowance before optional effects."""
        if not isinstance(candidate, CheckpointCandidate):
            raise TypeError("checkpoint reservation requires an owner candidate")
        root = self._owner
        with root._lock:
            if self._pending is not None:
                candidate = replace(candidate, checkpoint_in_flight=True)
            if len(root._pending) >= root._limit:
                candidate = replace(candidate, storage_admitted=False)
            accounting = root._accounting()
            # Pending attempts reserve possible success slots without reporting
            # them as completed publications in the root summary.
            proposed = replace(
                accounting,
                successful_checkpoints=accounting.successful_checkpoints + len(root._pending),
            )
            decision = decide_checkpoint(root.policy, proposed, candidate)
            if not decision.admitted:
                return decision, None
            grant = CheckpointReservation(self, decision.reserved_seconds)
            root._pending.add(grant)
            root._held += Fraction(grant.reserved_seconds)
            self._pending = grant
            return decision, grant


class CheckpointReservation:
    """An original root charge; abandonment and cancellation never refund it.

    The domain retains this grant before starting optional construction. Receive
    after the owning operation establishes actual cost/outcome and receiving.
    A successful receipt here is accounting, not checkpoint validity or custody.
    """

    def __init__(self, domain: RecoveryDomain, reserved_seconds: float) -> None:
        if not isinstance(domain, RecoveryDomain):
            raise TypeError("checkpoint reservation requires its original recovery domain")
        self._domain = domain
        self._reserved = _finite(reserved_seconds, "reserved_seconds")
        self._received = False

    @property
    def reserved_seconds(self) -> float:
        return self._reserved

    @property
    def received(self) -> bool:
        with self._domain._owner._lock:
            return self._received

    def receive(self, *, actual_seconds: float, published: bool) -> CheckpointCharge:
        actual = _finite(actual_seconds, "actual_seconds")
        _flag(published, "published")
        root = self._domain._owner
        with root._lock:
            if self._received or self not in root._pending or self._domain._pending is not self:
                raise RuntimeError("only the original pending reservation can receive once")
            root._charged += Fraction(actual)
            root._held -= Fraction(self._reserved)
            root._successful += int(published)
            root._pending.remove(self)
            self._domain._pending = None
            self._received = True
            return CheckpointCharge(self._reserved, actual, actual > self._reserved)
