"""Bounded in-memory accounting retained by one logical workflow owner."""

from __future__ import annotations

from dataclasses import dataclass, replace
from threading import RLock

from ._policy import (
    CheckpointCandidate,
    CheckpointDecision,
    RecoveryAccounting,
    RecoveryPolicy,
    _count,
    _flag,
    decide_checkpoint,
)


@dataclass(frozen=True, slots=True)
class CheckpointCharge:
    """Actual received optional cost, distinct from its admitted estimate."""

    reserved_ns: int
    actual_ns: int
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
        self._useful = accounting.useful_ns
        self._charged = accounting.charged_ns
        self._recovered_reservation = accounting.reserved_ns
        self._held = self._recovered_reservation
        self._recovered_slots = accounting.held_count_slots
        self._held_slots = accounting.held_count_slots
        self._successful = accounting.successful_checkpoints
        self._reconciled = recovered is None
        self._credit_cursor: int | None = None
        self._pending: set[CheckpointReservation] = set()
        self._lock = RLock()

    @property
    def policy(self) -> RecoveryPolicy:
        return self._policy

    def domain(self) -> RecoveryDomain:
        """Share this root without allocating another percentage or clock."""
        return RecoveryDomain(self)

    def record_completed_interval(
        self, start_ns: int, finish_ns: int, *, repeated_replay: bool = False
    ) -> None:
        """Credit only actual completed useful work selected by the root owner.

        Replayed work advances the clock watermark but earns no new credit.
        Gaps earn nothing. Child scopes have no corresponding credit method.
        No interval history is retained or scanned.
        """
        start = _count(start_ns, "start_ns")
        finish = _count(finish_ns, "finish_ns")
        _flag(repeated_replay, "repeated_replay")
        if finish < start:
            raise ValueError("completed work interval ends before it starts")
        with self._lock:
            cursor = start if self._credit_cursor is None else max(start, self._credit_cursor)
            if not repeated_replay and finish > cursor:
                self._useful += finish - cursor
            self._credit_cursor = (
                finish if self._credit_cursor is None else max(finish, self._credit_cursor)
            )

    def _accounting(self) -> RecoveryAccounting:
        return RecoveryAccounting(
            useful_ns=self._useful,
            charged_ns=self._charged,
            reserved_ns=self._held,
            successful_checkpoints=self._successful,
            held_count_slots=self._held_slots,
            reconciled=self._reconciled,
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

    def reconcile_recovered(self, *, actual_ns: int, additional_published_checkpoints: int) -> None:
        """Receive the owner's known original outcomes since its saved anchor.

        The domain owner must establish actual costs, publication and receiving.
        This method proves none of them. Existing charged costs/counts survive;
        lost advisory credit and repeated replay are never restored as new work.
        """
        actual = _count(actual_ns, "actual_ns")
        _count(additional_published_checkpoints, "additional_published_checkpoints")
        with self._lock:
            if self._reconciled or self._pending:
                raise RuntimeError("reconciliation requires an unresolved recovered root")
            self._charged += actual
            self._held -= self._recovered_reservation
            self._recovered_reservation = 0
            self._held_slots -= self._recovered_slots
            self._recovered_slots = 0
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
            decision = decide_checkpoint(root.policy, accounting, candidate)
            if not decision.admitted:
                return decision, None
            grant = CheckpointReservation(self, decision.reserved_ns)
            root._pending.add(grant)
            root._held += grant.reserved_ns
            root._held_slots += 1
            self._pending = grant
            return decision, grant


class CheckpointReservation:
    """An original root charge with separate selection and cleanup receiving.

    The owner supplies positive original selection/cost/receiving evidence.
    These methods account for that evidence; they do not establish authority,
    persist a selector, validate a generation or prove resource quiescence.
    """

    def __init__(self, domain: RecoveryDomain, reserved_ns: int) -> None:
        if not isinstance(domain, RecoveryDomain):
            raise TypeError("checkpoint reservation requires its original recovery domain")
        self._domain = domain
        self._reserved = _count(reserved_ns, "reserved_ns")
        self._remaining = self._reserved
        self._charged = 0
        self._generation: str | None = None
        self._received = False
        self._final: tuple[int, bool, CheckpointCharge] | None = None

    @property
    def reserved_ns(self) -> int:
        return self._reserved

    @property
    def received(self) -> bool:
        with self._domain._owner._lock:
            return self._received

    @property
    def selected_generation(self) -> str | None:
        with self._domain._owner._lock:
            return self._generation

    def _require_pending(self) -> None:
        root = self._domain._owner
        if self._received or self not in root._pending or self._domain._pending is not self:
            raise RuntimeError("only the original pending reservation can receive")

    def select_generation(self, generation: str) -> None:
        """Count an owner-confirmed selection once, retaining its unreceived tail."""
        if type(generation) is not str or not generation.strip():
            raise ValueError("checkpoint selection requires its exact generation identity")
        root = self._domain._owner
        with root._lock:
            self._require_pending()
            if self._generation is not None:
                if self._generation != generation:
                    raise ValueError("a grant cannot select another generation")
                return
            self._generation = generation
            root._successful += 1
            root._held_slots -= 1

    def _record_cost(self, total_actual_ns: int, remaining_reserved_ns: int) -> None:
        if total_actual_ns < self._charged:
            raise ValueError("original actual cost cannot decrease")
        root = self._domain._owner
        root._charged += total_actual_ns - self._charged
        root._held += remaining_reserved_ns - self._remaining
        self._charged = total_actual_ns
        self._remaining = remaining_reserved_ns

    def record_cost(self, *, total_actual_ns: int, remaining_reserved_ns: int) -> None:
        """Replace cumulative actuals and the owner's complete unreceived cost tail."""
        _count(total_actual_ns, "total_actual_ns")
        _count(remaining_reserved_ns, "remaining_reserved_ns")
        with self._domain._owner._lock:
            self._require_pending()
            self._record_cost(total_actual_ns, remaining_reserved_ns)

    def receive(
        self, *, total_actual_ns: int, nonpublication_proven: bool = False
    ) -> CheckpointCharge:
        """Receive the original's whole lifecycle cost and actual cleanup.

        A failed attempt needs known nonpublication as well as complete receiving.
        A known selection cannot be retracted. Matching final receipts may retry
        a lost reply through this original; a lookalike has no receiving authority.
        """
        _count(total_actual_ns, "total_actual_ns")
        _flag(nonpublication_proven, "nonpublication_proven")
        root = self._domain._owner
        with root._lock:
            if self._final is not None:
                if self._final[:2] != (total_actual_ns, nonpublication_proven):
                    raise ValueError("original complete receiving evidence changed")
                return self._final[2]
            self._require_pending()
            if (self._generation is None) != nonpublication_proven:
                raise ValueError("receiving requires exact selection or known nonpublication")
            self._record_cost(total_actual_ns, 0)
            if self._generation is None:
                root._held_slots -= 1
            root._pending.remove(self)
            self._domain._pending = None
            self._received = True
            charge = CheckpointCharge(
                self._reserved, total_actual_ns, total_actual_ns > self._reserved
            )
            self._final = total_actual_ns, nonpublication_proven, charge
            return charge
