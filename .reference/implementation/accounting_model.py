"""Single-writer reference simulation, NOT production storage or authorization.

Each _cas represents one summary publication at an existing durable boundary;
useful-time observations remain volatile between those boundaries. The real owner
must read the latest ArchiveWriteControlRecord, prove receipts/currentness, preserve
it after upload deletion, and charge CAS I/O including failed attempts. No model
method performs I/O, receives a worker, or proves a generation durable. Restore
accepts only a current authoritative root summary, never checkpoint-local counters.
The domain Job-owned resumable selector is outside this accounting model; removing
a fully received grant does not delete that selector or its usable generation.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Literal, TypedDict, Unpack

from replay_policy import Budget, Candidate, Decision, Policy, decide


def _nonnegative(name: str, value: int) -> None:
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer")


class StaleRevision(ValueError):
    pass


class OwnerCapacity(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class Grant:
    identity: str
    domain: str
    reserved_ns: int
    charged_ns: int = 0
    generation: str | None = None
    uncertain: bool = False
    nonpublication_proven: bool = False


@dataclass(frozen=True, slots=True)
class RootSummary:
    identity: str
    policy: Policy
    max_active: int
    revision: int = 0
    useful_ns: int = 0
    charged_ns: int = 0
    successes: int = 0
    issued_grants: int = 0
    grants: tuple[Grant, ...] = ()
    unreserved_uncertain: bool = False
    finished: bool = False
    cost_epoch_open: bool = True


class _RootChanges(TypedDict, total=False):
    charged_ns: int
    successes: int
    issued_grants: int
    grants: tuple[Grant, ...]
    unreserved_uncertain: bool
    finished: bool
    cost_epoch_open: bool


@dataclass(slots=True)
class Subgrant:
    """One consumable subdivision of ONE candidate's cost, not another checkpoint.

    Children have no useful credit, policy, count slot or publication authority.
    Unallocated/unused amounts remain charged to the original root reservation.
    """

    root_grant: str
    remaining_ns: int
    max_children: int
    _split: bool = False

    def split(self, *amounts_ns: int) -> tuple[Subgrant, ...]:
        if self._split or not amounts_ns or len(amounts_ns) > self.max_children:
            raise OwnerCapacity("subdivision already consumed or owner fanout unavailable")
        for amount in amounts_ns:
            _nonnegative("subgrant", amount)
        if sum(amounts_ns) > self.remaining_ns:
            raise OwnerCapacity("children exceed the disjoint parent envelope")
        self._split = True
        self.remaining_ns -= sum(amounts_ns)
        return tuple(Subgrant(self.root_grant, amount, self.max_children) for amount in amounts_ns)


class Root:
    def __init__(self, identity: str, policy: Policy, *, max_active: int) -> None:
        _nonnegative("max_active", max_active)
        if not identity or max_active == 0:
            raise ValueError("root identity and positive existing-owner capacity required")
        self._state = RootSummary(identity, policy, max_active)
        self._live_useful_ns = 0
        self._fresh_credit_enabled = True
        self._subdivided: set[str] = set()

    @classmethod
    def restore(cls, summary: RootSummary, *, authoritative_revision: int) -> Root:
        """Restore after interruption from the current durable root read.

        The open epoch was recorded at mandatory construction admission. It covers
        even failed pre-grant CAS I/O whose actual cost did not reach a summary.
        """
        if summary.revision != authoritative_revision:
            raise StaleRevision("older checkpoint/accounting summary is not the current root")
        root = cls(summary.identity, summary.policy, max_active=summary.max_active)
        root._state = replace(
            summary,
            unreserved_uncertain=summary.unreserved_uncertain or summary.cost_epoch_open,
            grants=tuple(replace(grant, uncertain=True) for grant in summary.grants),
        )
        root._live_useful_ns = summary.useful_ns
        root._fresh_credit_enabled = False
        root._subdivided = {grant.identity for grant in summary.grants}
        return root

    @property
    def summary(self) -> RootSummary:
        return self._state

    @property
    def budget(self) -> Budget:
        state = self._state
        return Budget(
            useful_ns=self._live_useful_ns,
            charged_ns=state.charged_ns,
            reserved_ns=sum(grant.reserved_ns for grant in state.grants),
            successful_checkpoints=state.successes,
            held_count_slots=sum(grant.generation is None for grant in state.grants),
            accounting_uncertain=state.unreserved_uncertain
            or any(grant.uncertain for grant in state.grants),
        )

    def _cas(self, revision: int, **changes: Unpack[_RootChanges]) -> None:
        if revision != self._state.revision:
            raise StaleRevision("root changed; reread and reevaluate before optional work")
        self._state = replace(
            self._state, revision=revision + 1, useful_ns=self._live_useful_ns, **changes
        )
        self._subdivided.intersection_update(grant.identity for grant in self._state.grants)

    def _lookup_grant(self, identity: str) -> Grant | None:
        prefix = f"{self._state.identity}:"
        if type(identity) is not str or not identity.startswith(prefix):
            raise ValueError("grant belongs to another root")
        sequence = identity[len(prefix) :]
        if not sequence.isdecimal() or str(int(sequence)) != sequence:
            raise ValueError("grant sequence is not canonical")
        if not 1 <= int(sequence) <= self._state.issued_grants:
            raise ValueError("grant was never allocated")
        return next((grant for grant in self._state.grants if grant.identity == identity), None)

    def _grant(self, identity: str) -> Grant:
        grant = self._lookup_grant(identity)
        if grant is None:
            raise ValueError("grant already received; inspect the domain frontier/receipt")
        return grant

    def _replace_grant(self, grant: Grant) -> tuple[Grant, ...]:
        return tuple(grant if old.identity == grant.identity else old for old in self._state.grants)

    def credit_useful(self, revision: int, total_ns: int) -> None:
        """Observe root cumulative credit in memory; never write on a semantic turn."""
        _nonnegative("total_ns", total_ns)
        if revision != self._state.revision:
            raise StaleRevision("credit observation belongs to an obsolete root")
        if total_ns < self._live_useful_ns:
            raise ValueError("credit cannot roll back on retry/phase/restart")
        if total_ns > self._live_useful_ns and not self._fresh_credit_enabled:
            raise ValueError("replay versus fresh frontier must be established after restart")
        self._live_useful_ns = total_ns

    def resume_fresh_credit(self, *, current_binding_and_frontier_proven: bool) -> None:
        """Caller proves current authority and excludes the replayed work frontier."""
        if current_binding_and_frontier_proven is not True:
            raise ValueError("fresh work cannot be inferred from a resumed process")
        self._fresh_credit_enabled = True

    def subdivision(self, identity: str) -> Subgrant:
        grant = self._grant(identity)
        if identity in self._subdivided or grant.uncertain or grant.nonpublication_proven:
            raise OwnerCapacity("original subdivision is consumed, lost or uncertain")
        self._subdivided.add(identity)
        return Subgrant(identity, grant.reserved_ns, self._state.max_active)

    def reserve(
        self, revision: int, domain: str, candidate: Candidate
    ) -> tuple[Decision, Grant | None]:
        if revision != self._state.revision:
            raise StaleRevision("proposal used an obsolete root")
        if not domain:
            raise ValueError("domain required")
        candidate = replace(
            candidate,
            completion_ready=candidate.completion_ready or self._state.finished,
            conflicting_domain_checkpoint=candidate.conflicting_domain_checkpoint
            or any(grant.domain == domain for grant in self._state.grants),
        )
        decision = decide(self._state.policy, candidate, self.budget)
        if decision.reservation is None:
            return decision, None
        if len(self._state.grants) >= self._state.max_active:
            raise OwnerCapacity("existing owner grant slots are full")
        sequence = self._state.issued_grants + 1
        grant = Grant(f"{self._state.identity}:{sequence}", domain, decision.reservation.cost_ns)
        self._cas(revision, grants=(*self._state.grants, grant), issued_grants=sequence)
        return decision, grant

    def publish(self, revision: int, identity: str, generation: str) -> None:
        """Simulate durable selection after the owner proved consistency/fencing."""
        if revision != self._state.revision:
            raise StaleRevision("selection used an obsolete root")
        grant = self._grant(identity)
        if grant.nonpublication_proven:
            raise ValueError("received nonpublication cannot later publish")
        if not generation:
            raise ValueError("complete generation identity required")
        if grant.generation is not None:
            if grant.generation != generation:
                raise ValueError("grant cannot publish another generation")
            return  # Lost selection reply: no second count or second cost reservation.
        if self._state.finished:
            raise ValueError("completed construction cannot select a new optional generation")
        self._cas(
            revision,
            grants=self._replace_grant(replace(grant, generation=generation)),
            successes=self._state.successes + 1,
        )

    def receive_cost(
        self,
        revision: int,
        identity: str,
        *,
        total_actual_ns: int,
        remaining_reserved_ns: int,
        complete: bool,
        nonpublication_proven: bool = False,
        reconciled: bool = False,
    ) -> Literal["updated", "received", "already_received"]:
        """Actuals are cumulative; remaining reservation covers unreceived tail.

        Boolean proofs stand for domain-owned receiving/reconciliation in this
        simulation. They do not implement or confer real-world authority.
        A final immutable original receipt may be retried after lost acknowledgement.
        Allocated-but-absent means already received, without retaining tombstones;
        the owner rejects contradictory fresh costs before calling this method.
        """
        for name, value in (("actual", total_actual_ns), ("remaining", remaining_reserved_ns)):
            _nonnegative(name, value)
        if any(type(flag) is not bool for flag in (complete, nonpublication_proven, reconciled)):
            raise ValueError("receipt flags must be booleans")
        grant = self._lookup_grant(identity)
        if grant is None:
            if not complete or remaining_reserved_ns:
                raise ValueError("received grant cannot resume partial cost reporting")
            return "already_received"
        nonpublication_proven = nonpublication_proven or grant.nonpublication_proven
        if total_actual_ns < grant.charged_ns:
            raise ValueError("actual cost cannot disappear")
        if complete and remaining_reserved_ns:
            raise ValueError("complete receiving cannot retain an unreceived cost tail")
        if complete and grant.generation is None and not nonpublication_proven:
            raise ValueError("failed attempt requires proof of nonpublication")
        if nonpublication_proven and grant.generation is not None:
            raise ValueError("published generation cannot become nonpublication")
        if grant.uncertain and not reconciled:
            raise ValueError("unknown original requires exact reconciliation")
        if grant.uncertain and grant.generation is None and not nonpublication_proven:
            raise ValueError("known cost alone cannot resolve unknown publication")
        updated = replace(
            grant,
            charged_ns=total_actual_ns,
            reserved_ns=remaining_reserved_ns,
            uncertain=False,
            nonpublication_proven=nonpublication_proven,
        )
        grants = (
            tuple(old for old in self._state.grants if old.identity != identity)
            if complete
            else self._replace_grant(updated)
        )
        self._cas(
            revision,
            grants=grants,
            charged_ns=self._state.charged_ns + total_actual_ns - grant.charged_ns,
        )
        return "received" if complete else "updated"

    def mark_unknown(self, revision: int, identity: str) -> None:
        grant = self._grant(identity)
        self._cas(revision, grants=self._replace_grant(replace(grant, uncertain=True)))

    def charge_unreserved_failure(self, revision: int, actual_ns: int | None) -> None:
        """An admission CAS can consume optional I/O before any grant exists."""
        if actual_ns is None:
            self._cas(revision, unreserved_uncertain=True)
        else:
            _nonnegative("actual_ns", actual_ns)
            self._cas(revision, charged_ns=self._state.charged_ns + actual_ns)

    def reconcile_unreserved_failure(self, revision: int, actual_ns: int) -> None:
        """Prove unrecorded cost outside held grants; reconcile those separately."""
        _nonnegative("actual_ns", actual_ns)
        if not self._state.unreserved_uncertain:
            raise ValueError("no uncertain admission attempt")
        self._cas(
            revision, unreserved_uncertain=False, charged_ns=self._state.charged_ns + actual_ns
        )

    def finish(self, revision: int) -> None:
        """User result closes new optional admission, not receiving/accounting."""
        self._cas(revision, finished=True)

    def close_cost_epoch(self, revision: int) -> None:
        if not self._state.finished or self._state.grants or self.budget.accounting_uncertain:
            raise ValueError("all original costs and resources must be received first")
        self._cas(revision, cost_epoch_open=False)


class UsefulElapsed:
    """O(1) union clock: optional overlap is conservatively wholly uncredited.

    Only fresh useful work increments fresh_delta. Replay, queue waits, idle
    polling and backoff do not. No child duration is summed into parent credit.
    """

    def __init__(self, now_ns: int = 0) -> None:
        _nonnegative("now_ns", now_ns)
        self.last_ns = now_ns
        self.fresh = 0
        self.optional = 0
        self.credited_ns = 0

    def event(self, now_ns: int, *, fresh_delta: int = 0, optional_delta: int = 0) -> int:
        _nonnegative("now_ns", now_ns)
        if type(fresh_delta) is not int or type(optional_delta) is not int:
            raise ValueError("activity deltas must be integers")
        if (
            now_ns < self.last_ns
            or self.fresh + fresh_delta < 0
            or self.optional + optional_delta < 0
        ):
            raise ValueError("clock/activity cannot move backward")
        if self.fresh and not self.optional:
            self.credited_ns += now_ns - self.last_ns
        self.last_ns = now_ns
        self.fresh += fresh_delta
        self.optional += optional_delta
        return self.credited_ns
