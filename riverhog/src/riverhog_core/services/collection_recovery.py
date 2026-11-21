"""Collection-owned optional recovery grants and original cost receiving.

The surviving write control owns the root. A domain persists its prepared files
before selecting its own frontier in the same catalog transaction as the root
count. Callbacks retain domain authority; these records do not confer it. No
file, provider call, synchronization or resource join belongs in a callback.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from typing import Literal

from riverhog_canonical_json import format_scalar, parse_scalar
from riverhog_protocol.errors import Conflict, NotFound
from riverhog_protocol.pack_ingress import canonical_json_bytes
from riverhog_recovery_policy import (
    CheckpointCandidate,
    CheckpointDecision,
    RecoveryAccounting,
    RecoveryPolicy,
    decide_checkpoint,
)
from sqlalchemy import select, update
from sqlalchemy.orm import Session
from state_schema import read_snapshot

from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord as Control
from riverhog_core.catalog_db import SessionFactory, session_scope

_FORMAT = "riverhog-recovery-accounting/v1"
Guard = Callable[[Session], None]


def _json(value: object) -> str:
    return canonical_json_bytes(value).decode("utf-8")


def _number(value: int) -> str:
    return format_scalar("nonnegative", value)


def _name(value: str) -> str:
    if type(value) is not str or not value.strip():
        raise ValueError("recovery identity requires a nonempty string")
    return value


@dataclass(frozen=True, slots=True)
class RecoveryRootBinding:
    collection_id: int
    construction_sha256: str

    @classmethod
    def original(cls, collection_id: int, construction_json: str) -> RecoveryRootBinding:
        return cls(collection_id, hashlib.sha256(construction_json.encode("utf-8")).hexdigest())

    def document(self) -> dict[str, str]:
        return {
            "collection_id": format_scalar("sequence63", self.collection_id),
            "construction_sha256": self.construction_sha256,
        }

    @property
    def grant_prefix(self) -> str:
        return f"{format_scalar('sequence63', self.collection_id)}:{self.construction_sha256}:"


@dataclass(frozen=True, slots=True)
class RecoveryGrant:
    identity: str
    domain: str
    owner_id: str
    owner_fence: str
    original_reserved_ns: int
    charged_ns: int = 0
    remaining_reserved_ns: int = 0
    generation: str | None = None
    selection_actual_ns: int | None = None
    selection_reserved_ns: int | None = None
    uncertain: bool = False

    def document(self) -> dict[str, object]:
        return {
            "identity": _name(self.identity),
            "domain": _name(self.domain),
            "owner_id": _name(self.owner_id),
            "owner_fence": _name(self.owner_fence),
            "original_reserved_ns": _number(self.original_reserved_ns),
            "charged_ns": _number(self.charged_ns),
            "remaining_reserved_ns": _number(self.remaining_reserved_ns),
            "generation": None if self.generation is None else _name(self.generation),
            "selection_actual_ns": None
            if self.selection_actual_ns is None
            else _number(self.selection_actual_ns),
            "selection_reserved_ns": None
            if self.selection_reserved_ns is None
            else _number(self.selection_reserved_ns),
            "uncertain": self.uncertain,
        }


@dataclass(frozen=True, slots=True)
class RecoveryRoot:
    binding: RecoveryRootBinding
    policy: RecoveryPolicy
    revision: int
    accounting: RecoveryAccounting
    next_grant_sequence: int = 0
    cost_epoch_open: bool = True
    credit_certain: bool = True
    optional_admission_sealed: bool = False
    unreserved_uncertain: bool = False
    grants: tuple[RecoveryGrant, ...] = ()

    def document(self) -> dict[str, object]:
        return {
            "format": _FORMAT,
            "root": self.binding.document(),
            **{
                name: _number(getattr(self.accounting, name))
                for name in (
                    "useful_ns",
                    "charged_ns",
                    "reserved_ns",
                    "successful_checkpoints",
                    "held_count_slots",
                )
            },
            "next_grant_sequence": _number(self.next_grant_sequence),
            "cost_epoch_open": self.cost_epoch_open,
            "credit_certain": self.credit_certain,
            "optional_admission_sealed": self.optional_admission_sealed,
            "unreserved_uncertain": self.unreserved_uncertain,
            "active_grants": [grant.document() for grant in self.grants],
        }


def initial_recovery_accounting(collection_id: int, construction_json: str) -> str:
    """Only original mandatory construction admission creates a zero summary."""
    return _json(
        RecoveryRoot(
            RecoveryRootBinding.original(collection_id, construction_json),
            RecoveryPolicy(),
            0,
            RecoveryAccounting(),
        ).document()
    )


def _sequence(root: RecoveryRoot, identity: str) -> int:
    if type(identity) is not str or not identity.startswith(root.binding.grant_prefix):
        raise ValueError("recovery grant belongs to another construction")
    sequence = parse_scalar("nonnegative", identity[len(root.binding.grant_prefix) :])
    if not 1 <= sequence <= root.next_grant_sequence:
        raise ValueError("recovery grant was never issued")
    return sequence


def _read(control: Control, *, active_limit: int, control_bytes: int) -> RecoveryRoot:
    """Bound and validate current mandatory accounting; corruption is not replay."""
    raw = control.recovery_accounting_json
    if len(raw.encode("utf-8")) > control_bytes:
        raise Conflict("recovery accounting exceeds admitted control bytes")
    try:
        value = json.loads(raw)
        if value["format"] != _FORMAT:
            raise ValueError("unsupported accounting format")
        binding = RecoveryRootBinding.original(control.collection_id, control.construction_json)
        if value["root"] != binding.document():
            raise ValueError("accounting differs from original construction")
        if type(value["active_grants"]) is not list or len(value["active_grants"]) > active_limit:
            raise ValueError("accounting exceeds admitted grant slots")
        grants = tuple(
            RecoveryGrant(
                identity=item["identity"],
                domain=item["domain"],
                owner_id=item["owner_id"],
                owner_fence=item["owner_fence"],
                original_reserved_ns=parse_scalar("nonnegative", item["original_reserved_ns"]),
                charged_ns=parse_scalar("nonnegative", item["charged_ns"]),
                remaining_reserved_ns=parse_scalar("nonnegative", item["remaining_reserved_ns"]),
                generation=item["generation"],
                selection_actual_ns=None
                if item["selection_actual_ns"] is None
                else parse_scalar("nonnegative", item["selection_actual_ns"]),
                selection_reserved_ns=None
                if item["selection_reserved_ns"] is None
                else parse_scalar("nonnegative", item["selection_reserved_ns"]),
                uncertain=item["uncertain"],
            )
            for item in value["active_grants"]
        )
        for name in (
            "cost_epoch_open",
            "credit_certain",
            "optional_admission_sealed",
            "unreserved_uncertain",
        ):
            if type(value[name]) is not bool:
                raise ValueError("accounting flags must be booleans")
        if any(type(grant.uncertain) is not bool for grant in grants):
            raise ValueError("grant uncertainty must be boolean")
        root = RecoveryRoot(
            binding=binding,
            policy=RecoveryPolicy.from_mapping(json.loads(control.recovery_policy_json)),
            revision=parse_scalar("nonnegative", control.recovery_revision),
            accounting=RecoveryAccounting(
                **{
                    name: parse_scalar("nonnegative", value[name])
                    for name in (
                        "useful_ns",
                        "charged_ns",
                        "reserved_ns",
                        "successful_checkpoints",
                        "held_count_slots",
                    )
                },
                reconciled=not value["optional_admission_sealed"]
                and not value["unreserved_uncertain"]
                and value["credit_certain"]
                and not any(grant.uncertain for grant in grants),
            ),
            next_grant_sequence=parse_scalar("nonnegative", value["next_grant_sequence"]),
            cost_epoch_open=value["cost_epoch_open"],
            credit_certain=value["credit_certain"],
            optional_admission_sealed=value["optional_admission_sealed"],
            unreserved_uncertain=value["unreserved_uncertain"],
            grants=grants,
        )
        sequences = {_sequence(root, grant.identity) for grant in grants}
        if (
            any(
                (grant.generation is None) != (grant.selection_actual_ns is None)
                or (grant.generation is None) != (grant.selection_reserved_ns is None)
                or (
                    grant.selection_actual_ns is not None
                    and grant.selection_actual_ns > grant.charged_ns
                )
                for grant in grants
            )
            or len(sequences) != len(grants)
            or len({grant.domain for grant in grants}) != len(grants)
            or root.accounting.reserved_ns != sum(g.remaining_reserved_ns for g in grants)
            or root.accounting.held_count_slots != sum(g.generation is None for g in grants)
            or root.accounting.charged_ns < sum(g.charged_ns for g in grants)
            or root.accounting.successful_checkpoints
            < sum(g.generation is not None for g in grants)
            or root.accounting.successful_checkpoints + root.accounting.held_count_slots
            > root.next_grant_sequence
            or (not root.cost_epoch_open and (grants or root.unreserved_uncertain))
            or _json(root.document()) != raw
        ):
            raise ValueError("inconsistent or noncanonical accounting summary")
        return root
    except (KeyError, TypeError, ValueError, RecursionError, AttributeError) as error:
        raise RuntimeError("invalid mandatory recovery accounting") from error


def _grant(root: RecoveryRoot, identity: str) -> RecoveryGrant | None:
    _sequence(root, identity)
    return next((grant for grant in root.grants if grant.identity == identity), None)


def _replace_grant(root: RecoveryRoot, grant: RecoveryGrant) -> tuple[RecoveryGrant, ...]:
    return tuple(grant if old.identity == grant.identity else old for old in root.grants)


class CollectionRecoveryAccounting:
    """Short control-first transactions inside existing workflow admission.

    The composing execution/storage owner supplies its admitted active capacity
    and metadata bytes. Guards retain permission/claim locks before the control;
    domain frontier/receipt callbacks retain their job after it. Root/useful
    observations are internal owner values, never trusted remote claims.
    """

    def __init__(
        self, factory: SessionFactory, *, active_grant_limit: int, control_bytes: int
    ) -> None:
        for value in (active_grant_limit, control_bytes):
            if type(value) is not int or value < 1:
                raise ValueError("recovery requires positive admitted owner capacity")
        self._factory = factory
        self._active_limit = active_grant_limit
        self._control_bytes = control_bytes

    def _read(self, control: Control) -> RecoveryRoot:
        return _read(control, active_limit=self._active_limit, control_bytes=self._control_bytes)

    @contextmanager
    def _locked(
        self, binding: RecoveryRootBinding, guard: Guard
    ) -> Iterator[tuple[Session, Control, RecoveryRoot]]:
        # Shared writer admission precedes even the guard's catalog reads.
        from riverhog_core.services.archive_write_admission import _writer

        with session_scope(self._factory) as session:
            _writer(session)
            guard(session)
            control = session.scalar(
                select(Control)
                .where(Control.collection_id == binding.collection_id)
                .with_for_update()
            )
            if control is None:
                raise NotFound("recovery construction is unavailable")
            root = self._read(control)
            if root.binding != binding:
                raise Conflict("recovery construction identity changed")
            yield session, control, root

    def snapshot(self, binding: RecoveryRootBinding, *, guard: Guard) -> RecoveryRoot:
        with read_snapshot(self._factory) as session:
            guard(session)
            control = session.get(Control, binding.collection_id)
            if control is None:
                raise NotFound("recovery construction is unavailable")
            root = self._read(control)
            if root.binding != binding:
                raise Conflict("recovery construction identity changed")
            return root

    @staticmethod
    def _revision(root: RecoveryRoot, revision: int) -> None:
        _number(revision)
        if root.revision != revision:
            raise Conflict("recovery root changed; reread and reassess")

    @staticmethod
    def _forward(control: Control, epoch: str) -> None:
        if control.gate != "open" or control.epoch != epoch:
            raise Conflict("recovery forward admission is closed or changed")

    def _save(self, session: Session, root: RecoveryRoot, updated: RecoveryRoot) -> RecoveryRoot:
        encoded = _json(updated.document())
        if len(encoded.encode("utf-8")) > self._control_bytes:
            raise Conflict("recovery accounting exceeds admitted control bytes")
        changed = session.execute(
            update(Control)
            .where(
                Control.collection_id == root.binding.collection_id,
                Control.recovery_revision == _number(root.revision),
            )
            .values(recovery_accounting_json=encoded, recovery_revision=_number(root.revision + 1))
            .returning(Control)
            .execution_options(populate_existing=True)
        ).scalar_one_or_none()
        if changed is None:
            raise Conflict("recovery root changed; reread and reassess")
        # Validate the same representation readers will receive, including all
        # aggregate and scalar checks, before the transaction can commit.
        return self._read(changed)

    def reserve(
        self,
        binding: RecoveryRootBinding,
        *,
        epoch: str,
        revision: int,
        domain: str,
        owner_id: str,
        owner_fence: str,
        useful_ns: int,
        candidate: CheckpointCandidate,
        guard: Guard,
        retain_domain: Guard,
    ) -> tuple[CheckpointDecision, RecoveryGrant | None, RecoveryRoot]:
        for value in (domain, owner_id, owner_fence):
            _name(value)
        _number(useful_ns)
        with self._locked(binding, guard) as (session, control, root):
            self._revision(root, revision)
            self._forward(control, epoch)
            retain_domain(session)
            if useful_ns < root.accounting.useful_ns or (
                not root.credit_certain and useful_ns > root.accounting.useful_ns
            ):
                raise ValueError("recovery useful credit is not a proven fresh frontier")
            accounting = replace(root.accounting, useful_ns=useful_ns)
            candidate = replace(
                candidate,
                checkpoint_in_flight=candidate.checkpoint_in_flight
                or any(grant.domain == domain for grant in root.grants),
                storage_admitted=candidate.storage_admitted
                and len(root.grants) < self._active_limit,
            )
            decision = decide_checkpoint(root.policy, accounting, candidate)
            if not decision.admitted:
                return decision, None, root
            grant = RecoveryGrant(
                identity=root.binding.grant_prefix + _number(root.next_grant_sequence + 1),
                domain=domain,
                owner_id=owner_id,
                owner_fence=owner_fence,
                original_reserved_ns=decision.reserved_ns,
                remaining_reserved_ns=decision.reserved_ns,
            )
            updated = replace(
                root,
                accounting=replace(
                    accounting,
                    reserved_ns=accounting.reserved_ns + decision.reserved_ns,
                    held_count_slots=accounting.held_count_slots + 1,
                ),
                next_grant_sequence=root.next_grant_sequence + 1,
                grants=(*root.grants, grant),
            )
            if len(_json(updated.document()).encode("utf-8")) > self._control_bytes:
                return CheckpointDecision("storage-not-admitted"), None, root
            return decision, grant, self._save(session, root, updated)

    def select_generation(
        self,
        binding: RecoveryRootBinding,
        *,
        epoch: str,
        revision: int,
        identity: str,
        generation: str,
        total_actual_ns: int,
        remaining_reserved_ns: int,
        guard: Guard,
        select_frontier: Callable[[Session, RecoveryGrant, str], None],
    ) -> RecoveryRoot:
        """Select a fully durable descriptor under fresh domain/source fences.

        The callback owns the typed job selector and exact original selection
        reconciliation. It must never rewind a newer selector on a lost reply.
        Receiving/pruning this grant leaves that selector and its pins intact.
        """
        _name(generation)
        _number(total_actual_ns)
        _number(remaining_reserved_ns)
        with self._locked(binding, guard) as (session, control, root):
            grant = _grant(root, identity)
            if grant is None:
                raise Conflict("recovery grant received; reconcile the domain frontier")
            if grant.generation is not None:
                if grant.generation != generation:
                    raise Conflict("recovery grant selected a different generation")
                if (total_actual_ns, remaining_reserved_ns) != (
                    grant.selection_actual_ns,
                    grant.selection_reserved_ns,
                ):
                    raise Conflict("selection retry differs from its original cost receipt")
                select_frontier(session, grant, generation)
                return root
            self._revision(root, revision)
            self._forward(control, epoch)
            if (
                root.optional_admission_sealed
                or grant.uncertain
                or total_actual_ns < grant.charged_ns
            ):
                raise Conflict("recovery selection cost or original execution is uncertain")
            select_frontier(session, grant, generation)
            updated_grant = replace(
                grant,
                generation=generation,
                selection_actual_ns=total_actual_ns,
                selection_reserved_ns=remaining_reserved_ns,
                charged_ns=total_actual_ns,
                remaining_reserved_ns=remaining_reserved_ns,
            )
            return self._save(
                session,
                root,
                replace(
                    root,
                    accounting=replace(
                        root.accounting,
                        charged_ns=root.accounting.charged_ns + total_actual_ns - grant.charged_ns,
                        reserved_ns=root.accounting.reserved_ns
                        - grant.remaining_reserved_ns
                        + remaining_reserved_ns,
                        held_count_slots=root.accounting.held_count_slots - 1,
                        successful_checkpoints=root.accounting.successful_checkpoints + 1,
                    ),
                    grants=_replace_grant(root, updated_grant),
                ),
            )

    def receive_cost(
        self,
        binding: RecoveryRootBinding,
        *,
        revision: int,
        identity: str,
        total_actual_ns: int,
        remaining_reserved_ns: int,
        guard: Guard,
        retain_original: Callable[[Session, RecoveryGrant | None], None],
        receive_original: Callable[[Session, str, int], Literal["published", "not-published"]]
        | None = None,
    ) -> tuple[Literal["updated", "received", "already-received"], RecoveryRoot]:
        """A final callback verifies immutable original cost and actual receiving.

        Allocated-but-absent is positive prior receiving, even at a stale CAS.
        The domain must reject contradictory final receipts before accepting that
        duplicate. A partial report can never resurrect a fully received grant.
        """
        _number(total_actual_ns)
        _number(remaining_reserved_ns)
        complete = receive_original is not None
        if complete and remaining_reserved_ns:
            raise ValueError("original receiving must settle the whole reservation")
        with self._locked(binding, guard) as (session, _control, root):
            grant = _grant(root, identity)
            retain_original(session, grant)
            if grant is None:
                if receive_original is None:
                    raise Conflict("recovery grant already received")
                if receive_original(session, identity, total_actual_ns) not in {
                    "published",
                    "not-published",
                }:
                    raise ValueError("original receiving requires a known publication outcome")
                return "already-received", root
            self._revision(root, revision)
            if total_actual_ns < grant.charged_ns:
                raise ValueError("recovery original cost cannot decrease")
            if receive_original is not None:
                outcome = receive_original(session, identity, total_actual_ns)
                if outcome != ("not-published" if grant.generation is None else "published"):
                    raise Conflict("original receiving contradicts selected publication")
            updated_grant = replace(
                grant, charged_ns=total_actual_ns, remaining_reserved_ns=remaining_reserved_ns
            )
            updated = replace(
                root,
                accounting=replace(
                    root.accounting,
                    charged_ns=root.accounting.charged_ns + total_actual_ns - grant.charged_ns,
                    reserved_ns=root.accounting.reserved_ns
                    - grant.remaining_reserved_ns
                    + remaining_reserved_ns,
                    held_count_slots=root.accounting.held_count_slots
                    - int(complete and grant.generation is None),
                ),
                grants=tuple(g for g in root.grants if g.identity != identity)
                if complete
                else _replace_grant(root, updated_grant),
            )
            return "received" if complete else "updated", self._save(session, root, updated)

    def charge_unreserved(
        self,
        binding: RecoveryRootBinding,
        *,
        revision: int,
        actual_ns: int | None,
        guard: Guard,
    ) -> RecoveryRoot:
        """Charge a known original admission failure, or seal unknown cost.

        The owner reports a known delta exactly once through its original attempt
        receiving. This includes CAS work which failed before a grant existed.
        An interrupted open epoch independently seals unreported attempts.
        """
        if actual_ns is not None:
            _number(actual_ns)
        with self._locked(binding, guard) as (session, _control, root):
            self._revision(root, revision)
            if not root.cost_epoch_open:
                raise Conflict("recovery cost epoch is closed")
            return self._save(
                session,
                root,
                replace(
                    root,
                    accounting=replace(
                        root.accounting, charged_ns=root.accounting.charged_ns + (actual_ns or 0)
                    ),
                    unreserved_uncertain=root.unreserved_uncertain or actual_ns is None,
                    optional_admission_sealed=root.optional_admission_sealed or actual_ns is None,
                ),
            )

    def seal_interrupted(
        self, binding: RecoveryRootBinding, *, revision: int, guard: Guard
    ) -> RecoveryRoot:
        """An unclean open epoch cannot manufacture a fresh percentage budget."""
        with self._locked(binding, guard) as (session, _control, root):
            self._revision(root, revision)
            if not root.cost_epoch_open:
                return root
            return self._save(
                session,
                root,
                replace(
                    root,
                    credit_certain=False,
                    optional_admission_sealed=True,
                    unreserved_uncertain=True,
                    grants=tuple(replace(grant, uncertain=True) for grant in root.grants),
                ),
            )

    def reconcile_unreserved(
        self,
        binding: RecoveryRootBinding,
        *,
        revision: int,
        additional_actual_ns: int,
        guard: Guard,
        receive_attempts: Guard,
    ) -> RecoveryRoot:
        """Accept finite exact owner reconciliation without reopening admission."""
        _number(additional_actual_ns)
        with self._locked(binding, guard) as (session, _control, root):
            self._revision(root, revision)
            if not root.unreserved_uncertain:
                raise Conflict("recovery has no uncertain unreserved attempts")
            receive_attempts(session)
            return self._save(
                session,
                root,
                replace(
                    root,
                    unreserved_uncertain=False,
                    accounting=replace(
                        root.accounting,
                        charged_ns=root.accounting.charged_ns + additional_actual_ns,
                    ),
                ),
            )

    def close_cost_epoch(
        self, binding: RecoveryRootBinding, *, revision: int, guard: Guard, receive_terminal: Guard
    ) -> RecoveryRoot:
        with self._locked(binding, guard) as (session, control, root):
            self._revision(root, revision)
            if control.gate != "closed" or root.grants or root.unreserved_uncertain:
                raise Conflict("recovery requires terminal and all original cost receiving")
            if not root.cost_epoch_open:
                return root
            receive_terminal(session)
            return self._save(session, root, replace(root, cost_epoch_open=False))
