"""Collection recovery values and original mandatory accounting encoding."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from riverhog_canonical_json import format_scalar
from riverhog_protocol.pack_ingress import canonical_json_bytes
from riverhog_recovery_policy import RecoveryAccounting, RecoveryPolicy

_FORMAT = "riverhog-recovery-accounting/v2"


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
    nonpublication_proven: bool = False

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
            "nonpublication_proven": self.nonpublication_proven,
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
