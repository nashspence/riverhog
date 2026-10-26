"""Exact private coordinates for the fixed canonical discovery selectors.

These values name indexed joins and shared scope incidences. They neither
validate their archived inputs nor confer corpus acceptance or current access.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any

from riverhog_archive_contracts import (
    BOUND_HISTORY_EXTENT,
    RETAINED_HISTORY_EXTENT,
    HistoryJournalAnchor,
)
from riverhog_provenance_contracts import ExternalReference, require_canonical_uuid_urn

from riverhog_core.canonical_discovery_archive_work import _digest

SELECTOR_DOMAIN = "riverhog-discovery-fixed-selector/v1"
_SNAPSHOT = frozenset({"source_key", "anchor"})
_OBJECT = _SNAPSHOT | {"object_id", "scope"}
_FIELDS = {
    "member": _SNAPSHOT | {"member_id", "history_sha256", "association_id", "context_id"},
    "history-documentary": frozenset({"history_sha256", "extent"}),
    "snapshot-documentary": _SNAPSHOT,
    "prefix-dependencies": _SNAPSHOT,
    "state": _OBJECT,
    "own": _OBJECT,
    "observed": _OBJECT,
    "activity": _OBJECT,
    "direct": _OBJECT,
    "collection": _SNAPSHOT | {"object_id"},
    "continuity": _SNAPSHOT | {"object_id"},
    "import-reachable": frozenset({"history_sha256"}),
    "causal-state": frozenset({"reference"}),
    "late-bound": frozenset({"history_sha256", "scope"}),
    "bound-endpoint": _SNAPSHOT | {"reference", "scope"},
}


@dataclass(frozen=True, slots=True)
class Selector:
    kind: str
    source_key: str | None = None
    anchor: HistoryJournalAnchor | None = None
    object_id: str | None = None
    scope: str | None = None
    history_sha256: str | None = None
    extent: str | None = None
    reference: ExternalReference | None = None
    member_id: str | None = None
    association_id: str | None = None
    context_id: str | None = None

    def __post_init__(self) -> None:
        present = {
            field.name
            for field in fields(self)
            if field.name != "kind" and getattr(self, field.name) is not None
        }
        if self.kind not in _FIELDS or present != _FIELDS[self.kind]:
            raise ValueError("selector requires its fixed exact coordinate fields")
        for value in (self.source_key, self.history_sha256):
            if value is not None:
                _digest(value)
        for value in (self.object_id, self.member_id, self.association_id, self.context_id):
            if value is not None:
                require_canonical_uuid_urn(value)
        if self.anchor is not None and not isinstance(self.anchor, HistoryJournalAnchor):
            raise TypeError("selector requires a typed complete exact snapshot anchor")
        if self.scope is not None and self.scope not in {
            "member",
            "input-history",
            "recorded-history",
        }:
            raise ValueError("selector scope is invalid")
        if self.kind == "late-bound" and self.scope not in {"member", "input-history"}:
            raise ValueError("late-bound claims require a bound causal scope")
        if self.extent is not None and self.extent not in {
            BOUND_HISTORY_EXTENT,
            RETAINED_HISTORY_EXTENT,
        }:
            raise ValueError("selector history extent is invalid")
        if self.reference is not None:
            if not isinstance(self.reference, ExternalReference):
                raise TypeError("selector requires a typed complete exact external reference")
            if self.kind == "causal-state" and self.reference.object_type != "state":
                raise ValueError("causal-State selector requires an exact State reference")

    def to_mapping(self) -> dict[str, Any]:
        coordinates: dict[str, Any] = {}
        for name in sorted(_FIELDS[self.kind]):
            value = getattr(self, name)
            if isinstance(value, HistoryJournalAnchor):
                value = value.to_mapping()
            elif isinstance(value, ExternalReference):
                value = value.model_dump(mode="json")
            coordinates[name] = value
        return {"domain": SELECTOR_DOMAIN, "kind": self.kind, "coordinates": coordinates}

    @classmethod
    def from_mapping(cls, value: Any) -> Selector:
        if (
            not isinstance(value, dict)
            or set(value) != {"domain", "kind", "coordinates"}
            or value["domain"] != SELECTOR_DOMAIN
            or not isinstance(value["coordinates"], dict)
            or not isinstance(value["kind"], str)
            or value["kind"] not in _FIELDS
            or set(value["coordinates"]) != _FIELDS[value["kind"]]
        ):
            raise ValueError("selector has invalid exact coordinate encoding")
        coordinates = dict(value["coordinates"])
        if "anchor" in coordinates:
            coordinates["anchor"] = HistoryJournalAnchor.from_mapping(coordinates["anchor"])
        if "reference" in coordinates:
            coordinates["reference"] = ExternalReference.model_validate(coordinates["reference"])
        return cls(value["kind"], **coordinates)


@dataclass(frozen=True, slots=True)
class ImportedLateGuard:
    """One fixed same-member conjunction, retained separately from unary edges."""

    history_sha256: str
    primary_state: ExternalReference

    def __post_init__(self) -> None:
        _digest(self.history_sha256)
        if not isinstance(self.primary_state, ExternalReference):
            raise TypeError("imported late guard requires the exact typed primary State")
        if self.primary_state.object_type != "state":
            raise ValueError("imported late guard primary must be a State")

    @property
    def operands(self) -> tuple[Selector, Selector, Selector]:
        return (
            Selector("import-reachable", history_sha256=self.history_sha256),
            Selector("causal-state", reference=self.primary_state),
            Selector("late-bound", history_sha256=self.history_sha256, scope="input-history"),
        )
