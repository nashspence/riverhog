"""Pure destination decisions for exact Riverhog artifact selections.

The caller supplies an exact canonical occurrence hint and separately qualifies the
destination filesystem. This package neither reads provenance nor writes files.
"""

from __future__ import annotations

import re
import unicodedata
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Literal

from riverhog_protocol.artifact_identity import ArtifactId
from riverhog_provenance_contracts import PROFILE, ContractCatalog

MaterializationMode = Literal["declared-hints", "id-layout"]
FallbackReason = Literal[
    "hint",
    "escaped-hint",
    "no-hint",
    "id-layout",
    "destination-limits",
    "destination-collision",
]

_HINT_SCHEMA = PROFILE + "/materialization-hint.schema.json"
_WINDOWS_FORBIDDEN = frozenset('<>:"|?*')
_WINDOWS_DEVICE = re.compile(r"(?:COM|LPT)[1-9¹²³]\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


@dataclass(frozen=True, slots=True)
class DestinationRules:
    """Qualified behavior of one destination, supplied by its filesystem owner."""

    windows_names: bool
    case_sensitive: bool
    unicode_equivalence: Literal["exact", "NFC"]
    component_bytes: int
    relative_path_bytes: int

    def __post_init__(self) -> None:
        if type(self.windows_names) is not bool or type(self.case_sensitive) is not bool:
            raise TypeError("destination naming flags must be booleans")
        if self.unicode_equivalence not in ("exact", "NFC"):
            raise ValueError("destination Unicode equivalence is unqualified")
        if (
            type(self.component_bytes) is not int
            or type(self.relative_path_bytes) is not int
            or self.component_bytes < 1
            or self.relative_path_bytes < 1
        ):
            raise ValueError("destination byte limits must be positive integers")

    def equivalent(self, component: str) -> str:
        value = (
            unicodedata.normalize("NFC", component)
            if self.unicode_equivalence == "NFC"
            else component
        )
        return value if self.case_sensitive else value.casefold()

    def fits(self, components: tuple[str, ...]) -> bool:
        return all(len(value.encode("utf-8")) <= self.component_bytes for value in components) and (
            len("/".join(components).encode("utf-8")) <= self.relative_path_bytes
        )


@dataclass(frozen=True, slots=True)
class MemberAdvice:
    """Advice selected at the exact member delivery anchor by the caller."""

    artifact_id: str
    materialization_hint: Mapping[str, Any] | None


@dataclass(frozen=True, slots=True)
class PlannedDestination:
    artifact_id: str
    components: tuple[str, ...]
    sidecar_components: tuple[str, ...]
    reason: FallbackReason
    materialization_hint: tuple[str, ...] | None


def id_components(artifact_id: str) -> tuple[str, str, str]:
    selected = str(ArtifactId(artifact_id))
    return ("artifacts", selected[:2], selected)


def primary_sidecar_components(artifact_id: str) -> tuple[str, str, str, str]:
    selected = str(ArtifactId(artifact_id))
    return ("provenance", "primary", selected[:2], selected + ".fprov.jsonseq")


def shared_journal_components(snapshot_sha256: str) -> tuple[str, str, str]:
    if _SHA256.fullmatch(snapshot_sha256) is None:
        raise ValueError("journal snapshot SHA-256 must be lowercase hex")
    return ("provenance", "journals", snapshot_sha256 + ".jsonseq")


def escape_component(component: str, rules: DestinationRules) -> str:
    """Keep the supplied spelling except for reversible destination safety escapes."""

    ContractCatalog().validate(_HINT_SCHEMA, {"components": [component]})
    return _escape_validated_component(component, rules)


def _escape_validated_component(component: str, rules: DestinationRules) -> str:
    trailing_start = len(component.rstrip(" ."))
    output = []
    for index, character in enumerate(component):
        needs_escape = character == "%" or (
            rules.windows_names and (character in _WINDOWS_FORBIDDEN or index >= trailing_start)
        )
        output.append(
            "".join(f"%{byte:02X}" for byte in character.encode("utf-8"))
            if needs_escape
            else character
        )
    escaped = "".join(output)
    stem = component.split(".", 1)[0].upper()
    if rules.windows_names and (
        stem in {"CON", "PRN", "AUX", "NUL"} or _WINDOWS_DEVICE.fullmatch(stem)
    ):
        first = component[0]
        prefix = "".join(f"%{byte:02X}" for byte in first.encode("utf-8"))
        escaped = prefix + escaped[len(first) :]
    return escaped


def plan_materialization(
    members: Iterable[MemberAdvice],
    *,
    rules: DestinationRules,
    mode: MaterializationMode = "declared-hints",
) -> tuple[PlannedDestination, ...]:
    """Plan a frozen selection; every conflicting hint falls back to its own ID.

    This operation is linear in the number of supplied hint components and members.
    The caller owns persistence of the resulting identity-to-destination mapping.
    """

    if mode not in ("declared-hints", "id-layout"):
        raise ValueError("unknown materialization mode")
    catalog = ContractCatalog()
    planned: dict[str, PlannedDestination] = {}
    for member in members:
        artifact_id = str(ArtifactId(member.artifact_id))
        if artifact_id in planned:
            raise ValueError("artifact selection contains a duplicate member")
        hint = None
        if member.materialization_hint is not None:
            catalog.validate(_HINT_SCHEMA, member.materialization_hint)
            hint = tuple(member.materialization_hint["components"])
        components: tuple[str, ...] = id_components(artifact_id)
        reason: FallbackReason = "id-layout" if mode == "id-layout" else "no-hint"
        if mode == "declared-hints" and hint is not None:
            candidate = ("files", *(_escape_validated_component(value, rules) for value in hint))
            if rules.fits(candidate):
                components = candidate
                reason = "hint" if candidate[1:] == hint else "escaped-hint"
            else:
                reason = "destination-limits"
        sidecar = primary_sidecar_components(artifact_id)
        if not rules.fits(id_components(artifact_id)) or not rules.fits(sidecar):
            raise ValueError("destination cannot represent mandatory ID and provenance layout")
        planned[artifact_id] = PlannedDestination(artifact_id, components, sidecar, reason, hint)

    # A canonical key can represent more than one actual spelling on a destination.
    # Check both file/dir collisions and directory-spelling aliases. Every involved
    # candidate falls back, so no ordering or arbitrary winner affects the result.
    leaves: dict[tuple[str, ...], set[str]] = defaultdict(set)
    prefixes: dict[tuple[str, ...], dict[tuple[str, ...], set[str]]] = defaultdict(
        lambda: defaultdict(set)
    )
    for planned_row in planned.values():
        if planned_row.components[0] != "files":
            continue
        normalized = tuple(rules.equivalent(part) for part in planned_row.components)
        leaves[normalized].add(planned_row.artifact_id)
        for size in range(1, len(normalized) + 1):
            prefixes[normalized[:size]][planned_row.components[:size]].add(planned_row.artifact_id)

    conflicts: set[str] = set()
    for key, spellings in prefixes.items():
        if len(spellings) > 1:
            for owners in spellings.values():
                conflicts.update(owners)
        leaf_owners = leaves.get(key)
        if leaf_owners and (
            len(leaf_owners) > 1 or sum(map(len, spellings.values())) > len(leaf_owners)
        ):
            for owners in spellings.values():
                conflicts.update(owners)
    return tuple(
        PlannedDestination(
            artifact_id,
            id_components(artifact_id) if artifact_id in conflicts else row.components,
            row.sidecar_components,
            "destination-collision" if artifact_id in conflicts else row.reason,
            row.materialization_hint,
        )
        for artifact_id, row in sorted(planned.items())
    )


__all__ = [
    "DestinationRules",
    "MemberAdvice",
    "PlannedDestination",
    "escape_component",
    "id_components",
    "plan_materialization",
    "primary_sidecar_components",
    "shared_journal_components",
]
