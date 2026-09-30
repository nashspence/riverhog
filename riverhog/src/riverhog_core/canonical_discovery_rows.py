"""Bounded row extraction from an already validated canonical journal snapshot.

Only the canonical graph's declared references become edges. Opaque profile
objects are indexed lexically and never interpreted as Riverhog graph structure.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any

from riverhog_canonical_json import canonical_json_bytes
from riverhog_provenance import JournalSummary

from riverhog_core.canonical_discovery_extraction import (
    ProfileSection,
    ScalarPosting,
    assertion_postings,
    declared_profile_sections,
)


@dataclass(frozen=True, slots=True)
class CoreEdge:
    role: str
    target_id: str
    target_type: str
    target_scope: str = "local"
    exact_foreign_reference: bytes | None = None


@dataclass(frozen=True, slots=True)
class IndexedAssertion:
    row_key: str
    journal_id: str
    prefix_sha256: str
    entry_id: str
    entry_sequence: int
    entry_sha256: str
    assertion_id: str
    referent_id: str
    kind: str
    assertion_state: str
    canonical_json: bytes
    postings: tuple[ScalarPosting, ...]
    profiles: tuple[ProfileSection, ...]
    edges: tuple[CoreEdge, ...]


_ID_EDGES: dict[str, dict[str, str]] = {
    "occurrence": {"artifact_id": "artifact", "source_context_id": "context"},
    "state": {"occurrence_id": "occurrence"},
    "observation": {"capture_id": "activity", "source_context_id": "context"},
    "reported_description": {"source_context_id": "context"},
    "locator_binding": {"context_id": "context", "observation_id": "observation"},
    "usage": {"activity_id": "activity"},
    "generation": {"activity_id": "activity"},
    "invalidation": {"activity_id": "activity"},
    "derivation": {
        "activity_id": "activity",
        "usage_id": "usage",
        "generation_id": "generation",
    },
    "delivery_association": {
        "delivery_context_id": "context",
        "verification_observation_id": "observation",
    },
    "custody_assertion": {"custodian_agent_id": "agent", "context_id": "context"},
    "journal_subject": {"journal_id": "journal"},
}
_REFERENCE_EDGES: dict[str, tuple[str, ...]] = {
    "observation": ("state",),
    "reported_description": ("state",),
    "locator_binding": ("target",),
    "usage": ("state",),
    "generation": ("state",),
    "invalidation": ("state",),
    "derivation": ("used_state", "generated_state"),
    "specialization": ("specific", "general"),
    "alternate": ("left", "right"),
    "extension": ("subject",),
    "delivery_association": ("state",),
    "custody_assertion": ("target",),
    "journal_subject": ("artifact",),
}


def _edge_from_reference(role: str, value: Mapping[str, Any]) -> CoreEdge:
    scope = value["scope"]
    foreign = canonical_json_bytes(value) if scope == "external" else None
    return CoreEdge(
        role,
        value["object_id"],
        value["object_type"],
        scope,
        foreign,
    )


def core_edges(row: Mapping[str, Any]) -> tuple[CoreEdge, ...]:
    """Extract exact core edges without recursively interpreting profile data."""

    kind = row["type"]
    edges = [
        CoreEdge(role, row[role], target_type)
        for role, target_type in _ID_EDGES.get(kind, {}).items()
        if role in row
    ]
    edges.extend(
        _edge_from_reference(role, row[role])
        for role in _REFERENCE_EDGES.get(kind, ())
        if role in row
    )
    if kind == "activity":
        edges.extend(
            CoreEdge("association", item["agent_id"], "agent")
            for item in row.get("associations", ())
        )
        edges.extend(
            CoreEdge("context:" + item["role"], item["context_id"], "context")
            for item in row.get("contexts", ())
        )
    if kind == "extension" and row["value"]["type"] == "reference":
        edges.append(_edge_from_reference("value", row["value"]["value"]))
    return tuple(edges)


def index_row_key(prefix_sha256: str, assertion_id: str) -> str:
    return hashlib.sha256(
        canonical_json_bytes({"snapshot": prefix_sha256, "assertion": assertion_id})
    ).hexdigest()


def iter_index_assertions(summary: JournalSummary) -> Iterator[IndexedAssertion]:
    """Retain each documentary assertion with exact entry and snapshot support."""

    for sequence, frame in enumerate(summary.frames):
        document = frame.document
        for assertions in document["body"].get("assertions", {}).values():
            for row in assertions:
                yield IndexedAssertion(
                    row_key=index_row_key(summary.journal_sha256, row["assertion_id"]),
                    journal_id=summary.journal_id,
                    prefix_sha256=summary.journal_sha256,
                    entry_id=document["id"],
                    entry_sequence=sequence,
                    entry_sha256=frame.sha256,
                    assertion_id=row["assertion_id"],
                    referent_id=row["id"],
                    kind=row["type"],
                    assertion_state=(
                        "retracted"
                        if row["assertion_id"] in summary.retracted_assertion_ids
                        else "effective"
                    ),
                    canonical_json=canonical_json_bytes(row),
                    postings=tuple(assertion_postings(row)),
                    profiles=tuple(declared_profile_sections(row)),
                    edges=core_edges(row),
                )


__all__ = ["CoreEdge", "IndexedAssertion", "core_edges", "index_row_key", "iter_index_assertions"]
