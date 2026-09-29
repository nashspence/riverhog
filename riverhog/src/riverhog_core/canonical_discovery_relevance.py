"""Exact member versus input and documentary scope for canonical discovery.

The directed traversal never treats journal co-residence, a shared activity, a
shared context, or matching payload bytes as an attribution to a member.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator, Mapping
from typing import Any

from riverhog_protocol.artifact_identity import ArtifactMemberIdentityDocument
from riverhog_protocol.provenance_transport import CollectionArtifactProvenanceBindingDocument
from riverhog_provenance import JournalSummary, ProvenanceValidationError, validate_journal_chunks
from riverhog_provenance_contracts import ContractCatalog

from riverhog_core.canonical_discovery_rows import index_row_key
from riverhog_core.provenance_binding import verify_member_binding

type RelevanceKey = tuple[str, str, str]


def _assertions(summary: JournalSummary) -> Iterator[dict[str, Any]]:
    for frame in summary.frames:
        for rows in frame.document["body"].get("assertions", {}).values():
            yield from rows


def _matches_local(ref: object, targets: set[str]) -> bool:
    return isinstance(ref, dict) and ref.get("scope") == "local" and ref.get("object_id") in targets


def _anchored_snapshot(
    full: JournalSummary,
    anchor: Mapping[str, Any],
    *,
    catalog: ContractCatalog | None,
) -> JournalSummary:
    if full.anchor == anchor:
        return full
    digest = hashlib.sha256()
    size = 0
    for index, frame in enumerate(full.frames):
        digest.update(frame.encoded)
        size += len(frame.encoded)
        if frame.reference == anchor["through"]:
            if (
                digest.hexdigest() != anchor["prefix_sha256"]
                or size != int(anchor["prefix_bytes"])
                or full.journal_id != anchor["journal_id"]
            ):
                raise ProvenanceValidationError("foreign prefix anchor differs")
            return validate_journal_chunks(
                (item.encoded for item in full.frames[: index + 1]),
                expected_anchor=anchor,
                require_exact_tail=True,
                catalog=catalog,
            )
    raise ProvenanceValidationError("foreign prefix anchor is absent")


def member_relevance(
    *,
    member: ArtifactMemberIdentityDocument,
    binding: CollectionArtifactProvenanceBindingDocument,
    primary: JournalSummary,
    corpus: Mapping[str, JournalSummary],
    delivery_context_id: str,
    catalog: ContractCatalog | None = None,
) -> dict[RelevanceKey, frozenset[str]]:
    """Return exact (journal,prefix,assertion) scopes for one bound member."""

    verified = verify_member_binding(
        member=member,
        binding=binding,
        summary=primary,
        delivery_context_id=delivery_context_id,
    )
    if primary.journal_id not in corpus:
        raise ProvenanceValidationError("primary journal is outside the admitted corpus")
    found: dict[RelevanceKey, set[str]] = {}
    snapshots: dict[tuple[str, str], JournalSummary] = {
        (primary.journal_id, primary.journal_sha256): primary
    }

    def mark(summary: JournalSummary, row: Mapping[str, Any], scope: str) -> None:
        key = (summary.journal_id, summary.journal_sha256, row["assertion_id"])
        found.setdefault(key, set()).add(scope)
        snapshots[(summary.journal_id, summary.journal_sha256)] = summary

    def resolve_anchor(anchor: Mapping[str, Any]) -> JournalSummary:
        key = (anchor["journal_id"], anchor["prefix_sha256"])
        if key not in snapshots:
            full = corpus.get(anchor["journal_id"])
            if full is None:
                raise ProvenanceValidationError("referenced journal is outside the admitted corpus")
            snapshots[key] = _anchored_snapshot(full, anchor, catalog=catalog)
        return snapshots[key]

    def resolve_state(
        summary: JournalSummary, ref: Mapping[str, Any]
    ) -> tuple[JournalSummary, dict[str, Any]]:
        if ref["object_type"] != "state":
            raise ProvenanceValidationError("input history does not reference a State")
        if ref["scope"] == "local":
            row = summary.graph_validation.objects.get(ref["object_id"])
            target = summary
        else:
            foreign = corpus.get(ref["journal_id"])
            if foreign is None:
                raise ProvenanceValidationError("input history journal is outside the corpus")
            through = ref["entry"]
            digest = hashlib.sha256()
            size = 0
            anchor: dict[str, Any] | None = None
            for frame in foreign.frames:
                digest.update(frame.encoded)
                size += len(frame.encoded)
                if frame.reference == through:
                    anchor = {
                        "journal_id": foreign.journal_id,
                        "through": through,
                        "prefix_sha256": digest.hexdigest(),
                        "prefix_bytes": str(size),
                    }
                    break
            if anchor is None:
                raise ProvenanceValidationError("input history foreign entry is absent")
            target = resolve_anchor(anchor)
            row = target.graph_validation.objects.get(ref["object_id"])
            if row is None or row["assertion_id"] != ref["assertion_id"]:
                raise ProvenanceValidationError("input history foreign State is not exact")
        if row is None or row["type"] != "state":
            raise ProvenanceValidationError("input history State is unresolved")
        return target, row

    visited_states: set[tuple[str, str, str, str]] = set()

    def state_history(summary: JournalSummary, state: Mapping[str, Any], scope: str) -> None:
        visit = (summary.journal_id, summary.journal_sha256, state["id"], scope)
        if visit in visited_states:
            return
        visited_states.add(visit)
        objects = summary.graph_validation.objects
        occurrence = objects[state["occurrence_id"]]
        artifact = objects[occurrence["artifact_id"]]
        own_ids = {state["id"], occurrence["id"], artifact["id"]}
        context_ids: set[str] = set()
        if "source_context_id" in occurrence:
            context_ids.add(occurrence["source_context_id"])
        activities: set[str] = set()
        for row in _assertions(summary):
            kind = row["type"]
            if kind in {"generation", "invalidation"} and _matches_local(
                row.get("state"), {state["id"]}
            ):
                mark(summary, row, scope)
                activities.add(row["activity_id"])
            elif kind == "derivation" and _matches_local(row.get("generated_state"), {state["id"]}):
                mark(summary, row, scope)
                if "activity_id" in row:
                    activities.add(row["activity_id"])
                for relation_id in (row.get("usage_id"), row.get("generation_id")):
                    relation = objects.get(relation_id)
                    if relation is not None:
                        mark(summary, relation, scope)
                target, source = resolve_state(summary, row["used_state"])
                state_history(target, source, "input-history")
        for activity_id in activities:
            activity = objects.get(activity_id)
            if activity is not None:
                mark(summary, activity, scope)
                context_ids.update(item["context_id"] for item in activity.get("contexts", ()))
        own_ids.update(activities)
        for row in _assertions(summary):
            kind = row["type"]
            direct = row["id"] in own_ids
            attached = _matches_local(row.get("subject", row.get("target")), own_ids)
            if kind == "journal_subject":
                attached = attached or _matches_local(row.get("artifact"), {artifact["id"]})
            observed = kind in {"observation", "reported_description"} and _matches_local(
                row.get("state"), {state["id"]}
            )
            if direct or attached or observed:
                mark(summary, row, scope)
                if kind in {"observation", "reported_description"} and "source_context_id" in row:
                    context_ids.add(row["source_context_id"])
        for context_id in context_ids:
            context = objects.get(context_id)
            if context is not None:
                mark(summary, context, scope)

    objects = primary.graph_validation.objects
    association = objects[binding.delivery_association_id]
    state = objects[association["state"]["object_id"]]
    if objects[state["occurrence_id"]]["id"] != verified.occurrence_id:
        raise ProvenanceValidationError("member relevance selected a different Occurrence")
    state_history(primary, state, "member")
    mark(primary, association, "member")
    context = objects.get(delivery_context_id)
    if context is not None:
        mark(primary, context, "collection")
    for row in _assertions(primary):
        if row["type"] == "extension" and _matches_local(row.get("subject"), {delivery_context_id}):
            mark(primary, row, "collection")

    # Recorded history extends to other States of the same declared continuity
    # Artifact; physical co-residence alone never adds a sibling's attributes.
    selected_artifact_id = objects[state["occurrence_id"]]["artifact_id"]
    for candidate in objects.values():
        if candidate["type"] != "state" or candidate["id"] == state["id"]:
            continue
        occurrence = objects.get(candidate["occurrence_id"])
        if occurrence is not None and occurrence["artifact_id"] == selected_artifact_id:
            state_history(primary, candidate, "recorded-history")

    pending = [primary]
    visited_snapshots: set[tuple[str, str]] = set()
    while pending:
        summary = pending.pop()
        key = (summary.journal_id, summary.journal_sha256)
        if key in visited_snapshots:
            continue
        visited_snapshots.add(key)
        for ref in summary.graph_validation.external_references:
            foreign = corpus.get(ref["journal_id"])
            if foreign is None:
                raise ProvenanceValidationError("documentary foreign journal is absent")
            digest = hashlib.sha256()
            size = 0
            for frame in foreign.frames:
                digest.update(frame.encoded)
                size += len(frame.encoded)
                if frame.reference == ref["entry"]:
                    pending.append(
                        resolve_anchor(
                            {
                                "journal_id": foreign.journal_id,
                                "through": frame.reference,
                                "prefix_sha256": digest.hexdigest(),
                                "prefix_bytes": str(size),
                            }
                        )
                    )
                    break
            else:
                raise ProvenanceValidationError("documentary foreign entry is absent")
        parent = summary.frames[0].document["body"]["journal"].get("forked_from")
        if parent is not None:
            pending.append(resolve_anchor(parent))
    return {key: frozenset(scopes) for key, scopes in found.items()}


def relevance_row_keys(
    relevance: Mapping[RelevanceKey, frozenset[str]],
) -> Iterator[tuple[str, str]]:
    """Translate exact assertions into content-addressed index rows and scopes."""
    for (_, prefix_sha256, assertion_id), scopes in sorted(relevance.items()):
        row_key = index_row_key(prefix_sha256, assertion_id)
        for scope in sorted(scopes):
            yield row_key, scope


def snapshots_for_relevance(
    relevance: Mapping[RelevanceKey, frozenset[str]],
    *,
    corpus: Mapping[str, JournalSummary],
    catalog: ContractCatalog | None = None,
) -> Iterator[JournalSummary]:
    """Reconstruct only exact selected prefix snapshots from admitted full journals."""
    for journal_id, prefix_sha256 in sorted({key[:2] for key in relevance}):
        full = corpus.get(journal_id)
        if full is None:
            raise ProvenanceValidationError("relevant journal is outside the admitted corpus")
        if full.journal_sha256 == prefix_sha256:
            yield full
            continue
        digest = hashlib.sha256()
        size = 0
        for frame in full.frames:
            digest.update(frame.encoded)
            size += len(frame.encoded)
            if digest.hexdigest() == prefix_sha256:
                yield _anchored_snapshot(
                    full,
                    {
                        "journal_id": journal_id,
                        "through": frame.reference,
                        "prefix_sha256": prefix_sha256,
                        "prefix_bytes": str(size),
                    },
                    catalog=catalog,
                )
                break
        else:
            raise ProvenanceValidationError("relevant prefix is outside the admitted journal")


__all__ = [
    "RelevanceKey",
    "member_relevance",
    "relevance_row_keys",
    "snapshots_for_relevance",
]
