"""Exact member versus input and documentary scope for canonical discovery.

The directed traversal never treats journal co-residence, a shared activity, a
shared context, or matching payload bytes as an attribution to a member.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import sqlite3
import sys
from collections.abc import Iterator, Mapping
from pathlib import Path
from types import TracebackType
from typing import Any, Self

from riverhog_archive_contracts import (
    BOUND_HISTORY_EXTENT,
    RETAINED_HISTORY_EXTENT,
    MemberHistoryBinding,
)
from riverhog_canonical_json import canonical_json_bytes
from riverhog_protocol.artifact_identity import ArtifactMemberIdentityDocument
from riverhog_protocol.provenance_transport import CollectionArtifactProvenanceBindingDocument
from riverhog_provenance import (
    JournalSummary,
    MemberHistoryClosure,
    ProvenanceValidationError,
    external_reference,
    validate_journal_chunks,
)
from riverhog_provenance_contracts import ContractCatalog

from riverhog_core.canonical_discovery_rows import index_row_key
from riverhog_core.provenance_binding import verify_member_binding
from riverhog_core.scratch_workspace import scratch_directory

type RelevanceKey = tuple[str, str, str]


class MemberRelevance(Mapping[RelevanceKey, frozenset[str]]):
    """Persistent member scope and traversal; no in-memory corpus or result set."""

    def __init__(self) -> None:
        self._scratch = contextlib.ExitStack()
        try:
            scratch = self._scratch.enter_context(
                scratch_directory(prefix="riverhog-member-relevance-")
            )
            self.db = sqlite3.connect(Path(scratch) / "relevance.sqlite3")
            self._scratch.callback(self.db.close)
            self.db.execute("PRAGMA cache_size = -512")
            self.db.execute("PRAGMA temp_store = FILE")
            self.db.executescript(
                "CREATE TABLE scopes(journal TEXT, prefix TEXT, assertion TEXT, scope TEXT, "
                "PRIMARY KEY(journal, prefix, assertion, scope));"
                "CREATE TABLE snapshots(journal TEXT, prefix TEXT, anchor BLOB, "
                "PRIMARY KEY(journal, prefix));"
                "CREATE TABLE states(journal TEXT, prefix TEXT, state TEXT, "
                "scope TEXT, value BLOB, "
                "done INTEGER, PRIMARY KEY(journal, prefix, state, scope));"
                "CREATE TABLE causal(reference BLOB PRIMARY KEY);"
                "CREATE TABLE views(history TEXT, extent TEXT, binding BLOB, "
                "own INTEGER, done INTEGER, "
                "PRIMARY KEY(history, extent));"
            )
        except BaseException:
            self._scratch.__exit__(*sys.exc_info())
            raise

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        return self._scratch.__exit__(exc_type, exc, traceback)

    def __del__(self) -> None:
        self.close()

    def close(self) -> None:
        self._scratch.close()

    def __getitem__(self, key: RelevanceKey) -> frozenset[str]:
        values = frozenset(
            row[0]
            for row in self.db.execute(
                "SELECT scope FROM scopes WHERE journal = ? AND prefix = ? AND assertion = ?", key
            )
        )
        if not values:
            raise KeyError(key)
        return values

    def __iter__(self) -> Iterator[RelevanceKey]:
        yield from self.db.execute(
            "SELECT DISTINCT journal, prefix, assertion FROM scopes "
            "ORDER BY journal, prefix, assertion"
        )

    def __len__(self) -> int:
        return int(
            self.db.execute(
                "SELECT count(*) FROM (SELECT DISTINCT journal, prefix, assertion FROM scopes)"
            ).fetchone()[0]
        )

    def remember(self, summary: JournalSummary) -> None:
        self.db.execute(
            "INSERT OR IGNORE INTO snapshots VALUES (?, ?, ?)",
            (summary.journal_id, summary.journal_sha256, canonical_json_bytes(summary.anchor)),
        )

    def mark(self, summary: JournalSummary, row: Mapping[str, Any], scope: str) -> None:
        self.remember(summary)
        self.db.execute(
            "INSERT OR IGNORE INTO scopes VALUES (?, ?, ?, ?)",
            (summary.journal_id, summary.journal_sha256, row["assertion_id"], scope),
        )

    def anchors(self) -> Iterator[Mapping[str, Any]]:
        for (anchor,) in self.db.execute("SELECT anchor FROM snapshots ORDER BY journal, prefix"):
            yield json.loads(anchor)

    def row_keys(self) -> Iterator[tuple[str, str]]:
        for prefix, assertion_id, scope in self.db.execute(
            "SELECT prefix, assertion, scope FROM scopes ORDER BY journal, prefix, assertion, scope"
        ):
            yield index_row_key(prefix, assertion_id), scope


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
                require_profiles=False,
            )
    raise ProvenanceValidationError("foreign prefix anchor is absent")


def member_relevance(
    *,
    member: ArtifactMemberIdentityDocument,
    binding: CollectionArtifactProvenanceBindingDocument,
    primary: JournalSummary,
    corpus: Mapping[str, JournalSummary],
    delivery_context_id: str,
    history_binding: MemberHistoryBinding,
    closure: MemberHistoryClosure,
    catalog: ContractCatalog | None = None,
) -> MemberRelevance:
    """Return exact (journal,prefix,assertion) scopes for one bound member."""

    verified = verify_member_binding(
        member=member,
        binding=binding,
        summary=primary,
        delivery_context_id=delivery_context_id,
    )
    if primary.journal_id not in corpus:
        raise ProvenanceValidationError("primary journal is outside the admitted corpus")
    found = MemberRelevance()
    found.remember(primary)

    def mark(summary: JournalSummary, row: Mapping[str, Any], scope: str) -> None:
        found.mark(summary, row, scope)

    def resolve_anchor(anchor: Mapping[str, Any]) -> JournalSummary:
        full = corpus.get(anchor["journal_id"])
        if full is None:
            raise ProvenanceValidationError("referenced journal is outside the admitted corpus")
        return _anchored_snapshot(full, anchor, catalog=catalog)

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

    def enqueue_state(summary: JournalSummary, state: Mapping[str, Any], scope: str) -> None:
        found.remember(summary)
        found.db.execute(
            "INSERT OR IGNORE INTO states VALUES (?, ?, ?, ?, ?, 0)",
            (
                summary.journal_id,
                summary.journal_sha256,
                state["id"],
                scope,
                canonical_json_bytes(state),
            ),
        )
        if scope in {"member", "input-history"}:
            found.db.execute(
                "INSERT OR IGNORE INTO causal VALUES (?)",
                (canonical_json_bytes(external_reference(summary, state["id"])),),
            )

    def state_history(summary: JournalSummary, state: Mapping[str, Any], scope: str) -> None:
        objects = summary.graph_validation.objects
        occurrence = objects[state["occurrence_id"]]
        artifact = objects[occurrence["artifact_id"]]
        own_ids = {state["id"], occurrence["id"], artifact["id"]}
        context_ids: set[str] = set()
        if "source_context_id" in occurrence:
            context_ids.add(occurrence["source_context_id"])
        activities: set[str] = set()
        for row in objects.values():
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
                    relation = objects.get(relation_id) if isinstance(relation_id, str) else None
                    if relation is not None:
                        mark(summary, relation, scope)
                target, source = resolve_state(summary, row["used_state"])
                enqueue_state(target, source, "input-history")
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
    enqueue_state(primary, state, "member")
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
            enqueue_state(primary, candidate, "recorded-history")

    while pending := found.db.execute(
        "SELECT journal, prefix, state, scope, value FROM states WHERE done = 0 LIMIT 1"
    ).fetchone():
        journal_id, prefix_sha256, state_id, scope, encoded = pending
        (anchor,) = found.db.execute(
            "SELECT anchor FROM snapshots WHERE journal = ? AND prefix = ?",
            (journal_id, prefix_sha256),
        ).fetchone()
        state_history(resolve_anchor(json.loads(anchor)), json.loads(encoded), scope)
        found.db.execute(
            "UPDATE states SET done = 1 WHERE journal = ? AND prefix = ? "
            "AND state = ? AND scope = ?",
            (journal_id, prefix_sha256, state_id, scope),
        )

    history = closure.store.descriptor(history_binding)
    if (history.artifact_id, history.bytes, history.sha256) != (
        member.artifact_id,
        member.bytes,
        member.sha256,
    ) or history.primary.to_mapping() != {
        "journal": binding.journal.model_dump(mode="json"),
        "delivery_association_id": binding.delivery_association_id,
    }:
        raise ProvenanceValidationError(
            "index history differs from the exact primary/member binding"
        )
    closure.resolve(history_binding, extent=RETAINED_HISTORY_EXTENT)
    found.db.execute(
        "INSERT INTO views VALUES (?, ?, ?, 1, 0)",
        (
            history_binding.history_sha256,
            RETAINED_HISTORY_EXTENT,
            canonical_json_bytes(history_binding.to_mapping()),
        ),
    )
    while pending := found.db.execute(
        "SELECT history, extent, binding, own FROM views WHERE done = 0 LIMIT 1"
    ).fetchone():
        history_id, extent, encoded, own = pending
        selected_binding = MemberHistoryBinding.from_mapping(json.loads(encoded))
        selected_history = closure.store.descriptor(selected_binding)
        selected_primary = closure.summary_at(selected_history.primary.journal)
        selected_state = selected_primary.graph_validation.objects[
            selected_primary.graph_validation.objects[
                selected_history.primary.delivery_association_id
            ]["state"]["object_id"]
        ]
        endpoints = [external_reference(selected_primary, selected_state["id"])]
        occurrence = selected_primary.graph_validation.objects[selected_state["occurrence_id"]]
        endpoints.extend(
            external_reference(selected_primary, identity)
            for identity in (occurrence["id"], occurrence["artifact_id"])
        )
        for row in selected_primary.graph_validation.view.get("relations", ()):
            if row["type"] == "generation" and row["state"] == {
                "scope": "local",
                "object_id": selected_state["id"],
                "object_type": "state",
            }:
                endpoints.append(external_reference(selected_primary, row["activity_id"]))
        applicable = (
            bool(own)
            or found.db.execute(
                "SELECT 1 FROM causal WHERE reference = ?", (canonical_json_bytes(endpoints[0]),)
            ).fetchone()
            is not None
        )
        if applicable:
            for root in closure.store.roots(selected_binding, extent=BOUND_HISTORY_EXTENT):
                selected = closure.summary_at(root.journal)
                for row in selected.graph_validation.view.get("extensions", ()):
                    subject = row["subject"]
                    exact = (
                        subject
                        if subject["scope"] == "external"
                        else external_reference(selected, subject["object_id"])
                    )
                    if exact in endpoints:
                        mark(selected, row, "member" if own else "input-history")
        for imported in closure.store.imports(selected_binding):
            source = closure.store.source_proof(imported).binding
            found.db.execute(
                "INSERT OR IGNORE INTO views VALUES (?, ?, ?, 0, 0)",
                (source.history_sha256, imported.extent, canonical_json_bytes(source.to_mapping())),
            )
        found.db.execute(
            "UPDATE views SET done = 1 WHERE history = ? AND extent = ?", (history_id, extent)
        )

    for anchor in closure.snapshots():
        snapshot = closure.summary_at(anchor)
        for row in _assertions(snapshot):
            mark(snapshot, row, "recorded-history")
    return found


def relevance_row_keys(
    relevance: MemberRelevance,
) -> Iterator[tuple[str, str]]:
    """Translate exact assertions into content-addressed index rows and scopes."""
    yield from relevance.row_keys()


def snapshots_for_relevance(
    relevance: MemberRelevance,
    *,
    corpus: Mapping[str, JournalSummary],
    catalog: ContractCatalog | None = None,
) -> Iterator[JournalSummary]:
    """Reconstruct only exact selected prefix snapshots from admitted full journals."""
    for anchor in relevance.anchors():
        full = corpus.get(anchor["journal_id"])
        if full is None:
            raise ProvenanceValidationError("relevant journal is outside the admitted corpus")
        yield _anchored_snapshot(full, anchor, catalog=catalog)


__all__ = [
    "RelevanceKey",
    "MemberRelevance",
    "member_relevance",
    "relevance_row_keys",
    "snapshots_for_relevance",
]
