"""Expose exact canonical facts without interpreting source profiles or filenames."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any

from riverhog_archive_contracts import (
    BOUND_HISTORY_EXTENT,
    MemberHistoryBinding,
    MemberHistoryDocument,
)
from riverhog_protocol import CollectionArtifactProvenanceBindingDocument
from riverhog_protocol.collection_production_provenance import COLLECTION_MEMBER_ROLE
from riverhog_provenance import JournalSummary, selected_delivery_occurrence
from stove0_observer_protocol import WorkArtifactSubject


def _origins(summary: JournalSummary) -> Mapping[str, dict[str, str]]:
    return summary.assertion_entries


def _endpoint(
    summary: JournalSummary,
    origins: Mapping[str, dict[str, str]],
    row: Mapping[str, Any],
) -> dict[str, Any]:
    entry = origins.get(row["assertion_id"])
    if entry is None:
        raise ValueError("effective canonical assertion lacks an exact entry")
    return {
        "journal_id": summary.journal_id,
        "entry": entry,
        "assertion_id": row["assertion_id"],
        "object_id": row["id"],
        "object_type": row["type"],
    }


def _support(
    summary: JournalSummary,
    origins: Mapping[str, dict[str, str]],
    row: Mapping[str, Any],
) -> dict[str, Any]:
    endpoint = _endpoint(summary, origins, row)
    return {
        "journal": summary.anchor,
        "entry": endpoint["entry"],
        "assertion_id": endpoint["assertion_id"],
        "referent_id": endpoint["object_id"],
        "pointer": "",
    }


def _local_object(
    objects: Mapping[str, dict[str, Any]], reference: Mapping[str, Any], kind: str
) -> dict[str, Any]:
    if reference.get("scope") != "local" or reference.get("object_type") != kind:
        raise ValueError("selected canonical reference is not local and exact")
    object_id = reference.get("object_id")
    if not isinstance(object_id, str):
        raise ValueError("selected canonical reference has no object identity")
    row = objects.get(object_id)
    if row is None or row["type"] != kind:
        raise ValueError("selected canonical reference has no matching object")
    return row


def _delivered_occurrence(
    subject: WorkArtifactSubject,
    binding: CollectionArtifactProvenanceBindingDocument,
    summary: JournalSummary,
) -> tuple[
    Mapping[str, dict[str, Any]],
    Mapping[str, dict[str, str]],
    dict[str, Any],
    dict[str, Any],
]:
    """Resolve only the root-selected primary member's directly verified Occurrence."""

    if (
        binding.artifact_id != subject.artifact_id
        or binding.journal.model_dump(mode="json") != summary.anchor
    ):
        raise ValueError("primary canonical snapshot differs from the frozen subject")
    objects = summary.graph_validation.objects
    origins = _origins(summary)
    state, occurrence = selected_delivery_occurrence(
        summary,
        binding=binding.model_dump(mode="json"),
        artifact_id=subject.artifact_id,
        byte_count=int(subject.bytes),
        sha256=subject.sha256,
        member_role=COLLECTION_MEMBER_ROLE,
    )
    return objects, origins, state, occurrence


def extract_core_facts(
    subject: WorkArtifactSubject,
    binding: CollectionArtifactProvenanceBindingDocument,
    summary: JournalSummary,
    *,
    history: MemberHistoryDocument,
    selected_summaries: Iterable[JournalSummary],
    predicates: Sequence[str] = (),
    resolve_external: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Read primary locator/hint facts and requested claims from explicit bound roots.

    Foreign relation endpoints require an exact corpus resolver. Until one is
    supplied, they fail closed instead of looking absent to a recipe.
    """

    if tuple(predicates) != tuple(sorted(set(predicates))):
        raise ValueError("requested predicate list must be unique and ordered")
    if len(predicates) > 64 or any(not value or len(value) > 2048 for value in predicates):
        raise ValueError("requested predicate scope is invalid")
    objects, origins, state, occurrence = _delivered_occurrence(subject, binding, summary)
    state_endpoint = _endpoint(summary, origins, state)
    occurrence_endpoint = _endpoint(summary, origins, occurrence)
    artifact = objects[occurrence["artifact_id"]]
    artifact_endpoint = _endpoint(summary, origins, artifact)
    if (
        history.primary.journal.to_mapping() != summary.anchor
        or history.primary.delivery_association_id != binding.delivery_association_id
        or (history.artifact_id, history.bytes, history.sha256)
        != (subject.artifact_id, int(subject.bytes), subject.sha256)
    ):
        raise ValueError("selected history differs from the exact delivered member")
    endpoints = [state_endpoint, occurrence_endpoint, artifact_endpoint]
    producing_activity = None
    generation_support = None
    for relation in summary.graph_validation.view.get("relations", ()):
        if relation["type"] == "generation" and relation["state"] == reference_endpoint(
            state_endpoint
        ):
            activity = objects[relation["activity_id"]]
            producing_activity = _endpoint(summary, origins, activity)
            generation_support = _support(summary, origins, relation)
            endpoints.append(producing_activity)
    locators: list[dict[str, Any]] = []
    for row in summary.graph_validation.view.get("locator_bindings", ()):
        if row["type"] != "locator_binding":
            continue
        target = row["target"]
        if target.get("scope") != "local" or target.get("object_id") not in {
            state["id"],
            occurrence["id"],
        }:
            continue
        observation_id = row.get("observation_id")
        observation = objects.get(observation_id) if isinstance(observation_id, str) else None
        if (
            observation is None
            or observation["type"] != "observation"
            or observation["state"]
            != {"scope": "local", "object_id": state["id"], "object_type": "state"}
        ):
            continue
        context = objects.get(row["context_id"])
        if context is None or context["type"] != "context":
            raise ValueError("observed locator has no exact context assertion")
        locators.append(
            {
                "subject_id": subject.id,
                "locator": row["locator"],
                "context_endpoint": _endpoint(summary, origins, context),
                "context_identifiers": context.get("identifiers", []),
                "context_support": _support(summary, origins, context),
                "locator_support": _support(summary, origins, row),
                "temporal_scope": row["temporal_scope"],
                "observation_endpoint": _endpoint(summary, origins, observation),
            }
        )
    claims: list[dict[str, Any]] = []
    requested = set(predicates)
    primary_seen = False
    for selected in selected_summaries:
        if selected.journal_id == summary.journal_id:
            if selected.anchor != summary.anchor or primary_seen:
                raise ValueError("bound roots substitute or repeat the exact primary snapshot")
            primary_seen = True
        selected_objects = selected.graph_validation.objects
        selected_origins = _origins(selected)
        for row in selected.graph_validation.view.get("extensions", ()):
            if row["property"] not in requested:
                continue
            attached = row["subject"]
            if attached["scope"] == "local":
                endpoint = _endpoint(
                    selected, selected_origins, selected_objects[attached["object_id"]]
                )
            else:
                endpoint = {key: value for key, value in attached.items() if key != "scope"}
            if endpoint not in endpoints:
                if endpoint["object_id"] in {item["object_id"] for item in endpoints}:
                    raise ValueError(
                        "requested claim targets an incompatible historical member endpoint"
                    )
                continue
            if attached["scope"] == "external":
                resolve_exact_endpoint(attached, resolve_external)
            value = row["value"]
            object_endpoint = None
            if value["type"] == "reference":
                target = value["value"]
                if target["scope"] == "local":
                    referent = _local_object(selected_objects, target, target["object_type"])
                    object_endpoint = _endpoint(selected, selected_origins, referent)
                else:
                    object_endpoint = resolve_exact_endpoint(target, resolve_external)
            claims.append(
                {
                    "predicate": row["property"],
                    "subject": endpoint,
                    "value_type": value["type"],
                    "object": object_endpoint,
                    "value": None if object_endpoint is not None else value,
                    "evidence": row["evidence"],
                    "support": _support(selected, selected_origins, row),
                }
            )
    if not primary_seen:
        raise ValueError("bound history omits its exact primary snapshot")
    history_binding = MemberHistoryBinding(
        history.artifact_id,
        history.bytes,
        history.sha256,
        history.identity,
        len(history.to_json_bytes()),
    )
    return {
        "subject_id": subject.id,
        "primary_binding": binding.model_dump(mode="json"),
        "history_binding": history_binding.to_mapping(),
        "member_history": history.to_mapping(),
        "history_extent": BOUND_HISTORY_EXTENT,
        "state": state_endpoint,
        "occurrence": occurrence_endpoint,
        "artifact": artifact_endpoint,
        "producing_activity": producing_activity,
        "generation_support": generation_support,
        "locators": locators,
        "claims": claims,
        "materialization_hint": occurrence.get("materialization_hint"),
    }


def reference_endpoint(endpoint: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "scope": "local",
        "object_id": endpoint["object_id"],
        "object_type": endpoint["object_type"],
    }


def resolve_exact_endpoint(
    target: Mapping[str, Any],
    resolve_external: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None,
) -> dict[str, Any]:
    if resolve_external is None:
        raise ValueError("foreign canonical claim requires an exact corpus resolver")
    result = dict(resolve_external(target))
    if result != {key: value for key, value in target.items() if key != "scope"}:
        raise ValueError("resolved foreign endpoint differs from the exact reference")
    return result


def extract_materialization_hint_fact(
    subject: WorkArtifactSubject,
    binding: CollectionArtifactProvenanceBindingDocument,
    summary: JournalSummary,
) -> dict[str, Any]:
    """Forward advice from the exact delivered Occurrence, with its canonical support."""

    _, origins, _, occurrence = _delivered_occurrence(subject, binding, summary)
    return {
        "subject_id": subject.id,
        "primary_binding": binding.model_dump(mode="json"),
        "occurrence": {"scope": "external", **_endpoint(summary, origins, occurrence)},
        "materialization_hint": occurrence.get("materialization_hint"),
    }
