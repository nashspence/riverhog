"""Expose exact canonical facts without interpreting source profiles or filenames."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from riverhog_protocol import CollectionArtifactProvenanceBindingDocument
from riverhog_protocol.collection_production_provenance import COLLECTION_MEMBER_ROLE
from riverhog_provenance import JournalSummary
from stove0_observer_protocol import WorkArtifactSubject


def _origins(summary: JournalSummary) -> dict[str, dict[str, str]]:
    origins: dict[str, dict[str, str]] = {}
    for frame in summary.frames:
        for rows in frame.document["body"].get("assertions", {}).values():
            for row in rows:
                assertion_id = row["assertion_id"]
                if assertion_id in origins:
                    raise ValueError("canonical assertion identity appears more than once")
                origins[assertion_id] = frame.reference
    return origins


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


def extract_core_facts(
    subject: WorkArtifactSubject,
    binding: CollectionArtifactProvenanceBindingDocument,
    summary: JournalSummary,
    *,
    predicates: Sequence[str] = (),
    resolve_external: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Read the exact selected primary snapshot and direct observed locator claims.

    Foreign relation endpoints require an exact corpus resolver. Until one is
    supplied, they fail closed instead of looking absent to a recipe.
    """

    if (
        binding.artifact_id != subject.artifact_id
        or binding.journal.model_dump(mode="json") != summary.anchor
    ):
        raise ValueError("primary canonical snapshot differs from the frozen subject")
    if tuple(predicates) != tuple(sorted(set(predicates))):
        raise ValueError("requested predicate list must be unique and ordered")
    if len(predicates) > 64 or any(not value or len(value) > 2048 for value in predicates):
        raise ValueError("requested predicate scope is invalid")
    objects = summary.graph_validation.objects
    origins = _origins(summary)
    association = objects.get(binding.delivery_association_id)
    if association is None or association["type"] != "delivery_association":
        raise ValueError("primary delivery association is absent")
    if association["role"] != COLLECTION_MEMBER_ROLE or association["slot"] != {
        "kind": "text",
        "text": subject.artifact_id,
    }:
        raise ValueError("primary delivery association names another member slot")
    state = _local_object(objects, association["state"], "state")
    occurrence = objects.get(state["occurrence_id"])
    if occurrence is None or occurrence["type"] != "occurrence":
        raise ValueError("selected canonical State has no Occurrence")
    measurement = objects.get(association["verification_observation_id"])
    if (
        measurement is None
        or measurement["type"] != "observation"
        or measurement["state"]
        != {"scope": "local", "object_id": state["id"], "object_type": "state"}
        or int(measurement["content"]["size_bytes"]) != int(subject.bytes)
        or (
            "sha-256",
            subject.sha256,
        )
        not in {(item["algorithm"], item["value"]) for item in measurement["content"]["digests"]}
    ):
        raise ValueError("primary canonical observation differs from member fixity")
    state_endpoint = _endpoint(summary, origins, state)
    occurrence_endpoint = _endpoint(summary, origins, occurrence)
    locators: list[dict[str, Any]] = []
    for row in summary.graph.get("locator_bindings", ()):
        if row["type"] != "locator_binding":
            continue
        target = row["target"]
        if target.get("scope") != "local" or target.get("object_id") not in {
            state["id"],
            occurrence["id"],
        }:
            continue
        observation = objects.get(row.get("observation_id"))
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
    for row in summary.graph.get("extensions", ()):
        if row["property"] not in requested:
            continue
        reference = row["subject"]
        if reference.get("scope") != "local" or reference.get("object_id") not in {
            state["id"],
            occurrence["id"],
        }:
            continue
        value = row["value"]
        object_endpoint = None
        if value["type"] == "reference":
            target = value["value"]
            if target.get("scope") == "local":
                referent = _local_object(objects, target, target["object_type"])
                object_endpoint = _endpoint(summary, origins, referent)
            elif target.get("scope") == "external":
                if resolve_external is None:
                    raise ValueError("foreign canonical claim requires an exact corpus resolver")
                object_endpoint = dict(resolve_external(target))
                if object_endpoint != {key: item for key, item in target.items() if key != "scope"}:
                    raise ValueError("resolved foreign endpoint differs from the exact reference")
            else:
                raise ValueError("canonical reference scope is invalid")
        claims.append(
            {
                "predicate": row["property"],
                "subject": (
                    state_endpoint if reference["object_id"] == state["id"] else occurrence_endpoint
                ),
                "value_type": value["type"],
                "object": object_endpoint,
                "value": None if object_endpoint is not None else value,
                "evidence": row["evidence"],
                "support": _support(summary, origins, row),
            }
        )
    return {
        "subject_id": subject.id,
        "state": state_endpoint,
        "occurrence": occurrence_endpoint,
        "locators": locators,
        "claims": claims,
        "materialization_hint": occurrence.get("materialization_hint"),
    }
