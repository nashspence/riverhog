"""Common bounded-byte observation engine; native/remote acquisition is a port."""

from __future__ import annotations

import copy
import hashlib
from collections.abc import Mapping
from typing import Any

from riverhog_provenance_contracts import ContractCatalog, canonical_document, profile_reference

from .common import artifact, assertion, evidence, new_id, reference, utc_now
from .constants import (
    OBSERVATION_PLAN,
    PACKAGE_NAME,
    PACKAGE_VERSION,
    PRIMARY_CONTENT_CATEGORY,
    PROFILE,
)
from .errors import IncompleteSourceError, ObservationError
from .graph import validate_graph
from .interface import ObservationSource
from .model import BinaryReadable, ObservationPolicy, ObservationRequest, ObservationResult


def measure(
    reader: BinaryReadable, policy: ObservationPolicy, *, expected_length: int | None = None
) -> dict[str, Any]:
    sha256 = hashlib.sha256()
    sha512 = hashlib.sha512() if policy.include_sha512 else None
    total = 0
    while True:
        # An extra byte distinguishes exact-limit EOF from a truncated observation.
        count = min(policy.hash_chunk_bytes, policy.maximum_content_bytes - total + 1)
        block = reader.read(count)
        if type(block) is not bytes or len(block) > count:
            raise ObservationError("source violates read(size) byte contract")
        if not block:
            break
        total += len(block)
        if total > policy.maximum_content_bytes:
            raise IncompleteSourceError("source exceeds maximum_content_bytes; no state emitted")
        if expected_length is not None and total > expected_length:
            raise IncompleteSourceError("source exceeds its declared length")
        sha256.update(block)
        if sha512 is not None:
            sha512.update(block)
    if expected_length is not None and total != expected_length:
        raise IncompleteSourceError("source ended before its declared boundary")
    digests = [{"algorithm": "sha-256", "value": sha256.hexdigest()}]
    if sha512 is not None:
        digests.append({"algorithm": "sha-512", "value": sha512.hexdigest()})
    return {"size_bytes": str(total), "digests": digests}


def _merge(graph: dict[str, Any], additions: Mapping[str, Any]) -> None:
    known = {row["id"]: row for values in graph.values() for row in values}
    for category, rows in additions.items():
        for supplied in rows:
            row = copy.deepcopy(supplied)
            if row["id"] in known:
                if row != known[row["id"]]:
                    raise ObservationError("source supplied conflicting support definitions")
                continue
            graph.setdefault(category, []).append(row)
            known[row["id"]] = row


class BoundedSourceObserver:
    def __init__(self, *, catalog: ContractCatalog | None = None) -> None:
        self.catalog = catalog or ContractCatalog()

    def observe(
        self, source: ObservationSource, request: ObservationRequest | None = None
    ) -> ObservationResult:
        request = request or ObservationRequest()
        if not isinstance(source, ObservationSource):
            raise TypeError("source does not implement the ObservationSource port")
        who = request.observer_agent_id
        started = utc_now()
        state_id, capture_id, observation_id = new_id(), new_id(), new_id()
        with source.open(observer_agent_id=who) as session:
            if session.expected_length is not None and (
                type(session.expected_length) is not int or session.expected_length < 0
            ):
                raise ObservationError("source declared an invalid expected length")
            if request.policy.second_content_hash and (
                not session.capabilities.repeatable or session.repeat_reader is None
            ):
                raise ObservationError("second read requested but not supported by this source")
            content = measure(
                session.reader, request.policy, expected_length=session.expected_length
            )
            if request.policy.second_content_hash:
                assert session.repeat_reader is not None
                with session.repeat_reader() as repeated:
                    again = measure(
                        repeated, request.policy, expected_length=session.expected_length
                    )
                if again != content:
                    raise IncompleteSourceError("repeated primary-content measurements disagree")
            source_evidence = session.finalize()
            ended = utc_now()
            artifact_row = (
                copy.deepcopy(dict(request.artifact))
                if request.artifact is not None
                else artifact(who)
            )
            if request.occurrence is not None:
                occurrence_row = copy.deepcopy(dict(request.occurrence))
                if occurrence_row.get("artifact_id") != artifact_row["id"]:
                    raise ObservationError("supplied occurrence and artifact differ")
                if occurrence_row.get("kind") != session.occurrence_kind:
                    raise ObservationError(
                        "supplied occurrence does not match source exposure kind"
                    )
            else:
                fields: dict[str, Any] = {
                    "artifact_id": artifact_row["id"],
                    "kind": session.occurrence_kind,
                }
                if session.source_context is not None:
                    fields["source_context_id"] = session.source_context["id"]
                occurrence_row = assertion("occurrence", who, **fields)
            if (
                session.source_context is not None
                and occurrence_row.get("source_context_id") != session.source_context["id"]
            ):
                raise ObservationError("supplied occurrence belongs to another subject context")
            measured = [evidence(who, "direct_measurement", method_uri=OBSERVATION_PLAN)]
            coverage = [
                {"profile_id": PROFILE, "category": PRIMARY_CONTENT_CATEGORY, "status": "complete"},
                *[copy.deepcopy(dict(c)) for c in source_evidence.coverage],
            ]
            partial = any(c["status"] in ("partial", "failed") for c in coverage)
            contexts: list[dict[str, Any]] = []
            roles: list[dict[str, str]] = []
            if session.source_context is not None:
                contexts.append(copy.deepcopy(dict(session.source_context)))
                roles.append({"context_id": session.source_context["id"], "role": "source"})
            if request.execution_context is not None:
                contexts.append(copy.deepcopy(dict(request.execution_context)))
                roles.append({"context_id": request.execution_context["id"], "role": "execution"})
            policy_data = {
                "hash_chunk_bytes": str(request.policy.hash_chunk_bytes),
                "maximum_content_bytes": str(request.policy.maximum_content_bytes),
                "second_content_hash": request.policy.second_content_hash,
                "include_sha512": request.policy.include_sha512,
            }
            capture_fields: dict[str, Any] = {
                "kind": "observation",
                "started_at": started,
                "ended_at": ended,
                "associations": [{"agent_id": who, "role": PROFILE + "/roles/observer"}],
                "plan_uri": OBSERVATION_PLAN,
                "outcome": "partial" if partial else "success",
                "configuration": {
                    "profile": profile_reference(
                        PROFILE + "/observers/schemas/observation-policy.json"
                    ),
                    "data": policy_data,
                },
                "configuration_sha256": hashlib.sha256(canonical_document(policy_data)).hexdigest(),
            }
            if roles:
                capture_fields["contexts"] = roles
            observation_fields: dict[str, Any] = {
                "state": reference(state_id, "state"),
                "capture_id": capture_id,
                "content": content,
                "coverage": coverage,
                "address_status": source_evidence.address_status,
                "consistency": copy.deepcopy(dict(source_evidence.consistency)),
                "capabilities": session.capabilities.to_mapping(),
            }
            if session.source_context is not None:
                observation_fields["source_context_id"] = session.source_context["id"]
            if source_evidence.metadata:
                observation_fields["metadata"] = [
                    copy.deepcopy(dict(v)) for v in source_evidence.metadata
                ]
            if source_evidence.profiles:
                observation_fields["profiles"] = [
                    copy.deepcopy(dict(v)) for v in source_evidence.profiles
                ]
            graph: dict[str, Any] = {
                "agents": [
                    assertion(
                        "agent",
                        who,
                        object_id=who,
                        kind="software",
                        name=PACKAGE_NAME,
                        version=PACKAGE_VERSION,
                    )
                ],
                "artifacts": [artifact_row],
                "occurrences": [occurrence_row],
                "states": [
                    assertion(
                        "state",
                        who,
                        object_id=state_id,
                        occurrence_id=occurrence_row["id"],
                        extent=copy.deepcopy(dict(session.extent)),
                    )
                ],
                "activities": [
                    assertion(
                        "activity",
                        who,
                        object_id=capture_id,
                        evidence_items=measured,
                        **capture_fields,
                    )
                ],
                "descriptions": [
                    assertion(
                        "observation",
                        who,
                        object_id=observation_id,
                        evidence_items=measured,
                        **observation_fields,
                    )
                ],
            }
            if contexts:
                graph["contexts"] = contexts
            for locator in source_evidence.locators:
                fields = copy.deepcopy(dict(locator))
                fields.setdefault(
                    "temporal_scope",
                    {
                        "kind": "unknown",
                        "reason": "Source adapter did not report a designation observation time.",
                    },
                )
                fields.update(target=reference(state_id, "state"), observation_id=observation_id)
                graph.setdefault("locator_bindings", []).append(
                    assertion("locator_binding", who, evidence_items=measured, **fields)
                )
            _merge(graph, source_evidence.supporting_assertions)
        validate_graph(graph, catalog=self.catalog)
        return ObservationResult.from_graph(graph, observation_id=observation_id)
