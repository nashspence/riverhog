"""Focused structural interface declarations for observer contract owners."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, cast

from pydantic import JsonValue
from stove0_protocol.jcs import canonical_json_bytes
from stove0_protocol.models import ObserverContract, WorkArtifactSubject
from stove0_protocol.observation_interfaces import (
    OBSERVATION_INTERFACE_SEMANTICS,
    ObservationInterface,
    ObservationInterfaceConformanceVectors,
    ObservationInterfacePayload,
    ObservationInterfaceVector,
    SubjectPort,
)
from stove0_protocol.observation_views import (
    SubjectView,
    project_interface,
)
from stove0_protocol.predicates import read_pointer

from stove0_observer_protocol.conformance import SemanticFactsConformanceVectors


def interface_subject_ports(
    interface: ObservationInterface,
    subjects: tuple[WorkArtifactSubject, ...],
    options: dict[str, JsonValue],
) -> dict[str, tuple[str, ...]]:
    ports: dict[str, tuple[str, ...]] = {}
    for name, port in interface.inputs.items():
        if not isinstance(port, SubjectPort):
            continue
        members = (
            [subject.id for subject in subjects]
            if port.option_ids_at is None
            else read_pointer(options, port.option_ids_at)
        )
        if not isinstance(members, list) or not all(isinstance(member, str) for member in members):
            raise ValueError("interface subject port does not name exact subject IDs")
        ports[name] = tuple(cast(list[str], members))
    return ports


def verify_interface_vectors(
    interface: ObservationInterface,
    contract: ObserverContract,
    vectors: ObservationInterfaceConformanceVectors,
) -> None:
    interface.validate_contract(contract)
    if (
        vectors.interface_id != interface.id
        or vectors.sha256 != interface.conformance_vectors_sha256
    ):
        raise ValueError("interface vectors differ from their exact committed document")
    for vector in vectors.vectors:
        accepted = True
        try:
            projected = project_interface(
                interface=interface,
                contract=contract,
                subjects=tuple(subject.id for subject in vector.subjects),
                ports=interface_subject_ports(interface, vector.subjects, vector.options),
                facts=vector.facts,
            )
        except ValueError:
            accepted = False
        if accepted != vector.accepted:
            raise ValueError(f"observation interface conformance differs: {vector.id}")
        if accepted and vector.expected_views is not None:
            actual = {}
            for name, view in projected.items():
                actual[name] = (
                    tuple(row for subject in sorted(view.rows) for row in view.rows[subject])
                    if isinstance(view, SubjectView)
                    else tuple(view.records)
                )
            if canonical_json_bytes(actual) != canonical_json_bytes(vector.expected_views):
                raise ValueError(f"observation interface expected view differs: {vector.id}")


def seal_owned_interface(
    *,
    contract: ObserverContract,
    id: str,
    inputs: dict[str, Any],
    views: dict[str, Any],
    partitioning: str,
    empty_scope: str,
    vectors: tuple[ObservationInterfaceVector, ...],
) -> tuple[ObservationInterface, ObservationInterfaceConformanceVectors]:
    document = ObservationInterfaceConformanceVectors(
        interface_id=id, vectors=tuple(sorted(vectors, key=lambda item: item.id))
    )
    interface = ObservationInterface.seal(
        ObservationInterfacePayload.model_validate(
            {
                "id": id,
                "observer_contract": {"id": contract.id, "sha256": contract.contract_sha256},
                "facts_profile": {
                    "id": contract.facts_schema.id,
                    "sha256": contract.facts_schema.profile_sha256,
                },
                "semantic_profile": {
                    "id": contract.facts_semantics.id,
                    "sha256": contract.facts_semantics.profile_sha256,
                },
                "inputs": inputs,
                "views": views,
                "partitioning": partitioning,
                "empty_scope": empty_scope,
                "interface_semantics": {
                    "id": OBSERVATION_INTERFACE_SEMANTICS.id,
                    "sha256": OBSERVATION_INTERFACE_SEMANTICS.profile_sha256,
                },
                "conformance_vectors_sha256": document.sha256,
            }
        )
    )
    verify_interface_vectors(interface, contract, document)
    return interface, document


def subject_interface(
    *,
    contract: ObserverContract,
    facts_vectors: SemanticFactsConformanceVectors,
    subject_at: str,
    id: str,
    additional_views: dict[str, Any] | None = None,
    partitioning: str = "independent-subjects",
    status: dict[str, Any] | None = None,
) -> tuple[ObservationInterface, ObservationInterfaceConformanceVectors]:
    sample = next(vector for vector in facts_vectors.vectors if vector.accepted)
    missing = deepcopy(sample.facts)
    missing["artifacts"] = []
    foreign = deepcopy(sample.facts)
    field = subject_at.removeprefix("/")
    artifacts = foreign["artifacts"]
    if not isinstance(artifacts, list) or not artifacts or not isinstance(artifacts[0], dict):
        raise ValueError("subject interface vector lacks an artifact fact record")
    artifacts[0][field] = "unrequested-subject"
    vectors = (
        ObservationInterfaceVector(
            id="accepted-complete-empty", accepted=True, subjects=(), options={}, facts=None
        ),
        ObservationInterfaceVector(
            id="accepted-exact-subjects",
            accepted=True,
            subjects=sample.subjects,
            options=sample.options,
            facts=sample.facts,
        ),
        ObservationInterfaceVector(
            id="rejected-missing-subjects",
            accepted=False,
            subjects=sample.subjects,
            options=sample.options,
            facts=missing,
        ),
        ObservationInterfaceVector(
            id="rejected-unrequested-subject",
            accepted=False,
            subjects=sample.subjects,
            options=sample.options,
            facts=foreign,
        ),
    )
    return seal_owned_interface(
        contract=contract,
        id=id,
        inputs={"subjects": {"kind": "subjects"}},
        views={
            "artifacts": {
                "kind": "subject-facts",
                "records": {"records_at": "/artifacts"},
                "subject_at": subject_at,
                "record_schema_at": "/properties/artifacts/items",
                "status": status,
            },
            **(additional_views or {}),
        },
        partitioning=partitioning,
        empty_scope="complete-empty",
        vectors=vectors,
    )
