"""Build questions from compiled named ports, never observer serializer recipes."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy

from pydantic import JsonValue
from stove0_observer_protocol import (
    ContentObservationRequest,
    ContentObservationRequestPayload,
    ObservationEvidenceSlot,
    ObserverDescriptor,
    validate_observation_request,
)
from stove0_protocol import (
    ArtifactSelectionRef,
    WorkArtifactSubject,
    WorkIdentity,
)
from stove0_protocol.observation_evidence import (
    AcceptedEvidenceSet,
    AcceptedTaskInput,
    ObservationQuestion,
    ObservationQuestionPayload,
)
from stove0_protocol.observation_interfaces import EvidencePort, ObservationInterface, SubjectPort
from stove0_protocol.predicates import pointer_parts
from stove0_recipe_config.compiled import CompiledObservationTask
from stove0_recipe_config.dependencies import ObserverResource
from stove0_recipe_config.source import EvidenceInput


def logical_question(
    *,
    work: WorkIdentity,
    task_id: str,
    task: CompiledObservationTask,
    resource: ObserverResource,
    scope: ArtifactSelectionRef,
    subject_ports: Mapping[str, ArtifactSelectionRef],
    predecessors: Mapping[str, AcceptedEvidenceSet],
) -> ObservationQuestion:
    interface = resource.interface
    if task.interface != interface.ref or task.observer != interface.observer_contract:
        raise ValueError("named task selected another exact observer/interface closure")
    if set(task.inputs) != set(interface.inputs):
        raise ValueError("compiled task inputs differ from its exact interface")
    subjects = {name for name, port in interface.inputs.items() if isinstance(port, SubjectPort)}
    if set(subject_ports) != subjects:
        raise ValueError("logical task subject ports differ from its exact interface")
    evidence = {}
    for name, port in interface.inputs.items():
        if not isinstance(port, EvidencePort):
            continue
        binding = task.inputs[name]
        if not isinstance(binding, EvidenceInput):
            raise ValueError("compiled evidence port has no exact predecessor task")
        accepted = predecessors.get(binding.evidence)
        if (
            accepted is None
            or accepted.question.work_id != work.work_id
            or accepted.question.task_id != binding.evidence
        ):
            raise ValueError("evidence port does not bind the exact completed predecessor task")
        if (
            accepted.question.observer_contract not in port.contracts
            or accepted.question.interface not in port.interfaces
        ):
            raise ValueError("evidence port selected another exact contract/interface")
        evidence[name] = AcceptedTaskInput(
            task_id=binding.evidence,
            question_sha256=accepted.question.question_sha256,
            evidence_set_sha256=accepted.evidence_set_sha256,
            interface=accepted.question.interface,
            scope=accepted.question.scope,
        )
    return ObservationQuestion.seal(
        ObservationQuestionPayload(
            work_id=work.work_id,
            task_id=task_id,
            observer_contract=interface.observer_contract,
            interface=interface.ref,
            scope=scope,
            subject_ports=dict(subject_ports),
            evidence_ports=evidence,
            options=deepcopy(task.options),
            retrieve=task.retrieve,
            read_actions=resource.contract.read_actions,
        )
    )


def physical_question(
    *,
    question: ObservationQuestion,
    interface: ObservationInterface,
    registration_id: str,
    descriptor: ObserverDescriptor,
    subjects: Sequence[WorkArtifactSubject],
    subject_ports: Mapping[str, tuple[str, ...]],
    evidence_ports: Mapping[str, tuple[ObservationEvidenceSlot, ...]],
    timeout_seconds: int = 300,
    maximum_result_bytes: int | None = None,
) -> ContentObservationRequest:
    if interface.ref != question.interface:
        raise ValueError("physical question selected another exact logical interface")
    support = descriptor.support_for(question.observer_contract.id)
    if (
        support.contract_sha256 != question.observer_contract.sha256
        or question.interface not in support.interfaces
    ):
        raise ValueError("executor does not support the exact named question contracts")
    if set(subject_ports) != set(question.subject_ports) or set(evidence_ports) != set(
        question.evidence_ports
    ):
        raise ValueError("physical question ports differ from the logical question")
    options = deepcopy(question.options)
    slots: list[ObservationEvidenceSlot] = []
    value: JsonValue
    for name, port in sorted(interface.inputs.items()):
        if isinstance(port, SubjectPort):
            at, value = port.option_ids_at, list(subject_ports[name])
        else:
            selected = evidence_ports[name]
            at, value = port.option_slots_at, [slot.slot for slot in selected]
            slots.extend(selected)
        if at is not None:
            _insert_option(options, at, value)
    request = ContentObservationRequest.seal(
        ContentObservationRequestPayload(
            task_id=question.task_id,
            question_sha256=question.question_sha256,
            interface=question.interface,
            work_id=question.work_id,
            observer_registration_id=registration_id,
            observer_descriptor_sha256=descriptor.descriptor_sha256,
            observer_contract_id=question.observer_contract.id,
            observer_contract_sha256=question.observer_contract.sha256,
            read_actions=question.read_actions,
            subjects=tuple(sorted(subjects, key=lambda subject: subject.id)),
            evidence_slots=tuple(sorted(slots, key=lambda slot: slot.slot)) or None,
            options=options,
            timeout_seconds=timeout_seconds,
            maximum_result_bytes=support.maximum_result_bytes
            if maximum_result_bytes is None
            else maximum_result_bytes,
            retrieval_policy=question.retrieve,
        )
    )
    validate_observation_request(request, descriptor)
    return request


def _insert_option(document: dict[str, JsonValue], pointer: str, value: JsonValue) -> None:
    parts = pointer_parts(pointer)
    if not parts:
        raise ValueError("generated port cannot replace the literal options root")
    parent = document
    for part in parts[:-1]:
        if part not in parent:
            parent[part] = {}
        ancestor = parent[part]
        if not isinstance(ancestor, dict):
            raise ValueError("generated port has a non-object option ancestor")
        parent = ancestor
    if parts[-1] in parent:
        raise ValueError("generated port collides with literal question options")
    parent[parts[-1]] = value
