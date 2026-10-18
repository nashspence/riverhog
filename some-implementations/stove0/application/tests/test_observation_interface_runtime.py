"""Independent contract-owned status and predecessor interface forms work after acceptance."""

from copy import deepcopy

import pytest
from stove0_core.accepted_view_queries import accepted_view_queries
from stove0_core.persistence import SqlAlchemyStateStore
from stove0_observer_protocol import (
    ObserverContractSupport,
    ObserverDescriptor,
    ObserverDescriptorPayload,
    SemanticFactsConformanceVectors,
    SemanticValidatorBinding,
    SemanticValidatorRegistry,
)
from stove0_observer_protocol.interfaces import seal_owned_interface, verify_interface_vectors
from stove0_observer_protocol.validation import semantic_status_resolver
from stove0_protocol import (
    ArtifactSelection,
    SemanticValidationProfile,
    SemanticValidationProfilePayload,
    canonical_json_sha256,
)
from stove0_protocol.models import (
    ContentObservationResult,
    ContentObservationResultPayload,
    JsonSchemaValidationProfile,
    ObserverContract,
    ObserverContractPayload,
)
from stove0_protocol.observation_evidence import ObservationQuestion, ObservationQuestionPayload
from stove0_protocol.observation_interfaces import (
    ObservationInterfaceEvidenceContext,
    ObservationInterfaceVector,
)
from test_accepted_observations import _evidence, _owners, _subject


def _semantic_owner():
    old, _, _ = _owners()
    subjects = (_subject(0), _subject(1))
    facts = {
        "artifacts": [
            {"subject_id": s.id, "value": value}
            for s, value in zip(subjects, ("complete", "unsupported"), strict=True)
        ]
    }
    conformance = SemanticFactsConformanceVectors(
        profile_id="example.status/v1",
        vectors=[
            {"id": "accepted", "accepted": True, "subjects": subjects, "facts": facts},
            {"id": "missing", "accepted": False, "subjects": subjects, "facts": {"artifacts": []}},
        ],
    )
    profile = SemanticValidationProfile.seal(
        SemanticValidationProfilePayload(
            id=conformance.profile_id,
            rules=("example.status/v1",),
            conformance_vectors_sha256=conformance.sha256,
        )
    )
    document = old.model_dump(mode="json", exclude={"contract_sha256"})
    document["facts_semantics"] = profile.model_dump(mode="json")
    contract = ObserverContract.seal(ObserverContractPayload.model_validate(document))
    statuses = {s.id: row["value"] for s, row in zip(subjects, facts["artifacts"], strict=True)}
    vectors = (
        ObservationInterfaceVector(
            id="accepted",
            accepted=True,
            subjects=subjects,
            options={},
            facts=facts,
            semantic_statuses=statuses,
        ),
        ObservationInterfaceVector(
            id="missing",
            accepted=False,
            subjects=subjects,
            options={},
            facts={"artifacts": []},
            semantic_statuses=statuses,
        ),
    )
    interface, interface_vectors = seal_owned_interface(
        contract=contract,
        id="example.semantic-interface/v1",
        inputs={"subjects": {"kind": "subjects"}},
        views={
            "artifacts": {
                "kind": "subject-facts",
                "records": {"records_at": "/artifacts"},
                "subject_at": "/subject_id",
                "record_schema_at": "/properties/artifacts/items",
                "cardinality": "one-per-subject",
                "status": {
                    "kind": "semantic-profile",
                    "profile": {"id": profile.id, "sha256": profile.profile_sha256},
                },
            }
        },
        partitioning="independent-subjects",
        empty_scope="complete-empty",
        vectors=vectors,
    )

    def validate(request, document):
        rows = document["artifacts"]
        if {row["subject_id"] for row in rows} != {s.id for s in request.subjects} or any(
            row["value"] not in {"complete", "unsupported"} for row in rows
        ):
            raise ValueError("semantic facts differ from their exact scope")

    def status(covered, document):
        rows = {row["subject_id"]: row["value"] for row in document["artifacts"]}
        return {subject: rows[subject] for subject in covered}

    binding = SemanticValidatorBinding.from_profile(profile, validate, status_resolver=status)
    descriptor = ObserverDescriptor.seal(
        ObserverDescriptorPayload(
            implementation_id="example.semantic/v1",
            implementation_version="1",
            source_revision="fixture",
            image_id="sha256:" + "d" * 64,
            contracts=(
                ObserverContractSupport.from_contract(contract, interfaces=(interface.ref,)),
            ),
        )
    )
    return contract, interface, interface_vectors, descriptor, binding, subjects, facts


@pytest.mark.parametrize("enabled", [False, True])
def test_exact_semantic_status_resolver_is_required_and_acceptance_survives_restart(
    tmp_path, enabled
):
    contract, interface, vectors, descriptor, binding, subjects, facts = _semantic_owner()
    provider = SemanticValidatorRegistry(
        (
            binding
            if enabled
            else SemanticValidatorBinding.from_profile(contract.facts_semantics, binding.validator),
        )
    )
    url = f"sqlite:///{tmp_path / 'semantic.db'}"
    state = SqlAlchemyStateStore(url)
    selection = ArtifactSelection.seal(subjects)
    state.retain_selection(selection)
    question = ObservationQuestion.seal(
        ObservationQuestionPayload(
            work_id="e" * 64,
            task_id="semantic",
            observer_contract=interface.observer_contract,
            interface=interface.ref,
            scope=selection.ref(),
            subject_ports={"subjects": selection.ref()},
            read_actions=contract.read_actions,
        )
    )
    state.accepted_observations.ensure_question(question)
    original = _evidence(question, descriptor, subjects)
    document = original.result.model_dump(mode="json", exclude={"result_sha256"})
    document.update(facts=facts, facts_sha256=canonical_json_sha256(facts))
    evidence = original.model_copy(
        update={
            "result": ContentObservationResult.seal(
                ContentObservationResultPayload.model_validate(document)
            )
        }
    )
    store = state.accepted_observations
    store.register_request(question, evidence.request, descriptor, interface)
    if not enabled:
        with pytest.raises(ValueError, match="exact semantic completeness"):
            store.accept(
                evidence,
                contract=contract,
                interface=interface,
                subject_ports={"subjects": tuple(s.id for s in subjects)},
                semantic_validators=provider,
            )
        assert store.accepted(question.work_id, question.task_id) is None
        state.engine.dispose()
        return
    verify_interface_vectors(
        interface, contract, vectors, semantic_status=semantic_status_resolver(provider)
    )
    store.accept(
        evidence,
        contract=contract,
        interface=interface,
        subject_ports={"subjects": tuple(s.id for s in subjects)},
        semantic_validators=provider,
    )
    accepted = store.seal_step(question, interface=interface)
    key = store.ensure_view(
        accepted, interface=interface, view_id="artifacts", selected_scope=selection.ref()
    )
    while store.view_step(key, interface=interface) is None:
        pass
    authority = store.retained_view(accepted, view_id="artifacts", selected_scope=selection.ref())
    state.engine.dispose()
    state = SqlAlchemyStateStore(url)
    view = accepted_view_queries(state.accepted_observations, authority, interface=interface)
    assert dict(view.statuses) == {subjects[0].id: "complete", subjects[1].id: "unsupported"}
    assert state.accepted_observations.accepted(question.work_id, question.task_id) == accepted
    state.engine.dispose()


def test_status_resolver_with_the_wrong_exact_profile_cannot_substitute():
    contract, interface, vectors, _, binding, _, _ = _semantic_owner()
    provider = SemanticValidatorRegistry(
        (
            SemanticValidatorBinding(
                binding.profile_id, "f" * 64, binding.validator, binding.status_resolver
            ),
        )
    )
    with pytest.raises(ValueError, match="conformance differs"):
        verify_interface_vectors(
            interface, contract, vectors, semantic_status=semantic_status_resolver(provider)
        )


def _predecessor_owner():
    previous, source_interface, _ = _owners()
    subjects = (_subject(0), _subject(1))
    predecessor = ObservationInterfaceEvidenceContext(
        contract=previous,
        interface=source_interface,
        subjects=subjects,
        options={},
        facts={
            "artifacts": [
                {"subject_id": s.id, "value": "key-" + str(i)} for i, s in enumerate(subjects)
            ]
        },
    )
    schema = {
        "type": "object",
        "properties": {
            "links": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {"left": {"type": "string"}, "right": {"type": "string"}},
                    "required": ["left", "right"],
                },
            },
            "statuses": {"type": "array", "items": {"type": "object"}},
        },
        "required": ["links", "statuses"],
    }
    document = previous.model_dump(mode="json", exclude={"contract_sha256"})
    document.update(
        id="example.links/v1",
        read_actions=["read-evidence"],
        facts_schema=JsonSchemaValidationProfile.from_schema(
            "example.links-facts/v1", schema
        ).model_dump(mode="json"),
    )
    contract = ObserverContract.seal(ObserverContractPayload.model_validate(document))
    facts = {
        "links": [{"left": "key-0", "right": subjects[1].id}],
        "statuses": [{"subject": s.id, "status": "ok"} for s in subjects],
    }
    wrong = predecessor.model_copy(
        update={"subjects": (subjects[0].model_copy(update={"sha256": "f" * 64}), subjects[1])}
    )
    ambiguous_facts = deepcopy(predecessor.facts)
    ambiguous_facts["artifacts"][1]["value"] = "key-0"
    ambiguous = predecessor.model_copy(update={"facts": ambiguous_facts})
    vectors = tuple(
        ObservationInterfaceVector(
            id=name,
            accepted=accepted,
            subjects=subjects,
            options={},
            facts=facts,
            evidence=context,
            expected_views={"links": tuple(facts["links"])} if accepted else None,
        )
        for name, accepted, context in (
            ("accepted", True, {"prior": predecessor}),
            ("missing", False, {}),
            ("wrong-subject", False, {"prior": wrong}),
            ("ambiguous", False, {"prior": ambiguous}),
        )
    )
    interface, conformance = seal_owned_interface(
        contract=contract,
        id="example.links-interface/v1",
        inputs={
            "subjects": {"kind": "subjects"},
            "prior": {
                "kind": "evidence",
                "contracts": [source_interface.observer_contract.model_dump(mode="json")],
                "interfaces": [source_interface.ref.model_dump(mode="json")],
                "covers": ["subjects"],
            },
        },
        views={
            "links": {
                "kind": "relation",
                "records": {"records_at": "/links"},
                "where": True,
                "require": True,
                "primary": {
                    "kind": "exact-endpoint",
                    "at": "/left",
                    "lookup": {
                        "source": {"input": "prior"},
                        "view": "artifacts",
                        "keys": ["/value"],
                    },
                },
                "associated": {"kind": "subject-id", "at": "/right"},
                "coverage": ["subjects"],
                "status": {
                    "kind": "records",
                    "records_at": "/statuses",
                    "subject_at": "/subject",
                    "value_at": "/status",
                    "values": {"ok": "complete"},
                },
            }
        },
        partitioning="whole-scope",
        empty_scope="complete-empty",
        vectors=vectors,
    )
    return predecessor, contract, interface, conformance, facts


def test_exact_predecessor_endpoint_vectors_have_positive_and_fail_closed_contexts():
    _, contract, interface, conformance, _ = _predecessor_owner()
    verify_interface_vectors(interface, contract, conformance)


def test_predecessor_endpoint_projects_original_accepted_evidence_across_restart(tmp_path):
    from stove0_core.observation_questions import physical_question
    from stove0_observer_support import ContentObservationResultBuilder
    from stove0_protocol.models import ContentObservationEvidence, ObservationEvidenceSlot
    from stove0_protocol.observation_evidence import AcceptedTaskInput
    from test_accepted_observations import _accept, _question

    predecessor, contract, interface, _, facts = _predecessor_owner()
    url = f"sqlite:///{tmp_path / 'predecessor.db'}"
    state = SqlAlchemyStateStore(url)
    subjects = predecessor.subjects
    first_question, first_contract, first_interface, first_descriptor = _question(
        state, subjects, task_id="source"
    )
    original = _evidence(first_question, first_descriptor, subjects)
    result_document = original.result.model_dump(mode="json", exclude={"result_sha256"})
    result_document.update(
        facts=predecessor.facts, facts_sha256=canonical_json_sha256(predecessor.facts)
    )
    original = original.model_copy(
        update={
            "result": ContentObservationResult.seal(
                ContentObservationResultPayload.model_validate(result_document)
            )
        }
    )
    _accept(state, first_question, first_contract, first_interface, first_descriptor, original)
    store = state.accepted_observations
    first = store.seal_step(first_question, interface=first_interface)
    assert first is not None
    key = store.ensure_view(
        first, interface=first_interface, view_id="artifacts", selected_scope=first_question.scope
    )
    while store.view_step(key, interface=first_interface) is None:
        pass
    for _ in range(10):
        authority = store.input_step(
            first, interface=first_interface, selected_scope=first_question.scope
        )
        if authority is not None:
            break
    else:
        pytest.fail("exact predecessor input did not seal")
    state.engine.dispose()
    state = SqlAlchemyStateStore(url)
    store = state.accepted_observations
    descriptor = ObserverDescriptor.seal(
        ObserverDescriptorPayload(
            implementation_id="example.links/v1",
            implementation_version="1",
            source_revision="fixture",
            image_id="sha256:" + "d" * 64,
            contracts=(
                ObserverContractSupport.from_contract(contract, interfaces=(interface.ref,)),
            ),
        )
    )
    question = ObservationQuestion.seal(
        ObservationQuestionPayload(
            work_id=first_question.work_id,
            task_id="links",
            observer_contract=interface.observer_contract,
            interface=interface.ref,
            scope=first_question.scope,
            subject_ports={"subjects": first_question.scope},
            evidence_ports={
                "prior": AcceptedTaskInput(
                    task_id=first_question.task_id,
                    question_sha256=first_question.question_sha256,
                    evidence_set_sha256=first.evidence_set_sha256,
                    interface=first_question.interface,
                    scope=first_question.scope,
                )
            },
            read_actions=contract.read_actions,
        )
    )
    request = physical_question(
        question=question,
        interface=interface,
        registration_id="fixture",
        descriptor=descriptor,
        subjects=subjects,
        subject_ports={"subjects": tuple(s.id for s in subjects)},
        evidence_ports={
            "prior": (
                ObservationEvidenceSlot(
                    slot="prior.exact",
                    accepted_input_sha256=authority.input_sha256,
                    observer_contract_id=first_contract.id,
                ),
            )
        },
    )
    evidence = ContentObservationEvidence(
        request=request, result=ContentObservationResultBuilder(descriptor, request).observed(facts)
    )
    store.register_request(question, request, descriptor, interface)
    with pytest.raises(ValueError, match="exact retained predecessor interface"):
        store.accept(
            evidence,
            contract=contract,
            interface=interface,
            subject_ports={"subjects": tuple(s.id for s in subjects)},
        )
    store.accept(
        evidence,
        contract=contract,
        interface=interface,
        subject_ports={"subjects": tuple(s.id for s in subjects)},
        predecessor_interfaces={first_interface.ref: first_interface},
    )
    accepted = store.seal_step(question, interface=interface)
    assert accepted is not None
    key = store.ensure_view(
        accepted, interface=interface, view_id="links", selected_scope=question.scope
    )
    while store.view_step(key, interface=interface) is None:
        pass
    retained = store.retained_view(accepted, view_id="links", selected_scope=question.scope)
    state.engine.dispose()
    state = SqlAlchemyStateStore(url)
    store = state.accepted_observations
    view = accepted_view_queries(store, retained, interface=interface)
    assert tuple(view.edges) == ((subjects[0].id, subjects[1].id),)
    assert dict(view.statuses) == {s.id: "complete" for s in subjects}
    assert store.accepted(question.work_id, question.task_id) == accepted
    assert store.accepted(first_question.work_id, first_question.task_id) == first
    state.engine.dispose()
