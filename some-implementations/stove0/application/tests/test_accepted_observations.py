"""Acceptance, continuation and disclosure boundaries for named observation tasks."""

from __future__ import annotations

import hashlib

import pytest
from stove0_core.persistence import SqlAlchemyStateStore
from stove0_observer_protocol import (
    ContentObservationEvidence,
    ContentObservationRequest,
    ContentObservationRequestPayload,
    ContentObservationResult,
    ContentObservationResultPayload,
    ObservationEvidenceSlot,
    SemanticFactsConformanceVectors,
)
from stove0_observer_protocol.interfaces import subject_interface
from stove0_protocol import (
    ArtifactSelection,
    CollectionRootIdentityRef,
    WorkArtifactSubject,
    canonical_json_sha256,
)
from stove0_protocol.models import (
    JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
    JsonSchemaValidationProfile,
    ObserverContract,
    ObserverContractPayload,
    ObserverContractSupport,
    ObserverDescriptor,
    ObserverDescriptorPayload,
    ObserverImplementation,
)
from stove0_protocol.observation_evidence import (
    AcceptedTaskInput,
    ObservationQuestion,
    ObservationQuestionPayload,
    update_accepted_view_commitment,
    update_evidence_result_commitment,
)
from stove0_protocol.observation_interfaces import ObservationInterface, ObservationInterfacePayload


def _subject(index):
    return WorkArtifactSubject(
        id=f"subject-{index:05}",
        role="stove0.source/v1",
        collection=CollectionRootIdentityRef(
            collection_id="1", archive_root_sha256="a" * 64, artifact_set_identity="b" * 64
        ),
        artifact_id=f"{index + 1:064x}",
        bytes="1",
        sha256="c" * 64,
    )


def _owners():
    contract = ObserverContract.seal(
        ObserverContractPayload(
            id="example.records/v1",
            options_schema=JsonSchemaValidationProfile.from_schema(
                "example.options/v1", {"type": "object"}
            ),
            facts_schema=JsonSchemaValidationProfile.from_schema(
                "example.rows/v1",
                {
                    "type": "object",
                    "required": ["artifacts"],
                    "properties": {
                        "artifacts": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "required": ["subject_id", "value"],
                                "properties": {
                                    "subject_id": {"type": "string"},
                                    "value": {"type": "string"},
                                },
                                "additionalProperties": False,
                            },
                        }
                    },
                    "additionalProperties": False,
                },
            ),
            facts_semantics=JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
        )
    )
    vectors = SemanticFactsConformanceVectors(
        profile_id="example.rows-conformance/v1",
        vectors=[
            {
                "id": "accepted",
                "accepted": True,
                "subjects": [_subject(0)],
                "facts": {"artifacts": [{"subject_id": _subject(0).id, "value": "full original"}]},
            },
            {
                "id": "rejected",
                "accepted": False,
                "subjects": [_subject(0)],
                "facts": {"artifacts": []},
            },
        ],
    )
    interface, _ = subject_interface(
        contract=contract,
        facts_vectors=vectors,
        subject_at="/subject_id",
        id="example.interface/v1",
    )
    descriptor = ObserverDescriptor.seal(
        ObserverDescriptorPayload(
            implementation_id="example.observer/v1",
            implementation_version="1",
            source_revision="fixture",
            image_id="sha256:" + "d" * 64,
            contracts=(
                ObserverContractSupport.from_contract(contract, interfaces=(interface.ref,)),
            ),
        )
    )
    return contract, interface, descriptor


def _question(store, subjects, *, task_id="probe", options=None):
    contract, interface, descriptor = _owners()
    selection = ArtifactSelection.seal(subjects)
    store.retain_selection(selection)
    question = ObservationQuestion.seal(
        ObservationQuestionPayload(
            work_id="e" * 64,
            task_id=task_id,
            observer_contract=interface.observer_contract,
            interface=interface.ref,
            scope=selection.ref(),
            subject_ports={"subjects": selection.ref()},
            options=options or {},
            read_actions=contract.read_actions,
        )
    )
    store.accepted_observations.ensure_question(question)
    return question, contract, interface, descriptor


def _evidence(question, descriptor, subjects, *, options=None):
    request = ContentObservationRequest.seal(
        ContentObservationRequestPayload(
            work_id=question.work_id,
            task_id=question.task_id,
            question_sha256=question.question_sha256,
            interface=question.interface,
            observer_registration_id="fixture",
            observer_descriptor_sha256=descriptor.descriptor_sha256,
            observer_contract_id=question.observer_contract.id,
            observer_contract_sha256=question.observer_contract.sha256,
            subjects=tuple(sorted(subjects, key=lambda s: s.id)),
            options=question.options if options is None else options,
        )
    )
    support = descriptor.support_for(request.observer_contract_id)
    facts = {
        "artifacts": [
            {"subject_id": subject.id, "value": "original " + subject.id}
            for subject in request.subjects
        ]
    }
    result = ContentObservationResult.seal(
        ContentObservationResultPayload(
            request_id=request.request_id,
            state="observed",
            observer=ObserverImplementation(
                id=descriptor.implementation_id,
                version=descriptor.implementation_version,
                source_revision=descriptor.source_revision,
                descriptor_sha256=descriptor.descriptor_sha256,
            ),
            observer_contract_id=support.contract_id,
            observer_contract_sha256=support.contract_sha256,
            subjects=request.subjects,
            facts_schema=support.facts_schema,
            facts=facts,
            facts_sha256=canonical_json_sha256(facts),
        )
    )
    return ContentObservationEvidence(request=request, result=result)


def _accept(store, question, contract, interface, descriptor, evidence):
    store.accepted_observations.register_request(question, evidence.request, descriptor, interface)
    store.accepted_observations.accept(
        evidence,
        contract=contract,
        interface=interface,
        subject_ports={"subjects": tuple(s.id for s in evidence.request.subjects)},
    )


def _complete_with_view(store, subjects, *, task_id):
    question, contract, interface, descriptor = _question(store, subjects, task_id=task_id)
    evidence = _evidence(question, descriptor, subjects)
    _accept(store, question, contract, interface, descriptor, evidence)
    accepted = store.accepted_observations.seal_step(question, interface=interface)
    assert accepted is not None
    key = store.accepted_observations.ensure_view(
        accepted, interface=interface, view_id="artifacts", selected_scope=question.scope
    )
    while store.accepted_observations.view_step(key, interface=interface) is None:
        pass
    return accepted, interface, evidence


def test_scoped_relation_disclosure_cannot_hide_positive_as_complete_negative(tmp_path):
    from test_compiled_groups import _observed_state

    state, _url, work, _subjects = _observed_state(
        tmp_path,
        ("primary", "associated"),
        preferred=((0, 1),),
    )
    accepted = state.accepted_observations.accepted(work.work_id, "relations")
    _recipe, closure = state.recipe_definitions.load(work.recipe)
    interface = closure.interface(
        id=accepted.question.interface.id, sha256=accepted.question.interface.sha256
    ).interface
    original = state.load_selection(accepted.question.scope.selection_sha256)
    selected = ArtifactSelection.seal((original.artifacts[1],))
    state.retain_selection(selected)
    key = state.accepted_observations.ensure_view(
        accepted,
        interface=interface,
        view_id="preferred",
        selected_scope=selected.ref(),
    )
    authority = state.accepted_observations.view_step(key, interface=interface)
    page = state.accepted_observations.view_page(authority, authorize=lambda scope: None)
    assert [record.value for record in page.records if record.kind == "coverage"] == [
        "insufficient"
    ]
    assert not any(record.kind == "relation" for record in page.records)
    full = state.accepted_observations.retained_view(
        accepted,
        view_id="preferred",
        selected_scope=accepted.question.scope,
    )
    full_page = state.accepted_observations.view_page(full, authorize=lambda scope: None)
    assert any(record.kind == "relation" for record in full_page.records)
    assert {record.value for record in full_page.records if record.kind == "coverage"} == {
        "complete"
    }


def test_scoped_view_can_change_recipe_role_without_changing_member_facts(tmp_path):
    store = SqlAlchemyStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    subjects = (_subject(0), _subject(1))
    accepted, interface, original = _complete_with_view(store, subjects, task_id="raw")
    selected = ArtifactSelection.seal((subjects[0].model_copy(update={"role": "media"}),))
    store.retain_selection(selected)
    key = store.accepted_observations.ensure_view(
        accepted, interface=interface, view_id="artifacts", selected_scope=selected.ref()
    )
    view = store.accepted_observations.view_step(key, interface=interface)
    assert view is not None and view.selected_scope == selected.ref()
    page = store.accepted_observations.view_page(view, authorize=lambda _: None)
    rows = [record for record in page.records if record.kind == "subject"]
    assert len(rows) == 1 and rows[0].subject_id == subjects[0].id
    assert rows[0].support[0].result_sha256 == original.result.result_sha256
    assert rows[0].support[0].scope == accepted.question.scope
    # Role assignment is mutable policy, while all immutable member facts remain exact.
    changed = ArtifactSelection.seal((subjects[0].model_copy(update={"sha256": "f" * 64}),))
    store.retain_selection(changed)
    with pytest.raises(ValueError, match="exceeds or changes"):
        store.accepted_observations.ensure_view(
            accepted, interface=interface, view_id="artifacts", selected_scope=changed.ref()
        )
    assert (
        store.accepted_observations.original_evidence(
            original.request.request_id, authorize=lambda _: None
        )
        == original
    )


def test_forwarding_requires_the_named_predecessor_and_complete_exact_port(tmp_path):
    from stove0_core.observation_questions import physical_question
    from stove0_observer_support import ContentObservationResultBuilder

    store = SqlAlchemyStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    subjects = (_subject(0), _subject(1))
    first, source_interface, first_original = _complete_with_view(store, subjects, task_id="first")
    _, _, second_original = _complete_with_view(store, subjects, task_id="second")
    contract, _, _ = _owners()
    contract = ObserverContract.seal(
        ObserverContractPayload.model_validate(
            {
                **contract.model_dump(mode="json", exclude={"contract_sha256"}),
                "id": "example.evidence-reader/v1",
                "read_actions": ["read-evidence"],
                "options_schema": JsonSchemaValidationProfile.from_schema(
                    "example.evidence-options/v1",
                    {
                        "type": "object",
                        "required": ["prior"],
                        "properties": {"prior": {"type": "array", "items": {"type": "string"}}},
                        "additionalProperties": False,
                    },
                ),
            }
        )
    )
    interface = ObservationInterface.seal(
        ObservationInterfacePayload.model_validate(
            {
                **source_interface.model_dump(mode="json", exclude={"interface_sha256"}),
                "id": "example.evidence-reader-interface/v1",
                "observer_contract": {"id": contract.id, "sha256": contract.contract_sha256},
                "inputs": {
                    "subjects": {"kind": "subjects"},
                    "provenance": {
                        "kind": "evidence",
                        "contracts": [first.question.observer_contract],
                        "interfaces": [first.question.interface],
                        "covers": ["subjects"],
                        "option_slots_at": "/prior",
                    },
                },
            }
        )
    )
    descriptor = ObserverDescriptor.seal(
        ObserverDescriptorPayload(
            implementation_id="example.evidence-reader/v1",
            implementation_version="1",
            source_revision="fixture",
            image_id="sha256:" + "d" * 64,
            contracts=(
                ObserverContractSupport.from_contract(contract, interfaces=(interface.ref,)),
            ),
        )
    )
    selected = ArtifactSelection.seal(
        tuple(s.model_copy(update={"role": "media"}) for s in subjects)
    )
    store.retain_selection(selected)
    question = ObservationQuestion.seal(
        ObservationQuestionPayload(
            work_id=first.question.work_id,
            task_id="consume",
            observer_contract=interface.observer_contract,
            interface=interface.ref,
            scope=selected.ref(),
            subject_ports={"subjects": selected.ref()},
            evidence_ports={
                "provenance": AcceptedTaskInput(
                    task_id=first.question.task_id,
                    question_sha256=first.question.question_sha256,
                    evidence_set_sha256=first.evidence_set_sha256,
                    interface=first.question.interface,
                    scope=first.question.scope,
                )
            },
            read_actions=contract.read_actions,
        )
    )

    def request(original):
        accepted = store.accepted_observations.accepted(
            original.request.work_id, original.request.task_id
        )
        for _ in range(10):
            authority = store.accepted_observations.input_step(
                accepted, interface=source_interface, selected_scope=selected.ref()
            )
            if authority is not None:
                break
        else:
            pytest.fail("scoped predecessor input did not seal")
        slot = ObservationEvidenceSlot(
            slot="prior.exact",
            accepted_input_sha256=authority.input_sha256,
            observer_contract_id=original.request.observer_contract_id,
        )
        return physical_question(
            question=question,
            interface=interface,
            registration_id="consumer",
            descriptor=descriptor,
            subjects=selected.artifacts,
            subject_ports={"subjects": tuple(s.id for s in selected.artifacts)},
            evidence_ports={"provenance": (slot,)},
        )

    assert (
        first_original.request.observer_contract_sha256
        == second_original.request.observer_contract_sha256
    )
    with pytest.raises(ValueError, match="exact predecessor or port scope"):
        store.accepted_observations.register_request(
            question, request(second_original), descriptor, interface
        )
    actual = request(first_original)
    store.accepted_observations.register_request(question, actual, descriptor, interface)
    result = ContentObservationResultBuilder(descriptor, actual).observed(
        {
            "artifacts": [
                {"subject_id": subject.id, "value": "accepted named input"}
                for subject in selected.artifacts
            ]
        }
    )
    evidence = ContentObservationEvidence(request=actual, result=result)
    with pytest.raises(ValueError, match="exact retained predecessor interface"):
        store.accepted_observations.accept(
            evidence,
            contract=contract,
            interface=interface,
            subject_ports={"subjects": tuple(s.id for s in selected.artifacts)},
        )
    store.accepted_observations.accept(
        evidence,
        contract=contract,
        interface=interface,
        subject_ports={"subjects": tuple(s.id for s in selected.artifacts)},
        predecessor_interfaces={source_interface.ref: source_interface},
    )
    assert store.accepted_observations.seal_step(question, interface=interface) is not None


def test_complete_set_and_view_seal_in_bounded_steps_across_restart(tmp_path):
    url = f"sqlite:///{tmp_path / 'state.db'}"
    store = SqlAlchemyStateStore(url)
    subjects = tuple(_subject(index) for index in range(205))
    question, contract, interface, descriptor = _question(store, subjects)
    for subject in subjects:
        _accept(
            store,
            question,
            contract,
            interface,
            descriptor,
            _evidence(question, descriptor, (subject,)),
        )
    assert store.accepted_observations.seal_step(question, interface=interface, limit=100) is None
    store.engine.dispose()
    store = SqlAlchemyStateStore(url)
    assert store.accepted_observations.seal_step(question, interface=interface, limit=100) is None
    authority = store.accepted_observations.seal_step(question, interface=interface, limit=100)
    assert authority.results.result_count == 205
    digest = hashlib.sha256()
    pages = []
    for ordinal in (0, 100, 200):
        page = store.accepted_observations.evidence_page(
            authority, start_ordinal=ordinal, authorize=lambda _: None
        )
        pages.append(page)
        for offset, result in enumerate(page.results):
            update_evidence_result_commitment(digest, ordinal=ordinal + offset, result=result)
    assert [len(page.results) for page in pages] == [100, 100, 5]
    assert pages[-1].complete
    assert digest.hexdigest() == authority.results.results_sha256
    key = store.accepted_observations.ensure_view(
        authority, interface=interface, view_id="artifacts", selected_scope=question.scope
    )
    steps = 0
    while (
        view := store.accepted_observations.view_step(key, interface=interface, limit=100)
    ) is None:
        steps += 1
        store.engine.dispose()
        store = SqlAlchemyStateStore(url)
    assert steps == 4
    assert view.record_count == 410
    digest = hashlib.sha256()
    for ordinal in range(0, 410, 100):
        page = store.accepted_observations.view_page(
            view, start_ordinal=ordinal, authorize=lambda _: None
        )
        for offset, record in enumerate(page.records):
            update_accepted_view_commitment(digest, ordinal=ordinal + offset, record=record)
    assert digest.hexdigest() == view.records_sha256
    store.engine.dispose()


def test_scoped_view_does_not_forge_or_disclose_original_larger_result(tmp_path):
    store = SqlAlchemyStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    subjects = (_subject(0), _subject(1))
    question, contract, interface, descriptor = _question(store, subjects)
    evidence = _evidence(question, descriptor, subjects)
    _accept(store, question, contract, interface, descriptor, evidence)
    accepted = store.accepted_observations.seal_step(question, interface=interface)
    selected = ArtifactSelection.seal(subjects[:1])
    store.retain_selection(selected)
    key = store.accepted_observations.ensure_view(
        accepted, interface=interface, view_id="artifacts", selected_scope=selected.ref()
    )
    view = store.accepted_observations.view_step(key, interface=interface)
    assert view.evidence_set_sha256 == accepted.evidence_set_sha256
    calls = []

    def authorize(scope):
        calls.append(scope)
        if scope != selected.ref():
            raise PermissionError("only the exact selected member is authorized")

    page = store.accepted_observations.view_page(view, authorize=authorize)
    assert calls == [selected.ref(), selected.ref()]
    assert {record.subject_id for record in page.records} == {subjects[0].id}
    assert subjects[1].id not in page.model_dump_json()
    with pytest.raises(PermissionError):
        store.accepted_observations.original_evidence(
            evidence.request.request_id, authorize=authorize
        )
    original = store.accepted_observations.original_evidence(
        evidence.request.request_id, authorize=lambda _: None
    )
    assert original == evidence
    assert original.result.result_sha256 == evidence.result.result_sha256
    authority = store.accepted_observations.input_step(
        accepted, interface=interface, selected_scope=selected.ref()
    )
    delivery = store.accepted_observations.input_delivery(
        authority.input_sha256, authorize=authorize
    )
    assert delivery.authority.source == accepted
    assert subjects[1].id not in delivery.model_dump_json()
    records = tuple(delivery.records("artifacts"))
    assert {record.subject_id for record in records} == {subjects[0].id}
    assert {support.result_sha256 for record in records for support in record.support} == {
        evidence.result.result_sha256
    }
    from stove0_protocol.accepted_inputs import AcceptedEvidenceInput

    corrupted = delivery.model_dump(mode="json")
    record = next(
        record
        for page in corrupted["pages"]
        for record in page["records"]
        if record["kind"] == "subject"
    )
    record["value"]["value"] = "forged selected fact"
    with pytest.raises(ValueError, match="exact view commitment"):
        AcceptedEvidenceInput.model_validate(corrupted)
    store.engine.dispose()


def test_same_contract_tasks_bind_distinct_logical_questions(tmp_path):
    store = SqlAlchemyStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    one, contract, interface, descriptor = _question(
        store, (_subject(0),), task_id="first", options={"answer": 1}
    )
    two, *_ = _question(store, (_subject(0),), task_id="second", options={"answer": 2})
    a, b = _evidence(one, descriptor, (_subject(0),)), _evidence(two, descriptor, (_subject(0),))
    assert (
        one.question_sha256 != two.question_sha256 and a.request.request_id != b.request.request_id
    )
    with pytest.raises(ValueError, match="exact named logical task"):
        store.accepted_observations.register_request(one, b.request, descriptor, interface)
    with pytest.raises(ValueError, match="rebound"):
        _question(store, (_subject(0),), task_id="first", options={"answer": 3})
    store.engine.dispose()


def test_fresh_planning_invocations_accept_distinct_answers_to_the_same_semantic_question(tmp_path):
    base = SqlAlchemyStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    stores = [base.planning_context("preview", value * 64) for value in ("1", "2")]
    authorities = []
    for store, answer in zip(stores, ("first answer", "second answer"), strict=True):
        question, contract, interface, descriptor = _question(store, (_subject(0),))
        original = _evidence(question, descriptor, (_subject(0),))
        document = original.result.model_dump(mode="json", exclude={"result_sha256"})
        document["facts"]["artifacts"][0]["value"] = answer
        document["facts_sha256"] = canonical_json_sha256(document["facts"])
        evidence = ContentObservationEvidence(
            request=original.request,
            result=ContentObservationResult.seal(
                ContentObservationResultPayload.model_validate(document)
            ),
        )
        _accept(store, question, contract, interface, descriptor, evidence)
        accepted = store.accepted_observations.seal_step(question, interface=interface)
        for _ in range(10):
            authority = store.accepted_observations.input_step(
                accepted,
                interface=interface,
                selected_scope=question.scope,
            )
            if authority is not None:
                break
        else:
            pytest.fail("owned accepted input did not seal")
        delivery = store.accepted_observations.input_delivery(
            authority.input_sha256, authorize=lambda _: None
        )
        assert (
            next(
                record for record in delivery.records("artifacts") if record.kind == "subject"
            ).value["value"]
            == answer
        )
        authorities.append(authority)
    assert authorities[0].source.question == authorities[1].source.question
    assert authorities[0].source.evidence_set_sha256 != authorities[1].source.evidence_set_sha256
    with pytest.raises(ValueError, match="no retained controller acceptance"):
        stores[1].accepted_observations.accepted_input(authorities[0].input_sha256)
    base.engine.dispose()


def test_missing_and_duplicate_physical_coverage_cannot_complete(tmp_path):
    store = SqlAlchemyStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    question, contract, interface, descriptor = _question(store, (_subject(0), _subject(1)))
    evidence = _evidence(question, descriptor, (_subject(0),))
    _accept(store, question, contract, interface, descriptor, evidence)
    assert store.accepted_observations.seal_step(question, interface=interface) is None
    alternate_descriptor = ObserverDescriptor.seal(
        ObserverDescriptorPayload.model_validate(
            {
                **descriptor.model_dump(mode="json", exclude={"descriptor_sha256"}),
                "source_revision": "another build",
            }
        )
    )
    duplicate = _evidence(question, alternate_descriptor, (_subject(0),))
    store.accepted_observations.register_request(
        question, duplicate.request, alternate_descriptor, interface
    )
    with pytest.raises(ValueError, match="duplicate subject"):
        store.accepted_observations.accept(
            duplicate,
            contract=contract,
            interface=interface,
            subject_ports={"subjects": (_subject(0).id,)},
        )
    assert store.accepted_observations.seal_step(question, interface=interface) is None
    store.engine.dispose()


def test_complete_empty_is_controller_completion_without_fabricated_result(tmp_path):
    store = SqlAlchemyStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    question, _, interface, _ = _question(store, ())
    accepted = store.accepted_observations.seal_step(question, interface=interface)
    assert accepted.state == "complete-empty" and accepted.results.result_count == 0
    key = store.accepted_observations.ensure_view(
        accepted, interface=interface, view_id="artifacts", selected_scope=question.scope
    )
    view = store.accepted_observations.view_step(key, interface=interface)
    assert view.record_count == 0
    assert store.accepted_observations.view_page(view, authorize=lambda _: None).complete
    assert (
        store.accepted_observations.evidence_page(accepted, authorize=lambda _: None).results == ()
    )
    store.engine.dispose()


def test_revocation_after_scoped_page_read_fails_closed(tmp_path):
    store = SqlAlchemyStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    question, contract, interface, descriptor = _question(store, (_subject(0),))
    _accept(
        store,
        question,
        contract,
        interface,
        descriptor,
        _evidence(question, descriptor, (_subject(0),)),
    )
    accepted = store.accepted_observations.seal_step(question, interface=interface)
    key = store.accepted_observations.ensure_view(
        accepted, interface=interface, view_id="artifacts", selected_scope=question.scope
    )
    view = store.accepted_observations.view_step(key, interface=interface)
    calls = 0

    def authorize(_):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise PermissionError("grant revoked during read")

    with pytest.raises(PermissionError, match="revoked"):
        store.accepted_observations.view_page(view, authorize=authorize)
    store.engine.dispose()


def test_physical_options_cannot_change_the_logical_question(tmp_path):
    store = SqlAlchemyStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    question, _, interface, descriptor = _question(store, (_subject(0),), options={"answer": 1})
    forged = _evidence(question, descriptor, (_subject(0),), options={"answer": 2})
    with pytest.raises(ValueError, match="literal options"):
        store.accepted_observations.register_request(
            question, forged.request, descriptor, interface
        )
    store.engine.dispose()


def test_question_port_union_cannot_omit_an_exact_member(tmp_path):
    store = SqlAlchemyStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    question, _, _, _ = _question(store, (_subject(0), _subject(1)))
    smaller = ArtifactSelection.seal((_subject(0),))
    store.retain_selection(smaller)
    invalid = ObservationQuestion.seal(
        ObservationQuestionPayload.model_validate(
            {
                **question.model_dump(mode="json", exclude={"question_sha256"}),
                "task_id": "invalid",
                "subject_ports": {"subjects": smaller.ref().model_dump(mode="json")},
            }
        )
    )
    with pytest.raises(ValueError, match="disjoint exact union"):
        store.accepted_observations.ensure_question(invalid)
    store.engine.dispose()


def test_lazy_planner_queries_retain_the_exact_sealed_view_scope(tmp_path):
    from stove0_core.accepted_view_queries import subject_view_queries

    store = SqlAlchemyStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    subjects = (_subject(0), _subject(1))
    question, contract, interface, descriptor = _question(store, subjects)
    _accept(
        store, question, contract, interface, descriptor, _evidence(question, descriptor, subjects)
    )
    accepted = store.accepted_observations.seal_step(question, interface=interface)
    selection = ArtifactSelection.seal(subjects[:1])
    store.retain_selection(selection)
    key = store.accepted_observations.ensure_view(
        accepted, interface=interface, view_id="artifacts", selected_scope=selection.ref()
    )
    authority = store.accepted_observations.view_step(key, interface=interface)
    view = subject_view_queries(store.accepted_observations, authority, interface=interface)
    assert tuple(view.rows) == (subjects[0].id,)
    assert len(view.statuses) == 1
    assert view.statuses[subjects[0].id] == "complete"
    assert view.rows[subjects[0].id] == (
        {"subject_id": subjects[0].id, "value": "original " + subjects[0].id},
    )
    assert subjects[1].id not in view.rows
    with pytest.raises(KeyError):
        view.statuses[subjects[1].id]
    store.engine.dispose()
