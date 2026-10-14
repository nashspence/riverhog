"""Current named-question fixtures over published Stove0 contracts."""

import hashlib

from stove0_observer_protocol import ContentObservationRequestPayload
from stove0_protocol import ArtifactSelection, canonical_json_sha256
from stove0_protocol.accepted_inputs import (
    AcceptedEvidenceInput,
    AcceptedInput,
    AcceptedInputPayload,
)
from stove0_protocol.observation_evidence import (
    AcceptedEvidenceSet,
    AcceptedEvidenceSetPayload,
    AcceptedView,
    AcceptedViewPage,
    AcceptedViewPayload,
    AcceptedViewRecord,
    EvidenceResultRef,
    EvidenceResultSetRef,
    ObservationQuestion,
    ObservationQuestionPayload,
    update_accepted_view_commitment,
    update_evidence_result_commitment,
)
from stove0_protocol.observation_interfaces import (
    OBSERVATION_INTERFACE_SEMANTICS,
    ExactDocumentRef,
    GlobalFactsView,
    ObservationInterface,
    ObservationInterfacePayload,
    SubjectPort,
)
from stove0_protocol.observation_views import GlobalView, SubjectView, project_interface
from stove0_protocol.predicates import read_pointer


def fixture_interface(contract):
    """A test-owned global view for synthetic observer contracts."""
    result = ObservationInterface.seal(
        ObservationInterfacePayload(
            id="fixture.whole-question/v1",
            observer_contract=ExactDocumentRef(id=contract.id, sha256=contract.contract_sha256),
            facts_profile=ExactDocumentRef(
                id=contract.facts_schema.id, sha256=contract.facts_schema.profile_sha256
            ),
            semantic_profile=ExactDocumentRef(
                id=contract.facts_semantics.id, sha256=contract.facts_semantics.profile_sha256
            ),
            inputs={"subjects": SubjectPort()},
            views={"facts": GlobalFactsView(record_at="", record_schema_at="")},
            partitioning="whole-scope",
            empty_scope="inapplicable",
            interface_semantics=ExactDocumentRef(
                id=OBSERVATION_INTERFACE_SEMANTICS.id,
                sha256=OBSERVATION_INTERFACE_SEMANTICS.profile_sha256,
            ),
            conformance_vectors_sha256=canonical_json_sha256({"fixture": "lifecycle-port"}),
        )
    )
    result.validate_contract(contract)
    return result


def observation_payload(*, contract, interface=None, task_id="facts", **fields):
    """Seal the actual logical question before constructing its physical request."""
    interface = interface or fixture_interface(contract)
    subjects = fields["subjects"]
    scope = ArtifactSelection.seal(subjects).ref()
    ports = {}
    options = fields.get("options", {})
    for name, port in interface.inputs.items():
        if not isinstance(port, SubjectPort):
            continue
        ids = read_pointer(options, port.option_ids_at) if port.option_ids_at else None
        ports[name] = ArtifactSelection.seal(
            tuple(subject for subject in subjects if ids is None or subject.id in ids)
        ).ref()
    question = ObservationQuestion.seal(
        ObservationQuestionPayload(
            work_id=fields["work_id"],
            task_id=task_id,
            observer_contract=interface.observer_contract,
            interface=interface.ref,
            scope=scope,
            subject_ports=ports,
            options=options,
            retrieve=fields.get("retrieval_policy", "available-only"),
            read_actions=contract.read_actions,
        )
    )
    return ContentObservationRequestPayload(
        task_id=task_id,
        question_sha256=question.question_sha256,
        interface=interface.ref,
        **fields,
    )


def accepted_input(evidence, *, contract, interface=None, subjects=None):
    """Synthetic controller acceptance retains the full original result reference.

    Consumer tests use published projection semantics and exact commitments.
    Actual durable acceptance and paging have separate controller witnesses.
    """
    interface = interface or fixture_interface(contract)
    request = evidence.request
    original_scope = ArtifactSelection.seal(request.subjects).ref()
    question = ObservationQuestion.seal(
        ObservationQuestionPayload(
            work_id=request.work_id,
            task_id=request.task_id,
            observer_contract=interface.observer_contract,
            interface=interface.ref,
            scope=original_scope,
            subject_ports={"subjects": original_scope},
            options=request.options,
            retrieve=request.retrieval_policy,
            read_actions=request.read_actions,
        )
    )
    assert question.question_sha256 == request.question_sha256
    reference = EvidenceResultRef(
        request_id=request.request_id,
        result_sha256=evidence.result.result_sha256,
        observer_descriptor_sha256=request.observer_descriptor_sha256,
        scope=original_scope,
    )
    digest = hashlib.sha256()
    update_evidence_result_commitment(digest, ordinal=0, result=reference)
    source = AcceptedEvidenceSet.seal(
        AcceptedEvidenceSetPayload(
            question=question,
            state="complete",
            results=EvidenceResultSetRef(result_count="1", results_sha256=digest.hexdigest()),
        )
    )
    selected = ArtifactSelection.seal(subjects or request.subjects).ref()
    selected_ids = {subject.id for subject in subjects or request.subjects}
    projected = project_interface(
        interface=interface,
        contract=contract,
        subjects=tuple(subject.id for subject in request.subjects),
        ports={"subjects": tuple(subject.id for subject in request.subjects)},
        facts=evidence.result.facts,
    )
    views, pages = [], []
    for view_id, view in sorted(projected.items()):
        rows = []
        if not isinstance(view, GlobalView):
            rows.extend(
                AcceptedViewRecord(
                    support=(reference,), kind="coverage", subject_id=id, value=status
                )
                for id, status in sorted(view.statuses.items())
                if id in selected_ids
            )
        if isinstance(view, SubjectView):
            rows.extend(
                AcceptedViewRecord(support=(reference,), kind="subject", subject_id=id, value=row)
                for id, values in sorted(view.rows.items())
                if id in selected_ids
                for row in values
            )
        elif isinstance(view, GlobalView):
            rows.extend(
                AcceptedViewRecord(support=(reference,), kind="global", value=row)
                for row in view.records
            )
        else:
            rows.extend(
                AcceptedViewRecord(
                    support=(reference,),
                    kind="relation",
                    value={"primary_id": primary, "associated_id": associated},
                )
                for primary, associated in view.edges
                if primary in selected_ids and associated in selected_ids
            )
        digest = hashlib.sha256()
        for ordinal, row in enumerate(rows):
            update_accepted_view_commitment(digest, ordinal=ordinal, record=row)
        view_authority = AcceptedView.seal(
            AcceptedViewPayload(
                work_id=question.work_id,
                task_id=question.task_id,
                question_sha256=question.question_sha256,
                evidence_set_sha256=source.evidence_set_sha256,
                interface=interface.ref,
                view_id=view_id,
                selected_scope=selected,
                view_semantics=interface.interface_semantics,
                record_count=str(len(rows)),
                records_sha256=digest.hexdigest(),
            )
        )
        views.append(view_authority)
        for offset in range(0, max(1, len(rows)), 100):
            part = tuple(rows[offset : offset + 100])
            pages.append(
                AcceptedViewPage(
                    authority=view_authority,
                    start_ordinal=str(offset),
                    records=part,
                    complete=offset + len(part) == len(rows),
                )
            )
    return AcceptedEvidenceInput(
        authority=AcceptedInput.seal(
            AcceptedInputPayload(
                source=source,
                selected_scope=selected,
                views=tuple(views),
            )
        ),
        pages=tuple(pages),
    )
