"""Durable task acceptance and bounded view sealing in Stove0's own database.

Original observer envelopes remain original envelopes. Projected records and hash
checkpoints are controller operational state; they never become observer testimony.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, cast

from pydantic import BaseModel, JsonValue
from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    ForeignKey,
    Index,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    and_,
    func,
    or_,
    select,
    union_all,
    update,
)
from sqlalchemy.dialects.postgresql import Insert as PgInsert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import Insert as SqliteInsert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine
from sqlalchemy.sql.elements import ColumnElement
from stove0_observer_protocol import (
    ContentObservationEvidence,
    ContentObservationRequest,
    ObserverDescriptor,
    SemanticValidatorProvider,
    accept_observation_result,
    validate_observation_request,
)
from stove0_observer_protocol.interfaces import interface_subject_ports
from stove0_observer_protocol.validation import semantic_status_resolver
from stove0_protocol import (
    ArtifactSelection,
    ArtifactSelectionRef,
    WorkArtifactSubject,
    canonical_json_bytes,
)
from stove0_protocol.accepted_inputs import (
    AcceptedEvidenceInput,
    AcceptedInput,
    AcceptedInputPayload,
)
from stove0_protocol.models import ObserverContract
from stove0_protocol.observation_evidence import (
    OBSERVATION_EVIDENCE_PAGE_MAX,
    AcceptedEvidencePage,
    AcceptedEvidenceSet,
    AcceptedEvidenceSetPayload,
    AcceptedView,
    AcceptedViewPage,
    AcceptedViewPayload,
    AcceptedViewRecord,
    EvidenceResultRef,
    EvidenceResultSetRef,
    ObservationQuestion,
    update_accepted_view_commitment,
    update_evidence_result_commitment,
)
from stove0_protocol.observation_interfaces import (
    EvidencePort,
    ExactDocumentRef,
    ObservationInterface,
    RelationView,
)
from stove0_protocol.observation_views import (
    GlobalView,
    ProjectedView,
    RelationViewResult,
    SubjectView,
    project_interface,
)
from stove0_protocol.predicates import MISSING, pointer_parts, read_pointer

from stove0_core._checkpoint_sha256 import CheckpointSHA256
from stove0_core.planning_context import PlanningContext
from stove0_core.subject_identity import subject_identity_sha256


def _json(model: BaseModel) -> str:
    return canonical_json_bytes(model.model_dump(mode="json", by_alias=True)).decode("utf-8")


def declare_observation_tables(metadata: MetaData) -> dict[str, Table]:
    question = Table(
        "stove0_observation_questions",
        metadata,
        Column("question_sha256", String(129), primary_key=True),
        Column(
            "context_id",
            String(64),
            ForeignKey("stove0_planning_contexts.context_id", ondelete="CASCADE"),
        ),
        Column("work_id", String(129), nullable=False),
        Column("task_id", Text, nullable=False),
        Column("scope_sha256", String(64), nullable=False),
        Column("question_json", Text, nullable=False),
        Column("state", String(16), nullable=False),
        Column("result_ordinal", BigInteger, nullable=False),
        Column("hash_state", Text, nullable=False),
        Column("evidence_set_sha256", String(64)),
        Column("evidence_set_json", Text),
        UniqueConstraint("work_id", "task_id", name="uq_stove0_observation_task"),
        CheckConstraint(
            "state IN ('collecting','sealing','complete')",
            name="ck_stove0_observation_question_state",
        ),
        CheckConstraint("result_ordinal >= 0", name="ck_stove0_observation_result_ordinal"),
        Index("ix_stove0_observation_questions_pending", "state", "work_id", "task_id"),
    )
    physical = Table(
        "stove0_observation_results",
        metadata,
        Column("request_id", String(129), primary_key=True),
        Column(
            "question_sha256",
            String(129),
            ForeignKey(question.c.question_sha256, ondelete="CASCADE"),
            nullable=False,
        ),
        Column("request_json", Text, nullable=False),
        Column("descriptor_json", Text, nullable=False),
        Column("evidence_json", Text),
        Column("ref_json", Text),
        Column("result_ordinal", BigInteger),
        UniqueConstraint(
            "question_sha256", "result_ordinal", name="uq_stove0_observation_result_ordinal"
        ),
        CheckConstraint(
            "result_ordinal IS NULL OR result_ordinal >= 0",
            name="ck_stove0_observation_physical_ordinal",
        ),
        Index("ix_stove0_observation_results_task", "question_sha256", "request_id"),
    )
    coverage = Table(
        "stove0_observation_coverage",
        metadata,
        Column(
            "question_sha256",
            String(129),
            ForeignKey(question.c.question_sha256, ondelete="CASCADE"),
            primary_key=True,
        ),
        Column("subject_id", Text, primary_key=True),
        Column(
            "request_id",
            String(129),
            ForeignKey(physical.c.request_id, ondelete="CASCADE"),
            nullable=False,
        ),
    )
    records = Table(
        "stove0_observation_view_records",
        metadata,
        Column(
            "question_sha256",
            String(129),
            ForeignKey(question.c.question_sha256, ondelete="CASCADE"),
            primary_key=True,
        ),
        Column("view_id", Text, primary_key=True),
        Column(
            "request_id",
            String(129),
            ForeignKey(physical.c.request_id, ondelete="CASCADE"),
            primary_key=True,
        ),
        Column("record_ordinal", BigInteger, primary_key=True),
        Column("subject_id", Text),
        Column("kind", String(16), nullable=False),
        Column("primary_id", Text),
        Column("associated_id", Text),
        Column("record_json", Text, nullable=False),
        CheckConstraint("record_ordinal >= 0", name="ck_stove0_observation_view_record_ordinal"),
        Index(
            "ix_stove0_observation_view_subject", "question_sha256", "view_id", "subject_id", "kind"
        ),
        Index(
            "ix_stove0_observation_view_relation",
            "question_sha256",
            "view_id",
            "associated_id",
            "primary_id",
        ),
    )
    view = Table(
        "stove0_accepted_views",
        metadata,
        Column("view_key", String(129), primary_key=True),
        Column(
            "question_sha256",
            String(129),
            ForeignKey(question.c.question_sha256, ondelete="CASCADE"),
            nullable=False,
        ),
        Column("view_id", Text, nullable=False),
        Column("scope_sha256", String(64), nullable=False),
        Column("state", String(16), nullable=False),
        Column("record_count", BigInteger, nullable=False),
        Column("after_request_id", Text, nullable=False),
        Column("after_record_ordinal", BigInteger, nullable=False),
        Column("hash_state", Text, nullable=False),
        Column("view_sha256", String(129), unique=True),
        Column("view_json", Text),
        CheckConstraint("state IN ('sealing','complete')", name="ck_stove0_accepted_view_state"),
        CheckConstraint("record_count >= 0", name="ck_stove0_accepted_view_count"),
    )
    page = Table(
        "stove0_accepted_view_members",
        metadata,
        Column(
            "view_key",
            String(129),
            ForeignKey(view.c.view_key, ondelete="CASCADE"),
            primary_key=True,
        ),
        Column("record_ordinal", BigInteger, primary_key=True),
        Column("record_json", Text, nullable=False),
        CheckConstraint("record_ordinal >= 0", name="ck_stove0_accepted_view_member_ordinal"),
    )
    accepted_input = Table(
        "stove0_accepted_observation_inputs",
        metadata,
        Column("input_sha256", String(129), primary_key=True),
        Column(
            "question_sha256",
            String(129),
            ForeignKey(question.c.question_sha256, ondelete="CASCADE"),
            nullable=False,
        ),
        Column("scope_sha256", String(64), nullable=False),
        Column("input_json", Text, nullable=False),
    )
    return {
        "question": question,
        "physical": physical,
        "coverage": coverage,
        "records": records,
        "view": view,
        "input": accepted_input,
        "page": page,
    }


@dataclass(frozen=True, slots=True)
class ObservationSelectionPorts:
    """Existing selection authority, not a second artifact inventory."""

    reference: Callable[[str], ArtifactSelectionRef | None]
    member: Callable[[str, str], WorkArtifactSubject | None]
    retain: Callable[[ArtifactSelection], None]


class AcceptedObservationStore:
    def __init__(
        self,
        engine: Engine,
        tables: Mapping[str, Table],
        selections: ObservationSelectionPorts,
        selection_members: Table,
        *,
        context: PlanningContext,
    ) -> None:
        self.engine, self.tables, self.selections = engine, tables, selections
        self.members = selection_members
        self.key, self.context_id = context.key, context.context_id

    def _insert(self, table: Table) -> PgInsert | SqliteInsert:
        return (
            pg_insert(table) if self.engine.dialect.name == "postgresql" else sqlite_insert(table)
        )

    def _scope(self, reference: ArtifactSelectionRef) -> None:
        if self.selections.reference(reference.selection_sha256) != reference:
            raise ValueError("exact task/view selection is unavailable")

    def ensure_question(self, question: ObservationQuestion) -> ObservationQuestion:
        # Re-parse nested mutable JSON before retaining an exact authority.
        question = ObservationQuestion.model_validate_json(_json(question))
        existing = self.question(question.work_id, question.task_id)
        if existing is not None:
            if existing != question:
                raise ValueError("named task was rebound to another exact logical question")
            return existing
        self._scope(question.scope)
        for reference in question.subject_ports.values():
            self._scope(reference)
        for predecessor in question.evidence_ports.values():
            accepted = self.accepted(question.work_id, predecessor.task_id)
            if accepted is None or (
                accepted.question.question_sha256,
                accepted.evidence_set_sha256,
                accepted.question.interface,
                accepted.question.scope,
            ) != (
                predecessor.question_sha256,
                predecessor.evidence_set_sha256,
                predecessor.interface,
                predecessor.scope,
            ):
                raise ValueError("logical evidence input has no exact completed predecessor")
        m = self.members
        with self.engine.connect() as connection:
            parts = []
            for reference in question.subject_ports.values():
                part = select(m.c.artifact_id, m.c.document_json).where(
                    m.c.selection_sha256 == reference.selection_sha256
                )
                parts.append(part)
            ports = union_all(*parts).subquery()
            source = m.alias("question_scope")
            invalid = connection.scalar(
                select(ports.c.artifact_id)
                .where(
                    ~select(source.c.artifact_id)
                    .where(
                        source.c.selection_sha256 == question.scope.selection_sha256,
                        source.c.artifact_id == ports.c.artifact_id,
                        source.c.document_json == ports.c.document_json,
                    )
                    .exists()
                )
                .limit(1)
            )
            count = connection.scalar(select(func.count()).select_from(ports))
            duplicate = connection.scalar(
                select(ports.c.artifact_id)
                .group_by(ports.c.artifact_id)
                .having(func.count() > 1)
                .limit(1)
            )
            if (
                invalid is not None
                or count != question.scope.artifact_count
                or duplicate is not None
            ):
                raise ValueError("logical question subject ports must be a disjoint exact union")
        q = self.tables["question"]
        with self.engine.begin() as connection:
            connection.execute(
                self._insert(q)
                .values(
                    question_sha256=self.key(question.question_sha256),
                    context_id=self.context_id,
                    work_id=self.key(question.work_id),
                    task_id=question.task_id,
                    scope_sha256=question.scope.selection_sha256,
                    question_json=_json(question),
                    state="collecting",
                    result_ordinal=0,
                    hash_state=CheckpointSHA256().export_state(),
                )
                .on_conflict_do_nothing()
            )
            current = connection.scalar(
                select(q.c.question_json).where(
                    q.c.work_id == self.key(question.work_id), q.c.task_id == question.task_id
                )
            )
            if current != _json(question):
                raise ValueError("named task was rebound to another exact logical question")
        return question

    def question(self, work_id: str, task_id: str) -> ObservationQuestion | None:
        q = self.tables["question"]
        with self.engine.connect() as connection:
            document = connection.scalar(
                select(q.c.question_json).where(
                    q.c.work_id == self.key(work_id), q.c.task_id == task_id
                )
            )
        return None if document is None else ObservationQuestion.model_validate_json(document)

    def register_request(
        self,
        question: ObservationQuestion,
        request: ContentObservationRequest,
        descriptor: ObserverDescriptor,
        interface: ObservationInterface,
    ) -> None:
        question = self.ensure_question(question)
        validate_observation_request(request, descriptor)
        if (
            request.work_id,
            request.task_id,
            request.question_sha256,
            request.interface,
            request.observer_contract_id,
            request.observer_contract_sha256,
        ) != (
            question.work_id,
            question.task_id,
            question.question_sha256,
            question.interface,
            question.observer_contract.id,
            question.observer_contract.sha256,
        ):
            raise ValueError("physical request differs from its exact named logical task")
        if interface.ref != question.interface:
            raise ValueError("physical request selected another exact interface")
        if (
            request.read_actions != question.read_actions
            or request.retrieval_policy != question.retrieve
        ):
            raise ValueError("physical request changed the logical task read authority")
        ports = interface_subject_ports(interface, request.subjects, request.options)
        for name, ids in ports.items():
            for subject_id in ids:
                actual = next(
                    (subject for subject in request.subjects if subject.id == subject_id), None
                )
                if (
                    actual is None
                    or self.selections.member(
                        question.subject_ports[name].selection_sha256, subject_id
                    )
                    != actual
                ):
                    raise ValueError("physical question changed an exact subject port")
        literal = deepcopy(request.options)
        for _name, port in interface.inputs.items():
            at = port.option_slots_at if isinstance(port, EvidencePort) else port.option_ids_at
            if at is not None:
                if read_pointer(question.options, at) is not MISSING:
                    raise ValueError("literal options collide with a generated interface field")
                parts = pointer_parts(at)
                parent = literal
                for part in parts[:-1]:
                    ancestor = parent.get(part)
                    if not isinstance(ancestor, dict):
                        raise ValueError("generated option ancestor is not an object")
                    parent = ancestor
                if not parts or parts[-1] not in parent:
                    raise ValueError("physical question omits a generated interface field")
                del parent[parts[-1]]
                # Compare only after removing the generated field; empty inserted
                # ancestors are normalized against the original literal tree.
                _remove_inserted_empty_ancestors(literal, question.options, parts[:-1])
        if canonical_json_bytes(literal) != canonical_json_bytes(question.options):
            raise ValueError("physical question changed the logical task literal options")
        for subject in request.subjects:
            if self.selections.member(question.scope.selection_sha256, subject.id) != subject:
                raise ValueError("physical request member differs from the exact task scope")
        if (
            interface.partitioning == "whole-scope"
            and len(request.subjects) != question.scope.artifact_count
        ):
            raise ValueError("whole-scope interface cannot treat a partition as complete")
        self._validate_evidence_ports(question, request, interface, ports)
        p, q = self.tables["physical"], self.tables["question"]
        with self.engine.begin() as connection:
            state = connection.scalar(
                select(q.c.state)
                .where(q.c.question_sha256 == self.key(question.question_sha256))
                .with_for_update()
            )
            existing = (
                connection.execute(select(p).where(p.c.request_id == self.key(request.request_id)))
                .mappings()
                .first()
            )
            if existing is not None:
                if existing["request_json"] != _json(request) or existing[
                    "descriptor_json"
                ] != _json(descriptor):
                    raise ValueError("physical request identity was rebound")
                return
            if state != "collecting":
                raise ValueError("sealed task cannot accept an additional physical question")
            connection.execute(
                p.insert().values(
                    request_id=self.key(request.request_id),
                    question_sha256=self.key(question.question_sha256),
                    request_json=_json(request),
                    descriptor_json=_json(descriptor),
                )
            )

    def _validate_evidence_ports(
        self,
        question: ObservationQuestion,
        request: ContentObservationRequest,
        interface: ObservationInterface,
        subject_ports: Mapping[str, tuple[str, ...]],
    ) -> None:
        """Validate sealed scoped inputs without substituting original testimony."""
        slots = {slot.slot: slot for slot in request.evidence_slots or ()}
        evidence_ports = {
            name: port for name, port in interface.inputs.items() if isinstance(port, EvidencePort)
        }
        used: set[str] = set()
        subjects = {subject.id: subject for subject in request.subjects}
        for name, port in evidence_ports.items():
            predecessor = question.evidence_ports.get(name)
            if predecessor is None:
                raise ValueError("physical evidence port has no declared predecessor task")
            accepted = self.accepted(question.work_id, predecessor.task_id)
            if accepted is None or (
                accepted.question.question_sha256 != predecessor.question_sha256
                or accepted.evidence_set_sha256 != predecessor.evidence_set_sha256
                or accepted.question.interface != predecessor.interface
                or accepted.question.scope != predecessor.scope
                or accepted.question.observer_contract not in port.contracts
                or predecessor.interface not in port.interfaces
            ):
                raise ValueError("physical evidence port changed its exact completed predecessor")
            labels = (
                read_pointer(request.options, port.option_slots_at)
                if port.option_slots_at is not None
                else sorted(slots)
                if len(evidence_ports) == 1
                else MISSING
            )
            if not isinstance(labels, list) or not all(isinstance(label, str) for label in labels):
                raise ValueError("physical evidence slots do not identify one exact named port")
            exact_labels = cast(list[str], labels)
            if (
                exact_labels != sorted(set(exact_labels))
                or not set(exact_labels) <= slots.keys()
                or used.intersection(exact_labels)
                or len(exact_labels) != 1
            ):
                raise ValueError("physical evidence slots do not identify one exact named port")
            used.update(exact_labels)
            slot = slots[exact_labels[0]]
            authority = self.accepted_input(slot.accepted_input_sha256)
            expected = {subject for covered in port.covers for subject in subject_ports[covered]}
            if (
                authority.source != accepted
                or slot.observer_contract_id != accepted.question.observer_contract.id
                or authority.selected_scope.artifact_count != len(expected)
            ):
                raise ValueError("forwarded input changed its exact predecessor or port scope")
            for subject_id in expected:
                actual = self.selections.member(
                    authority.selected_scope.selection_sha256, subject_id
                )
                target = subjects.get(subject_id)
                if (
                    actual is None
                    or target is None
                    or subject_identity_sha256(actual) != subject_identity_sha256(target)
                ):
                    raise ValueError("forwarded input exceeds or changes its evidence port scope")
        if used != slots.keys():
            raise ValueError("forwarded evidence contains undeclared port slots")

    def _accepted_input_views(
        self,
        question: ObservationQuestion,
        interface: ObservationInterface,
        predecessor_interfaces: Mapping[ExactDocumentRef, ObservationInterface],
    ) -> dict[str, dict[str, ProjectedView]]:
        from stove0_core.accepted_view_queries import accepted_view_queries

        views: dict[str, dict[str, ProjectedView]] = {}
        for name, port in interface.inputs.items():
            if not isinstance(port, EvidencePort):
                continue
            binding = question.evidence_ports[name]
            accepted = self.accepted(question.work_id, binding.task_id)
            source = predecessor_interfaces.get(binding.interface)
            if accepted is None or source is None or source.ref != binding.interface:
                raise ValueError("accepted input requires its exact retained predecessor interface")
            views[name] = {}
            for view_id in source.views:
                authority = self.retained_view(
                    accepted, view_id=view_id, selected_scope=binding.scope
                )
                if authority is None:
                    raise ValueError("accepted input view has not completed its exact sealing")
                views[name][view_id] = accepted_view_queries(self, authority, interface=source)
        return views

    def accept(
        self,
        evidence: ContentObservationEvidence,
        *,
        contract: ObserverContract,
        interface: ObservationInterface,
        subject_ports: Mapping[str, tuple[str, ...]],
        predecessor_interfaces: Mapping[ExactDocumentRef, ObservationInterface] | None = None,
        semantic_validators: SemanticValidatorProvider | None = None,
    ) -> None:
        p, q, c, records = (
            self.tables[name] for name in ("physical", "question", "coverage", "records")
        )
        request = ContentObservationRequest.model_validate_json(_json(evidence.request))
        with self.engine.connect() as connection:
            physical = (
                connection.execute(select(p).where(p.c.request_id == self.key(request.request_id)))
                .mappings()
                .first()
            )
            if physical is None or physical["request_json"] != _json(request):
                raise ValueError("observation result has no approved physical question")
            question_document = connection.scalar(
                select(q.c.question_json).where(
                    q.c.question_sha256 == self.key(physical["question_sha256"])
                )
            )
        if question_document is None:
            raise ValueError("approved physical request lost its logical question")
        question = ObservationQuestion.model_validate_json(question_document)
        descriptor = ObserverDescriptor.model_validate_json(physical["descriptor_json"])
        evidence = ContentObservationEvidence.model_validate_json(_json(evidence))
        accept_observation_result(evidence.result, request, descriptor, semantic_validators)
        if evidence.result.state != "observed":
            raise ValueError("failure or inapplicable testimony cannot complete a logical task")
        if (
            interface.ref != question.interface
            or interface.observer_contract != question.observer_contract
        ):
            raise ValueError("accepted view differs from the selected owning contracts")
        if dict(subject_ports) != interface_subject_ports(
            interface, request.subjects, request.options
        ):
            raise ValueError("acceptance subject ports differ from the approved physical question")
        self._validate_evidence_ports(question, request, interface, subject_ports)
        projected = project_interface(
            interface=interface,
            contract=contract,
            subjects=tuple(subject.id for subject in request.subjects),
            ports=subject_ports,
            facts=evidence.result.facts,
            semantic_status=semantic_status_resolver(semantic_validators),
            evidence_views=self._accepted_input_views(
                question, interface, predecessor_interfaces or {}
            ),
        )
        selection = ArtifactSelection.seal(request.subjects)
        self.selections.retain(selection)
        reference = EvidenceResultRef(
            request_id=request.request_id,
            result_sha256=evidence.result.result_sha256,
            observer_descriptor_sha256=request.observer_descriptor_sha256,
            scope=selection.ref(),
        )
        encoded = _json(evidence)
        with self.engine.begin() as connection:
            state = connection.scalar(
                select(q.c.state)
                .where(q.c.question_sha256 == self.key(question.question_sha256))
                .with_for_update()
            )
            current = connection.scalar(
                select(p.c.evidence_json).where(p.c.request_id == self.key(request.request_id))
            )
            if current is not None:
                if current != encoded:
                    raise ValueError("accepted physical result conflicts with retained testimony")
                return
            if state != "collecting":
                raise ValueError("sealed task cannot accept new testimony")
            # Unique coverage rejects overlap across producer batches, including
            # two well-formed results claiming the same member.
            for subject in request.subjects:
                if (
                    connection.scalar(
                        select(c.c.request_id).where(
                            c.c.question_sha256 == self.key(question.question_sha256),
                            c.c.subject_id == subject.id,
                        )
                    )
                    is not None
                ):
                    raise ValueError("logical task contains duplicate subject answers")
                connection.execute(
                    c.insert().values(
                        question_sha256=self.key(question.question_sha256),
                        subject_id=subject.id,
                        request_id=self.key(request.request_id),
                    )
                )
            connection.execute(
                update(p)
                .where(p.c.request_id == self.key(request.request_id))
                .values(evidence_json=encoded, ref_json=_json(reference))
            )
            for view_id, view in projected.items():
                for ordinal, (record, primary, associated) in enumerate(
                    _projected_records(view, reference)
                ):
                    connection.execute(
                        records.insert().values(
                            question_sha256=self.key(question.question_sha256),
                            view_id=view_id,
                            request_id=self.key(request.request_id),
                            record_ordinal=ordinal,
                            subject_id=record.subject_id,
                            kind=record.kind,
                            primary_id=primary,
                            associated_id=associated,
                            record_json=_json(record),
                        )
                    )

    def seal_step(
        self,
        question: ObservationQuestion,
        *,
        interface: ObservationInterface,
        limit: int = OBSERVATION_EVIDENCE_PAGE_MAX,
    ) -> AcceptedEvidenceSet | None:
        _limit(limit)
        q, p, c = (self.tables[name] for name in ("question", "physical", "coverage"))
        with self.engine.begin() as connection:
            row = (
                connection.execute(
                    select(q)
                    .where(q.c.question_sha256 == self.key(question.question_sha256))
                    .with_for_update()
                )
                .mappings()
                .first()
            )
            if (
                row is None
                or row["question_json"] != _json(question)
                or interface.ref != question.interface
            ):
                raise ValueError("task completion has no exact approved logical question")
            if row["state"] == "complete":
                return AcceptedEvidenceSet.model_validate_json(row["evidence_set_json"])
            pending = connection.scalar(
                select(p.c.request_id)
                .where(
                    p.c.question_sha256 == self.key(question.question_sha256),
                    p.c.evidence_json.is_(None),
                )
                .limit(1)
            )
            count = connection.scalar(
                select(func.count())
                .select_from(c)
                .where(c.c.question_sha256 == self.key(question.question_sha256))
            )
            if pending is not None or count != question.scope.artifact_count:
                return None
            if count == 0 and interface.empty_scope != "complete-empty":
                raise ValueError("interface does not define complete-empty task semantics")
            # Coverage is checked against immutable members, not only its count.
            invalid = connection.scalar(
                select(c.c.subject_id)
                .where(
                    c.c.question_sha256 == self.key(question.question_sha256),
                    ~select(self.members.c.artifact_id)
                    .where(
                        self.members.c.selection_sha256 == question.scope.selection_sha256,
                        self.members.c.artifact_id == c.c.subject_id,
                    )
                    .exists(),
                )
                .limit(1)
            )
            if invalid is not None:
                raise ValueError("task coverage contains an unasked exact member")
            rows = (
                connection.execute(
                    select(p)
                    .where(
                        p.c.question_sha256 == self.key(question.question_sha256),
                        p.c.result_ordinal.is_(None),
                    )
                    .order_by(p.c.request_id)
                    .limit(limit + 1)
                )
                .mappings()
                .all()
            )
            digest = CheckpointSHA256.from_state(row["hash_state"])
            ordinal = row["result_ordinal"]
            for physical in rows[:limit]:
                reference = EvidenceResultRef.model_validate_json(physical["ref_json"])
                update_evidence_result_commitment(digest, ordinal=ordinal, result=reference)
                connection.execute(
                    update(p)
                    .where(p.c.request_id == self.key(reference.request_id))
                    .values(result_ordinal=ordinal)
                )
                ordinal += 1
            if len(rows) > limit:
                connection.execute(
                    update(q)
                    .where(q.c.question_sha256 == self.key(question.question_sha256))
                    .values(
                        state="sealing", result_ordinal=ordinal, hash_state=digest.export_state()
                    )
                )
                return None
            accepted = AcceptedEvidenceSet.seal(
                AcceptedEvidenceSetPayload(
                    question=question,
                    state="complete-empty" if count == 0 else "complete",
                    results=EvidenceResultSetRef.model_validate(
                        {"result_count": str(ordinal), "results_sha256": digest.hexdigest()}
                    ),
                )
            )
            connection.execute(
                update(q)
                .where(q.c.question_sha256 == self.key(question.question_sha256))
                .values(
                    state="complete",
                    result_ordinal=ordinal,
                    hash_state=digest.export_state(),
                    evidence_set_sha256=accepted.evidence_set_sha256,
                    evidence_set_json=_json(accepted),
                )
            )
            return accepted

    def accepted(self, work_id: str, task_id: str) -> AcceptedEvidenceSet | None:
        q = self.tables["question"]
        with self.engine.connect() as connection:
            document = connection.scalar(
                select(q.c.evidence_set_json).where(
                    q.c.work_id == self.key(work_id), q.c.task_id == task_id
                )
            )
        return None if document is None else AcceptedEvidenceSet.model_validate_json(document)

    def retained_view(
        self, accepted: AcceptedEvidenceSet, *, view_id: str, selected_scope: ArtifactSelectionRef
    ) -> AcceptedView | None:
        from stove0_protocol import canonical_json_sha256

        key = canonical_json_sha256(
            {
                "evidence_set_sha256": accepted.evidence_set_sha256,
                "view_id": view_id,
                "scope": selected_scope.model_dump(mode="json"),
            }
        )
        v = self.tables["view"]
        with self.engine.connect() as connection:
            document = connection.scalar(
                select(v.c.view_json).where(
                    v.c.view_key == self.key(key),
                    v.c.state == "complete",
                )
            )
        if document is None:
            return None
        authority = AcceptedView.model_validate_json(document)
        if (
            authority.work_id,
            authority.task_id,
            authority.evidence_set_sha256,
            authority.question_sha256,
            authority.interface,
            authority.view_id,
            authority.selected_scope,
        ) != (
            accepted.question.work_id,
            accepted.question.task_id,
            accepted.evidence_set_sha256,
            accepted.question.question_sha256,
            accepted.question.interface,
            view_id,
            selected_scope,
        ):
            raise ValueError("retained view differs from its exact accepted source and scope")
        return authority

    def evidence_page(
        self,
        authority: AcceptedEvidenceSet,
        *,
        start_ordinal: int = 0,
        limit: int = 100,
        authorize: Callable[[ArtifactSelectionRef], None],
    ) -> AcceptedEvidencePage:
        _limit(limit)
        _ordinal(start_ordinal)
        authorize(authority.question.scope)
        if self.accepted(authority.question.work_id, authority.question.task_id) != authority:
            raise ValueError("accepted evidence page authority is unavailable")
        p = self.tables["physical"]
        with self.engine.connect() as connection:
            rows = connection.scalars(
                select(p.c.ref_json)
                .where(
                    p.c.question_sha256 == self.key(authority.question.question_sha256),
                    p.c.result_ordinal >= start_ordinal,
                )
                .order_by(p.c.result_ordinal)
                .limit(limit)
            ).all()
        end = start_ordinal + len(rows)
        result = AcceptedEvidencePage.model_validate(
            {
                "authority": authority,
                "start_ordinal": str(start_ordinal),
                "results": tuple(
                    EvidenceResultRef.model_validate_json(document) for document in rows
                ),
                "complete": end == authority.results.result_count,
            }
        )
        authorize(authority.question.scope)
        return result

    def original_evidence(
        self, request_id: str, *, authorize: Callable[[ArtifactSelectionRef], None]
    ) -> ContentObservationEvidence:
        p = self.tables["physical"]
        with self.engine.connect() as connection:
            ref_document = connection.scalar(
                select(p.c.ref_json).where(p.c.request_id == self.key(request_id))
            )
        if ref_document is None:
            raise KeyError(request_id)
        reference = EvidenceResultRef.model_validate_json(ref_document)
        authorize(reference.scope)
        with self.engine.connect() as connection:
            document = connection.scalar(
                select(p.c.evidence_json).where(p.c.request_id == self.key(request_id))
            )
        if document is None:
            raise KeyError(request_id)
        evidence = ContentObservationEvidence.model_validate_json(document)
        if (
            evidence.request.request_id != reference.request_id
            or evidence.result.result_sha256 != reference.result_sha256
        ):
            raise ValueError("retained original testimony differs from its accepted reference")
        authorize(reference.scope)
        return evidence

    def ensure_view(
        self,
        accepted: AcceptedEvidenceSet,
        *,
        interface: ObservationInterface,
        view_id: str,
        selected_scope: ArtifactSelectionRef,
    ) -> str:
        from stove0_protocol import canonical_json_sha256

        self._scope(selected_scope)
        if (
            self.accepted(accepted.question.work_id, accepted.question.task_id) != accepted
            or interface.ref != accepted.question.interface
        ):
            raise ValueError("view source is not the exact complete accepted task")
        definition = interface.views.get(view_id)
        if definition is None:
            raise ValueError("accepted view is not declared by the exact interface")
        from stove0_protocol.observation_interfaces import GlobalFactsView

        if isinstance(definition, GlobalFactsView) and selected_scope != accepted.question.scope:
            raise ValueError("global testimony cannot be presented as a scoped per-member view")
        m = self.members
        with self.engine.connect() as connection:
            source = m.alias("source")
            missing = (
                None
                if selected_scope == accepted.question.scope
                else connection.scalar(
                    select(m.c.artifact_id)
                    .where(
                        m.c.selection_sha256 == selected_scope.selection_sha256,
                        ~select(source.c.artifact_id)
                        .where(
                            source.c.selection_sha256 == accepted.question.scope.selection_sha256,
                            source.c.artifact_id == m.c.artifact_id,
                            source.c.member_identity_sha256 == m.c.member_identity_sha256,
                        )
                        .exists(),
                    )
                    .limit(1)
                )
            )
        if missing is not None:
            raise ValueError(
                "accepted view selection exceeds or changes the source question's members"
            )
        key = canonical_json_sha256(
            {
                "evidence_set_sha256": accepted.evidence_set_sha256,
                "view_id": view_id,
                "scope": selected_scope.model_dump(mode="json"),
            }
        )
        v = self.tables["view"]
        with self.engine.begin() as connection:
            connection.execute(
                self._insert(v)
                .values(
                    view_key=self.key(key),
                    question_sha256=self.key(accepted.question.question_sha256),
                    view_id=view_id,
                    scope_sha256=selected_scope.selection_sha256,
                    state="sealing",
                    record_count=0,
                    after_request_id="",
                    after_record_ordinal=-1,
                    hash_state=CheckpointSHA256().export_state(),
                )
                .on_conflict_do_nothing()
            )
        return key

    def input_step(
        self,
        accepted: AcceptedEvidenceSet,
        *,
        interface: ObservationInterface,
        selected_scope: ArtifactSelectionRef,
    ) -> AcceptedInput | None:
        """Seal at most one bounded view page before publishing a scoped input."""
        views = []
        for view_id in sorted(interface.views):
            key = self.ensure_view(
                accepted,
                interface=interface,
                view_id=view_id,
                selected_scope=selected_scope,
            )
            authority = self.retained_view(
                accepted,
                view_id=view_id,
                selected_scope=selected_scope,
            )
            if authority is None:
                self.view_step(key, interface=interface)
                return None
            views.append(authority)
        input_authority = AcceptedInput.seal(
            AcceptedInputPayload(
                source=accepted,
                selected_scope=selected_scope,
                views=tuple(views),
            )
        )
        table = self.tables["input"]
        document = _json(input_authority)
        with self.engine.begin() as connection:
            connection.execute(
                self._insert(table)
                .values(
                    input_sha256=self.key(input_authority.input_sha256),
                    question_sha256=self.key(accepted.question.question_sha256),
                    scope_sha256=selected_scope.selection_sha256,
                    input_json=document,
                )
                .on_conflict_do_nothing()
            )
            original = connection.scalar(
                select(table.c.input_json).where(
                    table.c.input_sha256 == self.key(input_authority.input_sha256),
                )
            )
            if original != document:
                raise ValueError("accepted input identity was rebound")
        return input_authority

    def accepted_input(self, input_sha256: str) -> AcceptedInput:
        table = self.tables["input"]
        with self.engine.connect() as connection:
            document = connection.scalar(
                select(table.c.input_json).where(
                    table.c.input_sha256 == self.key(input_sha256),
                )
            )
        if document is None:
            raise ValueError("observation input has no retained controller acceptance")
        authority = AcceptedInput.model_validate_json(document)
        if (
            self.accepted(
                authority.source.question.work_id,
                authority.source.question.task_id,
            )
            != authority.source
        ):
            raise ValueError("observation input lost its exact original task acceptance")
        return authority

    def input_delivery(
        self, input_sha256: str, *, authorize: Callable[[ArtifactSelectionRef], None]
    ) -> AcceptedEvidenceInput:
        authority = self.accepted_input(input_sha256)
        pages = []
        for view in authority.views:
            ordinal = 0
            while True:
                page = self.view_page(view, start_ordinal=ordinal, authorize=authorize)
                pages.append(page)
                ordinal += len(page.records)
                if page.complete:
                    break
        return AcceptedEvidenceInput(authority=authority, pages=tuple(pages))

    def view_step(
        self, key: str, *, interface: ObservationInterface, limit: int = 100
    ) -> AcceptedView | None:
        _limit(limit)
        v, r, page, q = (self.tables[name] for name in ("view", "records", "page", "question"))
        m = self.members
        with self.engine.begin() as connection:
            row = (
                connection.execute(select(v).where(v.c.view_key == self.key(key)).with_for_update())
                .mappings()
                .first()
            )
            if row is None:
                raise KeyError(key)
            accepted_document = connection.scalar(
                select(q.c.evidence_set_json).where(
                    q.c.question_sha256 == self.key(row["question_sha256"])
                )
            )
            if accepted_document is None:
                raise ValueError("accepted view lost its complete logical evidence set")
            accepted = AcceptedEvidenceSet.model_validate_json(accepted_document)
            if interface.ref != accepted.question.interface:
                raise ValueError("view sealing changed its exact interface")
            if row["state"] == "complete":
                return AcceptedView.model_validate_json(row["view_json"])

            def contains(field: ColumnElement[Any]) -> ColumnElement[bool]:
                return (
                    select(m.c.artifact_id)
                    .where(m.c.selection_sha256 == row["scope_sha256"], m.c.artifact_id == field)
                    .exists()
                )

            scoped = or_(
                contains(r.c.subject_id),
                and_(contains(r.c.primary_id), contains(r.c.associated_id)),
                and_(
                    r.c.subject_id.is_(None), r.c.primary_id.is_(None), r.c.associated_id.is_(None)
                ),
            )
            after = or_(
                r.c.request_id > row["after_request_id"],
                and_(
                    r.c.request_id == self.key(row["after_request_id"]),
                    r.c.record_ordinal > row["after_record_ordinal"],
                ),
            )
            rows = (
                connection.execute(
                    select(r)
                    .where(
                        r.c.question_sha256 == self.key(row["question_sha256"]),
                        r.c.view_id == row["view_id"],
                        scoped,
                        after,
                    )
                    .order_by(r.c.request_id, r.c.record_ordinal)
                    .limit(limit + 1)
                )
                .mappings()
                .all()
            )
            digest, ordinal = CheckpointSHA256.from_state(row["hash_state"]), row["record_count"]
            for record in rows[:limit]:
                document = AcceptedViewRecord.model_validate_json(record["record_json"])
                if (
                    isinstance(interface.views[row["view_id"]], RelationView)
                    and row["scope_sha256"] != accepted.question.scope.selection_sha256
                    and document.kind == "coverage"
                    and document.value == "complete"
                ):
                    outside = connection.scalar(
                        select(r.c.record_json)
                        .where(
                            r.c.question_sha256 == self.key(accepted.question.question_sha256),
                            r.c.view_id == row["view_id"],
                            r.c.kind == "relation",
                            or_(
                                and_(
                                    r.c.primary_id == document.subject_id,
                                    ~contains(r.c.associated_id),
                                ),
                                and_(
                                    r.c.associated_id == document.subject_id,
                                    ~contains(r.c.primary_id),
                                ),
                            ),
                        )
                        .limit(1)
                    )
                    if outside is not None:
                        edge = AcceptedViewRecord.model_validate_json(outside)
                        if not isinstance(edge.value, dict):
                            raise ValueError("accepted relation lacks its exact endpoint record")
                        if edge.kind != "relation" or document.subject_id not in (
                            edge.value["primary_id"],
                            edge.value["associated_id"],
                        ):
                            raise ValueError(
                                "scoped relation index differs from its exact positive evidence"
                            )
                        # A hidden positive relation cannot become a complete
                        # negative in a smaller disclosure. This is controller
                        # view coverage; original producer testimony is intact.
                        document = document.model_copy(update={"value": "insufficient"})
                update_accepted_view_commitment(digest, ordinal=ordinal, record=document)
                connection.execute(
                    page.insert().values(
                        view_key=self.key(key), record_ordinal=ordinal, record_json=_json(document)
                    )
                )
                ordinal += 1
            latest = rows[min(len(rows), limit) - 1] if rows else row
            values = {
                "record_count": ordinal,
                "hash_state": digest.export_state(),
                "after_request_id": latest["request_id"] if rows else row["after_request_id"],
                "after_record_ordinal": latest["record_ordinal"]
                if rows
                else row["after_record_ordinal"],
            }
            if len(rows) > limit:
                connection.execute(update(v).where(v.c.view_key == self.key(key)).values(**values))
                return None
            # Resolve summary through the selection port outside this transaction
            # only after all bounded record work is complete.
            scope = self.selections.reference(row["scope_sha256"])
            if scope is None:
                raise ValueError("accepted view lost its exact selected scope")
            authority = AcceptedView.seal(
                AcceptedViewPayload.model_validate(
                    {
                        "work_id": accepted.question.work_id,
                        "task_id": accepted.question.task_id,
                        "question_sha256": accepted.question.question_sha256,
                        "evidence_set_sha256": accepted.evidence_set_sha256,
                        "interface": interface.ref,
                        "view_id": row["view_id"],
                        "selected_scope": scope,
                        "view_semantics": interface.interface_semantics,
                        "record_count": str(ordinal),
                        "records_sha256": digest.hexdigest(),
                    }
                )
            )
            connection.execute(
                update(v)
                .where(v.c.view_key == self.key(key))
                .values(
                    **values,
                    state="complete",
                    view_sha256=self.key(authority.view_sha256),
                    view_json=_json(authority),
                )
            )
            return authority

    def view_page(
        self,
        authority: AcceptedView,
        *,
        start_ordinal: int = 0,
        limit: int = 100,
        authorize: Callable[[ArtifactSelectionRef], None],
    ) -> AcceptedViewPage:
        _limit(limit)
        _ordinal(start_ordinal)
        authorize(authority.selected_scope)
        v, p = self.tables["view"], self.tables["page"]
        with self.engine.connect() as connection:
            row = connection.execute(
                select(v.c.view_key, v.c.view_json).where(
                    v.c.view_sha256 == self.key(authority.view_sha256), v.c.state == "complete"
                )
            ).first()
            if row is None or row.view_json != _json(authority):
                raise ValueError("accepted view page has no exact sealed authority")
            documents = connection.scalars(
                select(p.c.record_json)
                .where(p.c.view_key == self.key(row.view_key), p.c.record_ordinal >= start_ordinal)
                .order_by(p.c.record_ordinal)
                .limit(limit)
            ).all()
        result = AcceptedViewPage.model_validate(
            {
                "authority": authority,
                "start_ordinal": str(start_ordinal),
                "records": tuple(
                    AcceptedViewRecord.model_validate_json(document) for document in documents
                ),
                "complete": start_ordinal + len(documents) == authority.record_count,
            }
        )
        authorize(authority.selected_scope)
        return result


def _limit(limit: int) -> None:
    if type(limit) is not int or not 1 <= limit <= OBSERVATION_EVIDENCE_PAGE_MAX:
        raise ValueError("observation page/continuation budget must be between 1 and 100")


def _ordinal(value: int) -> None:
    if type(value) is not int or value < 0:
        raise ValueError("observation continuation ordinal must be nonnegative")


def _projected_records(
    view: ProjectedView, reference: EvidenceResultRef
) -> Iterator[tuple[AcceptedViewRecord, str | None, str | None]]:
    if isinstance(view, (SubjectView, RelationViewResult)):
        for subject, status in sorted(view.statuses.items()):
            yield (
                AcceptedViewRecord(
                    support=(reference,), kind="coverage", subject_id=subject, value=status
                ),
                None,
                None,
            )
    if isinstance(view, SubjectView):
        for subject, rows in sorted(view.rows.items()):
            for row in rows:
                yield (
                    AcceptedViewRecord(
                        support=(reference,), kind="subject", subject_id=subject, value=row
                    ),
                    None,
                    None,
                )
    elif isinstance(view, GlobalView):
        for global_record in view.records:
            yield (
                AcceptedViewRecord(support=(reference,), kind="global", value=global_record),
                None,
                None,
            )
    else:
        for primary, associated in view.edges:
            yield (
                AcceptedViewRecord(
                    support=(reference,),
                    kind="relation",
                    value={"primary_id": primary, "associated_id": associated},
                ),
                primary,
                associated,
            )


def _remove_inserted_empty_ancestors(
    document: dict[str, JsonValue], literal: dict[str, JsonValue], path: tuple[str, ...]
) -> None:
    for size in range(len(path), 0, -1):
        parent = document
        for part in path[: size - 1]:
            ancestor = parent[part]
            if not isinstance(ancestor, dict):
                raise ValueError("generated option ancestor is not an object")
            parent = ancestor
        key = path[size - 1]
        if (
            parent.get(key) == {}
            and read_pointer(
                literal,
                "/" + "/".join(part.replace("~", "~0").replace("/", "~1") for part in path[:size]),
            )
            is MISSING
        ):
            del parent[key]
