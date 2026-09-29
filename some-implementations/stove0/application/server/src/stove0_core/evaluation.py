"""Generic materialized trial and bounded evaluation-run aggregation.

An evaluation is not another transformation ontology. It expands one immutable
matrix into ordinary stove0 work records. A completed variant exposes one
settled collection, produced directly or by its exact branch or join plan.
Each work remains independently fenced, verified, retryable, and recoverable.
"""

from __future__ import annotations

import threading
from typing import Literal, Protocol, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator
from stove0_operator_contracts import (
    validate_evaluation_child_shape,
    validate_evaluation_review_shape,
    validate_evaluation_state_shape,
)
from stove0_protocol import BranchPlan, EvaluationDefinition
from stove0_target_protocol import OutputCollectionRef
from time_formats import CanonicalUtcTimestamp

from stove0_core.work_state import Stove0WorkService, WorkRecord

EvaluationPhase = Literal[
    "planning",
    "running",
    "partially_complete",
    "complete",
    "failed",
    "canceled",
]
EvaluationChildState = Literal[
    "pending",
    "active",
    "complete",
    "no_action",
    "inapplicable",
    "failed",
    "canceled",
]


class ConcurrentEvaluationUpdate(RuntimeError):
    pass


class EvaluationModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class EvaluationChild(EvaluationModel):
    variant_id: str = Field(min_length=1, max_length=160)
    work_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    state: EvaluationChildState = "pending"
    output: OutputCollectionRef | None = None

    @model_validator(mode="after")
    def validate_output(self) -> Self:
        validate_evaluation_child_shape(self.state, self.output)
        return self


class EvaluationReview(EvaluationModel):
    variant_id: str = Field(min_length=1, max_length=160)
    rating: int | None = Field(default=None, ge=1, le=5)
    note: str | None = Field(default=None, max_length=4000)
    updated_by: str = Field(min_length=1, max_length=160)
    updated_at: CanonicalUtcTimestamp

    @model_validator(mode="after")
    def meaningful(self) -> Self:
        validate_evaluation_review_shape(self.rating, self.note)
        return self


class EvaluationRecord(EvaluationModel):
    format: Literal["stove0-evaluation-record/v1"] = "stove0-evaluation-record/v1"
    definition: EvaluationDefinition
    phase: EvaluationPhase = "planning"
    revision: int = Field(default=1, ge=1)
    children: tuple[EvaluationChild, ...]
    reviews: tuple[EvaluationReview, ...] = ()

    @model_validator(mode="after")
    def validate_children(self) -> Self:
        validate_evaluation_state_shape(self.definition, self.children, self.reviews)
        return self

    @property
    def evaluation_id(self) -> str:
        return self.definition.evaluation_id


class EvaluationStore(Protocol):
    def load(self, evaluation_id: str) -> EvaluationRecord | None: ...

    def create(self, record: EvaluationRecord) -> EvaluationRecord: ...

    def compare_and_swap(
        self,
        evaluation_id: str,
        *,
        expected_revision: int,
        replacement: EvaluationRecord,
    ) -> EvaluationRecord: ...


class EvaluationWorkController(Protocol):
    def step(self, work_id: str) -> WorkRecord: ...

    def cancel(self, work_id: str) -> WorkRecord: ...

    def retry(self, work_id: str) -> WorkRecord: ...


class InMemoryEvaluationStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._records: dict[str, EvaluationRecord] = {}

    def load(self, evaluation_id: str) -> EvaluationRecord | None:
        with self._lock:
            return self._records.get(evaluation_id)

    def create(self, record: EvaluationRecord) -> EvaluationRecord:
        with self._lock:
            existing = self._records.get(record.evaluation_id)
            if existing is not None:
                if existing.definition != record.definition:
                    raise ConcurrentEvaluationUpdate(
                        "evaluation identity was reused with another definition"
                    )
                return existing
            self._records[record.evaluation_id] = record
            return record

    def compare_and_swap(
        self,
        evaluation_id: str,
        *,
        expected_revision: int,
        replacement: EvaluationRecord,
    ) -> EvaluationRecord:
        with self._lock:
            current = self._records.get(evaluation_id)
            if current is None:
                raise KeyError(evaluation_id)
            if current.revision != expected_revision:
                raise ConcurrentEvaluationUpdate(
                    f"stale evaluation revision: {expected_revision} != {current.revision}"
                )
            if (
                replacement.evaluation_id != evaluation_id
                or replacement.revision != expected_revision + 1
            ):
                raise ValueError("replacement evaluation has an invalid identity or revision")
            self._records[evaluation_id] = replacement
            return replacement


class EvaluationService:
    """Persist and reconcile an evaluation over ordinary stove0 work records."""

    def __init__(
        self,
        store: EvaluationStore,
        *,
        work: Stove0WorkService,
    ) -> None:
        self.store = store
        self.work = work

    def create_or_resume(self, definition: EvaluationDefinition) -> EvaluationRecord:
        children: list[EvaluationChild] = []
        for variant in definition.matrix.variants:
            identity = definition.child_work(variant.id)
            self.work.create_or_resume(identity)
            children.append(EvaluationChild(variant_id=variant.id, work_id=identity.work_id))
        record = self.store.create(
            EvaluationRecord(definition=definition, children=tuple(children))
        )
        return self.refresh(record.evaluation_id)

    def refresh(self, evaluation_id: str) -> EvaluationRecord:
        record = self._load(evaluation_id)
        children = tuple(self._project_child(child) for child in record.children)
        phase = _evaluation_phase(children)
        if children == record.children and phase == record.phase:
            return record
        return self._replace(record, phase=phase, children=children)

    def step(
        self,
        evaluation_id: str,
        *,
        controller: EvaluationWorkController,
    ) -> EvaluationRecord:
        record = self.refresh(evaluation_id)
        child = next(
            (item for item in record.children if item.state in {"pending", "active"}),
            None,
        )
        if child is None:
            return record
        controller.step(child.work_id)
        return self.refresh(evaluation_id)

    def cancel(
        self,
        evaluation_id: str,
        *,
        controller: EvaluationWorkController,
    ) -> EvaluationRecord:
        record = self.refresh(evaluation_id)
        for child in record.children:
            if child.state in {"pending", "active"}:
                controller.cancel(child.work_id)
        return self.refresh(evaluation_id)

    def retry_failed(
        self,
        evaluation_id: str,
        variant_id: str,
        *,
        controller: EvaluationWorkController,
    ) -> EvaluationRecord:
        record = self.refresh(evaluation_id)
        child = next((item for item in record.children if item.variant_id == variant_id), None)
        if child is None:
            raise KeyError(variant_id)
        if child.state != "failed":
            raise RuntimeError("only failed evaluation variants can be retried")
        controller.retry(child.work_id)
        return self.refresh(evaluation_id)

    def review(
        self,
        evaluation_id: str,
        review: EvaluationReview,
    ) -> EvaluationRecord:
        record = self.refresh(evaluation_id)
        reviews = {current.variant_id: current for current in record.reviews}
        reviews[review.variant_id] = review
        return self._replace(
            record,
            reviews=tuple(reviews[key] for key in sorted(reviews)),
        )

    def _project_child(self, child: EvaluationChild) -> EvaluationChild:
        record = self.work.store.load(child.work_id)
        if record is None:
            raise RuntimeError("evaluation child work disappeared")
        state: EvaluationChildState
        if record.phase == "complete":
            state = "complete"
        elif record.phase == "no_action":
            state = "no_action"
        elif record.phase == "inapplicable":
            state = "inapplicable"
        elif record.phase == "failed":
            state = "failed"
        elif record.phase == "canceled":
            state = "canceled"
        elif record.phase == "eligible":
            state = "pending"
        else:
            state = "active"
        return EvaluationChild(
            variant_id=child.variant_id,
            work_id=child.work_id,
            state=state,
            output=self._completed_output(record) if state == "complete" else None,
        )

    def _completed_output(self, record: WorkRecord) -> OutputCollectionRef:
        if record.output is not None:
            return record.output
        plan = record.branch_set_plan
        settlement = record.coordination_settlement
        if plan is None or settlement is None:
            raise RuntimeError("completed evaluation work has no collection result")
        if plan.join is not None:
            result = settlement.collection_result
            if result is None:
                raise RuntimeError("completed evaluation join has no collection result")
            producer_work_id = result.producer_work_id
        else:
            collections = tuple(item for item in settlement.children if item.kind == "collection")
            if len(collections) != 1:
                raise RuntimeError("completed evaluation requires exactly one collection result")
            branch = next(
                (item for item in plan.branches if item.branch_id == collections[0].branch_id),
                None,
            )
            if not isinstance(branch, BranchPlan):
                raise RuntimeError("evaluation collection result has no declared leaf")
            producer_work_id = branch.workflow_plan.work.work_id
        producer = self.work.store.load(producer_work_id)
        if (
            producer is None
            or producer.phase not in {"settled", "source_collection_retirement_pending", "complete"}
            or producer.output is None
            or producer.target_settlement is None
        ):
            raise RuntimeError("evaluation collection producer is not settled")
        if plan.join is not None:
            assert settlement.collection_result is not None
            result = settlement.collection_result
            output = producer.output
            if (
                output.collection_id != result.output_collection.collection_id
                or output.archive_root_sha256 != result.output_collection.archive_root_sha256
                or output.artifact_set_identity != result.output_collection.artifact_set_identity
                or output.derivation_sha256 != result.derivation_sha256
            ):
                raise RuntimeError("evaluation join output differs from exact settlement")
        return producer.output

    def _load(self, evaluation_id: str) -> EvaluationRecord:
        record = self.store.load(evaluation_id)
        if record is None:
            raise KeyError(evaluation_id)
        return record

    def _replace(self, record: EvaluationRecord, **updates: object) -> EvaluationRecord:
        replacement = record.model_copy(update={**updates, "revision": record.revision + 1})
        replacement = EvaluationRecord.model_validate(replacement.model_dump(mode="python"))
        return self.store.compare_and_swap(
            record.evaluation_id,
            expected_revision=record.revision,
            replacement=replacement,
        )


def _evaluation_phase(children: tuple[EvaluationChild, ...]) -> EvaluationPhase:
    states = {item.state for item in children}
    if states & {"pending", "active"}:
        return "running"
    succeeded = sum(item.state in {"complete", "no_action"} for item in children)
    if succeeded == len(children):
        return "complete"
    if succeeded:
        return "partially_complete"
    if states == {"canceled"}:
        return "canceled"
    return "failed"


__all__ = [
    "ConcurrentEvaluationUpdate",
    "EvaluationChild",
    "EvaluationChildState",
    "EvaluationPhase",
    "EvaluationRecord",
    "EvaluationReview",
    "EvaluationService",
    "EvaluationStore",
    "EvaluationWorkController",
    "InMemoryEvaluationStore",
]
