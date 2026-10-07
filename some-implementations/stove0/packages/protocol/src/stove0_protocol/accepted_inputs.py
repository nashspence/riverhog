"""Controller acceptance of scoped views of an original completed question.

The source testimony keeps its original question and result commitment. A
selected view is a different controller acceptance artifact; it never pretends
that a larger observer result was an answer about a smaller domain.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from typing import Literal, Self

from pydantic import Field, model_validator

from stove0_protocol.jcs import canonical_json_sha256
from stove0_protocol.models import (
    AcceptedObservationJob,
    ContentObservationInvocation,
    ObservationJobDeclarationPayload,
    Sha256,
    Stove0ProtocolModel,
)
from stove0_protocol.observation_evidence import (
    AcceptedEvidenceSet,
    AcceptedView,
    AcceptedViewPage,
    AcceptedViewRecord,
    update_accepted_view_commitment,
)
from stove0_protocol.selection_refs import ArtifactSelectionRef


class AcceptedInputPayload(Stove0ProtocolModel):
    format: Literal["stove0-accepted-observation-input/v1"] = "stove0-accepted-observation-input/v1"
    source: AcceptedEvidenceSet
    selected_scope: ArtifactSelectionRef
    views: tuple[AcceptedView, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def exact_views(self) -> Self:
        ids = [view.view_id for view in self.views]
        if ids != sorted(set(ids)):
            raise ValueError("accepted input views must be unique and canonical")
        question = self.source.question
        for view in self.views:
            if (
                view.work_id,
                view.task_id,
                view.question_sha256,
                view.evidence_set_sha256,
                view.interface,
                view.selected_scope,
            ) != (
                question.work_id,
                question.task_id,
                question.question_sha256,
                self.source.evidence_set_sha256,
                question.interface,
                self.selected_scope,
            ):
                raise ValueError("accepted input changed its exact original task or selected view")
        return self


class AcceptedInput(AcceptedInputPayload):
    input_sha256: Sha256

    @model_validator(mode="after")
    def exact_identity(self) -> Self:
        document = self.model_dump(mode="json", by_alias=True, exclude={"input_sha256"})
        if canonical_json_sha256(document) != self.input_sha256:
            raise ValueError("accepted input differs from its exact source and selected views")
        return self

    @classmethod
    def seal(cls, payload: AcceptedInputPayload) -> Self:
        document = payload.model_dump(mode="json", by_alias=True)
        return cls.model_validate({**document, "input_sha256": canonical_json_sha256(document)})


class AcceptedEvidenceInput(Stove0ProtocolModel):
    """Exact view disclosures, carrying no out-of-scope original result body."""

    authority: AcceptedInput
    pages: tuple[AcceptedViewPage, ...]

    @model_validator(mode="after")
    def complete_views(self) -> Self:
        offset = 0
        for view in self.authority.views:
            digest, ordinal, complete = hashlib.sha256(), 0, False
            while offset < len(self.pages):
                page = self.pages[offset]
                if page.authority != view or page.start_ordinal != ordinal:
                    raise ValueError("accepted input view pages changed or have a gap")
                for record in page.records:
                    if any(
                        support.scope.artifact_count
                        > self.authority.source.question.scope.artifact_count
                        for support in record.support
                    ):
                        raise ValueError("view support exceeds the original question extent")
                    update_accepted_view_commitment(digest, ordinal=ordinal, record=record)
                    ordinal += 1
                offset += 1
                if page.complete:
                    complete = True
                    break
            if (
                not complete
                or ordinal != view.record_count
                or digest.hexdigest() != view.records_sha256
            ):
                raise ValueError("accepted input lacks its complete exact view commitment")
        if offset != len(self.pages):
            raise ValueError("accepted input includes undeclared view disclosures")
        return self

    def records(self, view_id: str) -> Iterator[AcceptedViewRecord]:
        if view_id not in {view.view_id for view in self.authority.views}:
            raise ValueError("accepted input does not declare the selected view")
        for page in self.pages:
            if page.authority.view_id == view_id:
                yield from page.records


__all__ = ["AcceptedInputPayload", "AcceptedInput", "AcceptedEvidenceInput"]


# Rebuild the mutually referenced invocation contracts after the focused input
# contracts exist. The source result models remain independent of controllers.
for _model in (
    ContentObservationInvocation,
    ObservationJobDeclarationPayload,
    AcceptedObservationJob,
):
    _model.model_rebuild(_types_namespace={"AcceptedEvidenceInput": AcceptedEvidenceInput})
