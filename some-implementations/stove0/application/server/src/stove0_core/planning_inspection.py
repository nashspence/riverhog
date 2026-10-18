"""Read only, owner-qualified pages over retained compiled observation state."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy.engine import RowMapping
from sqlalchemy.sql import Select
from stove0_observer_protocol import ContentObservationEvidence
from stove0_protocol.observation_evidence import AcceptedEvidencePage, AcceptedViewPage

from stove0_core.observation_state import ObservationOwnerKind

if TYPE_CHECKING:
    from stove0_core.persistence import SqlAlchemyStateStore

from sqlalchemy import or_, select
from stove0_operator_contracts import ObservationTaskDetail, ObservationTaskView
from stove0_protocol import ArtifactSelectionRef, WorkIdentity
from stove0_protocol.observation_evidence import AcceptedEvidenceSet, ObservationQuestion


class PlanningInspection:
    def __init__(
        self,
        state: SqlAlchemyStateStore,
        *,
        owner_kind: ObservationOwnerKind,
        owner_id: str,
        root_work_id: str,
    ) -> None:
        self.state = state.read_planning_context(owner_kind, owner_id)
        self.owner_kind, self.owner_id, self.root_work_id = owner_kind, owner_id, root_work_id

    def _query(self, *, documents: bool = False) -> Select[Any]:
        state = self.state
        if state is None:
            raise KeyError(self.owner_id)
        tasks, frames = (
            state.compiled_planning.tables["tasks"],
            state.compiled_runtime.tables["frames"],
        )
        q, h = state.accepted_observations.tables["question"], state.compiled_planning.header
        columns = [
            tasks.c.work_id,
            tasks.c.task_id,
            tasks.c.state,
            tasks.c.revision,
            q.c.question_sha256,
            q.c.scope_sha256,
            q.c.evidence_set_sha256,
            h.c.artifact_count,
            h.c.total_bytes,
        ]
        if documents:
            columns.extend((q.c.question_json, q.c.evidence_set_json))
        return (
            select(*columns)
            .select_from(
                tasks.join(frames, tasks.c.work_id == frames.c.work_id)
                .outerjoin(q, (q.c.work_id == tasks.c.work_id) & (q.c.task_id == tasks.c.task_id))
                .outerjoin(h, h.c.selection_sha256 == q.c.scope_sha256)
            )
            .where(
                frames.c.owner_work_id == state.planning_key(self.root_work_id),
                tasks.c.task_id != "$classify",
            )
        )

    @staticmethod
    def _view(row: RowMapping) -> ObservationTaskView:
        return ObservationTaskView(
            work_id=row["work_id"].rsplit(":", 1)[-1],
            task_id=row["task_id"],
            state=row["state"],
            revision=row["revision"],
            question_sha256=row["question_sha256"].rsplit(":", 1)[-1]
            if row["question_sha256"]
            else None,
            scope=ArtifactSelectionRef.model_validate(
                {
                    "selection_sha256": row["scope_sha256"],
                    "artifact_count": str(row["artifact_count"]),
                    "total_bytes": str(row["total_bytes"]),
                }
            )
            if row["scope_sha256"]
            else None,
            evidence_set_sha256=row["evidence_set_sha256"],
        )

    def tasks(
        self, *, page_size: int = 100, position: tuple[str, str] | None = None
    ) -> dict[str, Any]:
        if type(page_size) is not int or not 1 <= page_size <= 100:
            raise ValueError("observation task page size must be between 1 and 100")
        rows: tuple[RowMapping, ...] = ()
        if self.state is not None:
            table = self.state.compiled_planning.tables["tasks"]
            query = self._query()
            if position is not None:
                if len(position) != 2 or any(type(part) is not str for part in position):
                    raise ValueError("observation task continuation is invalid")
                work_key = self.state.planning_key(position[0])
                query = query.where(
                    or_(
                        table.c.work_id > work_key,
                        (table.c.work_id == work_key) & (table.c.task_id > position[1]),
                    )
                )
            with self.state.engine.connect() as connection:
                rows = tuple(
                    connection.execute(
                        query.order_by(table.c.work_id, table.c.task_id).limit(page_size + 1)
                    ).mappings()
                )
        page = tuple(self._view(row) for row in rows[:page_size])
        return {
            "owner_kind": self.owner_kind,
            "owner_id": self.owner_id,
            "page_size": page_size,
            "tasks": page,
            "_next_position": (page[-1].work_id, page[-1].task_id)
            if len(rows) > page_size
            else None,
        }

    def task(self, work_id: str, task_id: str) -> ObservationTaskDetail:
        if self.state is None:
            raise KeyError(task_id)
        table = self.state.compiled_planning.tables["tasks"]
        with self.state.engine.connect() as connection:
            row = (
                connection.execute(
                    self._query(documents=True).where(
                        table.c.work_id == self.state.planning_key(work_id),
                        table.c.task_id == task_id,
                    )
                )
                .mappings()
                .first()
            )
        if row is None:
            raise KeyError(task_id)
        frame = self.state.compiled_runtime.load(work_id)
        if frame is None:
            raise KeyError(task_id)
        work = WorkIdentity.model_validate_json(frame["work_json"])
        retained = self.state.recipe_definitions.load(work.recipe)
        if retained is None:
            raise KeyError(task_id)
        recipe, closure = retained
        task = recipe.observations[task_id]
        interface = closure.interface(id=task.interface.id, sha256=task.interface.sha256).interface
        store = self.state.accepted_observations
        question = (
            None
            if row["question_json"] is None
            else ObservationQuestion.model_validate_json(row["question_json"])
        )
        accepted = (
            None
            if row["evidence_set_json"] is None
            else AcceptedEvidenceSet.model_validate_json(row["evidence_set_json"])
        )
        return ObservationTaskDetail(
            task=self._view(row),
            question=question,
            accepted=accepted,
            views={
                name: None
                if accepted is None
                else store.retained_view(
                    accepted, view_id=name, selected_scope=accepted.question.scope
                )
                for name in sorted(interface.views)
            },
        )

    def results(
        self,
        work_id: str,
        task_id: str,
        *,
        evidence_set_sha256: str,
        start_ordinal: int = 0,
        limit: int = 100,
    ) -> AcceptedEvidencePage:
        detail = self.task(work_id, task_id)
        if detail.accepted is None or detail.accepted.evidence_set_sha256 != evidence_set_sha256:
            raise ValueError("observation task has no matching controller acceptance")
        assert self.state is not None
        return self.state.accepted_observations.evidence_page(
            detail.accepted, start_ordinal=start_ordinal, limit=limit, authorize=lambda scope: None
        )

    def original(self, request_id: str) -> ContentObservationEvidence:
        if self.state is None:
            raise KeyError(request_id)
        p, q = (self.state.accepted_observations.tables[name] for name in ("physical", "question"))
        frames = self.state.compiled_runtime.tables["frames"]
        with self.state.engine.connect() as connection:
            retained = connection.scalar(
                select(p.c.request_id)
                .select_from(
                    p.join(q, p.c.question_sha256 == q.c.question_sha256).join(
                        frames, frames.c.work_id == q.c.work_id
                    )
                )
                .where(
                    p.c.request_id == self.state.planning_key(request_id),
                    frames.c.owner_work_id == self.state.planning_key(self.root_work_id),
                    p.c.evidence_json.is_not(None),
                )
            )
        if retained is None:
            raise KeyError(request_id)
        return self.state.accepted_observations.original_evidence(
            request_id, authorize=lambda scope: None
        )

    def view(
        self,
        work_id: str,
        task_id: str,
        view_id: str,
        *,
        view_sha256: str,
        start_ordinal: int = 0,
        limit: int = 100,
    ) -> AcceptedViewPage:
        detail = self.task(work_id, task_id)
        if detail.accepted is None or view_id not in detail.views:
            raise KeyError(view_id)
        assert self.state is not None
        store = self.state.accepted_observations
        authority = detail.views[view_id]
        if authority is None:
            raise KeyError(view_id)
        if authority.view_sha256 != view_sha256:
            raise ValueError("observation page changed its exact view authority")
        return store.view_page(
            authority, start_ordinal=start_ordinal, limit=limit, authorize=lambda scope: None
        )
