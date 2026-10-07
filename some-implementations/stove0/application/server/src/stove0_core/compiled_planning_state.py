"""Bounded compiler-runtime metadata continuations in the Stove0 state owner."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    MetaData,
    String,
    Table,
    Text,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine
from stove0_protocol import ArtifactSelectionRef, WorkArtifactSubject
from stove0_protocol.predicates import Truth, facts_predicates
from stove0_recipe_config.compiled import CompiledRecipe

from stove0_core.planning_context import PlanningContext
from stove0_core.planning_program import TaskView, classify_members
from stove0_core.work_state import ConcurrentWorkUpdate


def declare_compiled_planning(metadata: MetaData) -> dict[str, Table]:
    planning = Table(
        "stove0_compiled_planning",
        metadata,
        Column("work_id", String(129), primary_key=True),
        Column(
            "context_id",
            String(64),
            ForeignKey("stove0_planning_contexts.context_id", ondelete="CASCADE"),
        ),
        Column("recipe_sha256", String(64), nullable=False),
        Column("revision", BigInteger, nullable=False),
        Column("phase", String(16), nullable=False),
        Column("scope_sha256", String(64)),
        Column("task_ordinal", BigInteger, nullable=False),
        Column("dispatch_turn", BigInteger, nullable=False),
        Column("input_ordinal", BigInteger, nullable=False),
        Column("decision_ordinal", BigInteger, nullable=False),
        Column("decision_json", Text),
        Column("classified_count", BigInteger, nullable=False),
        CheckConstraint(
            "revision >= 1 AND task_ordinal >= 0 AND dispatch_turn >= 0 "
            "AND input_ordinal >= 0 AND decision_ordinal >= 0 "
            "AND classified_count >= 0",
            name="ck_stove0_compiled_planning_counters",
        ),
        CheckConstraint(
            "phase IN ('inventory','observations','classified','decisions','complete')",
            name="ck_stove0_compiled_planning_phase",
        ),
        Index("ix_stove0_compiled_planning_pending", "phase", "work_id"),
    )
    tasks = Table(
        "stove0_compiled_tasks",
        metadata,
        Column(
            "work_id",
            String(129),
            ForeignKey(planning.c.work_id, ondelete="CASCADE"),
            primary_key=True,
        ),
        Column("task_id", Text, primary_key=True),
        Column("revision", BigInteger, nullable=False),
        Column("state", String(16), nullable=False),
        Column("dependencies_sha256", String(64), nullable=False),
        Column("dependency_ordinal", BigInteger, nullable=False),
        Column("port_ordinal", BigInteger, nullable=False),
        Column("view_ordinal", BigInteger, nullable=False),
        Column("checked_turn", BigInteger, nullable=False),
        CheckConstraint(
            "revision >= 1 AND dependency_ordinal >= 0 AND port_ordinal >= 0 "
            "AND view_ordinal >= 0 AND checked_turn >= 0",
            name="ck_stove0_compiled_task_counters",
        ),
        CheckConstraint(
            "state IN ('initializing','collecting','complete')",
            name="ck_stove0_compiled_task_state",
        ),
        Index("ix_stove0_compiled_tasks_due", "work_id", "state", "checked_turn", "task_id"),
    )
    dependencies = Table(
        "stove0_compiled_task_dependencies",
        metadata,
        Column("work_id", String(129), primary_key=True),
        Column("task_id", Text, primary_key=True),
        Column("predecessor_id", Text, primary_key=True),
        ForeignKeyConstraint(
            ("work_id", "task_id"), (tasks.c.work_id, tasks.c.task_id), ondelete="CASCADE"
        ),
        Index("ix_stove0_compiled_task_predecessors", "work_id", "predecessor_id", "task_id"),
    )
    members = Table(
        "stove0_compiled_classifications",
        metadata,
        Column(
            "work_id",
            String(129),
            ForeignKey(planning.c.work_id, ondelete="CASCADE"),
            primary_key=True,
        ),
        Column("subject_id", Text, primary_key=True),
        Column("role", Text),
        Index("ix_stove0_compiled_classification_roles", "work_id", "role", "subject_id"),
    )
    ports = Table(
        "stove0_compiled_task_ports",
        metadata,
        Column(
            "work_id",
            String(129),
            ForeignKey(planning.c.work_id, ondelete="CASCADE"),
            primary_key=True,
        ),
        Column("task_id", Text, primary_key=True),
        Column("port_id", Text, primary_key=True),
        Column("selection_sha256", String(64), nullable=False),
        Column("reference_json", Text, nullable=False),
    )
    facts = Table(
        "stove0_compiled_fact_evaluations",
        metadata,
        Column("evaluation_key", String(129), primary_key=True),
        Column(
            "work_id",
            String(129),
            ForeignKey(planning.c.work_id, ondelete="CASCADE"),
            nullable=False,
        ),
        Column("binding_json", Text, nullable=False),
        Column("scope_sha256", String(64), nullable=False),
        Column("revision", BigInteger, nullable=False),
        Column("state", String(16), nullable=False),
        Column("continuation", String(64)),
        Column("record_ordinal", BigInteger, nullable=False),
        Column("positive", Boolean, nullable=False),
        Column("negative", Boolean, nullable=False),
        Column("indeterminate", Boolean, nullable=False),
        Column("truth", String(16)),
        Column("result_json", Text),
        CheckConstraint(
            "revision >= 1 AND record_ordinal >= 0", name="ck_stove0_fact_evaluation_counters"
        ),
        CheckConstraint(
            "state IN ('collecting','complete')", name="ck_stove0_fact_evaluation_state"
        ),
        CheckConstraint(
            "truth IS NULL OR truth IN ('true','false','indeterminate')",
            name="ck_stove0_fact_evaluation_truth",
        ),
        Index("ix_stove0_fact_evaluations_work", "work_id", "evaluation_key"),
        Index("ix_stove0_fact_evaluations_scope", "scope_sha256"),
    )
    conditions = Table(
        "stove0_compiled_decision_conditions",
        metadata,
        Column(
            "work_id",
            String(129),
            ForeignKey(planning.c.work_id, ondelete="CASCADE"),
            primary_key=True,
        ),
        Column("decision_ordinal", BigInteger, primary_key=True),
        Column("proof_sha256", String(64), nullable=False),
        Column("proof_json", Text, nullable=False),
        CheckConstraint("decision_ordinal >= 0", name="ck_stove0_decision_condition_ordinal"),
    )
    return {
        "planning": planning,
        "tasks": tasks,
        "dependencies": dependencies,
        "classification": members,
        "ports": ports,
        "facts": facts,
        "conditions": conditions,
    }


class CompiledPlanningState:
    def __init__(
        self,
        engine: Engine,
        tables: Mapping[str, Table],
        selection_members: Table,
        selection_header: Table,
        *,
        context: PlanningContext,
    ) -> None:
        self.engine, self.tables, self.members = engine, tables, selection_members
        self.header = selection_header
        self.key, self.context_id = context.key, context.context_id

    def ensure(self, work_id: str, recipe: CompiledRecipe) -> dict[str, Any]:
        table = self.tables["planning"]
        insert = pg_insert if self.engine.dialect.name == "postgresql" else sqlite_insert
        with self.engine.begin() as connection:
            connection.execute(
                insert(table)
                .values(
                    work_id=self.key(work_id),
                    context_id=self.context_id,
                    recipe_sha256=recipe.sha256,
                    revision=1,
                    phase="inventory",
                    task_ordinal=0,
                    dispatch_turn=0,
                    input_ordinal=0,
                    decision_ordinal=0,
                    classified_count=0,
                )
                .on_conflict_do_nothing()
            )
            row = (
                connection.execute(select(table).where(table.c.work_id == self.key(work_id)))
                .mappings()
                .one()
            )
            if row["recipe_sha256"] != recipe.sha256:
                raise ValueError(
                    "planning continuation changed the exact queued compiled definition"
                )
            return dict(row)

    def bind_inventory(
        self, work_id: str, *, expected_revision: int, scope: ArtifactSelectionRef
    ) -> None:
        table = self.tables["planning"]
        with self.engine.connect() as connection:
            current = (
                connection.execute(select(table).where(table.c.work_id == self.key(work_id)))
                .mappings()
                .one()
            )
            if current["scope_sha256"] is not None or current["phase"] != "inventory":
                raise ValueError("compiled inventory cannot be rebound after planning begins")
            root = (
                connection.execute(
                    select(self.header).where(
                        self.header.c.selection_sha256 == scope.selection_sha256,
                        self.header.c.state == "sealed",
                    )
                )
                .mappings()
                .first()
            )
            if root is None or (root["artifact_count"], root["total_bytes"]) != (
                scope.artifact_count,
                scope.total_bytes,
            ):
                raise ValueError("compiled invocation inventory is not an exact sealed selection")
        self.advance(
            work_id,
            expected_revision=expected_revision,
            phase="observations",
            scope_sha256=scope.selection_sha256,
        )

    def inventory_ref(self, work_id: str) -> ArtifactSelectionRef | None:
        p, h = self.tables["planning"], self.header
        with self.engine.connect() as connection:
            row = (
                connection.execute(
                    select(h)
                    .select_from(h.join(p, p.c.scope_sha256 == h.c.selection_sha256))
                    .where(p.c.work_id == self.key(work_id), h.c.state == "sealed")
                )
                .mappings()
                .first()
            )
        return (
            None
            if row is None
            else ArtifactSelectionRef.model_validate(
                {
                    "selection_sha256": row["selection_sha256"],
                    "artifact_count": row["artifact_count"],
                    "total_bytes": str(row["total_bytes"]),
                }
            )
        )

    def advance(self, work_id: str, *, expected_revision: int, **changes: Any) -> None:
        allowed = {
            "phase",
            "scope_sha256",
            "task_ordinal",
            "dispatch_turn",
            "classified_count",
            "input_ordinal",
            "decision_ordinal",
            "decision_json",
        }
        if set(changes) - allowed:
            raise ValueError("planning continuation contains unknown state fields")
        table = self.tables["planning"]
        with self.engine.begin() as connection:
            row = (
                connection.execute(
                    select(table).where(table.c.work_id == self.key(work_id)).with_for_update()
                )
                .mappings()
                .one()
            )
            if row["revision"] != expected_revision:
                raise ConcurrentWorkUpdate("compiled planning changed concurrently")
            if "scope_sha256" in changes and row["scope_sha256"] is not None:
                raise ValueError("compiled original inventory is immutable")
            changed = connection.execute(
                update(table)
                .where(table.c.work_id == self.key(work_id), table.c.revision == expected_revision)
                .values(revision=expected_revision + 1, **changes)
            )
            if changed.rowcount != 1:
                raise ConcurrentWorkUpdate("compiled planning changed concurrently")

    def initialize_tasks(
        self, work_id: str, graph: Mapping[str, set[str]], *, expected_revision: int
    ) -> bool:
        """Publish one task and at most one dependency page per control step."""
        from stove0_protocol import canonical_json_sha256

        p, tasks, deps = self.tables["planning"], self.tables["tasks"], self.tables["dependencies"]
        insert = pg_insert if self.engine.dialect.name == "postgresql" else sqlite_insert
        key, names = self.key(work_id), sorted(graph)
        with self.engine.begin() as connection:
            current = (
                connection.execute(select(p).where(p.c.work_id == key).with_for_update())
                .mappings()
                .one()
            )
            if current["revision"] != expected_revision:
                raise ConcurrentWorkUpdate("compiled task initialization changed concurrently")
            position = current["task_ordinal"]
            if position == len(names):
                return True
            if position > len(names):
                raise ValueError("compiled task initialization exceeds its exact graph")
            name = names[position]
            predecessors = sorted(graph[name])
            commitment = canonical_json_sha256(predecessors)
            connection.execute(
                insert(tasks)
                .values(
                    work_id=key,
                    task_id=name,
                    revision=1,
                    state="initializing",
                    dependencies_sha256=commitment,
                    dependency_ordinal=0,
                    port_ordinal=0,
                    view_ordinal=0,
                    checked_turn=0,
                )
                .on_conflict_do_nothing()
            )
            row = (
                connection.execute(
                    select(tasks).where(tasks.c.work_id == key, tasks.c.task_id == name)
                )
                .mappings()
                .one()
            )
            if row["dependencies_sha256"] != commitment or row["dependency_ordinal"] > len(
                predecessors
            ):
                raise ValueError("compiled task changed its exact dependency graph")
            page = predecessors[row["dependency_ordinal"] : row["dependency_ordinal"] + 100]
            if page:
                connection.execute(
                    insert(deps)
                    .values(
                        [
                            {"work_id": key, "task_id": name, "predecessor_id": predecessor}
                            for predecessor in page
                        ]
                    )
                    .on_conflict_do_nothing()
                )
            end = row["dependency_ordinal"] + len(page)
            complete = end == len(predecessors)
            connection.execute(
                update(tasks)
                .where(
                    tasks.c.work_id == key,
                    tasks.c.task_id == name,
                    tasks.c.revision == row["revision"],
                )
                .values(
                    revision=row["revision"] + 1,
                    dependency_ordinal=end,
                    state="collecting" if complete else "initializing",
                )
            )
            changed = connection.execute(
                update(p)
                .where(p.c.work_id == key, p.c.revision == expected_revision)
                .values(
                    revision=expected_revision + 1,
                    task_ordinal=position + int(complete),
                )
            )
            if changed.rowcount != 1:
                raise ConcurrentWorkUpdate("compiled task initialization changed concurrently")
        return False

    def due_task(self, work_id: str) -> dict[str, Any] | None:
        """Reserve the oldest ready task without holding a lock across a contact."""
        p, tasks, deps = self.tables["planning"], self.tables["tasks"], self.tables["dependencies"]
        predecessor = tasks.alias("completed_predecessor")
        key = self.key(work_id)
        with self.engine.begin() as connection:
            current = (
                connection.execute(
                    select(p).where(p.c.work_id == key).with_for_update(skip_locked=True)
                )
                .mappings()
                .first()
            )
            if current is None:
                return None
            incomplete = (
                select(deps.c.predecessor_id)
                .where(
                    deps.c.work_id == tasks.c.work_id,
                    deps.c.task_id == tasks.c.task_id,
                    ~select(predecessor.c.task_id)
                    .where(
                        predecessor.c.work_id == deps.c.work_id,
                        predecessor.c.task_id == deps.c.predecessor_id,
                        predecessor.c.state == "complete",
                    )
                    .exists(),
                )
                .exists()
            )
            row = (
                connection.execute(
                    select(tasks)
                    .where(
                        tasks.c.work_id == key,
                        tasks.c.state == "collecting",
                        ~incomplete,
                    )
                    .order_by(tasks.c.checked_turn, tasks.c.task_id)
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
                .mappings()
                .first()
            )
            if row is None:
                return None
            turn = current["dispatch_turn"] + 1
            connection.execute(
                update(p)
                .where(p.c.work_id == key, p.c.revision == current["revision"])
                .values(
                    revision=current["revision"] + 1,
                    dispatch_turn=turn,
                )
            )
            changed = connection.execute(
                update(tasks)
                .where(
                    tasks.c.work_id == key,
                    tasks.c.task_id == row["task_id"],
                    tasks.c.revision == row["revision"],
                )
                .values(revision=row["revision"] + 1, checked_turn=turn)
            )
            if changed.rowcount != 1:
                raise ConcurrentWorkUpdate("compiled ready task changed concurrently")
            return {**row, "revision": row["revision"] + 1, "checked_turn": turn}

    def tasks_complete(self, work_id: str) -> bool:
        table = self.tables["tasks"]
        with self.engine.connect() as connection:
            return (
                connection.scalar(
                    select(table.c.task_id)
                    .where(
                        table.c.work_id == self.key(work_id),
                        table.c.state != "complete",
                    )
                    .limit(1)
                )
                is None
            )

    def advance_task(self, row: Mapping[str, Any], **changes: Any) -> None:
        if set(changes) - {"port_ordinal", "view_ordinal", "state"}:
            raise ValueError("task continuation contains unknown fields")
        table = self.tables["tasks"]
        with self.engine.begin() as connection:
            changed = connection.execute(
                update(table)
                .where(
                    table.c.work_id == row["work_id"],
                    table.c.task_id == row["task_id"],
                    table.c.revision == row["revision"],
                )
                .values(revision=row["revision"] + 1, **changes)
            )
            if changed.rowcount != 1:
                raise ConcurrentWorkUpdate("compiled observation task changed concurrently")

    def classify_step(
        self,
        work_id: str,
        *,
        recipe: CompiledRecipe,
        views: TaskView,
        limit: int = 100,
        input_answers: Mapping[str, Truth] | None = None,
    ) -> bool:
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("classification budget must be between 1 and 100")
        if input_answers is None and any(
            predicate.scope == "input"
            for case in recipe.classification.cases
            for predicate in facts_predicates(case.when)
        ):
            raise ValueError("classification requires completed whole-invocation input facts")
        p, c, m = self.tables["planning"], self.tables["classification"], self.members
        with self.engine.begin() as connection:
            state = (
                connection.execute(
                    select(p).where(p.c.work_id == self.key(work_id)).with_for_update()
                )
                .mappings()
                .one()
            )
            if state["recipe_sha256"] != recipe.sha256 or state["scope_sha256"] is None:
                raise ValueError("classification has no exact compiled policy and inventory")
            total = connection.scalar(
                select(self.header.c.artifact_count).where(
                    self.header.c.selection_sha256 == state["scope_sha256"],
                    self.header.c.state == "sealed",
                )
            )
            if total is None:
                raise ValueError("classification lost its sealed original inventory")
            if state["classified_count"] == total:
                return True
            rows = (
                connection.execute(
                    select(m)
                    .where(
                        m.c.selection_sha256 == state["scope_sha256"],
                        m.c.artifact_order >= state["classified_count"],
                    )
                    .order_by(m.c.artifact_order)
                    .limit(limit + 1)
                )
                .mappings()
                .all()
            )
            subjects = tuple(
                WorkArtifactSubject.model_validate_json(row["document_json"])
                for row in rows[:limit]
            )
            classified, unclassified = classify_members(
                recipe, subjects, views, input_answers=input_answers
            )
            assigned = {subject.id: subject.role for subject in classified}
            for subject in (*classified, *unclassified):
                connection.execute(
                    c.insert().values(
                        work_id=self.key(work_id),
                        subject_id=subject.id,
                        role=assigned.get(subject.id),
                    )
                )
            count = state["classified_count"] + len(subjects)
            complete = len(rows) <= limit
            changed = connection.execute(
                update(p)
                .where(p.c.work_id == self.key(work_id), p.c.revision == state["revision"])
                .values(
                    revision=state["revision"] + 1,
                    classified_count=count,
                    phase="classified" if complete else state["phase"],
                )
            )
            if changed.rowcount != 1:
                raise ConcurrentWorkUpdate("classification changed concurrently")
            return complete

    def retain_port(
        self,
        work_id: str,
        task_id: str,
        port_id: str,
        reference: ArtifactSelectionRef,
        *,
        expected_revision: int,
    ) -> None:
        """Link one finished port and advance its cursor in the same transaction."""
        from stove0_protocol import canonical_json_bytes

        p, ports = self.tables["tasks"], self.tables["ports"]
        encoded = canonical_json_bytes(reference.model_dump(mode="json")).decode("utf-8")
        insert = pg_insert if self.engine.dialect.name == "postgresql" else sqlite_insert
        with self.engine.begin() as connection:
            current = (
                connection.execute(
                    select(p)
                    .where(p.c.work_id == self.key(work_id), p.c.task_id == task_id)
                    .with_for_update()
                )
                .mappings()
                .one()
            )
            if current["revision"] != expected_revision:
                raise ConcurrentWorkUpdate("compiled input port changed concurrently")
            header = (
                connection.execute(
                    select(self.header).where(
                        self.header.c.selection_sha256 == reference.selection_sha256,
                        self.header.c.state == "sealed",
                    )
                )
                .mappings()
                .first()
            )
            if header is None or (header["artifact_count"], header["total_bytes"]) != (
                reference.artifact_count,
                reference.total_bytes,
            ):
                raise ValueError("compiled task port has no exact sealed selection")
            connection.execute(
                insert(ports)
                .values(
                    work_id=self.key(work_id),
                    task_id=task_id,
                    port_id=port_id,
                    selection_sha256=reference.selection_sha256,
                    reference_json=encoded,
                )
                .on_conflict_do_nothing()
            )
            existing = connection.scalar(
                select(ports.c.reference_json).where(
                    ports.c.work_id == self.key(work_id),
                    ports.c.task_id == task_id,
                    ports.c.port_id == port_id,
                )
            )
            if existing != encoded:
                raise ValueError("compiled task port was rebound")
            connection.execute(
                update(p)
                .where(
                    p.c.work_id == self.key(work_id),
                    p.c.task_id == task_id,
                    p.c.revision == expected_revision,
                )
                .values(
                    revision=expected_revision + 1,
                    port_ordinal=current["port_ordinal"] + 1,
                )
            )

    def task_ports(self, work_id: str, task_id: str) -> dict[str, ArtifactSelectionRef]:
        ports = self.tables["ports"]
        with self.engine.connect() as connection:
            rows = connection.execute(
                select(ports.c.port_id, ports.c.reference_json)
                .where(
                    ports.c.work_id == self.key(work_id),
                    ports.c.task_id == task_id,
                )
                .order_by(ports.c.port_id)
            ).all()
        return {name: ArtifactSelectionRef.model_validate_json(document) for name, document in rows}

    def role_page(
        self, work_id: str, *, roles: tuple[str, ...], after_subject_id: str = "", limit: int = 100
    ) -> tuple[tuple[WorkArtifactSubject, ...], str | None, bool]:
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("role selection page must be between 1 and 100")
        p, c, m = self.tables["planning"], self.tables["classification"], self.members
        with self.engine.connect() as connection:
            state = (
                connection.execute(select(p).where(p.c.work_id == self.key(work_id)))
                .mappings()
                .one()
            )
            total = connection.scalar(
                select(self.header.c.artifact_count).where(
                    self.header.c.selection_sha256 == state["scope_sha256"],
                    self.header.c.state == "sealed",
                )
            )
            if state["scope_sha256"] is None or state["classified_count"] != total:
                raise ValueError(
                    "role selection requires complete classification of the original invocation"
                )
            rows = (
                connection.execute(
                    select(m.c.document_json, c.c.role, c.c.subject_id)
                    .select_from(
                        m.join(
                            c,
                            (c.c.subject_id == m.c.artifact_id)
                            & (c.c.work_id == self.key(work_id)),
                        )
                    )
                    .where(
                        m.c.selection_sha256 == state["scope_sha256"],
                        c.c.role.in_(roles),
                        c.c.subject_id > after_subject_id,
                    )
                    .order_by(c.c.subject_id)
                    .limit(limit + 1)
                )
                .mappings()
                .all()
            )
        subjects = tuple(
            WorkArtifactSubject.model_validate_json(row["document_json"]).model_copy(
                update={"role": row["role"]}
            )
            for row in rows[:limit]
        )
        complete = len(rows) <= limit
        return subjects, None if complete else rows[limit - 1]["subject_id"], complete
