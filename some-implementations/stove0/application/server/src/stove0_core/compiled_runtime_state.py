"""Durable recipe frames resume nested planning without a Python call stack."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel
from sqlalchemy.dialects.postgresql import Insert as PgInsert
from sqlalchemy.dialects.sqlite import Insert as SqliteInsert
from stove0_protocol import BranchDeclaration

from stove0_core.compiled_state_ports import CompiledStatePort

if TYPE_CHECKING:
    from stove0_core.compiled_branches import CompiledBranchScope

import json

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    ForeignKey,
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
from stove0_protocol import WorkIdentity, canonical_json_bytes

from stove0_core.work_state import ConcurrentWorkUpdate

_PHASES = (
    "observations",
    "decisions",
    "selections",
    "coverage",
    "calls",
    "seal",
    "ready",
    "no-output",
    "inapplicable",
)
_TERMINAL = ("ready", "no-output", "inapplicable")


def declare_compiled_runtime(metadata: MetaData, planning: Table) -> dict[str, Table]:
    frames = Table(
        "stove0_compiled_recipe_frames",
        metadata,
        Column(
            "work_id",
            String(129),
            ForeignKey(planning.c.work_id, ondelete="CASCADE"),
            primary_key=True,
        ),
        Column("owner_work_id", String(129), nullable=False),
        Column("work_json", Text, nullable=False),
        Column("depth", BigInteger, nullable=False),
        Column("revision", BigInteger, nullable=False),
        Column("checked_turn", BigInteger, nullable=False),
        Column("phase", String(16), nullable=False),
        Column("branch_ordinal", BigInteger, nullable=False),
        Column("decision_sha256", String(64)),
        Column("outcome_json", Text),
        Column("decision_json", Text),
        CheckConstraint(
            "depth >= 0 AND revision >= 1 AND branch_ordinal >= 0 AND checked_turn >= 0",
            name="ck_stove0_compiled_recipe_frame_counters",
        ),
        CheckConstraint(
            "phase IN (" + ",".join(repr(p) for p in _PHASES) + ")",
            name="ck_stove0_compiled_recipe_frame_phase",
        ),
        Index(
            "ix_stove0_recipe_frame_due",
            "owner_work_id",
            "phase",
            "checked_turn",
            "depth",
            "work_id",
        ),
        Index("ix_stove0_recipe_frame_turn", "owner_work_id", "checked_turn"),
        Index("ix_stove0_recipe_frame_owner", "owner_work_id", "work_id"),
    )
    branches = Table(
        "stove0_compiled_recipe_frame_branches",
        metadata,
        Column(
            "work_id",
            String(129),
            ForeignKey(frames.c.work_id, ondelete="CASCADE"),
            primary_key=True,
        ),
        Column("branch_id", Text, primary_key=True),
        Column("scope_json", Text, nullable=False),
        Column("selection_sha256", String(64), nullable=False),
        Column("artifact_count", BigInteger, nullable=False),
        Column("declared", Boolean, nullable=False),
        Column("declaration_json", Text),
        Column("child_work_id", String(64)),
        Index("ix_stove0_recipe_frame_branch_selection", "selection_sha256"),
        Index("ix_stove0_recipe_frame_pending_call", "work_id", "declared", "artifact_count"),
        CheckConstraint("artifact_count >= 0", name="ck_stove0_recipe_frame_branch_count"),
    )
    providers = Table(
        "stove0_compiled_provider_bindings",
        metadata,
        Column(
            "work_id",
            String(129),
            ForeignKey(frames.c.work_id, ondelete="CASCADE"),
            primary_key=True,
        ),
        Column("binding_id", Text, primary_key=True),
        Column("binding_json", Text, nullable=False),
        Column("revision", BigInteger, nullable=False),
        Column("position", BigInteger, nullable=False),
        Column("provider_id", Text),
        Column("descriptor_json", Text),
        Column("phase", String(16), nullable=False),
        CheckConstraint(
            "revision >= 1 AND position >= 0", name="ck_stove0_provider_binding_counters"
        ),
        CheckConstraint(
            "phase IN ('search','chosen','ambiguous','unavailable')",
            name="ck_stove0_provider_binding_phase",
        ),
    )
    return {"frames": frames, "branches": branches, "providers": providers}


def _json(model: BaseModel) -> str:
    return canonical_json_bytes(model.model_dump(mode="json", by_alias=True)).decode("utf-8")


class CompiledRuntimeState:
    def __init__(self, state: CompiledStatePort, tables: Mapping[str, Table]) -> None:
        self.state, self.engine, self.tables = state, state.engine, tables

    def _insert(self, table: Table) -> PgInsert | SqliteInsert:
        return (pg_insert if self.engine.dialect.name == "postgresql" else sqlite_insert)(table)

    def ensure(self, work: WorkIdentity, *, owner_work_id: str, depth: int) -> dict[str, Any]:
        retained = self.state.recipe_definitions.load(work.recipe)
        if retained is None:
            raise ValueError("recipe frame lacks its retained exact compiled definition")
        self.state.compiled_planning.ensure(work.work_id, retained[0])
        f = self.tables["frames"]
        with self.engine.begin() as connection:
            connection.execute(
                self._insert(f)
                .values(
                    work_id=self.state.planning_key(work.work_id),
                    owner_work_id=self.state.planning_key(owner_work_id),
                    work_json=_json(work),
                    depth=depth,
                    revision=1,
                    checked_turn=0,
                    phase="observations",
                    branch_ordinal=0,
                )
                .on_conflict_do_nothing()
            )
            row = (
                connection.execute(
                    select(f).where(f.c.work_id == self.state.planning_key(work.work_id))
                )
                .mappings()
                .one()
            )
            if (row["owner_work_id"], row["work_json"], row["depth"]) != (
                self.state.planning_key(owner_work_id),
                _json(work),
                depth,
            ):
                raise ValueError("compiled recipe frame was rebound to another invocation")
            return dict(row)

    def load(self, work_id: str) -> dict[str, Any] | None:
        f = self.tables["frames"]
        with self.engine.connect() as connection:
            row = (
                connection.execute(select(f).where(f.c.work_id == self.state.planning_key(work_id)))
                .mappings()
                .first()
            )
        return None if row is None else dict(row)

    def due(self, owner_work_id: str) -> dict[str, Any] | None:
        f = self.tables["frames"]
        owner = self.state.planning_key(owner_work_id)
        with self.engine.begin() as connection:
            # Serialize only the short reservation, never an extension contact.
            if (
                connection.scalar(
                    select(f.c.work_id)
                    .where(
                        f.c.work_id == owner,
                    )
                    .with_for_update(skip_locked=True)
                )
                is None
            ):
                return None
            row = (
                connection.execute(
                    select(f)
                    .where(
                        f.c.owner_work_id == owner,
                        f.c.phase.not_in(_TERMINAL),
                    )
                    .order_by(f.c.checked_turn, f.c.depth.desc(), f.c.work_id)
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
                .mappings()
                .first()
            )
            if row is None:
                return None
            last = connection.scalar(
                select(f.c.checked_turn)
                .where(
                    f.c.owner_work_id == owner,
                )
                .order_by(f.c.checked_turn.desc())
                .limit(1)
            )
            if last is None:
                raise ValueError("compiled frame reservation lost its owner turn")
            turn = last + 1
            if (
                connection.execute(
                    update(f)
                    .where(
                        f.c.work_id == row["work_id"],
                        f.c.revision == row["revision"],
                    )
                    .values(revision=row["revision"] + 1, checked_turn=turn)
                ).rowcount
                != 1
            ):
                raise ConcurrentWorkUpdate("compiled recipe frame reservation changed concurrently")
            return {**row, "revision": row["revision"] + 1, "checked_turn": turn}

    def branches(self, work_id: str) -> tuple[dict[str, Any], ...]:
        b = self.tables["branches"]
        with self.engine.connect() as connection:
            return tuple(
                dict(row)
                for row in connection.execute(
                    select(b)
                    .where(b.c.work_id == self.state.planning_key(work_id))
                    .order_by(b.c.branch_id)
                ).mappings()
            )

    def branch(self, work_id: str, branch_id: str) -> dict[str, Any] | None:
        b = self.tables["branches"]
        with self.engine.connect() as connection:
            row = (
                connection.execute(
                    select(b).where(
                        b.c.work_id == self.state.planning_key(work_id),
                        b.c.branch_id == branch_id,
                    )
                )
                .mappings()
                .first()
            )
        return None if row is None else dict(row)

    def calls_complete(self, work_id: str) -> bool:
        b = self.tables["branches"]
        with self.engine.connect() as connection:
            return (
                connection.scalar(
                    select(b.c.branch_id)
                    .where(
                        b.c.work_id == self.state.planning_key(work_id),
                        b.c.artifact_count > 0,
                        b.c.declared.is_(False),
                    )
                    .limit(1)
                )
                is None
            )

    def ensure_provider(
        self, work_id: str, binding_id: str, binding: dict[str, Any]
    ) -> dict[str, Any]:
        p = self.tables["providers"]
        document = canonical_json_bytes(binding).decode("utf-8")
        with self.engine.begin() as connection:
            connection.execute(
                self._insert(p)
                .values(
                    work_id=self.state.planning_key(work_id),
                    binding_id=binding_id,
                    binding_json=document,
                    revision=1,
                    position=0,
                    phase="search",
                )
                .on_conflict_do_nothing()
            )
            row = (
                connection.execute(
                    select(p).where(
                        p.c.work_id == self.state.planning_key(work_id),
                        p.c.binding_id == binding_id,
                    )
                )
                .mappings()
                .one()
            )
            retained = json.loads(row["binding_json"])
            if {k: v for k, v in retained.items() if k != "providers"} != {
                k: v for k, v in binding.items() if k != "providers"
            }:
                raise ValueError("exact deployment provider search was rebound")
        return dict(row)

    def change_provider(self, row: Mapping[str, Any], **changes: Any) -> None:
        p = self.tables["providers"]
        with self.engine.begin() as connection:
            if (
                connection.execute(
                    update(p)
                    .where(
                        p.c.work_id == self.state.planning_key(row["work_id"]),
                        p.c.binding_id == row["binding_id"],
                        p.c.revision == row["revision"],
                    )
                    .values(revision=row["revision"] + 1, **changes)
                ).rowcount
                != 1
            ):
                raise ConcurrentWorkUpdate("deployment provider binding advanced concurrently")

    def change(
        self,
        row: Mapping[str, Any],
        *,
        scope: CompiledBranchScope | None = None,
        declaration: BranchDeclaration | None = None,
        branch_id: str | None = None,
        child_work_id: str | None = None,
        **changes: Any,
    ) -> dict[str, Any] | None:
        f, b = self.tables["frames"], self.tables["branches"]
        with self.engine.begin() as connection:
            current = connection.execute(
                select(f.c.revision)
                .where(
                    f.c.work_id == self.state.planning_key(row["work_id"]),
                )
                .with_for_update()
            ).scalar_one()
            if current != row["revision"]:
                raise ConcurrentWorkUpdate("compiled recipe frame advanced concurrently")
            if scope is not None:
                document = _json(scope)
                connection.execute(
                    self._insert(b)
                    .values(
                        work_id=self.state.planning_key(row["work_id"]),
                        branch_id=scope.branch_id,
                        scope_json=document,
                        selection_sha256=scope.selection.selection_sha256,
                        artifact_count=scope.selection.artifact_count,
                        declared=False,
                    )
                    .on_conflict_do_nothing()
                )
                retained = connection.scalar(
                    select(b.c.scope_json).where(
                        b.c.work_id == self.state.planning_key(row["work_id"]),
                        b.c.branch_id == scope.branch_id,
                    )
                )
                if retained != document:
                    raise ValueError("compiled branch selection was rebound")
            if declaration is not None:
                key = (
                    b.c.work_id == self.state.planning_key(row["work_id"]),
                    b.c.branch_id == declaration.branch_id,
                )
                retained = connection.scalar(select(b.c.declaration_json).where(*key))
                document = _json(declaration)
                if retained not in (None, document):
                    raise ValueError("compiled branch declaration was rebound")
                if (
                    connection.execute(
                        update(b)
                        .where(*key)
                        .values(
                            declaration_json=document,
                            declared=True,
                        )
                    ).rowcount
                    != 1
                ):
                    raise ValueError("compiled declaration lacks its exact selected scope")
            if child_work_id is not None:
                name = declaration.branch_id if declaration is not None else branch_id
                if name is None:
                    raise ValueError("compiled child has no exact branch")
                key = (b.c.work_id == row["work_id"], b.c.branch_id == name)
                retained = connection.execute(select(b.c.child_work_id).where(*key)).first()
                if retained is None or retained[0] not in (None, child_work_id):
                    raise ValueError("compiled child invocation was rebound")
                connection.execute(update(b).where(*key).values(child_work_id=child_work_id))
            if (
                connection.execute(
                    update(f)
                    .where(
                        f.c.work_id == self.state.planning_key(row["work_id"]),
                        f.c.revision == row["revision"],
                    )
                    .values(revision=row["revision"] + 1, **changes)
                ).rowcount
                != 1
            ):
                raise ConcurrentWorkUpdate("compiled recipe frame advanced concurrently")
        return self.load(row["work_id"])
