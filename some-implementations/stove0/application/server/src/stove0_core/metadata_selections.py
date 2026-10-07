"""Resumable construction of the existing exact artifact-selection authority."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from typing import Any

from pydantic import JsonValue
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
    or_,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import Insert as PgInsert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import Insert as SqliteInsert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine, RowMapping
from stove0_protocol import ArtifactSelectionRef, WorkArtifactSubject, canonical_json_bytes
from stove0_protocol.fork_join import update_artifact_selection_commitment

from stove0_core._checkpoint_sha256 import CheckpointSHA256
from stove0_core.planning_context import PlanningContext
from stove0_core.subject_identity import artifact_identity_sha256, subject_identity_sha256
from stove0_core.work_state import ConcurrentWorkUpdate


def declare_selection_builders(metadata: MetaData) -> dict[str, Table]:
    builder = Table(
        "stove0_selection_builders",
        metadata,
        Column("builder_id", String(129), primary_key=True),
        Column(
            "context_id",
            String(64),
            ForeignKey("stove0_planning_contexts.context_id", ondelete="CASCADE"),
        ),
        Column("revision", BigInteger, nullable=False),
        Column("binding_json", Text, nullable=False),
        Column("source_json", Text, nullable=False),
        Column("state", String(16), nullable=False),
        Column("member_count", BigInteger, nullable=False),
        Column("total_bytes", BigInteger, nullable=False),
        Column("hashed_count", BigInteger, nullable=False),
        Column("published_count", BigInteger, nullable=False),
        Column("after_collection_id", BigInteger, nullable=False),
        Column("after_artifact_id", Text, nullable=False),
        Column("hash_state", Text, nullable=False),
        Column("selection_sha256", String(64)),
        CheckConstraint("revision >= 1", name="ck_stove0_selection_builder_revision"),
        CheckConstraint(
            "state IN ('collecting','sealing','publishing','complete')",
            name="ck_stove0_selection_builder_state",
        ),
        CheckConstraint(
            "member_count >= 0 AND total_bytes >= 0 AND hashed_count >= 0 AND published_count >= 0",
            name="ck_stove0_selection_builder_counts",
        ),
        Index("ix_stove0_selection_builders_pending", "state", "builder_id"),
    )
    member = Table(
        "stove0_selection_builder_members",
        metadata,
        Column(
            "builder_id",
            String(129),
            ForeignKey(builder.c.builder_id, ondelete="CASCADE"),
            primary_key=True,
        ),
        Column("subject_id", Text, primary_key=True),
        Column("collection_id", BigInteger, nullable=False),
        Column("artifact_id", String(64), nullable=False),
        Column("document_json", Text, nullable=False),
        Column("artifact_order", BigInteger),
        UniqueConstraint(
            "builder_id",
            "collection_id",
            "artifact_id",
            name="uq_stove0_selection_builder_instance",
        ),
        Index(
            "ix_stove0_selection_builder_member_order", "builder_id", "collection_id", "artifact_id"
        ),
        Index("ix_stove0_selection_builder_publish", "builder_id", "artifact_order"),
    )
    return {"builder": builder, "member": member}


class MetadataSelectionStore:
    def __init__(
        self,
        engine: Engine,
        tables: Mapping[str, Table],
        selection_header: Table,
        selection_members: Table,
        *,
        context: PlanningContext,
    ) -> None:
        self.engine, self.tables = engine, tables
        self.header, self.members = selection_header, selection_members
        self.key, self.context_id = context.key, context.context_id

    def _insert(self, table: Table) -> PgInsert | SqliteInsert:
        return (
            pg_insert(table) if self.engine.dialect.name == "postgresql" else sqlite_insert(table)
        )

    def ensure(
        self, builder_id: str, *, binding: dict[str, JsonValue], source: dict[str, JsonValue]
    ) -> dict[str, Any]:
        encoded = canonical_json_bytes(binding).decode("utf-8")
        b = self.tables["builder"]
        with self.engine.begin() as connection:
            connection.execute(
                self._insert(b)
                .values(
                    builder_id=self.key(builder_id),
                    context_id=self.context_id,
                    revision=1,
                    binding_json=encoded,
                    source_json=canonical_json_bytes(source).decode("utf-8"),
                    state="collecting",
                    member_count=0,
                    total_bytes=0,
                    hashed_count=0,
                    published_count=0,
                    after_collection_id=0,
                    after_artifact_id="",
                    hash_state=CheckpointSHA256().export_state(),
                )
                .on_conflict_do_nothing()
            )
            row = (
                connection.execute(select(b).where(b.c.builder_id == self.key(builder_id)))
                .mappings()
                .one()
            )
            if row["binding_json"] != encoded:
                raise ValueError("selection builder was rebound to another exact input authority")
            return dict(row)

    def load(self, builder_id: str) -> dict[str, Any] | None:
        b = self.tables["builder"]
        with self.engine.connect() as connection:
            row = (
                connection.execute(select(b).where(b.c.builder_id == self.key(builder_id)))
                .mappings()
                .first()
            )
            return None if row is None else dict(row)

    def append(
        self,
        builder_id: str,
        *,
        expected_revision: int,
        subjects: Sequence[WorkArtifactSubject],
        source: dict[str, JsonValue],
        complete: bool,
    ) -> dict[str, Any] | None:
        if len(subjects) > 1000:
            raise ValueError("selection append exceeds the bounded metadata page budget")
        b, m = self.tables["builder"], self.tables["member"]
        with self.engine.begin() as connection:
            row = (
                connection.execute(
                    select(b).where(b.c.builder_id == self.key(builder_id)).with_for_update()
                )
                .mappings()
                .one()
            )
            if row["revision"] != expected_revision:
                raise ConcurrentWorkUpdate("selection source continuation changed concurrently")
            if row["state"] != "collecting":
                raise ValueError("sealed source cannot append additional members")
            amount, size = row["member_count"], row["total_bytes"]
            for subject in subjects:
                document = canonical_json_bytes(
                    subject.model_dump(mode="json", exclude_none=True)
                ).decode("utf-8")
                root_document = connection.scalar(
                    select(m.c.document_json)
                    .where(
                        m.c.builder_id == self.key(builder_id),
                        m.c.collection_id == subject.collection.collection_id,
                    )
                    .limit(1)
                )
                if (
                    root_document is not None
                    and WorkArtifactSubject.model_validate_json(root_document).collection
                    != subject.collection
                ):
                    raise ValueError("selection source contains conflicting roots for a collection")
                existing = connection.scalar(
                    select(m.c.document_json).where(
                        m.c.builder_id == self.key(builder_id), m.c.subject_id == subject.id
                    )
                )
                if existing is not None:
                    # A duplicate is a source error, rather than pagination silently
                    # narrowing the original complete invocation inventory.
                    raise ValueError("source metadata repeats an exact member instance")
                connection.execute(
                    m.insert().values(
                        builder_id=self.key(builder_id),
                        subject_id=subject.id,
                        collection_id=subject.collection.collection_id,
                        artifact_id=subject.artifact_id,
                        document_json=document,
                    )
                )
                amount, size = amount + 1, size + subject.bytes
            connection.execute(
                update(b)
                .where(b.c.builder_id == self.key(builder_id))
                .values(
                    revision=expected_revision + 1,
                    member_count=amount,
                    total_bytes=size,
                    source_json=canonical_json_bytes(source).decode("utf-8"),
                    state="sealing" if complete else "collecting",
                )
            )
        return self.load(builder_id)

    def seal_step(self, builder_id: str, *, limit: int = 100) -> ArtifactSelectionRef | None:
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("selection sealing budget must be between 1 and 100")
        b, m, h, target = self.tables["builder"], self.tables["member"], self.header, self.members
        with self.engine.begin() as connection:
            row = (
                connection.execute(
                    select(b).where(b.c.builder_id == self.key(builder_id)).with_for_update()
                )
                .mappings()
                .one()
            )
            if row["state"] == "collecting":
                return None
            if row["state"] == "complete":
                return _reference(row)
            if row["state"] == "sealing":
                after = or_(
                    m.c.collection_id > row["after_collection_id"],
                    and_(
                        m.c.collection_id == row["after_collection_id"],
                        m.c.artifact_id > row["after_artifact_id"],
                    ),
                )
                rows = (
                    connection.execute(
                        select(m)
                        .where(m.c.builder_id == self.key(builder_id), after)
                        .order_by(m.c.collection_id, m.c.artifact_id)
                        .limit(limit + 1)
                    )
                    .mappings()
                    .all()
                )
                digest, ordinal = (
                    CheckpointSHA256.from_state(row["hash_state"]),
                    row["hashed_count"],
                )
                for member in rows[:limit]:
                    artifact = WorkArtifactSubject.model_validate_json(member["document_json"])
                    update_artifact_selection_commitment(digest, ordinal=ordinal, artifact=artifact)
                    connection.execute(
                        update(m)
                        .where(
                            m.c.builder_id == self.key(builder_id), m.c.subject_id == artifact.id
                        )
                        .values(artifact_order=ordinal)
                    )
                    ordinal += 1
                values = {
                    "revision": row["revision"] + 1,
                    "hashed_count": ordinal,
                    "hash_state": digest.export_state(),
                }
                if rows:
                    last = rows[min(len(rows), limit) - 1]
                    values.update(
                        after_collection_id=last["collection_id"],
                        after_artifact_id=last["artifact_id"],
                    )
                if len(rows) <= limit:
                    if ordinal != row["member_count"]:
                        raise ValueError(
                            "selection seal cannot cover the retained source inventory"
                        )
                    sha256 = digest.hexdigest()
                    values.update(state="publishing", selection_sha256=sha256)
                    connection.execute(
                        self._insert(h)
                        .values(
                            selection_sha256=sha256,
                            artifact_count=ordinal,
                            total_bytes=row["total_bytes"],
                            state="building",
                        )
                        .on_conflict_do_nothing()
                    )
                    existing = (
                        connection.execute(select(h).where(h.c.selection_sha256 == sha256))
                        .mappings()
                        .one()
                    )
                    if (existing["artifact_count"], existing["total_bytes"]) != (
                        ordinal,
                        row["total_bytes"],
                    ):
                        raise ValueError("selection commitment conflicts with existing summary")
                connection.execute(
                    update(b).where(b.c.builder_id == self.key(builder_id)).values(**values)
                )
                return None
            rows = (
                connection.execute(
                    select(m)
                    .where(
                        m.c.builder_id == self.key(builder_id),
                        m.c.artifact_order >= row["published_count"],
                    )
                    .order_by(m.c.artifact_order)
                    .limit(limit + 1)
                )
                .mappings()
                .all()
            )
            ordinal = row["published_count"]
            for member in rows[:limit]:
                document = member["document_json"]
                continuation = hashlib.sha256(
                    b"stove0-artifact-selection-continuation/v1\x00"
                    + row["selection_sha256"].encode("ascii")
                    + b"\x00"
                    + document.encode("utf-8")
                ).hexdigest()
                connection.execute(
                    self._insert(target)
                    .values(
                        selection_sha256=row["selection_sha256"],
                        artifact_id=member["subject_id"],
                        member_identity_sha256=subject_identity_sha256(
                            WorkArtifactSubject.model_validate_json(document)
                        ),
                        artifact_identity_sha256=artifact_identity_sha256(
                            WorkArtifactSubject.model_validate_json(document)
                        ),
                        source_collection_id=member["collection_id"],
                        source_artifact_id=member["artifact_id"],
                        artifact_order=ordinal,
                        continuation_sha256=continuation,
                        document_bytes=len(document.encode("utf-8")),
                        document_json=document,
                    )
                    .on_conflict_do_nothing()
                )
                original = connection.scalar(
                    select(target.c.document_json).where(
                        target.c.selection_sha256 == row["selection_sha256"],
                        target.c.artifact_id == member["subject_id"],
                    )
                )
                if original != document:
                    raise ValueError("selection publication conflicts with existing exact members")
                ordinal += 1
            complete = len(rows) <= limit
            if complete:
                if ordinal != row["member_count"]:
                    raise ValueError("selection publication omits a retained member")
                connection.execute(
                    update(h)
                    .where(h.c.selection_sha256 == row["selection_sha256"])
                    .values(state="sealed")
                )
            connection.execute(
                update(b)
                .where(b.c.builder_id == self.key(builder_id))
                .values(
                    revision=row["revision"] + 1,
                    published_count=ordinal,
                    state="complete" if complete else "publishing",
                )
            )
            return _reference(row) if complete else None


def _reference(row: RowMapping) -> ArtifactSelectionRef:
    return ArtifactSelectionRef.model_validate(
        {
            "selection_sha256": row["selection_sha256"],
            "artifact_count": row["member_count"],
            "total_bytes": str(row["total_bytes"]),
        }
    )
