"""Collection-owned provider execution records; upload retirement cannot erase them."""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    ForeignKeyConstraint,
    Index,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column

from riverhog_core.catalog_base import Base
from riverhog_core.catalog_models import COLLECTION_ID_TYPE


class ArchiveWriteControlRecord(Base):
    __tablename__ = "archive_write_controls"

    collection_id: Mapped[int] = mapped_column(
        COLLECTION_ID_TYPE, primary_key=True, autoincrement=False
    )
    construction_json: Mapped[str] = mapped_column(Text)
    epoch: Mapped[str] = mapped_column(String(64))
    gate: Mapped[str] = mapped_column(String)
    next_call: Mapped[str] = mapped_column(Text)
    terminal_id: Mapped[str | None] = mapped_column(String(64), nullable=True)

    __table_args__ = (
        CheckConstraint("gate IN ('open','closed')", name="ck_archive_write_controls_gate"),
        CheckConstraint(
            "terminal_id IS NULL OR gate = 'closed'",
            name="ck_archive_write_controls_terminal",
        ),
    )


class ArchiveWritePreparationRecord(Base):
    __tablename__ = "archive_write_preparations"

    collection_id: Mapped[int] = mapped_column(COLLECTION_ID_TYPE, primary_key=True)
    object_id: Mapped[str] = mapped_column(String, primary_key=True)
    identity_json: Mapped[str] = mapped_column(Text)
    preparation_json: Mapped[str] = mapped_column(Text)

    __table_args__ = (
        ForeignKeyConstraint(["collection_id"], ["archive_write_controls.collection_id"]),
    )


class ArchiveWriteEffectRecord(Base):
    __tablename__ = "archive_write_effects"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    collection_id: Mapped[int] = mapped_column(COLLECTION_ID_TYPE)
    object_id: Mapped[str] = mapped_column(String)
    sequence: Mapped[str] = mapped_column(Text)
    epoch: Mapped[str] = mapped_column(String(64))
    executor_id: Mapped[str] = mapped_column(String(64))
    dispatch_sha256: Mapped[str] = mapped_column(String(64))
    request_sha256: Mapped[str] = mapped_column(String(64))
    request_json: Mapped[str] = mapped_column(Text)
    resource: Mapped[str] = mapped_column(String)
    kind: Mapped[str] = mapped_column(String)
    state: Mapped[str] = mapped_column(String)
    unresolved: Mapped[bool] = mapped_column(Boolean)
    control_bytes: Mapped[int] = mapped_column(BigInteger)
    result_capacity_bytes: Mapped[int] = mapped_column(BigInteger)
    observation_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    result_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(
            ["collection_id", "object_id"],
            ["archive_write_preparations.collection_id", "archive_write_preparations.object_id"],
        ),
        CheckConstraint(
            "kind IN ('begin','part','complete','lookup','put')",
            name="ck_archive_write_effects_kind",
        ),
        CheckConstraint(
            "state IN ('armed','uncertain','settled')", name="ck_archive_write_effects_state"
        ),
        CheckConstraint(
            "control_bytes > 0 AND result_capacity_bytes > 0",
            name="ck_archive_write_effects_bytes",
        ),
        CheckConstraint(
            "state <> 'settled' OR result_json IS NOT NULL",
            name="ck_archive_write_effects_result",
        ),
        CheckConstraint(
            "state = 'settled' OR result_json IS NULL",
            name="ck_archive_write_effects_unsettled",
        ),
        CheckConstraint(
            "state = 'settled' AND unresolved IS false OR "
            "state IN ('armed','uncertain') AND unresolved IS true",
            name="ck_archive_write_effects_unresolved",
        ),
        Index("ix_archive_write_effects_pending", "collection_id", "unresolved", "id"),
        Index(
            "ix_archive_write_effects_volume",
            "collection_id",
            "object_id",
            "unresolved",
            "resource",
        ),
        Index("ix_archive_write_effects_capacity", "collection_id", "id"),
    )


class ArchiveUploadTerminalRecord(Base):
    __tablename__ = "archive_upload_terminals"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    collection_id: Mapped[int] = mapped_column(COLLECTION_ID_TYPE)
    request_json: Mapped[str] = mapped_column(Text)
    principal_id: Mapped[str] = mapped_column(String)
    kind: Mapped[str] = mapped_column(String)
    phase: Mapped[str] = mapped_column(String)
    frontier: Mapped[str] = mapped_column(Text)
    original_epoch: Mapped[str] = mapped_column(String(64))
    result_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(["collection_id"], ["archive_write_controls.collection_id"]),
        CheckConstraint("kind IN ('cancel','discard')", name="ck_archive_upload_terminals_kind"),
        CheckConstraint(
            "phase IN ('draining','refused-sealed','needs-reconfirmation',"
            "'cleanup-ready','cleaning','done')",
            name="ck_archive_upload_terminals_phase",
        ),
        CheckConstraint(
            "phase <> 'done' OR result_json IS NOT NULL",
            name="ck_archive_upload_terminals_result",
        ),
        Index("ix_archive_upload_terminals_collection", "collection_id", "id"),
    )
