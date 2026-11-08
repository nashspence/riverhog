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
    text,
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
    custody_revision: Mapped[str] = mapped_column(Text, default="0", server_default=text("'0'"))
    terminal_id: Mapped[str | None] = mapped_column(String(64), nullable=True)

    custody_primary_count: Mapped[int] = mapped_column(
        BigInteger, default=0, server_default=text("0")
    )
    custody_history_count: Mapped[int] = mapped_column(
        BigInteger, default=0, server_default=text("0")
    )
    custody_due_count: Mapped[int] = mapped_column(BigInteger, default=0, server_default=text("0"))
    custody_requires_history: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default=text("false")
    )
    custody_after_artifact_id: Mapped[str | None] = mapped_column(String(64), nullable=True)

    __table_args__ = (
        CheckConstraint("gate IN ('open','closed')", name="ck_archive_write_controls_gate"),
        CheckConstraint(
            "custody_primary_count >= 0 AND custody_history_count >= 0 AND "
            "custody_history_count <= custody_primary_count AND "
            "custody_due_count >= 0 AND custody_due_count <= custody_primary_count",
            name="ck_archive_write_controls_custody_schedule",
        ),
        Index(
            "ix_archive_write_controls_custody_due",
            "collection_id",
            sqlite_where=text("gate = 'open' AND custody_due_count > 0"),
            postgresql_where=text("((gate) = 'open') AND (custody_due_count > 0)"),
        ),
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
    key_id: Mapped[str | None] = mapped_column(String, nullable=True)
    kind: Mapped[str] = mapped_column(String)
    phase: Mapped[str] = mapped_column(String)
    frontier: Mapped[str] = mapped_column(Text)
    original_epoch: Mapped[str] = mapped_column(String(64))
    custody_revision: Mapped[str] = mapped_column(Text, default="0", server_default=text("'0'"))
    drain_next_attempt_at: Mapped[str] = mapped_column(
        String,
        default="1970-01-01T00:00:00.000000000Z",
        server_default=text("'1970-01-01T00:00:00.000000000Z'"),
    )
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
        Index(
            "ix_archive_upload_terminals_draining_due",
            "drain_next_attempt_at",
            "id",
            sqlite_where=text("phase = 'draining'"),
            postgresql_where=text("(phase) = 'draining'"),
        ),
    )


class ArchiveUploadTerminalTagRecord(Base):
    """Retain the original staged tag selector independently of upload teardown."""

    __tablename__ = "archive_upload_terminal_tags"

    terminal_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    root_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    tag_set_identity: Mapped[str] = mapped_column(String(64))

    __table_args__ = (
        ForeignKeyConstraint(["terminal_id"], ["archive_upload_terminals.id"]),
        ForeignKeyConstraint(["root_sha256"], ["collection_tag_nodes.digest"], ondelete="RESTRICT"),
        Index("ix_archive_upload_terminal_tags_root", "root_sha256", "terminal_id"),
    )


class ArchiveCleanupPageRecord(Base):
    """One current exact selection; completed page history is not accumulated."""

    __tablename__ = "archive_cleanup_pages"

    terminal_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    request_sha256: Mapped[str] = mapped_column(String(64))
    inventory_call_id: Mapped[str] = mapped_column(String(64))
    inventory_request_json: Mapped[str] = mapped_column(Text)
    request_json: Mapped[str] = mapped_column(Text)
    disposed_json: Mapped[str] = mapped_column(Text)

    __table_args__ = (ForeignKeyConstraint(["terminal_id"], ["archive_upload_terminals.id"]),)


class ArchiveCleanupAttemptRecord(Base):
    """Independent physical execution charge, retained across later monotone retries."""

    __tablename__ = "archive_cleanup_attempts"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    terminal_id: Mapped[str] = mapped_column(String(64))
    page_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    dispatch_sha256: Mapped[str] = mapped_column(String(64))
    invocation_json: Mapped[str] = mapped_column(Text)
    unresolved: Mapped[bool] = mapped_column(Boolean)
    control_bytes: Mapped[str] = mapped_column(Text)
    provider_requests: Mapped[str] = mapped_column(Text)
    in_flight: Mapped[str] = mapped_column(Text)
    response_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(["terminal_id"], ["archive_upload_terminals.id"]),
        CheckConstraint(
            "unresolved IS true OR response_json IS NOT NULL",
            name="ck_archive_cleanup_attempts_closed",
        ),
        Index("ix_archive_cleanup_attempts_terminal", "terminal_id", "id"),
    )
