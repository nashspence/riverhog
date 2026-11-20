"""Independent cleanup scopes share terminal capacity, never disposal authority."""

from __future__ import annotations

from sqlalchemy import Boolean, CheckConstraint, ForeignKeyConstraint, Index, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from riverhog_core.catalog_base import Base


class ObjectCleanupPageRecord(Base):
    """One current exact selection; completed page history is not accumulated."""

    __tablename__ = "object_cleanup_pages"

    scope_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    terminal_id: Mapped[str] = mapped_column(String(64))
    request_sha256: Mapped[str] = mapped_column(String(64))
    inventory_call_id: Mapped[str] = mapped_column(String(64))
    inventory_request_json: Mapped[str] = mapped_column(Text)
    request_json: Mapped[str] = mapped_column(Text)
    disposed_json: Mapped[str] = mapped_column(Text)

    __table_args__ = (
        ForeignKeyConstraint(["terminal_id"], ["archive_upload_terminals.id"]),
        Index("ix_object_cleanup_pages_terminal", "terminal_id", "scope_id"),
    )


class ObjectCleanupAttemptRecord(Base):
    """Independent physical execution charge, retained across later monotone retries."""

    __tablename__ = "object_cleanup_attempts"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    terminal_id: Mapped[str] = mapped_column(String(64))
    scope_id: Mapped[str] = mapped_column(String(64))
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
            name="ck_object_cleanup_attempts_closed",
        ),
        Index("ix_object_cleanup_attempts_terminal", "terminal_id", "id"),
        Index("ix_object_cleanup_attempts_scope", "scope_id", "id"),
    )
