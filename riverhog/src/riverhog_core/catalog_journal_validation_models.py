"""Retained native validation ownership independent of source-row lifetime."""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
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


class JournalValidationJobRecord(Base):
    __tablename__ = "journal_validation_jobs"

    workspace_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    collection_id: Mapped[int | None] = mapped_column(COLLECTION_ID_TYPE, nullable=True)
    journal_id: Mapped[str | None] = mapped_column(String(45), nullable=True)
    attempt_id: Mapped[str] = mapped_column(String, nullable=False)
    source_binding_json: Mapped[str] = mapped_column(Text, nullable=False)
    certificate_binding_json: Mapped[str] = mapped_column(Text, nullable=False)
    worker_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    version: Mapped[str] = mapped_column(String(64), nullable=False)
    state: Mapped[str] = mapped_column(String, nullable=False, default="opening")
    progress_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    outcome_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    failure: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(
            ["collection_id", "journal_id"],
            [
                "collection_upload_provenance_journals.collection_id",
                "collection_upload_provenance_journals.journal_id",
            ],
            ondelete="SET NULL",
        ),
        CheckConstraint(
            "(collection_id IS NULL) = (journal_id IS NULL)",
            name="ck_journal_validation_source_link",
        ),
        CheckConstraint(
            "state NOT IN ('retiring','directory') OR collection_id IS NULL",
            name="ck_journal_validation_retired_source",
        ),
        CheckConstraint("worker_epoch >= 0", name="ck_journal_validation_worker_epoch"),
        CheckConstraint(
            "state IN ('opening','active','complete','failed','lost','retiring','directory')",
            name="ck_journal_validation_state",
        ),
        CheckConstraint(
            "length(workspace_id) = 32 AND lower(workspace_id) = workspace_id AND "
            "length(replace(replace(replace(replace(replace(replace(replace(replace("
            "replace(replace(replace(replace(replace(replace(replace(replace(workspace_id, "
            "'0',''),'1',''),'2',''),'3',''),'4',''),'5',''),'6',''),'7',''),'8',''),'9',''),"
            "'a',''),'b',''),'c',''),'d',''),'e',''),'f','')) = 0",
            name="ck_journal_validation_workspace",
        ),
        CheckConstraint(
            "state NOT IN ('complete','failed') OR outcome_json IS NOT NULL",
            name="ck_journal_validation_outcome",
        ),
        Index(
            "uq_journal_validation_source",
            "collection_id",
            "journal_id",
            unique=True,
            sqlite_where=text("collection_id IS NOT NULL"),
            postgresql_where=text("collection_id IS NOT NULL"),
        ),
        Index("ix_journal_validation_state_workspace", "state", "workspace_id"),
        Index(
            "ix_journal_validation_orphan_workspace",
            "workspace_id",
            sqlite_where=text("collection_id IS NULL AND state NOT IN ('retiring','directory')"),
            postgresql_where=text(
                "collection_id IS NULL AND state NOT IN ('retiring','directory')"
            ),
        ),
    )
