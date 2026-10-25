"""Catalog custody of one retained canonical discovery build attempt."""

from __future__ import annotations

from sqlalchemy import BigInteger, CheckConstraint, ForeignKeyConstraint, Index, String, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from riverhog_core.catalog_base import Base


class DiscoveryValidationJobRecord(Base):
    __tablename__ = "discovery_validation_jobs"

    workspace_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    build_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    attempt_id: Mapped[str] = mapped_column(String(32), nullable=False)
    admission_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    archive_generation: Mapped[str] = mapped_column(String(64), nullable=False)
    archive_root_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    provenance_identity: Mapped[str] = mapped_column(String(64), nullable=False)
    artifact_set_identity: Mapped[str] = mapped_column(String(64), nullable=False)
    journal_count: Mapped[str] = mapped_column(Text, nullable=False)
    build_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    policy_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    catalog_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    worker_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    version: Mapped[str] = mapped_column(String(64), nullable=False)
    state: Mapped[str] = mapped_column(String, nullable=False, default="preparing")
    source_manifest_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    source_count: Mapped[str | None] = mapped_column(Text, nullable=True)
    selection_manifest_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    selection_count: Mapped[str | None] = mapped_column(Text, nullable=True)
    failure: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(
            ["build_id"], ["collection_provenance_index_generations.build_id"], ondelete="SET NULL"
        ),
        CheckConstraint("build_epoch >= 1", name="ck_discovery_validation_build_epoch"),
        CheckConstraint("worker_epoch >= 0", name="ck_discovery_validation_worker_epoch"),
        CheckConstraint(
            "state IN ('preparing','ready','active','lost','retiring','directory')",
            name="ck_discovery_validation_state",
        ),
        CheckConstraint(
            "state NOT IN ('retiring','directory') OR build_id IS NULL",
            name="ck_discovery_validation_retired",
        ),
        CheckConstraint(
            "(source_manifest_sha256 IS NULL AND source_count IS NULL AND "
            "selection_manifest_sha256 IS NULL AND selection_count IS NULL) OR "
            "(source_manifest_sha256 IS NOT NULL AND source_count IS NOT NULL AND "
            "selection_manifest_sha256 IS NOT NULL AND selection_count IS NOT NULL)",
            name="ck_discovery_validation_pinned_inputs",
        ),
        CheckConstraint(
            "state NOT IN ('ready','active') OR source_manifest_sha256 IS NOT NULL",
            name="ck_discovery_validation_ready_inputs",
        ),
        CheckConstraint(
            "state != 'lost' OR failure IS NOT NULL",
            name="ck_discovery_validation_lost_reason",
        ),
        *(
            CheckConstraint(
                f"length({column}) = 32 AND lower({column}) = {column} AND "
                + "length("
                + "replace(" * 16
                + column
                + "".join(f",'{value}','')" for value in "0123456789abcdef")
                + ") = 0",
                name="ck_discovery_validation_" + column,
            )
            for column in ("workspace_id", "attempt_id")
        ),
        Index(
            "uq_discovery_validation_build",
            "build_id",
            unique=True,
            sqlite_where=text("build_id IS NOT NULL"),
            postgresql_where=text("build_id IS NOT NULL"),
        ),
        Index("ix_discovery_validation_state_workspace", "state", "workspace_id"),
        Index(
            "ix_discovery_validation_orphan_workspace",
            "workspace_id",
            sqlite_where=text("build_id IS NULL AND state NOT IN ('retiring','directory')"),
            postgresql_where=text("build_id IS NULL AND state NOT IN ('retiring','directory')"),
        ),
    )
