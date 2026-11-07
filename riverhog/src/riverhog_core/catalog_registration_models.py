"""Collection-owned operational registration state retained across upload retirement."""

from __future__ import annotations

from sqlalchemy import CheckConstraint, ForeignKeyConstraint, Index, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from riverhog_core.catalog_base import Base
from riverhog_core.catalog_models import (
    COLLECTION_ID_TYPE,
    _fixed_lowercase_integer_check,
)
from riverhog_core.catalog_types import archive_sequence_type


class CollectionRegistrationRecord(Base):
    __tablename__ = "collection_registrations"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    collection_id: Mapped[int] = mapped_column(COLLECTION_ID_TYPE)
    principal_id: Mapped[str] = mapped_column(String)
    construction_json: Mapped[str] = mapped_column(Text)
    request_sha256: Mapped[str] = mapped_column(String(64))
    request_json: Mapped[str] = mapped_column(Text)
    new_members_json: Mapped[str] = mapped_column(Text)
    base_checkpoint_json: Mapped[str] = mapped_column(Text)
    base_revision: Mapped[str] = mapped_column(Text)
    stage_frontier_json: Mapped[str] = mapped_column(Text)
    verify_frontier_json: Mapped[str] = mapped_column(Text)
    stage_chain: Mapped[str] = mapped_column(String(64))
    verify_chain: Mapped[str] = mapped_column(String(64))
    stage_totals_json: Mapped[str] = mapped_column(Text)
    verify_totals_json: Mapped[str] = mapped_column(Text)
    coverage_audit_after_artifact_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    coverage_audit_after_object_id: Mapped[str | None] = mapped_column(String, nullable=True)
    coverage_audited_rows: Mapped[str] = mapped_column(Text)
    phase: Mapped[str] = mapped_column(String)
    execution_epoch: Mapped[str] = mapped_column(String(64))
    authorized_tag_set_identity: Mapped[str] = mapped_column(String(64))
    key_id: Mapped[str | None] = mapped_column(String, nullable=True)
    key_app: Mapped[str | None] = mapped_column(String, nullable=True)
    key_generation: Mapped[str | None] = mapped_column(String(64), nullable=True)
    claim_id: Mapped[str | None] = mapped_column(String, nullable=True)
    acknowledgement_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    failure_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    disposition_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    cleanup_phase: Mapped[str | None] = mapped_column(String, nullable=True)
    due_at: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[str] = mapped_column(String)
    updated_at: Mapped[str] = mapped_column(String)

    # An upload FK would erase an uncertain-response acknowledgement during
    # finalization. Collection retirement owns removal of this operational state.
    __table_args__ = (
        CheckConstraint(
            "phase IN ('staging','verifying','auditing','ready','published',"
            "'paused','cleaning','disposed','failed')",
            name="ck_collection_registrations_phase",
        ),
        CheckConstraint(
            "key_id IS NULL AND key_app IS NULL AND key_generation IS NULL OR "
            "key_id IS NOT NULL AND key_app IS NOT NULL AND key_generation IS NOT NULL",
            name="ck_collection_registrations_key",
        ),
        CheckConstraint(
            "phase <> 'published' OR acknowledgement_json IS NOT NULL",
            name="ck_collection_registrations_ack",
        ),
        CheckConstraint(
            "acknowledgement_json IS NULL OR phase = 'published'",
            name="ck_collection_registrations_published",
        ),
        CheckConstraint(
            "cleanup_phase IS NULL OR cleanup_phase IN "
            "('coverage','plans','members','summaries','finish','done')",
            name="ck_collection_registrations_cleanup",
        ),
        CheckConstraint(
            "phase NOT IN ('cleaning','disposed') OR disposition_json IS NOT NULL",
            name="ck_collection_registrations_disposition",
        ),
        CheckConstraint(
            "phase NOT IN ('cleaning','disposed') OR cleanup_phase IS NOT NULL",
            name="ck_collection_registrations_cleanup_required",
        ),
        CheckConstraint(
            "phase <> 'disposed' OR cleanup_phase = 'done'",
            name="ck_collection_registrations_disposed",
        ),
        Index("ix_collection_registrations_collection", "collection_id", "id"),
        Index("ix_collection_registrations_request", "collection_id", "request_sha256"),
        Index("ix_collection_registrations_due", "phase", "due_at", "collection_id", "id"),
    )


class CollectionRegistrationVolumeRecord(Base):
    """Immutable summaries authored with each staging quantum, never payload authority."""

    __tablename__ = "collection_registration_volumes"

    registration_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    sequence: Mapped[int] = mapped_column(archive_sequence_type(), primary_key=True)
    object_id: Mapped[str] = mapped_column(String)
    kind: Mapped[str] = mapped_column(String)

    __table_args__ = (
        ForeignKeyConstraint(["registration_id"], ["collection_registrations.id"]),
        CheckConstraint(
            _fixed_lowercase_integer_check("sequence", 64),
            name="ck_collection_registration_volumes_sequence",
        ),
        CheckConstraint(
            "kind IN ('pack','segment')", name="ck_collection_registration_volumes_kind"
        ),
    )
