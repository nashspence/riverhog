"""Original physical provenance source facts and their receiving checkpoints."""

from __future__ import annotations

from sqlalchemy import CheckConstraint, ForeignKeyConstraint, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from riverhog_core.catalog_base import Base
from riverhog_core.catalog_models import COLLECTION_ID_TYPE
from riverhog_core.catalog_types import archive_sequence_type


class CollectionUploadProvenanceJournalPublicationRecord(Base):
    __tablename__ = "collection_upload_provenance_journal_publications"

    collection_id: Mapped[int] = mapped_column(COLLECTION_ID_TYPE, primary_key=True)
    plaintext_sha256: Mapped[str] = mapped_column(String(64), primary_key=True)
    source_json: Mapped[str] = mapped_column(Text)
    source_sha256: Mapped[str] = mapped_column(String(64))
    receipt_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(
            ["collection_id"], ["collection_uploads.collection_id"], ondelete="CASCADE"
        ),
        CheckConstraint("length(source_json) > 0", name="ck_provenance_journal_publication_source"),
    )


class CollectionUploadProvenanceVolumePublicationRecord(Base):
    __tablename__ = "collection_upload_provenance_volume_publications"

    collection_id: Mapped[int] = mapped_column(COLLECTION_ID_TYPE, primary_key=True)
    sequence: Mapped[int] = mapped_column(archive_sequence_type(), primary_key=True)
    document_json: Mapped[str] = mapped_column(Text)
    source_json: Mapped[str] = mapped_column(Text)
    source_sha256: Mapped[str] = mapped_column(String(64))
    binding_payload_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    payload_receipt_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    metadata_receipt_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    progress_receipt_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(
            ["collection_id"], ["collection_uploads.collection_id"], ondelete="CASCADE"
        ),
        UniqueConstraint(
            "collection_id",
            "binding_payload_sha256",
            name="uq_provenance_volume_publication_binding_payload",
        ),
        CheckConstraint(
            "length(document_json) > 0 AND length(source_json) > 0",
            name="ck_provenance_volume_publication_source",
        ),
    )


class CollectionUploadProvenanceAuthorityPublicationRecord(Base):
    __tablename__ = "collection_upload_provenance_authority_publications"

    collection_id: Mapped[int] = mapped_column(COLLECTION_ID_TYPE, primary_key=True)
    role: Mapped[str] = mapped_column(String, primary_key=True)
    document_json: Mapped[str] = mapped_column(Text)
    source_json: Mapped[str] = mapped_column(Text)
    source_sha256: Mapped[str] = mapped_column(String(64))
    receipt_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    progress_receipt_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(
            ["collection_id"], ["collection_uploads.collection_id"], ondelete="CASCADE"
        ),
        CheckConstraint("role IN ('terminal','root')", name="ck_provenance_authority_role"),
        CheckConstraint(
            "length(document_json) > 0 AND length(source_json) > 0",
            name="ck_provenance_authority_source",
        ),
    )
