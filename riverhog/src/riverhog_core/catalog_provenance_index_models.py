"""Rebuildable, versioned canonical provenance discovery projection.

Archive roots and exact journals remain authority. These rows may be discarded
and reconstructed from authenticated archive contents without changing a member.
"""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column

from riverhog_core.catalog_base import Base
from riverhog_core.catalog_models import COLLECTION_ID_TYPE


class CollectionProvenanceIndexStateRecord(Base):
    __tablename__ = "collection_provenance_index_state"

    collection_id: Mapped[int] = mapped_column(COLLECTION_ID_TYPE, primary_key=True)
    epoch: Mapped[int] = mapped_column(BigInteger, nullable=False, default=1)
    active_build_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    pending_build_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    phase: Mapped[str] = mapped_column(String, nullable=False, default="indexing")
    failure: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(["collection_id"], ["collections.id"], ondelete="CASCADE"),
        CheckConstraint("epoch >= 1", name="ck_provenance_index_state_epoch"),
        CheckConstraint(
            "phase IN ('indexing','ready','failed')",
            name="ck_provenance_index_state_phase",
        ),
    )


class CollectionProvenanceIndexGenerationRecord(Base):
    __tablename__ = "collection_provenance_index_generations"

    build_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    collection_id: Mapped[int] = mapped_column(COLLECTION_ID_TYPE, nullable=False)
    archive_generation: Mapped[str] = mapped_column(String(64), nullable=False)
    archive_root_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    provenance_identity: Mapped[str] = mapped_column(String(64), nullable=False)
    core_contract_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    extraction_contract_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    dataset_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    generation_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    complete: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    expected_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[str] = mapped_column(String, nullable=False)
    completed_at: Mapped[str | None] = mapped_column(String, nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(["collection_id"], ["collections.id"], ondelete="CASCADE"),
        CheckConstraint("expected_epoch >= 1", name="ck_provenance_index_generation_epoch"),
        CheckConstraint(
            "complete = false OR dataset_sha256 IS NOT NULL AND generation_id IS NOT NULL",
            name="ck_provenance_index_generation_complete",
        ),
        Index("ix_provenance_index_generations_collection", "collection_id", "complete"),
    )


class CollectionProvenanceIndexSnapshotRecord(Base):
    __tablename__ = "collection_provenance_index_snapshots"

    build_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    journal_id: Mapped[str] = mapped_column(String(45), primary_key=True)
    prefix_sha256: Mapped[str] = mapped_column(String(64), primary_key=True)
    prefix_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    through_entry_id: Mapped[str] = mapped_column(String(45), nullable=False)
    through_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    through_json_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    assertion_count: Mapped[int] = mapped_column(BigInteger, nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["build_id"],
            ["collection_provenance_index_generations.build_id"],
            ondelete="CASCADE",
        ),
        CheckConstraint("prefix_bytes > 0", name="ck_provenance_index_snapshot_bytes"),
        CheckConstraint("assertion_count >= 0", name="ck_provenance_index_snapshot_assertions"),
        Index("ix_provenance_index_snapshots_journal", "journal_id", "prefix_sha256"),
    )


class CollectionProvenanceIndexEntryRecord(Base):
    __tablename__ = "collection_provenance_index_entries"

    build_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    journal_id: Mapped[str] = mapped_column(String(45), primary_key=True)
    prefix_sha256: Mapped[str] = mapped_column(String(64), primary_key=True)
    sequence: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    entry_id: Mapped[str] = mapped_column(String(45), nullable=False)
    json_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    entry_kind: Mapped[str] = mapped_column(String, nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["build_id", "journal_id", "prefix_sha256"],
            [
                "collection_provenance_index_snapshots.build_id",
                "collection_provenance_index_snapshots.journal_id",
                "collection_provenance_index_snapshots.prefix_sha256",
            ],
            ondelete="CASCADE",
        ),
        CheckConstraint("sequence >= 0", name="ck_provenance_index_entry_sequence"),
        Index("ix_provenance_index_entries_identity", "build_id", "entry_id"),
    )


class CollectionProvenanceIndexAssertionRecord(Base):
    __tablename__ = "collection_provenance_index_assertions"

    build_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    row_key: Mapped[str] = mapped_column(String(64), primary_key=True)
    journal_id: Mapped[str] = mapped_column(String(45), nullable=False)
    prefix_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    entry_id: Mapped[str] = mapped_column(String(45), nullable=False)
    assertion_id: Mapped[str] = mapped_column(String(45), nullable=False)
    referent_id: Mapped[str] = mapped_column(String(45), nullable=False)
    kind: Mapped[str] = mapped_column(String(160), nullable=False)
    assertion_state: Mapped[str] = mapped_column(String, nullable=False)
    assertion_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    canonical_json: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["build_id", "journal_id", "prefix_sha256", "sequence"],
            [
                "collection_provenance_index_entries.build_id",
                "collection_provenance_index_entries.journal_id",
                "collection_provenance_index_entries.prefix_sha256",
                "collection_provenance_index_entries.sequence",
            ],
            ondelete="CASCADE",
        ),
        CheckConstraint(
            "assertion_state IN ('effective','retracted')",
            name="ck_provenance_index_assertion_state",
        ),
        Index("ix_provenance_index_assertion_kind", "build_id", "kind", "assertion_state"),
        Index("ix_provenance_index_assertion_referent", "build_id", "referent_id"),
        Index("ix_provenance_index_assertion_identity", "build_id", "assertion_id"),
    )


class CollectionProvenanceIndexProfileRecord(Base):
    __tablename__ = "collection_provenance_index_profiles"

    build_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    row_key: Mapped[str] = mapped_column(String(64), primary_key=True)
    pointer: Mapped[str] = mapped_column(String(4096), primary_key=True)
    contract_id: Mapped[str] = mapped_column(String(1024), nullable=False)
    contract_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    schema_id: Mapped[str] = mapped_column(String(1024), nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["build_id", "row_key"],
            [
                "collection_provenance_index_assertions.build_id",
                "collection_provenance_index_assertions.row_key",
            ],
            ondelete="CASCADE",
        ),
        Index("ix_provenance_index_profile_pin", "build_id", "contract_sha256", "schema_id"),
    )


class CollectionProvenanceIndexValueRecord(Base):
    __tablename__ = "collection_provenance_index_values"

    build_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    row_key: Mapped[str] = mapped_column(String(64), primary_key=True)
    ordinal: Mapped[int] = mapped_column(Integer, primary_key=True)
    pointer: Mapped[str] = mapped_column(String(4096), nullable=False)
    representation: Mapped[str] = mapped_column(String(32), nullable=False)
    scalar_type: Mapped[str] = mapped_column(String(16), nullable=False)
    source_value_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    exact_json: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    text_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    folded_text: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(
            ["build_id", "row_key"],
            [
                "collection_provenance_index_assertions.build_id",
                "collection_provenance_index_assertions.row_key",
            ],
            ondelete="CASCADE",
        ),
        CheckConstraint("ordinal >= 0", name="ck_provenance_index_value_ordinal"),
        CheckConstraint(
            "representation IN ('json-scalar','declared-text','byte-escape','exact-bytes')",
            name="ck_provenance_index_value_representation",
        ),
        CheckConstraint(
            "scalar_type IN ('string','integer','boolean','null')",
            name="ck_provenance_index_value_type",
        ),
        Index("ix_provenance_index_value_pointer", "build_id", "pointer", "representation"),
        Index("ix_provenance_index_value_type", "build_id", "scalar_type", "source_value_sha256"),
    )


class CollectionProvenanceIndexTextChunkRecord(Base):
    __tablename__ = "collection_provenance_index_text_chunks"

    build_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    row_key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value_ordinal: Mapped[int] = mapped_column(Integer, primary_key=True)
    chunk_ordinal: Mapped[int] = mapped_column(Integer, primary_key=True)
    codepoint_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)
    text_chunk: Mapped[str] = mapped_column(Text, nullable=False)
    folded_chunk: Mapped[str] = mapped_column(Text, nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["build_id", "row_key", "value_ordinal"],
            [
                "collection_provenance_index_values.build_id",
                "collection_provenance_index_values.row_key",
                "collection_provenance_index_values.ordinal",
            ],
            ondelete="CASCADE",
        ),
        CheckConstraint("chunk_ordinal >= 0", name="ck_provenance_index_text_ordinal"),
        CheckConstraint("codepoint_offset >= 0", name="ck_provenance_index_text_offset"),
        Index(
            "ix_provenance_index_text_trgm",
            "text_chunk",
            postgresql_using="gin",
            postgresql_ops={"text_chunk": "gin_trgm_ops"},
        ),
        Index(
            "ix_provenance_index_folded_trgm",
            "folded_chunk",
            postgresql_using="gin",
            postgresql_ops={"folded_chunk": "gin_trgm_ops"},
        ),
    )


class CollectionProvenanceIndexEdgeRecord(Base):
    __tablename__ = "collection_provenance_index_edges"

    build_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    row_key: Mapped[str] = mapped_column(String(64), primary_key=True)
    ordinal: Mapped[int] = mapped_column(Integer, primary_key=True)
    role: Mapped[str] = mapped_column(String(80), nullable=False)
    target_id: Mapped[str] = mapped_column(String(45), nullable=False)
    target_type: Mapped[str] = mapped_column(String(80), nullable=False)
    target_scope: Mapped[str] = mapped_column(String(16), nullable=False)
    exact_foreign_reference_json: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(
            ["build_id", "row_key"],
            [
                "collection_provenance_index_assertions.build_id",
                "collection_provenance_index_assertions.row_key",
            ],
            ondelete="CASCADE",
        ),
        Index("ix_provenance_index_edge_target", "build_id", "target_id", "role"),
    )


class CollectionProvenanceIndexMemberRecord(Base):
    __tablename__ = "collection_provenance_index_members"

    build_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    artifact_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    journal_id: Mapped[str] = mapped_column(String(45), nullable=False)
    prefix_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    delivery_association_id: Mapped[str] = mapped_column(String(45), nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["build_id"],
            ["collection_provenance_index_generations.build_id"],
            ondelete="CASCADE",
        ),
        CheckConstraint("bytes >= 0", name="ck_provenance_index_member_bytes"),
        Index("ix_provenance_index_member_digest", "build_id", "sha256"),
    )


class CollectionProvenanceIndexMembershipRecord(Base):
    __tablename__ = "collection_provenance_index_memberships"

    build_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    artifact_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    row_key: Mapped[str] = mapped_column(String(64), primary_key=True)
    scope: Mapped[str] = mapped_column(String(24), primary_key=True)

    __table_args__ = (
        ForeignKeyConstraint(
            ["build_id", "artifact_id"],
            [
                "collection_provenance_index_members.build_id",
                "collection_provenance_index_members.artifact_id",
            ],
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["build_id", "row_key"],
            [
                "collection_provenance_index_assertions.build_id",
                "collection_provenance_index_assertions.row_key",
            ],
            ondelete="CASCADE",
        ),
        CheckConstraint(
            "scope IN ('member','input-history','collection','recorded-history')",
            name="ck_provenance_index_membership_scope",
        ),
        Index("ix_provenance_index_membership_row", "build_id", "row_key", "scope"),
    )


__all__ = [
    "CollectionProvenanceIndexAssertionRecord",
    "CollectionProvenanceIndexEdgeRecord",
    "CollectionProvenanceIndexEntryRecord",
    "CollectionProvenanceIndexGenerationRecord",
    "CollectionProvenanceIndexMemberRecord",
    "CollectionProvenanceIndexMembershipRecord",
    "CollectionProvenanceIndexProfileRecord",
    "CollectionProvenanceIndexSnapshotRecord",
    "CollectionProvenanceIndexStateRecord",
    "CollectionProvenanceIndexTextChunkRecord",
    "CollectionProvenanceIndexValueRecord",
]
