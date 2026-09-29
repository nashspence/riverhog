from __future__ import annotations

import uuid

import pytest
from riverhog_core.catalog_db import Base, create_catalog_engine
from riverhog_core.catalog_models import (
    CollectionUploadArtifactProvenanceBindingRecord,
    CollectionUploadArtifactRecord,
    CollectionUploadProvenanceJournalRecord,
    CollectionUploadRecord,
    StorageIncarnationRecord,
)
from riverhog_protocol import collection_tag_set_identity
from sqlalchemy import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session


def test_upload_membership_is_opaque_and_provenance_binding_is_exact() -> None:
    engine = create_catalog_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session, session.begin():
        incarnation_id = str(uuid.uuid4())
        session.add(
            StorageIncarnationRecord(
                id=incarnation_id,
                kind="archive",
                name="archive",
                state="bound",
                created_at="2026-01-01T00:00:00Z",
            )
        )
        session.flush()
        session.add(
            CollectionUploadRecord(
                collection_id=1,
                idempotency_key="opaque-members",
                creation_identity_sha256="a" * 64,
                initial_tag_set_identity=collection_tag_set_identity(None),
                encryption_format="age-v1-scrypt",
                passphrase_id="test-key",
                archive_store="archive",
                archive_incarnation_id=incarnation_id,
                opened_at="2026-01-01T00:00:00Z",
                last_activity_at="2026-01-01T00:00:00Z",
                archive_phase_updated_at="2026-01-01T00:00:00Z",
                archive_storage_prefix="collections/1",
                planner_checkpoint_json="{}",
            )
        )
    with Session(engine) as session, session.begin():
        session.execute(
            insert(CollectionUploadArtifactRecord),
            [
                {
                    "collection_id": 1,
                    "artifact_id": artifact_id,
                    "artifact_order": order,
                    "bytes": 3,
                    "sha256": "b" * 64,
                }
                for order, artifact_id in enumerate(("1" * 64, "2" * 64))
            ],
        )
        session.add(
            CollectionUploadProvenanceJournalRecord(
                collection_id=1,
                journal_id="urn:uuid:00000000-0000-4000-8000-000000000001",
                bytes=32,
                sha256="c" * 64,
                content_hash_state="{}",
            )
        )
    with Session(engine) as session, session.begin():
        session.add(
            CollectionUploadArtifactProvenanceBindingRecord(
                collection_id=1,
                artifact_id="1" * 64,
                journal_id="urn:uuid:00000000-0000-4000-8000-000000000001",
                through_entry_id="urn:uuid:00000000-0000-4000-8000-000000000002",
                through_sequence=0,
                through_json_sha256="d" * 64,
                prefix_sha256="c" * 64,
                prefix_bytes=32,
                delivery_association_id="urn:uuid:00000000-0000-4000-8000-000000000003",
            )
        )
    with Session(engine) as session:
        assert session.get(CollectionUploadArtifactRecord, (1, "1" * 64)).sha256 == "b" * 64
        assert session.get(CollectionUploadArtifactRecord, (1, "2" * 64)).sha256 == "b" * 64
        assert session.get(CollectionUploadArtifactProvenanceBindingRecord, (1, "1" * 64))
        assert session.get(CollectionUploadArtifactProvenanceBindingRecord, (1, "2" * 64)) is None
    with Session(engine) as session, pytest.raises(IntegrityError):
        session.execute(
            insert(CollectionUploadArtifactRecord).values(
                collection_id=1,
                artifact_id="1" * 64,
                artifact_order=2,
                bytes=3,
                sha256="b" * 64,
            )
        )
