from __future__ import annotations

import json
import uuid
from pathlib import Path

from riverhog_core.catalog_base import Base
from riverhog_core.catalog_db import make_session_factory, session_scope
from riverhog_core.catalog_models import (
    CollectionArchiveObjectUploadRecord,
    CollectionUploadArtifactRecord,
    CollectionUploadArtifactVolumeRecord,
    CollectionUploadRecord,
    StorageIncarnationRecord,
)
from riverhog_core.services.collection_uploads import (
    SqlAlchemyCollectionUploadService,
    _artifact_payload,
    _custody_stats,
    _record_payload_custody_progress,
)
from riverhog_protocol import CollectionUploadArtifactCustodyReceiptDocument
from sqlalchemy import select

from tests.unit.db_helpers import sqlite_url


def test_source_release_waits_for_payload_and_provenance_roots(tmp_path: Path) -> None:
    factory = make_session_factory(sqlite_url(tmp_path / "custody.db"))
    engine = factory.kw["bind"]
    Base.metadata.create_all(engine)
    incarnation_id = str(uuid.uuid4())
    member_id = "a" * 64
    with session_scope(factory) as session:
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
                idempotency_key="one",
                creation_identity_sha256="1" * 64,
                initial_tag_set_identity="2" * 64,
                archive_generation="3" * 64,
                encryption_format="age-scrypt/v1",
                passphrase_id="passphrase-identity",
                archive_store="archive",
                archive_incarnation_id=incarnation_id,
                opened_at="2026-01-01T00:00:00Z",
                last_activity_at="2026-01-01T00:00:00Z",
                archive_phase_updated_at="2026-01-01T00:00:00Z",
                archive_storage_prefix="collection/1",
                planner_checkpoint_json="{}",
                state="finalizing",
                artifact_count=1,
                artifact_bytes=3,
                provenance_identity="5" * 64,
                provenance_archive_root_receipt_json=json.dumps({"identity": "5" * 64, "root": {}}),
            )
        )
        session.add(
            CollectionUploadArtifactRecord(
                collection_id=1,
                artifact_id=member_id,
                artifact_order=0,
                bytes=3,
                sha256="4" * 64,
            )
        )
        for sequence in range(2):
            volume_id = f"segment-{'0' * 63}{sequence}"
            session.add(
                CollectionArchiveObjectUploadRecord(
                    collection_id=1,
                    object_id=volume_id,
                    sequence=sequence,
                    kind="segment",
                    relative_path=f"volumes/{volume_id}.age",
                    object_path=f"collection/1/volumes/{volume_id}.age",
                    plaintext_bytes=2,
                    source_bytes=2,
                    source_artifact_id=member_id,
                    source_first_part=sequence,
                    source_part_count=1,
                    unit_plaintext_bytes=2,
                    plan_json="{}",
                    plan_sha256="6" * 64,
                    state="planned",
                    uploaded_bytes=0,
                    uploaded_units=0,
                    total_units=1,
                    updated_at="2026-01-01T00:00:00Z",
                )
            )
        session.flush()
        for sequence in range(2):
            session.add(
                CollectionUploadArtifactVolumeRecord(
                    collection_id=1,
                    artifact_id=member_id,
                    object_id=f"segment-{'0' * 63}{sequence}",
                )
            )
    service = object.__new__(SqlAlchemyCollectionUploadService)
    service._session_factory = factory

    with session_scope(factory) as session:
        upload = session.get(CollectionUploadRecord, 1)
        first = session.get(CollectionArchiveObjectUploadRecord, (1, "segment-" + "0" * 64))
        assert upload is not None and first is not None
        first.state = "sealed"
        first.sealed_receipt_json = '{"sealed":true}'
        _record_payload_custody_progress(session, upload, first, now="2026-01-01T00:01:00Z")
        assert upload.payload_sealed_artifact_count == 0
    with session_scope(factory) as session:
        upload = session.get(CollectionUploadRecord, 1)
        second = session.get(CollectionArchiveObjectUploadRecord, (1, "segment-" + "0" * 63 + "1"))
        assert upload is not None and second is not None
        second.state = "sealed"
        second.sealed_receipt_json = '{"sealed":true}'
        _record_payload_custody_progress(session, upload, second, now="2026-01-01T00:02:00Z")
        assert upload.payload_sealed_artifact_count == 1
        artifact = session.get(CollectionUploadArtifactRecord, (1, member_id))
        assert artifact is not None
        assert _artifact_payload(artifact)["custody_receipt"] is None
        assert _artifact_payload(artifact)["payload_sealed"] is True
        assert _custody_stats(session, 1) == (0, 0)
    assert service._publish_custody_receipts(1) is False
    with session_scope(factory) as session:
        upload = session.get(CollectionUploadRecord, 1)
        assert upload is not None
        upload.final_authority_json = json.dumps(
            {"root": {"plaintext_sha256": "7" * 64}, "recovery": {}}
        )
    assert service._publish_custody_receipts(1) is True
    assert service._publish_custody_receipts(1) is False
    with session_scope(factory) as session:
        row = session.scalar(select(CollectionUploadArtifactRecord))
        assert row is not None and row.custody_receipt_json is not None
        receipt = CollectionUploadArtifactCustodyReceiptDocument.model_validate_json(
            row.custody_receipt_json
        )
        assert receipt.archive_root_sha256 == "7" * 64
        assert receipt.provenance_root_sha256 == "5" * 64
        assert receipt.archive_object_count == 2
        assert _custody_stats(session, 1) == (1, 3)
