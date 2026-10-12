from __future__ import annotations

import hashlib
import uuid
from pathlib import Path

import pytest
from riverhog_core.catalog_db import (
    Base,
    create_catalog_engine,
    make_session_factory,
    session_scope,
)
from riverhog_core.catalog_models import (
    CollectionUploadProvenanceJournalChunkRecord,
    CollectionUploadProvenanceJournalRecord,
    CollectionUploadRecord,
    StorageIncarnationRecord,
)
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services.collection_uploads import (
    SqlAlchemyCollectionUploadService,
    _journal_payload,
    _validate_next_upload_journal_entry,
)
from riverhog_protocol import (
    CollectionUploadProvenanceJournalCreateDocument,
    collection_tag_set_identity,
)
from riverhog_protocol.collection_completion import (
    COMPLETION_REQUIRED_RECORD_KINDS,
    CollectionCompletionRecordingRequestDocument,
    CollectionCompletionRequirementDocument,
)
from riverhog_protocol.errors import Conflict
from riverhog_provenance import BoundedSourceObserver, BytesSource, create_journal, validate_journal
from sqlalchemy.orm import Session

from tests.unit.db_helpers import sqlite_url


def test_staged_canonical_journal_seals_to_its_exact_tail() -> None:
    observed = BoundedSourceObserver().observe(BytesSource(b"canonical content"))
    raw = create_journal(
        observed.graph_fragment(),
        recorded_by_agent_id=observed.observer_agent_id,
    )
    expected = validate_journal(raw)
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
                idempotency_key="canonical-journal",
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
        session.flush()
        record = CollectionUploadProvenanceJournalRecord(
            collection_id=1,
            journal_id=expected.journal_id,
            bytes=len(raw),
            sha256=hashlib.sha256(raw).hexdigest(),
            state="validating",
            accepted_bytes=len(raw),
            content_hash_state="{}",
        )
        session.add(record)
        session.flush()
        split = len(raw) // 2
        for ordinal, offset, content in ((0, 0, raw[:split]), (1, split, raw[split:])):
            session.add(
                CollectionUploadProvenanceJournalChunkRecord(
                    collection_id=1,
                    journal_id=expected.journal_id,
                    ordinal=ordinal,
                    byte_offset=offset,
                    content=content,
                )
            )
        session.flush()
        _validate_next_upload_journal_entry(session, record)
        assert record.state == "sealed"
        assert _journal_payload(record)["anchor"] == expected.anchor


def test_completion_requirement_and_journal_selection_are_immutable(tmp_path: Path) -> None:
    config = RuntimeConfig.for_testing(database_url=sqlite_url(tmp_path / "catalog.db"))
    factory = make_session_factory(config.database_url)
    Base.metadata.create_all(factory.kw["bind"])
    incarnation_id = str(uuid.uuid4())
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
                idempotency_key="operation-journal",
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
    service = object.__new__(SqlAlchemyCollectionUploadService)
    service._session_factory = factory
    service._config = config
    first = "urn:uuid:11111111-1111-4111-8111-111111111111"
    second = "urn:uuid:22222222-2222-4222-8222-222222222222"
    authority = CollectionUploadProvenanceJournalCreateDocument(
        bytes="10", sha256="b" * 64, selection_role="completion"
    )
    with pytest.raises(Conflict, match="no accepted construction requirement"):
        service.create_provenance_journal(1, first, authority)
    requirement = CollectionCompletionRequirementDocument(
        execution_id="a" * 64,
        execution_envelope_sha256="b" * 64,
        controller_evidence_sha256="c" * 64,
        record_kinds=COMPLETION_REQUIRED_RECORD_KINDS,
    )
    assert service.set_completion_requirement(1, requirement) == requirement
    assert service.set_completion_requirement(1, requirement) == requirement
    request = CollectionCompletionRecordingRequestDocument(
        requirement_sha256=requirement.identity, records_sha256="c" * 64
    )
    recording = service.reserve_completion_recording(1, request)
    assert service.reserve_completion_recording(1, request) == recording
    with pytest.raises(Conflict, match="preimages cannot be replaced"):
        service.reserve_completion_recording(
            1, request.model_copy(update={"records_sha256": "d" * 64})
        )
    first = recording.journal_id
    with pytest.raises(Conflict, match="cannot be replaced"):
        service.set_completion_requirement(
            1, requirement.model_copy(update={"execution_id": "d" * 64})
        )
    assert service.create_provenance_journal(1, first, authority)["journal_id"] == first
    assert service.create_provenance_journal(1, first, authority)["journal_id"] == first
    with session_scope(factory) as session:
        upload = session.get(CollectionUploadRecord, 1)
        assert upload is not None and upload.completion_journal_id == first
    with pytest.raises(Conflict, match="already selected"):
        service.create_provenance_journal(1, second, authority)
    with pytest.raises(Conflict, match="retain its root role"):
        service.create_provenance_journal(
            1,
            first,
            CollectionUploadProvenanceJournalCreateDocument(bytes="10", sha256="b" * 64),
        )
