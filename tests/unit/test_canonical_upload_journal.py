from __future__ import annotations

import hashlib
import uuid

from riverhog_core.catalog_db import Base, create_catalog_engine
from riverhog_core.catalog_models import (
    CollectionUploadProvenanceJournalChunkRecord,
    CollectionUploadProvenanceJournalRecord,
    CollectionUploadRecord,
    StorageIncarnationRecord,
)
from riverhog_core.services.collection_uploads import (
    _journal_payload,
    _validate_next_upload_journal_entry,
)
from riverhog_protocol import collection_tag_set_identity
from riverhog_provenance import BoundedSourceObserver, BytesSource, create_journal, validate_journal
from sqlalchemy.orm import Session


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
