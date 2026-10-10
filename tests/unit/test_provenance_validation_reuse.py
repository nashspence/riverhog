"""Pure journal reuse keeps exact bytes, validation context and failed checks separate."""

import hashlib
import uuid
from dataclasses import replace

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_core.catalog_db import Base, make_session_factory, session_scope
from riverhog_core.catalog_models import (
    CollectionUploadProvenanceJournalChunkRecord,
    CollectionUploadProvenanceJournalRecord,
    CollectionUploadRecord,
)
from riverhog_core.provenance_read_cache import ProvenanceReadCache
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services import collection_uploads
from riverhog_core.services.collection_uploads import SqlAlchemyCollectionUploadService
from riverhog_protocol import collection_tag_set_identity
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    ProvenanceValidationError,
    create_journal,
    validate_journal,
)
from riverhog_provenance_contracts import ContractCatalog

from tests.unit.db_helpers import sqlite_url
from tests.unit.storage_incarnation_fixtures import seed_storage_incarnation


def test_pure_validation_reuse_requires_exact_prefix_context_and_success(tmp_path, monkeypatch):
    config = RuntimeConfig.for_testing(database_url=sqlite_url(tmp_path / "catalog.db"))
    factory = make_session_factory(config.database_url)
    Base.metadata.create_all(factory.kw["bind"])
    observation = BoundedSourceObserver().observe(BytesSource(b"payload"))
    raw = create_journal(
        observation.graph_fragment(), recorded_by_agent_id=observation.observer_agent_id
    )
    summary = validate_journal(raw)
    anchor = HistoryJournalAnchor.from_mapping(summary.anchor)
    service = object.__new__(SqlAlchemyCollectionUploadService)
    service._closure_journal_validations = ProvenanceReadCache(byte_budget=1024, entry_budget=128)
    original_validate = collection_uploads.validate_journal_chunks
    original_catalog = collection_uploads.admission_provenance_catalog()
    different_catalog = ContractCatalog()
    calls = 0

    def validate(*args, **kwargs):
        nonlocal calls
        calls += 1
        if kwargs["catalog"] is different_catalog:
            raise ProvenanceValidationError("different validation context")
        return original_validate(*args, **kwargs)

    monkeypatch.setattr(collection_uploads, "validate_journal_chunks", validate)
    with session_scope(factory) as session:
        incarnation = seed_storage_incarnation(session, "archive", "archive")
        session.add(
            CollectionUploadRecord(
                collection_id=1,
                idempotency_key="pure-validation",
                creation_identity_sha256="a" * 64,
                initial_tag_set_identity=collection_tag_set_identity(None),
                encryption_format="age-v1-scrypt",
                passphrase_id=config.archive_active_passphrase_id,
                archive_store="archive",
                archive_incarnation_id=incarnation,
                opened_at="2026-01-01T00:00:00Z",
                last_activity_at="2026-01-01T00:00:00Z",
                archive_phase_updated_at="2026-01-01T00:00:00Z",
                archive_storage_prefix="collections/1",
                planner_checkpoint_json="{}",
                delivery_context_id="urn:uuid:" + str(uuid.uuid4()),
                state="finalizing",
                archive_phase="finalizing",
            )
        )
        session.flush()
        journal = CollectionUploadProvenanceJournalRecord(
            collection_id=1,
            journal_id=anchor.journal_id,
            bytes=len(raw),
            sha256=anchor.prefix_sha256,
            state="sealed",
            accepted_bytes=len(raw),
            next_chunk_ordinal=1,
            content_hash_state="{}",
            terminal_entry_id=anchor.through_entry_id,
            terminal_sequence=anchor.through_sequence,
            terminal_json_sha256=anchor.through_json_sha256,
        )
        session.add(journal)
        session.flush()
        chunk = CollectionUploadProvenanceJournalChunkRecord(
            collection_id=1, journal_id=anchor.journal_id, ordinal=0, byte_offset=0, content=raw
        )
        session.add(chunk)
        session.flush()
        service._validate_retained_journal_root(session, journal, anchor)
        service._validate_retained_journal_root(session, journal, anchor)
        assert calls == 1
        tampered = raw[:-1] + b"!"
        chunk.content = tampered
        session.flush()
        with pytest.raises(ProvenanceValidationError, match="prefix identity"):
            service._validate_retained_journal_root(session, journal, anchor)
        assert calls == 1
        changed = replace(anchor, prefix_sha256=hashlib.sha256(tampered).hexdigest())
        for _ in range(2):
            with pytest.raises(ProvenanceValidationError):
                service._validate_retained_journal_root(session, journal, changed)
        assert calls == 3
        chunk.content = raw
        session.flush()
        monkeypatch.setattr(
            collection_uploads, "admission_provenance_catalog", lambda: different_catalog
        )
        for _ in range(2):
            with pytest.raises(ProvenanceValidationError, match="different validation context"):
                service._validate_retained_journal_root(session, journal, anchor)
        assert calls == 5
        monkeypatch.setattr(
            collection_uploads, "admission_provenance_catalog", lambda: original_catalog
        )
        service._validate_retained_journal_root(session, journal, anchor)
        assert calls == 5
