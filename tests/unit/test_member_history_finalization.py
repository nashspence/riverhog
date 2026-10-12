from __future__ import annotations

import hashlib
import json
import uuid
from pathlib import Path
from types import SimpleNamespace

from riverhog_age import decrypt_age_scrypt
from riverhog_archive_contracts import MemberHistoryBinding, MemberHistoryRoot, RecordPage
from riverhog_client.canonical_production import ProducerAttribution, build_member_journal
from riverhog_core.catalog_db import Base, make_session_factory, session_scope
from riverhog_core.catalog_models import (
    CollectionUploadArtifactProvenanceBindingRecord,
    CollectionUploadArtifactRecord,
    CollectionUploadMemberHistoryRecord,
    CollectionUploadProvenanceJournalChunkRecord,
    CollectionUploadProvenanceJournalRecord,
    CollectionUploadProvenanceStructureRecord,
    CollectionUploadRecord,
    StorageIncarnationRecord,
)
from riverhog_core.checkpoint_sha256 import CheckpointSHA256
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services.collection_uploads import SqlAlchemyCollectionUploadService
from riverhog_protocol import ArtifactMemberIdentityDocument, collection_tag_set_identity
from riverhog_provenance import BoundedSourceObserver, BytesSource, create_journal, validate_journal
from sqlalchemy import select

from tests.unit.db_helpers import sqlite_url
from tests.unit.test_archive_root import MemoryImmutableStore


def test_final_history_and_encrypted_structure_resume_without_changing_early_primary(
    tmp_path: Path,
) -> None:
    config = RuntimeConfig.for_testing(
        database_url=sqlite_url(tmp_path / "catalog.db"),
        archive_scrypt_work_factor=1,
    )
    factory = make_session_factory(config.database_url)
    Base.metadata.create_all(factory.kw["bind"])
    store = MemoryImmutableStore()
    service = object.__new__(SqlAlchemyCollectionUploadService)
    service._session_factory = factory
    service._config = config
    service._archive_stores = SimpleNamespace(
        require=lambda _name: SimpleNamespace(immutable_objects=store)
    )
    member = ArtifactMemberIdentityDocument.model_validate(
        {
            "artifact_id": "c" * 64,
            "bytes": "7",
            "sha256": hashlib.sha256(b"payload").hexdigest(),
        }
    )
    delivery_context_id = f"urn:uuid:{uuid.uuid4()}"
    observation = BoundedSourceObserver().observe(BytesSource(b"payload"))
    produced = build_member_journal(
        member=member,
        observation=observation,
        delivery_context_id=delivery_context_id,
        attribution=ProducerAttribution(
            "fixture", "fixture/v1", "1", "event", "fixture", {}, "a" * 64
        ),
        materialization_hint=None,
    )
    late = create_journal(
        observation.graph_fragment(), recorded_by_agent_id=observation.observer_agent_id
    )
    late_summary = validate_journal(late)
    incarnation = str(uuid.uuid4())
    with session_scope(factory) as session:
        session.add(
            StorageIncarnationRecord(
                id=incarnation,
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
                idempotency_key="history-finalization",
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
                delivery_context_id=delivery_context_id,
                artifact_count=1,
                artifact_bytes=7,
                completion_journal_id=late_summary.journal_id,
                state="finalizing",
                archive_phase="finalizing",
            )
        )
        session.flush()
        session.add(
            CollectionUploadArtifactRecord(
                collection_id=1,
                artifact_id=member.artifact_id,
                artifact_order=0,
                bytes=member.bytes,
                sha256=member.sha256,
            )
        )
        for raw in (produced.content, late):
            summary = validate_journal(raw, require_profiles=False)
            session.add(
                CollectionUploadProvenanceJournalRecord(
                    collection_id=1,
                    journal_id=summary.journal_id,
                    bytes=len(raw),
                    sha256=hashlib.sha256(raw).hexdigest(),
                    state="sealed",
                    accepted_bytes=len(raw),
                    next_chunk_ordinal=1,
                    content_hash_state=CheckpointSHA256().export_state(),
                    terminal_entry_id=summary.anchor["through"]["entry_id"],
                    terminal_sequence=int(summary.anchor["through"]["sequence"]),
                    terminal_json_sha256=summary.anchor["through"]["json_sha256"],
                )
            )
            session.flush()
            session.add(
                CollectionUploadProvenanceJournalChunkRecord(
                    collection_id=1,
                    journal_id=summary.journal_id,
                    ordinal=0,
                    byte_offset=0,
                    content=raw,
                )
            )
        primary = produced.binding
        session.add(
            CollectionUploadArtifactProvenanceBindingRecord(
                collection_id=1,
                artifact_id=member.artifact_id,
                journal_id=primary.journal.journal_id,
                through_entry_id=primary.journal.through.entry_id,
                through_sequence=primary.journal.through.sequence,
                through_json_sha256=primary.journal.through.json_sha256,
                prefix_sha256=primary.journal.prefix_sha256,
                prefix_bytes=primary.journal.prefix_bytes,
                delivery_association_id=primary.delivery_association_id,
            )
        )
    assert service._stage_next_member_histories(1)
    assert service._stage_next_member_histories(1)
    assert not service._stage_next_member_histories(1)
    with session_scope(factory) as session:
        final = session.get(CollectionUploadMemberHistoryRecord, (1, member.artifact_id))
        assert final is not None
        structures = list(session.scalars(select(CollectionUploadProvenanceStructureRecord)))
        histories = [row for row in structures if row.kind == "history"]
        assert len(histories) == 1
        binding = MemberHistoryBinding.from_mapping(json.loads(final.binding_json))
        history = binding.verify_descriptor(histories[0].content)
        assert history.primary.journal.to_mapping() == primary.journal.model_dump(mode="json")
        root_pages = [
            RecordPage.from_json_bytes(row.content)
            for row in structures
            if row.kind == "record-page"
            and RecordPage.from_json_bytes(row.content).authority == history.roots
        ]
        roots = [
            MemberHistoryRoot.from_mapping(row["value"])
            for page in root_pages
            for row in page.records
        ]
        assert {root.journal.journal_id for root in roots} == {
            produced.journal_id,
            late_summary.journal_id,
        }
        upload = session.get(CollectionUploadRecord, 1)
        assert upload is not None
        upload.provenance_closure_validated = True
        staged = {row.object_id: row.content for row in structures}
    while service._publish_next_provenance_structure(1):
        pass
    assert len(store.objects) == len(staged)
    with session_scope(factory) as session:
        for row in session.scalars(select(CollectionUploadProvenanceStructureRecord)):
            assert row.receipt_json is not None
            assert row.content == staged[row.object_id]
            encrypted = store.objects["collections/1/" + row.relative_path].content
            assert (
                decrypt_age_scrypt(
                    encrypted, config.archive_passphrase_for(config.archive_active_passphrase_id)
                )
                == row.content
            )
        early = session.get(
            CollectionUploadProvenanceJournalChunkRecord, (1, produced.journal_id, 0)
        )
        assert early is not None and early.content == produced.content
