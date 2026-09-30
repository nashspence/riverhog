from __future__ import annotations

import hashlib
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest
from riverhog_age import decrypt_age_scrypt
from riverhog_archive_contracts import (
    MEMBER_HISTORY_IMPORTS_SCHEMA,
    ProvenancePayload,
    ProvenanceVolumeDocument,
    RecordPage,
    RecordSetCommitment,
)
from riverhog_client.canonical_production import ProducerAttribution, build_member_journal
from riverhog_core.archive_provenance import ArchiveProvenancePublisher
from riverhog_core.catalog_base import Base
from riverhog_core.catalog_db import make_session_factory, session_scope
from riverhog_core.catalog_models import (
    CollectionArchiveObjectUploadRecord,
    CollectionUploadArtifactProvenanceBindingRecord,
    CollectionUploadArtifactRecord,
    CollectionUploadArtifactVolumeRecord,
    CollectionUploadProvenanceJournalChunkRecord,
    CollectionUploadProvenanceJournalRecord,
    CollectionUploadRecord,
    StorageIncarnationRecord,
)
from riverhog_core.checkpoint_sha256 import CheckpointSHA256
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services.collection_uploads import (
    SqlAlchemyCollectionUploadService,
    _artifact_payload,
    _custody_stats,
    _record_payload_custody_progress,
)
from riverhog_protocol import (
    ArtifactId,
    ArtifactMemberIdentityDocument,
    CollectionUploadArtifactCustodyReceiptDocument,
)
from riverhog_protocol.collection_completion import (
    COMPLETION_REQUIRED_RECORD_KINDS,
    CollectionCompletionRequirementDocument,
)
from riverhog_protocol.errors import Conflict
from riverhog_provenance import BoundedSourceObserver, BytesSource, validate_journal
from sqlalchemy import select

from tests.unit.db_helpers import sqlite_url
from tests.unit.test_archive_root import MemoryImmutableStore


def _construction(
    tmp_path: Path, requirement: CollectionCompletionRequirementDocument | None = None
) -> SimpleNamespace:
    config = RuntimeConfig.for_testing(
        database_url=sqlite_url(tmp_path / "custody.db"), archive_scrypt_work_factor=1
    )
    factory = make_session_factory(config.database_url)
    engine = factory.kw["bind"]
    Base.metadata.create_all(engine)
    incarnation_id = str(uuid.uuid4())
    member_id = "a" * 64
    context = f"urn:uuid:{uuid.uuid4()}"
    # The registered fixity is the measured payload identity.
    member_sha256 = hashlib.sha256(b"one").hexdigest()
    primary = build_member_journal(
        member=ArtifactMemberIdentityDocument.model_validate(
            {"artifact_id": member_id, "bytes": "3", "sha256": member_sha256}
        ),
        observation=BoundedSourceObserver().observe(BytesSource(b"one")),
        delivery_context_id=context,
        attribution=ProducerAttribution(
            "fixture", "fixture/v1", "1", "event", "fixture", {}, "1" * 64, requirement
        ),
        materialization_hint=None,
        output_id=None if requirement is None else "output:one",
    )
    summary = validate_journal(primary.content, require_profiles=False)
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
                passphrase_id=config.archive_active_passphrase_id,
                archive_store="archive",
                archive_incarnation_id=incarnation_id,
                opened_at="2026-01-01T00:00:00Z",
                last_activity_at="2026-01-01T00:00:00Z",
                archive_phase_updated_at="2026-01-01T00:00:00Z",
                archive_storage_prefix="collection/1",
                planner_checkpoint_json="{}",
                state="open",
                artifact_count=1,
                artifact_bytes=3,
                delivery_context_id=context,
                completion_requirement_json=None
                if requirement is None
                else requirement.model_dump_json(),
            )
        )
        session.add(
            CollectionUploadArtifactRecord(
                collection_id=1,
                artifact_id=member_id,
                artifact_order=0,
                bytes=3,
                sha256=member_sha256,
            )
        )
        session.flush()
        session.add(
            CollectionUploadProvenanceJournalRecord(
                collection_id=1,
                journal_id=summary.journal_id,
                bytes=len(primary.content),
                sha256=hashlib.sha256(primary.content).hexdigest(),
                state="sealed",
                accepted_bytes=len(primary.content),
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
                content=primary.content,
            )
        )
        binding = primary.binding
        session.add(
            CollectionUploadArtifactProvenanceBindingRecord(
                collection_id=1,
                artifact_id=member_id,
                journal_id=binding.journal.journal_id,
                through_entry_id=binding.journal.through.entry_id,
                through_sequence=binding.journal.through.sequence,
                through_json_sha256=binding.journal.through.json_sha256,
                prefix_sha256=binding.journal.prefix_sha256,
                prefix_bytes=binding.journal.prefix_bytes,
                delivery_association_id=binding.delivery_association_id,
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
    service._config = config
    store = MemoryImmutableStore()
    service._archive_stores = SimpleNamespace(
        require=lambda _name: SimpleNamespace(immutable_objects=store)
    )
    return SimpleNamespace(
        service=service,
        factory=factory,
        config=config,
        store=store,
        primary=primary,
        member_id=member_id,
    )


def test_source_custody_precedes_final_roots_and_reuses_encrypted_primary(tmp_path: Path) -> None:
    f = _construction(tmp_path)
    service, factory, member_id = f.service, f.factory, f.member_id
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
    assert service.process_due_custody_receipts() == 1  # encrypted primary bytes
    assert service.process_due_custody_receipts() == 1  # early receipt
    assert service.process_due_custody_receipts() == 0
    with session_scope(factory) as session:
        row = session.scalar(select(CollectionUploadArtifactRecord))
        upload = session.get(CollectionUploadRecord, 1)
        assert upload is not None and upload.final_authority_json is None
        assert row is not None and row.custody_receipt_json is not None
        receipt = CollectionUploadArtifactCustodyReceiptDocument.model_validate_json(
            row.custody_receipt_json
        )
        assert receipt.primary == f.primary.binding
        assert receipt.completion_requirement_sha256 is None
        assert receipt.archive_object_count == 2 and receipt.provenance_object_count == 1
        assert _custody_stats(session, 1) == (1, 3)
    path, stored = next(iter(f.store.objects.items()))
    ciphertext = stored.content
    passphrase = f.config.archive_passphrase_for(f.config.archive_active_passphrase_id)
    assert decrypt_age_scrypt(ciphertext, passphrase) == f.primary.content
    document = ProvenanceVolumeDocument(
        archive_generation="3" * 64,
        artifact_set_sha256="4" * 64,
        sequence=0,
        payload=ProvenancePayload(
            "journal", 0, len(f.primary.content), hashlib.sha256(f.primary.content).hexdigest()
        ),
        journal_id=f.primary.journal_id,
        journal_offset=0,
        journal_bytes=len(f.primary.content),
        journal_sha256=hashlib.sha256(f.primary.content).hexdigest(),
    )
    final = ArchiveProvenancePublisher(
        object_store=f.store, passphrase=passphrase, scrypt_log_n=1
    ).publish_volume(
        archive_storage_prefix="collection/1", document=document, payload=f.primary.content
    )
    assert path == "collection/1/" + final.payload.relative_path
    assert f.store.objects[path].content == ciphertext
    with pytest.raises(Conflict, match="acknowledged early custody"):
        service.set_member_history_inputs(
            1, ArtifactId(member_id), RecordSetCommitment(MEMBER_HISTORY_IMPORTS_SCHEMA).ref()
        )


def _seal_payload(f: SimpleNamespace) -> None:
    with session_scope(f.factory) as session:
        upload = session.get(CollectionUploadRecord, 1)
        assert upload is not None
        for volume in session.scalars(select(CollectionArchiveObjectUploadRecord)):
            volume.state = "sealed"
            volume.sealed_receipt_json = '{"sealed":true}'
        session.flush()
        for volume in session.scalars(select(CollectionArchiveObjectUploadRecord)):
            _record_payload_custody_progress(session, upload, volume, now="2026-01-01T00:01:00Z")


def test_execution_early_custody_requires_input_selection_and_keeps_completion_pending(
    tmp_path: Path,
) -> None:
    requirement = CollectionCompletionRequirementDocument(
        execution_id="a" * 64,
        execution_envelope_sha256="b" * 64,
        controller_evidence_sha256="c" * 64,
        record_kinds=COMPLETION_REQUIRED_RECORD_KINDS,
    )
    f = _construction(tmp_path, requirement)
    _seal_payload(f)
    assert not f.service._advance_artifact_custody(1, f.member_id)
    inputs = RecordSetCommitment(MEMBER_HISTORY_IMPORTS_SCHEMA).ref()
    f.service.stage_history_structure(1, RecordPage(inputs, 0, (), True).to_json_bytes())
    f.service.set_member_history_inputs(1, ArtifactId(f.member_id), inputs)
    assert f.service._advance_artifact_custody(1, f.member_id)
    assert f.service._advance_artifact_custody(1, f.member_id)
    assert f.service._advance_artifact_custody(1, f.member_id)
    with session_scope(f.factory) as session:
        row = session.get(CollectionUploadArtifactRecord, (1, f.member_id))
        assert row is not None and row.custody_receipt_json is not None
        receipt = CollectionUploadArtifactCustodyReceiptDocument.model_validate_json(
            row.custody_receipt_json
        )
        assert receipt.completion_requirement_sha256 == requirement.identity
        assert _custody_stats(session, 1) == (1, 3)
    with pytest.raises(Conflict, match="completion"):
        f.service._stage_next_member_histories(1)


def test_lost_custody_store_response_reuses_durable_bytes(tmp_path: Path) -> None:
    f = _construction(tmp_path)
    _seal_payload(f)

    class LostResponseStore(MemoryImmutableStore):
        lost = False

        def put_immutable_object(self, **kwargs):
            receipt = super().put_immutable_object(**kwargs)
            if not self.lost:
                self.lost = True
                raise OSError("lost durable-store response")
            return receipt

    store = LostResponseStore()
    f.service._archive_stores = SimpleNamespace(
        require=lambda _name: SimpleNamespace(immutable_objects=store)
    )
    with pytest.raises(OSError, match="lost durable-store"):
        f.service._advance_artifact_custody(1, f.member_id)
    ciphertexts = {path: value.content for path, value in store.objects.items()}
    assert f.service._advance_artifact_custody(1, f.member_id)
    assert f.service._advance_artifact_custody(1, f.member_id)
    assert {path: value.content for path, value in store.objects.items()} == ciphertexts
    with session_scope(f.factory) as session:
        assert _custody_stats(session, 1) == (1, 3)
