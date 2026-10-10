from __future__ import annotations

import hashlib
import uuid

from riverhog_archive_contracts import provenance_structure_identity
from riverhog_canonical_json import canonical_json_bytes
from riverhog_client.canonical_production import ProducerAttribution, build_member_journal
from riverhog_core.canonical_discovery_index import (
    StaleIndexBuild,
    begin_index_build,
    complete_index_build,
    publish_index_build,
    stage_assertion_page,
    stage_entry_page,
    stage_member,
    stage_membership_page,
    stage_snapshot_header,
)
from riverhog_core.canonical_discovery_relevance import member_relevance, relevance_row_keys
from riverhog_core.canonical_discovery_rows import iter_index_assertions
from riverhog_core.catalog_db import Base, create_catalog_engine
from riverhog_core.catalog_models import (
    CollectionRecord,
    CollectionUploadArtifactProvenanceBindingRecord,
    CollectionUploadArtifactRecord,
    CollectionUploadMemberHistoryRecord,
    CollectionUploadProvenanceJournalChunkRecord,
    CollectionUploadProvenanceJournalRecord,
    CollectionUploadProvenanceStructureRecord,
    CollectionUploadRecord,
    StorageIncarnationRecord,
)
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexAssertionRecord,
    CollectionProvenanceIndexMembershipRecord,
    CollectionProvenanceIndexStateRecord,
    CollectionProvenanceIndexTextChunkRecord,
    CollectionProvenanceIndexValueRecord,
)
from riverhog_core.services.collection_uploads import _advance_catalog_canonical_index
from riverhog_protocol import ArtifactMemberIdentityDocument, collection_tag_set_identity
from riverhog_protocol.collection_production_provenance import collection_production_contract
from riverhog_provenance import BoundedSourceObserver, BytesSource, create_journal, validate_journal
from riverhog_provenance_contracts import ContractCatalog
from sqlalchemy import event, func, select
from sqlalchemy.orm import Session

from tests.support.member_history import member_history_selection_fixture


def _collection() -> CollectionRecord:
    return CollectionRecord(
        id=1,
        creation_idempotency_key="index-test",
        creation_identity_sha256="1" * 64,
        creation_custody_mode="custody-transfer",
        delivery_context_id="urn:uuid:cabfc827-91d5-4cde-8f0f-81255b75b1c4",
        artifact_set_identity="2" * 64,
        encryption_format="none",
        passphrase_id="test",
        provenance_identity="3" * 64,
        inventory_identity="4" * 64,
        archive_root_sha256="5" * 64,
        created_at="2026-01-01T00:00:00Z",
    )


def test_bounded_index_staging_retries_and_fences_late_workers() -> None:
    engine = create_catalog_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    observation = BoundedSourceObserver().observe(BytesSource(b"needle in opaque content"))
    raw = create_journal(
        observation.graph_fragment(), recorded_by_agent_id=observation.observer_agent_id
    )
    summary = validate_journal(raw)
    rows = tuple(iter_index_assertions(summary))
    with Session(engine) as session:
        session.add(_collection())
        session.commit()
        build_id = begin_index_build(session, collection_id=1)
        session.commit()
        stage_snapshot_header(session, build_id=build_id, summary=summary)
        session.commit()
        assert stage_entry_page(session, build_id=build_id, summary=summary, start=0) == 1
        session.commit()
        stage_assertion_page(session, build_id=build_id, rows=rows)
        session.commit()
        stage_assertion_page(session, build_id=build_id, rows=rows)
        session.commit()
        assert session.scalar(
            select(func.count()).select_from(CollectionProvenanceIndexAssertionRecord)
        ) == len(rows)
        assert session.scalar(
            select(func.count()).select_from(CollectionProvenanceIndexValueRecord)
        ) >= len(rows)
        assert (
            session.scalar(
                select(func.count()).select_from(CollectionProvenanceIndexTextChunkRecord)
            )
            > 0
        )
        begin_index_build(session, collection_id=1)
        session.commit()
        import pytest

        with pytest.raises(StaleIndexBuild, match="fence"):
            stage_assertion_page(session, build_id=build_id, rows=rows)
    engine.dispose()


def test_complete_generation_requires_every_snapshot_row_and_has_content_identity() -> None:
    import pytest

    engine = create_catalog_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    payload = b"opaque member with useful native facts"
    member = ArtifactMemberIdentityDocument(
        artifact_id="ab" * 32, bytes=str(len(payload)), sha256=hashlib.sha256(payload).hexdigest()
    )
    collection = _collection()
    produced = build_member_journal(
        member=member,
        observation=BoundedSourceObserver().observe(BytesSource(payload)),
        delivery_context_id=collection.delivery_context_id,
        attribution=ProducerAttribution("example", "bytes", "v1", "event", "test", {}, "a1" * 32),
        materialization_hint=("source.bin",),
    )
    catalog = ContractCatalog((collection_production_contract(),))
    summary = validate_journal(produced.content, catalog=catalog)
    rows = tuple(iter_index_assertions(summary))
    history_binding, history_closure, _ = member_history_selection_fixture(
        member, produced.binding, {summary.journal_id: summary}
    )
    with history_closure:
        relevance = member_relevance(
            member=member,
            binding=produced.binding,
            primary=summary,
            corpus={summary.journal_id: summary},
            delivery_context_id=collection.delivery_context_id,
            catalog=catalog,
            history_binding=history_binding,
            closure=history_closure,
        )

    memberships = tuple(
        (member.artifact_id, row_key, scope) for row_key, scope in relevance_row_keys(relevance)
    )
    with Session(engine) as session:
        session.add(collection)
        session.commit()
        generation_ids = []
        for _ in range(2):
            build_id = begin_index_build(session, collection_id=collection.id)
            session.commit()
            stage_snapshot_header(session, build_id=build_id, summary=summary)
            session.commit()
            with pytest.raises(StaleIndexBuild, match="incomplete"):
                complete_index_build(
                    session,
                    build_id=build_id,
                    expected_snapshots=1,
                    expected_members=1,
                    expected_memberships=len(memberships),
                )
            stage_entry_page(session, build_id=build_id, summary=summary, start=0)
            for offset in range(0, len(rows), 32):
                stage_assertion_page(session, build_id=build_id, rows=rows[offset : offset + 32])
            stage_member(
                session,
                build_id=build_id,
                artifact_id=member.artifact_id,
                bytes=int(member.bytes),
                sha256=member.sha256,
                journal_id=summary.journal_id,
                prefix_sha256=summary.journal_sha256,
                delivery_association_id=produced.binding.delivery_association_id,
            )
            session.flush()
            stage_membership_page(session, build_id=build_id, rows=memberships)
            session.commit()
            generation_id = complete_index_build(
                session,
                build_id=build_id,
                expected_snapshots=1,
                expected_members=1,
                expected_memberships=len(memberships),
            )
            assert publish_index_build(session, build_id=build_id) == generation_id
            session.commit()
            generation_ids.append(generation_id)
        assert generation_ids[0] == generation_ids[1]
        state = session.get(CollectionProvenanceIndexStateRecord, collection.id)
        assert state is not None and state.phase == "ready" and state.active_build_id == build_id
    engine.dispose()


def test_initial_catalog_publication_waits_for_exact_canonical_index() -> None:
    engine = create_catalog_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    payload = b"indexed opaque member"
    member = ArtifactMemberIdentityDocument(
        artifact_id="ab" * 32, bytes=str(len(payload)), sha256=hashlib.sha256(payload).hexdigest()
    )
    collection = _collection()
    collection.is_published = False
    produced = build_member_journal(
        member=member,
        observation=BoundedSourceObserver().observe(BytesSource(payload)),
        delivery_context_id=collection.delivery_context_id,
        attribution=ProducerAttribution("example", "bytes", "v1", "event", "test", {}, "a1" * 32),
        materialization_hint=None,
    )
    anchor = produced.binding.journal
    with Session(engine) as session:
        session.add(collection)
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
        upload = CollectionUploadRecord(
            collection_id=collection.id,
            idempotency_key="indexed-upload",
            creation_identity_sha256="a" * 64,
            initial_tag_set_identity=collection_tag_set_identity(None),
            archive_generation=collection.archive_generation,
            encryption_format="age-v1-scrypt",
            passphrase_id="test-key",
            archive_store="archive",
            archive_incarnation_id=incarnation_id,
            opened_at="2026-01-01T00:00:00Z",
            last_activity_at="2026-01-01T00:00:00Z",
            archive_phase_updated_at="2026-01-01T00:00:00Z",
            archive_storage_prefix="collections/1",
            delivery_context_id=collection.delivery_context_id,
            planner_checkpoint_json="{}",
            catalog_phase="index",
            artifact_count=1,
        )
        session.add(upload)
        session.flush()
        session.add(
            CollectionUploadArtifactRecord(
                collection_id=1,
                artifact_id=member.artifact_id,
                artifact_order=0,
                bytes=int(member.bytes),
                sha256=member.sha256,
            )
        )
        session.add(
            CollectionUploadProvenanceJournalRecord(
                collection_id=1,
                journal_id=produced.journal_id,
                bytes=len(produced.content),
                sha256=hashlib.sha256(produced.content).hexdigest(),
                state="sealed",
                accepted_bytes=len(produced.content),
                content_hash_state="{}",
            )
        )
        session.flush()
        session.add(
            CollectionUploadProvenanceJournalChunkRecord(
                collection_id=1,
                journal_id=produced.journal_id,
                ordinal=0,
                byte_offset=0,
                content=produced.content,
            )
        )
        session.add(
            CollectionUploadArtifactProvenanceBindingRecord(
                collection_id=1,
                artifact_id=member.artifact_id,
                journal_id=anchor.journal_id,
                through_entry_id=anchor.through.entry_id,
                through_sequence=int(anchor.through.sequence),
                through_json_sha256=anchor.through.json_sha256,
                prefix_sha256=anchor.prefix_sha256,
                prefix_bytes=int(anchor.prefix_bytes),
                delivery_association_id=produced.binding.delivery_association_id,
            )
        )
        selected, history_closure, structures = member_history_selection_fixture(
            member,
            produced.binding,
            {
                produced.journal_id: validate_journal(
                    produced.content, catalog=ContractCatalog((collection_production_contract(),))
                )
            },
        )
        with history_closure:
            pass
        for path, content in structures.items():
            identity = provenance_structure_identity(content)
            session.add(
                CollectionUploadProvenanceStructureRecord(
                    collection_id=1,
                    object_id=identity.object_id,
                    kind=identity.kind,
                    relative_path=path,
                    content=content,
                )
            )
        session.add(
            CollectionUploadMemberHistoryRecord(
                collection_id=1,
                artifact_id=member.artifact_id,
                history_sha256=selected.history_sha256,
                history_bytes=selected.history_bytes,
                binding_json=canonical_json_bytes(selected.to_mapping()).decode(),
            )
        )
        session.commit()
        for _ in range(3):
            _advance_catalog_canonical_index(session, upload)
            session.commit()
        assert upload.catalog_phase == "terminal"
        assert collection.is_published is False
        state = session.get(CollectionProvenanceIndexStateRecord, collection.id)
        assert state is not None and state.phase == "ready"
        assert state.active_build_id is not None
        assert (
            session.scalar(
                select(func.count()).select_from(CollectionProvenanceIndexMembershipRecord)
            )
            > 0
        )
    engine.dispose()


def test_membership_pages_bound_database_work_and_keep_exact_references_and_fences() -> None:
    import pytest

    engine = create_catalog_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    payload = b"native facts with repeated recorded support"
    observation = BoundedSourceObserver().observe(BytesSource(payload))
    summary = validate_journal(
        create_journal(
            observation.graph_fragment(), recorded_by_agent_id=observation.observer_agent_id
        )
    )
    assertions = tuple(iter_index_assertions(summary))
    artifact_ids = [
        hashlib.sha256(f"member-{ordinal}".encode()).hexdigest() for ordinal in range(128)
    ]
    with Session(engine) as session:
        session.add(_collection())
        session.commit()
        build_id = begin_index_build(session, collection_id=1)
        stage_snapshot_header(session, build_id=build_id, summary=summary)
        session.flush()
        stage_entry_page(session, build_id=build_id, summary=summary, start=0)
        stage_assertion_page(session, build_id=build_id, rows=assertions)
        for artifact_id in artifact_ids:
            stage_member(
                session,
                build_id=build_id,
                artifact_id=artifact_id,
                bytes=len(payload),
                sha256=hashlib.sha256(payload).hexdigest(),
                journal_id=summary.journal_id,
                prefix_sha256=summary.journal_sha256,
                delivery_association_id="urn:uuid:" + str(uuid.uuid4()),
            )
        session.commit()
        rows = [
            (artifact_id, row.row_key, scope)
            for artifact_id in artifact_ids
            for row in assertions[:2]
            for scope in ("member", "recorded-history")
        ]
        assert len(rows) == 512
        statements = []

        def record_statement(_connection, _cursor, statement, _parameters, _context, _executemany):
            statements.append(statement)

        event.listen(engine, "before_cursor_execute", record_statement)
        try:
            stage_membership_page(session, build_id=build_id, rows=rows)
            session.commit()
            assert len(statements) < 50
            statements.clear()
            stage_membership_page(session, build_id=build_id, rows=rows)
            session.commit()
            assert len(statements) < 50
        finally:
            event.remove(engine, "before_cursor_execute", record_statement)
        actual = set(
            session.execute(
                select(
                    CollectionProvenanceIndexMembershipRecord.artifact_id,
                    CollectionProvenanceIndexMembershipRecord.row_key,
                    CollectionProvenanceIndexMembershipRecord.scope,
                ).where(CollectionProvenanceIndexMembershipRecord.build_id == build_id)
            ).tuples()
        )
        assert actual == set(rows)
        with pytest.raises(StaleIndexBuild, match="member"):
            stage_membership_page(
                session, build_id=build_id, rows=[("f" * 64, assertions[0].row_key, "member")]
            )
        session.rollback()
        with pytest.raises(StaleIndexBuild, match="assertion"):
            stage_membership_page(
                session, build_id=build_id, rows=[(artifact_ids[0], "f" * 64, "member")]
            )
        session.rollback()
        begin_index_build(session, collection_id=1)
        session.commit()
        with pytest.raises(StaleIndexBuild, match="fence"):
            stage_membership_page(session, build_id=build_id, rows=rows)
    engine.dispose()
