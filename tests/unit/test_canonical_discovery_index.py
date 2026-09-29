from __future__ import annotations

import hashlib

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
from riverhog_core.catalog_models import CollectionRecord
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexAssertionRecord,
    CollectionProvenanceIndexStateRecord,
    CollectionProvenanceIndexTextChunkRecord,
    CollectionProvenanceIndexValueRecord,
)
from riverhog_protocol import ArtifactMemberIdentityDocument
from riverhog_protocol.collection_production_provenance import collection_production_contract
from riverhog_provenance import BoundedSourceObserver, BytesSource, create_journal, validate_journal
from riverhog_provenance_contracts import ContractCatalog
from sqlalchemy import func, select
from sqlalchemy.orm import Session


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
    relevance = member_relevance(
        member=member,
        binding=produced.binding,
        primary=summary,
        corpus={summary.journal_id: summary},
        delivery_context_id=collection.delivery_context_id,
        catalog=catalog,
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
