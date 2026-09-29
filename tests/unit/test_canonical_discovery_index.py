from __future__ import annotations

from riverhog_core.canonical_discovery_index import (
    StaleIndexBuild,
    begin_index_build,
    stage_assertion_page,
    stage_entry_page,
    stage_snapshot_header,
)
from riverhog_core.canonical_discovery_rows import iter_index_assertions
from riverhog_core.catalog_db import Base, create_catalog_engine
from riverhog_core.catalog_models import CollectionRecord
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexAssertionRecord,
    CollectionProvenanceIndexTextChunkRecord,
    CollectionProvenanceIndexValueRecord,
)
from riverhog_provenance import BoundedSourceObserver, BytesSource, create_journal, validate_journal
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
