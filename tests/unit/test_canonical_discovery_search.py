from __future__ import annotations

import hashlib

import pytest
from riverhog_api.schemas.search import DiscoveryPageOut
from riverhog_application_access import (
    ALL_RESOURCES,
    CATALOG_READ,
    PROVENANCE_READ,
    ApplicationAccess,
)
from riverhog_client.canonical_production import ProducerAttribution, build_member_journal
from riverhog_core.app_permissions import Principal
from riverhog_core.canonical_discovery_index import (
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
from riverhog_core.canonical_discovery_search import discover_artifacts
from riverhog_core.catalog_db import Base, create_catalog_engine
from riverhog_core.catalog_models import (
    CatalogSyncStateRecord,
    CollectionArtifactRecord,
    CollectionRecord,
)
from riverhog_core.catalog_provenance_index_models import CollectionProvenanceIndexStateRecord
from riverhog_protocol import ArtifactDiscoveryRequest, ArtifactMemberIdentityDocument
from riverhog_protocol.collection_production_provenance import collection_production_contract
from riverhog_protocol.errors import Conflict, ServiceUnavailable
from riverhog_provenance import BoundedSourceObserver, BytesSource, validate_journal
from riverhog_provenance_contracts import ContractCatalog
from sqlalchemy.orm import Session


def _indexed_collection(session: Session) -> str:
    payload = b"opaque bytes for an exact member"
    artifact_id = "ab" * 32
    member = ArtifactMemberIdentityDocument(
        artifact_id=artifact_id,
        bytes=str(len(payload)),
        sha256=hashlib.sha256(payload).hexdigest(),
    )
    collection = CollectionRecord(
        id=1,
        creation_idempotency_key="discovery-test",
        creation_identity_sha256="1" * 64,
        creation_custody_mode="custody-transfer",
        delivery_context_id="urn:uuid:cabfc827-91d5-4cde-8f0f-81255b75b1c4",
        artifact_set_identity="2" * 64,
        encryption_format="none",
        passphrase_id="test",
        provenance_identity="3" * 64,
        inventory_identity="4" * 64,
        archive_root_sha256="5" * 64,
        description="exact source description",
        description_search="exact source description",
        description_revision=1,
        description_identity="7" * 64,
        created_at="2026-01-01T00:00:00Z",
    )
    session.add_all(
        (
            collection,
            CollectionArtifactRecord(
                collection_id=1,
                artifact_id=artifact_id,
                bytes=len(payload),
                sha256=member.sha256,
            ),
            CatalogSyncStateRecord(singleton=1, source_identity="6" * 64),
        )
    )
    session.flush()
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
        (artifact_id, row_key, scope) for row_key, scope in relevance_row_keys(relevance)
    )
    build_id = begin_index_build(session, collection_id=1)
    stage_snapshot_header(session, build_id=build_id, summary=summary)
    stage_entry_page(session, build_id=build_id, summary=summary, start=0)
    for start in range(0, len(rows), 32):
        stage_assertion_page(session, build_id=build_id, rows=rows[start : start + 32])
    stage_member(
        session,
        build_id=build_id,
        artifact_id=artifact_id,
        bytes=len(payload),
        sha256=member.sha256,
        journal_id=summary.journal_id,
        prefix_sha256=summary.journal_sha256,
        delivery_association_id=produced.binding.delivery_association_id,
    )
    session.flush()
    stage_membership_page(session, build_id=build_id, rows=memberships)
    complete_index_build(
        session,
        build_id=build_id,
        expected_snapshots=1,
        expected_members=1,
        expected_memberships=len(memberships),
    )
    publish_index_build(session, build_id=build_id)
    session.commit()
    return artifact_id


def test_native_discovery_returns_exact_hint_support_and_fences_changed_reads() -> None:
    engine = create_catalog_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        artifact_id = _indexed_collection(session)
        request = ArtifactDiscoveryRequest.model_validate(
            {
                "description_contains": "source description",
                "provenance_all": [
                    {
                        "kind": "occurrence",
                        "values": [
                            {
                                "pointer": "/materialization_hint/components/0",
                                "operator": "equals",
                                "value": "source.bin",
                            }
                        ],
                    }
                ],
            }
        )
        page = discover_artifacts(session, request=request, principal=None, position=None)
        wire_page = {key: value for key, value in page.items() if key != "_next_position"}
        DiscoveryPageOut.model_validate({**wire_page, "next_page_token": None})
        assert page["complete"] is True
        assert page["artifacts"][0]["artifact"]["artifact_id"] == artifact_id
        assert page["artifacts"][0]["matches"][0]["pointer"] == (
            "/materialization_hint/components/0"
        )
        collection = session.get(CollectionRecord, 1)
        assert collection is not None
        collection.description_revision += 1
        session.commit()
        with pytest.raises(Conflict, match="read changed"):
            discover_artifacts(
                session,
                request=request,
                principal=None,
                position=(page["read_identity"], "1", artifact_id),
            )
    engine.dispose()


def test_provenance_query_fails_explicitly_when_index_is_lost() -> None:
    engine = create_catalog_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        artifact_id = _indexed_collection(session)
        state = session.get(CollectionProvenanceIndexStateRecord, 1)
        assert state is not None
        state.active_build_id = None
        session.commit()
        unqualified = discover_artifacts(
            session,
            request=ArtifactDiscoveryRequest(artifact_id=artifact_id),
            principal=None,
            position=None,
        )
        assert len(unqualified["artifacts"]) == 1
        with pytest.raises(ServiceUnavailable, match="index is unavailable"):
            discover_artifacts(
                session,
                request=ArtifactDiscoveryRequest.model_validate(
                    {"provenance_all": [{"values": [{"value": "source.bin"}]}]}
                ),
                principal=None,
                position=None,
            )
    engine.dispose()


def test_provenance_matches_require_current_provenance_disclosure() -> None:
    engine = create_catalog_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        _indexed_collection(session)
        request = ArtifactDiscoveryRequest.model_validate(
            {"provenance_all": [{"values": [{"value": "source.bin"}]}]}
        )
        catalog_only = Principal(
            id="reader",
            key_id="reader-key",
            access=frozenset({ApplicationAccess(CATALOG_READ, ALL_RESOURCES)}),
        )
        assert (
            discover_artifacts(session, request=request, principal=catalog_only, position=None)[
                "artifacts"
            ]
            == []
        )
        provenance_reader = Principal(
            id="reader",
            key_id="reader-key",
            access=frozenset(
                {
                    ApplicationAccess(CATALOG_READ, ALL_RESOURCES),
                    ApplicationAccess(PROVENANCE_READ, ALL_RESOURCES),
                }
            ),
        )
        assert (
            len(
                discover_artifacts(
                    session, request=request, principal=provenance_reader, position=None
                )["artifacts"]
            )
            == 1
        )
    engine.dispose()
