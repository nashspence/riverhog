"""Actual HTTP/Native witness for a durable primary across uncertain publication."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
from riverhog_client._prepared_primary_documents import (
    PrimaryScope,
    PrimarySession,
    primary_control_bytes,
)
from riverhog_client._prepared_primary_publisher import DeclaredPrimaryObservation, PrimaryPublisher
from riverhog_client._prepared_primary_store import PrimaryStore, PrimaryStoreLimits
from riverhog_client.canonical_production import ProducerAttribution, build_member_journal
from riverhog_client.initial_tags import prepare_initial_collection_tags
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import (
    CollectionUploadProvenanceJournalRecord,
    CollectionUploadRecord,
)
from riverhog_core.services.collection_uploads import _validate_staged_canonical_journal_set
from riverhog_protocol import (
    ArtifactMemberIdentityDocument,
    CollectionUploadRegistrationConstraintsDocument,
)
from riverhog_protocol.errors import Conflict
from riverhog_provenance import BoundedSourceObserver, BytesSource
from riverhog_provenance_contracts import ContractCatalog
from sqlalchemy import select

from tests.unit.test_incremental_collection_producer import _bounded_service_api


def test_durable_primary_replays_after_actual_http_stage_bind_and_decision_acknowledgement_loss(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    api, _archive = _bounded_service_api(tmp_path)
    root = tmp_path.resolve() / "primary-state"
    limits = PrimaryStoreLimits(
        pending_bytes=16 * 1024 * 1024,
        total_bytes=32 * 1024 * 1024,
        preparation_bytes=8 * 1024 * 1024,
        control_bytes=64 * 1024,
    )
    scope = PrimaryScope(service="fixture-service", account="fixture-account")
    request = b'{"fixture":"create-inputs"}'
    store = PrimaryStore(
        root, allocation_id="fixture-allocation", scope=scope, limits=limits, initialize=True
    )
    assert store.session_for(request) is None
    key = "interrupted-durable-primary"
    with prepare_initial_collection_tags(()) as tags:
        session = api.create_or_resume_collection_upload_session(
            key,
            ingest_source="fixture-ingress",
            tags=(),
            initial_tag_set_identity=tags.tag_set_identity,
            custody_mode="custody-transfer",
        )
    collection_id = int(session["collection_id"])
    attribution_identity = hashlib.sha256(key.encode()).hexdigest()
    store.bind_session(
        PrimarySession.model_validate(
            {
                "allocation_sha256": hashlib.sha256(
                    primary_control_bytes(store.allocation)
                ).hexdigest(),
                "create_request": {
                    "bytes": str(len(request)),
                    "sha256": hashlib.sha256(request).hexdigest(),
                },
                "collection_id": str(collection_id),
                "delivery_context_id": session["delivery_context_id"],
                "server_construction_identity_sha256": session["construction_identity_sha256"],
                "attribution_construction_identity_sha256": attribution_identity,
                "completion_requirement_sha256": None,
            }
        )
    )
    payload = b"exact interrupted publication"
    member = ArtifactMemberIdentityDocument.model_validate(
        {
            "artifact_id": "ab" * 32,
            "bytes": str(len(payload)),
            "sha256": hashlib.sha256(payload).hexdigest(),
        }
    )
    attribution = ProducerAttribution(
        "fixture", "fixture/v1", "1", key, "fixture-ingress", {}, attribution_identity
    )
    observations = []

    def observe():
        result = BoundedSourceObserver().observe(BytesSource(payload))
        observations.append(result)
        return result

    lazy = DeclaredPrimaryObservation(
        "fixture-observer/v1",
        {"source_identity": "fixture-source"},
        ContractCatalog().checkpoint_sha256(),
        observe,
    )

    def unexpected_observation():
        pytest.fail("cold Native replay observed or authored a second primary")

    def prepare(publisher, selection):
        return publisher.prepare(
            member=member,
            attribution=attribution,
            observation=selection,
            automatic_observation=unexpected_observation,
            materialization_hint=None,
            allow_missing_materialization_hint=True,
        )

    try:
        publisher = PrimaryPublisher(api, store)
        original = prepare(publisher, lazy)
        api.register_collection_upload_session_artifacts(
            collection_id,
            [member.model_dump(mode="json")],
            registration_constraints=CollectionUploadRegistrationConstraintsDocument.model_validate(
                session["registration_constraints"]
            ),
        )

        def lost_bind(*_args, **_kwargs):
            raise RuntimeError("interrupted after exact Native staging before bind")

        with monkeypatch.context() as patch:
            patch.setattr(api, "bind_collection_upload_session_artifact_provenance", lost_bind)
            with pytest.raises(RuntimeError, match="before bind"):
                publisher.replay(original)
    finally:
        store.close()
    store = PrimaryStore(
        root, allocation_id="fixture-allocation", scope=scope, limits=limits, initialize=False
    )
    try:
        assert store.session_for(request) == store.session
        publisher = PrimaryPublisher(api, store)
        lazy = DeclaredPrimaryObservation(
            lazy.observer, lazy.configuration, lazy.catalog_sha256, unexpected_observation
        )
        retained = prepare(publisher, lazy)
        assert retained.prepared == original.prepared
        accept = api.set_collection_upload_session_materialization_decisions

        def lost_decision(*args, **kwargs):
            accept(*args, **kwargs)
            raise RuntimeError("lost exact decision acknowledgement")

        with monkeypatch.context() as patch:
            patch.setattr(
                api, "set_collection_upload_session_materialization_decisions", lost_decision
            )
            with pytest.raises(RuntimeError, match="decision acknowledgement"):
                publisher.replay(retained)
        assert store.load(member).decision_accepted is None
    finally:
        store.close()
    store = PrimaryStore(
        root, allocation_id="fixture-allocation", scope=scope, limits=limits, initialize=False
    )
    try:
        publisher = PrimaryPublisher(api, store)
        accepted = publisher.replay(prepare(publisher, lazy))
        assert accepted.decision_accepted is not None
        assert len(observations) == 1
        assert (
            api.get_collection_upload_session_artifact_provenance_binding(
                collection_id, member.artifact_id
            )
            == original.prepared.primary
        )
        with session_scope(api.service._session_factory) as session_db:
            journals = set(
                session_db.scalars(
                    select(CollectionUploadProvenanceJournalRecord.journal_id).where(
                        CollectionUploadProvenanceJournalRecord.collection_id == collection_id
                    )
                )
            )
        assert journals == {original.prepared.primary.journal.journal_id}
        for _ in range(3):
            if not api.service._stage_next_member_histories(collection_id):
                break
        else:
            pytest.fail("the single member history did not seal in bounded steps")
        with session_scope(api.service._session_factory) as session_db:
            upload = session_db.get(CollectionUploadRecord, collection_id)
            _validate_staged_canonical_journal_set(session_db, upload)
        unrelated = build_member_journal(
            member=member,
            observation=observations[0],
            delivery_context_id=store.session.delivery_context_id,
            attribution=attribution,
            materialization_hint=None,
        )
        api.upload_collection_upload_session_provenance_journal(
            collection_id,
            unrelated.journal_id,
            content=(unrelated.content,),
            byte_count=len(unrelated.content),
            sha256=hashlib.sha256(unrelated.content).hexdigest(),
        )
        with session_scope(api.service._session_factory) as session_db:
            upload = session_db.get(CollectionUploadRecord, collection_id)
            with pytest.raises(Conflict, match="unrelated journals"):
                _validate_staged_canonical_journal_set(session_db, upload)
    finally:
        store.close()
        api.close()
