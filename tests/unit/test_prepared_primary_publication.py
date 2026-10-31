"""Actual HTTP/Native witness for a durable primary across uncertain publication."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
from riverhog_client._prepared_primary_documents import (
    PrimaryScope,
)
from riverhog_client._prepared_primary_publisher import DeclaredPrimaryObservation, PrimaryPublisher
from riverhog_client._prepared_primary_session import open_primary_session
from riverhog_client._prepared_primary_store import PrimaryStore, PrimaryStoreLimits
from riverhog_client.canonical_production import ProducerAttribution, build_member_journal
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import (
    CollectionUploadProvenanceJournalRecord,
    CollectionUploadRecord,
)
from riverhog_core.services.collection_uploads import _validate_staged_canonical_journal_set
from riverhog_protocol import (
    ArtifactMemberIdentityDocument,
    CollectionUploadArtifactCustodyReceiptDocument,
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
    store = PrimaryStore(
        root, allocation_id="fixture-allocation", scope=scope, limits=limits, initialize=True
    )
    key = "interrupted-durable-primary"
    attribution = ProducerAttribution(
        "fixture",
        "fixture/v1",
        "1",
        key,
        "fixture-ingress",
        {},
        hashlib.sha256(key.encode()).hexdigest(),
    )

    def open_session():
        return open_primary_session(
            api,
            store,
            attribution=attribution,
            idempotency_key=key,
            archive_store=None,
            use_cache=None,
            copy_to=None,
            description=None,
            tags=(),
            event_context=None,
        )

    session, attribution = open_session()
    collection_id = int(session["collection_id"])
    payload = b"exact interrupted publication"
    member = ArtifactMemberIdentityDocument.model_validate(
        {
            "artifact_id": "ab" * 32,
            "bytes": str(len(payload)),
            "sha256": hashlib.sha256(payload).hexdigest(),
        }
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
        assert open_session()[0]["resumed"] is True
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
        from riverhog_client._prepared_primary_closing import advance_closing, freeze_inventory

        freeze_inventory(store, (member,))
        assert advance_closing(
            store, limit=1, settle=lambda _member: pytest.fail("settled decision replayed")
        )
        assert store.load(member).custody is None
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


def test_shared_producer_restarts_exact_preparation_and_drains_finalized_custody(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from riverhog_client import (
        IncrementalCollectionProducer,
        ProducerArtifactIdentity,
        ProducerFile,
    )

    monkeypatch.setenv("RIVERHOG_UPLOAD_FILE_CONCURRENCY", "1")
    api, _archive = _bounded_service_api(tmp_path)
    root = tmp_path.resolve() / "producer-primary"
    source = tmp_path / "source.bin"
    payload = b"prepared through the maintained producer"
    source.write_bytes(payload)
    member = ProducerArtifactIdentity("ad" * 32, len(payload), hashlib.sha256(payload).hexdigest())
    limits = PrimaryStoreLimits(
        pending_bytes=16 * 1024 * 1024,
        total_bytes=32 * 1024 * 1024,
        preparation_bytes=8 * 1024 * 1024,
        control_bytes=64 * 1024,
    )
    scope = PrimaryScope(service="fixture-service", account="fixture-account")
    observations = []

    def observe():
        result = BoundedSourceObserver().observe(BytesSource(payload))
        observations.append(result)
        return result

    selection = DeclaredPrimaryObservation(
        "fixture-observer/v1",
        {"source_identity": "fixture-source"},
        ContractCatalog().checkpoint_sha256(),
        observe,
    )

    def construct(store, inventory=lambda: (member,)):
        return IncrementalCollectionProducer(
            api,
            primary_store=store,
            member_inventory=inventory,
            producer_app="fixture",
            adapter_id="fixture/v1",
            adapter_version="1",
            ingest_source="fixture",
            source_event_id="producer-interrupted-primary",
        )

    def input_file():
        return ProducerFile(
            source,
            member.artifact_id,
            observation=selection,
            allow_missing_materialization_hint=True,
        )

    store = PrimaryStore(
        root, allocation_id="fixture-allocation", scope=scope, limits=limits, initialize=True
    )
    producer = construct(store)
    collection_id = producer.collection_id
    try:
        registration = api.register_collection_upload_session_artifacts

        def require_prepared(cid, members, **kwargs):
            for registered in members:
                assert (
                    store.load(ArtifactMemberIdentityDocument.model_validate(registered))
                    is not None
                )
            return registration(cid, members, **kwargs)

        def lost_bind(*_args, **_kwargs):
            raise RuntimeError("producer stage before bind")

        with monkeypatch.context() as patch:
            patch.setattr(api, "register_collection_upload_session_artifacts", require_prepared)
            patch.setattr(api, "bind_collection_upload_session_artifact_provenance", lost_bind)
            with pytest.raises(RuntimeError, match="stage before bind"):
                producer.append_inputs((input_file(),))
    finally:
        producer.stop()
        store.close()

    store = PrimaryStore(
        root, allocation_id="fixture-allocation", scope=scope, limits=limits, initialize=False
    )
    producer = construct(store)
    try:
        accept = api.set_collection_upload_session_materialization_decisions

        def lost_ack(*args, **kwargs):
            accept(*args, **kwargs)
            raise RuntimeError("producer lost decision acknowledgement")

        with monkeypatch.context() as patch:
            patch.setattr(api, "set_collection_upload_session_materialization_decisions", lost_ack)
            with pytest.raises(RuntimeError, match="decision acknowledgement"):
                producer.append_inputs((input_file(),))
    finally:
        producer.stop()
        store.close()

    store = PrimaryStore(
        root, allocation_id="fixture-allocation", scope=scope, limits=limits, initialize=False
    )
    producer = construct(store)
    try:
        producer.append_inputs((input_file(),))
        retained = store.load(
            ArtifactMemberIdentityDocument.model_validate(
                {
                    "artifact_id": member.artifact_id,
                    "bytes": str(member.bytes),
                    "sha256": member.sha256,
                }
            )
        )
        with session_scope(api.service._session_factory) as session_db:
            assert set(
                session_db.scalars(
                    select(CollectionUploadProvenanceJournalRecord.journal_id).where(
                        CollectionUploadProvenanceJournalRecord.collection_id == collection_id
                    )
                )
            ) == {retained.prepared.primary.journal.journal_id}
        result = producer.finish(poll_seconds=0.01, timeout_seconds=300)
        assert result.receipt["state"] == "finalized"
        published = api.get_collection_upload_session_artifact(collection_id, member.artifact_id)
        assert (
            CollectionUploadArtifactCustodyReceiptDocument.model_validate(
                published["custody_receipt"]
            ).primary
            == retained.prepared.primary
        )
        assert len(observations) == 1
        assert store.usage.pending_bytes == 0
        assert (root / "custody-settled").is_file()
        source.unlink()
    finally:
        producer.stop()
        store.close()

    def no_source_inventory():
        pytest.fail("finalized custody retry enumerated released source inventory")
        yield

    store = PrimaryStore(
        root, allocation_id="fixture-allocation", scope=scope, limits=limits, initialize=False
    )
    producer = construct(store, no_source_inventory)
    try:
        resumed_result = producer.finish(poll_seconds=0.01, timeout_seconds=30)
        assert (
            resumed_result.collection_id,
            resumed_result.archive_root_sha256,
            resumed_result.artifact_set_identity,
        ) == (result.collection_id, result.archive_root_sha256, result.artifact_set_identity)
        custody = producer.resume_artifact_custody(member)
        assert custody is not None and custody.artifact == member
        assert not source.exists() and len(observations) == 1
    finally:
        producer.stop()
        store.close()
        api.close()


def test_authenticated_custody_survives_lost_decision_ack_and_damaged_local_journals(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from riverhog_client import (
        IncrementalCollectionProducer,
        PrimaryStateError,
        ProducerArtifactIdentity,
        ProducerFile,
    )
    from riverhog_client._prepared_primary_posix import PrimaryFiles
    from riverhog_protocol.errors import Unauthorized

    monkeypatch.setenv("RIVERHOG_UPLOAD_FILE_CONCURRENCY", "1")
    api, _archive = _bounded_service_api(tmp_path)
    root = tmp_path.resolve() / "primary"
    limits = PrimaryStoreLimits(
        pending_bytes=16 * 1024**2,
        total_bytes=32 * 1024**2,
        preparation_bytes=8 * 1024**2,
        control_bytes=64 * 1024,
    )
    scope = PrimaryScope(service="fixture-service", account="fixture-account")
    identities = []
    sources = []
    for number in range(3):
        content = f"receipt member {number}".encode()
        source = tmp_path / f"source-{number}"
        source.write_bytes(content)
        identities.append(
            ProducerArtifactIdentity(
                f"{number + 1:064x}", len(content), hashlib.sha256(content).hexdigest()
            )
        )
        sources.append(source)

    def construct(store):
        return IncrementalCollectionProducer(
            api,
            primary_store=store,
            member_inventory=lambda: tuple(identities),
            producer_app="fixture",
            adapter_id="fixture/v1",
            adapter_version="1",
            ingest_source="fixture",
            source_event_id="receipt-without-decision-ack",
        )

    store = PrimaryStore(
        root, allocation_id="fixture-allocation", scope=scope, limits=limits, initialize=True
    )
    producer = construct(store)
    collection_id = producer.collection_id
    originals = []
    try:
        accept = api.set_collection_upload_session_materialization_decisions

        def lost_ack(*args, **kwargs):
            accept(*args, **kwargs)
            raise RuntimeError("lost exact decision acknowledgement")

        with monkeypatch.context() as patch:
            patch.setattr(api, "set_collection_upload_session_materialization_decisions", lost_ack)
            for source, identity in zip(sources, identities, strict=True):
                with pytest.raises(RuntimeError, match="decision acknowledgement"):
                    producer.append_inputs(
                        (
                            ProducerFile(
                                source,
                                identity.artifact_id,
                                allow_missing_materialization_hint=True,
                            ),
                        )
                    )
        for identity in identities:
            member = ArtifactMemberIdentityDocument.model_validate(
                {
                    "artifact_id": identity.artifact_id,
                    "bytes": str(identity.bytes),
                    "sha256": identity.sha256,
                }
            )
            record = store.load(member)
            assert record.decision_accepted is None and record.custody is None
            originals.append(record)
        with session_scope(api.service._session_factory) as session_db:
            assert set(
                session_db.scalars(
                    select(CollectionUploadProvenanceJournalRecord.journal_id).where(
                        CollectionUploadProvenanceJournalRecord.collection_id == collection_id
                    )
                )
            ) == {record.prepared.primary.journal.journal_id for record in originals}
        for original in originals:
            assert (
                api.get_collection_upload_session_artifact_provenance_binding(
                    collection_id, original.prepared.member.artifact_id
                )
                == original.prepared.primary
            )
        # The service accepted all exact decisions despite the deliberately lost responses.
        api.complete_collection_upload_session(collection_id)
        producer._upload_available(reconcile=False)
        for _ in range(100):
            if api.get_collection_upload_session(collection_id)["state"] == "finalized":
                break
        else:
            pytest.fail("the three-member remote finalization did not settle")
        pending = store.usage.pending_bytes
        for number, record in enumerate(originals):
            with store._member(str(record.prepared.member.artifact_id), create=False) as files:
                path = files.root / "journal"
                raw = path.read_bytes()
                if number == 0:
                    path.unlink()
                else:
                    path.write_bytes(raw[:1] if number == 1 else b"x" * len(raw))
    finally:
        producer.stop()
        store.close()

    store = PrimaryStore(
        root, allocation_id="fixture-allocation", scope=scope, limits=limits, initialize=False
    )
    producer = construct(store)
    try:
        from riverhog_api.deps import get_container

        application_container = api._app.dependency_overrides[get_container]()
        with monkeypatch.context() as patch:
            patch.setattr(application_container.app_keys, "authenticate", lambda _token: None)
            with pytest.raises(Unauthorized):
                producer.resume_artifact_custody(identities[0])
        assert store.load_controls(originals[0].prepared.member).custody is None

        def no_byte_validation(*_args, **_kwargs):
            pytest.fail("receipt reconciliation attempted pending-byte validation")

        with monkeypatch.context() as patch:
            patch.setattr(PrimaryFiles, "verified", no_byte_validation)
            for source, identity, original in zip(sources, identities, originals, strict=True):
                custody = producer.resume_artifact_custody(identity)
                assert custody is not None
                assert custody.receipt.primary == original.prepared.primary
                assert source.exists()
                retained = store.load_controls(original.prepared.member)
                assert retained.custody.receipt_sha256 == custody.receipt.receipt_sha256
                assert retained.decision_accepted is None
                assert not store.reclaim(original.prepared.member)
                source.unlink()  # The application releases payload only after durable custody.
        assert store.usage.pending_bytes == pending
        assert not (root / "custody-settled").exists()
        for original in originals:
            with pytest.raises(PrimaryStateError):
                PrimaryPublisher(api, store).replay(store.load_controls(original.prepared.member))
            published = api.get_collection_upload_session_artifact(
                collection_id, original.prepared.member.artifact_id
            )
            exact_receipt = CollectionUploadArtifactCustodyReceiptDocument.model_validate(
                published["custody_receipt"]
            )
            assert exact_receipt.primary == original.prepared.primary
            assert (
                exact_receipt.receipt_sha256
                == store.load_controls(original.prepared.member).custody.receipt_sha256
            )
    finally:
        producer.stop()
        store.close()
        api.close()
