from __future__ import annotations

import hashlib
from dataclasses import replace
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from http_api_contracts import BrowseTokenCodec
from riverhog_api.app import create_app
from riverhog_api.deps import ServiceContainer
from riverhog_archive_contracts import (
    BOUND_HISTORY_EXTENT,
    MemberHistoryBinding,
    MemberHistoryBuilder,
    MemberHistoryPrimary,
)
from riverhog_client.canonical_completion import CompletionRecord
from riverhog_client.canonical_production import (
    ProducerAttribution,
    build_member_journal,
    member_materialization_decision,
)
from riverhog_client.client import ApiClient
from riverhog_client.initial_tags import prepare_initial_collection_tags
from riverhog_client.processing.models import ClaimedArtifact, DerivedCollectionSpec
from riverhog_client.processing.reader import ClaimedCollectionReader
from riverhog_client.processing.writer import IncrementalDerivedCollectionWriter
from riverhog_client.producer import ProducerArtifactIdentity, ProducerFile
from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.catalog_db import initialize_db, make_session_factory, session_scope
from riverhog_core.catalog_models import CollectionArchiveCopyRecord, CollectionUploadRecord
from riverhog_core.collection_access import SqlAlchemyCollectionAccessService
from riverhog_core.collection_plan import CollectionVolumePolicy
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services.app_keys import SqlAlchemyAppKeyService
from riverhog_core.services.archive_copy_jobs import SqlAlchemyArchiveCopyJobService
from riverhog_core.services.archive_copy_retirements import (
    SqlAlchemyArchiveCopyRetirementService,
)
from riverhog_core.services.archive_stores import SqlAlchemyArchiveStoreService
from riverhog_core.services.canonical_provenance import SqlAlchemyCanonicalProvenanceService
from riverhog_core.services.catalog_sync import SqlAlchemyCatalogSyncService
from riverhog_core.services.collection_deletions import SqlAlchemyCollectionDeletionService
from riverhog_core.services.collection_descriptions import (
    SqlAlchemyCollectionDescriptionService,
)
from riverhog_core.services.collection_tags import SqlAlchemyCollectionTagService
from riverhog_core.services.collection_uploads import SqlAlchemyCollectionUploadService
from riverhog_core.services.collection_workflows import (
    SqlAlchemyCollectionWorkflowService,
)
from riverhog_core.services.collections import SqlAlchemyCollectionService
from riverhog_core.services.download_allowances import SqlAlchemyDownloadAllowance
from riverhog_core.services.lifecycle_events import SqlAlchemyLifecycleEventService
from riverhog_core.services.retrieval import SqlAlchemyRetrievalService
from riverhog_core.services.search import SqlAlchemySearchService
from riverhog_protocol import (
    ArtifactId,
    ArtifactMemberIdentityDocument,
    CollectionArtifactProvenanceBindingBatchDocument,
    CollectionUploadRawDigestBatchDocument,
    CollectionUploadUnitWorkDocument,
    MemberHistoryBindingBatchDocument,
)
from riverhog_protocol.collection_production_provenance import COLLECTION_MEMBER_ROLE
from riverhog_protocol.collection_record_preimages import CollectionRecordPreimages
from riverhog_protocol.collection_workflows import (
    ArtifactDisposition,
    ArtifactDispositionOutput,
    ArtifactDispositionSetIdentity,
    CollectionProcessingOutcomeIdentity,
    CollectionRootIdentity,
    OperationIdentity,
    RecipeIdentity,
    canonical_json_bytes,
    canonical_json_sha256,
    processing_outcome_set_identity,
)
from riverhog_protocol.effect_settlement import ExternalEffectSettlement
from riverhog_protocol.errors import Conflict, Forbidden, NotFound
from riverhog_protocol.no_output_settlement import NoOutputSettlement
from riverhog_protocol.raw_ingress import ordered_raw_part_commitment
from riverhog_provenance import (
    MemberHistoryClosure,
    reference,
    validate_journal,
)
from sqlalchemy import select

from tests.operation_observer import OperationObserver, TimeoutNeutralTestClient
from tests.provenance_observer import native_provenance_provider
from tests.support.qualification.live_archive_recovery import qualify_offline_history_recovery
from tests.support.uploaded_archive import UploadedArchiveStore
from tests.unit.archive_object_fixtures import archive_store_binding
from tests.unit.db_helpers import sqlite_url
from tests.unit.storage_incarnation_fixtures import seed_storage_incarnation

SOURCE_ID = ArtifactId("1" * 64)
OUTPUT_ID = ArtifactId("2" * 64)


def _tag_set_identity(*tags: str) -> str:
    with prepare_initial_collection_tags(tags) as prepared:
        return prepared.tag_set_identity


def _container(tmp_path: Path, *, database_url: str | None = None) -> ServiceContainer:
    database_url = database_url or sqlite_url(tmp_path / "catalog.sqlite3")
    baseline = RuntimeConfig.for_testing(database_url=database_url, archive_scrypt_work_factor=1)
    primary_config = replace(
        baseline.archive_store("archive"),
        name="primary",
        base_url="http://127.0.0.1:9001",
    )
    secondary_config = replace(
        baseline.archive_store("archive"),
        name="secondary",
        base_url="http://127.0.0.1:9002",
    )
    config = replace(
        baseline,
        archive_write_store="primary",
        archive_read_order=("primary", "secondary"),
        archive_stores={"primary": primary_config, "secondary": secondary_config},
    )
    initialize_db(database_url)
    session_factory = make_session_factory(database_url)
    with session_scope(session_factory) as session:
        seed_storage_incarnation(session, "archive", "primary")
        seed_storage_incarnation(session, "archive", "secondary")
    stores = ArchiveStoreRegistry(
        {
            "primary": archive_store_binding(
                UploadedArchiveStore(passphrases=config.archive_passphrases), name="primary"
            ),
            "secondary": archive_store_binding(
                UploadedArchiveStore(passphrases=config.archive_passphrases), name="secondary"
            ),
        }
    )
    allowances = SqlAlchemyDownloadAllowance(config, session_factory=session_factory)
    return ServiceContainer(
        app_keys=SqlAlchemyAppKeyService(config, session_factory=session_factory),
        collection_access=SqlAlchemyCollectionAccessService(
            config, session_factory=session_factory
        ),
        collection_tags=SqlAlchemyCollectionTagService(
            config, stores, session_factory=session_factory
        ),
        collections=SqlAlchemyCollectionService(config, session_factory=session_factory),
        collection_descriptions=SqlAlchemyCollectionDescriptionService(
            config,
            stores,
            session_factory=session_factory,
        ),
        catalog_sync=SqlAlchemyCatalogSyncService(config, session_factory=session_factory),
        collection_uploads=SqlAlchemyCollectionUploadService(
            config,
            stores,
            policy=CollectionVolumePolicy(pack_member_bytes=1),
            session_factory=session_factory,
        ),
        collection_workflows=SqlAlchemyCollectionWorkflowService(
            config, session_factory=session_factory
        ),
        provenance=SqlAlchemyCanonicalProvenanceService(
            config, stores, session_factory=session_factory
        ),
        collection_deletions=SqlAlchemyCollectionDeletionService(
            config,
            stores,
            None,
            session_factory=session_factory,
        ),
        search=SqlAlchemySearchService(config, session_factory=session_factory),
        archive_copy_jobs=SqlAlchemyArchiveCopyJobService(
            config,
            stores,
            session_factory=session_factory,
        ),
        archive_copy_retirements=SqlAlchemyArchiveCopyRetirementService(
            config,
            stores,
            session_factory=session_factory,
        ),
        archive_stores=SqlAlchemyArchiveStoreService(
            config,
            stores,
            download_allowance=allowances,
            session_factory=session_factory,
        ),
        retrieval=SqlAlchemyRetrievalService(
            config,
            stores,
            None,
            download_allowance=allowances,
            session_factory=session_factory,
        ),
        lifecycle_events=SqlAlchemyLifecycleEventService(
            config,
            session_factory=session_factory,
        ),
        download_quotas=allowances,
        session_factory=session_factory,
        browse_tokens=BrowseTokenCodec(
            config.browse_token_signing_key,
            lifetime_seconds=int(config.browse_token_lifetime.total_seconds()),
        ),
    )


class _LocalApi(ApiClient):
    """Keep spawned SDK clients on real ASGI authentication and worker services."""

    transport: TestClient
    observer: OperationObserver | None
    workers: ServiceContainer | None

    def spawn(self):
        return _api(self.transport, self.token, observer=self.observer, workers=self.workers)

    def get_collection_upload_session(self, collection_id):
        if self.workers is not None:
            self.workers.collection_uploads.process_due_custody_receipts()
            self.workers.collection_uploads.process_due_finalizations()
        return super().get_collection_upload_session(collection_id)

    def seal_collection_upload_session_provenance_journal(self, collection_id, journal_id):
        if self.workers is not None:
            self.workers.collection_uploads.process_due_provenance_journal_validations()
        return super().seal_collection_upload_session_provenance_journal(collection_id, journal_id)


def _api(
    test_client: TestClient,
    token: str,
    *,
    observer: OperationObserver | None = None,
    workers: ServiceContainer | None = None,
) -> ApiClient:
    api = _LocalApi(
        base_url="http://testserver",
        token=token,
        allow_insecure_http=True,
    )
    bound = TimeoutNeutralTestClient(
        TestClient(test_client.app, headers={"Authorization": f"Bearer {token}"}),
        observer=observer,
    )
    api.transport = test_client
    api.observer = observer
    api.workers = workers
    api._request_client = bound  # type: ignore[assignment]
    api._download_client = bound  # type: ignore[assignment]
    return api


def _unit_content(root: dict[str, Path], unit: CollectionUploadUnitWorkDocument) -> bytes:
    content = bytearray()
    for source in unit.sources:
        payload = root[str(source.artifact_id)].read_bytes()
        content.extend(payload[source.offset : source.offset + source.bytes])
    assert len(content) == unit.payload_bytes
    return bytes(content)


def _finalize_upload(
    container: ServiceContainer,
    api: ApiClient,
    collection_id: int,
) -> None:
    for _ in range(256):
        if api.get_collection_upload_session(collection_id)["state"] == "finalized":
            return
        assert container.collection_uploads.process_due_finalizations() == 1
    raise AssertionError("bounded collection finalization did not terminate")


def test_riverhog_official_client_positive_disposable_lifecycle(
    tmp_path: Path,
) -> None:  # type: ignore[no-untyped-def]
    container = _container(tmp_path)
    container.bootstrap_token = "qualification-bootstrap"
    application = create_app(container=container)
    observer = OperationObserver.install(application, application="riverhog")
    transport = TestClient(application)
    bootstrap = _api(transport, "qualification-bootstrap", observer=observer)

    operator_key = bootstrap.create_app_key(
        "qualification-operator",
        access=[{"permission": "*", "resource": "*"}],
    )
    bootstrap.set_app_key_download_quota(
        "qualification-operator",
        str(operator_key["id"]),
        monthly_bytes=1024 * 1024,
    )
    operator_token = str(operator_key["token"])
    operator_headers = {"Authorization": f"Bearer {operator_token}"}
    operator = _api(transport, operator_token, observer=observer)

    missing_archive_source = transport.post(
        "/v1/archive/copy-jobs",
        headers=operator_headers,
        json={"collection_id": "999", "destination_store": "secondary"},
    )
    assert missing_archive_source.status_code == 404
    assert missing_archive_source.json()["error"]["code"] == "not_found"
    archive_copy_errors = application.openapi()["paths"]["/v1/archive/copy-jobs"]["post"][
        "responses"
    ]
    assert "not_found" in archive_copy_errors["404"]["x-riverhog-error-codes"]
    assert transport.get("/health/live").json() == {"service": "riverhog", "status": "ok"}
    assert transport.get("/health/ready").json() == {"service": "riverhog", "status": "ok"}

    delegated = operator.create_app_key(
        "qualification-reader",
        access=[{"permission": "catalog:read", "resource": "*"}],
    )
    delegated_id = str(delegated["id"])
    assert len(operator.list_apps(q="qualification", page_size=100, page_token=None)["apps"]) == 2
    assert (
        len(operator.list_app_keys("qualification-reader", page_size=100, page_token=None)["keys"])
        == 1
    )
    assert (
        len(
            operator.list_app_key_access(key_id=delegated_id, page_size=100, page_token=None)[
                "access"
            ]
        )
        == 1
    )
    operator.replace_app_key_access(
        "qualification-reader",
        delegated_id,
        access=[{"permission": "catalog:read", "resource": "*"}],
    )
    operator.add_app_key_access(
        "qualification-reader",
        delegated_id,
        permission="events:read",
        resource="*",
    )
    operator.remove_app_key_access(
        "qualification-reader",
        delegated_id,
        permission="events:read",
        resource="*",
    )
    operator.set_app_key_download_quota(
        "qualification-reader",
        delegated_id,
        monthly_bytes=1024,
    )
    assert (
        operator.list_download_quotas(app="qualification-reader", page_size=100, page_token=None)[
            "quotas"
        ][0]["monthly_bytes"]
        == 1024
    )
    assert operator.get_download_quota()["app"] == "qualification-operator"
    rotated = operator.rotate_app_key("qualification-reader", delegated_id)
    operator.revoke_app_key("qualification-reader", str(rotated["id"]))

    source_root = tmp_path / "source"
    source_root.mkdir()
    source = source_root / "document.txt"
    source.write_bytes(b"qualified archive content\n")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    member = ArtifactMemberIdentityDocument(
        artifact_id=SOURCE_ID, bytes=str(source.stat().st_size), sha256=digest
    )
    opened = operator.create_or_resume_collection_upload_session(
        "qualification-upload",
        ingest_source="disposable-test",
        description="Qualification source collection",
        tags=["docs"],
        initial_tag_set_identity=_tag_set_identity("docs", "qualified"),
        archive_store="primary",
    )
    assert opened["resumed"] is False
    collection_id = int(opened["collection_id"])
    staged_tags = operator.add_collection_upload_session_tags(
        collection_id,
        ["qualified"],
    )
    assert staged_tags["added"] == 1
    assert staged_tags["tag_count"] == 2
    assert (
        len(operator.list_collection_upload_sessions(page_size=100, page_token=None)["uploads"])
        == 1
    )
    observed = native_provenance_provider().observe_native_file(
        source, host_id="urn:uuid:00000000-0000-4000-8000-000000000469"
    )
    produced = build_member_journal(
        member=member,
        observation=observed,
        delivery_context_id=opened["delivery_context_id"],
        attribution=ProducerAttribution(
            producer_app="riverhog-operation-qualification",
            adapter_id="qualification-ingress/v1",
            adapter_version="1.0.0",
            source_event_id="source-1",
            ingest_source="disposable-test",
            source_context={},
            construction_identity=opened["construction_identity_sha256"],
        ),
        materialization_hint=("document.txt",),
    )
    journal = produced.content
    journal_summary = validate_journal(journal, require_profiles=False)
    binding = member
    _count, part_commitment = ordered_raw_part_commitment((digest,))
    operator.register_collection_upload_session_artifacts(
        collection_id,
        [
            {
                **member.model_dump(mode="json"),
                "raw_parts": {
                    "part_plaintext_bytes": opened["registration_constraints"][
                        "raw_part_plaintext_bytes"
                    ],
                    "part_count": "1",
                    "ordered_sha256": part_commitment,
                },
            }
        ],
        registration_constraints=opened["registration_constraints"],
    )
    operator.register_collection_upload_session_raw_part_digests(
        collection_id,
        CollectionUploadRawDigestBatchDocument(
            artifact_id=SOURCE_ID, first_part="0", sha256s=[digest]
        ),
    )
    staged = operator.upload_collection_upload_session_provenance_journal(
        collection_id,
        produced.journal_id,
        content=(journal,),
        byte_count=len(journal),
        sha256=journal_summary.journal_sha256,
    )
    while staged.state != "sealed":
        assert staged.state == "validating"
        assert container.collection_uploads.process_due_provenance_journal_validations() == 1
        staged = operator.get_collection_upload_session_provenance_journal(
            collection_id, produced.journal_id
        )
    operator.bind_collection_upload_session_artifact_provenance(
        collection_id,
        CollectionArtifactProvenanceBindingBatchDocument(bindings=[produced.binding]),
    )
    operator.set_collection_upload_session_materialization_decisions(
        collection_id,
        member_materialization_decision(
            artifact_id=str(SOURCE_ID),
            materialization_hint=("document.txt",),
            allow_missing_materialization_hint=False,
        ),
    )
    assert (
        len(
            operator.list_collection_upload_session_artifacts(
                collection_id, page_size=100, page_token=None
            )["artifacts"]
        )
        == 1
    )
    # An ordinary producer may author its explicit history selection before publication.
    with MemberHistoryBuilder(
        artifact_id=str(SOURCE_ID),
        bytes=int(member.bytes),
        sha256=member.sha256,
        primary=MemberHistoryPrimary.from_mapping(
            {
                "journal": produced.binding.journal.model_dump(mode="json"),
                "delivery_association_id": produced.binding.delivery_association_id,
            }
        ),
    ) as histories:
        selected, descriptor = histories.seal()
        for authority in (descriptor.roots, descriptor.imports):
            for page in histories.pages(authority):
                operator.stage_collection_upload_session_history_structure(
                    collection_id, page.to_json_bytes()
                )
        operator.stage_collection_upload_session_history_structure(
            collection_id, descriptor.to_json_bytes()
        )
        accepted_history = operator.bind_collection_upload_session_member_histories(
            collection_id,
            MemberHistoryBindingBatchDocument.model_validate({"bindings": [selected.to_mapping()]}),
        )
        assert accepted_history.bindings[0].model_dump(mode="json") == selected.to_mapping()
    operator.complete_collection_upload_session(collection_id)
    while work := operator.acquire_collection_upload_session_work(collection_id).work:
        for assignment in work:
            operator.put_collection_upload_session_unit(
                collection_id,
                assignment.volume.volume_id,
                assignment.unit.unit,
                plan_sha256=assignment.plan_sha256,
                content=_unit_content({str(SOURCE_ID): source}, assignment.unit),
            )
    assert (
        len(
            operator.list_collection_upload_session_artifacts(
                collection_id, page_size=100, page_token=None
            )["artifacts"]
        )
        == 1
    )
    _finalize_upload(container, operator, collection_id)
    assert operator.get_collection_upload_session(collection_id)["state"] == "finalized"
    assert operator.get_upload_copy_intents(collection_id) == {
        "collection_id": str(collection_id),
        "archive_store": "primary",
        "use_cache": False,
        "copy_to": [],
        "intents": [],
    }
    from scripts.provider_qualification import CorpusFile, CorpusManifest, _corpus_artifacts

    corpus = CorpusManifest(
        "lifecycle",
        (CorpusFile("document.txt", int(member.bytes), digest),),
        int(member.bytes),
        "a" * 64,
    )
    assert _corpus_artifacts(operator, collection_id, corpus) == {"document.txt": str(SOURCE_ID)}
    source_collection = operator.get_collection(collection_id)
    assert source_collection["id"] == str(collection_id)
    assert source_collection["description"] == "Qualification source collection"
    updated_description = operator.replace_collection_description(
        collection_id,
        "Qualified source collection",
        expected_identity=str(source_collection["description_identity"]),
    )
    assert updated_description["description"] == "Qualified source collection"
    assert (
        operator.get_collection(collection_id)["description_identity"]
        == updated_description["description_identity"]
    )
    tag_authority = operator.get_collection(collection_id)
    listed_tags = operator.list_collection_tags(
        collection_id,
        revision=int(tag_authority["tag_revision"]),
        tag_set_identity=str(tag_authority["tag_set_identity"]),
        page_size=100,
        page_token=None,
    )
    assert set(listed_tags["tags"]) == {"docs", "qualified"}
    assert (
        operator.collection_contains_tag(
            collection_id,
            tag="docs",
            revision=int(tag_authority["tag_revision"]),
            tag_set_identity=str(tag_authority["tag_set_identity"]),
        )["present"]
        is True
    )
    assert [item["tag"] for item in operator.list_tags(q="doc")["tags"]] == ["docs"]
    assert (
        len(
            operator.list_collection_archive_copies(collection_id, page_size=100, page_token=None)[
                "copies"
            ]
        )
        == 1
    )
    assert len(operator.list_collections(page_size=100, page_token=None)["collections"]) == 1
    assert (
        len(
            operator.search(
                str(SOURCE_ID), collection=collection_id, page_size=100, page_token=None
            )["artifacts"]
        )
        == 1
    )
    removed = operator.remove_collection_tag(
        collection_id,
        tag="docs",
        operation_id="qualification-remove-docs",
        expected_revision=int(tag_authority["tag_revision"]),
        expected_tag_set_identity=str(tag_authority["tag_set_identity"]),
    )
    assert removed["changed"] is True
    replayed_after_tag_edit = operator.create_or_resume_collection_upload_session(
        "qualification-upload",
        ingest_source="disposable-test",
        description="Qualification source collection",
        tags=["docs"],
        initial_tag_set_identity=_tag_set_identity("docs", "qualified"),
        archive_store="primary",
    )
    assert replayed_after_tag_edit["state"] == "finalized"
    assert int(replayed_after_tag_edit["collection_id"]) == collection_id
    restored = operator.add_collection_tag(
        collection_id,
        tag="docs",
        operation_id="qualification-restore-docs",
        expected_revision=int(removed["revision"]),
        expected_tag_set_identity=str(removed["tag_set_identity"]),
    )
    assert restored["changed"] is True

    listed = operator.list_collection_artifact_provenance(collection_id, page_size=100)
    assert len(listed["artifacts"]) == 1
    discovery = operator.discover_artifacts(
        {
            "collections": [str(collection_id)],
            "artifact_id": str(SOURCE_ID),
            "provenance_all": [
                {
                    "scopes": ["member"],
                    "kind": "occurrence",
                    "values": [
                        {
                            "pointer": "/materialization_hint/components/0",
                            "operator": "equals",
                            "value": "document.txt",
                        }
                    ],
                }
            ],
        }
    )
    assert discovery["complete"] and len(discovery["artifacts"]) == 1
    discovered = discovery["artifacts"][0]
    assert discovered["artifact"] == member.model_dump(mode="json")
    assert discovered["matches"][0]["pointer"] == "/materialization_hint/components/0"
    source_provenance = operator.get_collection_artifact_provenance(collection_id, SOURCE_ID)
    assert source_provenance["binding"]["journal"]["journal_id"] == journal_summary.journal_id
    listed_journals = operator.list_collection_provenance_journals(collection_id, page_size=100)
    assert [row["journal_id"] for row in listed_journals["journals"]] == [
        journal_summary.journal_id
    ]
    with operator.stream_collection_provenance_journal(
        collection_id, journal_summary.journal_id
    ) as chunks:
        assert b"".join(chunks) == journal
    metadata = operator.collection_provenance_journal_metadata(
        collection_id, journal_summary.journal_id
    )
    assert metadata == (len(journal), hashlib.sha256(journal).hexdigest())
    operator.download_collection_provenance_journal(
        collection_id, journal_summary.journal_id, output=tmp_path / "source.jsonseq"
    )
    assert (tmp_path / "source.jsonseq").read_bytes() == journal
    checkpoint = operator.create_catalog_sync_checkpoint()
    catalog = operator.list_catalog_sync_collections(checkpoint.catalog_cursor, limit=100)
    assert [item.collection_id for item in catalog.collections] == [collection_id]
    assert catalog.changes_cursor is not None
    changes = operator.list_catalog_sync_changes(catalog.changes_cursor, limit=100)
    assert changes.caught_up is True
    inventory = operator.get_portable_collection_inventory(collection_id)
    assert inventory.authority.header.collection == collection_id
    assert len(inventory.artifacts) == 1
    assert inventory.complete is True

    assert {
        item["store"]
        for item in operator.list_archive_stores(page_size=100, page_token=None)["stores"]
    } == {"primary", "secondary"}
    assert operator.get_archive_store("primary")["store"] == "primary"
    assert operator.retrieval_cache_status()["configured"] is False
    assert operator.list_retrieval_cache_objects(page_size=100, page_token=None)["objects"] == []

    plan = operator.plan_retrieval([(collection_id, SOURCE_ID)], restore_policy="never")
    plan = operator.advance_retrieval_plan(str(plan["id"]))
    assert plan["state"] == "ready"
    plan_files = operator.list_retrieval_plan_artifacts(
        str(plan["id"]),
        plan_etag=str(plan["etag"]),
    )
    assert plan_files["complete"] is True
    assert [current["artifact_id"] for current in plan_files["artifacts"]] == [str(SOURCE_ID)]
    job = operator.create_retrieval_job(
        str(plan["id"]),
        plan_etag=str(plan["etag"]),
    )
    job_id = str(job["id"])
    assert operator.get_retrieval_job(job_id)["state"] == "ready"
    assert operator.renew_retrieval_job(job_id, lease_seconds=3600)["state"] == "ready"
    head = transport.head(
        f"/v1/retrieval-jobs/{job_id}/content",
        params={"collection_id": collection_id, "artifact_id": str(SOURCE_ID)},
        headers={**operator_headers, "If-Match": f'"{binding.sha256}"'},
    )
    assert head.status_code == 200
    assert head.headers["etag"] == f'"{binding.sha256}"'
    partial = transport.get(
        f"/v1/retrieval-jobs/{job_id}/content",
        params={"collection_id": collection_id, "artifact_id": str(SOURCE_ID)},
        headers={
            **operator_headers,
            "If-Match": f'"{binding.sha256}"',
            "Range": "bytes=-4",
        },
    )
    assert partial.status_code == 206
    assert partial.content == source.read_bytes()[-4:]
    assert (
        partial.headers["content-range"]
        == f"bytes {int(binding.bytes) - 4}-{int(binding.bytes) - 1}/{int(binding.bytes)}"
    )
    stale = transport.get(
        f"/v1/retrieval-jobs/{job_id}/content",
        params={"collection_id": collection_id, "artifact_id": str(SOURCE_ID)},
        headers={**operator_headers, "If-Match": f'"{"0" * 64}"'},
    )
    assert stale.status_code == 412
    unsatisfiable = transport.get(
        f"/v1/retrieval-jobs/{job_id}/content",
        params={"collection_id": collection_id, "artifact_id": str(SOURCE_ID)},
        headers={
            **operator_headers,
            "If-Match": f'"{binding.sha256}"',
            "Range": f"bytes={int(binding.bytes)}-",
        },
    )
    assert unsatisfiable.status_code == 416
    output = tmp_path / "retrieved.txt"
    operator.download_retrieval_artifact(
        job_id,
        collection_id=collection_id,
        artifact_id=SOURCE_ID,
        output=output,
        expected_bytes=int(binding.bytes),
        expected_sha256=binding.sha256,
    )
    assert output.read_bytes() == source.read_bytes()
    assert operator.acknowledge_retrieval_job(job_id)["state"] == "completed"
    cancel_plan = operator.plan_retrieval([(collection_id, SOURCE_ID)], restore_policy="never")
    cancel_job = operator.create_retrieval_job(
        str(cancel_plan["id"]),
        plan_etag=str(cancel_plan["etag"]),
    )
    assert operator.cancel_retrieval_job(str(cancel_job["id"]))["state"] == "canceled"

    container.collection_uploads._archive_stores.require(
        "primary",
    ).store.new_archive_prefix = "archives/qualified-canceled"
    canceled_upload = operator.create_or_resume_collection_upload_session(
        "qualification-canceled-upload",
        initial_tag_set_identity=_tag_set_identity(),
    )
    assert (
        operator.cancel_collection_upload_session(int(canceled_upload["collection_id"]))["state"]
        == "canceled"
    )

    container.collection_uploads._archive_stores.require(
        "primary",
    ).store.new_archive_prefix = "archives/qualified-orphaned"
    orphaned_upload = operator.create_or_resume_collection_upload_session(
        "qualification-orphaned-upload",
        initial_tag_set_identity=_tag_set_identity(),
        custody_mode="custody-transfer",
    )
    orphaned_id = int(orphaned_upload["collection_id"])
    assert operator.heartbeat_collection_upload_session(orphaned_id)["state"] == "open"
    with session_scope(container.session_factory) as database:
        record = database.get(CollectionUploadRecord, orphaned_id)
        assert record is not None
        record.lease_expires_at = "2020-01-01T00:00:00.000000000Z"
    assert container.collection_uploads.reap_expired_custody_transfers() == 1
    discard_plan = operator.plan_collection_upload_discard(orphaned_id)
    assert discard_plan["status"] == "ready"
    assert (
        operator.discard_collection_upload(
            orphaned_id,
            challenge=str(discard_plan["challenge"]),
        )["status"]
        == "discarded"
    )

    copy = operator.create_or_resume_archive_copy_job(
        collection_id,
        destination_store="secondary",
        source_store="primary",
    )
    assert (
        operator.get_archive_copy_job(collection_id, destination_store="secondary")["state"]
        == "requested"
    )
    assert len(operator.list_archive_copy_jobs(page_size=100, page_token=None)["jobs"]) == 1
    assert (
        operator.cancel_archive_copy_job(collection_id, destination_store="secondary")["state"]
        == "canceled"
    )
    assert copy["destination_store"] == "secondary"

    source_collection = operator.get_collection(collection_id)
    source_identity = CollectionRootIdentity(
        collection_id=collection_id,
        archive_root_sha256=str(source_collection["archive_root_sha256"]),
        artifact_set_identity=str(source_collection["artifact_set_identity"]),
    )
    source_artifact = {
        "collection": source_identity.as_dict(),
        "artifact_id": str(SOURCE_ID),
        "bytes": str(int(binding.bytes)),
        "sha256": binding.sha256,
    }

    abandoned_work = {
        "format": "qualification-work/v1",
        "kind": "abandonment-witness",
        "inputs": [source_identity.as_dict()],
    }
    abandoned_work_id = canonical_json_sha256(abandoned_work)
    abandoned_claim = operator.create_or_resume_processing_claim(
        work_id=abandoned_work_id,
        work_document=abandoned_work,
        work_document_sha256=abandoned_work_id,
        inputs=[source_identity.as_dict()],
    )
    abandoned_claim_id = str(abandoned_claim["id"])
    abandoned_fence = int(abandoned_claim["fence"])
    read_capability = operator.create_processing_capability(
        abandoned_claim_id,
        fence=abandoned_fence,
        audience="qualification.observer/v1",
        actions=("read-inputs",),
        artifacts=(source_artifact,),
    )
    restarted = operator.restart_processing_claim(
        abandoned_claim_id,
        fence=abandoned_fence,
        lease_seconds=1800,
    )
    abandoned_fence = int(restarted["fence"])
    assert abandoned_fence == 2
    assert restarted["plan"] is None
    assert (
        transport.get(
            f"/v1/collections/{collection_id}",
            headers={"Authorization": f"Bearer {read_capability['token']}"},
        ).status_code
        == 401
    )
    abandonment_reason = "qualification: explicit terminal no-output work"
    abandoned = operator.abandon_processing_claim(
        abandoned_claim_id,
        fence=abandoned_fence,
        reason=abandonment_reason,
    )
    assert abandoned["state"] == "abandoned"
    assert abandoned["abandonment_reason"] == abandonment_reason
    assert (
        operator.abandon_processing_claim(
            abandoned_claim_id,
            fence=abandoned_fence,
            reason=abandonment_reason,
        )["state"]
        == "abandoned"
    )

    operation_contract = {
        "id": "qualification-transform/v1",
        "result_kind": "collection",
        "source_collection_retirement_permitted": True,
    }
    operation_identity = OperationIdentity(
        "qualification-transform/v1", canonical_json_sha256(operation_contract)
    )
    recipe_identity = RecipeIdentity(
        "qualification-recipe/v1",
        1,
        hashlib.sha256(b"qualification-recipe-contract").hexdigest(),
    )
    multi_output_work = {
        "format": "qualification-multi-output-work/v1",
        "inputs": [source_identity.as_dict()],
    }
    multi_output_work_id = canonical_json_sha256(multi_output_work)
    outcome_claim = operator.create_or_resume_processing_claim(
        work_id=multi_output_work_id,
        work_document=multi_output_work,
        work_document_sha256=multi_output_work_id,
        inputs=[source_identity.as_dict()],
        purpose="qualification-multi-output/v1",
    )
    outcome_claim_id = str(outcome_claim["id"])
    outcome_fence = int(outcome_claim["fence"])
    work_document = {
        "format": "qualification-work/v1",
        "recipe": recipe_identity.as_dict(),
        "inputs": [source_identity.as_dict()],
    }
    work_id = canonical_json_sha256(work_document)
    claim = operator.create_or_resume_processing_claim(
        work_id=work_id,
        work_document=work_document,
        work_document_sha256=work_id,
        inputs=[source_identity.as_dict()],
    )
    claim_id = str(claim["id"])
    claim_fence = int(claim["fence"])
    assert operator.get_processing_claim(claim_id)["work_id"] == work_id
    assert len(operator.list_processing_claims(page_size=100, page_token=None).claims) == 3
    assert (
        operator.renew_processing_claim(
            claim_id,
            fence=claim_fence,
            lease_seconds=1800,
        )["state"]
        == "active"
    )

    execution_id = hashlib.sha256(b"qualification-execution-envelope").hexdigest()
    controller_evidence = {
        "format": "qualification-controller-evidence/v1",
        "claim": {"id": claim_id, "fence": claim_fence},
        "work_id": work_id,
        "execution_id": execution_id,
    }
    controller_evidence_sha256 = canonical_json_sha256(controller_evidence)
    sealed = operator.seal_processing_claim_plan(
        claim_id,
        fence=claim_fence,
        execution_id=execution_id,
        controller_evidence=controller_evidence,
        controller_evidence_sha256=controller_evidence_sha256,
        operation_id=operation_identity.id,
        operation_sha256=operation_identity.sha256,
        operation_contract=operation_contract,
        input_artifacts=(source_artifact,),
        source_collection_retirement_policy="retain",
    )
    assert sealed["plan"]["execution_id"] == execution_id
    sealed_plan = sealed["plan"]
    observation_request_body = {
        "work_id": work_id,
        "observer_contract_id": "qualification.consideration/v1",
        "observer_contract_sha256": "6" * 64,
        "subjects": [source_artifact],
    }
    observation_request = {
        **observation_request_body,
        "request_id": canonical_json_sha256(observation_request_body),
    }
    observation_facts = {"considered": True}
    observation_result_body = {
        "request_id": observation_request["request_id"],
        "observer_contract_id": observation_request_body["observer_contract_id"],
        "observer_contract_sha256": observation_request_body["observer_contract_sha256"],
        "subjects": [source_artifact],
        "facts_schema": {"profile_sha256": "5" * 64},
        "facts": observation_facts,
        "facts_sha256": canonical_json_sha256(observation_facts),
        "state": "observed",
    }
    observation_document = {
        "request": observation_request,
        "result": {
            **observation_result_body,
            "result_sha256": canonical_json_sha256(observation_result_body),
        },
    }
    observation_sha256 = canonical_json_sha256(observation_document)
    retained_observation = operator.record_processing_claim_consideration_evidence(
        claim_id,
        fence=claim_fence,
        document=observation_document,
        sha256=observation_sha256,
    )
    assert retained_observation.sha256 == observation_sha256
    assert (
        operator.get_processing_claim_consideration_evidence(claim_id, observation_sha256).document
        == observation_document
    )
    assert (
        operator.list_processing_claim_inputs(
            claim_id,
            identity_sha256=str(sealed["inputs"]["identity"]["sha256"]),
        )
        .inputs[0]
        .collection_id
        == collection_id
    )
    assert (
        operator.list_processing_claim_artifacts(
            claim_id,
            identity_sha256=str(sealed_plan["artifacts"]["sha256"]),
        )
        .artifacts[0]
        .artifact_id
        == SOURCE_ID
    )
    payload_capability = operator.create_processing_capability(
        claim_id,
        fence=claim_fence,
        audience="qualification.payload-only/v1",
        actions=("read-inputs", "write-output"),
        artifacts=(source_artifact,),
    )
    payload_reader = _api(transport, str(payload_capability["token"]), observer=observer)
    with pytest.raises(Forbidden, match="provenance:read"):
        payload_reader.get_collection_artifact_provenance(collection_id, SOURCE_ID)
    payload_reader.close()
    output_capability = operator.create_processing_capability(
        claim_id,
        fence=claim_fence,
        audience="qualification.target/v1",
        actions=("read-inputs", "read-provenance", "write-output"),
        artifacts=(source_artifact,),
    )
    target = _api(transport, str(output_capability["token"]), observer=observer, workers=container)

    output_payload_path = tmp_path / "target-workspace" / "disposable.tmp"
    output_payload_path.parent.mkdir()
    output_payload_path.write_bytes(source.read_bytes().upper())
    disposition = ArtifactDisposition(
        input_collection_id=collection_id,
        input_archive_root_sha256=source_identity.archive_root_sha256,
        input_artifact_id=SOURCE_ID,
        status="transformed",
    )
    disposition_output = ArtifactDispositionOutput(
        input_collection_id=collection_id,
        input_archive_root_sha256=source_identity.archive_root_sha256,
        input_artifact_id=SOURCE_ID,
        output_artifact_id=OUTPUT_ID,
    )
    operator.record_processing_claim_dispositions(
        claim_id,
        fence=claim_fence,
        dispositions=(disposition.as_dict(),),
    )
    operator.record_processing_claim_disposition_outputs(
        claim_id,
        fence=claim_fence,
        outputs=(disposition_output.as_dict(),),
    )
    disposition_state = operator.seal_processing_claim_dispositions(
        claim_id,
        fence=claim_fence,
    )
    while disposition_state.state != "sealed":
        assert disposition_state.state == "sealing"
        assert container.collection_workflows.process_due_disposition_sets() == 1
        disposition_state = operator.get_processing_claim_dispositions(claim_id)
    assert disposition_state.identity is not None
    disposition_identity = ArtifactDispositionSetIdentity.from_mapping(
        disposition_state.identity.model_dump(mode="json")
    )
    disposition_page = target.list_processing_claim_dispositions(
        claim_id,
        identity_sha256=disposition_identity.sha256,
    )
    assert len(disposition_page.dispositions) == 1
    output_page = target.list_processing_claim_disposition_outputs(
        claim_id,
        identity_sha256=disposition_identity.sha256,
    )
    assert len(output_page.outputs) == 1
    assert len(target.list_collection_artifact_provenance(collection_id)["artifacts"]) == 1
    with target.stream_collection_provenance_journal(
        collection_id, journal_summary.journal_id
    ) as chunks:
        assert b"".join(chunks) == journal
    target_retrieval_plan = target.plan_retrieval(
        [(collection_id, SOURCE_ID)], restore_policy="never"
    )
    target_retrieval = target.create_retrieval_job(
        str(target_retrieval_plan["id"]), plan_etag=str(target_retrieval_plan["etag"])
    )
    assert target_retrieval["state"] == "ready"
    assert target.acknowledge_retrieval_job(str(target_retrieval["id"]))["state"] == "completed"
    for key, source_name, store in (
        (hashlib.sha256(b"unauthorized-output").hexdigest(), f"processing:{execution_id}", None),
        (execution_id, "processing:another-execution", None),
        (execution_id, f"processing:{execution_id}", "primary"),
    ):
        with pytest.raises(Forbidden):
            target.create_or_resume_collection_upload_session(
                key,
                initial_tag_set_identity=_tag_set_identity(),
                ingest_source=source_name,
                archive_store=store,
            )

    source_reader = ClaimedCollectionReader(
        target, inputs=(source_identity,), work_id=work_id, claim_id=claim_id, fence=claim_fence
    )
    input_history = source_reader.provenance(
        ClaimedArtifact(source_identity, SOURCE_ID, int(binding.bytes), binding.sha256)
    )
    spec = DerivedCollectionSpec(
        inputs=(source_identity,), recipe=recipe_identity, operation=operation_identity
    )
    container.collection_uploads._archive_stores.require(
        "primary"
    ).store.new_archive_prefix = "archives/qualified-output"

    def writer():
        return IncrementalDerivedCollectionWriter(
            target,
            spec=spec,
            claim_id=claim_id,
            fence=claim_fence,
            work_id=work_id,
            execution_id=execution_id,
            controller_evidence=controller_evidence,
            producer_app="qualification.target/v1",
            producer_version="1.0.0",
            execution_envelope_sha256=execution_id,
        )

    identity = ProducerArtifactIdentity(
        OUTPUT_ID,
        output_payload_path.stat().st_size,
        hashlib.sha256(output_payload_path.read_bytes()).hexdigest(),
    )
    early = writer()
    output_collection_id = early.producer.collection_id
    try:
        early.append(
            ProducerFile(
                output_payload_path,
                OUTPUT_ID,
                allow_missing_materialization_hint=True,
                output_id="output-1",
            ),
            identity=identity,
            output_id="output-1",
            source_histories=(input_history,),
            history_extent=BOUND_HISTORY_EXTENT,
        )
        for _ in range(64):
            container.collection_uploads.process_due_custody_receipts()
            receipts = early.producer.reconcile_custody()
            if receipts:
                break
        else:
            raise AssertionError("early output did not reach verified canonical custody")
        primary = target.get_collection_upload_session_artifact_provenance_binding(
            output_collection_id, OUTPUT_ID
        )
        with target.stream_collection_upload_session_provenance_journal(
            output_collection_id, primary.journal.journal_id
        ) as chunks:
            early_primary_bytes = b"".join(chunks)
        state = target.get_collection_upload_session(output_collection_id)
        assert state["state"] == "open" and state["archive_root_sha256"] is None
        assert state["completion_journal_id"] is None
        assert state["custody"] == {"state": "complete"}
        assert receipts[0].receipt.completion_requirement_sha256 == early.requirement.identity
    finally:
        early.stop()
    # A safe receipt permits deletion of disposable local bytes. Resume must not reread them.
    output_payload_path.unlink()
    execution_preimage = (
        b'{"format":"qualification-target-execution/v1","optional":null,'
        b'"quality":1.2300,"state":"succeeded"}'
    )
    output_declaration = {
        "output_id": "output-1",
        "artifact_id": str(OUTPUT_ID),
        "bytes": str(identity.bytes),
        "sha256": identity.sha256,
    }
    preimages = {
        "implementation": canonical_json_bytes(
            {"implementation_id": "qualification.target/v1", "implementation_version": "1.0.0"}
        ),
        "invocation": canonical_json_bytes(
            {
                "work_id": work_id,
                "operation": operation_identity.as_dict(),
                "input": source_artifact,
            }
        ),
        "target-execution": execution_preimage,
        "target-output-declarations": canonical_json_bytes({"outputs": [output_declaration]}),
        "target-result": canonical_json_bytes(
            {"state": "succeeded", "outputs": [output_declaration]}
        ),
    }

    def completion_record(kind, raw):
        return CompletionRecord(kind, len(raw), hashlib.sha256(raw).hexdigest(), lambda: (raw,))

    resumed_writer = writer()
    try:
        resumed_receipt = resumed_writer.producer.resume_artifact_custody(identity)
        assert resumed_receipt is not None and resumed_receipt.receipt == receipts[0].receipt
        assert (
            target.get_collection_upload_session_artifact_provenance_binding(
                output_collection_id, OUTPUT_ID
            )
            == primary
        )
        receipt = resumed_writer.finish(
            execution_sha256=hashlib.sha256(execution_preimage).hexdigest(),
            disposition_set=disposition_identity,
            completion_records=tuple(
                completion_record(kind, raw) for kind, raw in preimages.items()
            ),
            poll_seconds=0.05,
            timeout_seconds=120,
        )
        derivation = receipt.derivation
    finally:
        resumed_writer.stop()
    published_retry = writer()
    try:
        assert published_retry.producer.collection_id == output_collection_id
        # Publication retires the upload projection; the final root receipt resumes completion.
        with pytest.raises(NotFound):
            target.get_collection_upload_session_artifact(output_collection_id, OUTPUT_ID)
        replayed_output = published_retry.finish(
            execution_sha256=hashlib.sha256(execution_preimage).hexdigest(),
            disposition_set=disposition_identity,
            completion_records=tuple(
                completion_record(kind, raw) for kind, raw in preimages.items()
            ),
        )
        assert replayed_output == receipt
        with pytest.raises(ValueError, match="changes accepted evidence"):
            published_retry.finish(
                execution_sha256=hashlib.sha256(execution_preimage).hexdigest(),
                disposition_set=disposition_identity,
                completion_records=tuple(
                    completion_record(kind, raw + b" " if kind == "invocation" else raw)
                    for kind, raw in preimages.items()
                ),
            )
        with pytest.raises(ValueError, match="exact sealed preimage"):
            published_retry.finish(
                execution_sha256="f" * 64,
                disposition_set=disposition_identity,
                completion_records=tuple(
                    completion_record(kind, raw) for kind, raw in preimages.items()
                ),
            )
    finally:
        published_retry.stop()
    final_output = operator.get_collection_artifact_provenance(output_collection_id, OUTPUT_ID)
    assert final_output["binding"] == primary.model_dump(mode="json")
    with operator.stream_collection_provenance_journal(
        output_collection_id, primary.journal.journal_id
    ) as chunks:
        assert b"".join(chunks) == early_primary_bytes
    archived_journal_ids = {
        row["journal_id"]
        for row in operator.list_collection_provenance_journals(
            output_collection_id, page_size=100
        )["journals"]
    }
    archive_reader = container.provenance._archives.reader(output_collection_id)
    history_binding = MemberHistoryBinding.from_mapping(final_output["history_binding"])
    history_store = archive_reader.history_store()
    selected_roots = tuple(history_store.roots(history_binding, extent=BOUND_HISTORY_EXTENT))
    assert len(selected_roots) == 2
    completion_anchor = next(
        root.journal
        for root in selected_roots
        if root.journal.journal_id != primary.journal.journal_id
    )
    completion_journal_id = completion_anchor.journal_id
    assert archived_journal_ids == {
        journal_summary.journal_id,
        primary.journal.journal_id,
        completion_journal_id,
    }
    with MemberHistoryClosure(
        history_store,
        lambda journal_id, size: archive_reader.iter_journal_range(journal_id, size=size),
        member_role=COLLECTION_MEMBER_ROLE,
    ) as closure:
        closure.resolve(history_binding, extent=BOUND_HISTORY_EXTENT)
        assert {anchor.journal_id for anchor in closure.journal_anchors()} == archived_journal_ids
        completion = closure.summary_at(completion_anchor)
        assert completion.graph["activities"][0]["kind"] == "recording"
        assert not completion.graph.get("relations")
        with CollectionRecordPreimages(
            completion.graph["extensions"],
            subject=reference(completion.graph["activities"][0]["id"], "activity"),
        ) as retained:
            retained.validate(expected_kinds=resumed_writer.requirement.record_kinds)
            for kind, raw in preimages.items():
                assert b"".join(retained.chunks(kind)) == raw
    recovery_archive = tmp_path / "offline-completed-operation" / "archive"
    recovery_archive.mkdir(parents=True)
    with session_scope(container.session_factory) as session:
        copy = session.scalar(
            select(CollectionArchiveCopyRecord).where(
                CollectionArchiveCopyRecord.collection_id == output_collection_id,
                CollectionArchiveCopyRecord.store == "primary",
            )
        )
        assert copy is not None and copy.archive_storage_prefix is not None
        prefix = copy.archive_storage_prefix + "/"
    store = container.collection_uploads._archive_stores.require("primary").store
    assert isinstance(store, UploadedArchiveStore)
    for path, raw in store.objects.items():
        if path.startswith(prefix):
            destination = recovery_archive / path.removeprefix(prefix)
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(raw)
    expected_journals = {}
    for journal_id in archived_journal_ids:
        with operator.stream_collection_provenance_journal(
            output_collection_id, journal_id
        ) as chunks:
            raw = b"".join(chunks)
        expected_journals[hashlib.sha256(raw).hexdigest()] = raw
    qualify_offline_history_recovery(
        recovery_archive,
        archive_root_sha256=operator.get_collection(output_collection_id)["archive_root_sha256"],
        passphrases=dict(RuntimeConfig.for_testing().archive_passphrases),
        artifact_id=str(OUTPUT_ID),
        payload_bytes=identity.bytes,
        payload_sha256=identity.sha256,
        primary=early_primary_bytes,
        journals=expected_journals,
    )
    target.close()

    conflicting_derivation = {**derivation.as_dict(), "execution_sha256": "f" * 64}
    with pytest.raises(Conflict, match="archived completion evidence"):
        operator.settle_processing_claim(
            claim_id,
            fence=claim_fence,
            output_collection_id=output_collection_id,
            derivation=conflicting_derivation,
            outcome_claim_id=outcome_claim_id,
            outcome_fence=outcome_fence,
            outcome_id="qualification-output",
        )

    settled = operator.settle_processing_claim(
        claim_id,
        fence=claim_fence,
        output_collection_id=output_collection_id,
        derivation=derivation.as_dict(),
        outcome_claim_id=outcome_claim_id,
        outcome_fence=outcome_fence,
        outcome_id="qualification-output",
    )
    assert settled["state"] == "settled"
    replayed_settlement = operator.settle_processing_claim(
        claim_id,
        fence=claim_fence,
        output_collection_id=output_collection_id,
        derivation=derivation.as_dict(),
        outcome_claim_id=outcome_claim_id,
        outcome_fence=outcome_fence,
        outcome_id="qualification-output",
    )
    assert replayed_settlement["state"] == "settled"
    assert (
        operator.get_collection_derivation(output_collection_id)["document_sha256"]
        == derivation.sha256
    )
    assert operator.release_processing_claim(claim_id, fence=claim_fence)["state"] == "released"
    outcomes = operator.get_processing_claim(outcome_claim_id)["outcomes"]
    assert outcomes["count"] == "1"
    assert outcomes["identity"] is None
    # A real typed-client/API round trip for a generic opaque external effect.
    effect_work = {"format": "qualification-effect-work/v1", "inputs": [source_identity.as_dict()]}
    effect_claim = operator.create_or_resume_processing_claim(
        work_id=canonical_json_sha256(effect_work),
        work_document=effect_work,
        work_document_sha256=canonical_json_sha256(effect_work),
        inputs=[source_identity.as_dict()],
    )
    effect_execution = hashlib.sha256(b"qualification-effect-execution").hexdigest()
    effect_contract = {
        "id": "qualification-effect/v1",
        "result_kind": "external-effect",
        "source_collection_retirement_permitted": True,
    }
    effect_evidence = {"execution": effect_execution, "controller": "qualification"}
    effect_claim = operator.seal_processing_claim_plan(
        effect_claim.id,
        fence=effect_claim.fence,
        execution_id=effect_execution,
        controller_evidence=effect_evidence,
        controller_evidence_sha256=canonical_json_sha256(effect_evidence),
        operation_id=str(effect_contract["id"]),
        operation_sha256=canonical_json_sha256(effect_contract),
        operation_contract=effect_contract,
        result_kind="external-effect",
        input_artifacts=(source_artifact,),
    )
    effect_plan = effect_claim.plan
    assert effect_plan is not None
    opaque_receipt = {
        "format": "qualification-effect-receipt/v1",
        "execution": effect_execution,
        "succeeded": True,
    }
    operator.record_processing_claim_dispositions(
        effect_claim.id,
        fence=effect_claim.fence,
        dispositions=[
            ArtifactDisposition(
                input_collection_id=source_identity.collection_id,
                input_archive_root_sha256=source_identity.archive_root_sha256,
                input_artifact_id=source_artifact["artifact_id"],
                status="effect-applied",
                effect_receipt_sha256=canonical_json_sha256(opaque_receipt),
            ).as_dict()
        ],
    )
    effect_dispositions = operator.seal_processing_claim_dispositions(
        effect_claim.id, fence=effect_claim.fence
    )
    while effect_dispositions.state == "sealing":
        assert container.collection_workflows.process_due_disposition_sets() == 1
        effect_dispositions = operator.get_processing_claim_dispositions(effect_claim.id)
    assert effect_dispositions.state == "sealed" and effect_dispositions.identity is not None
    effect_document = ExternalEffectSettlement(
        claim_id=effect_claim.id,
        fence=effect_claim.fence,
        execution_id=effect_execution,
        execution_sha256=hashlib.sha256(b"qualification-effect-attempt").hexdigest(),
        operation=OperationIdentity(
            str(effect_contract["id"]), canonical_json_sha256(effect_contract)
        ),
        input_set_sha256=effect_plan.inputs.sha256,
        artifact_set_sha256=effect_plan.artifacts.sha256,
        controller_evidence_sha256=canonical_json_sha256(effect_evidence),
        receipt=opaque_receipt,
        receipt_sha256=canonical_json_sha256(opaque_receipt),
        disposition_set=ArtifactDispositionSetIdentity.from_mapping(
            effect_dispositions.identity.model_dump(mode="json")
        ),
    )
    effect_settled = operator.settle_processing_claim_effect(
        effect_claim.id, fence=effect_claim.fence, settlement=effect_document.as_dict()
    )
    assert effect_settled.effect_settlement_sha256 == effect_document.sha256
    assert (
        operator.settle_processing_claim_effect(
            effect_claim.id, fence=effect_claim.fence, settlement=effect_document.as_dict()
        ).state
        == "settled"
    )
    operator.release_processing_claim(effect_claim.id, fence=effect_claim.fence)
    no_output_work = {
        "format": "qualification-no-output-work/v1",
        "inputs": [source_identity.as_dict()],
    }
    no_output_work_id = canonical_json_sha256(no_output_work)
    no_output_claim = operator.create_or_resume_processing_claim(
        work_id=no_output_work_id,
        work_document=no_output_work,
        work_document_sha256=no_output_work_id,
        inputs=[source_identity.as_dict()],
    )
    no_output_execution = hashlib.sha256(b"qualification-no-output-execution").hexdigest()
    no_output_operation = {
        "id": "qualification.no-output/v1",
        "result_kind": "no-output",
        "source_collection_retirement_permitted": False,
    }
    no_output_evidence = {"format": "qualification-no-output-controller/v1"}
    no_output_claim = operator.seal_processing_claim_plan(
        no_output_claim.id,
        fence=no_output_claim.fence,
        execution_id=no_output_execution,
        controller_evidence=no_output_evidence,
        controller_evidence_sha256=canonical_json_sha256(no_output_evidence),
        operation_id=str(no_output_operation["id"]),
        operation_sha256=canonical_json_sha256(no_output_operation),
        operation_contract=no_output_operation,
        result_kind="no-output",
        input_artifacts=(source_artifact,),
    )
    operator.record_processing_claim_dispositions(
        no_output_claim.id,
        fence=no_output_claim.fence,
        dispositions=[
            ArtifactDisposition(
                input_collection_id=source_identity.collection_id,
                input_archive_root_sha256=source_identity.archive_root_sha256,
                input_artifact_id=str(source_artifact["artifact_id"]),
                status="not-carried-forward",
                code="qualification.no-action/v1",
                message="No target output is needed.",
            ).as_dict()
        ],
    )
    no_output_dispositions = operator.seal_processing_claim_dispositions(
        no_output_claim.id, fence=no_output_claim.fence
    )
    while no_output_dispositions.state == "sealing":
        assert container.collection_workflows.process_due_disposition_sets() == 1
        no_output_dispositions = operator.get_processing_claim_dispositions(no_output_claim.id)
    assert no_output_claim.plan is not None and no_output_dispositions.identity is not None
    no_output_decision = {"format": "qualification-no-output-decision/v1", "result": "done"}
    no_output_document = NoOutputSettlement(
        claim_id=no_output_claim.id,
        fence=no_output_claim.fence,
        execution_id=no_output_execution,
        operation=OperationIdentity(
            str(no_output_operation["id"]), canonical_json_sha256(no_output_operation)
        ),
        input_set_sha256=no_output_claim.plan.inputs.sha256,
        artifact_set_sha256=no_output_claim.plan.artifacts.sha256,
        controller_evidence_sha256=canonical_json_sha256(no_output_evidence),
        decision=no_output_decision,
        decision_sha256=canonical_json_sha256(no_output_decision),
        disposition_set=ArtifactDispositionSetIdentity.from_mapping(
            no_output_dispositions.identity.model_dump(mode="json")
        ),
    )
    no_output_settled = operator.settle_processing_claim_no_output(
        no_output_claim.id,
        fence=no_output_claim.fence,
        settlement=no_output_document.as_dict(),
    )
    assert no_output_settled.no_output_settlement_sha256 == no_output_document.sha256
    operator.release_processing_claim(no_output_claim.id, fence=no_output_claim.fence)
    effect_outcome = CollectionProcessingOutcomeIdentity(
        outcome_id="qualification-effect",
        source_claim_id=effect_claim.id,
        source_fence=effect_claim.fence,
        execution_id=effect_execution,
        result_kind="external-effect",
        effect_receipt_sha256=effect_document.receipt_sha256,
        effect_settlement_sha256=effect_document.sha256,
    )
    operator.append_processing_claim_outcomes(
        outcome_claim_id, fence=outcome_fence, outcomes=(effect_outcome.as_dict(),)
    )
    output_root_identity = operator.get_collection(output_collection_id)
    collection_outcome = CollectionProcessingOutcomeIdentity(
        outcome_id="qualification-output",
        source_claim_id=claim_id,
        source_fence=claim_fence,
        execution_id=execution_id,
        result_kind="collection",
        derivation_sha256=derivation.sha256,
        output_collection=CollectionRootIdentity(
            output_collection_id,
            str(output_root_identity["archive_root_sha256"]),
            str(output_root_identity["artifact_set_identity"]),
        ),
    )
    required_outcomes = processing_outcome_set_identity((effect_outcome, collection_outcome))
    settled_outcomes = operator.settle_processing_claim_outcomes(
        outcome_claim_id,
        fence=outcome_fence,
        outcomes_count=2,
        outcomes_sha256=str(required_outcomes["sha256"]),
        source_collection_retirement_policy="retire-after-settlement",
    )
    while settled_outcomes["state"] == "active":
        assert container.collection_workflows.process_due_outcome_sets() == 1
        settled_outcomes = operator.settle_processing_claim_outcomes(
            outcome_claim_id,
            fence=outcome_fence,
            outcomes_count=2,
            outcomes_sha256=str(required_outcomes["sha256"]),
            source_collection_retirement_policy="retire-after-settlement",
        )
    assert settled_outcomes["state"] == "settled"
    assert settled_outcomes["outcomes"]["identity"] is not None
    outcome_authority = settled_outcomes["outcomes"]["identity"]
    assert (
        operator.list_processing_claim_outcomes(
            outcome_claim_id,
            identity_sha256=str(outcome_authority["sha256"]),
        )
        .outcomes[0]
        .outcome_id
        == "qualification-effect"
    )
    retiring = operator.begin_source_collection_retirement(
        outcome_claim_id,
        fence=outcome_fence,
    )
    assert retiring["state"] == "retiring"
    replayed_outcomes = operator.settle_processing_claim_outcomes(
        outcome_claim_id,
        fence=outcome_fence,
        outcomes_count=2,
        outcomes_sha256=str(required_outcomes["sha256"]),
        source_collection_retirement_policy="retire-after-settlement",
    )
    assert replayed_outcomes["state"] == "retiring"
    retirement = operator.plan_collection_deletion(
        collection_id,
        source_collection_retirement_claim_id=outcome_claim_id,
    )
    assert retirement["status"] == "ready"
    assert (
        operator.delete_collection(
            collection_id,
            challenge=str(retirement["challenge"]),
            source_collection_retirement_claim_id=outcome_claim_id,
        )["status"]
        == "deleting"
    )
    while container.collection_deletions.process_due(limit=1):
        pass
    # Input retirement must leave the derivative's exact accepted source history intact.
    assert {
        row["journal_id"]
        for row in operator.list_collection_provenance_journals(
            output_collection_id, page_size=100
        )["journals"]
    } == archived_journal_ids
    with operator.stream_collection_provenance_journal(
        output_collection_id, journal_summary.journal_id
    ) as chunks:
        assert b"".join(chunks) == journal
    assert (
        operator.release_processing_claim(
            outcome_claim_id,
            fence=outcome_fence,
        )["state"]
        == "released"
    )
    replayed_released_settlement = operator.settle_processing_claim_outcomes(
        outcome_claim_id,
        fence=outcome_fence,
        outcomes_count=2,
        outcomes_sha256=str(required_outcomes["sha256"]),
        source_collection_retirement_policy="retire-after-settlement",
    )
    assert replayed_released_settlement["state"] == "released"

    events = operator.list_lifecycle_events(limit=100)
    assert events.events
    assert int(events.next_cursor) > 0
    resumed_events = operator.list_lifecycle_events(after=events.next_cursor, limit=100)
    assert resumed_events.events == []
    assert resumed_events.next_cursor == events.next_cursor
    restarted_container = _container(tmp_path)
    restarted_transport = TestClient(create_app(container=restarted_container))
    restarted_operator = _api(restarted_transport, operator_token)
    restarted_events = restarted_operator.list_lifecycle_events(
        after=events.next_cursor,
        limit=100,
    )
    assert restarted_events.events == []
    assert restarted_events.next_cursor == events.next_cursor
    restarted_transport.close()
    restarted_container.close()
    from scripts.operation_qualification import operation_matrix

    observer.require(
        operation.operation_id
        for operation in operation_matrix()
        if operation.application == "riverhog" and operation.provider_evidence is None
    )

    transport.close()
    container.close()
