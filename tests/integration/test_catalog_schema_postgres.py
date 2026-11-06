from __future__ import annotations

import hashlib
import os
import re
import threading
import weakref
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

import pytest
from psycopg import sql as pgsql
from riverhog_age import CHUNK_SIZE
from riverhog_core.app_permissions import (
    ALL_RESOURCES,
    CATALOG_READ,
    COLLECTIONS_CREATE,
    ApplicationAccess,
    Principal,
)
from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.catalog_db import (
    catalog_state_schema,
    create_catalog_engine,
    dispose_session_factory,
    initialize_db,
    make_session_factory,
    session_scope,
    validate_db,
)
from riverhog_core.catalog_events import record_catalog_event
from riverhog_core.catalog_models import (
    CatalogEventRecord,
    CatalogSyncStateRecord,
    CollectionArchiveCopyRecord,
    CollectionArchiveObjectUploadRecord,
    CollectionDescriptionPublicationRecord,
    CollectionMutableDocumentPublicationAttemptRecord,
    CollectionMutableDocumentReclamationRecord,
    CollectionRecord,
    CollectionTagMutationRecord,
    CollectionTagNodeGcRecord,
    CollectionTagNodeReclamationRecord,
    CollectionTagNodeRecord,
    CollectionTagPublicationFrontierRecord,
    CollectionTagPublicationRecord,
    CollectionTagRevisionRecord,
    CollectionUploadProvenanceArchiveVolumeRecord,
    CollectionUploadRecord,
    RetrievalPlanObjectRecord,
    RetrievalPlanPlacementRecord,
)
from riverhog_core.collection_plan import CollectionVolumePolicy
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services import collection_tags as collection_tags_module
from riverhog_core.services.catalog_sync import (
    SqlAlchemyCatalogSyncService,
    _reap_unreferenced_tag_history,
)
from riverhog_core.services.collection_descriptions import (
    SqlAlchemyCollectionDescriptionService,
)
from riverhog_core.services.collection_tags import SqlAlchemyCollectionTagService
from riverhog_core.services.collection_uploads import (
    SqlAlchemyCollectionUploadService,
    _record_payload_custody_progress,
)
from riverhog_core.services.retrieval import SqlAlchemyRetrievalService
from riverhog_protocol import (
    COLLECTION_DESCRIPTION_UTF8_BYTES_MAX,
    MAX_COLLECTION_DESCRIPTION_REVISION,
    CollectionUploadRawDigestBatchDocument,
    collection_description_identity,
)
from riverhog_protocol.errors import ServiceUnavailable
from riverhog_protocol.raw_ingress import ordered_raw_part_commitment
from sqlalchemy import event, inspect, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError

from tests.unit.archive_object_fixtures import (
    MemoryArchiveStore,
    archive_store_binding,
)
from tests.unit.storage_incarnation_fixtures import seed_storage_incarnation
from tests.unit.test_collection_descriptions import PRINCIPAL as description_principal
from tests.unit.test_collection_descriptions import (
    DelayedDescriptionStore,
    VersionedDescriptionStore,
)
from tests.unit.test_collection_descriptions import (
    _seed as description_seed,
)
from tests.unit.test_collection_tags import (
    _AmbiguousTagDeleteStore,
    _assert_delayed_old_tag_gc_result_cannot_change_successor,
    _assert_unrelated_head_advance_preserves_interrupted_tag_gc,
)
from tests.unit.test_collection_tags import _principal as tag_principal
from tests.unit.test_collection_tags import _service as tag_service
from tests.unit.test_retrieval_service import _seed_collection

pytestmark = pytest.mark.integration
V1_FIXTURE = Path(__file__).resolve().parents[1] / "fixtures/state/v1_0001/riverhog.postgresql.sql"


@pytest.fixture
def isolated_database_url() -> Iterator[str]:
    value = os.getenv("RIVERHOG_TEST_POSTGRES_URL", "").strip()
    if not value:
        pytest.skip("RIVERHOG_TEST_POSTGRES_URL is required")
    schema = f"riverhog_catalog_{uuid4().hex}"
    admin_engine = create_catalog_engine(value)
    with admin_engine.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    url = make_url(value).update_query_dict({"options": f"-csearch_path={schema},public"})
    try:
        yield url.render_as_string(hide_password=False)
    finally:
        with admin_engine.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin_engine.dispose()


def test_postgres_catalog_schema_is_current_and_stays_operator_controlled(
    isolated_database_url: str,
) -> None:
    upgraded = initialize_db(isolated_database_url)
    engine = create_catalog_engine(isolated_database_url)
    before = {index["name"] for index in inspect(engine).get_indexes("retrieval_jobs")}

    validated = validate_db(isolated_database_url)

    after = {index["name"] for index in inspect(engine).get_indexes("retrieval_jobs")}
    assert upgraded.condition == validated.condition == "current"
    assert upgraded.current_revision == validated.current_revision == "v1_0001"
    assert after == before
    engine.dispose()


def test_postgres_current_v1_fixture_validates_and_restarts(
    isolated_database_url: str,
) -> None:
    engine = create_catalog_engine(isolated_database_url)
    fixture_sql = V1_FIXTURE.read_text(encoding="utf-8")
    with engine.begin() as connection:
        for statement in fixture_sql.split(";\n"):
            if statement.strip():
                connection.exec_driver_sql(statement)
    status = catalog_state_schema(isolated_database_url).validate()
    engine.dispose()

    restarted = create_catalog_engine(isolated_database_url)
    with restarted.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO collection_tags "
                "(tag_sha256, tag, search_text, created_at, updated_at, collection_count) "
                "VALUES (:id, 'fixture', 'fixture', "
                "'2026-01-01T00:00:00.000000000Z', "
                "'2026-01-01T00:00:00.000000000Z', 0)"
            ),
            {"id": "0" * 64},
        )
    restarted.dispose()

    assert status.condition == "current"
    assert status.current_revision == "v1_0001"
    assert validate_db(isolated_database_url).condition == "current"


def test_postgres_retrieval_plan_advances_in_bounded_restartable_steps(
    isolated_database_url: str,
    tmp_path: Path,
) -> None:
    segment_count = 65
    content = bytes(index % 251 for index in range(segment_count * CHUNK_SIZE))
    service, collection_id, _ranges, _store = _seed_collection(
        tmp_path,
        {"1" * 64: content},
        database_url=isolated_database_url,
        raw=True,
        raw_volume_plaintext_bytes=CHUNK_SIZE,
        raw_part_plaintext_bytes=CHUNK_SIZE,
    )

    plan = service.plan(((collection_id, "1" * 64),))
    assert plan["state"] == "planning"
    with session_scope(service._session_factory) as session:
        assert len(session.scalars(select(RetrievalPlanObjectRecord)).all()) == 32
        assert len(session.scalars(select(RetrievalPlanPlacementRecord)).all()) == 32

    restarted = SqlAlchemyRetrievalService(
        service._config,
        service._archive_stores,
        service._cache,
        session_factory=make_session_factory(isolated_database_url),
    )
    plan = restarted.advance_plan(principal_id="", plan_id=str(plan["id"]))
    assert plan["state"] == "planning"
    plan = restarted.advance_plan(principal_id="", plan_id=str(plan["id"]))
    assert plan["state"] == "ready"
    with session_scope(make_session_factory(isolated_database_url)) as session:
        assert len(session.scalars(select(RetrievalPlanObjectRecord)).all()) == segment_count
        assert len(session.scalars(select(RetrievalPlanPlacementRecord)).all()) == segment_count


def test_postgres_upload_idempotency_is_independent_per_principal(
    isolated_database_url: str,
) -> None:
    initialize_db(isolated_database_url)
    with session_scope(make_session_factory(isolated_database_url)) as session:
        seed_storage_incarnation(session, "archive", "archive")
    access = frozenset({ApplicationAccess(COLLECTIONS_CREATE, ALL_RESOURCES)})
    memory_store = MemoryArchiveStore()
    archive_stores = ArchiveStoreRegistry({"archive": archive_store_binding(memory_store)})

    def create(app: str, key_id: str) -> dict[str, object]:
        service = SqlAlchemyCollectionUploadService(
            RuntimeConfig.for_testing(database_url=isolated_database_url),
            archive_stores,
        )
        return service.create_or_resume(
            idempotency_key="shared-retry-key",
            ingest_source="postgres-fixture",
            archive_store=None,
            initiator=Principal(id=app, key_id=key_id, access=access),
            event_context=None,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(create, "first", "first-key")
        second_future = executor.submit(create, "second", "second-key")
        first = first_future.result()
        second = second_future.result()

    rotated = create("first", "replacement-key")
    assert first["collection_id"] != second["collection_id"]
    assert rotated["collection_id"] == first["collection_id"]

    engine = create_catalog_engine(isolated_database_url)
    assert {
        tuple(str(column) for column in constraint["column_names"])
        for constraint in inspect(engine).get_unique_constraints("collections")
    } == {("created_by_principal_id", "creation_idempotency_key")}
    indexes = {
        str(index["name"]): index for index in inspect(engine).get_indexes("collection_uploads")
    }
    idempotency_index = indexes["ux_collection_uploads_principal_idempotency_key"]
    assert idempotency_index["column_names"] == ["initiated_by_principal_id", "idempotency_key"]
    assert idempotency_index["unique"] is True
    engine.dispose()


def test_postgres_catalog_revisions_serialize_commit_and_restart(
    isolated_database_url: str,
) -> None:
    initialize_db(isolated_database_url)
    factory = make_session_factory(isolated_database_url)
    with session_scope(factory) as session:
        for collection_id in (1, 2):
            identity = f"{collection_id:064x}"
            session.add(
                CollectionRecord(
                    id=collection_id,
                    creation_idempotency_key=f"catalog-sync-{collection_id}",
                    creation_identity_sha256=identity,
                    creation_custody_mode="producer-retained",
                    archive_generation=identity,
                    artifact_set_identity=identity,
                    encryption_format="age-v1-scrypt",
                    passphrase_id="fixture",
                    provenance_identity=identity,
                    delivery_context_id=f"00000000-0000-4000-8000-{collection_id:012x}",
                    inventory_identity=identity,
                    archive_root_sha256=identity,
                    created_by_principal_id="fixture",
                    created_at="2026-09-07T00:00:00.000000000Z",
                    is_published=True,
                    artifact_count=0,
                    artifact_bytes=0,
                )
            )

    first_locked = threading.Event()
    second_started = threading.Event()
    release_first = threading.Event()

    def publish(collection_id: int) -> int:
        with session_scope(make_session_factory(isolated_database_url)) as session:
            if collection_id == 2:
                second_started.set()
            event = record_catalog_event(
                session,
                change="created",
                collection_id=collection_id,
                occurred_at="2026-09-07T00:00:00.000000000Z",
                inventory_identity=f"{collection_id:064x}",
                before_tags=(),
                after_tags=(),
            )
            if collection_id == 1:
                first_locked.set()
                assert release_first.wait(timeout=5)
            assert event.revision is not None
            return event.revision

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(publish, 1)
        assert first_locked.wait(timeout=5)
        second = executor.submit(publish, 2)
        assert second_started.wait(timeout=5)
        release_first.set()
        assert (first.result(timeout=5), second.result(timeout=5)) == (1, 2)

    with session_scope(make_session_factory(isolated_database_url)) as session:
        state = session.get(CatalogSyncStateRecord, 1)
        assert state is not None and state.committed_revision == 2
        assert list(
            session.scalars(
                select(CatalogEventRecord.revision).order_by(CatalogEventRecord.revision)
            )
        ) == [1, 2]

    restarted = SqlAlchemyCatalogSyncService(
        RuntimeConfig.for_testing(database_url=isolated_database_url),
        session_factory=make_session_factory(isolated_database_url),
    )
    reader = Principal(
        id="indexer",
        key_id="indexer-key",
        access=frozenset({ApplicationAccess(CATALOG_READ, ALL_RESOURCES)}),
    )
    checkpoint = restarted.checkpoint(principal=reader)
    page = restarted.collections(cursor=checkpoint.catalog_cursor, limit=100, principal=reader)
    assert [item.collection_id for item in page.collections] == [1, 2]


def test_postgres_tag_history_cleanup_serializes_its_row_work_budget(
    isolated_database_url: str,
) -> None:
    initialize_db(isolated_database_url)
    factory = make_session_factory(isolated_database_url)
    digests = tuple(f"{index:064x}" for index in range(1, 5))
    with session_scope(factory) as session:
        session.add_all(
            CollectionTagNodeRecord(
                digest=digest,
                encoded=f"node-{index}".encode(),
                created_at="2026-09-08T00:00:00.000000000Z",
            )
            for index, digest in enumerate(digests, start=1)
        )

    def reap_one() -> int:
        service = SqlAlchemyCatalogSyncService(
            RuntimeConfig.for_testing(
                database_url=isolated_database_url,
                catalog_sync_history_reap_batch_size=1,
            ),
            session_factory=make_session_factory(isolated_database_url),
        )
        return service.reap_expired_history()

    for expected_progress in range(2, 13, 2):
        with ThreadPoolExecutor(max_workers=2) as executor:
            assert list(executor.map(lambda _index: reap_one(), range(2))) == [0, 0]
        with session_scope(factory) as session:
            nodes = set(session.scalars(select(CollectionTagNodeRecord.digest)))
            claims = set(session.scalars(select(CollectionTagNodeReclamationRecord.node_digest)))
            progress = sum(
                (
                    1
                    if digest in nodes and digest in claims
                    else 2
                    if digest not in nodes and digest in claims
                    else 3
                    if digest not in nodes and digest not in claims
                    else 0
                )
                for digest in digests
            )
            assert progress == expected_progress
    assert not nodes
    assert not claims


def test_postgres_tag_mutation_protects_an_aba_root_before_its_first_commit(
    isolated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, factory, _store = tag_service(None, database_url=isolated_database_url)
    principal = tag_principal("source:camera", "workflow:archive")
    with session_scope(factory) as session:
        collection = session.get(CollectionRecord, 1)
        assert collection is not None
        initial_identity = collection.tag_set_identity
        initial_root = collection.tag_root_sha256
    added = service.add(
        1,
        tag="workflow:archive",
        operation_id="establish-postgres-aba-root",
        expected_revision=1,
        expected_tag_set_identity=initial_identity,
        principal=principal,
    )
    with session_scope(factory) as session:
        retired = session.get(CollectionTagRevisionRecord, (1, 1))
        assert retired is not None and retired.root_sha256 == initial_root
        retired.cleanup_started_at = "2026-01-01T00:00:00.000000000Z"

    entered = threading.Event()
    release = threading.Event()
    original_scope = collection_tags_module.session_scope
    paused = False

    @contextmanager
    def pause_before_commit(session_factory: object) -> Iterator[object]:
        nonlocal paused
        with original_scope(session_factory) as session:  # type: ignore[arg-type]
            yield session
            mutation = session.get(
                CollectionTagMutationRecord,
                (1, "reuse-postgres-aba-root"),
            )
            if not paused and mutation is not None and mutation.state == "pending":
                paused = True
                entered.set()
                assert release.wait(timeout=10)

    monkeypatch.setattr(collection_tags_module, "session_scope", pause_before_commit)
    with ThreadPoolExecutor(max_workers=1) as executor:
        pending = executor.submit(
            service.remove,
            1,
            tag="workflow:archive",
            operation_id="reuse-postgres-aba-root",
            expected_revision=2,
            expected_tag_set_identity=str(added["tag_set_identity"]),
            principal=principal,
        )
        assert entered.wait(timeout=10)
        with session_scope(factory) as session:
            metrics = _reap_unreferenced_tag_history(
                session,
                limit=100,
                cleanup_before="2026-02-01T00:00:00.000000000Z",
                cleanup_started_at="2026-02-01T00:00:00.000000000Z",
            )
            assert metrics.changed_rows > 0
        with session_scope(factory) as session:
            assert session.get(CollectionTagNodeRecord, initial_root) is not None
            assert session.get(CollectionTagNodeReclamationRecord, initial_root) is None
        release.set()
        result = pending.result(timeout=10)

    assert result["revision"] == 3
    page = service.list_collection(
        1,
        page_size=100,
        position=None,
        expected_revision=3,
        expected_tag_set_identity=str(result["tag_set_identity"]),
        principal=principal,
    )
    assert page["tags"] == ["source:camera"]


def test_postgres_superseded_document_cleanup_workers_claim_distinct_receipts(
    isolated_database_url: str,
) -> None:
    config, factory, _initial, _registry = description_seed(
        None,
        database_url=isolated_database_url,
    )
    store = VersionedDescriptionStore()
    stores = ArchiveStoreRegistry({"archive": archive_store_binding(store)})
    service = SqlAlchemyCollectionDescriptionService(
        config,
        stores,
        session_factory=factory,
    )
    identity = collection_description_identity(
        archive_root_sha256="5" * 64,
        revision=0,
        description=None,
    )
    for description in ("first", "second", "third"):
        result = service.replace(
            1,
            description=description,
            expected_identity=identity,
            principal=description_principal,
        )
        identity = str(result["description_identity"])

    with session_scope(factory) as session:
        assert session.query(CollectionMutableDocumentReclamationRecord).count() == 2
    with ThreadPoolExecutor(max_workers=2) as executor:
        assert sorted(executor.map(lambda _index: service.process_due(limit=1), range(2))) == [
            1,
            1,
        ]
    with session_scope(factory) as session:
        assert session.query(CollectionMutableDocumentReclamationRecord).count() == 0
    assert not store.retained_revisions


def test_postgres_primary_description_claim_excludes_replica_worker(
    isolated_database_url: str,
) -> None:
    config, factory, _initial, _registry = description_seed(
        None,
        database_url=isolated_database_url,
    )
    store = DelayedDescriptionStore()
    stores = ArchiveStoreRegistry({"archive": archive_store_binding(store)})
    service = SqlAlchemyCollectionDescriptionService(config, stores, session_factory=factory)
    initial_identity = collection_description_identity(
        archive_root_sha256="5" * 64,
        revision=0,
        description=None,
    )
    store.delay_next_description = True

    with ThreadPoolExecutor(max_workers=1) as executor:
        primary = executor.submit(
            service.replace,
            1,
            description="PostgreSQL primary owner",
            expected_identity=initial_identity,
            principal=description_principal,
        )
        assert store.started.wait(timeout=10)
        competing = SqlAlchemyCollectionDescriptionService(
            config,
            stores,
            session_factory=make_session_factory(isolated_database_url),
        )
        assert competing.process_due(limit=1) == 0
        assert len(store.description_documents) == 1
        store.resume.set()
        assert primary.result(timeout=10)["description"] == "PostgreSQL primary owner"

    with session_scope(factory) as session:
        assert session.query(CollectionMutableDocumentPublicationAttemptRecord).count() == 0


def test_postgres_replica_description_claim_excludes_primary_writer(
    isolated_database_url: str,
) -> None:
    config, factory, _initial, _registry = description_seed(
        None,
        database_url=isolated_database_url,
    )
    store = DelayedDescriptionStore()
    stores = ArchiveStoreRegistry({"archive": archive_store_binding(store)})
    service = SqlAlchemyCollectionDescriptionService(config, stores, session_factory=factory)
    initial_identity = collection_description_identity(
        archive_root_sha256="5" * 64,
        revision=0,
        description=None,
    )
    current = service.replace(
        1,
        description="Current",
        expected_identity=initial_identity,
        principal=description_principal,
    )
    with session_scope(factory) as session:
        publication = session.get(CollectionDescriptionPublicationRecord, (1, "archive"))
        assert publication is not None
        publication.state = "pending"
        publication.next_attempt_at = "2026-09-07T00:00:00.000000000Z"

    store.delay_next_description = True
    with ThreadPoolExecutor(max_workers=1) as executor:
        replica = executor.submit(service.process_due, limit=1)
        assert store.started.wait(timeout=10)
        with pytest.raises(ServiceUnavailable, match="no retained archive copy"):
            service.replace(
                1,
                description="Next",
                expected_identity=str(current["description_identity"]),
                principal=description_principal,
            )
        assert len(store.description_documents) == 2
        with session_scope(factory) as session:
            collection = session.get(CollectionRecord, 1)
            attempt = session.get(
                CollectionMutableDocumentPublicationAttemptRecord,
                (1, "archive", "description"),
            )
            assert collection is not None and collection.pending_description_revision == 2
            assert attempt is not None and attempt.document_revision == 1
            collection.description_next_attempt_at = "2026-09-07T00:00:00.000000000Z"
        store.resume.set()
        assert replica.result(timeout=10) == 1

    assert service.process_due(limit=1) == 1
    with session_scope(factory) as session:
        collection = session.get(CollectionRecord, 1)
        assert collection is not None
        assert collection.description == "Next"
        assert collection.description_revision == 2
        assert session.query(CollectionMutableDocumentPublicationAttemptRecord).count() == 0


def test_postgres_description_attempt_accepts_maximum_canonical_document(
    isolated_database_url: str,
) -> None:
    config, factory, store, registry = description_seed(
        None,
        database_url=isolated_database_url,
    )
    with session_scope(factory) as session:
        collection = session.get(CollectionRecord, 1)
        publication = session.get(CollectionDescriptionPublicationRecord, (1, "archive"))
        assert collection is not None and publication is not None
        collection.description_revision = MAX_COLLECTION_DESCRIPTION_REVISION - 1
        collection.description_identity = collection_description_identity(
            archive_root_sha256="5" * 64,
            revision=MAX_COLLECTION_DESCRIPTION_REVISION - 1,
            description=None,
        )
        expected_identity = collection.description_identity
    service = SqlAlchemyCollectionDescriptionService(
        config,
        registry,
        session_factory=factory,
    )
    result = service.replace(
        1,
        description='"' * COLLECTION_DESCRIPTION_UTF8_BYTES_MAX,
        expected_identity=expected_identity,
        principal=description_principal,
    )

    assert result["description_revision"] == MAX_COLLECTION_DESCRIPTION_REVISION
    assert store.objects


def test_postgres_mutable_replica_attempt_serializes_reconciliation_before_newer_desired(
    isolated_database_url: str,
) -> None:
    config, factory, primary, _registry = description_seed(
        None,
        database_url=isolated_database_url,
    )
    initial_identity = collection_description_identity(
        archive_root_sha256="5" * 64,
        revision=0,
        description=None,
    )
    with session_scope(factory) as session:
        session.add(
            CollectionArchiveCopyRecord(
                collection_id=1,
                store="mirror",
                incarnation_id=seed_storage_incarnation(session, "archive", "mirror"),
                state="uploaded",
                archive_storage_prefix="archives/mirror/1",
                last_uploaded_at="2026-09-07T00:00:00.000000000Z",
                last_verified_at="2026-09-07T00:00:00.000000000Z",
            )
        )
        session.add(
            CollectionDescriptionPublicationRecord(
                collection_id=1,
                store="mirror",
                incarnation_id=seed_storage_incarnation(session, "archive", "mirror"),
                desired_revision=0,
                desired_identity=initial_identity,
                published_revision=0,
                published_identity=initial_identity,
                state="published",
                next_attempt_at=None,
            )
        )
    mirror = VersionedDescriptionStore()
    stores = ArchiveStoreRegistry(
        {
            "archive": archive_store_binding(primary),
            "mirror": archive_store_binding(mirror, name="mirror"),
        }
    )
    service = SqlAlchemyCollectionDescriptionService(
        config,
        stores,
        session_factory=factory,
    )
    first = service.replace(
        1,
        description="first",
        expected_identity=initial_identity,
        principal=description_principal,
    )
    assert service.process_due(limit=1) == 1
    second = service.replace(
        1,
        description="second",
        expected_identity=str(first["description_identity"]),
        principal=description_principal,
    )
    mirror.lose_next_response = True
    assert service.process_due(limit=1) == 1
    newest = service.replace(
        1,
        description="newest",
        expected_identity=str(second["description_identity"]),
        principal=description_principal,
    )
    with session_scope(factory) as session:
        attempt = session.get(
            CollectionMutableDocumentPublicationAttemptRecord,
            (1, "mirror", "description"),
        )
        publication = session.get(CollectionDescriptionPublicationRecord, (1, "mirror"))
        assert attempt is not None and attempt.document_revision == 2
        assert publication is not None and publication.desired_revision == 3
        publication.next_attempt_at = "2026-09-07T00:00:00.000000000Z"

    def advance() -> int:
        worker = SqlAlchemyCollectionDescriptionService(
            config,
            stores,
            session_factory=make_session_factory(isolated_database_url),
        )
        return worker.process_due(limit=1)

    with ThreadPoolExecutor(max_workers=2) as executor:
        assert all(result in {0, 1} for result in executor.map(lambda _index: advance(), range(2)))
    for _ in range(16):
        if service.process_due(limit=1) == 0:
            break
    with session_scope(factory) as session:
        publication = session.get(CollectionDescriptionPublicationRecord, (1, "mirror"))
        assert publication is not None and publication.state == "published"
        assert publication.published_revision == newest["description_revision"] == 3
        assert session.query(CollectionMutableDocumentPublicationAttemptRecord).count() == 0
        assert session.query(CollectionMutableDocumentReclamationRecord).count() == 0
    assert not mirror.retained_revisions


def test_postgres_reused_tag_node_gc_and_publication_workers_converge(
    isolated_database_url: str,
) -> None:
    store = _AmbiguousTagDeleteStore()
    service, factory, _stored = tag_service(
        None,
        archive_store=store,
        database_url=isolated_database_url,
    )
    principal = tag_principal("source:camera", "workflow:archive")
    with session_scope(factory) as session:
        collection = session.get(CollectionRecord, 1)
        assert collection is not None
        initial_identity = collection.tag_set_identity
    added = service.add(
        1,
        tag="workflow:archive",
        operation_id="postgres-gc-add",
        expected_revision=1,
        expected_tag_set_identity=initial_identity,
        principal=principal,
    )
    with session_scope(factory) as session:
        added_mutation = session.get(CollectionTagMutationRecord, (1, "postgres-gc-add"))
        assert added_mutation is not None and added_mutation.result_root_sha256 is not None
        obsolete_parent = added_mutation.result_root_sha256
    service.remove(
        1,
        tag="workflow:archive",
        operation_id="postgres-gc-remove",
        expected_revision=2,
        expected_tag_set_identity=str(added["tag_set_identity"]),
        principal=principal,
    )
    with session_scope(factory) as session:
        publication = session.get(CollectionTagPublicationRecord, (1, "archive"))
        assert publication is not None and publication.published_head_identity is not None
        session.query(CollectionTagPublicationFrontierRecord).filter(
            CollectionTagPublicationFrontierRecord.collection_id == 1,
            CollectionTagPublicationFrontierRecord.store == "archive",
            CollectionTagPublicationFrontierRecord.head_identity
            != publication.published_head_identity,
        ).delete(synchronize_session=False)
    for _ in range(128):
        assert service.process_due(limit=1) in {0, 1}
        with session_scope(factory) as session:
            gc = session.scalar(select(CollectionTagNodeGcRecord))
            if gc is not None and gc.state == "retry_wait":
                digest = gc.node_digest
                break
    else:  # pragma: no cover - fixed-depth tag closure is much smaller
        raise AssertionError("tag-node reclamation did not reach its ambiguous result")
    assert digest == obsolete_parent

    with pytest.raises(ServiceUnavailable):
        service.add(
            1,
            tag="workflow:archive",
            operation_id="postgres-gc-readd",
            expected_revision=3,
            expected_tag_set_identity=initial_identity,
            principal=principal,
        )
    with session_scope(factory) as session:
        gc = session.get(CollectionTagNodeGcRecord, (1, "archive", digest))
        publication = session.get(CollectionTagPublicationRecord, (1, "archive"))
        assert gc is not None and publication is not None
        gc.next_attempt_at = "2026-09-07T00:00:00.000000000Z"
        publication.next_attempt_at = "2026-09-07T00:00:00.000000000Z"

    def advance() -> int:
        worker = SqlAlchemyCollectionTagService(
            RuntimeConfig.for_testing(database_url=isolated_database_url),
            ArchiveStoreRegistry({"archive": archive_store_binding(store)}),
            session_factory=make_session_factory(isolated_database_url),
        )
        return worker.process_due(limit=1)

    with ThreadPoolExecutor(max_workers=2) as executor:
        assert all(result in {0, 1} for result in executor.map(lambda _index: advance(), range(2)))
    for _ in range(128):
        progressed = service.process_due(limit=1)
        with session_scope(factory) as session:
            publication = session.get(CollectionTagPublicationRecord, (1, "archive"))
            if publication is not None and publication.published_revision == 4:
                break
        assert progressed == 1
    else:  # pragma: no cover - fixed-depth tag closure is much smaller
        raise AssertionError("reused tag authority did not converge")
    with session_scope(factory) as session:
        assert session.get(CollectionTagNodeGcRecord, (1, "archive", digest)) is None
        assert session.query(CollectionMutableDocumentPublicationAttemptRecord).count() == 0


def test_postgres_delayed_success_from_old_tag_gc_cannot_consume_successor(
    isolated_database_url: str,
) -> None:
    _assert_delayed_old_tag_gc_result_cannot_change_successor(
        path=None,
        database_url=isolated_database_url,
        fail_old_result=False,
    )


def test_postgres_delayed_failure_from_old_tag_gc_cannot_reschedule_successor(
    isolated_database_url: str,
) -> None:
    _assert_delayed_old_tag_gc_result_cannot_change_successor(
        path=None,
        database_url=isolated_database_url,
        fail_old_result=True,
    )


def test_postgres_unrelated_head_advance_preserves_interrupted_tag_gc_until_reuse_is_safe(
    isolated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _assert_unrelated_head_advance_preserves_interrupted_tag_gc(
        path=None,
        database_url=isolated_database_url,
        monkeypatch=monkeypatch,
    )


def test_postgres_archive_sequence_state_round_trips_full_v1_domain(
    isolated_database_url: str,
) -> None:
    initialize_db(isolated_database_url)
    with session_scope(make_session_factory(isolated_database_url)) as session:
        seed_storage_incarnation(session, "archive", "archive")
    access = frozenset({ApplicationAccess(COLLECTIONS_CREATE, ALL_RESOURCES)})
    service = SqlAlchemyCollectionUploadService(
        RuntimeConfig.for_testing(database_url=isolated_database_url),
        ArchiveStoreRegistry({"archive": archive_store_binding(MemoryArchiveStore())}),
    )
    created = service.create_or_resume(
        idempotency_key="archive-sequence-persistence",
        ingest_source="postgres-fixture",
        archive_store=None,
        initiator=Principal(id="fixture", key_id="fixture-key", access=access),
        event_context=None,
    )
    collection_id = int(created["collection_id"])
    values = (1 << 63, (1 << 256) - 1)
    with session_scope(make_session_factory(isolated_database_url)) as session:
        upload = session.get(CollectionUploadRecord, collection_id)
        assert upload is not None
        upload.archive_volume_next_sequence = values[-1]
        upload.provenance_archive_next_sequence = values[-1]
        for index, sequence in enumerate(values):
            session.add(
                CollectionArchiveObjectUploadRecord(
                    collection_id=collection_id,
                    object_id=f"pack-{sequence:064x}",
                    sequence=sequence,
                    kind="pack",
                    relative_path=f"volumes/pack-{sequence:064x}.tar.age",
                    object_path=f"archives/fixture/volumes/pack-{sequence:064x}.tar.age",
                    plaintext_bytes=0,
                    source_bytes=0,
                    source_artifact_id=None,
                    source_first_part=None,
                    source_part_count=None,
                    unit_plaintext_bytes=1,
                    plan_json="{}",
                    plan_sha256=f"{index + 1:064x}",
                    state="planned",
                    checkpoint_json=None,
                    sealed_receipt_json=None,
                    metadata_receipt_json=None,
                    failure=None,
                    uploaded_bytes=0,
                    uploaded_units=0,
                    total_units=0,
                    updated_at="2026-01-01T00:00:00.000000000Z",
                    sealed_at=None,
                )
            )
            session.add(
                CollectionUploadProvenanceArchiveVolumeRecord(
                    collection_id=collection_id,
                    sequence=sequence,
                    kind="bindings",
                    document_json="{}",
                    payload_receipt_json="{}",
                    metadata_receipt_json="{}",
                )
            )

    with session_scope(make_session_factory(isolated_database_url)) as session:
        upload = session.get(CollectionUploadRecord, collection_id)
        assert upload is not None
        assert upload.archive_volume_next_sequence == values[-1]
        assert upload.provenance_archive_next_sequence == values[-1]
        assert list(
            session.scalars(
                select(CollectionArchiveObjectUploadRecord.sequence)
                .where(CollectionArchiveObjectUploadRecord.collection_id == collection_id)
                .order_by(CollectionArchiveObjectUploadRecord.sequence)
                .execution_options(include_unpublished_registration=True)
            )
        ) == list(values)

    engine = create_catalog_engine(isolated_database_url)
    with engine.begin() as connection:
        assert connection.execute(
            text(
                "SELECT archive_volume_next_sequence, provenance_archive_next_sequence "
                "FROM collection_uploads WHERE collection_id = :collection_id"
            ),
            {"collection_id": collection_id},
        ).one() == (f"{values[-1]:064x}", f"{values[-1]:064x}")
    with pytest.raises(IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE collection_uploads SET archive_volume_next_sequence = :sequence "
                    "WHERE collection_id = :collection_id"
                ),
                {"sequence": "g" * 64, "collection_id": collection_id},
            )
    engine.dispose()


@pytest.mark.parametrize("changed", ["check", "partial-index"])
def test_postgres_catalog_rejects_changed_journal_validation_predicates(
    isolated_database_url: str, changed: str
) -> None:
    from state_schema import StateSchemaError

    initialize_db(isolated_database_url)
    engine = create_catalog_engine(isolated_database_url)
    try:
        with engine.begin() as connection:
            if changed == "check":
                connection.exec_driver_sql(
                    "ALTER TABLE journal_validation_jobs DROP CONSTRAINT "
                    "ck_journal_validation_outcome"
                )
                connection.exec_driver_sql(
                    "ALTER TABLE journal_validation_jobs ADD CONSTRAINT "
                    "ck_journal_validation_outcome CHECK "
                    "(state NOT IN ('complete','failed') OR outcome_json IS NULL)"
                )
            else:
                connection.exec_driver_sql("DROP INDEX ix_journal_validation_orphan_workspace")
                connection.exec_driver_sql(
                    "CREATE INDEX ix_journal_validation_orphan_workspace "
                    "ON journal_validation_jobs(workspace_id) WHERE collection_id IS NULL"
                )
        with pytest.raises(
            StateSchemaError, match="checks" if changed == "check" else "index definitions"
        ):
            validate_db(isolated_database_url)
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "prefix_mode", ["sealed", "raw-unready", "raw-ready-tail", "raw-parts-frontier"]
)
def test_postgres_acquisition_and_readiness_use_generic_index_plans(
    isolated_database_url: str, prefix_mode: str, record_property
) -> None:
    initialize_db(isolated_database_url)
    factory = make_session_factory(isolated_database_url)
    with session_scope(factory) as session:
        seed_storage_incarnation(session, "archive", "archive")
    policy = CollectionVolumePolicy(
        pack_artifacts=1,
        pack_member_bytes=1,
        raw_part_plaintext_bytes=CHUNK_SIZE,
        raw_volume_plaintext_bytes=CHUNK_SIZE,
    )
    service = SqlAlchemyCollectionUploadService(
        RuntimeConfig.for_testing(database_url=isolated_database_url),
        ArchiveStoreRegistry({"archive": archive_store_binding(MemoryArchiveStore())}),
        policy=policy,
        session_factory=factory,
    )
    collection_id = int(
        service.create_or_resume(
            idempotency_key="generic-acquisition-work",
            ingest_source="fixture",
            archive_store=None,
            initiator=Principal(
                id="uploader",
                key_id="key",
                access=frozenset({ApplicationAccess(COLLECTIONS_CREATE, ALL_RESOURCES)}),
            ),
            event_context=None,
        )["collection_id"]
    )
    empty_sha = hashlib.sha256(b"").hexdigest()
    raw_sha = hashlib.sha256(b"x").hexdigest()
    _, one_commitment = ordered_raw_part_commitment((raw_sha,))
    members = []
    if prefix_mode == "raw-parts-frontier":
        block = b"x" * CHUNK_SIZE
        raw_sha = hashlib.sha256(block).hexdigest()
        source_hash = hashlib.sha256()
        for _ in range(1000):
            source_hash.update(block)
        _, commitment = ordered_raw_part_commitment([raw_sha] * 1000)
        members.append(
            {
                "artifact_id": f"{1:064x}",
                "bytes": str(1000 * CHUNK_SIZE),
                "sha256": source_hash.hexdigest(),
                "raw_parts": {
                    "part_plaintext_bytes": str(CHUNK_SIZE),
                    "part_count": "1000",
                    "ordered_sha256": commitment,
                },
            }
        )
        members.extend(
            {"artifact_id": f"{index:064x}", "bytes": "0", "sha256": empty_sha} for index in (2, 3)
        )
    else:
        for index in range(1002):
            member = {"artifact_id": f"{index + 1:064x}", "bytes": "0", "sha256": empty_sha}
            if prefix_mode != "sealed" and index < 1000:
                member.update(
                    bytes="1",
                    sha256=raw_sha,
                    raw_parts={
                        "part_plaintext_bytes": str(CHUNK_SIZE),
                        "part_count": "1",
                        "ordered_sha256": one_commitment,
                    },
                )
            members.append(member)
    for start in range(0, len(members), 16):
        service.register_artifacts(collection_id, members[start : start + 16])
    with session_scope(factory) as session:
        volumes = list(
            session.scalars(
                select(CollectionArchiveObjectUploadRecord)
                .where(CollectionArchiveObjectUploadRecord.collection_id == collection_id)
                .order_by(CollectionArchiveObjectUploadRecord.sequence)
            )
        )
        assert len(volumes) == 1001
        expected_volume = volumes[-1].object_id
        if prefix_mode == "sealed":
            # Model catalog state only, independently of payload/custody qualification.
            for volume in volumes[:-1]:
                volume.state = "sealed"
                volume.uploaded_units = volume.total_units
        elif prefix_mode == "raw-ready-tail":
            expected_volume = volumes[-2].object_id
        elif prefix_mode == "raw-parts-frontier":
            expected_volume = volumes[0].object_id
    engine = factory.kw["bind"]
    captured = []
    promotions = []

    def capture(_connection, _cursor, statement, parameters, _context, _many):
        if statement.startswith("UPDATE collection_archive_object_uploads SET work_ready="):
            promotions.append((statement, parameters))
        elif statement.lstrip().upper().startswith("SELECT") and (
            "FROM collection_archive_object_uploads" in statement
        ):
            captured.append((statement, parameters))

    event.listen(engine, "before_cursor_execute", capture)
    try:
        promoted = 0
        if prefix_mode in ("raw-ready-tail", "raw-parts-frontier"):
            promoted = 16 if prefix_mode == "raw-parts-frontier" else 1
            artifact_id = members[0 if promoted == 16 else 999]["artifact_id"]
            service.register_raw_part_digests(
                collection_id,
                CollectionUploadRawDigestBatchDocument(
                    artifact_id=artifact_id, first_part="0", sha256s=[raw_sha] * promoted
                ),
            )
        reply = service.acquire_work(collection_id, limit=1)
    finally:
        event.remove(engine, "before_cursor_execute", capture)
    assert len(reply["work"]) == 1
    assert reply["work"][0]["volume"]["volume_id"] == expected_volume
    assert len(captured) == 1
    assert len(promotions) == bool(promoted)

    def prepare(cursor, name, statement, parameters):
        keys = []

        def bind_parameter(match):
            key = match.group(1)
            if key not in keys:
                keys.append(key)
            return "$" + str(keys.index(key) + 1)

        prepared = re.sub(r"%\(([^)]+)\)s", bind_parameter, statement)
        cursor.execute("PREPARE " + name + " AS " + prepared)
        arguments = pgsql.SQL(", ").join(pgsql.Literal(parameters[key]) for key in keys)
        return pgsql.SQL("EXECUTE " + name + " (") + arguments + pgsql.SQL(")")

    with engine.begin() as connection:
        connection.exec_driver_sql("ANALYZE collection_archive_object_uploads")
        with connection.connection.driver_connection.cursor() as cursor:
            cursor.execute("SET LOCAL plan_cache_mode = force_generic_plan")
            execute = prepare(cursor, "acquisition_work", *captured[0])
            cursor.execute(execute)
            rows = cursor.fetchall()
            assert len(rows) == 1 and rows[0][1] == expected_volume
            cursor.execute(pgsql.SQL("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ") + execute)
            plan = cursor.fetchone()[0][0]["Plan"]
            cursor.execute("DEALLOCATE acquisition_work")
            promotion_plan = None
            if promotions:
                execute = prepare(cursor, "readiness_promotion", *promotions[0])
                cursor.execute(pgsql.SQL("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ") + execute)
                promotion_plan = cursor.fetchone()[0][0]["Plan"]
                cursor.execute("DEALLOCATE readiness_promotion")

    def nodes(node):
        yield node
        for child in node.get("Plans", []):
            yield from nodes(child)

    def examined(root):
        return sum(
            (node["Actual Rows"] + node.get("Rows Removed by Filter", 0)) * node["Actual Loops"]
            for node in nodes(root)
            if node.get("Relation Name") == "collection_archive_object_uploads"
        )

    record_property("prefix_mode", prefix_mode)
    record_property("returned_units", len(rows))
    record_property("postgres_archive_rows_examined", examined(plan))
    assert examined(plan) == 1
    if promotion_plan is not None:
        record_property("postgres_promotion_rows_examined", examined(promotion_plan))
        assert examined(promotion_plan) == promoted
    engine.dispose()


@pytest.mark.parametrize("pending_state", ["unsealed", "receipt-missing", "complete"])
def test_postgres_payload_accounting_uses_generic_index_plans(
    isolated_database_url: str, pending_state: str, record_property
) -> None:
    initialize_db(isolated_database_url)
    factory = make_session_factory(isolated_database_url)
    with session_scope(factory) as session:
        seed_storage_incarnation(session, "archive", "archive")
    policy = CollectionVolumePolicy(
        pack_member_bytes=1,
        raw_part_plaintext_bytes=CHUNK_SIZE,
        raw_volume_plaintext_bytes=CHUNK_SIZE,
    )
    service = SqlAlchemyCollectionUploadService(
        RuntimeConfig.for_testing(database_url=isolated_database_url),
        ArchiveStoreRegistry({"archive": archive_store_binding(MemoryArchiveStore())}),
        policy=policy,
        session_factory=factory,
    )
    collection_id = int(
        service.create_or_resume(
            idempotency_key="generic-payload-accounting",
            ingest_source="fixture",
            archive_store=None,
            initiator=Principal(
                id="uploader",
                key_id="key",
                access=frozenset({ApplicationAccess(COLLECTIONS_CREATE, ALL_RESOURCES)}),
            ),
            event_context=None,
        )["collection_id"]
    )
    block = b"x" * CHUNK_SIZE
    part_sha = hashlib.sha256(block).hexdigest()
    source_sha = hashlib.sha256()
    for _ in range(1000):
        source_sha.update(block)
    _, commitment = ordered_raw_part_commitment([part_sha] * 1000)
    artifact_id = "a" * 64
    service.register_artifacts(
        collection_id,
        [
            {
                "artifact_id": artifact_id,
                "bytes": str(1000 * CHUNK_SIZE),
                "sha256": source_sha.hexdigest(),
                "raw_parts": {
                    "part_plaintext_bytes": str(CHUNK_SIZE),
                    "part_count": "1000",
                    "ordered_sha256": commitment,
                },
            }
        ],
    )
    with session_scope(factory) as session:
        volumes = list(
            session.scalars(
                select(CollectionArchiveObjectUploadRecord)
                .where(CollectionArchiveObjectUploadRecord.collection_id == collection_id)
                .order_by(CollectionArchiveObjectUploadRecord.sequence)
            )
        )
        assert len(volumes) == 1000
        first_id, last_id = volumes[0].object_id, volumes[-1].object_id
        # Supply catalog accounting inputs, independently of archive custody.
        for volume in volumes:
            volume.state = "sealed"
            volume.sealed_receipt_json = "{}"
        if pending_state != "complete":
            volumes[-1].sealed_receipt_json = None
        if pending_state == "unsealed":
            volumes[-1].state = "planned"
    del volume, volumes

    engine = factory.kw["bind"]
    captured = {}

    def capture(_connection, _cursor, statement, parameters, _context, _many):
        if statement.startswith("SELECT collection_upload_artifact_volumes.artifact_id"):
            captured["reverse"] = (statement, parameters)
        elif statement.startswith("SELECT collection_archive_object_uploads.object_id"):
            captured["pending"] = (statement, parameters)

    event.listen(engine, "before_cursor_execute", capture)
    try:
        with session_scope(factory) as session:
            upload = session.get(CollectionUploadRecord, collection_id)
            volume = session.get(CollectionArchiveObjectUploadRecord, (collection_id, first_id))
            live = weakref.WeakSet([volume])
            working_set = {"peak_live_archive_records": 1}

            def loaded(_session, row):
                if isinstance(row, CollectionArchiveObjectUploadRecord):
                    live.add(row)
                    working_set["peak_live_archive_records"] = max(
                        working_set["peak_live_archive_records"], len(live)
                    )

            event.listen(session, "loaded_as_persistent", loaded)
            try:
                _record_payload_custody_progress(
                    session, upload, volume, now="2026-01-01T00:00:00Z"
                )
            finally:
                event.remove(session, "loaded_as_persistent", loaded)
            assert working_set["peak_live_archive_records"] <= 130
            expected = int(pending_state == "complete")
            assert upload.payload_sealed_artifact_count == expected
            assert upload.payload_sealed_artifact_bytes == expected * 1000 * CHUNK_SIZE
    finally:
        event.remove(engine, "before_cursor_execute", capture)
    assert set(captured) == {"reverse", "pending"}

    def explain(cursor, name, statement, parameters):
        keys = []

        def parameter(match):
            key = match.group(1)
            if key not in keys:
                keys.append(key)
            return "$" + str(keys.index(key) + 1)

        prepared = re.sub(r"%\(([^)]+)\)s", parameter, statement)
        cursor.execute("PREPARE " + name + " AS " + prepared)
        arguments = pgsql.SQL(", ").join(pgsql.Literal(parameters[key]) for key in keys)
        execute = pgsql.SQL("EXECUTE " + name + " (") + arguments + pgsql.SQL(")")
        cursor.execute(execute)
        rows = cursor.fetchall()
        cursor.execute(pgsql.SQL("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ") + execute)
        plan = cursor.fetchone()[0][0]["Plan"]
        cursor.execute("DEALLOCATE " + name)
        return rows, plan

    with engine.begin() as connection:
        connection.exec_driver_sql("ANALYZE collection_archive_object_uploads")
        connection.exec_driver_sql("ANALYZE collection_upload_artifact_volumes")
        with connection.connection.driver_connection.cursor() as cursor:
            cursor.execute("SET LOCAL plan_cache_mode = force_generic_plan")
            reverse_rows, reverse_plan = explain(cursor, "payload_reverse", *captured["reverse"])
            pending_rows, pending_plan = explain(cursor, "payload_pending", *captured["pending"])
    assert reverse_rows == [(artifact_id,)]
    assert pending_rows == ([] if pending_state == "complete" else [(last_id,)])

    def examined(node, relation):
        own = (
            (node["Actual Rows"] + node.get("Rows Removed by Filter", 0)) * node["Actual Loops"]
            if node.get("Relation Name") == relation
            else 0
        )
        return own + sum(examined(child, relation) for child in node.get("Plans", []))

    reverse_examined = examined(reverse_plan, "collection_upload_artifact_volumes")
    pending_examined = examined(pending_plan, "collection_archive_object_uploads")
    assert reverse_examined == 1
    assert pending_examined == int(pending_state != "complete")
    record_property("source_volumes", 1000)
    record_property("pending_state", pending_state)
    record_property("postgres_reverse_rows_examined", reverse_examined)
    record_property("postgres_pending_rows_examined", pending_examined)
    record_property("postgres_peak_live_archive_records", working_set["peak_live_archive_records"])
    engine.dispose()


@pytest.mark.parametrize("prior_pending_pack", [False, True])
def test_postgres_registration_reopens_bounded_quantum_and_publishes_once(
    isolated_database_url, tmp_path, monkeypatch, prior_pending_pack
):
    from functools import partial

    from tests.unit import test_collection_registration_operations as witness

    monkeypatch.setattr(
        witness,
        "_service_with_archive_objects",
        partial(witness._service_with_archive_objects, database_url=isolated_database_url),
    )
    witness.test_service_registration_reopens_hidden_quantum_and_publishes_one_exact_acknowledgement(
        tmp_path, monkeypatch, prior_pending_pack
    )


def test_postgres_registration_advancers_serialize_without_repeating_a_quantum(
    isolated_database_url, tmp_path, monkeypatch
):
    from concurrent.futures import ThreadPoolExecutor
    from functools import partial
    from threading import Barrier

    from riverhog_core.catalog_registration_models import CollectionRegistrationRecord
    from riverhog_core.incremental_emission import parse_volume_emission_frontier
    from riverhog_core.services.collection_registration import CollectionRegistrationOwner

    from tests.unit import test_collection_registration_operations as witness

    monkeypatch.setattr(
        witness,
        "_service_with_archive_objects",
        partial(witness._service_with_archive_objects, database_url=isolated_database_url),
    )
    service, keys, principal, created, collection_id = witness._open(tmp_path)
    other = SqlAlchemyCollectionUploadService(
        service._config, service._archive_stores, policy=service._policy
    )
    first = CollectionRegistrationOwner(service)
    second = CollectionRegistrationOwner(other)
    operation_id = first.admit(collection_id, [witness._member()], principal=principal)[
        "operation_id"
    ]
    start = Barrier(2)

    def advance(owner):
        start.wait(timeout=10)
        return owner.advance(collection_id, operation_id, limits=witness.LIMITS)

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            replies = list(executor.map(advance, (first, second)))
        assert all(reply["phase"] == "staging" for reply in replies)
        with session_scope(service._session_factory) as session:
            session.info["include_unpublished_registration"] = True
            operation = session.get(CollectionRegistrationRecord, operation_id)
            frontier = parse_volume_emission_frontier(operation.stage_frontier_json)
            assert frontier.checkpoint.next_sequence == 6
            rows = list(
                session.scalars(
                    select(CollectionArchiveObjectUploadRecord.sequence)
                    .where(
                        CollectionArchiveObjectUploadRecord.collection_id == collection_id,
                        CollectionArchiveObjectUploadRecord.registration_id == operation_id,
                    )
                    .order_by(CollectionArchiveObjectUploadRecord.sequence)
                )
            )
            assert rows == list(range(6))
            assert session.get(CollectionUploadRecord, collection_id).planner_next_sequence == 0
        assert service.list_volumes(collection_id)["volumes"] == []
    finally:
        other.close()
        dispose_session_factory(other._session_factory)
        witness._close(service, keys)


def test_postgres_due_registration_skips_a_locked_construction_and_preserves_readonly_status(
    isolated_database_url, tmp_path, monkeypatch
):
    from functools import partial

    from riverhog_core.services.collection_registration import CollectionRegistrationOwner

    from tests.unit import test_collection_registration_operations as witness
    from tests.unit.test_collection_registration_maintenance import _other, _sequence, _tune

    monkeypatch.setattr(
        witness,
        "_service_with_archive_objects",
        partial(witness._service_with_archive_objects, database_url=isolated_database_url),
    )
    service, keys, principal, created, first = witness._open(tmp_path)
    try:
        _tune(service)
        second = _other(service, principal)
        owner = CollectionRegistrationOwner(service)
        a = owner.admit(first, [witness._member()], principal=principal)["operation_id"]
        b = owner.admit(second, [witness._member("3", 4)], principal=principal)["operation_id"]
        with session_scope(service._session_factory) as blocker:
            blocker.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == first)
                .with_for_update()
            )
            assert CollectionRegistrationOwner(service).process_due() == 1
            assert (_sequence(service, a), _sequence(service, b)) == (0, 3)
            assert owner.status(first, a, principal=principal)["phase"] == "staging"
        assert CollectionRegistrationOwner(service).process_due() == 1
        assert (_sequence(service, a), _sequence(service, b)) == (3, 3)
    finally:
        witness._close(service, keys)


@pytest.mark.parametrize("combined", [False, True])
def test_postgres_registration_retains_actual_delegated_claim_authority(
    isolated_database_url, tmp_path, monkeypatch, combined
):
    from functools import partial

    from tests.unit import test_collection_registration_delegation as delegated
    from tests.unit import test_collection_registration_operations as witness

    monkeypatch.setattr(
        witness,
        "_service_with_archive_objects",
        partial(witness._service_with_archive_objects, database_url=isolated_database_url),
    )
    delegated.test_registration_accepts_the_real_write_capability_and_retains_its_live_claim(
        tmp_path, combined
    )


@pytest.mark.parametrize("prior_pack", [False, True])
def test_postgres_registration_disposal_preserves_the_accepted_prefix_after_cold_resume(
    isolated_database_url, tmp_path, monkeypatch, prior_pack
):
    from functools import partial

    from tests.unit import test_collection_registration_cleanup as cleanup
    from tests.unit import test_collection_registration_operations as witness

    monkeypatch.setattr(
        witness,
        "_service_with_archive_objects",
        partial(witness._service_with_archive_objects, database_url=isolated_database_url),
    )
    cleanup.test_cleanup_cold_resumes_bounded_windows_and_preserves_the_accepted_prefix(
        tmp_path, prior_pack
    )


@pytest.mark.parametrize("case", ["interruption", "capacity", "dependent-state"])
def test_postgres_registration_disposal_preserves_retained_state_on_refusal(
    isolated_database_url, tmp_path, monkeypatch, case
):
    from functools import partial

    from tests.unit import test_collection_registration_cleanup as cleanup
    from tests.unit import test_collection_registration_operations as witness

    monkeypatch.setattr(
        witness,
        "_service_with_archive_objects",
        partial(witness._service_with_archive_objects, database_url=isolated_database_url),
    )
    if case == "interruption":
        cleanup.test_cleanup_transaction_interruption_rolls_back_deletes_and_progress(
            tmp_path, monkeypatch
        )
    elif case == "capacity":
        cleanup.test_cleanup_capacity_preserves_disposition_and_cursor_and_backs_off(tmp_path)
    else:
        cleanup.test_cleanup_refuses_member_children_instead_of_using_fk_cascade(tmp_path)


@pytest.mark.parametrize("changed", ["construction", "owner", "disposition"])
def test_postgres_registration_disposal_checks_current_construction_and_selected_owner(
    isolated_database_url, tmp_path, monkeypatch, changed
):
    from functools import partial

    from tests.unit import test_collection_registration_cleanup as cleanup
    from tests.unit import test_collection_registration_operations as witness

    monkeypatch.setattr(
        witness,
        "_service_with_archive_objects",
        partial(witness._service_with_archive_objects, database_url=isolated_database_url),
    )
    cleanup.test_cleanup_refuses_a_changed_construction_or_terminal_selection(tmp_path, changed)


@pytest.mark.parametrize(
    "witness",
    [
        "test_original_preparation_is_retained_before_arm_and_cold_retry",
        "test_preparation_identity_and_authority_are_checked_before_new_effect",
        "test_live_capability_dispatches_once_and_cold_observation_cannot_close_it",
        "test_terminal_selection_keeps_armed_begin_and_original_volume",
        "test_returned_result_and_catalog_change_commit_together_and_retry_exactly",
        "test_positive_observation_keeps_execution_uncertainty",
        "test_part_lanes_keep_identity_exclusion_and_completion_single_flight",
        "test_retained_effect_reservations_cover_calls_and_future_result_bytes",
        "test_oversize_preparation_terminal_and_result_keep_original_state",
        "test_terminal_exact_selection_replays_and_changed_kind_or_request_conflicts",
        "test_concurrent_terminal_requests_have_one_durable_selection",
        "test_preparation_and_terminal_records_do_not_cascade_away_with_upload",
        "test_absent_control_never_synthetically_settles_legacy_untracked_execution",
        "test_call_frontier_does_not_wrap_at_database_integer_width",
        "test_selected_exact_terminal_retry_keeps_original_plan_but_checks_current_access",
        "test_changed_volume_cannot_reuse_original_preparation_for_dispatch",
        "test_closed_unused_dispatch_cannot_invoke_a_late_provider_effect",
        "test_running_dispatch_keeps_its_reservation_until_actual_return",
        "test_archive_withdrawal_survives_terminal_and_parent_volume_retirement",
        "test_archive_withdrawal_commit_and_ack_failures_retain_exact_live_proof",
    ],
)
def test_postgres_archive_write_admission_preserves_exact_execution_ownership(
    isolated_database_url, tmp_path, witness
):
    from tests.unit import test_archive_write_admission as admission

    with admission._case(tmp_path, database_url=isolated_database_url) as case:
        getattr(admission, witness)(case)


def test_postgres_checkpoint_progress_uses_construction_before_volume_locks(
    isolated_database_url, tmp_path
):
    from tests.unit import test_archive_write_settlement as settlement

    with settlement._source_case(tmp_path, database_url=isolated_database_url) as case:
        settlement._assert_checkpoint_lock_order(case)


def test_postgres_lost_checkpoint_report_settles_atomically_without_part_replay(
    isolated_database_url, tmp_path, monkeypatch
):
    from tests.unit import test_archive_write_settlement as settlement

    with settlement._source_case(tmp_path, database_url=isolated_database_url) as case:
        settlement._assert_lost_checkpoint_report(case, monkeypatch)


def test_postgres_concurrent_part_checkpoints_preserve_independent_construction_progress(
    isolated_database_url, tmp_path, monkeypatch
):
    from tests.unit import test_archive_write_settlement as settlement

    with settlement._source_case(
        tmp_path, database_url=isolated_database_url, concurrent_parts=True
    ) as case:
        settlement._assert_parallel_checkpoint_progress(case, monkeypatch)


def test_postgres_archive_write_actor_cannot_gain_a_new_admission_epoch(
    tmp_path, isolated_database_url: str
) -> None:
    from tests.unit.test_archive_write_admission import _assert_actor_epoch_is_pinned, _case

    with _case(tmp_path, database_url=isolated_database_url) as case:
        _assert_actor_epoch_is_pinned(case)


def test_postgres_original_crypto_is_committed_before_provider_begin(
    tmp_path, isolated_database_url: str, monkeypatch
) -> None:
    from tests.unit.test_archive_write_admission import (
        _assert_original_crypto_precedes_begin,
        _case,
    )

    with _case(tmp_path, database_url=isolated_database_url) as case:
        _assert_original_crypto_precedes_begin(case, monkeypatch)


@pytest.mark.parametrize("entry", ["admit", "advance", "due"])
def test_postgres_registration_locks_live_key_and_sealed_claim_before_construction(
    isolated_database_url, tmp_path, monkeypatch, entry
):
    from functools import partial

    from riverhog_core.catalog_models import AppKeyRecord
    from riverhog_core.catalog_registration_models import CollectionRegistrationRecord
    from riverhog_core.catalog_workflow_models import CollectionProcessingClaimRecord
    from riverhog_core.services.collection_registration import CollectionRegistrationOwner

    from tests.unit import test_collection_registration_operations as witness
    from tests.unit.test_collection_registration_delegation import _delegated

    monkeypatch.setattr(
        witness,
        "_service_with_archive_objects",
        partial(witness._service_with_archive_objects, database_url=isolated_database_url),
    )
    service, keys, workflows, principal, collection_id = _delegated(tmp_path)
    owner = CollectionRegistrationOwner(service)
    expected = [
        AppKeyRecord.__tablename__,
        CollectionProcessingClaimRecord.__tablename__,
        CollectionUploadRecord.__tablename__,
        CollectionRegistrationRecord.__tablename__,
    ]
    locked = []

    def capture(connection, cursor, statement, parameters, context, executemany):
        if statement.lstrip().startswith("SELECT") and "FOR UPDATE" in statement:
            for table in expected:
                if f"FROM {table} " in statement.replace("\n", " "):
                    locked.append(table)

    engine = service._session_factory.kw["bind"]
    try:
        if entry != "admit":
            operation = owner.admit(collection_id, [witness._member()], principal=principal)[
                "operation_id"
            ]
        event.listen(engine, "before_cursor_execute", capture)
        try:
            if entry == "admit":
                assert (
                    owner.admit(collection_id, [witness._member()], principal=principal)["phase"]
                    == "staging"
                )
            elif entry == "advance":
                assert (
                    owner.advance(collection_id, operation, limits=witness.LIMITS)["phase"]
                    == "staging"
                )
            else:
                assert owner.process_due() == 1
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        assert locked == expected
    finally:
        dispose_session_factory(workflows._session_factory)
        witness._close(service, keys)


@pytest.mark.parametrize("blocked", ["key", "claim", "upload", "registration"])
@pytest.mark.parametrize(
    ("candidate_budget", "independent_phase"),
    [(1, "staging"), (2, "staging"), (1, "ready")],
)
def test_postgres_due_registration_defers_busy_authority_without_starving_independent_work(
    isolated_database_url, tmp_path, monkeypatch, blocked, candidate_budget, independent_phase
):
    from dataclasses import replace
    from functools import partial

    from riverhog_core.catalog_models import AppKeyRecord
    from riverhog_core.catalog_registration_models import CollectionRegistrationRecord
    from riverhog_core.catalog_workflow_models import CollectionProcessingClaimRecord
    from riverhog_core.services.collection_registration import CollectionRegistrationOwner

    from tests.unit import test_collection_registration_operations as witness
    from tests.unit.test_app_keys import BOOTSTRAP
    from tests.unit.test_collection_registration_delegation import _delegated
    from tests.unit.test_collection_registration_maintenance import _other, _sequence, _tune

    monkeypatch.setattr(
        witness,
        "_service_with_archive_objects",
        partial(witness._service_with_archive_objects, database_url=isolated_database_url),
    )
    workflows = None
    if blocked == "claim":
        service, keys, workflows, principal, first = _delegated(tmp_path)
    else:
        service, keys, principal, created, first = witness._open(tmp_path)
    try:
        _tune(service)
        service._config = replace(
            service._config,
            registration=replace(
                service._config.registration, candidates_per_step=candidate_budget
            ),
        )
        other_key = keys.create(
            app="independent-registration",
            access=(ApplicationAccess(COLLECTIONS_CREATE),),
            grantor=BOOTSTRAP,
        )
        other_principal = keys.authenticate(str(other_key["token"]))
        assert other_principal is not None
        second = _other(service, other_principal)
        owner = CollectionRegistrationOwner(service)
        a = owner.admit(first, [witness._member()], principal=principal)["operation_id"]
        b = owner.admit(second, [witness._member("3", 4)], principal=other_principal)[
            "operation_id"
        ]
        if independent_phase != "staging":
            witness._until(service, second, b, independent_phase)
        second_sequence = _sequence(service, b)
        before = witness._read(service, a)
        with session_scope(service._session_factory) as blocker:
            if blocked == "key":
                statement = select(AppKeyRecord).where(AppKeyRecord.id == principal.key_id)
            elif blocked == "claim":
                statement = select(CollectionProcessingClaimRecord).where(
                    CollectionProcessingClaimRecord.execution_id
                    == principal.id.removeprefix("processing:")
                )
            elif blocked == "upload":
                statement = select(CollectionUploadRecord).where(
                    CollectionUploadRecord.collection_id == first
                )
            else:
                statement = select(CollectionRegistrationRecord).where(
                    CollectionRegistrationRecord.id == a
                )
            assert blocker.scalar(statement.with_for_update()) is not None
            if candidate_budget == 1:
                assert CollectionRegistrationOwner(service).process_due() == 0
                assert (_sequence(service, a), _sequence(service, b)) == (0, second_sequence)
            # A new callback shares only the service's volatile seek hint.
            assert CollectionRegistrationOwner(service).process_due() == 1
            assert _sequence(service, a) == 0
            if independent_phase == "staging":
                assert _sequence(service, b) == 3
            else:
                assert witness._read(service, b)["phase"] == "published"
            assert witness._read(service, a) == before
            assert owner.status(first, a, principal=principal)["phase"] == "staging"
        assert CollectionRegistrationOwner(service).process_due() == 1
        assert _sequence(service, a) == 3
    finally:
        if workflows is not None:
            dispose_session_factory(workflows._session_factory)
        witness._close(service, keys)


def test_postgres_waiting_registration_cannot_rebind_a_selected_disposal_epoch(
    isolated_database_url, tmp_path, monkeypatch
):
    from functools import partial

    from riverhog_core.services import collection_registration_cleanup as cleanup
    from riverhog_core.services.collection_registration import CollectionRegistrationOwner
    from riverhog_protocol.errors import Conflict

    from tests.unit import test_collection_registration_operations as witness
    from tests.unit.test_collection_registration_maintenance import _sequence

    monkeypatch.setattr(
        witness,
        "_service_with_archive_objects",
        partial(witness._service_with_archive_objects, database_url=isolated_database_url),
    )
    service, keys, principal, created, collection_id = witness._open(tmp_path)
    owner = CollectionRegistrationOwner(service)
    operation = owner.admit(collection_id, [witness._member()], principal=principal)["operation_id"]
    waiting = threading.Event()
    caller = threading.get_ident()
    engine = service._session_factory.kw["bind"]

    def at_construction(connection, cursor, statement, parameters, context, executemany):
        if (
            threading.get_ident() != caller
            and "FOR UPDATE" in statement
            and "FROM collection_uploads " in statement.replace("\n", " ")
        ):
            waiting.set()

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            with session_scope(service._session_factory) as blocker:
                upload = blocker.scalar(
                    select(CollectionUploadRecord)
                    .where(CollectionUploadRecord.collection_id == collection_id)
                    .with_for_update()
                )
                event.listen(engine, "before_cursor_execute", at_construction)
                try:
                    future = executor.submit(
                        owner.advance, collection_id, operation, limits=witness.LIMITS
                    )
                    assert waiting.wait(timeout=10)
                    assert not future.done()
                    upload.state = "discarding"
                    selected = cleanup.fence_registration_disposal(
                        blocker, upload, kind="orphan-discard", identity="e" * 64
                    )
                    assert selected is not None
                finally:
                    event.remove(engine, "before_cursor_execute", at_construction)
            with pytest.raises(Conflict, match="execution changed"):
                future.result(timeout=10)
        assert witness._read(service, operation)["phase"] == "cleaning"
        assert _sequence(service, operation) == 0
    finally:
        witness._close(service, keys)


@pytest.mark.parametrize("entry", ["admit", "advance"])
@pytest.mark.parametrize("expires", ["key", "claim"])
def test_postgres_registration_rechecks_authority_expiry_after_waiting_for_construction(
    isolated_database_url, tmp_path, monkeypatch, entry, expires
):
    from datetime import timedelta
    from functools import partial

    from riverhog_core.catalog_models import AppKeyRecord
    from riverhog_core.catalog_workflow_models import CollectionProcessingClaimRecord
    from riverhog_core.services import (
        app_keys,
        collection_registration,
        collection_upload_authority,
    )
    from riverhog_core.services.collection_registration import CollectionRegistrationOwner
    from riverhog_protocol.errors import NotFound
    from time_formats import format_utc_timestamp, utc_now

    from tests.unit import test_collection_registration_operations as witness
    from tests.unit.test_collection_registration_delegation import _delegated

    monkeypatch.setattr(
        witness,
        "_service_with_archive_objects",
        partial(witness._service_with_archive_objects, database_url=isolated_database_url),
    )
    service, keys, workflows, principal, collection_id = _delegated(tmp_path)
    owner = CollectionRegistrationOwner(service)
    waiting = threading.Event()
    caller = threading.get_ident()
    engine = service._session_factory.kw["bind"]
    now = utc_now()

    def clock():
        return now + timedelta(minutes=2) if waiting.is_set() else now

    def at_construction(connection, cursor, statement, parameters, context, executemany):
        if (
            threading.get_ident() != caller
            and "FOR UPDATE" in statement
            and "FROM collection_uploads " in statement.replace("\n", " ")
        ):
            waiting.set()

    try:
        operation = None
        if entry == "advance":
            operation = owner.admit(collection_id, [witness._member()], principal=principal)[
                "operation_id"
            ]
            before = witness._read(service, operation)
        with session_scope(service._session_factory) as session:
            if expires == "key":
                record = session.get(AppKeyRecord, principal.key_id)
            else:
                record = session.scalar(
                    select(CollectionProcessingClaimRecord).where(
                        CollectionProcessingClaimRecord.execution_id
                        == principal.id.removeprefix("processing:")
                    )
                )
            record.expires_at = format_utc_timestamp(now + timedelta(minutes=1))
        monkeypatch.setattr(app_keys, "utc_now", clock)
        monkeypatch.setattr(
            collection_registration, "utc_timestamp_now", lambda: format_utc_timestamp(clock())
        )
        monkeypatch.setattr(
            collection_upload_authority, "utc_timestamp_now", lambda: format_utc_timestamp(clock())
        )
        with ThreadPoolExecutor(max_workers=1) as executor:
            with session_scope(service._session_factory) as blocker:
                blocker.scalar(
                    select(CollectionUploadRecord)
                    .where(CollectionUploadRecord.collection_id == collection_id)
                    .with_for_update()
                )
                event.listen(engine, "before_cursor_execute", at_construction)
                try:
                    if entry == "admit":
                        future = executor.submit(
                            owner.admit, collection_id, [witness._member()], principal=principal
                        )
                    else:
                        future = executor.submit(
                            owner.advance, collection_id, operation, limits=witness.LIMITS
                        )
                    assert waiting.wait(timeout=10)
                    assert not future.done()
                finally:
                    event.remove(engine, "before_cursor_execute", at_construction)
            message = "no longer active" if expires == "key" else "execution authority"
            with pytest.raises(NotFound, match=message):
                future.result(timeout=10)
        if operation is None:
            with session_scope(service._session_factory) as session:
                assert (
                    session.get(CollectionUploadRecord, collection_id).active_registration_id
                    is None
                )
        else:
            assert witness._read(service, operation) == before
    finally:
        dispose_session_factory(workflows._session_factory)
        witness._close(service, keys)


@pytest.mark.parametrize("background", [False, True])
def test_postgres_selected_registration_cleanup_retains_its_rights_after_key_revocation(
    isolated_database_url, tmp_path, monkeypatch, background
):
    from functools import partial

    from tests.unit import test_collection_registration_operations as witness
    from tests.unit.test_collection_registration_cleanup import (
        test_selected_cleanup_keeps_its_original_rights_after_forward_key_revocation,
    )

    monkeypatch.setattr(
        witness,
        "_service_with_archive_objects",
        partial(witness._service_with_archive_objects, database_url=isolated_database_url),
    )
    test_selected_cleanup_keeps_its_original_rights_after_forward_key_revocation(
        tmp_path, background
    )


def test_postgres_payload_entry_locks_live_key_and_sealed_claim_before_construction(
    isolated_database_url, tmp_path, monkeypatch
):
    from functools import partial

    from riverhog_core.catalog_models import AppKeyRecord
    from riverhog_core.catalog_workflow_models import CollectionProcessingClaimRecord
    from riverhog_protocol.errors import Conflict

    from tests.unit import test_collection_registration_operations as witness
    from tests.unit.test_collection_registration_delegation import _delegated

    monkeypatch.setattr(
        witness,
        "_service_with_archive_objects",
        partial(witness._service_with_archive_objects, database_url=isolated_database_url),
    )
    service, keys, workflows, principal, collection_id = _delegated(tmp_path)
    expected = [
        AppKeyRecord.__tablename__,
        CollectionProcessingClaimRecord.__tablename__,
        CollectionUploadRecord.__tablename__,
    ]
    locked = []

    def capture(connection, cursor, statement, parameters, context, executemany):
        if statement.lstrip().startswith("SELECT") and "FOR UPDATE" in statement:
            for table in expected:
                if f"FROM {table} " in statement.replace("\n", " "):
                    locked.append(table)

    engine = service._session_factory.kw["bind"]
    try:
        service.register_artifacts(collection_id, [witness._member("1", 1)])
        volume = service.list_volumes(collection_id)["volumes"][0]
        event.listen(engine, "before_cursor_execute", capture)
        try:
            with pytest.raises(Conflict, match="plan identity changed"):
                service.upload_unit(
                    collection_id,
                    volume["volume_id"],
                    0,
                    principal=principal,
                    plan_sha256="f" * 64,
                    content=b"",
                )
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        assert locked == expected
    finally:
        dispose_session_factory(workflows._session_factory)
        witness._close(service, keys)


@pytest.mark.parametrize("expires", ["key", "claim", "lease"])
def test_postgres_payload_entry_rechecks_expiry_after_waiting_for_construction(
    isolated_database_url, tmp_path, monkeypatch, expires
):
    from datetime import timedelta
    from functools import partial

    from riverhog_core.catalog_models import AppKeyRecord
    from riverhog_core.catalog_workflow_models import CollectionProcessingClaimRecord
    from riverhog_core.services import app_keys, collection_upload_authority, collection_uploads
    from riverhog_protocol.errors import Conflict, NotFound
    from time_formats import format_utc_timestamp, utc_now

    from tests.unit import test_collection_registration_operations as witness
    from tests.unit.test_collection_registration_delegation import _delegated

    monkeypatch.setattr(
        witness,
        "_service_with_archive_objects",
        partial(witness._service_with_archive_objects, database_url=isolated_database_url),
    )
    service, keys, workflows, principal, collection_id = _delegated(tmp_path)
    waiting = threading.Event()
    caller = threading.get_ident()
    engine = service._session_factory.kw["bind"]
    now = utc_now()

    def clock():
        return now + timedelta(minutes=2) if waiting.is_set() else now

    def at_construction(connection, cursor, statement, parameters, context, executemany):
        if (
            threading.get_ident() != caller
            and "FOR UPDATE" in statement
            and "FROM collection_uploads " in statement.replace("\n", " ")
        ):
            waiting.set()

    try:
        service.register_artifacts(collection_id, [witness._member("1", 1)])
        volume = service.list_volumes(collection_id)["volumes"][0]
        with session_scope(service._session_factory) as session:
            if expires == "key":
                record = session.get(AppKeyRecord, principal.key_id)
                record.expires_at = format_utc_timestamp(now + timedelta(minutes=1))
            elif expires == "claim":
                record = session.scalar(
                    select(CollectionProcessingClaimRecord).where(
                        CollectionProcessingClaimRecord.execution_id
                        == principal.id.removeprefix("processing:")
                    )
                )
                record.expires_at = format_utc_timestamp(now + timedelta(minutes=1))
            else:
                upload = session.get(CollectionUploadRecord, collection_id)
                upload.custody_mode = "custody-transfer"
                upload.lease_expires_at = format_utc_timestamp(now + timedelta(minutes=1))
        monkeypatch.setattr(app_keys, "utc_now", clock)
        for owner in (collection_upload_authority, collection_uploads):
            monkeypatch.setattr(owner, "utc_timestamp_now", lambda: format_utc_timestamp(clock()))
        with ThreadPoolExecutor(max_workers=1) as executor:
            with session_scope(service._session_factory) as blocker:
                blocker.scalar(
                    select(CollectionUploadRecord)
                    .where(CollectionUploadRecord.collection_id == collection_id)
                    .with_for_update()
                )
                event.listen(engine, "before_cursor_execute", at_construction)
                try:
                    future = executor.submit(
                        service.upload_unit,
                        collection_id,
                        volume["volume_id"],
                        0,
                        principal=principal,
                        plan_sha256=volume["plan_sha256"],
                        content=b"",
                    )
                    assert waiting.wait(timeout=10)
                    assert not future.done()
                finally:
                    event.remove(engine, "before_cursor_execute", at_construction)
            expected = Conflict if expires == "lease" else NotFound
            message = (
                "lease"
                if expires == "lease"
                else ("no longer active" if expires == "key" else "execution authority")
            )
            with pytest.raises(expected, match=message):
                future.result(timeout=10)
        with session_scope(service._session_factory) as session:
            record = session.get(
                CollectionArchiveObjectUploadRecord, (collection_id, volume["volume_id"])
            )
            assert record.checkpoint_json is None
    finally:
        dispose_session_factory(workflows._session_factory)
        witness._close(service, keys)


@pytest.mark.parametrize("kind", ["pack", "segment"])
def test_postgres_actual_unit_retains_original_crypto_before_lookup_and_begin(
    isolated_database_url, tmp_path, monkeypatch, kind
):
    from tests.unit import test_collection_upload_preparation as witness

    with witness._case(tmp_path, kind, database_url=isolated_database_url) as case:
        witness._assert_preparation_committed_before_provider(case, monkeypatch)


@pytest.mark.parametrize("kind", ["pack", "segment"])
@pytest.mark.parametrize("vector", ["terminal-during-crypto", "original-checkpoint"])
def test_postgres_preparation_epoch_and_original_checkpoint_survive_interruption(
    isolated_database_url, tmp_path, monkeypatch, kind, vector
):
    from functools import partial

    from tests.unit import test_collection_upload_preparation as witness

    monkeypatch.setattr(
        witness, "_case", partial(witness._case, database_url=isolated_database_url)
    )
    if vector == "terminal-during-crypto":
        witness.test_terminal_selection_during_crypto_preparation_prevents_provider_dispatch(
            tmp_path, monkeypatch, kind
        )
    else:
        witness.test_existing_checkpoint_preparation_is_validated_without_provider_access(
            tmp_path, monkeypatch, kind
        )


@pytest.mark.parametrize("kind", ["pack", "segment"])
def test_postgres_competing_preparations_retain_one_exact_original(
    isolated_database_url, tmp_path, monkeypatch, kind
):
    from tests.unit import test_collection_upload_preparation as witness

    with witness._case(tmp_path, kind, database_url=isolated_database_url) as case:
        witness._assert_competing_candidates_retain_one_original(case, monkeypatch)


@pytest.mark.parametrize("kind", ["pack", "segment"])
@pytest.mark.parametrize(
    "vector",
    [
        "actual",
        "lost-report",
        "rollback",
        "positive-unknown",
        "late-begin",
        "late-complete",
        "multiple-segments",
        "lost-retirement",
        "mixed-segments",
    ],
)
def test_postgres_actual_uploader_admission_and_atomic_reports(
    isolated_database_url, tmp_path, monkeypatch, kind, vector
):
    from functools import partial

    from tests.unit import test_archive_upload_writes as witness

    monkeypatch.setattr(
        witness, "_actor", partial(witness._actor, database_url=isolated_database_url)
    )
    if vector == "actual":
        witness.test_actual_uploader_commits_admitted_filesystem_outcomes_with_checkpoints(
            tmp_path, monkeypatch, kind, "direct"
        )
    elif vector == "lost-report":
        witness.test_checkpoint_report_retry_after_lost_commit_ack_never_resends_provider(
            tmp_path, monkeypatch, kind
        )
    elif vector == "rollback":
        witness.test_checkpoint_and_all_segment_reports_roll_back_together(
            tmp_path, monkeypatch, kind
        )
    elif vector == "positive-unknown":
        witness.test_positive_unknown_begin_retains_original_checkpoint_and_late_closure(
            tmp_path, monkeypatch, kind
        )
    elif vector in {"late-begin", "late-complete"}:
        witness.test_terminal_selection_retains_exact_late_provider_outcome(
            tmp_path, monkeypatch, kind, vector.removeprefix("late-")
        )
    elif vector == "multiple-segments":
        witness.test_multiple_provider_segments_settle_with_one_logical_checkpoint(
            tmp_path, monkeypatch, kind
        )
    elif vector == "lost-retirement":
        witness.test_lost_retirement_ack_retries_only_the_confirmed_exact_checkpoint(
            tmp_path, monkeypatch, kind
        )
    else:
        witness.test_mixed_closed_and_unknown_segments_commit_one_checkpoint_and_keep_unknown_charge(
            tmp_path, monkeypatch, kind
        )


@pytest.mark.parametrize(
    "vector",
    [
        "staging-cancel",
        "verifying-cancel",
        "auditing-cancel",
        "ready-cancel",
        "staging-discard",
        "verifying-discard",
        "auditing-discard",
        "ready-discard",
        "rollback",
        "revision",
        "checkpoint",
        "construction",
        "published",
        "unresolved",
    ],
)
def test_postgres_terminal_selection_freezes_registration_without_cleanup(
    isolated_database_url, tmp_path, monkeypatch, vector
):
    from functools import partial

    from tests.unit import test_collection_registration_operations as fixtures
    from tests.unit import test_collection_terminal_registration as witness

    monkeypatch.setattr(
        fixtures,
        "_service_with_archive_objects",
        partial(fixtures._service_with_archive_objects, database_url=isolated_database_url),
    )
    if vector == "rollback":
        witness.test_terminal_freeze_rolls_back_with_its_selection_transaction(
            tmp_path, monkeypatch
        )
    elif vector in {"revision", "checkpoint", "construction"}:
        witness.test_terminal_selection_refuses_changed_active_registration_reservation(
            tmp_path, vector
        )
    elif vector == "published":
        witness.test_terminal_freeze_preserves_published_acknowledgement_replay(tmp_path)
    elif vector == "unresolved":
        witness.test_terminal_freeze_keeps_original_unresolved_effect_and_hidden_rows(tmp_path)
    else:
        phase, kind = vector.split("-")
        witness.test_terminal_selection_freezes_hidden_registration_without_cleanup(
            tmp_path, phase, kind
        )


def test_postgres_registration_actor_cannot_cross_selected_terminal(
    isolated_database_url, tmp_path, monkeypatch
):
    import threading
    from concurrent.futures import ThreadPoolExecutor
    from functools import partial

    from riverhog_core.services import collection_registration
    from riverhog_protocol.errors import Conflict

    from tests.unit import test_collection_registration_operations as fixtures
    from tests.unit import test_collection_terminal_registration as witness

    monkeypatch.setattr(
        fixtures,
        "_service_with_archive_objects",
        partial(fixtures._service_with_archive_objects, database_url=isolated_database_url),
    )
    with witness._case(tmp_path, "ready") as case:
        service, principal, collection_id, registration, operation_id, _ = case
        original = collection_registration._execution
        entered, release = threading.Event(), threading.Event()

        def paused(*args):
            execution = original(*args)
            entered.set()
            assert release.wait(timeout=10)
            return execution

        monkeypatch.setattr(collection_registration, "_execution", paused)
        before = witness._snapshot(service, collection_id, operation_id)
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(
                registration.advance, collection_id, operation_id, limits=witness.LIMITS
            )
            try:
                assert entered.wait(timeout=10)
                witness._select(service, principal, collection_id)
            finally:
                release.set()
            with pytest.raises(Conflict, match="execution changed"):
                future.result(timeout=10)
        after = witness._snapshot(service, collection_id, operation_id)
        assert after["operation"]["phase"] == "ready"
        assert after["operation"]["acknowledgement_json"] is None
        assert after["rows"] == before["rows"] and after["upload"] == before["upload"]


@pytest.mark.parametrize("delegated", [False, True])
@pytest.mark.parametrize("state", ["open", "orphaned"])
@pytest.mark.parametrize("kind", ["cancel", "discard"])
def test_postgres_output_resume_retains_selected_terminal(
    isolated_database_url, tmp_path, monkeypatch, delegated, state, kind
):
    from functools import partial

    from tests.unit import test_collection_registration_operations as fixtures
    from tests.unit import test_collection_terminal_resume as witness

    monkeypatch.setattr(
        fixtures,
        "_service_with_archive_objects",
        partial(fixtures._service_with_archive_objects, database_url=isolated_database_url),
    )
    witness.test_existing_output_resume_cannot_cross_selected_terminal(
        tmp_path, delegated, state, kind
    )


@pytest.mark.parametrize("delegated", [False, True])
@pytest.mark.parametrize("kind", ["cancel", "discard"])
def test_postgres_output_resume_waits_for_terminal_selection(
    isolated_database_url, tmp_path, monkeypatch, delegated, kind
):
    import threading
    import time
    from concurrent.futures import ThreadPoolExecutor
    from functools import partial

    from riverhog_core.services import archive_write_admission
    from riverhog_protocol.errors import Conflict
    from sqlalchemy import event, text

    from tests.unit import test_collection_registration_operations as fixtures
    from tests.unit import test_collection_terminal_resume as witness

    monkeypatch.setattr(
        fixtures,
        "_service_with_archive_objects",
        partial(fixtures._service_with_archive_objects, database_url=isolated_database_url),
    )
    with witness._case(tmp_path, delegated=delegated) as case:
        service, principal, collection_id, request = case
        entered, release, waiting = threading.Event(), threading.Event(), threading.Event()
        selected_pid, resume_pid = [], []
        execution = threading.local()
        original = archive_write_admission.freeze_registration_execution

        def paused(session, *args, **kwargs):
            original(session, *args, **kwargs)
            selected_pid.append(session.scalar(text("SELECT pg_backend_pid()")))
            entered.set()
            assert release.wait(timeout=10)

        monkeypatch.setattr(archive_write_admission, "freeze_registration_execution", paused)
        engine = service._session_factory.kw["bind"]

        def capture(connection, cursor, statement, parameters, context, executemany):
            owner = "collection_processing_claims" if delegated else "collection_uploads"
            if (
                getattr(execution, "resuming", False)
                and owner in statement
                and "FOR UPDATE" in statement
            ):
                cursor.execute("SELECT pg_backend_pid()")
                resume_pid.append(cursor.fetchone()[0])
                waiting.set()

        event.listen(engine, "before_cursor_execute", capture)
        try:
            with ThreadPoolExecutor(max_workers=2, thread_name_prefix="terminal_resume") as pool:
                selected = pool.submit(witness._select, service, principal, collection_id, kind)
                try:
                    assert entered.wait(timeout=10)

                    def resume():
                        execution.resuming = True
                        return service.create_or_resume(**request)

                    resumed = pool.submit(resume)
                    assert waiting.wait(timeout=10)
                    deadline = time.monotonic() + 10
                    blocked = False
                    with engine.connect() as observer:
                        while time.monotonic() < deadline:
                            blockers = observer.scalar(
                                text("SELECT pg_blocking_pids(:pid)"),
                                {"pid": resume_pid[0]},
                            )
                            if selected_pid[0] in blockers:
                                blocked = True
                                break
                            time.sleep(0.01)
                    assert blocked
                finally:
                    release.set()
                assert selected.result(timeout=10)
                with pytest.raises(Conflict, match="admission is closed"):
                    resumed.result(timeout=10)
        finally:
            release.set()
            event.remove(engine, "before_cursor_execute", capture)
        snapshot = witness._snapshot(service, collection_id)
        assert snapshot[0] and snapshot[1]
        assert witness._select(service, principal, collection_id, kind)
        assert witness._snapshot(service, collection_id) == snapshot


@pytest.mark.parametrize("state", ["open", "orphaned"])
def test_postgres_output_claim_expiry_rechecked_after_upload_lock(
    isolated_database_url, tmp_path, monkeypatch, state
):
    import threading
    import time
    from concurrent.futures import ThreadPoolExecutor
    from functools import partial

    from riverhog_core.catalog_db import session_scope
    from riverhog_core.catalog_models import CollectionUploadRecord
    from riverhog_core.services import collection_upload_authority, collection_uploads
    from riverhog_protocol.errors import NotFound
    from sqlalchemy import event, select, text
    from time_formats import format_utc_ns, parse_utc_timestamp

    from tests.unit import test_collection_registration_operations as fixtures
    from tests.unit import test_collection_terminal_resume as witness

    monkeypatch.setattr(
        fixtures,
        "_service_with_archive_objects",
        partial(fixtures._service_with_archive_objects, database_url=isolated_database_url),
    )
    with witness._case(tmp_path, delegated=True) as case:
        service, _, collection_id, request = case
        with session_scope(service._session_factory) as session:
            upload = session.get(CollectionUploadRecord, collection_id)
            upload.state = state
            if state == "orphaned":
                upload.orphaned_at = "2000-01-01T00:00:00Z"
                upload.archive_phase = "orphaned"
            original = tuple(getattr(upload, column.name) for column in upload.__table__.columns)
        clock = [parse_utc_timestamp("2026-11-01T00:00:00Z")]
        monkeypatch.setattr(collection_uploads, "utc_epoch_ns_now", lambda: clock[0])
        monkeypatch.setattr(
            collection_upload_authority, "utc_timestamp_now", lambda: format_utc_ns(clock[0])
        )
        waiting, execution, pids = threading.Event(), threading.local(), []
        engine = service._session_factory.kw["bind"]

        def capture(connection, cursor, statement, parameters, context, executemany):
            if (
                getattr(execution, "resuming", False)
                and "FROM collection_uploads" in statement
                and "FOR UPDATE" in statement
            ):
                cursor.execute("SELECT pg_backend_pid()")
                pids.append(cursor.fetchone()[0])
                waiting.set()

        def resume():
            execution.resuming = True
            return service.create_or_resume(**request)

        blocker = service._session_factory()
        event.listen(engine, "before_cursor_execute", capture)
        try:
            blocker.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == collection_id)
                .with_for_update()
            )
            blocker_pid = blocker.scalar(text("SELECT pg_backend_pid()"))
            with ThreadPoolExecutor(max_workers=1) as pool:
                result = pool.submit(resume)
                try:
                    assert waiting.wait(timeout=10)
                    deadline, blocked = time.monotonic() + 10, False
                    with engine.connect() as observer:
                        while time.monotonic() < deadline:
                            if blocker_pid in observer.scalar(
                                text("SELECT pg_blocking_pids(:pid)"), {"pid": pids[0]}
                            ):
                                blocked = True
                                break
                            time.sleep(0.01)
                    assert blocked
                    clock[0] = parse_utc_timestamp("2999-01-02T00:00:00Z")
                finally:
                    blocker.rollback()
                with pytest.raises(NotFound, match="execution authority"):
                    result.result(timeout=10)
        finally:
            blocker.close()
            event.remove(engine, "before_cursor_execute", capture)
        with session_scope(service._session_factory) as session:
            upload = session.get(CollectionUploadRecord, collection_id)
            assert (
                tuple(getattr(upload, column.name) for column in upload.__table__.columns)
                == original
            )


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("phase", ("begin", "complete"))
def test_postgres_original_mutable_actor_receives_before_terminal_drain(
    isolated_database_url, tmp_path, monkeypatch, kind, mode, phase
):
    from tests.unit.test_archive_upload_writes import _assert_original_terminal_drain

    _assert_original_terminal_drain(
        tmp_path, monkeypatch, kind, mode, phase, database_url=isolated_database_url
    )


@pytest.mark.parametrize("lock", ("upload", "control", "terminal"))
def test_postgres_mutable_actor_terminal_fence_defers_actual_row_locks(
    isolated_database_url, tmp_path, monkeypatch, lock
):
    from concurrent.futures import ThreadPoolExecutor

    from riverhog_core.catalog_archive_write_models import (
        ArchiveUploadTerminalRecord,
        ArchiveWriteControlRecord,
    )
    from riverhog_core.catalog_models import CollectionUploadRecord

    from tests.unit.test_archive_upload_writes import _actor, _open, _selected_terminal

    with _actor(tmp_path, monkeypatch, "segment", database_url=isolated_database_url) as actor:
        _open(actor)
        selected = _selected_terminal(actor)
        model, key = {
            "upload": (CollectionUploadRecord, actor.case.collection_id),
            "control": (ArchiveWriteControlRecord, actor.case.collection_id),
            "terminal": (ArchiveUploadTerminalRecord, selected),
        }[lock]
        with session_scope(actor.case.factory) as session:
            session.get(model, key, with_for_update=True)
            with ThreadPoolExecutor(max_workers=1) as pool:
                assert (
                    pool.submit(actor.journal.drain_terminal, selected).result(timeout=3) == "busy"
                )
            assert actor.journal._terminal_operation_id is None
        assert actor.journal.drain_terminal(selected) == "closed"
