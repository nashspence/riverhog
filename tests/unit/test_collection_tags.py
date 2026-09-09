from __future__ import annotations

import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from http_api_contracts import BrowseTokenCodec
from riverhog_age import decrypt_age_scrypt
from riverhog_application_access import ApplicationAccess
from riverhog_core.app_permissions import (
    ALL_RESOURCES,
    CATALOG_READ,
    COLLECTION_TAGS_MANAGE,
    ApplicationPrincipal,
    tag_resource,
)
from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.catalog_db import make_session_factory, session_scope
from riverhog_core.catalog_events import (
    begin_catalog_event,
    open_catalog_tag_visibility,
    publish_catalog_event,
)
from riverhog_core.catalog_models import (
    CatalogEventRecord,
    CollectionArchiveCopyRecord,
    CollectionMutableDocumentPublicationAttemptRecord,
    CollectionMutableDocumentReclamationRecord,
    CollectionRecord,
    CollectionTagMembershipRecord,
    CollectionTagMutationNodeReferenceRecord,
    CollectionTagMutationRecord,
    CollectionTagNodeEdgeRecord,
    CollectionTagNodeGcRecord,
    CollectionTagNodeReclamationRecord,
    CollectionTagNodeRecord,
    CollectionTagPublicationFrontierRecord,
    CollectionTagPublicationRecord,
    CollectionTagPublishedNodeRecord,
    CollectionTagRecord,
    CollectionTagRevisionRecord,
    CollectionTagVisibilityRecord,
)
from riverhog_core.ports.archive_store import CollectionTagObjectReceipt
from riverhog_core.runtime_config import DEV_ARCHIVE_PASSPHRASE, RuntimeConfig
from riverhog_core.services.catalog_sync import (
    SqlAlchemyCatalogSyncService,
    _reap_unreferenced_tag_history,
)
from riverhog_core.services.collection_tags import (
    SqlAlchemyCollectionTagService,
    build_collection_tag_set,
    ensure_tag_publication_for_copy,
)
from riverhog_core.services.mutable_document_reclamation import (
    process_due_mutable_document_reclamations,
)
from riverhog_core.stores.storage_adapter_archive_store import StorageAdapterArchiveStore
from riverhog_protocol import (
    COLLECTION_TAG_HEAD_RELATIVE_PATH,
    COLLECTION_TAG_UTF8_BYTES_MAX,
    CatalogSyncDelete,
    CollectionTagChild,
    CollectionTagHeadDocument,
    CollectionTagNode,
    CollectionTagSet,
    CollectionTagSetRoot,
    collection_tag_node_digest,
    collection_tag_node_path,
    collection_tag_sha256,
    encode_collection_tag_node,
)
from riverhog_protocol.errors import NotFound, PreconditionFailed, ServiceUnavailable
from riverhog_storage_adapter_protocol import DeleteObjectRequest
from sqlalchemy import exists, select
from time_formats import utc_timestamp_now

from tests.unit.archive_object_fixtures import (
    MemoryArchiveStore,
    archive_store_binding,
    seed_archive_copy,
)
from tests.unit.db_helpers import sqlite_url
from tests.unit.test_storage_adapter_archive_store import _VersionedMemoryAdapter


def _principal(*tags: str) -> ApplicationPrincipal:
    return ApplicationPrincipal(
        app="tag-editor",
        key_id="tag-editor-key",
        access=frozenset(
            ApplicationAccess(permission, tag_resource(tag))
            for permission in (CATALOG_READ, COLLECTION_TAGS_MANAGE)
            for tag in tags
        ),
    )


def _service(
    path: Path | None,
    *,
    archive_store: MemoryArchiveStore | None = None,
    database_url: str | None = None,
    archive_storage_prefix: str = "archives/archive/opaque-docs",
) -> tuple[SqlAlchemyCollectionTagService, object, MemoryArchiveStore]:
    config, archive = seed_archive_copy(
        path,
        {"camera/clip.bin": b"clip"},
        store="archive",
        database_url=database_url,
    )
    store = archive_store or MemoryArchiveStore()
    factory = make_session_factory(config.database_url)
    with session_scope(factory) as session:
        collection = session.get(CollectionRecord, archive.collection_id)
        assert collection is not None and collection.archive_root_sha256 is not None
        copy = session.get(CollectionArchiveCopyRecord, (archive.collection_id, "archive"))
        assert copy is not None
        copy.archive_storage_prefix = archive_storage_prefix
        tag_set, _created = build_collection_tag_set(session, ("source:camera",))
        head = CollectionTagHeadDocument.seal(
            archive_root_sha256=collection.archive_root_sha256,
            revision=1,
            root_sha256=tag_set.root.root_sha256,
        )
        collection.tag_root_sha256 = head.root_sha256
        collection.tag_set_identity = head.tag_set_identity
        collection.tag_head_identity = head.head_identity
        revision = session.get(CollectionTagRevisionRecord, (archive.collection_id, 1))
        assert revision is not None
        revision.root_sha256 = head.root_sha256
        revision.tag_set_identity = head.tag_set_identity
        revision.head_identity = head.head_identity
        digest = collection_tag_sha256("source:camera")
        session.add(
            CollectionTagRecord(
                tag_sha256=digest,
                tag="source:camera",
                search_text="source:camera",
                created_at=utc_timestamp_now(),
                updated_at=utc_timestamp_now(),
                collection_count=1,
            )
        )
        session.flush()
        session.add(
            CollectionTagMembershipRecord(
                collection_id=archive.collection_id,
                tag_sha256=digest,
                added_at=utc_timestamp_now(),
            )
        )
        open_catalog_tag_visibility(
            session,
            collection_id=archive.collection_id,
            tag_sha256=digest,
            revision=1,
        )
        publication = session.get(
            CollectionTagPublicationRecord, (archive.collection_id, "archive")
        )
        assert publication is not None
        publication.desired_revision = head.revision
        publication.desired_tag_set_identity = head.tag_set_identity
        publication.desired_head_identity = head.head_identity
        publication.published_revision = head.revision
        publication.published_tag_set_identity = head.tag_set_identity
        publication.published_head_identity = head.head_identity
    receipt = store.publish_collection_tag_head(
        collection_id=archive.collection_id,
        archive_storage_prefix=archive_storage_prefix,
        document=head.to_json_bytes(),
        passphrase_id="riverhog-dev-key-v1",
    )
    with session_scope(factory) as session:
        publication = session.get(
            CollectionTagPublicationRecord, (archive.collection_id, "archive")
        )
        assert publication is not None
        publication.head_object_path = receipt.object_path
        publication.head_provider_revision = receipt.revision
        publication.head_stored_bytes = receipt.stored_bytes
        publication.head_stored_sha256 = receipt.stored_sha256
        publication.published_at = receipt.published_at
    service = SqlAlchemyCollectionTagService(
        config,
        ArchiveStoreRegistry({"archive": archive_store_binding(store)}),
        session_factory=factory,
    )
    return service, factory, store


class _EncryptedMemoryTagNodes:
    def __init__(self, store: MemoryArchiveStore, prefix: str) -> None:
        self.store = store
        self.prefix = prefix

    def get(self, digest: str) -> bytes:
        path = f"{self.prefix}/{collection_tag_node_path(digest)}"
        return decrypt_age_scrypt(self.store.objects[path], DEV_ARCHIVE_PASSPHRASE)

    def put(self, digest: str, encoded: bytes) -> None:
        raise AssertionError((digest, encoded))


class _EncryptedAdapterTagNodes:
    def __init__(self, adapter: _VersionedMemoryAdapter, prefix: str) -> None:
        self.adapter = adapter
        self.prefix = prefix

    def get(self, digest: str) -> bytes:
        path = f"{self.prefix}/{collection_tag_node_path(digest)}"
        return decrypt_age_scrypt(self.adapter.objects[path].content, DEV_ARCHIVE_PASSPHRASE)

    def put(self, digest: str, encoded: bytes) -> None:
        raise AssertionError((digest, encoded))


def _recover_stored_tags(
    store: MemoryArchiveStore,
    *,
    prefix: str = "archives/archive/opaque-docs",
) -> tuple[CollectionTagHeadDocument, set[str]]:
    head = CollectionTagHeadDocument.from_json_bytes(
        decrypt_age_scrypt(
            store.objects[f"{prefix}/{COLLECTION_TAG_HEAD_RELATIVE_PATH}"],
            DEV_ARCHIVE_PASSPHRASE,
        )
    )
    tags = CollectionTagSet(
        _EncryptedMemoryTagNodes(store, prefix),
        CollectionTagSetRoot.seal(head.root_sha256),
    )
    return head, set(tags.iter_tags())


def _recover_adapter_tags(
    adapter: _VersionedMemoryAdapter,
    *,
    prefix: str = "archives/archive/opaque-docs",
) -> tuple[CollectionTagHeadDocument, set[str]]:
    head = CollectionTagHeadDocument.from_json_bytes(
        decrypt_age_scrypt(
            adapter.objects[f"{prefix}/{COLLECTION_TAG_HEAD_RELATIVE_PATH}"].content,
            DEV_ARCHIVE_PASSPHRASE,
        )
    )
    tags = CollectionTagSet(
        _EncryptedAdapterTagNodes(adapter, prefix),
        CollectionTagSetRoot.seal(head.root_sha256),
    )
    return head, set(tags.iter_tags())


class _CountingTagStore(MemoryArchiveStore):
    def __init__(self) -> None:
        super().__init__()
        self.published_nodes: list[str] = []
        self.deleted_nodes: list[str] = []

    def publish_collection_tag_node(
        self,
        *,
        collection_id: int,
        archive_storage_prefix: str,
        digest: str,
        encoded: bytes,
        passphrase_id: str,
    ) -> CollectionTagObjectReceipt:
        self.published_nodes.append(digest)
        return super().publish_collection_tag_node(
            collection_id=collection_id,
            archive_storage_prefix=archive_storage_prefix,
            digest=digest,
            encoded=encoded,
            passphrase_id=passphrase_id,
        )

    def delete_collection_tag_node(
        self,
        *,
        collection_id: int,
        archive_storage_prefix: str,
        digest: str,
        expected_current_stored_sha256: str,
        provider_revision: str | None,
    ) -> None:
        self.deleted_nodes.append(digest)
        super().delete_collection_tag_node(
            collection_id=collection_id,
            archive_storage_prefix=archive_storage_prefix,
            digest=digest,
            expected_current_stored_sha256=expected_current_stored_sha256,
            provider_revision=provider_revision,
        )


def test_tag_edit_inherits_complete_published_subtrees_without_rewalking_them(
    tmp_path: Path,
) -> None:
    store = _CountingTagStore()
    service, factory, _stored = _service(
        tmp_path / "catalog.sqlite3",
        archive_store=store,
    )
    principal = _principal("source:camera", "workflow:first", "workflow:second")
    with session_scope(factory) as session:  # type: ignore[arg-type]
        collection = session.get(CollectionRecord, 1)
        assert collection is not None
        revision = collection.tag_revision
        identity = collection.tag_set_identity
    first = service.add(
        1,
        tag="workflow:first",
        operation_id="closure-first",
        expected_revision=revision,
        expected_tag_set_identity=identity,
        principal=principal,
    )
    with session_scope(factory) as session:  # type: ignore[arg-type]
        inherited_node_count = session.query(CollectionTagPublishedNodeRecord).count()
    before = len(store.published_nodes)

    service.add(
        1,
        tag="workflow:second",
        operation_id="closure-second",
        expected_revision=int(first["revision"]),
        expected_tag_set_identity=str(first["tag_set_identity"]),
        principal=principal,
    )

    newly_published = len(store.published_nodes) - before
    assert inherited_node_count > newly_published > 0
    head, tags = _recover_stored_tags(store)
    assert head.revision == 3
    assert tags == {"source:camera", "workflow:first", "workflow:second"}


def test_tag_provider_gc_removes_obsolete_parent_before_its_children(tmp_path: Path) -> None:
    store = _CountingTagStore()
    service, factory, _stored = _service(
        tmp_path / "catalog.sqlite3",
        archive_store=store,
    )
    principal = _principal("source:camera", "workflow:archive")
    with session_scope(factory) as session:  # type: ignore[arg-type]
        collection = session.get(CollectionRecord, 1)
        assert collection is not None
        initial_identity = collection.tag_set_identity
    added = service.add(
        1,
        tag="workflow:archive",
        operation_id="gc-parent-add",
        expected_revision=1,
        expected_tag_set_identity=initial_identity,
        principal=principal,
    )
    with session_scope(factory) as session:  # type: ignore[arg-type]
        mutation = session.get(CollectionTagMutationRecord, (1, "gc-parent-add"))
        assert mutation is not None and mutation.result_root_sha256 is not None
        obsolete_parent = mutation.result_root_sha256
        child_digests = set(
            session.scalars(
                select(CollectionTagNodeEdgeRecord.child_digest).where(
                    CollectionTagNodeEdgeRecord.parent_digest == obsolete_parent
                )
            )
        )
        assert child_digests
    service.remove(
        1,
        tag="workflow:archive",
        operation_id="gc-parent-remove",
        expected_revision=2,
        expected_tag_set_identity=str(added["tag_set_identity"]),
        principal=principal,
    )
    with session_scope(factory) as session:  # type: ignore[arg-type]
        publication = session.get(CollectionTagPublicationRecord, (1, "archive"))
        assert publication is not None and publication.published_head_identity is not None
        session.query(CollectionTagPublicationFrontierRecord).filter(
            CollectionTagPublicationFrontierRecord.collection_id == 1,
            CollectionTagPublicationFrontierRecord.store == "archive",
            CollectionTagPublicationFrontierRecord.head_identity
            != publication.published_head_identity,
        ).delete(synchronize_session=False)

    for _ in range(16):
        assert service.process_due(limit=1) in {0, 1}
        if store.deleted_nodes:
            break
    else:  # pragma: no cover - only two superseded tag heads precede node GC
        raise AssertionError("obsolete tag parent was not reclaimed")
    assert store.deleted_nodes == [obsolete_parent]
    with session_scope(factory) as session:  # type: ignore[arg-type]
        assert all(
            session.get(CollectionTagPublishedNodeRecord, (1, "archive", digest)) is not None
            for digest in child_digests
        )


def test_tag_publication_budget_defers_but_does_not_limit_logical_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "catalog.sqlite3"
    service, factory, store = _service(path)
    principal = _principal("source:camera", "workflow:archive")
    with session_scope(factory) as session:  # type: ignore[arg-type]
        collection = session.get(CollectionRecord, 1)
        assert collection is not None
        initial_identity = collection.tag_set_identity
    monkeypatch.setattr(
        "riverhog_core.services.collection_tags._SYNCHRONOUS_PUBLICATION_STEP_BUDGET",
        1,
    )

    with pytest.raises(ServiceUnavailable, match="continues asynchronously"):
        service.add(
            1,
            tag="workflow:archive",
            operation_id="deferred-publication",
            expected_revision=1,
            expected_tag_set_identity=initial_identity,
            principal=principal,
        )

    restarted = SqlAlchemyCollectionTagService(
        RuntimeConfig(database_url=sqlite_url(path)),
        ArchiveStoreRegistry({"archive": archive_store_binding(store)}),
        session_factory=make_session_factory(sqlite_url(path)),
    )
    for _ in range(128):
        progressed = restarted.process_due(limit=1)
        with session_scope(factory) as session:  # type: ignore[arg-type]
            mutation = session.get(CollectionTagMutationRecord, (1, "deferred-publication"))
            if mutation is not None and mutation.state == "succeeded":
                break
        assert progressed == 1
    else:  # pragma: no cover - one compressed path is much smaller
        raise AssertionError("deferred tag publication did not converge")

    replay = restarted.add(
        1,
        tag="workflow:archive",
        operation_id="deferred-publication",
        expected_revision=1,
        expected_tag_set_identity=initial_identity,
        principal=principal,
    )
    assert replay["revision"] == 2
    _head, tags = _recover_stored_tags(store)
    assert tags == {"source:camera", "workflow:archive"}


def test_tag_mutation_is_exact_replayable_and_aba_safe(tmp_path: Path) -> None:
    service, factory, _store = _service(tmp_path / "catalog.sqlite3")
    principal = _principal("source:camera", "workflow:archive")
    with session_scope(factory) as session:  # type: ignore[arg-type]
        collection = session.get(CollectionRecord, 1)
        assert collection is not None
        initial_identity = collection.tag_set_identity

    added = service.add(
        1,
        tag="workflow:archive",
        operation_id="add-workflow",
        expected_revision=1,
        expected_tag_set_identity=initial_identity,
        principal=principal,
    )
    assert added["changed"] is True
    assert added["revision"] == 2
    assert (
        service.add(
            1,
            tag="workflow:archive",
            operation_id="add-workflow",
            expected_revision=1,
            expected_tag_set_identity=initial_identity,
            principal=principal,
        )
        == added
    )

    removed = service.remove(
        1,
        tag="workflow:archive",
        operation_id="remove-workflow",
        expected_revision=2,
        expected_tag_set_identity=str(added["tag_set_identity"]),
        principal=principal,
    )
    assert removed["revision"] == 3
    assert removed["tag_set_identity"] == initial_identity
    with pytest.raises(PreconditionFailed):
        service.add(
            1,
            tag="workflow:archive",
            operation_id="stale-after-aba",
            expected_revision=1,
            expected_tag_set_identity=initial_identity,
            principal=principal,
        )

    with session_scope(factory) as session:  # type: ignore[arg-type]
        revisions = list(
            session.scalars(
                select(CollectionTagRevisionRecord.revision).order_by(
                    CollectionTagRevisionRecord.revision
                )
            )
        )
    assert revisions == [1, 2, 3]


def test_tag_addition_cannot_grant_its_own_collection_access(tmp_path: Path) -> None:
    service, factory, _store = _service(tmp_path / "catalog.sqlite3")
    with session_scope(factory) as session:  # type: ignore[arg-type]
        collection = session.get(CollectionRecord, 1)
        assert collection is not None
        identity = collection.tag_set_identity

    with pytest.raises(NotFound):
        service.add(
            1,
            tag="workflow:archive",
            operation_id="self-authorizing-add",
            expected_revision=1,
            expected_tag_set_identity=identity,
            principal=_principal("workflow:archive"),
        )


def test_tag_removal_emits_exact_loss_of_visibility_without_event_tag_snapshots(
    tmp_path: Path,
) -> None:
    path = tmp_path / "catalog.sqlite3"
    tags, factory, _store = _service(path)
    principal = _principal("source:camera")
    catalog = SqlAlchemyCatalogSyncService(
        RuntimeConfig(
            database_url=sqlite_url(path),
            browse_token_signing_key="catalog-tag-visibility-test-key-v1",
        ),
        session_factory=factory,
    )
    with session_scope(factory) as session:  # type: ignore[arg-type]
        collection = session.get(CollectionRecord, 1)
        assert collection is not None
        event = begin_catalog_event(
            session,
            change="created",
            collection_id=1,
            occurred_at=utc_timestamp_now(),
            inventory_identity=collection.inventory_identity,
            before_tag_revision=None,
            after_tag_revision=1,
        )
        publish_catalog_event(session, event=event)
    checkpoint = catalog.checkpoint(principal=principal)
    baseline = catalog.collections(
        cursor=checkpoint.catalog_cursor,
        limit=1,
        principal=principal,
    )
    assert [item.collection_id for item in baseline.collections] == [1]
    assert baseline.changes_cursor is not None

    removed = tags.remove(
        1,
        tag="source:camera",
        operation_id="remove-own-visibility",
        expected_revision=1,
        expected_tag_set_identity=baseline.collections[0].tag_set_identity,
        principal=principal,
    )
    assert removed["revision"] == 2
    catchup = catalog.changes(
        cursor=baseline.changes_cursor,
        limit=1,
        principal=principal,
    )
    assert catchup.changes == [] and catchup.caught_up is True
    changes = catalog.changes(
        cursor=catchup.next_cursor,
        limit=1,
        principal=principal,
    )

    assert changes.changes == [CatalogSyncDelete(collection_id=1, revision="2")]
    with session_scope(factory) as session:  # type: ignore[arg-type]
        intervals = list(session.scalars(select(CollectionTagVisibilityRecord)))
        assert len(intervals) == 1
        assert intervals[0].start_revision == 1
        assert intervals[0].end_revision == 2


def test_tag_exact_revision_membership_and_bounded_pages(tmp_path: Path) -> None:
    service, factory, _store = _service(tmp_path / "catalog.sqlite3")
    principal = _principal("source:camera", "workflow:archive")
    with session_scope(factory) as session:  # type: ignore[arg-type]
        collection = session.get(CollectionRecord, 1)
        assert collection is not None
        initial_identity = collection.tag_set_identity
    added = service.add(
        1,
        tag="workflow:archive",
        operation_id="add-workflow",
        expected_revision=1,
        expected_tag_set_identity=initial_identity,
        principal=principal,
    )

    assert (
        service.contains(
            1,
            tag="workflow:archive",
            revision=2,
            tag_set_identity=str(added["tag_set_identity"]),
            principal=principal,
        )["present"]
        is True
    )
    first = service.list_collection(
        1,
        page_size=1,
        position=None,
        expected_revision=2,
        expected_tag_set_identity=str(added["tag_set_identity"]),
        principal=principal,
    )
    assert len(first["tags"]) == 1
    assert first["_next_position"] is not None
    second = service.list_collection(
        1,
        page_size=1,
        position=first["_next_position"],  # type: ignore[arg-type]
        expected_revision=2,
        expected_tag_set_identity=str(added["tag_set_identity"]),
        principal=principal,
    )
    assert set(first["tags"]) | set(second["tags"]) == {
        "source:camera",
        "workflow:archive",
    }


def test_pending_mutation_nodes_survive_maintenance_and_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "catalog.sqlite3"
    service, factory, store = _service(path)
    principal = _principal("source:camera", "workflow:archive")
    with session_scope(factory) as session:  # type: ignore[arg-type]
        collection = session.get(CollectionRecord, 1)
        assert collection is not None
        initial_identity = collection.tag_set_identity

    entered = threading.Event()
    resume = threading.Event()
    finish = service._finish_mutation

    def paused_finish(collection_id: int, operation_id: str) -> None:
        entered.set()
        assert resume.wait(timeout=10)
        finish(collection_id, operation_id)

    monkeypatch.setattr(service, "_finish_mutation", paused_finish)
    catalog = SqlAlchemyCatalogSyncService(
        RuntimeConfig(database_url=sqlite_url(path)),
        session_factory=factory,
    )
    with ThreadPoolExecutor(max_workers=1) as executor:
        mutation = executor.submit(
            service.add,
            1,
            tag="workflow:archive",
            operation_id="pause-after-construction",
            expected_revision=1,
            expected_tag_set_identity=initial_identity,
            principal=principal,
        )
        assert entered.wait(timeout=10)
        with session_scope(factory) as session:  # type: ignore[arg-type]
            pending = session.get(
                CollectionTagMutationRecord,
                (1, "pause-after-construction"),
            )
            protected = set(
                session.scalars(
                    select(CollectionTagMutationNodeReferenceRecord.node_digest).where(
                        CollectionTagMutationNodeReferenceRecord.collection_id == 1,
                        CollectionTagMutationNodeReferenceRecord.operation_id
                        == "pause-after-construction",
                    )
                )
            )
            assert pending is not None and pending.state == "pending"
            assert protected

        assert catalog.reap_expired_history(limit=100) == 0
        with session_scope(factory) as session:  # type: ignore[arg-type]
            assert protected <= set(session.scalars(select(CollectionTagNodeRecord.digest)))
            assert protected == set(
                session.scalars(
                    select(CollectionTagMutationNodeReferenceRecord.node_digest).where(
                        CollectionTagMutationNodeReferenceRecord.collection_id == 1,
                        CollectionTagMutationNodeReferenceRecord.operation_id
                        == "pause-after-construction",
                    )
                )
            )
        resume.set()
        result = mutation.result(timeout=10)

    restarted = SqlAlchemyCollectionTagService(
        RuntimeConfig(database_url=sqlite_url(path)),
        ArchiveStoreRegistry({"archive": archive_store_binding(store)}),
        session_factory=make_session_factory(sqlite_url(path)),
    )
    first = restarted.list_collection(
        1,
        page_size=1,
        position=None,
        expected_revision=2,
        expected_tag_set_identity=str(result["tag_set_identity"]),
        principal=principal,
    )
    second = restarted.list_collection(
        1,
        page_size=1,
        position=first["_next_position"],  # type: ignore[arg-type]
        expected_revision=2,
        expected_tag_set_identity=str(result["tag_set_identity"]),
        principal=principal,
    )
    assert set(first["tags"]) | set(second["tags"]) == {
        "source:camera",
        "workflow:archive",
    }
    head, recovered = _recover_stored_tags(store)
    assert head.revision == 2
    assert recovered == {"source:camera", "workflow:archive"}

    catalog.reap_expired_history(limit=100)
    with session_scope(factory) as session:  # type: ignore[arg-type]
        assert not list(session.scalars(select(CollectionTagMutationNodeReferenceRecord)))


def test_pending_mutation_reuses_a_retiring_root_without_a_retention_gap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "catalog.sqlite3"
    service, factory, store = _service(path)
    principal = _principal("source:camera", "workflow:archive")
    with session_scope(factory) as session:  # type: ignore[arg-type]
        collection = session.get(CollectionRecord, 1)
        assert collection is not None
        initial_identity = collection.tag_set_identity
        initial_root = collection.tag_root_sha256
    added = service.add(
        1,
        tag="workflow:archive",
        operation_id="establish-intermediate-root",
        expected_revision=1,
        expected_tag_set_identity=initial_identity,
        principal=principal,
    )
    with session_scope(factory) as session:  # type: ignore[arg-type]
        retired = session.get(CollectionTagRevisionRecord, (1, 1))
        assert retired is not None and retired.root_sha256 == initial_root
        retired.cleanup_started_at = "2026-01-01T00:00:00.000000Z"

    entered = threading.Event()
    resume = threading.Event()
    finish = service._finish_mutation

    def paused_finish(collection_id: int, operation_id: str) -> None:
        entered.set()
        assert resume.wait(timeout=10)
        finish(collection_id, operation_id)

    monkeypatch.setattr(service, "_finish_mutation", paused_finish)
    with ThreadPoolExecutor(max_workers=1) as executor:
        mutation = executor.submit(
            service.remove,
            1,
            tag="workflow:archive",
            operation_id="reuse-retiring-root",
            expected_revision=2,
            expected_tag_set_identity=str(added["tag_set_identity"]),
            principal=principal,
        )
        assert entered.wait(timeout=10)
        with session_scope(factory) as session:  # type: ignore[arg-type]
            pending = session.get(CollectionTagMutationRecord, (1, "reuse-retiring-root"))
            assert pending is not None and pending.result_root_sha256 == initial_root
            assert session.get(CollectionTagNodeRecord, initial_root) is not None
            assert session.scalar(
                select(
                    exists().where(
                        CollectionTagPublicationFrontierRecord.collection_id == 1,
                        CollectionTagPublicationFrontierRecord.head_identity
                        == pending.result_head_identity,
                        CollectionTagPublicationFrontierRecord.node_digest == initial_root,
                    )
                )
            )

        with session_scope(factory) as session:  # type: ignore[arg-type]
            _reap_unreferenced_tag_history(
                session,
                limit=1_000,
                cleanup_before="2026-02-01T00:00:00.000000Z",
                cleanup_started_at="2026-02-01T00:00:00.000000Z",
            )
        with session_scope(factory) as session:  # type: ignore[arg-type]
            assert session.get(CollectionTagRevisionRecord, (1, 1)) is None
            assert session.get(CollectionTagNodeRecord, initial_root) is not None
        resume.set()
        result = mutation.result(timeout=10)

    assert result["revision"] == 3
    assert result["tag_set_identity"] == initial_identity
    head, recovered = _recover_stored_tags(store)
    assert head.revision == 3
    assert recovered == {"source:camera"}


def test_maximum_length_tag_is_a_nonfinal_browse_page(tmp_path: Path) -> None:
    service, factory, _store = _service(tmp_path / "catalog.sqlite3")
    maximum = "m" * COLLECTION_TAG_UTF8_BYTES_MAX
    maximum_digest = collection_tag_sha256(maximum)
    candidates = (f"short/{index}" for index in range(10_000))
    before = next(tag for tag in candidates if collection_tag_sha256(tag) < maximum_digest)
    after = next(tag for tag in candidates if collection_tag_sha256(tag) > maximum_digest)
    with session_scope(factory) as session:  # type: ignore[arg-type]
        tag_set, _created = build_collection_tag_set(session, (before, maximum, after))
        revision = session.get(CollectionTagRevisionRecord, (1, 1))
        assert revision is not None
        revision.root_sha256 = tag_set.root.root_sha256
        revision.tag_set_identity = tag_set.identity
    principal = ApplicationPrincipal(
        app="catalog-reader",
        key_id="catalog-reader-key",
        access=frozenset({ApplicationAccess(CATALOG_READ, ALL_RESOURCES)}),
    )

    first = service.list_collection(
        1,
        page_size=1,
        position=None,
        expected_revision=1,
        expected_tag_set_identity=tag_set.identity,
        principal=principal,
    )
    second = service.list_collection(
        1,
        page_size=1,
        position=first["_next_position"],  # type: ignore[arg-type]
        expected_revision=1,
        expected_tag_set_identity=tag_set.identity,
        principal=principal,
    )
    third = service.list_collection(
        1,
        page_size=1,
        position=second["_next_position"],  # type: ignore[arg-type]
        expected_revision=1,
        expected_tag_set_identity=tag_set.identity,
        principal=principal,
    )

    assert second["tags"] == [maximum]
    assert second["_next_position"] == (maximum_digest,)
    assert third["tags"] == [after]
    assert third["_next_position"] is None


def test_provider_nodes_for_retained_exact_revisions_remain_recoverable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, factory, store = _service(tmp_path / "catalog.sqlite3")
    principal = _principal("source:camera", "workflow:archive")
    with session_scope(factory) as session:  # type: ignore[arg-type]
        collection = session.get(CollectionRecord, 1)
        assert collection is not None
        initial_identity = collection.tag_set_identity
    added = service.add(
        1,
        tag="workflow:archive",
        operation_id="add-workflow",
        expected_revision=1,
        expected_tag_set_identity=initial_identity,
        principal=principal,
    )
    service.remove(
        1,
        tag="workflow:archive",
        operation_id="remove-workflow",
        expected_revision=2,
        expected_tag_set_identity=str(added["tag_set_identity"]),
        principal=principal,
    )
    with session_scope(factory) as session:  # type: ignore[arg-type]
        events = list(session.scalars(select(CatalogEventRecord)))
        assert len(events) == 2
        for event in events:
            event.committed_at = "2026-01-01T00:00:00.000000Z"
    monkeypatch.setattr(
        "riverhog_core.services.catalog_sync.utc_now",
        lambda: datetime(2026, 9, 8, tzinfo=UTC),
    )
    catalog = SqlAlchemyCatalogSyncService(
        RuntimeConfig(
            database_url=sqlite_url(tmp_path / "catalog.sqlite3"),
            catalog_sync_history_retention=timedelta(days=1),
            catalog_sync_bootstrap_lifetime=timedelta(hours=1),
            catalog_sync_cursor_lifetime=timedelta(hours=1),
            browse_token_lifetime=timedelta(hours=1),
        ),
        session_factory=factory,
    )
    assert catalog.reap_expired_history(limit=100) == 2
    monkeypatch.setattr(
        "riverhog_core.services.catalog_sync.utc_now",
        lambda: datetime(2026, 9, 8, 1, 0, 1, tzinfo=UTC),
    )
    for _ in range(256):
        catalog.reap_expired_history(limit=100)
        with session_scope(factory) as session:  # type: ignore[arg-type]
            if list(session.scalars(select(CollectionTagRevisionRecord.revision))) == [3]:
                break
    else:  # pragma: no cover - fixed cleanup state is much smaller
        raise AssertionError("retired exact tag authorities did not converge")
    for _ in range(256):
        if service.process_due(limit=1) == 0:
            break
    else:  # pragma: no cover - fixed-depth tag tree is much smaller
        raise AssertionError("tag-node garbage collection did not settle")

    with session_scope(factory) as session:  # type: ignore[arg-type]
        publication = session.get(CollectionTagPublicationRecord, (1, "archive"))
        assert publication is not None and publication.published_head_identity is not None
        current = set(
            session.scalars(
                select(CollectionTagPublicationFrontierRecord.node_digest).where(
                    CollectionTagPublicationFrontierRecord.collection_id == 1,
                    CollectionTagPublicationFrontierRecord.store == "archive",
                    CollectionTagPublicationFrontierRecord.head_identity
                    == publication.published_head_identity,
                )
            )
        )
        published = set(
            session.scalars(
                select(CollectionTagPublishedNodeRecord.node_digest).where(
                    CollectionTagPublishedNodeRecord.collection_id == 1,
                    CollectionTagPublishedNodeRecord.store == "archive",
                )
            )
        )
    assert current == published
    assert {path for path in store.objects if "/tags/nodes/" in path} == {
        f"archives/archive/opaque-docs/{collection_tag_node_path(digest)}" for digest in published
    }
    recovered_head, recovered_tags = _recover_stored_tags(store)
    assert recovered_head.revision == 3
    assert recovered_tags == {"source:camera"}


class _VersionedTagHeadStore(MemoryArchiveStore):
    def __init__(self) -> None:
        super().__init__()
        self.current_revisions: dict[str, str] = {}
        self.retained_revisions: dict[tuple[str, str], bytes] = {}
        self.deleted_revisions: list[tuple[str, str]] = []
        self.next_revision = 1
        self.lose_next_head_response = False

    def publish_collection_tag_head(
        self,
        *,
        collection_id: int,
        archive_storage_prefix: str,
        document: bytes,
        passphrase_id: str,
        expected_current_stored_sha256: str | None = None,
    ) -> CollectionTagObjectReceipt:
        path = f"{archive_storage_prefix}/{COLLECTION_TAG_HEAD_RELATIVE_PATH}"
        prior = self.objects.get(path)
        prior_revision = self.current_revisions.get(path)
        receipt = super().publish_collection_tag_head(
            collection_id=collection_id,
            archive_storage_prefix=archive_storage_prefix,
            document=document,
            passphrase_id=passphrase_id,
            expected_current_stored_sha256=expected_current_stored_sha256,
        )
        if prior is self.objects.get(path) and prior_revision is not None:
            return replace(receipt, revision=prior_revision)
        if prior is not None and prior_revision is not None:
            self.retained_revisions[(path, prior_revision)] = prior
        revision = f"tag-head-revision-{self.next_revision}"
        self.next_revision += 1
        self.current_revisions[path] = revision
        result = replace(receipt, revision=revision)
        if self.lose_next_head_response:
            self.lose_next_head_response = False
            raise OSError("ambiguous tag-head replacement response")
        return result

    def delete_collection_document_revision(
        self,
        *,
        object_path: str,
        provider_revision: str,
        expected_stored_sha256: str,
    ) -> None:
        self.deleted_revisions.append((object_path, provider_revision))
        if self.current_revisions.get(object_path) == provider_revision:
            raise RuntimeError("refusing to reclaim current mutable collection document")
        prior = self.retained_revisions.get((object_path, provider_revision))
        if prior is None:
            return
        if hashlib.sha256(prior).hexdigest() != expected_stored_sha256:
            raise RuntimeError("mutable collection document revision differs")
        del self.retained_revisions[(object_path, provider_revision)]


@pytest.mark.parametrize("destination_initialized", (False, True))
def test_tag_replica_reconciles_exact_ambiguous_attempt_before_newer_desired(
    tmp_path: Path,
    destination_initialized: bool,
) -> None:
    path = tmp_path / "catalog.sqlite3"
    service, factory, primary = _service(path)
    mirror = _VersionedTagHeadStore()
    config = RuntimeConfig(database_url=sqlite_url(path))
    registry = ArchiveStoreRegistry(
        {
            "archive": archive_store_binding(primary),
            "mirror": archive_store_binding(mirror),
        }
    )
    with session_scope(factory) as session:  # type: ignore[arg-type]
        collection = session.get(CollectionRecord, 1)
        assert collection is not None
        session.add(
            copy := CollectionArchiveCopyRecord(
                collection_id=1,
                store="mirror",
                state="uploaded",
                archive_storage_prefix="archives/mirror/opaque-docs",
                last_uploaded_at=utc_timestamp_now(),
                last_verified_at=utc_timestamp_now(),
            )
        )
        session.flush()
        ensure_tag_publication_for_copy(
            session,
            collection=collection,
            store_name=copy.store,
        )
        initial_identity = collection.tag_set_identity
    service = SqlAlchemyCollectionTagService(
        config,
        registry,
        session_factory=factory,
    )

    if destination_initialized:
        for _ in range(16):
            with session_scope(factory) as session:  # type: ignore[arg-type]
                publication = session.get(CollectionTagPublicationRecord, (1, "mirror"))
                assert publication is not None
                if publication.state == "published":
                    break
            assert service.process_due(limit=1) == 1
        added = service.add(
            1,
            tag="workflow:archive",
            operation_id="replica-add",
            expected_revision=1,
            expected_tag_set_identity=initial_identity,
            principal=_principal("source:camera", "workflow:archive"),
        )
        ambiguous_revision = 2
        expected_revision = 2
        expected_identity = str(added["tag_set_identity"])
    else:
        ambiguous_revision = 1
        expected_revision = 1
        expected_identity = initial_identity

    mirror.lose_next_head_response = True
    for _ in range(16):
        assert service.process_due(limit=1) == 1
        with session_scope(factory) as session:  # type: ignore[arg-type]
            publication = session.get(CollectionTagPublicationRecord, (1, "mirror"))
            assert publication is not None
            if publication.state == "retry_wait":
                break
    with session_scope(factory) as session:  # type: ignore[arg-type]
        attempt = session.get(
            CollectionMutableDocumentPublicationAttemptRecord,
            (1, "mirror", "tag_head"),
        )
        assert attempt is not None and attempt.document_revision == ambiguous_revision

    if destination_initialized:
        newest = service.remove(
            1,
            tag="workflow:archive",
            operation_id="replica-remove",
            expected_revision=expected_revision,
            expected_tag_set_identity=expected_identity,
            principal=_principal("source:camera", "workflow:archive"),
        )
    else:
        newest = service.add(
            1,
            tag="workflow:archive",
            operation_id="replica-add",
            expected_revision=expected_revision,
            expected_tag_set_identity=expected_identity,
            principal=_principal("source:camera", "workflow:archive"),
        )
    with session_scope(factory) as session:  # type: ignore[arg-type]
        publication = session.get(CollectionTagPublicationRecord, (1, "mirror"))
        attempt = session.get(
            CollectionMutableDocumentPublicationAttemptRecord,
            (1, "mirror", "tag_head"),
        )
        assert publication is not None
        assert publication.desired_revision == newest["revision"]
        assert publication.state == "retry_wait"
        assert attempt is not None and attempt.document_revision == ambiguous_revision
        publication.next_attempt_at = utc_timestamp_now()
        attempted_head = CollectionTagHeadDocument.from_json_bytes(attempt.document_bytes)
        attempted_revision = session.get(
            CollectionTagRevisionRecord,
            (1, ambiguous_revision),
        )
        assert attempted_revision is not None
        attempted_revision.cleanup_started_at = "2026-01-01T00:00:00.000000Z"

    for _ in range(16):
        with session_scope(factory) as session:  # type: ignore[arg-type]
            metrics = _reap_unreferenced_tag_history(
                session,
                limit=100,
                cleanup_before="2026-02-01T00:00:00.000000Z",
                cleanup_started_at="2026-02-01T00:00:00.000000Z",
            )
        if metrics.changed_rows == 0:
            break
    with session_scope(factory) as session:  # type: ignore[arg-type]
        assert session.scalar(
            select(
                exists().where(
                    CollectionTagPublicationFrontierRecord.collection_id == 1,
                    CollectionTagPublicationFrontierRecord.store == "mirror",
                    CollectionTagPublicationFrontierRecord.head_identity
                    == attempted_head.head_identity,
                )
            )
        )
        if attempted_head.root_sha256 is not None:
            assert session.get(CollectionTagNodeRecord, attempted_head.root_sha256) is not None

    restarted = SqlAlchemyCollectionTagService(
        config,
        registry,
        session_factory=make_session_factory(config.database_url),
    )
    for _ in range(64):
        if restarted.process_due(limit=1) == 0:
            break

    head, tags = _recover_stored_tags(
        mirror,
        prefix="archives/mirror/opaque-docs",
    )
    assert head.revision == newest["revision"]
    assert tags == (
        {"source:camera"} if destination_initialized else {"source:camera", "workflow:archive"}
    )
    with session_scope(factory) as session:  # type: ignore[arg-type]
        publication = session.get(CollectionTagPublicationRecord, (1, "mirror"))
        assert publication is not None and publication.state == "published"
        assert publication.published_revision == newest["revision"]
        assert session.query(CollectionMutableDocumentPublicationAttemptRecord).count() == 0
        assert session.query(CollectionMutableDocumentReclamationRecord).count() == 0
    assert not mirror.retained_revisions


class _DelayedTagHeadStore(MemoryArchiveStore):
    def __init__(self) -> None:
        super().__init__()
        self.delay_next_head = False
        self.started = threading.Event()
        self.resume = threading.Event()

    def publish_collection_tag_head(
        self,
        *,
        collection_id: int,
        archive_storage_prefix: str,
        document: bytes,
        passphrase_id: str,
        expected_current_stored_sha256: str | None = None,
    ) -> CollectionTagObjectReceipt:
        if self.delay_next_head:
            self.delay_next_head = False
            self.started.set()
            assert self.resume.wait(timeout=10)
        return super().publish_collection_tag_head(
            collection_id=collection_id,
            archive_storage_prefix=archive_storage_prefix,
            document=document,
            passphrase_id=passphrase_id,
            expected_current_stored_sha256=expected_current_stored_sha256,
        )


class _CommittedTagHeadWithoutResponseStore(MemoryArchiveStore):
    def __init__(self) -> None:
        super().__init__()
        self.lose_next_head_response = False

    def publish_collection_tag_head(
        self,
        *,
        collection_id: int,
        archive_storage_prefix: str,
        document: bytes,
        passphrase_id: str,
        expected_current_stored_sha256: str | None = None,
    ) -> CollectionTagObjectReceipt:
        receipt = super().publish_collection_tag_head(
            collection_id=collection_id,
            archive_storage_prefix=archive_storage_prefix,
            document=document,
            passphrase_id=passphrase_id,
            expected_current_stored_sha256=expected_current_stored_sha256,
        )
        if self.lose_next_head_response:
            self.lose_next_head_response = False
            raise OSError("simulated lost response after durable head replacement")
        return receipt


def test_superseded_tag_heads_retain_exact_cleanup_custody(tmp_path: Path) -> None:
    store = _VersionedTagHeadStore()
    service, factory, _store = _service(
        tmp_path / "catalog.sqlite3",
        archive_store=store,
    )
    principal = _principal("source:camera", "workflow:archive")
    with session_scope(factory) as session:  # type: ignore[arg-type]
        collection = session.get(CollectionRecord, 1)
        assert collection is not None
        initial_identity = collection.tag_set_identity
    added = service.add(
        1,
        tag="workflow:archive",
        operation_id="versioned-add",
        expected_revision=1,
        expected_tag_set_identity=initial_identity,
        principal=principal,
    )
    service.remove(
        1,
        tag="workflow:archive",
        operation_id="versioned-remove",
        expected_revision=2,
        expected_tag_set_identity=str(added["tag_set_identity"]),
        principal=principal,
    )

    with session_scope(factory) as session:  # type: ignore[arg-type]
        assert session.query(CollectionMutableDocumentReclamationRecord).count() == 2
    assert len(store.retained_revisions) == 2
    assert (
        process_due_mutable_document_reclamations(
            factory,
            ArchiveStoreRegistry({"archive": archive_store_binding(store)}),
            document_kind="tag_head",
            limit=1,
        )
        == 1
    )
    assert len(store.retained_revisions) == 1
    assert (
        process_due_mutable_document_reclamations(
            factory,
            ArchiveStoreRegistry({"archive": archive_store_binding(store)}),
            document_kind="tag_head",
            limit=1,
        )
        == 1
    )
    with session_scope(factory) as session:  # type: ignore[arg-type]
        assert session.query(CollectionMutableDocumentReclamationRecord).count() == 0
    assert not store.retained_revisions
    head, tags = _recover_stored_tags(store)
    assert head.revision == 3
    assert tags == {"source:camera"}


def test_committed_tag_head_reconciles_after_its_response_is_lost(tmp_path: Path) -> None:
    store = _CommittedTagHeadWithoutResponseStore()
    service, factory, _store = _service(tmp_path / "catalog.sqlite3", archive_store=store)
    principal = _principal("source:camera", "workflow:archive")
    with session_scope(factory) as session:  # type: ignore[arg-type]
        collection = session.get(CollectionRecord, 1)
        assert collection is not None
        initial_identity = collection.tag_set_identity
    store.lose_next_head_response = True

    with pytest.raises(ServiceUnavailable):
        service.add(
            1,
            tag="workflow:archive",
            operation_id="lost-head-response",
            expected_revision=1,
            expected_tag_set_identity=initial_identity,
            principal=principal,
        )

    result = service.add(
        1,
        tag="workflow:archive",
        operation_id="lost-head-response",
        expected_revision=1,
        expected_tag_set_identity=initial_identity,
        principal=principal,
    )
    assert result["revision"] == 2
    head, tags = _recover_stored_tags(store)
    assert head.revision == 2
    assert tags == {"source:camera", "workflow:archive"}


def test_delayed_old_head_writer_cannot_overwrite_newer_acknowledged_authority(
    tmp_path: Path,
) -> None:
    store = _DelayedTagHeadStore()
    service, factory, _store = _service(tmp_path / "catalog.sqlite3", archive_store=store)
    principal = _principal("source:camera", "workflow:archive")
    with session_scope(factory) as session:  # type: ignore[arg-type]
        collection = session.get(CollectionRecord, 1)
        assert collection is not None
        initial_identity = collection.tag_set_identity
    store.delay_next_head = True

    with ThreadPoolExecutor(max_workers=1) as executor:
        delayed = executor.submit(
            service.add,
            1,
            tag="workflow:archive",
            operation_id="delayed-add",
            expected_revision=1,
            expected_tag_set_identity=initial_identity,
            principal=principal,
        )
        assert store.started.wait(timeout=10)
        restarted = SqlAlchemyCollectionTagService(
            RuntimeConfig(database_url=sqlite_url(tmp_path / "catalog.sqlite3")),
            ArchiveStoreRegistry({"archive": archive_store_binding(store)}),
            session_factory=factory,
        )
        assert restarted.requeue_interrupted_for_startup(limit=1) == 1
        assert restarted.process_due(limit=1) == 1
        with session_scope(factory) as session:  # type: ignore[arg-type]
            added = session.get(CollectionTagMutationRecord, (1, "delayed-add"))
            assert added is not None and added.state == "succeeded"
            assert added.result_revision == 2
            added_identity = added.result_tag_set_identity
        removed = restarted.remove(
            1,
            tag="workflow:archive",
            operation_id="newer-remove",
            expected_revision=2,
            expected_tag_set_identity=added_identity,
            principal=principal,
        )
        assert removed["revision"] == 3
        store.resume.set()
        delayed.result(timeout=10)

    head, tags = _recover_stored_tags(store)
    assert head.revision == 3
    assert tags == {"source:camera"}


class _DelayedTagDeleteStore(MemoryArchiveStore):
    def __init__(self) -> None:
        super().__init__()
        self.delay_next_delete = False
        self.started = threading.Event()
        self.resume = threading.Event()

    def delete_collection_tag_node(
        self,
        *,
        collection_id: int,
        archive_storage_prefix: str,
        digest: str,
        expected_current_stored_sha256: str,
        provider_revision: str | None,
    ) -> None:
        if self.delay_next_delete:
            self.delay_next_delete = False
            self.started.set()
            assert self.resume.wait(timeout=10)
        super().delete_collection_tag_node(
            collection_id=collection_id,
            archive_storage_prefix=archive_storage_prefix,
            digest=digest,
            expected_current_stored_sha256=expected_current_stored_sha256,
            provider_revision=provider_revision,
        )


class _TagDeleteCompletionRaceStore(MemoryArchiveStore):
    def __init__(self) -> None:
        super().__init__()
        self.delay_after_effect_digest: str | None = None
        self.delay_before_effect_digest: str | None = None
        self.fail_delayed_result = False
        self.old_effect_applied = threading.Event()
        self.release_old_result = threading.Event()
        self.successor_effect_started = threading.Event()
        self.release_successor_effect = threading.Event()
        self._delay_lock = threading.Lock()

    def delete_collection_tag_node(
        self,
        *,
        collection_id: int,
        archive_storage_prefix: str,
        digest: str,
        expected_current_stored_sha256: str,
        provider_revision: str | None,
    ) -> None:
        with self._delay_lock:
            delay_after = self.delay_after_effect_digest == digest
            delay_before = self.delay_before_effect_digest == digest
            if delay_after:
                self.delay_after_effect_digest = None
            if delay_before:
                self.delay_before_effect_digest = None
        if delay_before:
            self.successor_effect_started.set()
            assert self.release_successor_effect.wait(timeout=10)
        super().delete_collection_tag_node(
            collection_id=collection_id,
            archive_storage_prefix=archive_storage_prefix,
            digest=digest,
            expected_current_stored_sha256=expected_current_stored_sha256,
            provider_revision=provider_revision,
        )
        if delay_after:
            self.old_effect_applied.set()
            assert self.release_old_result.wait(timeout=10)
            if self.fail_delayed_result:
                raise RuntimeError("delayed provider completion failed")


def _advance_until_tag_delete_pauses(
    service: SqlAlchemyCollectionTagService,
    paused: threading.Event,
) -> int:
    for _ in range(256):
        progressed = service.process_due(limit=1)
        if paused.is_set():
            return progressed
        if progressed == 0:
            break
    raise AssertionError("expected tag-node reclamation did not reach the provider")


def _remove_retired_tag_publication_frontiers(factory: object) -> None:
    with session_scope(factory) as session:  # type: ignore[arg-type]
        publication = session.get(CollectionTagPublicationRecord, (1, "archive"))
        assert publication is not None and publication.published_head_identity is not None
        session.query(CollectionTagPublicationFrontierRecord).filter(
            CollectionTagPublicationFrontierRecord.collection_id == 1,
            CollectionTagPublicationFrontierRecord.store == "archive",
            CollectionTagPublicationFrontierRecord.head_identity
            != publication.published_head_identity,
        ).delete(synchronize_session=False)


def _assert_delayed_old_tag_gc_result_cannot_change_successor(
    *,
    path: Path | None,
    database_url: str | None = None,
    fail_old_result: bool,
) -> None:
    store = _TagDeleteCompletionRaceStore()
    service, factory, _stored = _service(
        path,
        archive_store=store,
        database_url=database_url,
    )
    principal = _principal("source:camera", "workflow:archive")
    with session_scope(factory) as session:  # type: ignore[arg-type]
        collection = session.get(CollectionRecord, 1)
        assert collection is not None
        initial_identity = collection.tag_set_identity
    added = service.add(
        1,
        tag="workflow:archive",
        operation_id="generation-fence-add",
        expected_revision=1,
        expected_tag_set_identity=initial_identity,
        principal=principal,
    )
    with session_scope(factory) as session:  # type: ignore[arg-type]
        mutation = session.get(CollectionTagMutationRecord, (1, "generation-fence-add"))
        assert mutation is not None and mutation.result_root_sha256 is not None
        digest = mutation.result_root_sha256
    service.remove(
        1,
        tag="workflow:archive",
        operation_id="generation-fence-remove",
        expected_revision=2,
        expected_tag_set_identity=str(added["tag_set_identity"]),
        principal=principal,
    )
    for _ in range(256):
        if service.process_due(limit=1) == 0:
            break
    _remove_retired_tag_publication_frontiers(factory)

    store.delay_after_effect_digest = digest
    store.fail_delayed_result = fail_old_result
    config_url = database_url if database_url is not None else sqlite_url(path)
    restarted = SqlAlchemyCollectionTagService(
        RuntimeConfig(database_url=config_url),
        ArchiveStoreRegistry({"archive": archive_store_binding(store)}),
        session_factory=factory,  # type: ignore[arg-type]
    )
    with ThreadPoolExecutor(max_workers=2) as executor:
        old_result = executor.submit(
            _advance_until_tag_delete_pauses,
            service,
            store.old_effect_applied,
        )
        successor_result = None
        try:
            assert store.old_effect_applied.wait(timeout=10)
            with session_scope(factory) as session:  # type: ignore[arg-type]
                old_gc = session.get(CollectionTagNodeGcRecord, (1, "archive", digest))
                assert old_gc is not None and old_gc.state == "deleting"
                old_generation_identity = old_gc.expected_head_identity

            assert restarted.requeue_interrupted_for_startup(limit=1) == 1
            assert restarted.process_due(limit=1) == 1
            with session_scope(factory) as session:  # type: ignore[arg-type]
                assert session.get(CollectionTagNodeGcRecord, (1, "archive", digest)) is None
                assert session.get(CollectionTagPublishedNodeRecord, (1, "archive", digest)) is None

            readded = restarted.add(
                1,
                tag="workflow:archive",
                operation_id="generation-fence-readd",
                expected_revision=3,
                expected_tag_set_identity=initial_identity,
                principal=principal,
            )
            restarted.remove(
                1,
                tag="workflow:archive",
                operation_id="generation-fence-reremove",
                expected_revision=4,
                expected_tag_set_identity=str(readded["tag_set_identity"]),
                principal=principal,
            )
            _remove_retired_tag_publication_frontiers(factory)

            store.delay_before_effect_digest = digest
            successor_result = executor.submit(
                _advance_until_tag_delete_pauses,
                restarted,
                store.successor_effect_started,
            )
            assert store.successor_effect_started.wait(timeout=10)
            with session_scope(factory) as session:  # type: ignore[arg-type]
                successor_gc = session.get(CollectionTagNodeGcRecord, (1, "archive", digest))
                published = session.get(
                    CollectionTagPublishedNodeRecord,
                    (1, "archive", digest),
                )
                assert successor_gc is not None and successor_gc.state == "deleting"
                assert successor_gc.expected_head_identity != old_generation_identity
                assert successor_gc.failure is None
                assert published is not None
                successor_identity = successor_gc.expected_head_identity
                successor_receipt = (
                    published.object_path,
                    published.provider_revision,
                    published.stored_bytes,
                    published.stored_sha256,
                )
                assert successor_receipt == (
                    successor_gc.object_path,
                    successor_gc.provider_revision,
                    published.stored_bytes,
                    published.stored_sha256,
                )

            store.release_old_result.set()
            assert old_result.result(timeout=10) == 1
            with session_scope(factory) as session:  # type: ignore[arg-type]
                successor_gc = session.get(CollectionTagNodeGcRecord, (1, "archive", digest))
                published = session.get(
                    CollectionTagPublishedNodeRecord,
                    (1, "archive", digest),
                )
                assert successor_gc is not None
                assert successor_gc.expected_head_identity == successor_identity
                assert successor_gc.state == "deleting"
                assert successor_gc.failure is None
                assert published is not None
                assert (
                    published.object_path,
                    published.provider_revision,
                    published.stored_bytes,
                    published.stored_sha256,
                ) == successor_receipt

            store.release_successor_effect.set()
            assert successor_result.result(timeout=10) == 1
        finally:
            store.release_old_result.set()
            store.release_successor_effect.set()

    with session_scope(factory) as session:  # type: ignore[arg-type]
        assert session.get(CollectionTagNodeGcRecord, (1, "archive", digest)) is None
        assert session.get(CollectionTagPublishedNodeRecord, (1, "archive", digest)) is None
    assert f"archives/archive/opaque-docs/{collection_tag_node_path(digest)}" not in store.objects


def test_delayed_gc_cannot_delete_a_node_republished_by_a_newer_authority(
    tmp_path: Path,
) -> None:
    path = tmp_path / "catalog.sqlite3"
    store = _DelayedTagDeleteStore()
    service, factory, _store = _service(path, archive_store=store)
    principal = _principal("source:camera", "workflow:archive")
    with session_scope(factory) as session:  # type: ignore[arg-type]
        collection = session.get(CollectionRecord, 1)
        assert collection is not None
        initial_identity = collection.tag_set_identity
    added = service.add(
        1,
        tag="workflow:archive",
        operation_id="add-before-gc",
        expected_revision=1,
        expected_tag_set_identity=initial_identity,
        principal=principal,
    )
    service.remove(
        1,
        tag="workflow:archive",
        operation_id="remove-before-gc",
        expected_revision=2,
        expected_tag_set_identity=str(added["tag_set_identity"]),
        principal=principal,
    )
    for _ in range(256):
        if service.process_due(limit=1) == 0:
            break
    with session_scope(factory) as session:  # type: ignore[arg-type]
        publication = session.get(CollectionTagPublicationRecord, (1, "archive"))
        assert publication is not None and publication.published_head_identity is not None
        session.query(CollectionTagPublicationFrontierRecord).filter(
            CollectionTagPublicationFrontierRecord.collection_id == 1,
            CollectionTagPublicationFrontierRecord.store == "archive",
            CollectionTagPublicationFrontierRecord.head_identity
            != publication.published_head_identity,
        ).delete(synchronize_session=False)

    store.delay_next_delete = True
    with ThreadPoolExecutor(max_workers=1) as executor:
        delayed = executor.submit(service.process_due, limit=1)
        assert store.started.wait(timeout=10)
        restarted = SqlAlchemyCollectionTagService(
            RuntimeConfig(database_url=sqlite_url(path)),
            ArchiveStoreRegistry({"archive": archive_store_binding(store)}),
            session_factory=factory,
        )
        assert restarted.requeue_interrupted_for_startup(limit=1) == 1
        assert restarted.process_due(limit=1) == 1
        readded = restarted.add(
            1,
            tag="workflow:archive",
            operation_id="readd-after-gc",
            expected_revision=3,
            expected_tag_set_identity=initial_identity,
            principal=principal,
        )
        assert readded["revision"] == 4
        store.resume.set()
        assert delayed.result(timeout=10) == 1

    head, tags = _recover_stored_tags(store)
    assert head.revision == 4
    assert tags == {"source:camera", "workflow:archive"}


def test_delayed_success_from_old_tag_gc_cannot_consume_successor(
    tmp_path: Path,
) -> None:
    _assert_delayed_old_tag_gc_result_cannot_change_successor(
        path=tmp_path / "delayed-success.sqlite3",
        fail_old_result=False,
    )


def test_delayed_failure_from_old_tag_gc_cannot_reschedule_successor(
    tmp_path: Path,
) -> None:
    _assert_delayed_old_tag_gc_result_cannot_change_successor(
        path=tmp_path / "delayed-failure.sqlite3",
        fail_old_result=True,
    )


class _AmbiguousTagDeleteStore(MemoryArchiveStore):
    fail_delete_once = True

    def delete_collection_tag_node(
        self,
        *,
        collection_id: int,
        archive_storage_prefix: str,
        digest: str,
        expected_current_stored_sha256: str,
        provider_revision: str | None,
    ) -> None:
        super().delete_collection_tag_node(
            collection_id=collection_id,
            archive_storage_prefix=archive_storage_prefix,
            digest=digest,
            expected_current_stored_sha256=expected_current_stored_sha256,
            provider_revision=provider_revision,
        )
        if self.fail_delete_once:
            self.fail_delete_once = False
            raise RuntimeError("ambiguous provider response")


class _InterruptedExactTagNodeDeleteAdapter(_VersionedMemoryAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.fail_exact_path: str | None = None
        self.interrupted_revision: str | None = None

    def delete_object(self, request: DeleteObjectRequest) -> None:
        path = request.object.object_path
        if request.mode == "current":
            self.deleted.append(request)
            current = self.objects.get(path)
            if current is None:
                return
            expected = request.expected_current_stored_sha256
            if expected is not None:
                assert hashlib.sha256(current.content).hexdigest() == expected
            self.revisions[(path, current.revision)] = current
            del self.objects[path]
            return
        if path == self.fail_exact_path:
            assert request.mode == "exact_revision"
            assert request.object.revision is not None
            self.deleted.append(request)
            self.interrupted_revision = request.object.revision
            self.fail_exact_path = None
            raise RuntimeError("exact provider revision cleanup was interrupted")
        super().delete_object(request)


def _expire_prior_tag_authorities(
    factory: object,
    *,
    database_url: str,
    current_revision: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with session_scope(factory) as session:  # type: ignore[arg-type]
        for event in session.scalars(select(CatalogEventRecord)):
            event.committed_at = "2026-01-01T00:00:00.000000Z"
    monkeypatch.setattr(
        "riverhog_core.services.catalog_sync.utc_now",
        lambda: datetime(2026, 9, 8, tzinfo=UTC),
    )
    catalog = SqlAlchemyCatalogSyncService(
        RuntimeConfig(
            database_url=database_url,
            catalog_sync_history_retention=timedelta(days=1),
            catalog_sync_bootstrap_lifetime=timedelta(hours=1),
            catalog_sync_cursor_lifetime=timedelta(hours=1),
            browse_token_lifetime=timedelta(hours=1),
        ),
        session_factory=factory,  # type: ignore[arg-type]
    )
    catalog.reap_expired_history(limit=64)
    monkeypatch.setattr(
        "riverhog_core.services.catalog_sync.utc_now",
        lambda: datetime(2026, 9, 8, 1, 0, 1, tzinfo=UTC),
    )
    for _ in range(64):
        catalog.reap_expired_history(limit=1)
        with session_scope(factory) as session:  # type: ignore[arg-type]
            revisions = list(
                session.scalars(
                    select(CollectionTagRevisionRecord.revision).order_by(
                        CollectionTagRevisionRecord.revision
                    )
                )
            )
        if revisions == [current_revision]:
            return
    raise AssertionError("retired tag authorities did not expire")


def _assert_unrelated_head_advance_preserves_interrupted_tag_gc(
    *,
    path: Path | None,
    monkeypatch: pytest.MonkeyPatch,
    database_url: str | None = None,
) -> None:
    adapter = _InterruptedExactTagNodeDeleteAdapter()
    archive = StorageAdapterArchiveStore(
        RuntimeConfig(),
        name="archive",
        adapter=adapter,
    )
    service, factory, _unused = _service(
        path,
        archive_store=archive,  # type: ignore[arg-type]
        database_url=database_url,
        archive_storage_prefix="archives/opaque-docs",
    )
    config_url = database_url if database_url is not None else sqlite_url(path)
    principal = _principal("source:camera", "workflow:archive", "where:unrelated")
    with session_scope(factory) as session:  # type: ignore[arg-type]
        collection = session.get(CollectionRecord, 1)
        assert collection is not None
        initial_identity = collection.tag_set_identity
    added = service.add(
        1,
        tag="workflow:archive",
        operation_id="head-advance-add",
        expected_revision=1,
        expected_tag_set_identity=initial_identity,
        principal=principal,
    )
    service.remove(
        1,
        tag="workflow:archive",
        operation_id="head-advance-remove",
        expected_revision=2,
        expected_tag_set_identity=str(added["tag_set_identity"]),
        principal=principal,
    )
    with session_scope(factory) as session:  # type: ignore[arg-type]
        mutation = session.get(CollectionTagMutationRecord, (1, "head-advance-add"))
        assert mutation is not None and mutation.result_root_sha256 is not None
        reclaimed_digest = mutation.result_root_sha256
    _expire_prior_tag_authorities(
        factory,
        database_url=config_url,
        current_revision=3,
        monkeypatch=monkeypatch,
    )

    node_path = f"archives/opaque-docs/{collection_tag_node_path(reclaimed_digest)}"
    assert node_path in adapter.objects
    old_revision = adapter.objects[node_path].revision
    adapter.fail_exact_path = node_path
    for _ in range(128):
        assert service.process_due(limit=1) in {0, 1}
        with session_scope(factory) as session:  # type: ignore[arg-type]
            gc = session.get(CollectionTagNodeGcRecord, (1, "archive", reclaimed_digest))
            if gc is not None and gc.state == "retry_wait":
                gc.next_attempt_at = "9999-12-31T23:59:59.999999Z"
                obligation_head_identity = gc.expected_head_identity
                break
    else:  # pragma: no cover - the fixed-depth tag closure is much smaller
        raise AssertionError("target tag-node cleanup was not interrupted")
    assert adapter.interrupted_revision == old_revision
    assert node_path not in adapter.objects
    assert (node_path, old_revision) in adapter.revisions

    unrelated = service.add(
        1,
        tag="where:unrelated",
        operation_id="head-advance-unrelated-add",
        expected_revision=3,
        expected_tag_set_identity=initial_identity,
        principal=principal,
    )
    with session_scope(factory) as session:  # type: ignore[arg-type]
        publication = session.get(CollectionTagPublicationRecord, (1, "archive"))
        gc = session.get(CollectionTagNodeGcRecord, (1, "archive", reclaimed_digest))
        receipt = session.get(CollectionTagPublishedNodeRecord, (1, "archive", reclaimed_digest))
        assert publication is not None and publication.published_revision == 4
        assert publication.published_head_identity != obligation_head_identity
        assert gc is not None and receipt is not None
        gc.next_attempt_at = utc_timestamp_now()

    restarted = SqlAlchemyCollectionTagService(
        RuntimeConfig(database_url=config_url),
        ArchiveStoreRegistry(
            {"archive": archive_store_binding(archive)}  # type: ignore[arg-type]
        ),
        session_factory=make_session_factory(config_url),
    )
    assert restarted.process_due(limit=1) == 1
    with session_scope(factory) as session:  # type: ignore[arg-type]
        assert session.get(CollectionTagNodeGcRecord, (1, "archive", reclaimed_digest)) is None
        assert (
            session.get(CollectionTagPublishedNodeRecord, (1, "archive", reclaimed_digest)) is None
        )
    assert (node_path, old_revision) not in adapter.revisions

    reverted = restarted.remove(
        1,
        tag="where:unrelated",
        operation_id="head-advance-unrelated-remove",
        expected_revision=4,
        expected_tag_set_identity=str(unrelated["tag_set_identity"]),
        principal=principal,
    )
    readded = restarted.add(
        1,
        tag="workflow:archive",
        operation_id="head-advance-reuse",
        expected_revision=5,
        expected_tag_set_identity=str(reverted["tag_set_identity"]),
        principal=principal,
    )
    with session_scope(factory) as session:  # type: ignore[arg-type]
        mutation = session.get(CollectionTagMutationRecord, (1, "head-advance-reuse"))
        frontier = session.get(
            CollectionTagPublicationFrontierRecord,
            (1, "archive", str(readded["head_identity"]), reclaimed_digest),
        )
        assert mutation is not None and mutation.result_root_sha256 == reclaimed_digest
        assert frontier is not None and frontier.published and frontier.expanded
    assert node_path in adapter.objects
    assert adapter.objects[node_path].revision != old_revision
    assert (node_path, old_revision) not in adapter.revisions
    recovered_head, recovered_tags = _recover_adapter_tags(
        adapter,
        prefix="archives/opaque-docs",
    )
    assert recovered_head.revision == 6
    assert recovered_tags == {"source:camera", "workflow:archive"}


def test_unrelated_head_advance_preserves_interrupted_tag_gc_until_reuse_is_safe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _assert_unrelated_head_advance_preserves_interrupted_tag_gc(
        path=tmp_path / "head-advance.sqlite3",
        monkeypatch=monkeypatch,
    )


class _PersistentTagDeleteFailureStore(MemoryArchiveStore):
    def __init__(self) -> None:
        super().__init__()
        self.fail_deletes = True
        self.delete_attempts = 0

    def delete_collection_tag_node(
        self,
        *,
        collection_id: int,
        archive_storage_prefix: str,
        digest: str,
        expected_current_stored_sha256: str,
        provider_revision: str | None,
    ) -> None:
        self.delete_attempts += 1
        if self.fail_deletes:
            raise RuntimeError("persistent provider failure")
        super().delete_collection_tag_node(
            collection_id=collection_id,
            archive_storage_prefix=archive_storage_prefix,
            digest=digest,
            expected_current_stored_sha256=expected_current_stored_sha256,
            provider_revision=provider_revision,
        )


def test_tag_node_gc_resumes_idempotently_after_an_ambiguous_delete(tmp_path: Path) -> None:
    path = tmp_path / "catalog.sqlite3"
    store = _AmbiguousTagDeleteStore()
    service, factory, _store = _service(path, archive_store=store)
    principal = _principal("source:camera", "workflow:archive")
    with session_scope(factory) as session:  # type: ignore[arg-type]
        collection = session.get(CollectionRecord, 1)
        assert collection is not None
        initial_identity = collection.tag_set_identity
    added = service.add(
        1,
        tag="workflow:archive",
        operation_id="add-workflow",
        expected_revision=1,
        expected_tag_set_identity=initial_identity,
        principal=principal,
    )
    service.remove(
        1,
        tag="workflow:archive",
        operation_id="remove-workflow",
        expected_revision=2,
        expected_tag_set_identity=str(added["tag_set_identity"]),
        principal=principal,
    )
    for _ in range(256):
        if service.process_due(limit=1) == 0:
            break
    with session_scope(factory) as session:  # type: ignore[arg-type]
        publication = session.get(CollectionTagPublicationRecord, (1, "archive"))
        assert publication is not None and publication.published_head_identity is not None
        session.query(CollectionTagPublicationFrontierRecord).filter(
            CollectionTagPublicationFrontierRecord.collection_id == 1,
            CollectionTagPublicationFrontierRecord.store == "archive",
            CollectionTagPublicationFrontierRecord.head_identity
            != publication.published_head_identity,
        ).delete(synchronize_session=False)
    assert service.process_due(limit=1) == 1
    with session_scope(factory) as session:  # type: ignore[arg-type]
        gc = session.scalar(select(CollectionTagNodeGcRecord))
        assert gc is not None and gc.state == "retry_wait"
        gc.next_attempt_at = utc_timestamp_now()
    restarted = SqlAlchemyCollectionTagService(
        RuntimeConfig(database_url=sqlite_url(path)),
        ArchiveStoreRegistry({"archive": archive_store_binding(store)}),
        session_factory=factory,
    )
    for _ in range(256):
        if restarted.process_due(limit=1) == 0:
            break
    with session_scope(factory) as session:  # type: ignore[arg-type]
        assert session.scalar(select(CollectionTagNodeGcRecord)) is None


@pytest.mark.parametrize("restart_state", ("pending", "deleting", "retry_wait"))
def test_reused_tag_node_advances_its_exact_gc_dependency_through_public_maintenance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    restart_state: str,
) -> None:
    path = tmp_path / f"catalog-{restart_state}.sqlite3"
    store = _AmbiguousTagDeleteStore()
    service, factory, _store = _service(path, archive_store=store)
    principal = _principal("source:camera", "workflow:archive")
    with session_scope(factory) as session:  # type: ignore[arg-type]
        collection = session.get(CollectionRecord, 1)
        assert collection is not None
        initial_identity = collection.tag_set_identity
    added = service.add(
        1,
        tag="workflow:archive",
        operation_id="gc-liveness-add",
        expected_revision=1,
        expected_tag_set_identity=initial_identity,
        principal=principal,
    )
    service.remove(
        1,
        tag="workflow:archive",
        operation_id="gc-liveness-remove",
        expected_revision=2,
        expected_tag_set_identity=str(added["tag_set_identity"]),
        principal=principal,
    )

    with session_scope(factory) as session:  # type: ignore[arg-type]
        for event in session.scalars(select(CatalogEventRecord)):
            event.committed_at = "2026-01-01T00:00:00.000000Z"
    monkeypatch.setattr(
        "riverhog_core.services.catalog_sync.utc_now",
        lambda: datetime(2026, 9, 8, tzinfo=UTC),
    )
    catalog = SqlAlchemyCatalogSyncService(
        RuntimeConfig(
            database_url=sqlite_url(path),
            catalog_sync_history_retention=timedelta(days=1),
            catalog_sync_bootstrap_lifetime=timedelta(hours=1),
            catalog_sync_cursor_lifetime=timedelta(hours=1),
            browse_token_lifetime=timedelta(hours=1),
        ),
        session_factory=factory,
    )
    catalog.reap_expired_history(limit=64)
    monkeypatch.setattr(
        "riverhog_core.services.catalog_sync.utc_now",
        lambda: datetime(2026, 9, 8, 1, 0, 1, tzinfo=UTC),
    )
    for _ in range(64):
        catalog.reap_expired_history(limit=1)
        with session_scope(factory) as session:  # type: ignore[arg-type]
            revisions = list(
                session.scalars(
                    select(CollectionTagRevisionRecord.revision).order_by(
                        CollectionTagRevisionRecord.revision
                    )
                )
            )
        if revisions == [3]:
            break
    else:  # pragma: no cover - three tiny authorities have bounded cleanup state
        raise AssertionError("retired tag authorities did not expire")

    for _ in range(128):
        assert service.process_due(limit=1) in {0, 1}
        with session_scope(factory) as session:  # type: ignore[arg-type]
            gc = session.scalar(select(CollectionTagNodeGcRecord))
            if gc is not None and gc.state == "retry_wait":
                reclaimed_digest = gc.node_digest
                break
    else:  # pragma: no cover - fixed-depth tag closure is much smaller
        raise AssertionError("tag-node reclamation did not reach its ambiguous result")

    with pytest.raises(ServiceUnavailable, match="waiting for node cleanup"):
        service.add(
            1,
            tag="workflow:archive",
            operation_id="gc-liveness-readd",
            expected_revision=3,
            expected_tag_set_identity=initial_identity,
            principal=principal,
        )
    with session_scope(factory) as session:  # type: ignore[arg-type]
        publication = session.get(CollectionTagPublicationRecord, (1, "archive"))
        gc = session.get(CollectionTagNodeGcRecord, (1, "archive", reclaimed_digest))
        attempt = session.get(
            CollectionMutableDocumentPublicationAttemptRecord,
            (1, "archive", "tag_head"),
        )
        assert publication is not None and gc is not None and attempt is not None
        assert publication.desired_revision == 4
        assert attempt.document_revision == 4
        assert (
            session.get(
                CollectionTagPublicationFrontierRecord,
                (1, "archive", publication.desired_head_identity, reclaimed_digest),
            )
            is not None
        )

    # Independent due cleanup may advance, but an unchanged future dependency never
    # masquerades as progress once all ready work has drained.
    for _ in range(16):
        if service.process_due(limit=1) == 0:
            break
    else:  # pragma: no cover - only prior-head cleanup can be independently due
        raise AssertionError("future dependency did not become an idle scheduler result")

    with session_scope(factory) as session:  # type: ignore[arg-type]
        gc = session.get(CollectionTagNodeGcRecord, (1, "archive", reclaimed_digest))
        publication = session.get(CollectionTagPublicationRecord, (1, "archive"))
        assert gc is not None and publication is not None
        gc.state = restart_state
        gc.next_attempt_at = utc_timestamp_now()
        publication.next_attempt_at = utc_timestamp_now()
    restarted = SqlAlchemyCollectionTagService(
        RuntimeConfig(database_url=sqlite_url(path)),
        ArchiveStoreRegistry({"archive": archive_store_binding(store)}),
        session_factory=make_session_factory(sqlite_url(path)),
    )
    requeued = restarted.requeue_interrupted_for_startup(limit=1)
    assert requeued == (1 if restart_state == "deleting" else 0)

    for _ in range(128):
        progressed = restarted.process_due(limit=1)
        assert progressed in {0, 1}
        with session_scope(factory) as session:  # type: ignore[arg-type]
            publication = session.get(CollectionTagPublicationRecord, (1, "archive"))
            settled = (
                publication is not None
                and publication.state == "published"
                and publication.published_revision == 4
            )
        if settled:
            break
        assert progressed == 1
    else:  # pragma: no cover - fixed-depth tag closure is much smaller
        raise AssertionError("reused tag authority did not converge")

    replay = restarted.add(
        1,
        tag="workflow:archive",
        operation_id="gc-liveness-readd",
        expected_revision=3,
        expected_tag_set_identity=initial_identity,
        principal=principal,
    )
    assert replay["revision"] == 4
    head, tags = _recover_stored_tags(store)
    assert head.revision == 4
    assert tags == {"source:camera", "workflow:archive"}
    with session_scope(factory) as session:  # type: ignore[arg-type]
        assert session.get(CollectionTagNodeGcRecord, (1, "archive", reclaimed_digest)) is None
        assert session.query(CollectionMutableDocumentPublicationAttemptRecord).count() == 0
        current_nodes = set(
            session.scalars(
                select(CollectionTagPublicationFrontierRecord.node_digest).where(
                    CollectionTagPublicationFrontierRecord.collection_id == 1,
                    CollectionTagPublicationFrontierRecord.store == "archive",
                    CollectionTagPublicationFrontierRecord.head_identity == head.head_identity,
                )
            )
        )
        assert reclaimed_digest in current_nodes
    assert restarted.process_due(limit=1) in {0, 1}
    assert f"archives/archive/opaque-docs/{collection_tag_node_path(reclaimed_digest)}" in (
        store.objects
    )


def test_persistent_tag_gc_failure_is_bounded_and_does_not_starve_other_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "catalog.sqlite3"
    archive = _PersistentTagDeleteFailureStore()
    service, factory, _store = _service(path, archive_store=archive)
    principal = _principal("source:camera", "workflow:archive")
    with session_scope(factory) as session:  # type: ignore[arg-type]
        collection = session.get(CollectionRecord, 1)
        assert collection is not None
        initial_identity = collection.tag_set_identity
    added = service.add(
        1,
        tag="workflow:archive",
        operation_id="persistent-gc-add",
        expected_revision=1,
        expected_tag_set_identity=initial_identity,
        principal=principal,
    )
    service.remove(
        1,
        tag="workflow:archive",
        operation_id="persistent-gc-remove",
        expected_revision=2,
        expected_tag_set_identity=str(added["tag_set_identity"]),
        principal=principal,
    )

    with session_scope(factory) as session:  # type: ignore[arg-type]
        for event in session.scalars(select(CatalogEventRecord)):
            event.committed_at = "2026-01-01T00:00:00.000000Z"
    monkeypatch.setattr(
        "riverhog_core.services.catalog_sync.utc_now",
        lambda: datetime(2026, 9, 8, tzinfo=UTC),
    )
    catalog = SqlAlchemyCatalogSyncService(
        RuntimeConfig(
            database_url=sqlite_url(path),
            catalog_sync_history_retention=timedelta(days=1),
            catalog_sync_bootstrap_lifetime=timedelta(hours=1),
            catalog_sync_cursor_lifetime=timedelta(hours=1),
            browse_token_lifetime=timedelta(hours=1),
        ),
        session_factory=factory,
    )
    catalog.reap_expired_history(limit=64)
    monkeypatch.setattr(
        "riverhog_core.services.catalog_sync.utc_now",
        lambda: datetime(2026, 9, 8, 1, 0, 1, tzinfo=UTC),
    )
    for _ in range(64):
        catalog.reap_expired_history(limit=1)
        with session_scope(factory) as session:  # type: ignore[arg-type]
            revisions = list(
                session.scalars(
                    select(CollectionTagRevisionRecord.revision).order_by(
                        CollectionTagRevisionRecord.revision
                    )
                )
            )
        if revisions == [3]:
            break
    else:  # pragma: no cover - three tiny authorities have bounded cleanup state
        raise AssertionError("retired tag authorities did not expire")

    for _ in range(128):
        assert service.process_due(limit=1) in {0, 1}
        with session_scope(factory) as session:  # type: ignore[arg-type]
            gc = session.scalar(select(CollectionTagNodeGcRecord))
            if gc is not None and gc.state == "retry_wait":
                reclaimed_digest = gc.node_digest
                break
    else:  # pragma: no cover - fixed-depth tag closure is much smaller
        raise AssertionError("tag-node reclamation did not reach provider failure")
    assert archive.delete_attempts == 1

    with pytest.raises(ServiceUnavailable, match="waiting for node cleanup"):
        service.add(
            1,
            tag="workflow:archive",
            operation_id="persistent-gc-readd",
            expected_revision=3,
            expected_tag_set_identity=initial_identity,
            principal=principal,
        )

    mirror = MemoryArchiveStore()
    registry = ArchiveStoreRegistry(
        {
            "archive": archive_store_binding(archive),
            "mirror": archive_store_binding(mirror),
        }
    )
    with session_scope(factory) as session:  # type: ignore[arg-type]
        gc = session.get(CollectionTagNodeGcRecord, (1, "archive", reclaimed_digest))
        archive_publication = session.get(CollectionTagPublicationRecord, (1, "archive"))
        collection = session.get(CollectionRecord, 1)
        assert gc is not None and archive_publication is not None and collection is not None
        gc.next_attempt_at = "9999-12-31T23:59:59.999999Z"
        archive_publication.next_attempt_at = gc.next_attempt_at
        session.add(
            mirror_copy := CollectionArchiveCopyRecord(
                collection_id=1,
                store="mirror",
                state="uploaded",
                archive_storage_prefix="archives/mirror/opaque-docs",
                last_uploaded_at=utc_timestamp_now(),
                last_verified_at=utc_timestamp_now(),
            )
        )
        session.flush()
        ensure_tag_publication_for_copy(
            session,
            collection=collection,
            store_name=mirror_copy.store,
        )

    restarted = SqlAlchemyCollectionTagService(
        RuntimeConfig(database_url=sqlite_url(path)),
        registry,
        session_factory=make_session_factory(sqlite_url(path)),
    )
    for _ in range(128):
        progressed = restarted.process_due(limit=1)
        with session_scope(factory) as session:  # type: ignore[arg-type]
            mirror_publication = session.get(CollectionTagPublicationRecord, (1, "mirror"))
            mirror_settled = (
                mirror_publication is not None and mirror_publication.state == "published"
            )
        if mirror_settled:
            break
        assert progressed == 1
        assert archive.delete_attempts == 1
    else:  # pragma: no cover - one tiny tag authority has fixed-depth work
        raise AssertionError("unrelated replica publication was starved")
    mirror_head, mirror_tags = _recover_stored_tags(
        mirror,
        prefix="archives/mirror/opaque-docs",
    )
    assert mirror_head.revision == 3
    assert mirror_tags == {"source:camera"}
    for _ in range(128):
        if restarted.process_due(limit=1) == 0:
            break
    assert archive.delete_attempts == 1

    with session_scope(factory) as session:  # type: ignore[arg-type]
        gc = session.get(CollectionTagNodeGcRecord, (1, "archive", reclaimed_digest))
        publication = session.get(CollectionTagPublicationRecord, (1, "archive"))
        assert gc is not None and publication is not None
        gc.next_attempt_at = utc_timestamp_now()
        publication.next_attempt_at = utc_timestamp_now()
    assert restarted.process_due(limit=1) == 1
    assert archive.delete_attempts == 2
    assert restarted.process_due(limit=128) == 0
    assert archive.delete_attempts == 2

    archive.fail_deletes = False
    with session_scope(factory) as session:  # type: ignore[arg-type]
        gc = session.get(CollectionTagNodeGcRecord, (1, "archive", reclaimed_digest))
        publication = session.get(CollectionTagPublicationRecord, (1, "archive"))
        assert gc is not None and publication is not None
        gc.next_attempt_at = utc_timestamp_now()
        publication.next_attempt_at = utc_timestamp_now()
    for _ in range(128):
        progressed = restarted.process_due(limit=1)
        with session_scope(factory) as session:  # type: ignore[arg-type]
            publication = session.get(CollectionTagPublicationRecord, (1, "archive"))
            settled = (
                publication is not None
                and publication.state == "published"
                and publication.published_revision == 4
            )
        if settled:
            break
        assert progressed == 1
    else:  # pragma: no cover - fixed-depth tag closure is much smaller
        raise AssertionError("publication did not recover after provider failure")
    replay = restarted.add(
        1,
        tag="workflow:archive",
        operation_id="persistent-gc-readd",
        expected_revision=3,
        expected_tag_set_identity=initial_identity,
        principal=principal,
    )
    assert replay["revision"] == 4


def test_tag_history_cleanup_bounds_all_subordinate_rows_and_restarts(
    tmp_path: Path,
) -> None:
    path = tmp_path / "catalog.sqlite3"
    service, factory, _store = _service(path)
    principal = _principal("source:camera", "workflow:archive")
    with session_scope(factory) as session:  # type: ignore[arg-type]
        collection = session.get(CollectionRecord, 1)
        assert collection is not None
        initial_identity = collection.tag_set_identity
    service.add(
        1,
        tag="workflow:archive",
        operation_id="make-initial-revision-retired",
        expected_revision=1,
        expected_tag_set_identity=initial_identity,
        principal=principal,
    )

    frontier_rows = 4_096
    terminal_mutations = 257
    with session_scope(factory) as session:  # type: ignore[arg-type]
        retired = session.get(CollectionTagRevisionRecord, (1, 1))
        assert retired is not None
        retired_head_identity = retired.head_identity
        retired.cleanup_started_at = utc_timestamp_now()
        session.query(CollectionTagMutationNodeReferenceRecord).delete(synchronize_session=False)
        session.add_all(
            CollectionTagPublicationFrontierRecord(
                collection_id=1,
                store="archive",
                head_identity=retired.head_identity,
                node_digest=f"{index + 1:064x}",
                expanded=True,
                published=True,
            )
            for index in range(frontier_rows)
        )
        tag_digest = collection_tag_sha256("source:camera")
        now = utc_timestamp_now()
        session.add_all(
            CollectionTagMutationRecord(
                collection_id=1,
                operation_id=f"retired-noop-{index:04d}",
                action="add",
                tag="source:camera",
                tag_sha256=tag_digest,
                expected_revision=1,
                expected_tag_set_identity=retired.tag_set_identity,
                result_revision=1,
                result_root_sha256=retired.root_sha256,
                result_tag_set_identity=retired.tag_set_identity,
                result_head_identity=retired.head_identity,
                changed=False,
                state="succeeded",
                initiated_by_app="fixture",
                initiated_by_key_id="fixture-key",
                created_at=now,
                updated_at=now,
                failure=None,
            )
            for index in range(terminal_mutations)
        )

    steps = 0
    work_limit = 1
    while True:
        with session_scope(factory) as session:  # type: ignore[arg-type]
            if session.get(CollectionTagRevisionRecord, (1, 1)) is None:
                break
            metrics = _reap_unreferenced_tag_history(
                session,
                limit=work_limit,
                cleanup_before="9999-12-31T23:59:59.999999Z",
                cleanup_started_at="9999-12-31T23:59:59.999999Z",
            )
            assert 1 <= metrics.selected_rows <= work_limit
            assert metrics.locked_rows == metrics.selected_rows
            assert metrics.changed_rows == metrics.selected_rows
            assert metrics.deleted_rows == metrics.changed_rows
        steps += metrics.changed_rows
        work_limit = 97
    assert steps == frontier_rows + terminal_mutations + 1
    with session_scope(factory) as session:  # type: ignore[arg-type]
        assert not list(
            session.scalars(
                select(CollectionTagPublicationFrontierRecord).where(
                    CollectionTagPublicationFrontierRecord.head_identity == retired_head_identity
                )
            )
        )
        assert not list(
            session.scalars(
                select(CollectionTagMutationRecord).where(
                    CollectionTagMutationRecord.result_revision == 1
                )
            )
        )


def test_tag_node_reclamation_counts_every_edge_and_node_row_at_work_one(
    tmp_path: Path,
) -> None:
    path = tmp_path / "catalog.sqlite3"
    _service(path)
    factory = make_session_factory(sqlite_url(path))
    children: list[CollectionTagChild] = []
    with session_scope(factory) as session:
        for label in range(256):
            encoded = encode_collection_tag_node(
                CollectionTagNode(prefix=bytes((label,)), tag=f"child-{label}".encode())
            )
            digest = collection_tag_node_digest(encoded)
            session.add(
                CollectionTagNodeRecord(
                    digest=digest,
                    encoded=encoded,
                    created_at=utc_timestamp_now(),
                )
            )
            children.append(CollectionTagChild(label=label, digest=digest))
        parent_encoded = encode_collection_tag_node(
            CollectionTagNode(prefix=b"", children=tuple(children))
        )
        parent_digest = collection_tag_node_digest(parent_encoded)
        session.add(
            CollectionTagNodeRecord(
                digest=parent_digest,
                encoded=parent_encoded,
                created_at=utc_timestamp_now(),
            )
        )
        session.flush()
        session.add_all(
            CollectionTagNodeEdgeRecord(parent_digest=parent_digest, child_digest=child.digest)
            for child in children
        )

    steps = 0
    while True:
        with session_scope(factory) as session:
            before = sum(
                int(session.query(model).count())
                for model in (
                    CollectionTagNodeRecord,
                    CollectionTagNodeEdgeRecord,
                    CollectionTagNodeReclamationRecord,
                )
            )
            metrics = _reap_unreferenced_tag_history(
                session,
                limit=1,
                cleanup_before="9999-12-31T23:59:59.999999Z",
                cleanup_started_at="9999-12-31T23:59:59.999999Z",
            )
            session.flush()
            after = sum(
                int(session.query(model).count())
                for model in (
                    CollectionTagNodeRecord,
                    CollectionTagNodeEdgeRecord,
                    CollectionTagNodeReclamationRecord,
                )
            )
            assert metrics.changed_rows == 1
            assert abs(after - before) == 1
            parent_present = session.get(CollectionTagNodeRecord, parent_digest) is not None
            claim_present = (
                session.get(CollectionTagNodeReclamationRecord, parent_digest) is not None
            )
        steps += 1
        if not parent_present and not claim_present:
            break

    assert steps == 259


def test_exact_tag_revisions_expire_with_the_catalog_history_that_names_them(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "catalog.sqlite3"
    service, factory, _store = _service(path)
    principal = _principal("source:camera", "workflow:archive")
    with session_scope(factory) as session:  # type: ignore[arg-type]
        collection = session.get(CollectionRecord, 1)
        assert collection is not None
        initial_identity = collection.tag_set_identity
    added = service.add(
        1,
        tag="workflow:archive",
        operation_id="add-workflow",
        expected_revision=1,
        expected_tag_set_identity=initial_identity,
        principal=principal,
    )
    service.remove(
        1,
        tag="workflow:archive",
        operation_id="remove-workflow",
        expected_revision=2,
        expected_tag_set_identity=str(added["tag_set_identity"]),
        principal=principal,
    )
    assert (
        service.contains(
            1,
            tag="workflow:archive",
            revision=2,
            tag_set_identity=str(added["tag_set_identity"]),
            principal=principal,
        )["present"]
        is True
    )
    with session_scope(factory) as session:  # type: ignore[arg-type]
        events = list(
            session.scalars(select(CatalogEventRecord).order_by(CatalogEventRecord.revision))
        )
        assert len(events) == 2
        events[0].committed_at = "2026-01-01T00:00:00.000000Z"
        events[1].committed_at = "2026-09-07T00:00:00.000000Z"
    monkeypatch.setattr(
        "riverhog_core.services.catalog_sync.utc_now",
        lambda: datetime(2026, 9, 8, tzinfo=UTC),
    )
    catalog = SqlAlchemyCatalogSyncService(
        RuntimeConfig(
            database_url=sqlite_url(path),
            catalog_sync_history_retention=timedelta(days=1),
            catalog_sync_bootstrap_lifetime=timedelta(hours=1),
            catalog_sync_cursor_lifetime=timedelta(hours=1),
            browse_token_lifetime=timedelta(hours=1),
        ),
        session_factory=factory,
    )
    assert catalog.reap_expired_history(limit=1) == 1
    for _ in range(32):
        assert catalog.reap_expired_history(limit=1) == 0
    restarted = SqlAlchemyCollectionTagService(
        RuntimeConfig(database_url=sqlite_url(path)),
        ArchiveStoreRegistry({"archive": archive_store_binding(_store)}),
        session_factory=make_session_factory(sqlite_url(path)),
    )
    first = restarted.list_collection(
        1,
        page_size=1,
        position=None,
        expected_revision=2,
        expected_tag_set_identity=str(added["tag_set_identity"]),
        principal=principal,
    )
    browse_now = [datetime(2026, 9, 8, tzinfo=UTC).timestamp()]
    browse_tokens = BrowseTokenCodec(
        b"exact-tag-revision-lifetime-test-key",
        lifetime_seconds=60 * 60,
        clock=lambda: browse_now[0],
    )
    first_token = browse_tokens.issue(
        operation="list_collection_tags",
        principal="reader",
        selectors={"collection_id": 1, "revision": 2},
        position=first["_next_position"],  # type: ignore[arg-type]
    )
    monkeypatch.setattr(
        "riverhog_core.services.catalog_sync.utc_now",
        lambda: datetime(2026, 9, 8, 0, 0, 1, tzinfo=UTC),
    )
    assert catalog.reap_expired_history(limit=1) == 1
    assert catalog.reap_expired_history(limit=1) == 0
    with pytest.raises(PreconditionFailed):
        restarted.list_collection(
            1,
            page_size=1,
            position=None,
            expected_revision=2,
            expected_tag_set_identity=str(added["tag_set_identity"]),
            principal=principal,
        )
    monkeypatch.setattr(
        "riverhog_core.services.collection_tags.utc_timestamp_now",
        lambda: "2026-09-08T00:59:59.000000Z",
    )
    browse_now[0] += (60 * 60) - 1
    continuation = browse_tokens.verify(
        first_token,
        operation="list_collection_tags",
        principal="reader",
        selectors={"collection_id": 1, "revision": 2},
    )
    second = restarted.list_collection(
        1,
        page_size=1,
        position=continuation,
        expected_revision=2,
        expected_tag_set_identity=str(added["tag_set_identity"]),
        principal=principal,
    )
    assert set(first["tags"]) | set(second["tags"]) == {
        "source:camera",
        "workflow:archive",
    }

    monkeypatch.setattr(
        "riverhog_core.services.catalog_sync.utc_now",
        lambda: datetime(2026, 9, 8, 1, 0, 2, tzinfo=UTC),
    )
    catalog.reap_expired_history(limit=100)
    with session_scope(factory) as session:  # type: ignore[arg-type]
        assert session.get(CollectionTagRevisionRecord, (1, 2)) is not None

    monkeypatch.setattr(
        "riverhog_core.services.catalog_sync.utc_now",
        lambda: datetime(2026, 9, 8, 2, 0, tzinfo=UTC),
    )
    for _ in range(256):
        catalog.reap_expired_history(limit=1)
        with session_scope(factory) as session:  # type: ignore[arg-type]
            if session.get(CollectionTagRevisionRecord, (1, 2)) is None:
                break
    else:  # pragma: no cover - fixed cleanup state is much smaller
        raise AssertionError("retired exact tag authority did not converge")
    with pytest.raises(PreconditionFailed):
        restarted.contains(
            1,
            tag="workflow:archive",
            revision=2,
            tag_set_identity=str(added["tag_set_identity"]),
            principal=principal,
        )
    with session_scope(factory) as session:  # type: ignore[arg-type]
        assert list(session.scalars(select(CollectionTagRevisionRecord.revision))) == [3]
