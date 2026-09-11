from __future__ import annotations

import hashlib
import os
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from typing import Any
from uuid import uuid4

import pytest
from piggity.main import _create_or_resume_collection_upload_session
from riverhog_client.initial_tags import prepare_initial_collection_tags
from riverhog_core.app_permissions import (
    ALL_RESOURCES,
    COLLECTION_TAGS_MANAGE,
    COLLECTIONS_CREATE,
    COLLECTIONS_DELETE,
    ApplicationAccess,
    ApplicationPrincipal,
    tag_resource,
)
from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.catalog_db import (
    create_catalog_engine,
    initialize_db,
    make_session_factory,
    session_scope,
)
from riverhog_core.catalog_models import (
    CollectionTagNodeReclamationRecord,
    CollectionTagNodeRecord,
    CollectionUploadFileRecord,
    CollectionUploadProvenanceJournalChunkRecord,
    CollectionUploadProvenanceJournalRecord,
    CollectionUploadRecord,
    CollectionUploadTagNodeReferenceRecord,
)
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services import collection_uploads as collection_uploads_module
from riverhog_core.services.catalog_sync import _reap_unreferenced_tag_history
from riverhog_core.services.collection_tags import build_collection_tag_set
from riverhog_core.services.collection_uploads import SqlAlchemyCollectionUploadService
from riverhog_protocol import (
    COLLECTION_TAG_REQUEST_MEMBERS_MAX,
    CollectionUploadProvenanceJournalCreateDocument,
)
from riverhog_protocol.errors import Conflict, ServiceUnavailable
from sqlalchemy import event, func, select, text
from sqlalchemy.engine import make_url

from tests.unit.archive_object_fixtures import MemoryArchiveStore, archive_store_binding

pytestmark = pytest.mark.integration

_CREATOR = ApplicationPrincipal(
    app="fixture-target",
    key_id="target-key",
    access=frozenset({ApplicationAccess(COLLECTIONS_CREATE, ALL_RESOURCES)}),
)
_OPERATOR = ApplicationPrincipal(
    app="fixture-operator",
    key_id="operator-key",
    access=frozenset({ApplicationAccess(COLLECTIONS_DELETE, ALL_RESOURCES)}),
)
_TAGGED_CREATOR = ApplicationPrincipal(
    app="tagged-fixture-target",
    key_id="tagged-target-key",
    access=frozenset(
        {
            ApplicationAccess(COLLECTIONS_CREATE, ALL_RESOURCES),
            ApplicationAccess(COLLECTION_TAGS_MANAGE, tag_resource("source:camera")),
        }
    ),
)
_FILE = {
    "path": "output/artifact.bin",
    "bytes": 0,
    "sha256": hashlib.sha256(b"").hexdigest(),
}


@pytest.fixture
def database_url() -> Iterator[str]:
    value = os.environ.get("RIVERHOG_TEST_POSTGRES_URL", "").strip()
    if not value:
        pytest.skip("RIVERHOG_TEST_POSTGRES_URL is required")
    schema = "riverhog_upload_custody_" + uuid4().hex
    admin = create_catalog_engine(value)
    with admin.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    scoped = (
        make_url(value)
        .update_query_dict({"options": f"-csearch_path={schema},public"})
        .render_as_string(hide_password=False)
    )
    initialize_db(scoped)
    try:
        yield scoped
    finally:
        with admin.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()


def _services(
    database_url: str,
) -> tuple[SqlAlchemyCollectionUploadService, SqlAlchemyCollectionUploadService]:
    base = RuntimeConfig.for_testing(database_url=database_url, archive_scrypt_work_factor=1)
    archive = replace(base.archive_store("archive"), name="archive")
    config = replace(
        base,
        archive_stores={"archive": archive},
        archive_write_store="archive",
        archive_read_order=("archive",),
    )
    stores = ArchiveStoreRegistry({"archive": archive_store_binding(MemoryArchiveStore())})
    return (
        SqlAlchemyCollectionUploadService(
            config,
            stores,
        ),
        SqlAlchemyCollectionUploadService(
            config,
            stores,
        ),
    )


def _create(service: SqlAlchemyCollectionUploadService) -> int:
    payload = service.create_or_resume(
        idempotency_key="fixture-execution",
        ingest_source="transform:fixture",
        archive_store=None,
        initiator=_CREATOR,
        event_context=None,
        provenance_mode="omitted",
        provenance_omission_reason="fixture",
        custody_mode="custody-transfer",
    )
    return int(payload["collection_id"])


def _initial_tag_identity(tags: tuple[str, ...]) -> str:
    with prepare_initial_collection_tags(tags) as prepared:
        return prepared.tag_set_identity


def test_postgres_upload_tag_authorization_never_returns_the_accumulated_set(
    database_url: str,
) -> None:
    service, _other = _services(database_url)
    tags = tuple(f"scope/tag-{index:04d}" for index in range(301))
    principal = ApplicationPrincipal(
        app="bounded-tag-uploader",
        key_id="bounded-tag-key",
        access=frozenset(
            {
                ApplicationAccess(COLLECTION_TAGS_MANAGE, ALL_RESOURCES),
                *(ApplicationAccess(COLLECTIONS_CREATE, tag_resource(tag)) for tag in tags),
            }
        ),
    )
    opened = service.create_or_resume(
        idempotency_key="bounded-tag-authorization",
        ingest_source="fixture",
        tags=tags[:100],
        initial_tag_set_identity=_initial_tag_identity(tags),
        archive_store=None,
        initiator=principal,
        event_context=None,
        provenance_mode="omitted",
        provenance_omission_reason="fixture",
    )
    collection_id = int(opened["collection_id"])
    service.add_tags(collection_id, tags[100:200], principal=principal)
    service.add_tags(collection_id, tags[200:300], principal=principal)

    statements: list[str] = []
    staged_hash_fetch_sizes: list[int | None] = []
    engine = service._session_factory.kw["bind"]

    def record_statement(
        _connection: object,
        _cursor: object,
        statement: str,
        _parameters: object,
        context: Any,
        _executemany: bool,
    ) -> None:
        normalized = " ".join(statement.casefold().split())
        statements.append(normalized)
        if "select collection_upload_tags.tag_sha256 " in normalized and "count(" not in normalized:
            staged_hash_fetch_sizes.append(context.execution_options.get("yield_per"))

    event.listen(engine, "before_cursor_execute", record_statement)
    try:
        result = service.add_tags(collection_id, tags[300:], principal=principal)
        service.require_access(collection_id, principal)
        service.require_read_access(collection_id, principal)
    finally:
        event.remove(engine, "before_cursor_execute", record_statement)

    assert result["tag_count"] == len(tags)
    assert any("count(collection_upload_tags.tag_sha256)" in item for item in statements)
    assert staged_hash_fetch_sizes
    assert set(staged_hash_fetch_sizes) == {COLLECTION_TAG_REQUEST_MEMBERS_MAX}
    assert not any(
        "select collection_upload_tags.tag " in item and "from collection_upload_tags" in item
        for item in statements
    )


def test_piggity_reconciles_real_closed_discovery_without_replaying_tags(
    database_url: str,
) -> None:
    service, _other = _services(database_url)
    tags = [f"camera/retry-{index:04d}" for index in range(205)]
    principal = ApplicationPrincipal(
        app="piggity-retry",
        key_id="piggity-retry-key",
        access=frozenset(
            {
                ApplicationAccess(COLLECTIONS_CREATE, ALL_RESOURCES),
                ApplicationAccess(COLLECTION_TAGS_MANAGE, ALL_RESOURCES),
            }
        ),
    )
    tag_calls = 0

    class Api:
        def create_or_resume_collection_upload_session(
            self,
            idempotency_key: str,
            **kwargs: object,
        ) -> dict[str, object]:
            return service.create_or_resume(
                idempotency_key=idempotency_key,
                ingest_source=str(kwargs.get("ingest_source") or "fixture"),
                tags=tuple(kwargs.get("tags") or ()),
                initial_tag_set_identity=str(kwargs["initial_tag_set_identity"]),
                archive_store=None,
                initiator=principal,
                event_context=None,
                provenance_mode="omitted",
                provenance_omission_reason="fixture",
            )

        def add_collection_upload_session_tags(
            self,
            collection_id: int,
            batch: tuple[str, ...],
        ) -> dict[str, object]:
            nonlocal tag_calls
            tag_calls += 1
            return service.add_tags(collection_id, batch, principal=principal)

    api = Api()
    opened = _create_or_resume_collection_upload_session(
        api,  # type: ignore[arg-type]
        "piggity-closing-retry",
        ingest_source="fixture",
        tags=tags,
        provenance_mode="omitted",
        provenance_omission_reason="fixture",
    )
    collection_id = int(opened["collection_id"])
    service.register_files(collection_id, (_FILE,))
    closed = service.complete(collection_id)
    assert closed["state"] in {"closing", "uploading", "finalizing"}
    staged_calls = tag_calls

    resumed = _create_or_resume_collection_upload_session(
        api,  # type: ignore[arg-type]
        "piggity-closing-retry",
        ingest_source="fixture",
        tags=tags,
        provenance_mode="omitted",
        provenance_omission_reason="fixture",
    )

    assert resumed["state"] == closed["state"]
    assert tag_calls == staged_calls


def test_postgres_upload_protects_a_reused_tag_root_before_its_first_commit(
    database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, _other = _services(database_url)
    factory = make_session_factory(database_url)
    with session_scope(factory) as session:
        tag_set, _created = build_collection_tag_set(session, ("source:camera",))
        root_digest = tag_set.root.root_sha256
    assert root_digest is not None

    entered = threading.Event()
    release = threading.Event()
    original_scope = collection_uploads_module.session_scope
    paused = False

    @contextmanager
    def pause_before_commit(session_factory: object) -> Iterator[object]:
        nonlocal paused
        with original_scope(session_factory) as session:  # type: ignore[arg-type]
            yield session
            upload = session.scalar(
                select(CollectionUploadRecord).where(
                    CollectionUploadRecord.initiated_by_app == _TAGGED_CREATOR.app,
                    CollectionUploadRecord.idempotency_key == "reuse-orphan-tag-root",
                )
            )
            if not paused and upload is not None:
                paused = True
                entered.set()
                assert release.wait(timeout=10)

    monkeypatch.setattr(collection_uploads_module, "session_scope", pause_before_commit)
    with ThreadPoolExecutor(max_workers=1) as executor:
        pending = executor.submit(
            service.create_or_resume,
            idempotency_key="reuse-orphan-tag-root",
            ingest_source="postgres-fixture",
            tags=("source:camera",),
            archive_store=None,
            initiator=_TAGGED_CREATOR,
            event_context=None,
            provenance_mode="omitted",
            provenance_omission_reason="fixture",
        )
        assert entered.wait(timeout=10)
        with session_scope(factory) as session:
            metrics = _reap_unreferenced_tag_history(
                session,
                limit=1,
                cleanup_before="9999-12-31T23:59:59.999999Z",
                cleanup_started_at="9999-12-31T23:59:59.999999Z",
            )
            assert metrics.changed_rows == 0
        with session_scope(factory) as session:
            assert session.get(CollectionTagNodeRecord, root_digest) is not None
            assert session.get(CollectionTagNodeReclamationRecord, root_digest) is None
        release.set()
        created = pending.result(timeout=10)

    collection_id = int(created["collection_id"])
    with session_scope(factory) as session:
        assert (
            session.get(
                CollectionUploadTagNodeReferenceRecord,
                (collection_id, root_digest),
            )
            is not None
        )


def test_postgres_upload_reuse_fails_before_commit_after_reclamation_claim(
    database_url: str,
) -> None:
    service, _other = _services(database_url)
    factory = make_session_factory(database_url)
    with session_scope(factory) as session:
        tag_set, _created = build_collection_tag_set(session, ("source:camera",))
        root_digest = tag_set.root.root_sha256
    assert root_digest is not None
    with session_scope(factory) as session:
        metrics = _reap_unreferenced_tag_history(
            session,
            limit=1,
            cleanup_before="9999-12-31T23:59:59.999999Z",
            cleanup_started_at="9999-12-31T23:59:59.999999Z",
        )
        assert metrics.changed_rows == 1
    with pytest.raises(ServiceUnavailable, match="being reclaimed"):
        service.create_or_resume(
            idempotency_key="claimed-tag-root",
            ingest_source="postgres-fixture",
            tags=("source:camera",),
            archive_store=None,
            initiator=_TAGGED_CREATOR,
            event_context=None,
            provenance_mode="omitted",
            provenance_omission_reason="fixture",
        )
    with session_scope(factory) as session:
        assert (
            session.scalar(
                select(CollectionUploadRecord).where(
                    CollectionUploadRecord.initiated_by_app == _TAGGED_CREATOR.app,
                    CollectionUploadRecord.idempotency_key == "claimed-tag-root",
                )
            )
            is None
        )


def _expire(database_url: str, collection_id: int) -> None:
    with session_scope(make_session_factory(database_url)) as session:
        upload = session.get(CollectionUploadRecord, collection_id)
        assert upload is not None
        upload.lease_expires_at = "2000-01-01T00:00:00.000000Z"


def _race(
    first: Callable[[], Any],
    second: Callable[[], Any],
) -> tuple[Any, Any]:
    barrier = threading.Barrier(2)

    def invoke(call: Callable[[], Any]) -> Any:
        barrier.wait(timeout=10)
        try:
            return call()
        except Exception as exc:  # return exact competing outcome for assertions
            return exc

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = (executor.submit(invoke, first), executor.submit(invoke, second))
        return futures[0].result(timeout=20), futures[1].result(timeout=20)


def test_exact_concurrent_registration_is_one_physical_planner_step(
    database_url: str,
) -> None:
    first, second = _services(database_url)
    collection_id = _create(first)

    results = _race(
        lambda: first.register_files(collection_id, (_FILE,)),
        lambda: second.register_files(collection_id, (_FILE,)),
    )

    assert all(not isinstance(result, Exception) for result in results)
    with session_scope(make_session_factory(database_url)) as session:
        upload = session.get(CollectionUploadRecord, collection_id)
        assert upload is not None
        assert (
            session.scalar(
                select(func.count(CollectionUploadFileRecord.path)).where(
                    CollectionUploadFileRecord.collection_id == collection_id
                )
            )
            == 1
        )
        assert upload.planner_checkpoint_json is not None
        assert '"next_file_order":1' in upload.planner_checkpoint_json
        assert (upload.file_count, upload.file_bytes) == (1, 0)


def test_distinct_concurrent_registrations_preserve_both_members(
    database_url: str,
) -> None:
    first, second = _services(database_url)
    collection_id = _create(first)
    first_file = {**_FILE, "path": "z/output.bin"}
    second_file = {**_FILE, "path": "a/output.bin"}

    results = _race(
        lambda: first.register_files(collection_id, (first_file,)),
        lambda: second.register_files(collection_id, (second_file,)),
    )

    assert all(not isinstance(result, Exception) for result in results)
    with session_scope(make_session_factory(database_url)) as session:
        rows = list(
            session.scalars(
                select(CollectionUploadFileRecord)
                .where(CollectionUploadFileRecord.collection_id == collection_id)
                .order_by(CollectionUploadFileRecord.file_order)
            )
        )
        upload = session.get(CollectionUploadRecord, collection_id)
        assert upload is not None
        assert {row.path for row in rows} == {"a/output.bin", "z/output.bin"}
        assert [row.file_order for row in rows] == [0, 1]
        assert upload.file_count == 2


def test_concurrent_provenance_retry_commits_one_next_ordinal(
    database_url: str,
) -> None:
    journal_id = "urn:uuid:00000000-0000-4000-8000-000000000077"
    first, second = _services(database_url)
    opened = first.create_or_resume(
        idempotency_key="fixture-provenance",
        ingest_source="fixture",
        archive_store=None,
        initiator=_CREATOR,
        event_context=None,
        provenance_mode="captured",
        provenance_omission_reason=None,
    )
    collection_id = int(opened["collection_id"])
    content = b"one bounded provenance append"
    first.create_provenance_journal(
        collection_id,
        journal_id,
        CollectionUploadProvenanceJournalCreateDocument(
            bytes=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
        ),
    )

    results = _race(
        lambda: first.append_provenance_journal(
            collection_id, journal_id, offset=0, content=content
        ),
        lambda: second.append_provenance_journal(
            collection_id, journal_id, offset=0, content=content
        ),
    )

    assert all(not isinstance(result, Exception) for result in results)
    with session_scope(make_session_factory(database_url)) as session:
        journal = session.get(
            CollectionUploadProvenanceJournalRecord,
            (collection_id, journal_id),
        )
        assert journal is not None
        assert journal.next_chunk_ordinal == 1
        assert (
            session.scalar(
                select(func.count(CollectionUploadProvenanceJournalChunkRecord.ordinal)).where(
                    CollectionUploadProvenanceJournalChunkRecord.collection_id == collection_id,
                    CollectionUploadProvenanceJournalChunkRecord.journal_id == journal_id,
                )
            )
            == 1
        )


def test_provenance_append_and_expiry_serialize_at_the_custody_fence(
    database_url: str,
) -> None:
    journal_id = "urn:uuid:00000000-0000-4000-8000-000000000078"
    first, second = _services(database_url)
    opened = first.create_or_resume(
        idempotency_key="fixture-provenance-fence",
        ingest_source="fixture",
        archive_store=None,
        initiator=_CREATOR,
        event_context=None,
        provenance_mode="captured",
        provenance_omission_reason=None,
        custody_mode="custody-transfer",
    )
    collection_id = int(opened["collection_id"])
    content = b"one fenced provenance append"
    first.create_provenance_journal(
        collection_id,
        journal_id,
        CollectionUploadProvenanceJournalCreateDocument(
            bytes=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
        ),
    )
    _expire(database_url, collection_id)

    appended, reaped = _race(
        lambda: first.append_provenance_journal(
            collection_id, journal_id, offset=0, content=content
        ),
        lambda: second.reap_expired_custody_transfers(),
    )
    state = str(first.get(collection_id)["state"])

    with session_scope(make_session_factory(database_url)) as session:
        journal = session.get(
            CollectionUploadProvenanceJournalRecord,
            (collection_id, journal_id),
        )
        assert journal is not None
        if state == "open":
            assert not isinstance(appended, Exception)
            assert reaped == 0
            assert journal.next_chunk_ordinal == 1
        else:
            assert state == "orphaned"
            assert isinstance(appended, Conflict)
            assert reaped == 1
            assert journal.next_chunk_ordinal == 0

    if state == "orphaned":
        resumed = first.create_or_resume(
            idempotency_key="fixture-provenance-fence",
            ingest_source="fixture",
            archive_store=None,
            initiator=_CREATOR,
            event_context=None,
            provenance_mode="captured",
            provenance_omission_reason=None,
            custody_mode="custody-transfer",
        )
        assert resumed["state"] == "open"
        current = first.append_provenance_journal(
            collection_id, journal_id, offset=0, content=content
        )
        assert current["accepted_bytes"] == len(content)


def test_heartbeat_and_expiry_serialize_without_losing_resumable_custody(
    database_url: str,
) -> None:
    first, second = _services(database_url)
    collection_id = _create(first)
    first.register_files(collection_id, (_FILE,))
    _expire(database_url, collection_id)

    heartbeat, reaped = _race(
        lambda: first.heartbeat(collection_id),
        lambda: second.reap_expired_custody_transfers(),
    )
    state = str(first.get(collection_id)["state"])

    assert state in {"open", "orphaned"}
    if state == "open":
        assert not isinstance(heartbeat, Exception)
        assert reaped == 0
    else:
        assert isinstance(heartbeat, Conflict)
        assert reaped == 1
        resumed = first.create_or_resume(
            idempotency_key="fixture-execution",
            ingest_source="transform:fixture",
            archive_store=None,
            initiator=_CREATOR,
            event_context=None,
            provenance_mode="omitted",
            provenance_omission_reason="fixture",
            custody_mode="custody-transfer",
        )
        assert resumed["collection_id"] == collection_id
        assert resumed["state"] == "open"
    page = first.list_files(collection_id, page_size=100, position=None)
    files = page["files"]
    assert isinstance(files, list)
    assert len(files) == 1


def test_completion_and_expiry_have_one_serial_terminal_intent(
    database_url: str,
) -> None:
    first, second = _services(database_url)
    collection_id = _create(first)
    first.register_files(collection_id, (_FILE,))
    _expire(database_url, collection_id)
    completed, reaped = _race(
        lambda: first.complete(collection_id),
        lambda: second.reap_expired_custody_transfers(),
    )
    state = str(first.get(collection_id)["state"])

    if state == "orphaned":
        assert isinstance(completed, Conflict)
        assert reaped == 1
        _create(first)
        resumed = first.complete(collection_id)
        assert resumed["state"] in {"closing", "uploading", "finalizing", "finalized"}
    else:
        assert not isinstance(completed, Exception)
        assert reaped == 0
        assert state in {"closing", "uploading", "finalizing", "finalized"}


def test_guarded_discard_and_exact_resume_cannot_both_win_for_old_custody(
    database_url: str,
) -> None:
    first, second = _services(database_url)
    collection_id = _create(first)
    first.register_files(collection_id, (_FILE,))
    _expire(database_url, collection_id)
    assert first.reap_expired_custody_transfers() == 1
    plan = first.plan_orphan_discard(collection_id)
    challenge = str(plan["challenge"])

    resumed, discarded = _race(
        lambda: _create(first),
        lambda: second.discard_orphan(collection_id, challenge=challenge),
    )

    if isinstance(discarded, Conflict):
        assert resumed == collection_id
        assert first.get(collection_id)["state"] == "open"
    else:
        assert discarded["status"] == "discarded"
        assert isinstance(resumed, Conflict)
        replacement = _create(first)
        assert replacement != collection_id
        assert first.get(replacement)["state"] == "open"
