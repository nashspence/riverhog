from __future__ import annotations

import hashlib
import json
import logging
import threading
from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path

import pytest
from riverhog_core.app_permissions import (
    ALL_RESOURCES,
    ARCHIVES_MANAGE,
    COLLECTIONS_CREATE,
    ApplicationAccess,
    Principal,
)
from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.catalog_db import initialize_db, make_session_factory, session_scope
from riverhog_core.catalog_models import (
    AppKeyAccessGrantRecord,
    AppKeyRecord,
    ArchiveCopyJobRecord,
    ArchiveCopyObjectUploadRecord,
    CollectionArchiveCopyRecord,
    CollectionRecord,
    CollectionUploadCopyIntentRecord,
    CollectionUploadRecord,
    LifecycleEventRecord,
    RetrievalCacheLeaseRecord,
    RetrievalCacheObjectRecord,
)
from riverhog_core.placement_choices import archive_binding_sha256
from riverhog_core.ports.archive_objects import (
    WriteSegmentReceipt,
    WriteSession,
)
from riverhog_core.ports.archive_store import ArchiveObjectIdentity, ArchiveReadStatus
from riverhog_core.ports.retrieval_cache import RetrievalCacheAdmission
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services.archive_copy_jobs import SqlAlchemyArchiveCopyJobService
from riverhog_core.services.collection_uploads import SqlAlchemyCollectionUploadService
from riverhog_core.services.lifecycle_events import SqlAlchemyLifecycleEventService
from riverhog_core.stores.mirrored_archive_resumable_object_store import (
    MirroredArchiveResumableObjectStore,
)
from riverhog_protocol.errors import Conflict
from sqlalchemy import select
from time_formats import format_utc_timestamp, utc_now

from tests.unit.archive_object_fixtures import (
    COLLECTION_ID,
    FixtureArchive,
    MemoryArchiveStore,
    archive_store_binding,
    make_archive,
    make_captured_provenance_archive,
    seed_archive_copy,
    sqlite_url,
)

FILES = {"document.txt": b"archive copy service\n", "notes.txt": b"small notes\n"}
PACK_ID = f"pack-{0:064x}"
VOLUME_METADATA_ID = f"volume-metadata-{0:064x}"
VOLUME_TERMINAL_ID = f"volume-terminal-{1:064x}"
INITIATOR = Principal(
    id="operator",
    key_id="operator-key",
    access=frozenset(),
)
COPY_INTENT_KEY = "a" * 16


class _ArchiveCopyCache:
    def __init__(self) -> None:
        self.store = MemoryArchiveStore(new_archive_prefix="archives/cache/new-copy")

    def admit(
        self,
        *,
        owner: str,
        source_store: str,
        collection_id: int,
        object_id: str,
        expected_bytes: int,
    ) -> RetrievalCacheAdmission:
        path = f"cache/{source_store}/{collection_id}/{object_id}"
        session = self.store.begin_write(
            object_path=path,
            expected_bytes=expected_bytes,
            content_type="application/octet-stream",
            metadata={},
        )
        return RetrievalCacheAdmission(
            owner=owner,
            cache_store="memory",
            source_store=source_store,
            collection_id=collection_id,
            object_id=object_id,
            object_path=path,
            expected_bytes=expected_bytes,
            write_token=session.write_token,
            admitted_at="2026-08-08T00:00:00.000000000Z",
        )

    def resumable_object_store(self, **_: object) -> MemoryArchiveStore:
        return self.store

    def release(self, *, owner: str) -> int:
        _ = owner
        return 0

    def is_current(self, *, admission: RetrievalCacheAdmission) -> bool:
        _ = admission
        return True

    def reap_abandoned_populations(self, *, limit: int = 100) -> int:
        _ = limit
        return 0


def _multiple_archive_parts(files: dict[str, bytes], *, parts: int = 4) -> FixtureArchive:
    archive = make_archive(files)
    ciphertext = archive.stored_objects[f"volumes/{archive.pack_plan.volume_id}.tar.age"]
    plaintext = archive.pack_plaintext

    def split(content: bytes) -> list[bytes]:
        return [
            content[len(content) * index // parts : len(content) * (index + 1) // parts]
            for index in range(parts)
        ]

    plaintext_parts = split(plaintext)
    stored_parts = split(ciphertext)
    plaintext_start = 0
    receipts: list[dict[str, object]] = []
    for number, (plain, stored) in enumerate(
        zip(plaintext_parts, stored_parts, strict=True),
        start=1,
    ):
        receipts.append(
            {
                "number": number,
                "plaintext_start": plaintext_start,
                "plaintext_bytes": len(plain),
                "plaintext_sha256": hashlib.sha256(plain).hexdigest(),
                "stored_bytes": len(stored),
                "stored_sha256": hashlib.sha256(stored).hexdigest(),
            }
        )
        plaintext_start += len(plain)
    return replace(
        archive,
        pack_parts_json=json.dumps(receipts, sort_keys=True, separators=(",", ":")),
    )


def _service(
    path: Path,
    *,
    source_ready: bool = True,
    destination: MemoryArchiveStore | None = None,
    archive: FixtureArchive | None = None,
) -> tuple[
    RuntimeConfig,
    FixtureArchive,
    MemoryArchiveStore,
    MemoryArchiveStore,
    SqlAlchemyArchiveCopyJobService,
]:
    config, archive = seed_archive_copy(path, FILES, archive=archive)
    b2_config = replace(
        config.archive_store("deep"),
        name="b2",
        base_url="http://127.0.0.1/b2",
    )
    config = replace(
        config,
        archive_stores={"deep": config.archive_store("deep"), "b2": b2_config},
    )
    source = MemoryArchiveStore(archive, ready=source_ready)
    destination = destination or MemoryArchiveStore(new_archive_prefix="archives/b2/new-copy")
    service = SqlAlchemyArchiveCopyJobService(
        config,
        ArchiveStoreRegistry(
            {
                "deep": archive_store_binding(source),
                "b2": archive_store_binding(destination),
            },
        ),
    )
    return config, archive, source, destination, service


def _pending_upload_copy_intent(config: RuntimeConfig) -> None:
    now = format_utc_timestamp(utc_now())
    with session_scope(make_session_factory(config.database_url)) as session:
        collection = session.get(CollectionRecord, COLLECTION_ID)
        assert collection is not None
        collection.creation_archive_store = "deep"
        collection.creation_use_cache = False
        collection.creation_copy_to_json = '["b2"]'
        session.add(
            AppKeyRecord(
                id=COPY_INTENT_KEY,
                app="operator",
                token_sha256="b" * 64,
                created_at=now,
            )
        )
        session.flush()
        session.add(
            AppKeyAccessGrantRecord(
                key_id=COPY_INTENT_KEY,
                permission=ARCHIVES_MANAGE,
                resource=ALL_RESOURCES,
                created_at=now,
            )
        )
        session.add(
            CollectionUploadCopyIntentRecord(
                collection_id=COLLECTION_ID,
                destination_store="b2",
                source_store="deep",
                destination_binding_sha256=archive_binding_sha256(config, "b2"),
                source_binding_sha256=archive_binding_sha256(config, "deep"),
                initiated_by_app="operator",
                initiated_by_key_id=COPY_INTENT_KEY,
                event_context_json=None,
                use_cache=False,
                state="pending",
                accepted_at=now,
                next_attempt_at=now,
            )
        )


def test_upload_copy_choices_are_atomic_and_idempotent(tmp_path: Path) -> None:
    config, _, _, _, copy_service = _service(tmp_path / "catalog.sqlite3")
    producer = SqlAlchemyCollectionUploadService(config, copy_service._archive_stores)
    now = format_utc_timestamp(utc_now())
    with session_scope(make_session_factory(config.database_url)) as session:
        session.add(
            AppKeyRecord(
                id=COPY_INTENT_KEY,
                app="producer",
                token_sha256="c" * 64,
                created_at=now,
            )
        )
        session.flush()
        for permission in (COLLECTIONS_CREATE, ARCHIVES_MANAGE):
            session.add(
                AppKeyAccessGrantRecord(
                    key_id=COPY_INTENT_KEY,
                    permission=permission,
                    resource=ALL_RESOURCES,
                    created_at=now,
                )
            )
    actor = Principal(
        id="producer",
        key_id=COPY_INTENT_KEY,
        access=frozenset(
            {
                ApplicationAccess(COLLECTIONS_CREATE, ALL_RESOURCES),
                ApplicationAccess(ARCHIVES_MANAGE, ALL_RESOURCES),
            }
        ),
    )
    request = {
        "idempotency_key": "durable-copy-choice",
        "ingest_source": "fixture",
        "archive_store": "deep",
        "use_cache": False,
        "copy_to": ["b2"],
        "initiator": actor,
        "event_context": None,
        "provenance_mode": "omitted",
        "provenance_omission_reason": "fixture has no source provenance",
    }
    opened = producer.create_or_resume(**request)
    assert opened["use_cache"] is False
    assert opened["copy_to"] == ["b2"]
    assert opened["copy_intents"][0]["state"] == "accepted"
    replay = producer.create_or_resume(
        **{**request, "archive_store": None, "use_cache": None, "copy_to": None}
    )
    assert replay["collection_id"] == opened["collection_id"]
    assert replay["resumed"] is True
    assert replay["copy_to"] == ["b2"]
    with pytest.raises(Conflict, match="idempotency identity changed"):
        producer.create_or_resume(**{**request, "copy_to": []})
    with pytest.raises(Conflict, match="initiator changed"):
        producer.create_or_resume(**{**request, "initiator": replace(actor, key_id="c" * 16)})
    with session_scope(make_session_factory(config.database_url)) as session:
        rows = session.scalars(
            select(CollectionUploadCopyIntentRecord).where(
                CollectionUploadCopyIntentRecord.collection_id == int(opened["collection_id"])
            )
        ).all()
        assert len(rows) == 1


def test_upload_copy_handoff_and_event_commit_together(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, _, _, _, service = _service(tmp_path / "catalog.sqlite3")
    _pending_upload_copy_intent(config)
    original_emit = service._emit

    def fail_event(*args: object, **kwargs: object) -> None:
        raise RuntimeError("injected event failure")

    monkeypatch.setattr(service, "_emit", fail_event)
    assert service.process_due_upload_copy_intents(limit=1) == 1
    with session_scope(make_session_factory(config.database_url)) as session:
        intent = session.get(CollectionUploadCopyIntentRecord, (COLLECTION_ID, "b2"))
        assert intent is not None and intent.state == "pending" and intent.attempts == 1
        assert session.get(ArchiveCopyJobRecord, (COLLECTION_ID, "b2")) is None
        intent.next_attempt_at = format_utc_timestamp(utc_now())
    monkeypatch.setattr(service, "_emit", original_emit)
    assert service.process_due_upload_copy_intents(limit=1) == 1
    with session_scope(make_session_factory(config.database_url)) as session:
        intent = session.get(CollectionUploadCopyIntentRecord, (COLLECTION_ID, "b2"))
        job = session.get(ArchiveCopyJobRecord, (COLLECTION_ID, "b2"))
        assert intent is not None and intent.state == "handed_off"
        assert intent.job_created is True
        assert job is not None and job.initiated_by_key_id == COPY_INTENT_KEY
        assert job.use_cache is False
        assert any(
            "archive_copy_job.requested" in event.event_json
            for event in session.scalars(select(LifecycleEventRecord))
        )
    assert service.process_due_upload_copy_intents(limit=1) == 0


def test_upload_copy_intent_does_not_claim_a_separately_created_job(tmp_path: Path) -> None:
    config, _, _, _, service = _service(tmp_path / "catalog.sqlite3")
    _pending_upload_copy_intent(config)
    independent = service.create_or_resume(
        COLLECTION_ID,
        source_store="deep",
        destination_store="b2",
        use_cache=False,
        initiator=INITIATOR,
    )
    assert service.process_due_upload_copy_intents(limit=1) == 1
    with session_scope(make_session_factory(config.database_url)) as session:
        intent = session.get(CollectionUploadCopyIntentRecord, (COLLECTION_ID, "b2"))
        job = session.get(ArchiveCopyJobRecord, (COLLECTION_ID, "b2"))
        assert intent is not None and intent.state == "failed"
        assert intent.failure_code == "destination_job_conflict"
        assert job is not None and job.initiated_by_key_id == INITIATOR.key_id
    assert service.get(COLLECTION_ID, destination_store="b2") == independent


def test_upload_publication_atomically_queues_the_accepted_copy(tmp_path: Path) -> None:
    database_url = sqlite_url(tmp_path / "catalog.sqlite3")
    baseline = RuntimeConfig.for_testing(database_url=database_url)
    deep = replace(baseline.archive_store("archive"), name="deep", base_url="http://127.0.0.1/deep")
    b2 = replace(deep, name="b2", base_url="http://127.0.0.1/b2")
    config = RuntimeConfig.for_testing(
        database_url=database_url,
        archive_write_store="deep",
        archive_read_order=("deep", "b2"),
        archive_stores={"deep": deep, "b2": b2},
        archive_scrypt_work_factor=1,
    )
    initialize_db(database_url)
    source = MemoryArchiveStore(new_archive_prefix="archives/deep/upload")
    destination = MemoryArchiveStore(new_archive_prefix="archives/b2/copy")
    registry = ArchiveStoreRegistry(
        {
            "deep": archive_store_binding(source),
            "b2": archive_store_binding(destination),
        }
    )
    now = format_utc_timestamp(utc_now())
    with session_scope(make_session_factory(database_url)) as session:
        session.add(
            AppKeyRecord(
                id=COPY_INTENT_KEY,
                app="producer",
                token_sha256="d" * 64,
                created_at=now,
            )
        )
        session.flush()
        for permission in (COLLECTIONS_CREATE, ARCHIVES_MANAGE):
            session.add(
                AppKeyAccessGrantRecord(
                    key_id=COPY_INTENT_KEY,
                    permission=permission,
                    resource=ALL_RESOURCES,
                    created_at=now,
                )
            )
    actor = Principal(
        id="producer",
        key_id=COPY_INTENT_KEY,
        access=frozenset(
            {
                ApplicationAccess(COLLECTIONS_CREATE, ALL_RESOURCES),
                ApplicationAccess(ARCHIVES_MANAGE, ALL_RESOURCES),
            }
        ),
    )
    uploader = SqlAlchemyCollectionUploadService(config, registry)
    opened = uploader.create_or_resume(
        idempotency_key="publish-with-copy",
        ingest_source="fixture",
        archive_store=None,
        use_cache=False,
        copy_to=["b2"],
        initiator=actor,
        event_context=None,
        provenance_mode="omitted",
        provenance_omission_reason="fixture has no source provenance",
    )
    collection_id = int(opened["collection_id"])
    content = b"durable copy intent from accepted upload"
    uploader.register_files(
        collection_id,
        (
            {
                "path": "document.txt",
                "bytes": str(len(content)),
                "sha256": hashlib.sha256(content).hexdigest(),
            },
        ),
    )
    uploader.complete(collection_id)
    volume = uploader.list_volumes(collection_id)["volumes"][0]
    unit = volume["units"][0]
    uploader.upload_unit(
        collection_id,
        str(volume["volume_id"]),
        int(unit["unit"]),
        plan_sha256=str(volume["plan_sha256"]),
        content=content,
    )
    for _ in range(256):
        result = uploader.get(collection_id)
        if result["state"] == "finalized":
            break
        assert uploader.process_due_finalizations() == 1
    else:
        raise AssertionError("upload did not finalize")
    with session_scope(make_session_factory(database_url)) as session:
        collection = session.get(CollectionRecord, collection_id)
        intent = session.get(CollectionUploadCopyIntentRecord, (collection_id, "b2"))
        assert collection is not None and collection.is_published
        assert session.get(CollectionUploadRecord, collection_id) is None
        assert intent is not None and intent.state == "pending"
        assert session.get(ArchiveCopyJobRecord, (collection_id, "b2")) is None
    worker = SqlAlchemyArchiveCopyJobService(config, registry)
    assert worker.process_due_upload_copy_intents(limit=1) == 1
    observed = worker.get_upload_copy_intents(collection_id, principal=actor)
    assert observed["intents"][0]["state"] == "handed_off"
    assert observed["intents"][0]["job_state"] == "requested"
    source.new_archive_prefix = "archives/deep/canceled-upload"
    canceled_upload = uploader.create_or_resume(
        idempotency_key="cancel-with-copy",
        ingest_source="fixture",
        archive_store=None,
        use_cache=False,
        copy_to=["b2"],
        initiator=actor,
        event_context=None,
        provenance_mode="omitted",
        provenance_omission_reason="fixture has no source provenance",
    )
    canceled_id = int(canceled_upload["collection_id"])
    assert uploader.cancel(canceled_id)["state"] == "canceled"
    with session_scope(make_session_factory(database_url)) as session:
        intent = session.get(CollectionUploadCopyIntentRecord, (canceled_id, "b2"))
        assert intent is not None and intent.state == "canceled"
        assert session.get(CollectionUploadRecord, canceled_id) is None
        assert session.get(ArchiveCopyJobRecord, (canceled_id, "b2")) is None


def test_upload_copy_handoff_fails_closed_after_key_revocation(tmp_path: Path) -> None:
    config, _, _, _, service = _service(tmp_path / "catalog.sqlite3")
    _pending_upload_copy_intent(config)
    with session_scope(make_session_factory(config.database_url)) as session:
        key = session.get(AppKeyRecord, COPY_INTENT_KEY)
        assert key is not None
        key.revoked_at = format_utc_timestamp(utc_now())
    assert service.process_due_upload_copy_intents(limit=1) == 1
    with session_scope(make_session_factory(config.database_url)) as session:
        intent = session.get(CollectionUploadCopyIntentRecord, (COLLECTION_ID, "b2"))
        assert intent is not None and intent.state == "failed"
        assert intent.failure_code == "authorization_denied"
        assert session.get(ArchiveCopyJobRecord, (COLLECTION_ID, "b2")) is None
    manager = Principal(
        id="second-operator",
        key_id=None,
        access=frozenset({ApplicationAccess(ARCHIVES_MANAGE, ALL_RESOURCES)}),
    )
    observed = service.get_upload_copy_intents(COLLECTION_ID, principal=manager)
    assert observed["copy_to"] == ["b2"]
    assert observed["intents"][0]["failure_code"] == "authorization_denied"


def test_upload_copy_handoff_rejects_a_remapped_destination(tmp_path: Path) -> None:
    config, _, _, _, service = _service(tmp_path / "catalog.sqlite3")
    _pending_upload_copy_intent(config)
    service._config = replace(
        config,
        archive_stores={
            **config.archive_stores,
            "b2": replace(config.archive_store("b2"), base_url="http://127.0.0.1/other"),
        },
    )
    assert service.process_due_upload_copy_intents(limit=1) == 1
    with session_scope(make_session_factory(config.database_url)) as session:
        intent = session.get(CollectionUploadCopyIntentRecord, (COLLECTION_ID, "b2"))
        assert intent is not None and intent.state == "failed"
        assert intent.failure_code == "configuration_changed"
        assert session.get(ArchiveCopyJobRecord, (COLLECTION_ID, "b2")) is None


def test_copy_job_explicit_cache_choice_overrides_direct_store_policy(tmp_path: Path) -> None:
    config, _, source, destination, service = _service(tmp_path / "catalog.sqlite3")
    registry = ArchiveStoreRegistry(
        {
            "deep": archive_store_binding(source),
            "b2": archive_store_binding(destination),
        }
    )
    cached = SqlAlchemyArchiveCopyJobService(
        config,
        registry,
        retrieval_cache=_ArchiveCopyCache(),  # type: ignore[arg-type]
    )
    opened = cached.create_or_resume(
        COLLECTION_ID,
        destination_store="b2",
        use_cache=True,
        initiator=INITIATOR,
    )
    assert opened["use_cache"] is True
    assert isinstance(
        cached._volume_object_store(
            store_name="b2", collection_id=COLLECTION_ID, object_id=PACK_ID
        ),
        MirroredArchiveResumableObjectStore,
    )
    with pytest.raises(Conflict, match="cache choice changed"):
        cached.create_or_resume(
            COLLECTION_ID,
            destination_store="b2",
            use_cache=False,
            initiator=INITIATOR,
        )
    assert service.get(COLLECTION_ID, destination_store="b2")["use_cache"] is True


def test_archive_copy_preserves_the_independent_object_manifest(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="riverhog.transfer")
    config, archive, source, destination, service = _service(tmp_path / "catalog.sqlite3")

    requested = service.create_or_resume(
        COLLECTION_ID,
        source_store="deep",
        destination_store="b2",
        initiator=INITIATOR,
        event_context={"workflow": "promotion"},
    )
    assert service.process_due(limit=1) == 1

    assert requested["state"] == "requested"
    prefix = "archives/b2/new-copy"
    assert destination.objects == {
        f"{prefix}/{relative_path}": content
        for relative_path, content in archive.stored_objects.items()
    }
    expected_ids = (
        archive.pack_plan.volume_id,
        f"volume-metadata-{0:064x}",
        f"volume-terminal-{1:064x}",
        "manifest",
        "recovery-descriptor",
    )
    assert source.prepared == [expected_ids]
    assert source.cleaned == [expected_ids]
    with session_scope(make_session_factory(config.database_url)) as session:
        copy = session.get(CollectionArchiveCopyRecord, (COLLECTION_ID, "b2"))
        assert copy is not None
        assert [(current.kind, current.object_id) for current in copy.objects] == [
            ("pack", archive.pack_plan.volume_id),
            ("volume-metadata", f"volume-metadata-{0:064x}"),
            ("volume-terminal", f"volume-terminal-{1:064x}"),
            ("manifest", "manifest"),
            ("recovery-descriptor", "recovery-descriptor"),
        ]
        pack = copy.objects[0]
        assert [current.path for current in pack.placements] == sorted(FILES)
        assert pack.plan_sha256 == archive.pack_plan_sha256
        assert pack.index_sha256 == archive.pack_index_sha256
        job = session.get(ArchiveCopyJobRecord, (COLLECTION_ID, "b2"))
        assert job is not None
        assert job.state == "completed"
        assert job.finished_at is not None
    shown = service.get(COLLECTION_ID, destination_store="b2")
    listed = service.list(
        page_size=25,
        position=None,
        q="b2",
        sort="requested_at",
        order="desc",
    )
    assert shown["state"] == "completed"
    assert shown["initiated_by_app"] == "operator"
    assert (
        service.create_or_resume(
            COLLECTION_ID,
            source_store="deep",
            destination_store="b2",
            initiator=INITIATOR,
        )
        == shown
    )
    assert listed["jobs"] == [shown]
    events = (
        SqlAlchemyLifecycleEventService(config)
        .page(
            owner_principal_id="operator",
            after=None,
            limit=100,
        )
        .events
    )
    assert [event.type.rsplit(".", 1)[-1] for event in events] == [
        "requested",
        "completed",
    ]
    assert events[-1].data["context"] == {"workflow": "promotion"}
    transfer_messages = [message for message in caplog.messages if "transfer operation=" in message]
    assert any("operation=archive_copy_segment" in message for message in transfer_messages)
    assert sum("operation=archive_copy_object" in message for message in transfer_messages) == 4
    assert all("integrity_seconds=" in message for message in transfer_messages)
    assert all(PACK_ID not in message for message in transfer_messages)


def test_existing_stored_copy_without_job_cannot_create_a_synthetic_job(
    tmp_path: Path,
) -> None:
    _config, _archive, _source, _destination, service = _service(tmp_path / "catalog.sqlite3")

    with pytest.raises(Conflict, match="already has an uploaded archive copy"):
        service.create_or_resume(
            COLLECTION_ID,
            source_store="b2",
            destination_store="deep",
            initiator=INITIATOR,
        )


def test_failed_archive_copy_job_has_terminal_evidence_and_can_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    config, _archive, _source, _destination, service = _service(tmp_path / "catalog.sqlite3")
    service.create_or_resume(
        COLLECTION_ID,
        source_store="deep",
        destination_store="b2",
        initiator=INITIATOR,
    )

    def fail_copy(*, collection_id: int, destination_store: str) -> None:
        assert collection_id == COLLECTION_ID
        assert destination_store == "b2"
        raise RuntimeError("test transfer failure")

    monkeypatch.setattr(service, "_process_one", fail_copy)
    with caplog.at_level(logging.ERROR, logger="riverhog_core.services.archive_copy_jobs"):
        assert service.process_due(limit=1) == 1
    failed = service.get(COLLECTION_ID, destination_store="b2")
    assert failed["state"] == "failed"
    assert failed["finished_at"] is not None
    assert failed["failure"] == "RuntimeError: test transfer failure"
    events = (
        SqlAlchemyLifecycleEventService(config)
        .page(owner_principal_id="operator", after=None, limit=100)
        .events
    )
    assert [event.type.rsplit(".", 1)[-1] for event in events] == [
        "requested",
        "failed",
    ]

    restarted = service.create_or_resume(
        COLLECTION_ID,
        source_store="deep",
        destination_store="b2",
        initiator=INITIATOR,
    )
    assert restarted["state"] == "requested"
    assert restarted["finished_at"] is None
    assert restarted["failure"] is None


def test_archive_copy_pipelines_source_parts_into_parallel_destination_requests(
    tmp_path: Path,
) -> None:
    archive = _multiple_archive_parts(FILES)

    class ConcurrentDestination(MemoryArchiveStore):
        def __init__(self) -> None:
            super().__init__(new_archive_prefix="archives/b2/new-copy")
            self.lock = threading.Lock()
            self.rendezvous = threading.Barrier(2)
            self.active = 0
            self.maximum_active = 0

        def write_segment(
            self,
            *,
            session: WriteSession,
            number: int,
            content: bytes,
        ) -> WriteSegmentReceipt:
            with self.lock:
                self.active += 1
                self.maximum_active = max(self.maximum_active, self.active)
            if number <= 2:
                self.rendezvous.wait(timeout=2)
            try:
                return super().write_segment(session=session, number=number, content=content)
            finally:
                with self.lock:
                    self.active -= 1

    destination = ConcurrentDestination()
    _config, archive, _source, destination, service = _service(
        tmp_path / "catalog.sqlite3",
        destination=destination,
        archive=archive,
    )
    service.create_or_resume(
        COLLECTION_ID,
        source_store="deep",
        destination_store="b2",
        initiator=INITIATOR,
    )

    assert service.process_due(limit=1) == 1
    assert destination.maximum_active >= 2
    assert (
        destination.objects[f"archives/b2/new-copy/volumes/{archive.pack_plan.volume_id}.tar.age"]
        == archive.stored_objects[f"volumes/{archive.pack_plan.volume_id}.tar.age"]
    )


def test_archive_copy_preserves_immutable_provenance_objects(tmp_path: Path) -> None:
    archive = make_captured_provenance_archive(FILES, tmp_path / "source")
    config, archive, source, destination, service = _service(
        tmp_path / "catalog.sqlite3",
        archive=archive,
    )

    service.create_or_resume(
        COLLECTION_ID,
        source_store="deep",
        destination_store="b2",
        initiator=INITIATOR,
    )
    assert service.process_due(limit=1) == 1
    assert archive.provenance is not None

    expected_ids = (
        PACK_ID,
        VOLUME_METADATA_ID,
        VOLUME_TERMINAL_ID,
        *(
            f"provenance-payload-{item.document.sequence:064x}"
            for item in archive.provenance.volumes
        ),
        *(
            f"provenance-volume-{item.document.sequence:064x}"
            for item in archive.provenance.volumes
        ),
        f"provenance-terminal-{len(archive.provenance.volumes):064x}",
        "provenance-root",
        "manifest",
        "recovery-descriptor",
    )
    prefix = "archives/b2/new-copy"
    assert destination.objects == {
        f"{prefix}/{relative_path}": content
        for relative_path, content in archive.stored_objects.items()
    }
    assert source.prepared == [expected_ids]
    assert source.cleaned == [expected_ids]
    with session_scope(make_session_factory(config.database_url)) as session:
        copy = session.get(CollectionArchiveCopyRecord, (COLLECTION_ID, "b2"))
        assert copy is not None
        assert [(current.kind, current.object_id) for current in copy.objects] == [
            ("pack", PACK_ID),
            ("volume-metadata", VOLUME_METADATA_ID),
            ("volume-terminal", VOLUME_TERMINAL_ID),
            *(
                (
                    "provenance-bindings"
                    if item.document.payload.kind == "bindings"
                    else "provenance-journal-segment",
                    f"provenance-payload-{item.document.sequence:064x}",
                )
                for item in archive.provenance.volumes
            ),
            *(
                (
                    "provenance-volume-metadata",
                    f"provenance-volume-{item.document.sequence:064x}",
                )
                for item in archive.provenance.volumes
            ),
            (
                "provenance-terminal",
                f"provenance-terminal-{len(archive.provenance.volumes):064x}",
            ),
            ("provenance-root", "provenance-root"),
            ("manifest", "manifest"),
            ("recovery-descriptor", "recovery-descriptor"),
        ]


def test_archive_copy_to_restore_required_store_writes_final_custody(
    tmp_path: Path,
) -> None:
    config, archive = seed_archive_copy(
        tmp_path / "catalog.sqlite3",
        FILES,
        store="b2",
    )
    deep = replace(
        config.archive_store("b2"),
        name="deep",
        base_url="http://127.0.0.1/deep",
    )
    config = replace(
        config,
        archive_stores={"b2": config.archive_store("b2"), "deep": deep},
        archive_read_order=("b2", "deep"),
    )
    source = MemoryArchiveStore(archive, new_archive_prefix="archives/b2/new-copy")
    destination = MemoryArchiveStore(
        new_archive_prefix="archives/deep/new-copy",
        read_mode="restore_required",
    )
    cache = _ArchiveCopyCache()
    service = SqlAlchemyArchiveCopyJobService(
        config,
        ArchiveStoreRegistry(
            {
                "b2": archive_store_binding(source),
                "deep": archive_store_binding(destination),
            }
        ),
        retrieval_cache=cache,  # type: ignore[arg-type]
    )
    service.create_or_resume(
        COLLECTION_ID,
        source_store="b2",
        destination_store="deep",
        initiator=INITIATOR,
    )

    assert service.process_due(limit=1) == 1

    with session_scope(make_session_factory(config.database_url)) as session:
        copy = session.get(CollectionArchiveCopyRecord, (COLLECTION_ID, "deep"))
        checkpoints = session.scalars(select(ArchiveCopyObjectUploadRecord)).all()
        assert copy is not None
        assert checkpoints == []
        cached = session.get(
            RetrievalCacheObjectRecord,
            ("deep", COLLECTION_ID, PACK_ID),
        )
        lease = session.get(
            RetrievalCacheLeaseRecord,
            ("new-archive", "deep", COLLECTION_ID, PACK_ID),
        )
        assert cached is not None
        assert lease is not None
    assert set(destination.objects) == {
        f"archives/deep/new-copy/volumes/{archive.pack_plan.volume_id}.tar.age",
        f"archives/deep/new-copy/metadata/volume-{0:064x}.json.age",
        f"archives/deep/new-copy/metadata/volume-{1:064x}.json.age",
        "archives/deep/new-copy/manifest.json.age",
        "archives/deep/new-copy/recovery.json",
    }
    pack_path = f"archives/deep/new-copy/volumes/{archive.pack_plan.volume_id}.tar.age"
    assert cache.store.objects[cached.object_path] == destination.objects[pack_path]


def test_restore_required_copy_uses_archive_only_when_new_archive_cache_is_disabled(
    tmp_path: Path,
) -> None:
    config, archive = seed_archive_copy(
        tmp_path / "catalog.sqlite3",
        FILES,
        store="b2",
    )
    deep = replace(
        config.archive_store("b2"),
        name="deep",
        base_url="http://127.0.0.1/deep",
    )
    config = replace(
        config,
        archive_stores={"b2": config.archive_store("b2"), "deep": deep},
        retrieval_cache_new_archive_enabled=False,
    )
    source = MemoryArchiveStore(archive, new_archive_prefix="archives/b2/new-copy")
    destination = MemoryArchiveStore(
        new_archive_prefix="archives/deep/new-copy",
        read_mode="restore_required",
    )
    service = SqlAlchemyArchiveCopyJobService(
        config,
        ArchiveStoreRegistry(
            {
                "b2": archive_store_binding(source),
                "deep": archive_store_binding(destination),
            }
        ),
        retrieval_cache=_ArchiveCopyCache(),  # type: ignore[arg-type]
    )

    selected = service._volume_object_store(
        store_name="deep",
        collection_id=COLLECTION_ID,
        object_id=PACK_ID,
    )

    assert selected is destination


def test_archive_copy_waits_for_selected_source_objects(tmp_path: Path) -> None:
    config, _archive, source, destination, service = _service(
        tmp_path / "catalog.sqlite3", source_ready=False
    )
    service.create_or_resume(
        COLLECTION_ID,
        destination_store="b2",
        initiator=INITIATOR,
    )

    assert service.process_due(limit=1) == 1

    with session_scope(make_session_factory(config.database_url)) as session:
        job = session.get(ArchiveCopyJobRecord, (COLLECTION_ID, "b2"))
        assert job is not None and job.state == "waiting"
    assert source.prepared == [
        (
            PACK_ID,
            VOLUME_METADATA_ID,
            VOLUME_TERMINAL_ID,
            "manifest",
            "recovery-descriptor",
        )
    ]
    assert destination.objects == {}


def test_archive_copy_checks_remote_source_outside_its_catalog_transaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, _archive, source, _destination, service = _service(
        tmp_path / "catalog.sqlite3",
        source_ready=False,
    )
    service.create_or_resume(COLLECTION_ID, destination_store="b2", initiator=INITIATOR)
    original_prepare = source.prepare_archive_objects_read

    def inspect_claim(
        *,
        objects: Sequence[ArchiveObjectIdentity],
        **kwargs: object,
    ) -> ArchiveReadStatus:
        with session_scope(make_session_factory(config.database_url)) as session:
            job = session.get(ArchiveCopyJobRecord, (COLLECTION_ID, "b2"))
            assert job is not None and job.state == "checking"
        return original_prepare(objects=objects, **kwargs)

    monkeypatch.setattr(source, "prepare_archive_objects_read", inspect_claim)

    assert service.process_due(limit=1) == 1
    assert service.get(COLLECTION_ID, destination_store="b2")["state"] == "waiting"


def test_archive_copy_canceled_during_source_check_cleans_the_requested_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _config, _archive, source, destination, service = _service(
        tmp_path / "catalog.sqlite3",
        source_ready=False,
    )
    service.create_or_resume(COLLECTION_ID, destination_store="b2", initiator=INITIATOR)
    original_prepare = source.prepare_archive_objects_read

    def cancel_during_check(
        *,
        objects: Sequence[ArchiveObjectIdentity],
        **kwargs: object,
    ) -> ArchiveReadStatus:
        canceling = service.cancel(COLLECTION_ID, destination_store="b2")
        assert canceling["state"] == "canceling"
        return original_prepare(objects=objects, **kwargs)

    monkeypatch.setattr(source, "prepare_archive_objects_read", cancel_during_check)

    assert service.process_due(limit=1) == 1
    assert service.get(COLLECTION_ID, destination_store="b2")["state"] == "canceled"
    assert source.cleaned == [
        (
            PACK_ID,
            VOLUME_METADATA_ID,
            VOLUME_TERMINAL_ID,
            "manifest",
            "recovery-descriptor",
        )
    ]
    assert destination.discarded_uploads == ["archives/b2/new-copy"]


def test_archive_copy_cancellation_closes_waiting_job_and_discards_prefix(
    tmp_path: Path,
) -> None:
    config, _archive, source, destination, service = _service(
        tmp_path / "catalog.sqlite3", source_ready=False
    )
    service.create_or_resume(COLLECTION_ID, destination_store="b2", initiator=INITIATOR)
    service.process_due(limit=1)

    canceled = service.cancel(COLLECTION_ID, destination_store="b2")

    assert canceled["state"] == "canceled"
    assert canceled["finished_at"] is not None
    assert source.cleaned == [
        (
            PACK_ID,
            VOLUME_METADATA_ID,
            VOLUME_TERMINAL_ID,
            "manifest",
            "recovery-descriptor",
        )
    ]
    assert destination.discarded_uploads == ["archives/b2/new-copy"]
    filtered = service.list(
        page_size=25,
        position=None,
        q=None,
        state="canceled",
        sort="requested_at",
        order="desc",
    )
    assert filtered["filters"] == {"state": "canceled"}
    assert filtered["jobs"] == [canceled]
    events = (
        SqlAlchemyLifecycleEventService(config)
        .page(
            owner_principal_id="operator",
            after=None,
            limit=100,
        )
        .events
    )
    assert [event.type.rsplit(".", 1)[-1] for event in events] == [
        "requested",
        "canceled",
    ]

    restarted = service.create_or_resume(
        COLLECTION_ID,
        destination_store="b2",
        initiator=INITIATOR,
    )
    assert restarted["state"] == "requested"


def test_archive_copy_cancellation_stops_an_active_transfer_before_commit(
    tmp_path: Path,
) -> None:
    started = threading.Event()
    release = threading.Event()

    class BlockingDestination(MemoryArchiveStore):
        def write_segment(
            self,
            *,
            session: WriteSession,
            number: int,
            content: bytes,
        ) -> WriteSegmentReceipt:
            started.set()
            assert release.wait(timeout=5)
            return super().write_segment(session=session, number=number, content=content)

    destination = BlockingDestination(new_archive_prefix="archives/b2/new-copy")
    config, _archive, _source, _destination, service = _service(
        tmp_path / "catalog.sqlite3",
        destination=destination,
    )
    service.create_or_resume(COLLECTION_ID, destination_store="b2", initiator=INITIATOR)
    worker = threading.Thread(target=service.process_due, daemon=True)
    worker.start()
    assert started.wait(timeout=5)

    canceling = service.cancel(COLLECTION_ID, destination_store="b2")
    assert canceling["state"] == "canceling"
    release.set()
    worker.join(timeout=5)
    assert not worker.is_alive()
    while service.get(COLLECTION_ID, destination_store="b2")["state"] == "canceling":
        assert service.process_due(limit=1) == 1

    canceled = service.get(COLLECTION_ID, destination_store="b2")
    assert canceled["state"] == "canceled"
    assert canceled["finished_at"] is not None
    assert destination.objects == {}
    assert destination._writes == {}
    assert destination.discarded_uploads == ["archives/b2/new-copy"]
    with session_scope(make_session_factory(config.database_url)) as session:
        assert session.get(CollectionArchiveCopyRecord, (COLLECTION_ID, "b2")) is None


@pytest.mark.parametrize("interrupted_state", ["checking", "copying"])
def test_startup_resumes_a_claimed_archive_copy(
    tmp_path: Path,
    interrupted_state: str,
) -> None:
    config, _archive, _source, _destination, service = _service(tmp_path / "catalog.sqlite3")
    service.create_or_resume(
        COLLECTION_ID,
        destination_store="b2",
        initiator=INITIATOR,
    )
    factory = make_session_factory(config.database_url)
    with session_scope(factory) as session:
        job = session.get(ArchiveCopyJobRecord, (COLLECTION_ID, "b2"))
        assert job is not None
        job.state = interrupted_state

    assert service.requeue_interrupted_jobs_for_startup() == 1
    with session_scope(factory) as session:
        job = session.get(ArchiveCopyJobRecord, (COLLECTION_ID, "b2"))
        assert job is not None
        assert job.state == "requested"
        assert job.next_attempt_at is not None
