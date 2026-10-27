from __future__ import annotations

import hashlib
from dataclasses import replace
from pathlib import Path

import pytest
from riverhog_client import (
    IncrementalCollectionProducer,
    ProducerArtifactIdentity,
    ProducerFile,
)
from riverhog_client.initial_tags import prepare_initial_collection_tags
from riverhog_core.app_permissions import (
    ALL_RESOURCES,
    COLLECTIONS_CREATE,
    ApplicationAccess,
    Principal,
)
from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.catalog_db import initialize_db, make_session_factory, session_scope
from riverhog_core.catalog_models import (
    CollectionArchiveObjectUploadRecord,
    CollectionArtifactRecord,
    CollectionRecord,
    CollectionUploadRecord,
)
from riverhog_core.collection_plan import CollectionVolumePolicy
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services.collection_uploads import SqlAlchemyCollectionUploadService
from riverhog_protocol import (
    COLLECTION_TAG_REQUEST_MEMBERS_MAX,
    ArtifactId,
    ArtifactMemberIdentityDocument,
    CollectionArtifactProvenanceBindingDocument,
    CollectionUploadArtifactCustodyReceiptDocument,
    CollectionUploadCustodyObjectDocument,
    CollectionUploadProvenanceCustodyObjectDocument,
    CollectionUploadWorkBatchDocument,
)
from riverhog_protocol.errors import NotFound
from riverhog_protocol.manifest import artifact_set_identity_ordered
from sqlalchemy import func, select

from tests.support.upload_api import UploadServiceApi
from tests.unit.archive_object_fixtures import MemoryArchiveStore, archive_store_binding
from tests.unit.db_helpers import sqlite_url
from tests.unit.storage_incarnation_fixtures import seed_storage_incarnation


class _CustodyApi:
    def __init__(self) -> None:
        self.rows = {}
        self.registration_calls = 0
        self.bindings = {}
        self.journals = {}
        self.completed = None
        self.heartbeats = 0
        self.session_calls = 0
        self.initial_tags = ()
        self.initial_tag_set_identity = ""
        self.tag_batches = []

    def spawn(self):
        return self

    def close(self):
        pass

    def create_or_resume_collection_upload_session(self, _key, **kwargs):
        assert kwargs["custody_mode"] == "custody-transfer"
        self.initial_tags = tuple(kwargs.get("tags", ()))
        self.initial_tag_set_identity = kwargs["initial_tag_set_identity"]
        self.session_calls += 1
        return {
            "collection_id": "42",
            "resumed": self.session_calls > 1,
            "construction_identity_sha256": "c" * 64,
            "state": "open",
            "delivery_context_id": "urn:uuid:11111111-1111-4111-8111-111111111111",
            "registration_constraints": {
                "pack_member_bytes": "1024",
                "raw_part_plaintext_bytes": "65536",
            },
        }

    def get_collection_upload_session(self, _collection_id):
        return {
            "collection_id": "42",
            "state": "finalized" if self.completed is not None else "open",
            "delivery_context_id": "urn:uuid:11111111-1111-4111-8111-111111111111",
            "construction_identity_sha256": "c" * 64,
            "registration_constraints": {
                "pack_member_bytes": "1024",
                "raw_part_plaintext_bytes": "65536",
            },
            **(self.completed or {}),
        }

    def add_collection_upload_session_tags(self, _collection_id, tags):
        self.tag_batches.append(tuple(tags))
        return {"collection_id": "42", "added": len(tags), "tag_count": len(tags)}

    def heartbeat_collection_upload_session(self, _collection_id):
        self.heartbeats += 1
        return {"state": "open"}

    def _receipt(self, row):
        artifact_id = row["artifact_id"]
        primary = self.bindings.get(artifact_id)
        if primary is None:
            return None
        return CollectionUploadArtifactCustodyReceiptDocument.seal(
            collection_id=42,
            artifact_id=artifact_id,
            bytes=int(row["bytes"]),
            sha256=row["sha256"],
            primary=primary,
            completion_requirement_sha256=None,
            archive_objects=(
                CollectionUploadCustodyObjectDocument(
                    volume_id="pack-" + "0" * 64,
                    sealed_receipt_sha256="f" * 64,
                ),
            ),
            provenance_objects=(
                CollectionUploadProvenanceCustodyObjectDocument(
                    object_id="fixture-primary",
                    relative_path="provenance/payloads/fixture.bin.age",
                    plaintext_bytes=str(primary.journal.prefix_bytes),
                    plaintext_sha256=primary.journal.prefix_sha256,
                    sealed_receipt_sha256="e" * 64,
                ),
            ),
        ).model_dump(mode="json")

    def list_collection_upload_session_artifacts(self, _collection_id, **_kwargs):
        return {
            "page_size": 100,
            "next_page_token": None,
            "artifacts": [
                {**row, "custody_receipt": self._receipt(row)} for row in self.rows.values()
            ],
        }

    def get_collection_upload_session_artifact(self, _collection_id, artifact_id):
        if artifact_id not in self.rows:
            raise NotFound("artifact absent")
        row = self.rows[artifact_id]
        return {**row, "custody_receipt": self._receipt(row)}

    def register_collection_upload_session_artifacts(self, _collection_id, artifacts, **_kwargs):
        self.registration_calls += 1
        for supplied in artifacts:
            row = dict(supplied)
            key = row["artifact_id"]
            assert self.rows.setdefault(key, row) == row
        return {
            "artifacts": [
                {**self.rows[row["artifact_id"]], "custody_receipt": self._receipt(row)}
                for row in artifacts
            ],
            "volumes": [],
        }

    def acquire_collection_upload_session_work(self, collection_id, *, limit=16):
        return CollectionUploadWorkBatchDocument(
            collection_id=str(collection_id),
            planning_complete=True,
            complete=True,
            committed_payload_bytes="0",
            work=[],
        )

    def upload_collection_upload_session_provenance_journal(
        self, _collection_id, journal_id, *, content, byte_count, sha256, **_kwargs
    ):
        raw = b"".join(content)
        assert (len(raw), hashlib.sha256(raw).hexdigest()) == (byte_count, sha256)
        assert self.journals.setdefault(journal_id, raw) == raw

    def bind_collection_upload_session_artifact_provenance(self, _collection_id, batch):
        for binding in batch.bindings:
            assert self.bindings.setdefault(binding.artifact_id, binding) == binding

        return batch

    def get_collection_upload_session_artifact_provenance_binding(
        self, _collection_id, artifact_id
    ):
        if artifact_id not in self.bindings:
            raise NotFound("binding absent")
        return self.bindings[artifact_id]

    def set_collection_upload_session_materialization_decisions(self, _collection_id, batch):
        return batch

    def complete_collection_upload_session(self, _collection_id):
        identity = artifact_set_identity_ordered(
            ArtifactMemberIdentityDocument.model_validate(row)
            for _key, row in sorted(self.rows.items())
        )
        self.completed = {
            "state": "finalized",
            "artifact_set_identity": identity,
            "archive_root_sha256": "e" * 64,
            "collection": {
                "id": 42,
                "artifact_set_identity": identity,
                "archive_root_sha256": "e" * 64,
            },
        }
        return dict(self.completed)


def _producer(api: _CustodyApi, primary_store) -> IncrementalCollectionProducer:
    return IncrementalCollectionProducer(
        api,  # type: ignore[arg-type]
        primary_store=primary_store,
        member_inventory=lambda: (
            ProducerArtifactIdentity(row["artifact_id"], int(row["bytes"]), row["sha256"])
            for _key, row in sorted(api.rows.items())
        ),
        producer_app="fixture-target",
        adapter_id="fixture-target/v1",
        adapter_version="1.0.0",
        ingest_source="processing:fixture",
        source_event_id="fixture-execution",
        idempotency_key="fixture-execution",
    )


def test_incremental_producer_stages_unbounded_logical_tags_in_bounded_requests(
    primary_store,
) -> None:
    api = _CustodyApi()
    tags = tuple(f"classification/{index:04d}" for index in range(205))

    producer = IncrementalCollectionProducer(
        api,  # type: ignore[arg-type]
        primary_store=primary_store,
        member_inventory=lambda: (
            ProducerArtifactIdentity(row["artifact_id"], int(row["bytes"]), row["sha256"])
            for _key, row in sorted(api.rows.items())
        ),
        producer_app="fixture-target",
        adapter_id="fixture-target/v1",
        adapter_version="1.0.0",
        ingest_source="processing:fixture",
        source_event_id="fixture-execution",
        tags=tags,
    )
    producer.stop()

    assert api.initial_tags == tags[:COLLECTION_TAG_REQUEST_MEMBERS_MAX]
    with prepare_initial_collection_tags(tags) as prepared:
        assert api.initial_tag_set_identity == prepared.tag_set_identity
    assert api.tag_batches == [
        tags[COLLECTION_TAG_REQUEST_MEMBERS_MAX : 2 * COLLECTION_TAG_REQUEST_MEMBERS_MAX],
        tags[2 * COLLECTION_TAG_REQUEST_MEMBERS_MAX :],
    ]
    assert all(len(batch) <= COLLECTION_TAG_REQUEST_MEMBERS_MAX for batch in api.tag_batches)


def test_incremental_producer_rejects_late_invalid_tags_before_remote_mutation(
    primary_store,
) -> None:
    api = _CustodyApi()
    tags = [*(f"classification/{index:04d}" for index in range(150)), " invalid"]

    with pytest.raises(ValueError):
        IncrementalCollectionProducer(
            api,  # type: ignore[arg-type]
            primary_store=primary_store,
            member_inventory=lambda: (
                ProducerArtifactIdentity(row["artifact_id"], int(row["bytes"]), row["sha256"])
                for _key, row in sorted(api.rows.items())
            ),
            producer_app="fixture-target",
            adapter_id="fixture-target/v1",
            adapter_version="1.0.0",
            ingest_source="processing:fixture",
            source_event_id="fixture-execution",
            tags=tags,
        )
    assert api.session_calls == 0


def test_incremental_producer_resumes_without_rereading_custodied_local_bytes(
    primary_store, tmp_path
):
    api = _CustodyApi()
    source = tmp_path / "artifact.bin"
    payload = b"completed artifact"
    source.write_bytes(payload)
    artifact_id = ArtifactId("a" * 64)
    identity = ProducerArtifactIdentity(
        artifact_id, len(payload), hashlib.sha256(payload).hexdigest()
    )
    first = _producer(api, primary_store)
    try:
        receipts = first.append_inputs(
            (ProducerFile(source, artifact_id, allow_missing_materialization_hint=True),)
        )
        assert identity in {receipt.artifact for receipt in receipts}
    finally:
        first.stop()
    source.unlink()
    resumed = _producer(api, primary_store)
    try:
        before_resume = api.registration_calls
        receipt = resumed.resume_artifact_custody(identity)
        assert api.registration_calls == before_resume
        assert receipt is not None and receipt.artifact == identity
        produced = resumed.finish()
    finally:
        resumed.stop()
    assert produced.collection_id == 42
    assert produced.archive_root_sha256 == "e" * 64
    assert tuple(api.rows) == (artifact_id,)
    assert len(api.journals) == 1
    assert api.completed is not None


def test_incremental_producer_keeps_local_custody_when_receipt_identity_is_wrong(
    primary_store, tmp_path
):
    class WrongReceiptApi(_CustodyApi):
        def _receipt(self, row):
            primary = self.bindings.get(row["artifact_id"])
            if primary is None:
                return None
            wrong = "b" * 64
            wrong_primary = CollectionArtifactProvenanceBindingDocument.model_validate(
                {
                    **primary.model_dump(mode="json"),
                    "artifact_id": wrong,
                }
            )
            self.bindings[wrong] = wrong_primary
            return super()._receipt({**row, "artifact_id": wrong})

    api = WrongReceiptApi()
    source = tmp_path / "artifact.bin"
    source.write_bytes(b"completed artifact")
    producer = _producer(api, primary_store)
    try:
        with pytest.raises(ValueError, match="upload member identity"):
            producer.append_inputs(
                (ProducerFile(source, "a" * 64, allow_missing_materialization_hint=True),)
            )
        assert "a" * 64 in producer._sources
        assert source.exists()
    finally:
        producer.stop()


def test_incremental_producer_keeps_equal_bytes_as_distinct_members(primary_store, tmp_path):
    api = _CustodyApi()
    source = tmp_path / "source.bin"
    source.write_bytes(b"identical bytes")
    producer = _producer(api, primary_store)
    try:
        producer.append_inputs(
            tuple(
                ProducerFile(source, key * 64, allow_missing_materialization_hint=True)
                for key in ("a", "b")
            )
        )
        producer.finish()
    finally:
        producer.stop()
    assert tuple(api.rows) == ("a" * 64, "b" * 64)
    assert len(api.journals) == 2
    assert api.rows["a" * 64]["sha256"] == api.rows["b" * 64]["sha256"]
    assert api.bindings["a" * 64].journal != api.bindings["b" * 64].journal


def _bounded_service_api(
    tmp_path: Path, *, policy: CollectionVolumePolicy | None = None
) -> tuple[UploadServiceApi, MemoryArchiveStore]:
    database_url = sqlite_url(tmp_path / "catalog.sqlite3")
    config = RuntimeConfig.for_testing(database_url=database_url, archive_scrypt_work_factor=1)
    initialize_db(database_url)
    with session_scope(make_session_factory(database_url)) as session:
        seed_storage_incarnation(session, "archive", "archive")
    store = MemoryArchiveStore()
    binding = replace(archive_store_binding(store), store=store)
    service = SqlAlchemyCollectionUploadService(
        config,
        ArchiveStoreRegistry({"archive": binding}),
        policy=policy
        or CollectionVolumePolicy(
            pack_source_bytes=1024,
            pack_artifacts=4,
            pack_member_bytes=1024,
            pack_part_plaintext_bytes=5 * 1024 * 1024,
            raw_volume_plaintext_bytes=5 * 1024 * 1024,
            raw_part_plaintext_bytes=5 * 1024 * 1024,
        ),
    )
    principal = Principal(
        id="fixture-target",
        key_id="fixture-key",
        access=frozenset({ApplicationAccess(COLLECTIONS_CREATE, ALL_RESOURCES)}),
    )
    return UploadServiceApi(service, principal), store


def test_many_artifact_publication_retains_only_the_unsealed_pack_window(
    primary_store,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RIVERHOG_UPLOAD_FILE_CONCURRENCY", "1")
    api, store = _bounded_service_api(tmp_path)
    identities = {}
    producer = IncrementalCollectionProducer(
        api,  # type: ignore[arg-type]
        primary_store=primary_store,
        member_inventory=lambda: (identities[key] for key in sorted(identities)),
        producer_app="fixture-target",
        adapter_id="fixture-target/v1",
        adapter_version="1.0.0",
        ingest_source="processing:fixture",
        source_event_id="many-artifact-execution",
        idempotency_key="many-artifact-execution",
    )
    local: dict[str, Path] = {}
    high_water = 0
    try:
        for index in range(25):
            artifact_id = f"{index:064x}"
            source = tmp_path / f"output-{index:04d}.bin"
            content = f"artifact-{index}".encode()
            source.write_bytes(content)
            identities[artifact_id] = ProducerArtifactIdentity(
                artifact_id, len(content), hashlib.sha256(content).hexdigest()
            )
            local[artifact_id] = source
            high_water = max(high_water, sum(item.exists() for item in local.values()))
            receipts = producer.append_inputs(
                (ProducerFile(source, artifact_id, allow_missing_materialization_hint=True),)
            )
            # The production worker advances one durable custody milestone at
            # a time. Drive it separately from the actual HTTP request path.
            for _ in range(4):
                if api.service.process_due_custody_receipts(limit=4) == 0:
                    break
            else:
                raise AssertionError("the bounded pack custody window did not drain")
            receipts = (*receipts, *producer.reconcile_custody())
            for receipt in receipts:
                owned = local.get(receipt.artifact.artifact_id)
                if owned is not None:
                    owned.unlink()
            high_water = max(high_water, sum(item.exists() for item in local.values()))
        result = producer.finish(
            poll_seconds=0.01,
            # Finalization crosses many bounded worker milestones. Allow CPU
            # contention from the full suite without changing the 25-artifact
            # custody-window proof below.
            timeout_seconds=300,
        )
        for owned in local.values():
            if owned.exists():
                owned.unlink()
    finally:
        producer.stop()

    assert result.receipt["state"] == "finalized"
    assert high_water <= 5  # four unsealed members plus the newly completed artifact
    assert not any(path.exists() for path in local.values())
    assert len([path for path in store.objects if "/volumes/" in path]) >= 7
    assert api.work_calls < len(local)


@pytest.mark.parametrize(
    "members, append_mode, concurrency",
    [
        (15, "batch", 1),
        (16, "batch", 1),
        (17, "batch", 1),
        (15, "repeated", 1),
        (16, "repeated", 1),
        (17, "repeated", 1),
        (17, "batch", 8),
    ],
)
def test_native_ingest_preserves_members_across_registration_and_payload_regimes(
    primary_store,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    members: int,
    append_mode: str,
    concurrency: int,
) -> None:
    monkeypatch.setenv("RIVERHOG_UPLOAD_FILE_CONCURRENCY", str(concurrency))
    api, store = _bounded_service_api(
        tmp_path,
        policy=CollectionVolumePolicy(
            pack_source_bytes=1024,
            pack_artifacts=4,
            pack_member_bytes=1024,
            pack_part_plaintext_bytes=65536,
            raw_volume_plaintext_bytes=131072,
            raw_part_plaintext_bytes=65536,
        ),
    )
    real_register = api.register_collection_upload_session_artifacts
    registration_sizes = []
    first_registration_sizes = []
    registered_ids = set()

    def register(collection_id, artifacts, **kwargs):
        registration_sizes.append(len(artifacts))
        new_ids = {artifact["artifact_id"] for artifact in artifacts} - registered_ids
        if new_ids:
            first_registration_sizes.append(len(new_ids))
            registered_ids.update(new_ids)
        return real_register(collection_id, artifacts, **kwargs)

    monkeypatch.setattr(api, "register_collection_upload_session_artifacts", register)
    real_status = api.get_collection_upload_session
    committed_totals = []

    def status(collection_id):
        with session_scope(api.service._session_factory) as session:
            upload = session.get(CollectionUploadRecord, collection_id)
            if upload is not None and upload.state == "finalizing":
                planned = session.scalar(
                    select(func.sum(CollectionArchiveObjectUploadRecord.plaintext_bytes)).where(
                        CollectionArchiveObjectUploadRecord.collection_id == collection_id
                    )
                )
                committed_totals.append((upload.uploaded_payload_bytes, planned))
        return real_status(collection_id)

    monkeypatch.setattr(api, "get_collection_upload_session", status)
    sizes = (0, 1023, 1024, 1025, 65535, 65536, 65537)
    inputs = []
    expected = {}
    for index in range(members):
        payload = bytes([index]) * sizes[index % len(sizes)]
        source = tmp_path / f"member-{index}.bin"
        source.write_bytes(payload)
        artifact_id = f"{index:064x}"
        inputs.append(ProducerFile(source, artifact_id, allow_missing_materialization_hint=True))
        expected[artifact_id] = (len(payload), hashlib.sha256(payload).hexdigest())
    producer = IncrementalCollectionProducer(
        api,
        primary_store=primary_store,
        member_inventory=lambda: (
            ProducerArtifactIdentity(key, value[0], value[1])
            for key, value in sorted(expected.items())
        ),
        producer_app="fixture-target",
        adapter_id="fixture-target/v1",
        adapter_version="1.0.0",
        ingest_source="fixture",
        source_event_id="regime-fixture",
    )
    try:
        if append_mode == "batch":
            producer.append_inputs(tuple(inputs))
        else:
            for source in inputs:
                producer.append_inputs((source,))
        # Original sources remain until complete archive custody is confirmed.
        assert all(source.source.exists() for source in inputs)
        result = producer.finish(poll_seconds=0.01, timeout_seconds=300)
        assert result.archive_root_sha256 is not None and result.receipt["state"] == "finalized"
        with session_scope(api.service._session_factory) as session:
            collection = session.get(CollectionRecord, producer.collection_id)
            assert (
                collection is not None
                and collection.archive_root_sha256 == result.archive_root_sha256
            )
            rows = list(
                session.scalars(
                    select(CollectionArtifactRecord).where(
                        CollectionArtifactRecord.collection_id == producer.collection_id
                    )
                )
            )
            assert {row.artifact_id: (row.bytes, row.sha256) for row in rows} == expected
        assert committed_totals and all(actual == planned for actual, planned in committed_totals)
        assert registered_ids == set(expected)
        assert all(1 <= size <= 16 for size in registration_sizes)
        expected_batches = (
            [16] * (members // 16) + ([members % 16] if members % 16 else [])
            if append_mode == "batch"
            else [1] * members
        )
        assert first_registration_sizes == expected_batches
        assert any("/volumes/" in path for path in store.objects)
    finally:
        producer.stop()
        api.close()


def test_stop_retains_owned_resources_until_the_heartbeat_worker_actually_exits():
    import threading

    producer = object.__new__(IncrementalCollectionProducer)
    producer._operation_lock = threading.RLock()
    producer._heartbeat_stop = threading.Event()
    entered_join = threading.Event()
    release_worker = threading.Event()
    exited_worker = threading.Event()
    released_source = threading.Event()
    stopped = threading.Event()

    class Source:
        def close(self):
            assert exited_worker.is_set()
            released_source.set()

    producer._sources = {"pending": Source()}
    producer._restored_sources = {}

    def worker():
        release_worker.wait()
        exited_worker.set()

    heartbeat = threading.Thread(target=worker)
    heartbeat.start()
    producer._heartbeat_thread = heartbeat
    original_join = heartbeat.join
    join_timeouts = []

    def observed_join(timeout=None):
        join_timeouts.append(timeout)
        entered_join.set()
        # A deadline that expires cannot authorize release of a live worker.
        original_join(None if timeout is None else 0)

    heartbeat.join = observed_join
    failures = []

    def stop():
        try:
            producer.stop()
        except BaseException as exc:
            failures.append(exc)
        finally:
            stopped.set()

    stopper = threading.Thread(target=stop)
    stopper.start()
    try:
        assert entered_join.wait(5)
        assert join_timeouts == [None]
        assert not released_source.is_set() and not stopped.is_set()
        assert producer._heartbeat_thread is heartbeat
    finally:
        release_worker.set()
        original_join(5)
        stopper.join(5)
    assert not heartbeat.is_alive() and not stopper.is_alive()
    assert stopped.is_set() and released_source.is_set() and not failures
    assert producer._heartbeat_thread is None and not producer._sources


def test_resume_custody_rejects_every_changed_receipt_coordinate(primary_store, tmp_path):
    from riverhog_canonical_json import canonical_json_sha256

    class ChangedReceiptApi(_CustodyApi):
        change = None

        def _receipt(self, row):
            value = super()._receipt(row)
            if value is None or self.change is None:
                return value
            if self.change == "collection":
                value["collection_id"] = "43"
            elif self.change == "bytes":
                value["bytes"] = str(int(value["bytes"]) + 1)
            elif self.change == "sha256":
                value["sha256"] = "1" * 64
            elif self.change == "completion":
                value["completion_requirement_sha256"] = "1" * 64
            elif self.change == "delivery_association_id":
                value["primary"][self.change] = "urn:uuid:ffffffff-4321-4321-8321-123456789abc"
            else:
                journal = value["primary"]["journal"]
                if self.change == "journal_id":
                    journal[self.change] = "urn:uuid:ffffffff-4321-4321-8321-123456789abc"
                elif self.change == "prefix_bytes":
                    journal[self.change] = str(int(journal[self.change]) + 1)
                elif self.change == "prefix_sha256":
                    journal[self.change] = "1" * 64
                elif self.change == "sequence":
                    journal["through"][self.change] = str(int(journal["through"][self.change]) + 1)
                elif self.change == "entry_id":
                    journal["through"][self.change] = (
                        "urn:uuid:ffffffff-4321-4321-8321-123456789abc"
                    )
                else:
                    journal["through"][self.change] = "1" * 64
            value["receipt_sha256"] = canonical_json_sha256(
                {key: item for key, item in value.items() if key != "receipt_sha256"}
            )
            # Every injected receipt remains a complete internally consistent document.
            CollectionUploadArtifactCustodyReceiptDocument.model_validate(value)
            return value

    api = ChangedReceiptApi()
    source = tmp_path / "source.bin"
    content = b"original exact receipt"
    source.write_bytes(content)
    identity = ProducerArtifactIdentity("a" * 64, len(content), hashlib.sha256(content).hexdigest())
    producer = _producer(api, primary_store)
    try:
        assert producer.append_inputs(
            (ProducerFile(source, identity.artifact_id, allow_missing_materialization_hint=True),)
        )
        member = ArtifactMemberIdentityDocument.model_validate(
            {
                "artifact_id": identity.artifact_id,
                "bytes": str(identity.bytes),
                "sha256": identity.sha256,
            }
        )
        original = primary_store.load_controls(member)
        assert original.compact
        source.unlink()
        for coordinate in (
            "collection",
            "bytes",
            "sha256",
            "completion",
            "delivery_association_id",
            "journal_id",
            "entry_id",
            "sequence",
            "json_sha256",
            "prefix_bytes",
            "prefix_sha256",
        ):
            api.change = coordinate
            with pytest.raises(ValueError):
                producer.resume_artifact_custody(identity)
            assert primary_store.load_controls(member) == original
        api.change = None
        assert (
            producer.resume_artifact_custody(identity).receipt.receipt_sha256
            == original.custody.receipt_sha256
        )
        assert len(api.journals) == 1 and not source.exists()
    finally:
        producer.stop()
