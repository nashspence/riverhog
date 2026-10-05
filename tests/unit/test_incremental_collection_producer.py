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
            "state": "open",
            "delivery_context_id": "urn:uuid:11111111-1111-4111-8111-111111111111",
            "registration_constraints": {
                "pack_member_bytes": "1024",
                "raw_part_plaintext_bytes": "65536",
            },
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

    def get_collection_upload_session_artifact_provenance_binding(
        self, _collection_id, artifact_id
    ):
        if artifact_id not in self.bindings:
            raise NotFound("binding absent")
        return self.bindings[artifact_id]

    def set_collection_upload_session_materialization_decisions(self, *_args):
        pass

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


def _producer(api: _CustodyApi) -> IncrementalCollectionProducer:
    return IncrementalCollectionProducer(
        api,  # type: ignore[arg-type]
        producer_app="fixture-target",
        adapter_id="fixture-target/v1",
        adapter_version="1.0.0",
        ingest_source="processing:fixture",
        source_event_id="fixture-execution",
        idempotency_key="fixture-execution",
    )


def test_incremental_producer_stages_unbounded_logical_tags_in_bounded_requests() -> None:
    api = _CustodyApi()
    tags = tuple(f"classification/{index:04d}" for index in range(205))

    producer = IncrementalCollectionProducer(
        api,  # type: ignore[arg-type]
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


def test_incremental_producer_rejects_late_invalid_tags_before_remote_mutation() -> None:
    api = _CustodyApi()
    tags = [*(f"classification/{index:04d}" for index in range(150)), " invalid"]

    with pytest.raises(ValueError):
        IncrementalCollectionProducer(
            api,  # type: ignore[arg-type]
            producer_app="fixture-target",
            adapter_id="fixture-target/v1",
            adapter_version="1.0.0",
            ingest_source="processing:fixture",
            source_event_id="fixture-execution",
            tags=tags,
        )
    assert api.session_calls == 0


def test_incremental_producer_resumes_without_rereading_custodied_local_bytes(tmp_path):
    api = _CustodyApi()
    source = tmp_path / "artifact.bin"
    payload = b"completed artifact"
    source.write_bytes(payload)
    artifact_id = ArtifactId("a" * 64)
    identity = ProducerArtifactIdentity(
        artifact_id, len(payload), hashlib.sha256(payload).hexdigest()
    )
    first = _producer(api)
    try:
        receipts = first.append_inputs(
            (ProducerFile(source, artifact_id, allow_missing_materialization_hint=True),)
        )
        assert identity in {receipt.artifact for receipt in receipts}
    finally:
        first.stop()
    source.unlink()
    resumed = _producer(api)
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


def test_incremental_producer_keeps_local_custody_when_receipt_identity_is_wrong(tmp_path):
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
    producer = _producer(api)
    try:
        with pytest.raises(ValueError, match="upload member identity"):
            producer.append_inputs(
                (ProducerFile(source, "a" * 64, allow_missing_materialization_hint=True),)
            )
        assert "a" * 64 in producer._sources
        assert source.exists()
    finally:
        producer.stop()


def test_incremental_producer_keeps_equal_bytes_as_distinct_members(tmp_path):
    api = _CustodyApi()
    source = tmp_path / "source.bin"
    source.write_bytes(b"identical bytes")
    producer = _producer(api)
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


def _bounded_service_api(tmp_path: Path) -> tuple[UploadServiceApi, MemoryArchiveStore]:
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
        policy=CollectionVolumePolicy(
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
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RIVERHOG_UPLOAD_ARTIFACT_CONCURRENCY", "1")
    api, store = _bounded_service_api(tmp_path)
    producer = IncrementalCollectionProducer(
        api,  # type: ignore[arg-type]
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
            source.write_bytes(f"artifact-{index}".encode())
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
