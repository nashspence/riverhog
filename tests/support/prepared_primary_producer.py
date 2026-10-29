"""Maintained public-producer transport fixture and prepared-storage execution witness."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from pathlib import Path
from typing import Any

from riverhog_client import PrimaryStateBusy, PrimaryStore
from riverhog_client._prepared_primary_storage import PrimaryStorage, PrimaryStorageFactory
from riverhog_protocol.artifact_identity import ArtifactId, ArtifactMemberIdentityDocument
from riverhog_protocol.errors import NotFound

from tests.support.prepared_primary_replay import ALLOCATION, LIMITS, SCOPE


class PreparedProducerApi:
    def __init__(self) -> None:
        self.create_calls = 0
        self.registered: list[dict[str, object]] = []
        self.journals: list[bytes] = []
        self.bindings: list[dict[str, object]] = []
        self.decisions: list[dict[str, object]] = []

    def create_or_resume_collection_upload_session(
        self, *args: object, **kwargs: object
    ) -> dict[str, Any]:
        self.create_calls += 1
        assert "provenance_mode" not in kwargs
        return {
            "collection_id": "1",
            "state": "open",
            "resumed": False,
            "construction_identity_sha256": "c" * 64,
            "delivery_context_id": "urn:uuid:11111111-1111-4111-8111-111111111111",
            "registration_constraints": {
                "pack_member_bytes": "1048576",
                "raw_part_plaintext_bytes": "65536",
            },
        }

    def get_collection_upload_session(self, _collection_id):
        return {
            "collection_id": "1",
            "state": "open",
            "delivery_context_id": "urn:uuid:11111111-1111-4111-8111-111111111111",
            "construction_identity_sha256": "c" * 64,
            "registration_constraints": {
                "pack_member_bytes": "1048576",
                "raw_part_plaintext_bytes": "65536",
            },
        }

    def get_collection_upload_session_artifact(self, _collection_id, artifact_id):
        row = next((row for row in self.registered if row["artifact_id"] == artifact_id), None)
        if row is None:
            raise NotFound("member absent")
        return {**row, "custody_receipt": None}

    def add_collection_upload_session_tags(self, *args: object) -> None:
        raise AssertionError("unexpected tags")

    def register_collection_upload_session_artifacts(
        self,
        collection_id: int,
        artifacts: list[dict[str, object]],
        *,
        registration_constraints: object,
    ) -> dict[str, object]:
        for item in artifacts:
            retained = next(
                (row for row in self.registered if row["artifact_id"] == item["artifact_id"]), None
            )
            if retained is None:
                self.registered.append(item)
            else:
                assert retained == item
        return {"artifacts": [{**item, "custody_receipt": None} for item in artifacts]}

    def upload_collection_upload_session_provenance_journal(
        self, collection_id: int, journal_id: str, *, content: object, byte_count: int, sha256: str
    ) -> None:
        raw = b"".join(content)  # type: ignore[arg-type]
        assert len(raw) == byte_count
        assert hashlib.sha256(raw).hexdigest() == sha256
        self.journals.append(raw)

    def bind_collection_upload_session_artifact_provenance(
        self, collection_id: int, batch: object
    ) -> None:
        self.bindings.extend(batch.model_dump(mode="json")["bindings"])  # type: ignore[attr-defined]

        return batch

    def get_collection_upload_session_artifact_provenance_binding(
        self, collection_id: int, artifact_id: ArtifactId
    ) -> object:
        raise NotFound("binding absent")

    def set_collection_upload_session_materialization_decisions(
        self, collection_id: int, batch: object
    ) -> None:
        self.decisions.extend(batch.model_dump(mode="json")["decisions"])  # type: ignore[attr-defined]

        return batch

    def spawn(self) -> PreparedProducerApi:
        return self

    def close(self) -> None:
        pass


def witness(
    path: Path,
    factory: PrimaryStorageFactory,
    prepare_storage: Callable[[PrimaryStorage], bool],
) -> dict[str, int]:
    """Run actual Native authorship and exact cold replay; transport does not transfer payload."""
    from riverhog_client import producer as owner

    api = PreparedProducerApi()
    content = b"exact original bytes"
    artifact_id = ArtifactId("a" * 64)
    member = ArtifactMemberIdentityDocument.model_validate(
        dict(
            artifact_id=artifact_id,
            bytes=str(len(content)),
            sha256=hashlib.sha256(content).hexdigest(),
        )
    )
    source = owner.ProducerStream(
        artifact_id=artifact_id,
        bytes=len(content),
        sha256=member.sha256,
        read_range=lambda offset, size: content[offset : offset + size],
        materialization_hint=("source.txt",),
    )
    phase = "root"
    counts = dict(root_calls=0, root_deferred=0, member_calls=0, member_deferred=0, observations=0)

    def prepare(storage: PrimaryStorage) -> bool:
        counts[phase + "_calls"] += 1
        done = prepare_storage(storage)
        if not done:
            counts[phase + "_deferred"] += 1
        return done

    def opened(initialize=False):
        return PrimaryStore(
            path,
            allocation_id=ALLOCATION.allocation_id,
            scope=SCOPE,
            limits=LIMITS,
            initialize=initialize,
            storage_factory=factory,
        )

    def producer(store: PrimaryStore):
        creates, observations = api.create_calls, counts["observations"]
        for _ in range(1024):
            try:
                return owner.IncrementalCollectionProducer(
                    api,
                    primary_store=store,
                    member_inventory=lambda: (),
                    storage_preparation=prepare,
                    producer_app="a-test-producer",
                    adapter_id="test-adapter/v1",
                    adapter_version="1",
                    ingest_source="test",
                    source_event_id="prepared-producer-event",
                    source_context={"source": "fixture"},
                )
            except PrimaryStateBusy:
                assert api.create_calls == creates and counts["observations"] == observations
        raise AssertionError("fixture producer session preparation did not finish")

    def append(current):
        for _ in range(1024):
            try:
                assert current.append_inputs((source,)) == ()
            except PrimaryStateBusy:
                assert not api.registered and counts["observations"] == 0
            else:
                return
        raise AssertionError("fixture producer member preparation did not finish")

    observe, upload = owner._observe_source, owner.upload_collection_units

    def observed(value):
        counts["observations"] += 1
        return observe(value)

    owner._observe_source = observed
    owner.upload_collection_units = lambda *_args, **_kwargs: None
    try:
        with opened(True) as store:
            current = producer(store)
            try:
                assert api.create_calls == 1 and counts["observations"] == 0
                phase = "member"
                append(current)
            finally:
                current.stop()
            assert counts["member_calls"] >= 1 and counts["observations"] == 1
            assert len(api.registered) == len(api.journals) == 1, dict(
                registered=len(api.registered), journals=len(api.journals), **counts
            )
            assert len(api.bindings) == len(api.decisions) == 1
            record = store.load(member)
            assert record is not None
            with store.byte_reader(record, "journal") as stream:
                original = stream.read()
            assert original == api.journals[0]
            usage = store.usage
        with opened() as store:
            phase = "root"
            current = producer(store)
            try:
                member_calls = counts["member_calls"]
                phase = "member"
                append(current)
                assert counts["member_calls"] == member_calls
            finally:
                current.stop()
            assert api.create_calls == 1 and counts["observations"] == 1
            assert api.journals == [original] and store.usage == usage
            assert store.load(member) == record
            with store.byte_reader(record, "journal") as stream:
                assert stream.read() == original
    finally:
        owner._observe_source, owner.upload_collection_units = observe, upload
    assert counts["root_deferred"] >= 1
    return dict(
        **counts,
        remote_create_calls=api.create_calls,
        cold_replay=1,
        journal_exact=1,
        usage_exact=1,
        journal_bytes=len(original),
    )
