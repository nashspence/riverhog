"""The public producer writes members and their canonical history by opaque ID."""

from __future__ import annotations

import hashlib
from typing import Any

import riverhog_client.producer as producer_module
from riverhog_client.producer import IncrementalCollectionProducer, ProducerStream
from riverhog_protocol.artifact_identity import ArtifactId
from riverhog_protocol.errors import NotFound


class _Api:
    def __init__(self) -> None:
        self.registered: list[dict[str, object]] = []
        self.journals: list[bytes] = []
        self.bindings: list[dict[str, object]] = []
        self.decisions: list[dict[str, object]] = []

    def create_or_resume_collection_upload_session(
        self, *args: object, **kwargs: object
    ) -> dict[str, Any]:
        assert "provenance_mode" not in kwargs
        return {
            "collection_id": "1",
            "state": "open",
            "delivery_context_id": "urn:uuid:11111111-1111-4111-8111-111111111111",
            "registration_constraints": {
                "pack_member_bytes": "1048576",
                "raw_part_plaintext_bytes": "65536",
            },
        }

    def add_collection_upload_session_tags(self, *args: object) -> None:
        raise AssertionError("unexpected tags")

    def register_collection_upload_session_artifacts(
        self,
        collection_id: int,
        artifacts: list[dict[str, object]],
        *,
        registration_constraints: object,
    ) -> dict[str, object]:
        self.registered.extend(artifacts)
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

    def get_collection_upload_session_artifact_provenance_binding(
        self, collection_id: int, artifact_id: ArtifactId
    ) -> object:
        raise NotFound("binding absent")

    def set_collection_upload_session_materialization_decisions(
        self, collection_id: int, batch: object
    ) -> None:
        self.decisions.extend(batch.model_dump(mode="json")["decisions"])  # type: ignore[attr-defined]

    def spawn(self) -> _Api:
        return self

    def close(self) -> None:
        pass


def test_producer_registers_pathless_member_and_exact_canonical_history(monkeypatch: Any) -> None:
    monkeypatch.setattr(producer_module, "upload_collection_units", lambda *a, **k: None)
    content = b"exact original bytes"
    artifact_id = ArtifactId("a" * 64)
    api = _Api()
    producer = IncrementalCollectionProducer(
        api,  # type: ignore[arg-type]
        producer_app="a-test-producer",
        adapter_id="test-adapter/v1",
        adapter_version="1",
        ingest_source="test",
        source_event_id="event-1",
        source_context={"source": "fixture"},
    )
    try:
        receipts = producer.append_inputs(
            (
                ProducerStream(
                    artifact_id=artifact_id,
                    bytes=len(content),
                    sha256=hashlib.sha256(content).hexdigest(),
                    read_range=lambda start, size: content[start : start + size],
                    materialization_hint=("source.txt",),
                ),
            )
        )
        assert receipts == ()
        assert api.registered[0]["artifact_id"] == artifact_id
        assert "path" not in api.registered[0]
        assert len(api.journals) == 1
        assert api.bindings[0]["artifact_id"] == artifact_id
        assert api.decisions == [
            {
                "artifact_id": artifact_id,
                "materialization_hint": {"components": ["source.txt"]},
                "allow_missing_materialization_hint": False,
            }
        ]
    finally:
        producer.stop()
