"""Installed-wheel proof for a generic non-Stove0 Riverhog processing application."""

from __future__ import annotations

import hashlib
import sys
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import riverhog_client.producer as producer_module
from riverhog_client import (
    ApiClient,
    ProducerArtifactIdentity,
    ProducerStream,
    RawSourceHash,
    create_or_resume_with_initial_collection_tags,
    hash_raw_source_chunks,
)
from riverhog_protocol import PortableCollectionInventoryPage
from riverhog_protocol.artifact_identity import ArtifactId
from riverhog_protocol.collection_workflows import (
    ArtifactDispositionSetIdentity,
    CollectionRootIdentity,
    OperationIdentity,
    RecipeIdentity,
)
from riverhog_protocol.errors import NotFound
from riverhog_protocol.output_collection_policy import OutputCollectionPolicy
from riverhog_provenance import validate_journal

assert not any(name.startswith("riverhog_client.processing") for name in sys.modules)

from riverhog_client.processing import (  # noqa: E402 - validates the explicit boundary
    CapabilityApiClient,
    ClaimedCollectionReader,
    ClaimedCollectionRuntimeRegistry,
    CollectionTransformRuntime,
    DerivedCollectionSpec,
)

WORK_ID = "1" * 64
EXECUTION_ID = "2" * 64
CLAIM_ID = "b" * 64
INPUT_ID = ArtifactId("c" * 64)
OUTPUT_ID = ArtifactId("d" * 64)
INPUT_ROOT = CollectionRootIdentity(1, "3" * 64, "4" * 64)
INPUT_CONTENT = b"external application input"
INPUT_SHA256 = hashlib.sha256(INPUT_CONTENT).hexdigest()
OUTPUT_CONTENT = b"external application output"
OUTPUT_SHA256 = hashlib.sha256(OUTPUT_CONTENT).hexdigest()
DISPOSITIONS = ArtifactDispositionSetIdentity(1, 1, 1, "5" * 64)


class ReadApi:
    def __init__(self) -> None:
        self.closed = False

    def get_collection(self, collection_id: int) -> dict[str, object]:
        assert collection_id == INPUT_ROOT.collection_id
        return {
            "id": str(collection_id),
            "archive_root_sha256": INPUT_ROOT.archive_root_sha256,
            "artifact_set_identity": INPUT_ROOT.artifact_set_identity,
        }

    def get_portable_collection_inventory(
        self, collection_id: int, **kwargs: Any
    ) -> PortableCollectionInventoryPage:
        assert collection_id == 1 and kwargs["cursor"] is None
        return PortableCollectionInventoryPage.model_validate(
            {
                "authority": {
                    "header": {
                        "collection": "1",
                        "artifact_set_identity": INPUT_ROOT.artifact_set_identity,
                        "encryption_format": "age-v1-scrypt",
                        "passphrase_id": "external-archive-key-v1",
                        "provenance_identity": "e" * 64,
                    },
                    "inventory_identity": "6" * 64,
                    "artifact_count": "1",
                    "artifact_bytes": str(len(INPUT_CONTENT)),
                },
                "artifacts": [
                    {
                        "artifact_id": INPUT_ID,
                        "bytes": str(len(INPUT_CONTENT)),
                        "sha256": INPUT_SHA256,
                    }
                ],
                "complete": True,
            }
        )

    def plan_retrieval(
        self, artifacts: Sequence[tuple[int, str]], **kwargs: Any
    ) -> dict[str, object]:
        assert artifacts == [(1, INPUT_ID)] and kwargs["restore_policy"] == "never"
        return {"id": "read-1", "etag": "7" * 64, "artifact_count": 1}

    def list_retrieval_plan_artifacts(self, plan_id: str, **kwargs: Any) -> dict[str, object]:
        assert plan_id == "read-1"
        return {
            "plan_id": plan_id,
            "etag": kwargs["plan_etag"],
            "start_ordinal": kwargs["start_ordinal"],
            "artifacts": [
                {
                    "collection_id": "1",
                    "artifact_id": INPUT_ID,
                    "bytes": str(len(INPUT_CONTENT)),
                    "sha256": INPUT_SHA256,
                }
            ],
            "complete": True,
            "next_ordinal": None,
        }

    def create_retrieval_job(self, plan_id: str, **kwargs: Any) -> dict[str, object]:
        return {
            "id": "read-job-1",
            "plan_id": plan_id,
            "plan_etag": kwargs["plan_etag"],
            "state": "ready",
        }

    @contextmanager
    def stream_retrieval_artifact(
        self, _job_id: str, *, start: int = 0, end: int | None = None, **_kwargs: Any
    ) -> Iterator[Iterator[bytes]]:
        yield iter((INPUT_CONTENT[start:end],))

    def acknowledge_retrieval_job(self, _job_id: str) -> dict[str, str]:
        return {"state": "completed"}

    def cancel_retrieval_job(self, _job_id: str) -> dict[str, str]:
        return {"state": "canceled"}

    def close(self) -> None:
        self.closed = True


class UploadApi(ReadApi):
    def __init__(self) -> None:
        super().__init__()
        self.registered: dict[str, dict[str, Any]] = {}
        self.journals: dict[str, bytes] = {}
        self.bindings: list[dict[str, object]] = []
        self.decisions: list[dict[str, object]] = []

    def get_processing_claim(self, claim_id: str) -> SimpleNamespace:
        assert claim_id == CLAIM_ID
        return SimpleNamespace(
            plan=SimpleNamespace(
                execution_id=EXECUTION_ID,
                inputs=SimpleNamespace(sha256="9" * 64),
                artifacts=SimpleNamespace(sha256="a" * 64),
                output_policy=OutputCollectionPolicy(),
            )
        )

    def create_or_resume_collection_upload_session(
        self, *_args: Any, **_kwargs: Any
    ) -> dict[str, object]:
        return {
            "collection_id": "2",
            "resumed": False,
            "state": "open",
            "delivery_context_id": "urn:uuid:11111111-1111-4111-8111-111111111111",
            "registration_constraints": {
                "pack_member_bytes": "1048576",
                "raw_part_plaintext_bytes": "65536",
            },
        }

    def register_collection_upload_session_artifacts(
        self, _collection_id: int, artifacts: Sequence[Mapping[str, Any]], **_kwargs: Any
    ) -> dict[str, object]:
        for item in artifacts:
            self.registered[str(item["artifact_id"])] = dict(item)
        return {
            "artifacts": [{**item, "custody_receipt": None} for item in artifacts],
            "volumes": [],
        }

    def upload_collection_upload_session_provenance_journal(
        self,
        _collection_id: int,
        journal_id: str,
        *,
        content: Iterator[bytes],
        byte_count: int,
        sha256: str,
    ) -> None:
        raw = b"".join(content)
        assert len(raw) == byte_count
        assert hashlib.sha256(raw).hexdigest() == sha256
        validate_journal(raw, require_profiles=False)
        self.journals[journal_id] = raw

    def get_collection_upload_session_artifact_provenance_binding(
        self, _collection_id: int, _artifact_id: ArtifactId
    ) -> object:
        raise NotFound("binding absent")

    def bind_collection_upload_session_artifact_provenance(
        self, _collection_id: int, batch: Any
    ) -> None:
        self.bindings.extend(batch.model_dump(mode="json")["bindings"])

    def set_collection_upload_session_materialization_decisions(
        self, _collection_id: int, batch: Any
    ) -> None:
        self.decisions.extend(batch.model_dump(mode="json")["decisions"])

    def heartbeat_collection_upload_session(self, _collection_id: int) -> dict[str, str]:
        return {"state": "open"}

    def complete_collection_upload_session(self, _collection_id: int) -> dict[str, object]:
        return {
            "state": "finalized",
            "artifact_set_identity": "8" * 64,
            "collection": {
                "id": "2",
                "archive_root_sha256": "f" * 64,
                "artifact_set_identity": "8" * 64,
            },
        }

    def spawn(self) -> UploadApi:
        return self


class SettlementClient(ApiClient):
    def __init__(self) -> None:
        self.request: tuple[str, str, str, object] | None = None

    def _claim_response(
        self, operation_id: str, claim_id: str, suffix: str, request: object
    ) -> Any:
        self.request = (operation_id, claim_id, suffix, request)
        return {"state": "settled"}


def main() -> None:
    raw = hash_raw_source_chunks(
        artifact_id=INPUT_ID,
        chunks=(INPUT_CONTENT,),
        expected_bytes=len(INPUT_CONTENT),
        part_plaintext_bytes=65536,
    )
    assert isinstance(raw, RawSourceHash) and raw.summary.sha256 == INPUT_SHA256
    assert tuple(raw.iter_batches())[0][0] == 0
    raw.close()

    staged_tags: list[str] = []
    create_or_resume_with_initial_collection_tags(
        ("source:external", "workflow:fixture"),
        create_or_resume=lambda first, _identity: {
            "collection_id": "2",
            "state": "open",
            "first": staged_tags.extend(first),
        },
        add_tags=lambda _collection_id, batch: staged_tags.extend(batch),
    )
    assert staged_tags == ["source:external", "workflow:fixture"]

    first = ReadApi()
    second = ReadApi()
    capability = CapabilityApiClient(first, owns_client=True)
    worker = capability.spawn()
    assert worker.current is first
    capability.replace(second, owns_client=True)
    assert worker.current is second
    capability.close()
    assert first.closed and second.closed

    refreshed: list[str] = []
    registry = ClaimedCollectionRuntimeRegistry()
    registry.refresh("job-1", "replacement-capability")
    with registry.bind(
        "job-1", SimpleNamespace(refresh_capability=refreshed.append, close=lambda: None)
    ):
        pass
    assert refreshed == ["replacement-capability"]

    read_api = ReadApi()
    reader = ClaimedCollectionReader(
        read_api, inputs=(INPUT_ROOT,), work_id=WORK_ID, claim_id=CLAIM_ID, fence=1
    )
    artifacts = tuple(reader.iter_inventory())
    assert len(artifacts) == 1 and artifacts[0].artifact_id == INPUT_ID
    with reader.prepare(artifacts, poll_seconds=0.01) as retrieval:
        assert retrieval.read_bytes(artifacts[0], maximum_bytes=len(INPUT_CONTENT)) == INPUT_CONTENT

    upload_api = UploadApi()
    producer_module.upload_collection_units = lambda *args, **kwargs: None
    spec = DerivedCollectionSpec(
        inputs=(INPUT_ROOT,),
        recipe=RecipeIdentity("external.recipe/v1", 1, "e" * 64),
        operation=OperationIdentity("external.transform/v1", "f" * 64),
    )
    runtime = CollectionTransformRuntime(
        upload_api,
        spec=spec,
        claim_id=CLAIM_ID,
        fence=1,
        work_id=WORK_ID,
        execution_id=EXECUTION_ID,
        controller_evidence={"format": "external-controller-evidence/v1"},
        producer_app="external-riverhog-client-fixture",
    )
    writer = runtime.open_incremental_publication(execution_envelope_sha256="0" * 64)
    identity = ProducerArtifactIdentity(OUTPUT_ID, len(OUTPUT_CONTENT), OUTPUT_SHA256)
    writer.append(
        ProducerStream(
            artifact_id=OUTPUT_ID,
            bytes=identity.bytes,
            sha256=identity.sha256,
            read_range=lambda offset, size: OUTPUT_CONTENT[offset : offset + size],
            materialization_hint=("output.bin",),
        ),
        identity=identity,
    )
    receipt = runtime.finish_incremental_publication(
        writer, execution_sha256="1" * 64, disposition_set=DISPOSITIONS, poll_seconds=0.01
    )
    runtime.close()
    assert receipt.collection_id == 2
    assert len(upload_api.registered) == len(upload_api.journals) == len(upload_api.bindings) == 1
    assert upload_api.decisions[0]["materialization_hint"] == {"components": ["output.bin"]}

    settlement = SettlementClient()
    assert settlement.settle_processing_claim(
        CLAIM_ID,
        fence=1,
        output_collection_id=receipt.collection_id,
        derivation=receipt.derivation.as_dict(),
    ) == {"state": "settled"}
    assert settlement.request is not None and settlement.request[:3] == (
        "settle_processing_claim",
        CLAIM_ID,
        "settle",
    )


if __name__ == "__main__":
    main()
