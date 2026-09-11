"""Installed-wheel proof for a generic non-Stove0 Riverhog processing application."""

from __future__ import annotations

import hashlib
import sys
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

from riverhog_client import (
    ApiClient,
    ProducerArtifactIdentity,
    ProducerStream,
    RawSourceHash,
    create_or_resume_with_initial_collection_tags,
    hash_raw_source_chunks,
)

assert not any(name.startswith("riverhog_client.transform") for name in sys.modules)

from riverhog_client.transform import (  # noqa: E402 - validates the explicit boundary
    CapabilityApiClient,
    ClaimedCollectionReader,
    ClaimedCollectionRuntimeRegistry,
    CollectionTransformRuntime,
    DerivedCollectionSpec,
)
from riverhog_protocol import (  # noqa: E402
    CollectionUploadUnitAssignmentDocument,
    CollectionUploadUnitWorkDocument,
    CollectionUploadWorkBatchDocument,
    PortableCollectionInventoryPage,
)
from riverhog_protocol.collection_workflow_transport import (  # noqa: E402
    ArtifactDispositionOutputPageDocument,
    ArtifactDispositionPageDocument,
)
from riverhog_protocol.collection_workflows import (  # noqa: E402
    ArtifactDispositionSetIdentity,
    CollectionRootIdentity,
    OperationIdentity,
    RecipeIdentity,
)

WORK_ID = "1" * 64
EXECUTION_ID = "2" * 64
CLAIM_ID = "b" * 64
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
            "id": collection_id,
            "archive_root_sha256": INPUT_ROOT.archive_root_sha256,
            "content_identity": INPUT_ROOT.content_identity,
        }

    def get_portable_collection_inventory(
        self,
        collection_id: int,
        **kwargs: Any,
    ) -> PortableCollectionInventoryPage:
        assert collection_id == 1
        assert kwargs["cursor"] is None
        return PortableCollectionInventoryPage.model_validate(
            {
                "authority": {
                    "header": {
                        "collection": 1,
                        "content_identity": INPUT_ROOT.content_identity,
                        "encryption_format": "age-v1-scrypt",
                        "passphrase_id": "external-archive-key-v1",
                        "provenance_mode": "omitted",
                    },
                    "inventory_identity": "6" * 64,
                    "file_count": 1,
                    "file_bytes": len(INPUT_CONTENT),
                },
                "files": [
                    {
                        "path": "input.bin",
                        "bytes": len(INPUT_CONTENT),
                        "sha256": INPUT_SHA256,
                    }
                ],
                "complete": True,
            }
        )

    def plan_retrieval(self, files: Sequence[tuple[int, str]], **kwargs: Any) -> dict[str, object]:
        assert files == [(1, "input.bin")]
        assert kwargs["restore_policy"] == "never"
        return {"id": "read-1", "etag": "7" * 64, "file_count": 1}

    def list_retrieval_plan_files(self, plan_id: str, **kwargs: Any) -> dict[str, object]:
        assert plan_id == "read-1"
        return {
            "plan_id": plan_id,
            "etag": kwargs["plan_etag"],
            "start_ordinal": kwargs["start_ordinal"],
            "files": [
                {
                    "collection_id": 1,
                    "path": "input.bin",
                    "bytes": len(INPUT_CONTENT),
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
    def stream_retrieval_file(
        self,
        _job_id: str,
        *,
        start: int = 0,
        end: int | None = None,
        **_kwargs: Any,
    ) -> Iterator[Iterator[bytes]]:
        yield iter((INPUT_CONTENT[start:end],))

    def acknowledge_retrieval_job(self, _job_id: str) -> dict[str, str]:
        return {"state": "completed"}

    def cancel_retrieval_job(self, _job_id: str) -> dict[str, str]:
        return {"state": "canceled"}

    def close(self) -> None:
        self.closed = True


class UploadApi:
    def __init__(self) -> None:
        self.registered: dict[str, dict[str, Any]] = {}
        self.discovery_closed = False
        self.committed = False
        self.content_identity = "8" * 64

    def get_processing_claim(self, claim_id: str) -> SimpleNamespace:
        assert claim_id == CLAIM_ID
        return SimpleNamespace(
            plan=SimpleNamespace(
                execution_id=EXECUTION_ID,
                inputs=SimpleNamespace(sha256="9" * 64),
                artifacts=SimpleNamespace(sha256="a" * 64),
            )
        )

    def list_processing_claim_dispositions(
        self, claim_id: str, *, authority_sha256: str, start_ordinal: int = 0
    ) -> ArtifactDispositionPageDocument:
        assert claim_id == CLAIM_ID and authority_sha256 == DISPOSITIONS.sha256
        return ArtifactDispositionPageDocument.model_validate(
            {
                "authority": DISPOSITIONS.as_dict(),
                "start_ordinal": start_ordinal,
                "dispositions": [
                    {
                        "input": {
                            "collection_id": 1,
                            "archive_root_sha256": INPUT_ROOT.archive_root_sha256,
                            "path": "input.bin",
                        },
                        "status": "transformed",
                    }
                ],
            }
        )

    def list_processing_claim_disposition_outputs(
        self, claim_id: str, *, authority_sha256: str, start_ordinal: int = 0
    ) -> ArtifactDispositionOutputPageDocument:
        assert claim_id == CLAIM_ID and authority_sha256 == DISPOSITIONS.sha256
        return ArtifactDispositionOutputPageDocument.model_validate(
            {
                "authority": DISPOSITIONS.as_dict(),
                "start_ordinal": start_ordinal,
                "outputs": [
                    {
                        "input": {
                            "collection_id": 1,
                            "archive_root_sha256": INPUT_ROOT.archive_root_sha256,
                            "path": "input.bin",
                        },
                        "output_path": "output.bin",
                    }
                ],
            }
        )

    def create_or_resume_collection_upload_session(
        self, *_args: Any, **_kwargs: Any
    ) -> dict[str, object]:
        return {
            "collection_id": 2,
            "resumed": False,
            "state": "open",
            "registration_constraints": {
                "pack_member_bytes": 1024,
                "raw_part_plaintext_bytes": 65536,
            },
        }

    def register_collection_upload_session_files(
        self,
        _collection_id: int,
        files: Sequence[Mapping[str, Any]],
        **_kwargs: Any,
    ) -> dict[str, object]:
        for item in files:
            self.registered[str(item["path"])] = dict(item)
        return {"state": "open", "files": [dict(item) for item in files], "volumes": []}

    def upload_collection_upload_session_provenance_journal(
        self, *_args: Any, **_kwargs: Any
    ) -> None:
        raise AssertionError("external fixture uses server-generated provenance")

    def list_collection_upload_session_files(
        self, _collection_id: int, **_kwargs: Any
    ) -> dict[str, object]:
        return {"page_size": 100, "next_page_token": None, "files": list(self.registered.values())}

    def heartbeat_collection_upload_session(self, _collection_id: int) -> dict[str, str]:
        return {"state": "open"}

    def acquire_collection_upload_session_work(
        self, collection_id: int, *, limit: int = 16
    ) -> CollectionUploadWorkBatchDocument:
        assignment = None if not self.discovery_closed or self.committed else self._assignment()
        return CollectionUploadWorkBatchDocument(
            collection_id=collection_id,
            planning_complete=self.discovery_closed,
            complete=self.discovery_closed and assignment is None,
            committed_payload_bytes=sum(int(item["bytes"]) for item in self.registered.values())
            if self.committed
            else 0,
            work=([] if assignment is None else [assignment])[:limit],
        )

    def _assignment(self) -> CollectionUploadUnitAssignmentDocument:
        sources = [
            {
                "path": item["path"],
                "offset": 0,
                "bytes": item["bytes"],
                "artifact_sha256": item["sha256"],
            }
            for item in self.registered.values()
        ]
        size = sum(int(item["bytes"]) for item in sources)
        return CollectionUploadUnitAssignmentDocument.model_validate(
            {
                "volume": {"volume_id": "pack-" + "0" * 64, "sequence": 0, "kind": "pack"},
                "plan_sha256": "c" * 64,
                "unit": {
                    "unit": 0,
                    "payload_bytes": size,
                    "plaintext_bytes": size,
                    "sources": sources,
                    "state": "pending",
                },
            }
        )

    def put_collection_upload_session_unit(
        self, *_args: Any, content: bytes, **_kwargs: Any
    ) -> CollectionUploadUnitWorkDocument:
        assert content
        self.committed = True
        unit = self._assignment().unit.model_dump(mode="json")
        return CollectionUploadUnitWorkDocument.model_validate({**unit, "state": "committed"})

    def get_collection_upload_session_unit(self, *_args: Any) -> CollectionUploadUnitWorkDocument:
        unit = self._assignment().unit.model_dump(mode="json")
        return CollectionUploadUnitWorkDocument.model_validate(
            {**unit, "state": "committed" if self.committed else "pending"}
        )

    def complete_collection_upload_session(self, _collection_id: int) -> dict[str, str]:
        self.discovery_closed = True
        return {"state": "uploading", "content_identity": self.content_identity}

    def get_collection_upload_session(self, _collection_id: int) -> dict[str, object]:
        assert self.committed
        return {
            "state": "finalized",
            "content_identity": self.content_identity,
            "collection": {
                "id": 2,
                "archive_root_sha256": "d" * 64,
                "content_identity": self.content_identity,
            },
        }

    def spawn(self) -> UploadApi:
        return self


class SettlementClient(ApiClient):
    def __init__(self) -> None:
        self.request: tuple[str, str, str, object] | None = None

    def _claim_response(
        self,
        operation_id: str,
        claim_id: str,
        suffix: str,
        request: object,
    ) -> Any:
        self.request = (operation_id, claim_id, suffix, request)
        return {"state": "settled"}


def main() -> None:
    raw = hash_raw_source_chunks(
        path="input.bin",
        chunks=(INPUT_CONTENT,),
        expected_bytes=len(INPUT_CONTENT),
        part_plaintext_bytes=65536,
    )
    assert isinstance(raw, RawSourceHash)
    assert raw.summary.sha256 == INPUT_SHA256
    assert tuple(raw.iter_batches())[0][0] == 0
    raw.close()
    staged_tags: list[str] = []
    session = create_or_resume_with_initial_collection_tags(
        ("source:external", "workflow:fixture"),
        create_or_resume=lambda first, _identity: {
            "collection_id": 2,
            "state": "open",
            "first": staged_tags.extend(first),
        },
        add_tags=lambda _collection_id, batch: staged_tags.extend(batch),
    )
    assert session["collection_id"] == 2
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
    runtime_stub = SimpleNamespace(
        refresh_capability=refreshed.append,
        close=lambda **_kwargs: None,
    )
    registry = ClaimedCollectionRuntimeRegistry()
    registry.refresh("job-1", "replacement-capability")
    with registry.bind("job-1", runtime_stub):
        pass
    assert refreshed == ["replacement-capability"]

    read_api = ReadApi()
    reader = ClaimedCollectionReader(
        read_api,
        inputs=(INPUT_ROOT,),
        work_id=WORK_ID,
        claim_id=CLAIM_ID,
        fence=1,
    )
    artifacts = tuple(reader.iter_inventory())
    assert len(artifacts) == 1
    with reader.prepare(artifacts, poll_seconds=0.01) as retrieval:
        assert retrieval.read_bytes(artifacts[0], maximum_bytes=len(INPUT_CONTENT)) == INPUT_CONTENT

    upload_api = UploadApi()
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
    identity = ProducerArtifactIdentity("output.bin", len(OUTPUT_CONTENT), OUTPUT_SHA256)
    writer.append(
        ProducerStream(
            path=identity.path,
            bytes=identity.bytes,
            sha256=identity.sha256,
            read_range=lambda offset, size: OUTPUT_CONTENT[offset : offset + size],
        ),
        identity=identity,
    )
    receipt = runtime.finish_incremental_publication(
        writer,
        execution_sha256="1" * 64,
        disposition_set=DISPOSITIONS,
        poll_seconds=0.01,
    )
    runtime.close()
    assert receipt.collection_id == 2

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
