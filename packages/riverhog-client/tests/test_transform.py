from __future__ import annotations

import hashlib
import json
import threading
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from riverhog_archive_contracts import (
    BOUND_HISTORY_EXTENT,
    ArchiveProvenanceIdentity,
    CollectionArchiveManifest,
    CollectionArtifactSetIdentity,
    MemberHistoryBuilder,
    MemberHistoryPrimary,
    MemberHistoryRoot,
    ProvenanceRootDocument,
    ProvenanceRootIdentity,
    SourceMemberHistoryBindingProof,
    binding_tree_commitment,
    provenance_structure_identity,
)
from riverhog_canonical_json import canonical_json_bytes
from riverhog_client import ApiClient
from riverhog_client.canonical_completion import CompletionRecord
from riverhog_client.canonical_production import ProducerAttribution, build_member_journal
from riverhog_client.processing import (
    ClaimedCollectionReader,
    ClaimedCollectionRuntime,
    CollectionTransformRuntime,
    DerivedCollectionReceipt,
    DerivedCollectionSpec,
    DerivedCollectionWriter,
    ProcessingWorkspace,
)
from riverhog_client.processing.writer import IncrementalDerivedCollectionWriter
from riverhog_client.producer import (
    CollectionProducer,
    ProducerArtifactIdentity,
    ProducerFile,
    ProducerStream,
)
from riverhog_protocol import (
    ArtifactId,
    ArtifactMemberIdentityDocument,
    CollectionUploadUnitAssignmentDocument,
    CollectionUploadUnitWorkDocument,
    CollectionUploadWorkBatchDocument,
    OutputCollectionPolicy,
    PortableCollectionHeader,
    PortableCollectionInventoryAuthority,
    PortableCollectionInventoryPage,
)
from riverhog_protocol.collection_completion import CollectionCompletionRecordingDocument
from riverhog_protocol.collection_workflow_transport import (
    ArtifactDispositionOutputPageDocument,
    ArtifactDispositionPageDocument,
)
from riverhog_protocol.collection_workflows import (
    ArtifactDispositionSetIdentity,
    CollectionDerivation,
    CollectionRootIdentity,
    OperationIdentity,
    RecipeIdentity,
)
from riverhog_protocol.errors import InvalidState, NotFound
from riverhog_protocol.manifest import artifact_set_identity_ordered
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    assertion,
    create_journal,
    external_reference,
    validate_journal,
)

WORK_ID = "3" * 64
EXECUTION_ID = "4" * 64
CONTROLLER_EVIDENCE = {
    "format": "stove0-controller-evidence/v1",
    "execution_id": EXECUTION_ID,
}
CONTROLLER_EVIDENCE_SHA256 = hashlib.sha256(canonical_json_bytes(CONTROLLER_EVIDENCE)).hexdigest()
INPUT_ID = ArtifactId("a" * 64)
OUTPUT_ID = ArtifactId("b" * 64)


def _portable_page(files: Sequence[Mapping[str, Any]]) -> PortableCollectionInventoryPage:
    ordered = [
        ArtifactMemberIdentityDocument(
            artifact_id=str(file["artifact_id"]),
            bytes=str(file["bytes"]),
            sha256=str(file["sha256"]),
        )
        for file in sorted(files, key=lambda item: str(item["artifact_id"]).encode("utf-8"))
    ]
    return PortableCollectionInventoryPage(
        authority=PortableCollectionInventoryAuthority(
            header=PortableCollectionHeader(
                collection="1",
                artifact_set_identity="2" * 64,
                encryption_format="age-v1-scrypt",
                passphrase_id="fixture-archive-key-v1",
                provenance_identity="c" * 64,
            ),
            inventory_identity="9" * 64,
            artifact_count=str(len(ordered)),
            artifact_bytes=str(sum(file.bytes for file in ordered)),
        ),
        artifacts=ordered,
        complete=True,
    )


def _spec() -> DerivedCollectionSpec:
    return DerivedCollectionSpec(
        recipe=RecipeIdentity("camera/v1", 1, "a" * 64),
        operation=OperationIdentity("archive-video/v1", "b" * 64),
        inputs=(CollectionRootIdentity(1, "1" * 64, "2" * 64),),
    )


def _disposition_set(
    *,
    disposition_count: int = 1,
    output_edge_count: int = 1,
    output_artifact_count: int = 1,
) -> ArtifactDispositionSetIdentity:
    return ArtifactDispositionSetIdentity(
        disposition_count=disposition_count,
        output_edge_count=output_edge_count,
        output_artifact_count=output_artifact_count,
        sha256="6" * 64,
    )


def _processing_claim() -> SimpleNamespace:
    return SimpleNamespace(
        plan=SimpleNamespace(
            execution_id=EXECUTION_ID,
            output_policy=OutputCollectionPolicy(),
            inputs=SimpleNamespace(sha256="7" * 64),
            artifacts=SimpleNamespace(sha256="8" * 64),
        )
    )


def _derivation(spec: DerivedCollectionSpec) -> CollectionDerivation:
    return CollectionDerivation(
        execution_id=EXECUTION_ID,
        claim_id="claim-1",
        fence=1,
        recipe=spec.recipe,
        operation=spec.operation,
        input_set_sha256="7" * 64,
        artifact_set_sha256="8" * 64,
        execution_envelope_sha256="c" * 64,
        execution_sha256="d" * 64,
        controller_evidence=CONTROLLER_EVIDENCE,
        controller_evidence_sha256=CONTROLLER_EVIDENCE_SHA256,
        disposition_set=_disposition_set(),
    )


class RetrievalApi:
    def __init__(self, *, changed_root: bool = False) -> None:
        self.data = b"immutable input"
        self.sha256 = hashlib.sha256(self.data).hexdigest()
        self.changed_root = changed_root
        self.acknowledged: list[str] = []
        self.canceled: list[str] = []
        self.restore_policies: list[str] = []

    def get_processing_claim(self, claim_id: str) -> SimpleNamespace:
        assert claim_id == "claim-1"
        return _processing_claim()

    def get_collection(self, collection_id: int) -> dict[str, Any]:
        return {
            "id": str(collection_id),
            "archive_root_sha256": "3" * 64 if self.changed_root else "1" * 64,
            "artifact_set_identity": "2" * 64,
        }

    def get_portable_collection_inventory(
        self, collection_id: int, **kwargs: Any
    ) -> PortableCollectionInventoryPage:
        assert collection_id == 1
        assert kwargs["cursor"] is None
        return _portable_page(
            (
                {
                    "artifact_id": INPUT_ID,
                    "bytes": str(len(self.data)),
                    "sha256": self.sha256,
                },
            )
        )

    def _rows(self, files: Sequence[tuple[int, str]]) -> list[dict[str, object]]:
        return [
            {
                "collection_id": str(collection_id),
                "artifact_id": str(path),
                "bytes": str(len(self.data)),
                "sha256": self.sha256,
            }
            for collection_id, path in files
        ]

    def plan_retrieval(
        self,
        files: Sequence[tuple[int, str]],
        **kwargs: Any,
    ) -> dict[str, Any]:
        self.restore_policies.append(str(kwargs["restore_policy"]))
        self.planned_artifacts = self._rows(files)
        return {
            "id": "plan-1",
            "etag": "9" * 64,
            "artifact_count": len(self.planned_artifacts),
        }

    def list_retrieval_plan_artifacts(
        self,
        plan_id: str,
        *,
        plan_etag: str,
        start_ordinal: int = 0,
        page_size: int = 100,
    ) -> dict[str, Any]:
        assert plan_id == "plan-1"
        assert plan_etag == "9" * 64
        assert start_ordinal == 0
        assert page_size == 100
        return {
            "plan_id": plan_id,
            "etag": plan_etag,
            "start_ordinal": start_ordinal,
            "artifacts": self.planned_artifacts,
            "complete": True,
            "next_ordinal": None,
        }

    def create_retrieval_job(
        self,
        plan_id: str,
        *,
        plan_etag: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        assert plan_id == "plan-1"
        return {
            "id": "retrieval-1",
            "plan_id": plan_id,
            "state": "ready",
            "plan_etag": plan_etag,
        }

    def get_retrieval_job(self, job_id: str) -> dict[str, Any]:
        raise AssertionError(f"unexpected retrieval poll: {job_id}")

    def renew_retrieval_job(self, job_id: str, *, lease_seconds: int) -> dict[str, Any]:
        return {"id": job_id, "state": "ready", "lease_seconds": lease_seconds}

    def acknowledge_retrieval_job(self, job_id: str) -> dict[str, Any]:
        self.acknowledged.append(job_id)
        return {"id": job_id, "state": "completed"}

    def cancel_retrieval_job(self, job_id: str) -> dict[str, Any]:
        self.canceled.append(job_id)
        return {"id": job_id, "state": "canceled"}

    def download_retrieval_artifact(
        self,
        _job_id: str,
        *,
        output: Path,
        **_kwargs: Any,
    ) -> int:
        output.write_bytes(self.data)
        return len(self.data)

    @contextmanager
    def stream_retrieval_artifact(
        self,
        _job_id: str,
        *,
        start: int = 0,
        end: int | None = None,
        **_kwargs: Any,
    ) -> Iterator[Iterator[bytes]]:
        resolved_end = len(self.data) if end is None else end
        yield iter((self.data[start:resolved_end],))


class FailingCleanupApi(RetrievalApi):
    def __init__(self, *, failures: int) -> None:
        super().__init__()
        self.failures = failures
        self.created = 0
        self.acknowledge_attempts: list[str] = []

    def create_retrieval_job(
        self,
        plan_id: str,
        *,
        plan_etag: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        self.created += 1
        return {
            "id": f"retrieval-{self.created}",
            "plan_id": plan_id,
            "state": "ready",
            "plan_etag": plan_etag,
        }

    def acknowledge_retrieval_job(self, job_id: str) -> dict[str, Any]:
        self.acknowledge_attempts.append(job_id)
        if self.failures:
            self.failures -= 1
            raise ConnectionError("fixture acknowledgement unavailable")
        return super().acknowledge_retrieval_job(job_id)


class _HeartbeatCanceled(BaseException):
    pass


class PendingPreparationApi(RetrievalApi):
    def __init__(self, *, poll_failure: bool = False, cancel_failures: int = 2) -> None:
        super().__init__()
        self.poll_failure = poll_failure
        self.cancel_failures = cancel_failures
        self.created = 0
        self.cancel_attempts: list[str] = []

    def create_retrieval_job(
        self,
        plan_id: str,
        *,
        plan_etag: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        self.created += 1
        return {
            "id": f"retrieval-{self.created}",
            "plan_id": plan_id,
            "state": "requested" if self.created == 1 else "ready",
            "plan_etag": plan_etag,
        }

    def get_retrieval_job(self, job_id: str) -> dict[str, Any]:
        assert job_id == "retrieval-1"
        if self.poll_failure:
            raise ConnectionError("fixture retrieval polling unavailable")
        return {
            "id": job_id,
            "plan_id": "plan-1",
            "state": "requested",
            "plan_etag": "9" * 64,
        }

    def cancel_retrieval_job(self, job_id: str) -> dict[str, Any]:
        self.cancel_attempts.append(job_id)
        if self.cancel_failures:
            self.cancel_failures -= 1
            raise ConnectionError("fixture retrieval cancellation unavailable")
        return {"id": job_id, "state": "canceled"}


def test_claimed_reader_verifies_roots_and_reads_exact_artifact_ranges(tmp_path: Path) -> None:
    api = RetrievalApi()
    reader = ClaimedCollectionReader(
        api,  # type: ignore[arg-type]
        inputs=_spec().inputs,
        work_id=WORK_ID,
        claim_id="claim-1",
        fence=1,
    )

    inventory = tuple(reader.iter_inventory())

    assert [item.artifact_id for item in inventory] == [INPUT_ID]
    with reader.prepare(inventory, poll_seconds=0.01) as retrieval:
        assert retrieval.read_bytes(inventory[0], maximum_bytes=1024) == api.data
        with retrieval.stream(inventory[0], start=2, end=7) as chunks:
            assert b"".join(chunks) == api.data[2:7]
        output = tmp_path / "input.mov"
        assert retrieval.download(inventory[0], output) == len(api.data)
        assert output.read_bytes() == api.data

    assert api.acknowledged == ["retrieval-1"]
    assert not api.canceled
    assert api.restore_policies == ["never"]


def test_claimed_reader_fails_closed_when_root_changed() -> None:
    reader = ClaimedCollectionReader(
        RetrievalApi(changed_root=True),  # type: ignore[arg-type]
        inputs=_spec().inputs,
        work_id=WORK_ID,
        claim_id="claim-1",
        fence=1,
    )

    with pytest.raises(RuntimeError, match="root changed"):
        tuple(reader.iter_inventory())


def test_claimed_reader_streams_multiple_inventory_pages_without_eager_surface() -> None:
    class PagedApi(RetrievalApi):
        def __init__(self) -> None:
            super().__init__()
            self.cursors: list[str | None] = []

        def get_portable_collection_inventory(
            self, collection_id: int, **kwargs: Any
        ) -> PortableCollectionInventoryPage:
            assert collection_id == 1
            cursor = kwargs["cursor"]
            self.cursors.append(cursor)
            artifact_id = INPUT_ID if cursor is None else OUTPUT_ID
            content = b"first" if cursor is None else b"other"
            return PortableCollectionInventoryPage.model_validate(
                {
                    "authority": {
                        "header": {
                            "collection": "1",
                            "artifact_set_identity": "2" * 64,
                            "encryption_format": "age-v1-scrypt",
                            "passphrase_id": "fixture-archive-key-v1",
                            "provenance_identity": "c" * 64,
                        },
                        "inventory_identity": "9" * 64,
                        "artifact_count": "2",
                        "artifact_bytes": "10",
                    },
                    "artifacts": [
                        {
                            "artifact_id": artifact_id,
                            "bytes": str(len(content)),
                            "sha256": hashlib.sha256(content).hexdigest(),
                        }
                    ],
                    "complete": cursor is not None,
                    "next_cursor": None if cursor is not None else "page-two",
                }
            )

    api = PagedApi()
    reader = ClaimedCollectionReader(
        api,  # type: ignore[arg-type]
        inputs=_spec().inputs,
        work_id=WORK_ID,
        claim_id="claim-1",
        fence=1,
    )
    iterator = reader.iter_inventory()

    assert next(iterator).artifact_id == INPUT_ID
    assert api.cursors == [None]
    assert next(iterator).artifact_id == OUTPUT_ID
    assert api.cursors == [None, "page-two"]
    with pytest.raises(StopIteration):
        next(iterator)
    assert "inventory" not in ClaimedCollectionReader.__dict__


def test_claimed_reader_releases_completed_retrieval_ownership() -> None:
    api = RetrievalApi()
    reader = ClaimedCollectionReader(
        api,  # type: ignore[arg-type]
        inputs=_spec().inputs,
        work_id=WORK_ID,
        claim_id="claim-1",
        fence=1,
    )
    artifacts = tuple(reader.iter_inventory())

    for _ in range(256):
        retrieval = reader.prepare(artifacts, poll_seconds=0.01)
        assert len(reader._retrievals) == 1
        retrieval.close()
        assert not reader._retrievals


def test_claimed_reader_cleanup_failure_blocks_admission_until_exact_retry_succeeds() -> None:
    api = FailingCleanupApi(failures=2)
    reader = ClaimedCollectionReader(
        api,  # type: ignore[arg-type]
        inputs=_spec().inputs,
        work_id=WORK_ID,
        claim_id="claim-1",
        fence=1,
    )
    artifacts = tuple(reader.iter_inventory())
    first = reader.prepare(artifacts, poll_seconds=0.01)

    with pytest.raises(ConnectionError, match="acknowledgement unavailable"):
        first.close()
    assert first.cleanup_pending
    assert len(reader._retrievals) == 1

    with pytest.raises(RuntimeError, match="prevents new admission"):
        reader.prepare(artifacts, poll_seconds=0.01)
    assert api.created == 1
    assert api.acknowledge_attempts == ["retrieval-1", "retrieval-1"]
    assert len(reader._retrievals) == 1

    second = reader.prepare(artifacts, poll_seconds=0.01)
    assert first.closed
    assert api.acknowledged == ["retrieval-1"]
    assert api.created == 2
    assert len(reader._retrievals) == 1
    second.close()
    assert not reader._retrievals


@pytest.mark.parametrize(
    ("failure_mode", "expected_failure"),
    [
        ("timeout", TimeoutError),
        ("poll", ConnectionError),
        ("heartbeat", _HeartbeatCanceled),
    ],
)
def test_claimed_reader_owns_pending_job_through_preparation_failure_and_cleanup(
    failure_mode: str,
    expected_failure: type[BaseException],
) -> None:
    api = PendingPreparationApi(poll_failure=failure_mode == "poll")
    heartbeat: Callable[[], None] | None = None
    if failure_mode == "heartbeat":

        def cancel_heartbeat() -> None:
            raise _HeartbeatCanceled("fixture claim was canceled")

        heartbeat = cancel_heartbeat
    reader = ClaimedCollectionReader(
        api,  # type: ignore[arg-type]
        inputs=_spec().inputs,
        work_id=WORK_ID,
        claim_id="claim-1",
        fence=1,
        heartbeat=heartbeat,
    )
    artifacts = tuple(reader.iter_inventory())
    timeout_seconds = 0.0 if failure_mode == "timeout" else 10.0

    with pytest.raises(expected_failure):
        reader.prepare(
            artifacts,
            poll_seconds=0.001,
            timeout_seconds=timeout_seconds,
        )

    assert api.created == 1
    assert api.cancel_attempts == ["retrieval-1"]
    assert set(reader._retrievals) == {"retrieval-1"}
    pending = reader._retrievals["retrieval-1"]
    assert pending.cleanup_pending
    assert not pending.closed

    with pytest.raises(RuntimeError, match="prevents new admission"):
        reader.prepare(artifacts, poll_seconds=0.001)
    assert api.created == 1
    assert api.cancel_attempts == ["retrieval-1", "retrieval-1"]

    ready = reader.prepare(artifacts, poll_seconds=0.001)
    assert pending.closed
    assert api.cancel_attempts == ["retrieval-1", "retrieval-1", "retrieval-1"]
    assert api.created == 2
    assert set(reader._retrievals) == {"retrieval-2"}
    ready.close()
    assert not reader._retrievals


@pytest.mark.parametrize("runtime_type", [ClaimedCollectionRuntime, CollectionTransformRuntime])
def test_maintained_client_runtimes_release_completed_retrievals_promptly(
    runtime_type: type[Any],
) -> None:
    api = RetrievalApi()
    common = {
        "claim_id": "claim-1",
        "fence": 1,
        "work_id": WORK_ID,
        "execution_id": EXECUTION_ID,
    }
    runtime = (
        ClaimedCollectionRuntime(api, inputs=_spec().inputs, **common)
        if runtime_type is ClaimedCollectionRuntime
        else CollectionTransformRuntime(
            api,
            spec=_spec(),
            controller_evidence=CONTROLLER_EVIDENCE,
            producer_app="fixture-transform",
            **common,
        )
    )
    artifacts = tuple(runtime.iter_inventory())

    for _ in range(64):
        with runtime.prepare_inputs(artifacts, poll_seconds=0.01):
            assert len(runtime.reader._retrievals) == 1
        assert not runtime.reader._retrievals
    with pytest.raises(RuntimeError, match="fixture body failed"):
        with runtime.prepare_inputs(artifacts, poll_seconds=0.01):
            raise RuntimeError("fixture body failed")
    assert not runtime.reader._retrievals
    assert api.canceled == ["retrieval-1"]
    runtime.close()
    runtime.close()


class UploadApi:
    def __init__(self) -> None:
        self.registered: list[dict[str, Any]] = []
        self.registration_batches: list[list[dict[str, Any]]] = []
        self.uploaded = b""
        self.completion_artifact_set_identity = ""
        self.committed = False
        self.discovery_closed = False
        self.work_calls = 0
        self.session_calls = 0
        self.derivation_identity = _disposition_set()
        self.journals: dict[str, bytes] = {}
        self.bindings: dict[str, Any] = {}
        self.materialization_decisions: dict[str, Any] = {}
        self.structures: dict[str, bytes] = {}
        self.history_inputs: dict[str, Any] = {}
        self.requirement: Any = None
        self.recording: Any = None
        self.dispositions: list[dict[str, Any]] | None = None
        self.output_edges: list[dict[str, Any]] | None = None

    def get_processing_claim(self, claim_id: str) -> SimpleNamespace:
        assert claim_id == "claim-1"
        return _processing_claim()

    def list_processing_claim_dispositions(
        self,
        claim_id: str,
        *,
        identity_sha256: str,
        start_ordinal: int = 0,
    ) -> ArtifactDispositionPageDocument:
        assert claim_id == "claim-1"
        identity = self.derivation_identity
        assert identity_sha256 == identity.sha256 and start_ordinal == 0
        return ArtifactDispositionPageDocument.model_validate(
            {
                "identity": identity.as_dict(),
                "start_ordinal": "0",
                "dispositions": self.dispositions
                if self.dispositions is not None
                else [
                    {
                        "input": {
                            "collection_id": "1",
                            "archive_root_sha256": "1" * 64,
                            "artifact_id": f"{index:064x}",
                        },
                        "status": "transformed",
                    }
                    for index in range(identity.disposition_count)
                ],
            }
        )

    def list_processing_claim_disposition_outputs(
        self,
        claim_id: str,
        *,
        identity_sha256: str,
        start_ordinal: int = 0,
    ) -> ArtifactDispositionOutputPageDocument:
        assert claim_id == "claim-1"
        identity = self.derivation_identity
        assert identity_sha256 == identity.sha256 and start_ordinal == 0
        return ArtifactDispositionOutputPageDocument.model_validate(
            {
                "identity": identity.as_dict(),
                "start_ordinal": "0",
                "outputs": self.output_edges
                if self.output_edges is not None
                else [
                    {
                        "input": {
                            "collection_id": "1",
                            "archive_root_sha256": "1" * 64,
                            "artifact_id": f"{index:064x}",
                        },
                        "output_artifact_id": f"{index + 100:064x}",
                    }
                    for index in range(identity.output_edge_count)
                ],
            }
        )

    def create_or_resume_collection_upload_session(
        self,
        *_args: Any,
        **_kwargs: Any,
    ) -> dict[str, Any]:
        self.session_calls += 1
        return {
            "collection_id": "7",
            "resumed": self.session_calls > 1,
            "state": "open",
            "delivery_context_id": "urn:uuid:11111111-1111-4111-8111-111111111111",
            "construction_identity_sha256": "c" * 64,
            "registration_constraints": {
                "pack_member_bytes": "1024",
                "raw_part_plaintext_bytes": "65536",
            },
        }

    def register_collection_upload_session_artifacts(
        self,
        _collection_id: int,
        files: Sequence[Mapping[str, Any]],
        *,
        registration_constraints: object,
    ) -> dict[str, Any]:
        assert registration_constraints.raw_part_plaintext_bytes == 65536
        batch = [dict(item) for item in files]
        self.registration_batches.append(batch)
        existing = {str(item["artifact_id"]): item for item in self.registered}
        for item in batch:
            prior = existing.get(str(item["artifact_id"]))
            if prior is not None:
                assert prior == item
                continue
            self.registered.append(item)
        by_path = {str(item["artifact_id"]): item for item in self.registered}
        return {
            "state": "open",
            "artifacts": [dict(by_path[str(item["artifact_id"])]) for item in batch],
            "volumes": [],
        }

    def list_collection_upload_session_artifacts(
        self,
        _collection_id: int,
        **_kwargs: Any,
    ) -> dict[str, Any]:
        return {
            "page_size": 100,
            "next_page_token": None,
            "artifacts": [dict(item) for item in self.registered],
        }

    def heartbeat_collection_upload_session(self, _collection_id: int) -> dict[str, Any]:
        return {"state": "open"}

    def acquire_collection_upload_session_work(
        self,
        collection_id: int,
        *,
        limit: int = 16,
    ) -> CollectionUploadWorkBatchDocument:
        self.work_calls += 1
        assignment = self._assignment()
        work = [] if assignment is None else [assignment]
        return CollectionUploadWorkBatchDocument(
            collection_id=str(collection_id),
            planning_complete=self.discovery_closed,
            complete=self.discovery_closed and not work,
            committed_payload_bytes=str(len(self.uploaded)),
            work=work[:limit],
        )

    def _assignment(self) -> CollectionUploadUnitAssignmentDocument | None:
        if not self.discovery_closed or self.committed:
            return None
        sources = [
            {
                "artifact_id": item["artifact_id"],
                "offset": "0",
                "bytes": item["bytes"],
                "artifact_sha256": item["sha256"],
            }
            for item in self.registered
        ]
        total_bytes = sum(int(item["bytes"]) for item in sources)
        return CollectionUploadUnitAssignmentDocument.model_validate(
            {
                "volume": {
                    "volume_id": "pack-" + "0" * 64,
                    "sequence": "0" * 64,
                    "kind": "pack",
                },
                "plan_sha256": "8" * 64,
                "unit": {
                    "unit": "0",
                    "payload_bytes": str(total_bytes),
                    "plaintext_bytes": str(total_bytes),
                    "sources": sources,
                    "state": "pending",
                },
            }
        )

    def put_collection_upload_session_unit(
        self,
        _collection_id: int,
        _volume_id: str,
        _unit: int,
        *,
        content: bytes,
        **_kwargs: Any,
    ) -> CollectionUploadUnitWorkDocument:
        self.uploaded = content
        self.committed = True
        return CollectionUploadUnitWorkDocument.model_validate(
            {
                **self._unit_payload(),
                "state": "committed",
            }
        )

    def get_collection_upload_session_unit(
        self,
        collection_id: int,
        _volume_id: str,
        _unit: int,
    ) -> CollectionUploadUnitWorkDocument:
        del collection_id
        return CollectionUploadUnitWorkDocument.model_validate(
            {
                **self._unit_payload(),
                "state": "committed" if self.committed else "pending",
            }
        )

    def _unit_payload(self) -> dict[str, object]:
        sources = [
            {
                "artifact_id": item["artifact_id"],
                "offset": "0",
                "bytes": item["bytes"],
                "artifact_sha256": item["sha256"],
            }
            for item in self.registered
        ]
        total_bytes = sum(int(item["bytes"]) for item in sources)
        return {
            "unit": "0",
            "payload_bytes": str(total_bytes),
            "plaintext_bytes": str(total_bytes),
            "sources": sources,
        }

    def complete_collection_upload_session(
        self,
        _collection_id: int,
    ) -> dict[str, Any]:
        ordered = sorted(
            self.registered,
            key=lambda item: str(str(item["artifact_id"])),
        )
        artifact_set_identity = artifact_set_identity_ordered(
            ArtifactMemberIdentityDocument.model_validate(item) for item in ordered
        )
        self.completion_artifact_set_identity = artifact_set_identity
        self.discovery_closed = True
        return {
            "state": "uploading",
            "artifact_set_identity": artifact_set_identity,
        }

    def get_collection_upload_session(self, _collection_id: int) -> dict[str, Any]:
        if not self.discovery_closed:
            return {"state": "open", "construction_identity_sha256": "c" * 64}
        assert self.committed
        return {
            "state": "finalized",
            "artifact_set_identity": self.completion_artifact_set_identity,
            "collection": {
                "id": "7",
                "archive_root_sha256": "7" * 64,
                "artifact_set_identity": self.completion_artifact_set_identity,
            },
        }

    def upload_collection_upload_session_provenance_journal(
        self,
        _collection_id: int,
        journal_id: str,
        *,
        content: Iterable[bytes],
        byte_count: int,
        sha256: str,
        **_kwargs: Any,
    ) -> None:
        raw = b"".join(content)
        assert (len(raw), hashlib.sha256(raw).hexdigest()) == (byte_count, sha256)
        assert validate_journal(raw, require_profiles=False).journal_id == journal_id
        assert self.journals.setdefault(journal_id, raw) == raw

    def bind_collection_upload_session_artifact_provenance(
        self, _collection_id: int, batch: Any
    ) -> None:
        for binding in batch.bindings:
            assert self.bindings.setdefault(binding.artifact_id, binding) == binding

    def get_collection_upload_session_artifact_provenance_binding(
        self,
        _collection_id: int,
        artifact_id: str,
    ) -> Any:
        try:
            return self.bindings[artifact_id]
        except KeyError as exc:
            raise NotFound("primary binding absent") from exc

    def set_collection_upload_session_materialization_decisions(
        self, _collection_id: int, batch: Any
    ) -> None:
        for decision in batch.decisions:
            assert (
                self.materialization_decisions.setdefault(decision.artifact_id, decision)
                == decision
            )

    def stage_collection_upload_session_history_structure(
        self, _collection_id: int, raw: bytes
    ) -> None:
        identity = provenance_structure_identity(raw).object_id
        assert self.structures.setdefault(identity, raw) == raw

    def set_collection_upload_session_member_history_inputs(
        self, _collection_id: int, artifact_id: str, inputs: Any
    ) -> None:
        assert self.history_inputs.setdefault(artifact_id, inputs) == inputs

    def get_collection_upload_session_member_history_inputs(
        self, _collection_id: int, artifact_id: str
    ) -> Any:
        from riverhog_protocol.provenance_transport import ArchiveRecordSetReferenceDocument

        return ArchiveRecordSetReferenceDocument.model_validate(
            self.history_inputs[artifact_id].to_mapping()
        )

    def set_collection_upload_session_completion_requirement(
        self, _collection_id: int, requirement: Any
    ) -> None:
        if self.requirement is not None:
            assert self.requirement == requirement
        self.requirement = requirement

    def get_collection_upload_session_provenance_journal(
        self, _collection_id: int, journal_id: str
    ) -> Any:
        if journal_id not in self.journals:
            raise NotFound("staged journal absent")
        raw = self.journals[journal_id]
        return SimpleNamespace(
            state="sealed", bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest()
        )

    @contextmanager
    def stream_collection_upload_session_provenance_journal(
        self, _collection_id: int, journal_id: str
    ) -> Iterator[Iterator[bytes]]:
        yield iter((self.journals[journal_id],))

    def reserve_collection_upload_session_completion_recording(
        self, _collection_id: int, request: Any
    ) -> Any:
        value = CollectionCompletionRecordingDocument(
            **request.model_dump(),
            journal_id="urn:uuid:99999999-9999-4999-8999-999999999999",
            recorded_at="2026-10-01T00:00:00.000000000Z",
        )
        if self.recording is not None:
            assert self.recording == value
        self.recording = value
        return value

    def spawn(self) -> UploadApi:
        return self

    def close(self) -> None:
        pass


class InputHistoryApi:
    """One source archive with an exact primary and explicitly bound late claim."""

    def __init__(self, collection_id: int, artifact_id: ArtifactId, content: bytes) -> None:
        self.collection_id = collection_id
        self.member = ArtifactMemberIdentityDocument(
            artifact_id=artifact_id,
            bytes=str(len(content)),
            sha256=hashlib.sha256(content).hexdigest(),
        )
        produced = build_member_journal(
            member=self.member,
            observation=BoundedSourceObserver().observe(BytesSource(content)),
            delivery_context_id="urn:uuid:22222222-2222-4222-8222-222222222222",
            attribution=ProducerAttribution(
                "fixture", "fixture/v1", "1", "event", "fixture", {}, "f" * 64
            ),
            materialization_hint=None,
        )
        self.primary = produced.binding
        self.summary = validate_journal(produced.content, require_profiles=False)
        who = self.summary.graph["agents"][0]["id"]
        late = create_journal(
            {
                "agents": self.summary.graph["agents"],
                "extensions": [
                    assertion(
                        "extension",
                        who,
                        subject=external_reference(self.summary, self.summary.states[0]["id"]),
                        property="https://fixture.invalid/late-record",
                        value={"type": "text", "value": "retained late evidence"},
                    )
                ],
            },
            recorded_by_agent_id=who,
        )
        late_summary = validate_journal(late, require_profiles=False)
        self.late_journal_id = late_summary.journal_id
        self.journals = {produced.journal_id: produced.content, late_summary.journal_id: late}
        with MemberHistoryBuilder(
            artifact_id=artifact_id,
            bytes=len(content),
            sha256=self.member.sha256,
            primary=MemberHistoryPrimary.from_mapping(
                {
                    "journal": self.primary.journal.model_dump(mode="json"),
                    "delivery_association_id": self.primary.delivery_association_id,
                }
            ),
        ) as builder:
            builder.add_root(
                MemberHistoryRoot.from_mapping(
                    {"journal": late_summary.anchor, "inclusion": "bound"}
                )
            )
            self.history_binding, self.history = builder.seal()
            self.objects = {
                provenance_structure_identity(raw).object_id: raw for raw in builder.objects()
            }
        identity = artifact_set_identity_ordered((self.member,))
        provenance = ProvenanceRootDocument(
            archive_generation="1" * 64,
            artifact_set_sha256=identity,
            delivery_context_id="urn:uuid:22222222-2222-4222-8222-222222222222",
            binding_count=1,
            binding_tree_sha256=binding_tree_commitment((self.history_binding,)).root_sha256,
            journal_count=2,
            ordered_volume_sha256="e" * 64,
        )
        archive = CollectionArchiveManifest(
            archive_generation=provenance.archive_generation,
            artifact_set=CollectionArtifactSetIdentity(1, len(content), identity),
            ordered_volume_sha256="f" * 64,
            provenance=ArchiveProvenanceIdentity(
                provenance.identity,
                ProvenanceRootIdentity(
                    id="provenance-root",
                    kind="provenance-root",
                    path="provenance/root.json.age",
                    plaintext_bytes=len(provenance.to_json_bytes()),
                    sha256=provenance.identity,
                    stored_bytes=1234,
                    stored_sha256="0" * 64,
                ),
            ),
        )
        self.proof = SourceMemberHistoryBindingProof(
            "a" * 64,
            collection_id,
            archive.to_json_bytes(),
            provenance.to_json_bytes(),
            self.history_binding,
            0,
            (),
        )
        self.root = CollectionRootIdentity(
            collection_id, hashlib.sha256(archive.to_json_bytes()).hexdigest(), identity
        )

    def get_collection(self, collection_id: int) -> dict[str, Any]:
        assert collection_id == self.collection_id
        return {
            "id": str(collection_id),
            "archive_root_sha256": self.root.archive_root_sha256,
            "artifact_set_identity": self.root.artifact_set_identity,
        }

    def get_collection_artifact_provenance(
        self, collection_id: int, artifact_id: ArtifactId
    ) -> dict[str, Any]:
        assert collection_id == self.collection_id and artifact_id == self.member.artifact_id
        return {
            "collection_id": str(collection_id),
            "archive_root_sha256": self.root.archive_root_sha256,
            "artifact": self.member.model_dump(mode="json"),
            "binding": self.primary.model_dump(mode="json"),
            "history_binding": self.history_binding.to_mapping(),
            "member_history": self.history.to_mapping(),
        }

    def get_collection_provenance_structure(
        self, collection_id: int, object_id: str, *, archive_root_sha256: str
    ) -> bytes:
        assert (
            collection_id == self.collection_id
            and archive_root_sha256 == self.root.archive_root_sha256
        )
        return self.objects[object_id]

    def get_collection_artifact_history_binding_proof(
        self, collection_id: int, artifact_id: ArtifactId, *, archive_root_sha256: str
    ) -> SourceMemberHistoryBindingProof:
        assert collection_id == self.collection_id and artifact_id == self.member.artifact_id
        assert archive_root_sha256 == self.root.archive_root_sha256
        return self.proof

    def list_collection_provenance_journals(
        self, collection_id: int, **kwargs: Any
    ) -> dict[str, Any]:
        assert collection_id == self.collection_id and kwargs["after_journal_id"] is None
        return {
            "collection_id": str(collection_id),
            "archive_root_sha256": self.root.archive_root_sha256,
            "journals": [
                {
                    "journal_id": key,
                    "bytes": str(len(raw)),
                    "sha256": hashlib.sha256(raw).hexdigest(),
                }
                for key, raw in sorted(self.journals.items())
            ],
            "next_journal_id": None,
        }

    @contextmanager
    def stream_collection_provenance_journal(
        self,
        collection_id: int,
        journal_id: str,
        *,
        expected_bytes: int,
        expected_sha256: str,
        end: int | None,
    ) -> Iterator[Iterator[bytes]]:
        assert collection_id == self.collection_id
        raw = self.journals[journal_id]
        assert (len(raw), hashlib.sha256(raw).hexdigest()) == (expected_bytes, expected_sha256)
        yield iter((raw[:end],))

    def accepted(self) -> Any:
        from riverhog_client.processing import ClaimedArtifact

        reader = ClaimedCollectionReader(
            self, inputs=(self.root,), work_id=WORK_ID, claim_id="claim-1", fence=1
        )
        return reader.provenance(
            ClaimedArtifact(
                self.root, self.member.artifact_id, self.member.bytes, self.member.sha256
            )
        )


def _derived_stream(index: int, content: bytes) -> ProducerStream:
    return ProducerStream(
        artifact_id=ArtifactId(f"{index + 100:064x}"),
        bytes=len(content),
        sha256=hashlib.sha256(content).hexdigest(),
        output_id=f"output-{index}",
        read_range=lambda offset, size: content[offset : offset + size],
        allow_missing_materialization_hint=True,
    )


def _completion_records() -> tuple[CompletionRecord, ...]:
    # Sealed runtime bytes retain null and numeric tokens exactly.
    raw = b'{"optional":null,"quality":1.2300}'
    return tuple(
        CompletionRecord(kind, len(raw), hashlib.sha256(raw).hexdigest(), lambda: (raw,))
        for kind in (
            "implementation",
            "invocation",
            "target-execution",
            "target-output-declarations",
            "target-result",
        )
    )


def _incremental_writer(
    api: UploadApi, inputs: Sequence[InputHistoryApi]
) -> IncrementalDerivedCollectionWriter:
    return IncrementalDerivedCollectionWriter(
        api,
        spec=DerivedCollectionSpec(
            inputs=tuple(value.root for value in inputs),
            recipe=_spec().recipe,
            operation=_spec().operation,
        ),
        claim_id="claim-1",
        fence=1,
        work_id=WORK_ID,
        execution_id=EXECUTION_ID,
        controller_evidence=CONTROLLER_EVIDENCE,
        producer_app="fixture-transform",
        producer_version="1",
        execution_envelope_sha256="c" * 64,
    )


def test_producer_rejects_empty_iterable_before_opening_construction() -> None:
    api = UploadApi()

    with pytest.raises(ValueError, match="at least one source file"):
        CollectionProducer(
            api,  # type: ignore[arg-type]
            producer_app="fixture-transform",
            adapter_id="test-transform/v1",
            adapter_version="1",
            ingest_source="processing:test",
        ).publish_inputs(iter(()), source_event_id="empty")

    assert api.session_calls == 0


def test_producer_stream_has_no_shared_filesystem_and_is_snapshot_verified(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RIVERHOG_UPLOAD_FILE_CONCURRENCY", "1")
    content = b"generated output"
    api = UploadApi()
    stream = ProducerStream(
        artifact_id=OUTPUT_ID,
        allow_missing_materialization_hint=True,
        bytes=len(content),
        sha256=hashlib.sha256(content).hexdigest(),
        read_range=lambda offset, size: content[offset : offset + size],
    )

    receipt = CollectionProducer(
        api,  # type: ignore[arg-type]
        producer_app="stove0-worker",
        adapter_id="test-transform/v1",
        adapter_version="1",
        ingest_source="processing:test",
    ).publish_inputs((stream,), source_event_id="event-1")

    assert receipt.collection_id == 7
    assert api.work_calls >= 2
    assert api.completion_artifact_set_identity == receipt.artifact_set_identity
    uploaded_by_path = {
        str(item["artifact_id"]): api.uploaded[
            sum(int(previous["bytes"]) for previous in api.registered[:index]) : sum(
                int(previous["bytes"]) for previous in api.registered[: index + 1]
            )
        ]
        for index, item in enumerate(api.registered)
    }
    assert uploaded_by_path[OUTPUT_ID] == content


def test_producer_streams_bounded_batches_without_limiting_collection_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RIVERHOG_UPLOAD_FILE_CONCURRENCY", "1")
    api = UploadApi()

    def streams() -> Iterator[ProducerStream]:
        for index in range(128):
            if index == 16:
                assert api.registered
            value = bytes([index % 251])
            yield ProducerStream(
                artifact_id=ArtifactId(f"{index:064x}"),
                allow_missing_materialization_hint=True,
                bytes=1,
                sha256=hashlib.sha256(value).hexdigest(),
                read_range=lambda offset, size, content=value: content[offset : offset + size],
            )

    receipt = CollectionProducer(
        api,  # type: ignore[arg-type]
        producer_app="stove0-worker",
        adapter_id="test-transform/v1",
        adapter_version="1",
        ingest_source="processing:test",
    ).publish_inputs(streams(), source_event_id="event-many")

    assert receipt.collection_id == 7
    assert all(1 <= len(batch) <= 16 for batch in api.registration_batches)
    assert len(api.registered) == 128


def test_producer_streams_exact_observation_into_primary_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RIVERHOG_UPLOAD_FILE_CONCURRENCY", "1")
    content = b"generated output"
    observed = BoundedSourceObserver().observe(BytesSource(content))
    api = UploadApi()
    CollectionProducer(
        api,
        producer_app="fixture-transform",
        adapter_id="test-transform/v1",
        adapter_version="1",
        ingest_source="processing:test",
    ).publish_inputs(
        (
            ProducerStream(
                artifact_id=OUTPUT_ID,
                bytes=len(content),
                sha256=hashlib.sha256(content).hexdigest(),
                read_range=lambda offset, size: content[offset : offset + size],
                observation=observed,
                allow_missing_materialization_hint=True,
            ),
        ),
        source_event_id="event-1",
    )
    binding = api.bindings[OUTPUT_ID]
    summary = validate_journal(api.journals[binding.journal.journal_id], require_profiles=False)
    assert summary.anchor == binding.journal.model_dump(mode="json")
    assert any(row["id"] == observed.state_id for row in summary.states)
    assert len(api.registered) == 1
    assert api.registered[0]["artifact_id"] == OUTPUT_ID
    assert "path" not in api.registered[0]


def test_transform_retains_late_input_history_across_fanout_fanin_and_lost_response() -> None:
    from riverhog_canonical_json import canonical_json_sha256
    from riverhog_protocol.collection_completion_validation import validate_disposition_record_pages
    from riverhog_protocol.collection_record_preimages import CollectionRecordPreimages
    from riverhog_provenance import reference

    sources = (
        InputHistoryApi(1, INPUT_ID, b"source a"),
        InputHistoryApi(2, OUTPUT_ID, b"source b"),
    )

    class LostRegistrationApi(UploadApi):
        failures = 1

        def register_collection_upload_session_artifacts(
            self, *args: Any, **kwargs: Any
        ) -> dict[str, Any]:
            if self.failures:
                self.failures -= 1
                raise ConnectionError("lost registration response")
            return super().register_collection_upload_session_artifacts(*args, **kwargs)

    api = LostRegistrationApi()
    outputs = tuple(
        _derived_stream(i, content) for i, content in enumerate((b"a one", b"a two", b"joined"))
    )
    accepted = tuple(value.accepted() for value in sources)
    selected = ((accepted[0],), (accepted[0],), accepted)
    api.dispositions = [
        {
            "input": {
                "collection_id": str(value.root.collection_id),
                "archive_root_sha256": value.root.archive_root_sha256,
                "artifact_id": str(value.member.artifact_id),
            },
            "status": "transformed",
        }
        for value in sources
    ]
    api.output_edges = [
        {
            "input": {
                "collection_id": str(value.artifact.root.collection_id),
                "archive_root_sha256": value.artifact.root.archive_root_sha256,
                "artifact_id": str(value.artifact.artifact_id),
            },
            "output_artifact_id": str(output.artifact_id),
        }
        for output, histories in zip(outputs, selected, strict=True)
        for value in histories
    ]
    disposition = ArtifactDispositionSetIdentity(
        2,
        4,
        3,
        canonical_json_sha256(
            {
                "format": "riverhog-artifact-disposition-set/v1",
                "disposition_count": "2",
                "dispositions_sha256": hashlib.sha256(
                    b"".join(canonical_json_bytes(row) + b"\n" for row in api.dispositions)
                ).hexdigest(),
                "output_edge_count": "4",
                "output_artifact_count": "3",
                "outputs_sha256": hashlib.sha256(
                    b"".join(canonical_json_bytes(row) + b"\n" for row in api.output_edges)
                ).hexdigest(),
            }
        ),
    )
    api.derivation_identity = disposition
    first = _incremental_writer(api, sources)
    try:
        with pytest.raises(ConnectionError, match="lost registration"):
            first.append(
                outputs[0],
                identity=ProducerArtifactIdentity(
                    outputs[0].artifact_id, outputs[0].bytes, outputs[0].sha256
                ),
                output_id=outputs[0].output_id,
                source_histories=selected[0],
                history_extent=BOUND_HISTORY_EXTENT,
            )
    finally:
        first.stop()
    copied = dict(api.journals)
    assert sources[0].late_journal_id in copied
    resumed = _incremental_writer(api, sources)
    try:
        for output, histories in zip(outputs, selected, strict=True):
            resumed.append(
                output,
                identity=ProducerArtifactIdentity(output.artifact_id, output.bytes, output.sha256),
                output_id=output.output_id,
                source_histories=histories,
                history_extent=BOUND_HISTORY_EXTENT,
            )
        records = _completion_records()
        receipt = resumed.finish(
            execution_sha256=records[2].sha256,
            disposition_set=disposition,
            completion_records=records,
            poll_seconds=0.01,
        )
    finally:
        resumed.stop()
    assert receipt.collection_id == 7
    assert all(api.journals[key] == raw for key, raw in copied.items())
    assert all(value.late_journal_id in api.journals for value in sources)
    assert len(api.registered) == 3
    assert [api.history_inputs[value.artifact_id].record_count for value in outputs] == [1, 1, 2]
    completion = validate_journal(api.journals[api.recording.journal_id], require_profiles=False)
    subject = reference(completion.graph["activities"][0]["id"], "activity")
    with CollectionRecordPreimages(completion.graph["extensions"], subject=subject) as retained:
        retained.validate(expected_kinds=api.requirement.record_kinds)
        validate_disposition_record_pages(
            retained.chunks("disposition-pages"), identity=disposition
        )
        assert (
            b"".join(retained.chunks("target-execution")) == b'{"optional":null,"quality":1.2300}'
        )
    assert completion.graph["activities"][0]["kind"] == "recording"
    assert not completion.graph.get("relations")


def test_incremental_transform_recovers_accepted_primary_after_lost_binding_response() -> None:
    source = InputHistoryApi(1, INPUT_ID, b"source")
    accepted = source.accepted()
    output = _derived_stream(0, b"derived")
    identity = ProducerArtifactIdentity(output.artifact_id, output.bytes, output.sha256)

    class LostBindingApi(UploadApi):
        failures = 1

        def bind_collection_upload_session_artifact_provenance(self, *args: Any) -> None:
            super().bind_collection_upload_session_artifact_provenance(*args)
            if self.failures:
                self.failures -= 1
                raise ConnectionError("lost accepted primary response")

    api = LostBindingApi()
    first = _incremental_writer(api, (source,))
    try:
        with pytest.raises(ConnectionError, match="lost accepted primary"):
            first.append(
                output,
                identity=identity,
                output_id=output.output_id,
                source_histories=(accepted,),
                history_extent=BOUND_HISTORY_EXTENT,
            )
    finally:
        first.stop()
    primary = api.bindings[output.artifact_id]
    exact = api.journals[primary.journal.journal_id]
    resumed = _incremental_writer(api, (source,))
    try:
        resumed.append(
            output,
            identity=identity,
            output_id=output.output_id,
            source_histories=(accepted,),
            history_extent=BOUND_HISTORY_EXTENT,
        )
    finally:
        resumed.stop()
    assert api.bindings[output.artifact_id] == primary
    assert api.journals[primary.journal.journal_id] == exact
    assert len(api.registered) == 1
    assert len(api.journals) == 3


@pytest.mark.parametrize("extent", ["unknown", ""])
def test_incremental_transform_rejects_unaccepted_history_extent_before_output_registration(
    extent: str,
) -> None:
    source = InputHistoryApi(1, INPUT_ID, b"source")
    output = _derived_stream(0, b"derived")
    api = UploadApi()
    writer = _incremental_writer(api, (source,))
    try:
        with pytest.raises(ValueError, match="extent"):
            writer.append(
                output,
                identity=ProducerArtifactIdentity(output.artifact_id, output.bytes, output.sha256),
                output_id=output.output_id,
                source_histories=(source.accepted(),),
                history_extent=extent,
            )
    finally:
        writer.stop()
    assert not api.registered


def test_producer_stream_rejects_mutation_between_hash_and_upload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RIVERHOG_UPLOAD_FILE_CONCURRENCY", "1")
    content = b"generated output"
    calls = 0

    def mutable(offset: int, size: int) -> bytes:
        nonlocal calls
        calls += 1
        value = content if calls == 1 else b"X" * len(content)
        return value[offset : offset + size]

    stream = ProducerStream(
        artifact_id=OUTPUT_ID,
        allow_missing_materialization_hint=True,
        bytes=len(content),
        sha256=hashlib.sha256(content).hexdigest(),
        read_range=mutable,
    )

    with pytest.raises(RuntimeError, match="changed during upload"):
        CollectionProducer(
            UploadApi(),  # type: ignore[arg-type]
            producer_app="stove0-worker",
            adapter_id="test-transform/v1",
            adapter_version="1",
            ingest_source="processing:test",
        ).publish_inputs((stream,), source_event_id="event-1")


def test_producer_file_rejects_symlink_sources(tmp_path: Path) -> None:
    source = tmp_path / "output.mkv"
    source.write_bytes(b"generated output")
    link = tmp_path / "linked.mkv"
    link.symlink_to(source)

    with pytest.raises(ValueError, match="symlink"):
        ProducerFile(source=link, artifact_id=OUTPUT_ID, allow_missing_materialization_hint=True)


def test_producer_file_rejects_mutation_between_hash_and_upload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("RIVERHOG_UPLOAD_FILE_CONCURRENCY", "1")
    source = tmp_path / "output.mkv"
    source.write_bytes(b"generated output")

    class MutatingUploadApi(UploadApi):
        def acquire_collection_upload_session_work(
            self,
            collection_id: int,
            *,
            limit: int = 16,
        ) -> CollectionUploadWorkBatchDocument:
            source.write_bytes(b"X" * len(b"generated output"))
            return super().acquire_collection_upload_session_work(collection_id, limit=limit)

    with pytest.raises(RuntimeError, match="source changed during upload verification"):
        CollectionProducer(
            MutatingUploadApi(),  # type: ignore[arg-type]
            producer_app="stove0-worker",
            adapter_id="test-transform/v1",
            adapter_version="1",
            ingest_source="processing:test",
        ).publish(
            (
                ProducerFile(
                    source=source, artifact_id=OUTPUT_ID, allow_missing_materialization_hint=True
                ),
            ),
            source_event_id="event-1",
        )


def test_derived_writer_binds_outputs_to_dispositions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = _spec()
    output = b"derived"
    stream = ProducerStream(
        artifact_id=OUTPUT_ID,
        allow_missing_materialization_hint=True,
        bytes=len(output),
        sha256=hashlib.sha256(output).hexdigest(),
        read_range=lambda offset, size: output[offset : offset + size],
    )
    captured: dict[str, Any] = {}

    class StubWriter:
        def __init__(self, _api: object, **kwargs: Any) -> None:
            captured["init"] = kwargs

        def append(self, source: object, **kwargs: Any) -> None:
            captured["append"] = (source, kwargs)

        def finish(self, **kwargs: Any) -> DerivedCollectionReceipt:
            captured["finish"] = kwargs
            return DerivedCollectionReceipt(44, "e" * 64, "f" * 64, _derivation(spec))

        def stop(self) -> None:
            captured["stopped"] = True

    import riverhog_client.processing.writer as module

    monkeypatch.setattr(module, "IncrementalDerivedCollectionWriter", StubWriter)
    writer = DerivedCollectionWriter(
        UploadApi(),
        spec=spec,
        claim_id="claim-1",
        fence=1,
        work_id=WORK_ID,
        execution_id=EXECUTION_ID,
        controller_evidence=CONTROLLER_EVIDENCE,
        producer_app="fixture-transform",
    )

    identity = ProducerArtifactIdentity(stream.artifact_id, stream.bytes, stream.sha256)
    histories = (object(),)
    records = (object(),)
    from dataclasses import replace

    stream = replace(stream, output_id="output-1")
    receipt = writer.publish(
        (stream,),
        identities={OUTPUT_ID: identity},
        source_histories={OUTPUT_ID: histories},
        history_extent=BOUND_HISTORY_EXTENT,
        completion_records=records,
        execution_envelope_sha256="c" * 64,
        execution_sha256="d" * 64,
        disposition_set=_disposition_set(),
        source_context={"target": "fixture"},
    )
    assert receipt.collection_id == 44
    assert captured["append"] == (
        stream,
        {
            "identity": identity,
            "output_id": "output-1",
            "source_histories": histories,
            "history_extent": BOUND_HISTORY_EXTENT,
        },
    )
    assert captured["finish"]["completion_records"] is records
    assert captured["finish"]["disposition_set"] == _disposition_set()
    assert captured["stopped"]
    with pytest.raises(ValueError, match="exact input-history correspondence"):
        writer.publish(
            (stream,),
            identities={},
            source_histories={},
            history_extent=BOUND_HISTORY_EXTENT,
            completion_records=records,
            execution_envelope_sha256="c" * 64,
            execution_sha256="d" * 64,
            disposition_set=_disposition_set(),
        )


def test_runtime_passes_the_sealed_disposition_identity_to_publication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = _spec()
    api = RetrievalApi()
    runtime = CollectionTransformRuntime(
        api,  # type: ignore[arg-type]
        spec=spec,
        claim_id="claim-1",
        fence=1,
        work_id=WORK_ID,
        execution_id=EXECUTION_ID,
        controller_evidence=CONTROLLER_EVIDENCE,
        producer_app="fixture-transform",
    )
    output = b"derived"
    stream = ProducerStream(
        artifact_id=OUTPUT_ID,
        allow_missing_materialization_hint=True,
        bytes=len(output),
        sha256=hashlib.sha256(output).hexdigest(),
        read_range=lambda offset, size: output[offset : offset + size],
    )
    derivation = _derivation(spec)
    expected_receipt = DerivedCollectionReceipt(44, "e" * 64, "f" * 64, derivation)
    captured: dict[str, Any] = {}

    def publish(*_args: object, **kwargs: Any) -> DerivedCollectionReceipt:
        captured.update(kwargs)
        return expected_receipt

    monkeypatch.setattr(runtime.writer, "publish", publish)
    authority = _disposition_set()

    assert (
        runtime.publish(
            (stream,),
            execution_envelope_sha256="c" * 64,
            execution_sha256="d" * 64,
            disposition_set=authority,
        )
        == expected_receipt
    )
    assert captured["disposition_set"] == authority


def test_finalized_receipt_is_not_revoked_by_a_late_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = _spec()
    checks = 0

    def cancellation_check() -> None:
        nonlocal checks
        checks += 1
        if checks > 1:
            raise RuntimeError("canceled after publication")

    runtime = CollectionTransformRuntime(
        RetrievalApi(),  # type: ignore[arg-type]
        spec=spec,
        claim_id="claim-1",
        fence=1,
        work_id=WORK_ID,
        execution_id=EXECUTION_ID,
        controller_evidence=CONTROLLER_EVIDENCE,
        producer_app="fixture-transform",
        cancellation_check=cancellation_check,
    )
    output = b"derived"
    stream = ProducerStream(
        artifact_id=OUTPUT_ID,
        allow_missing_materialization_hint=True,
        bytes=len(output),
        sha256=hashlib.sha256(output).hexdigest(),
        read_range=lambda offset, size: output[offset : offset + size],
    )
    derivation = _derivation(spec)
    expected = DerivedCollectionReceipt(44, "e" * 64, "f" * 64, derivation)
    monkeypatch.setattr(runtime.writer, "publish", lambda *_args, **_kwargs: expected)

    assert (
        runtime.publish(
            (stream,),
            execution_envelope_sha256="c" * 64,
            execution_sha256="d" * 64,
            disposition_set=_disposition_set(),
        )
        == expected
    )
    assert checks == 1


def test_workspace_requires_explicit_protected_storage(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    root.mkdir(mode=0o700)
    root.chmod(0o700)

    with ProcessingWorkspace.open(
        root,
        execution_id=EXECUTION_ID,
        declared_protection="memory-backed",
    ) as workspace:
        marker = json.loads((workspace.root / ".riverhog-processing-workspace.json").read_text())
        assert marker["declared_protection"] == "memory-backed"
        with pytest.raises(ValueError, match="protection declaration"):
            ProcessingWorkspace.open(
                root,
                execution_id=EXECUTION_ID,
                declared_protection="encrypted-at-rest",
            )
        output = workspace.resolve("video/output.mkv")
        output.parent.mkdir(parents=True)
        output.write_bytes(b"derived")
        assert output.is_file()
        escaped = workspace.root / "escaped"
        escaped.symlink_to(tmp_path, target_is_directory=True)
        with pytest.raises(ValueError, match="symlinks"):
            workspace.resolve("escaped/outside.bin")
        workspace.release()

    assert not (root / EXECUTION_ID).exists()


def test_capability_client_refreshes_workers_without_closing_active_delegate() -> None:
    from riverhog_client.processing import CapabilityApiClient

    class Client:
        def __init__(self, value: str) -> None:
            self.value = value
            self.closed = False

        def identity(self) -> str:
            assert not self.closed
            return self.value

        @contextmanager
        def stream(self):
            assert not self.closed
            yield iter((self.value,))
            assert not self.closed

        def close(self) -> None:
            self.closed = True

    first = Client("first")
    second = Client("second")
    root = CapabilityApiClient(first, owns_client=True)
    worker = root.spawn()

    with worker.stream() as body:
        root.replace(second, owns_client=True)
        assert next(body) == "first"
        assert worker.identity() == "second"
        assert not first.closed
    assert first.closed
    root.close()
    assert first.closed
    assert second.closed


def test_capability_refresh_keeps_only_current_and_actively_streaming_clients() -> None:
    from riverhog_client.processing import CapabilityApiClient

    clients: list[Any] = []

    class Client:
        def __init__(self, value: int) -> None:
            self.value = value
            self.closed = 0
            clients.append(self)

        @contextmanager
        def stream(self):
            assert not self.closed
            yield iter((self.value,))
            assert not self.closed

        def close(self):
            self.closed += 1

    root = CapabilityApiClient(Client(0), owns_client=True)
    worker = root.spawn()
    with worker.stream() as body:
        for value in range(1, 129):
            root.replace(Client(value), owns_client=True)
            assert sum(not client.closed for client in clients) == 2
        assert tuple(body) == (0,)
    assert sum(not client.closed for client in clients) == 1
    worker.close()
    assert not clients[-1].closed
    root.close()
    assert all(client.closed == 1 for client in clients)


@pytest.mark.parametrize("finish", ["exhaust", "close", "failure"])
def test_capability_iterator_releases_retired_client_after_its_cleanup(finish: str) -> None:
    from riverhog_client.processing import CapabilityApiClient

    class Client:
        closed = False
        cleaned = False

        def rows(self):
            try:
                yield "first"
                if finish == "failure":
                    raise ValueError("stream failed")
            finally:
                assert not self.closed
                self.cleaned = True

        def close(self):
            self.closed = True

    first = Client()
    root = CapabilityApiClient(first, owns_client=True)
    rows = root.rows()
    assert next(rows) == "first"
    root.replace(Client(), owns_client=True)
    assert not first.closed
    if finish == "failure":
        with pytest.raises(ValueError, match="stream failed"):
            next(rows)
    elif finish == "exhaust":
        assert tuple(rows) == ()
    else:
        rows.close()
    assert first.cleaned and first.closed
    root.close()


def test_capability_refresh_preserves_in_flight_regular_request_until_return() -> None:
    from riverhog_client.processing import CapabilityApiClient

    started = threading.Event()
    finish = threading.Event()

    class Client:
        closed = False

        def request(self):
            started.set()
            assert finish.wait(timeout=5)
            assert not self.closed

        def close(self):
            self.closed = True

    first = Client()
    root = CapabilityApiClient(first, owns_client=True)
    thread = threading.Thread(target=root.request)
    thread.start()
    try:
        assert started.wait(timeout=5)
        root.replace(Client(), owns_client=True)
        assert not first.closed
    finally:
        finish.set()
        thread.join(timeout=5)
        root.close()
    assert not thread.is_alive() and first.closed


@pytest.mark.parametrize("runtime_type", [ClaimedCollectionRuntime, CollectionTransformRuntime])
def test_capability_refresh_does_not_mutate_collection_liveness(runtime_type: type[Any]) -> None:
    class Current:
        base_url = "https://riverhog.invalid"
        allow_insecure_http = False
        host_header = None
        http2 = True
        timeout_seconds = 30.0
        upload_timeout_seconds = 60.0

    class Facade:
        current = Current()
        replacement: Any = None

        def replace(self, client: Any, *, owns_client: bool) -> None:
            assert owns_client
            self.replacement = client

    runtime = runtime_type.__new__(runtime_type)
    runtime.api = Facade()

    runtime.refresh_capability("replacement-secret")

    assert runtime.api.replacement.token == "replacement-secret"


def test_runtime_registry_applies_refresh_arriving_before_target_start() -> None:
    from riverhog_client.processing import ClaimedCollectionRuntimeRegistry

    class Runtime:
        def __init__(self) -> None:
            self.tokens: list[str] = []
            self.closed = False

        def refresh_capability(self, token: str) -> None:
            self.tokens.append(token)

        def close(self) -> None:
            self.closed = True

    registry = ClaimedCollectionRuntimeRegistry()
    runtime = Runtime()
    registry.refresh("job-1", "replacement")
    assert registry.capability_token("job-1", fallback="initial") == "replacement"
    assert registry.capability_token("job-2", fallback="independent") == "independent"

    with registry.bind("job-1", runtime):  # type: ignore[arg-type]
        assert runtime.tokens == ["replacement"]
        registry.refresh("job-1", "newer")
        assert runtime.tokens == ["replacement", "newer"]
        assert registry.capability_token("job-1", fallback="initial") == "newer"

    assert registry.capability_token("job-1", fallback="initial") == "newer"

    registry.discard("job-1")
    assert not runtime.closed
    assert registry.capability_token("job-1", fallback="initial") == "initial"


def test_runtime_rejects_empty_capability_without_environment_fallback() -> None:
    spec = _spec()
    with pytest.raises(ValueError, match="nonempty"):
        CollectionTransformRuntime.from_capability(
            base_url="https://riverhog.invalid",
            capability_token="  ",
            spec=spec,
            claim_id="claim-1",
            fence=1,
            work_id=WORK_ID,
            execution_id=EXECUTION_ID,
            controller_evidence=CONTROLLER_EVIDENCE,
            producer_app="fixture-transform",
        )


def test_runtime_registry_cleans_up_failed_pending_refresh() -> None:
    from riverhog_client.processing import ClaimedCollectionRuntimeRegistry

    class FailingRuntime:
        def refresh_capability(self, _token: str) -> None:
            raise RuntimeError("refresh rejected")

        def close(self) -> None:
            pass

    class WorkingRuntime:
        def refresh_capability(self, _token: str) -> None:
            pass

        def close(self) -> None:
            pass

    registry = ClaimedCollectionRuntimeRegistry()
    registry.refresh("job-1", "replacement")
    with pytest.raises(RuntimeError, match="refresh rejected"):
        with registry.bind("job-1", FailingRuntime()):  # type: ignore[arg-type]
            raise AssertionError("unreachable")

    with registry.bind("job-1", WorkingRuntime()):  # type: ignore[arg-type]
        pass


def test_api_client_streams_verified_full_and_range_content() -> None:
    content = b"0123456789"
    digest = hashlib.sha256(content).hexdigest()

    def handle(request: httpx.Request) -> httpx.Response:
        assert request.headers["Accept-Encoding"] == "identity"
        byte_range = request.headers.get("Range")
        if byte_range is None:
            return httpx.Response(
                200,
                headers={
                    "Content-Length": str(len(content)),
                    "ETag": f'"{digest}"',
                },
                content=content,
            )
        assert byte_range == "bytes=2-6"
        selected = content[2:7]
        return httpx.Response(
            206,
            headers={
                "Content-Length": str(len(selected)),
                "Content-Range": f"bytes 2-6/{len(content)}",
                "ETag": f'"{digest}"',
            },
            content=selected,
        )

    api = ApiClient(base_url="https://riverhog.invalid", token="scoped")
    api._download_client = httpx.Client(  # type: ignore[attr-defined]
        base_url="https://riverhog.invalid",
        transport=httpx.MockTransport(handle),
    )
    try:
        with api.stream_retrieval_artifact(
            "job-1",
            collection_id=1,
            artifact_id=INPUT_ID,
            expected_bytes=len(content),
            expected_sha256=digest,
            chunk_size=3,
        ) as chunks:
            assert b"".join(chunks) == content
        with api.stream_retrieval_artifact(
            "job-1",
            collection_id=1,
            artifact_id=INPUT_ID,
            expected_bytes=len(content),
            expected_sha256=digest,
            start=2,
            end=7,
            chunk_size=2,
        ) as chunks:
            assert b"".join(chunks) == content[2:7]
    finally:
        api.close()


def test_api_client_stream_requires_complete_consumption() -> None:
    content = b"0123456789"
    digest = hashlib.sha256(content).hexdigest()
    api = ApiClient(base_url="https://riverhog.invalid", token="scoped")
    api._download_client = httpx.Client(  # type: ignore[attr-defined]
        base_url="https://riverhog.invalid",
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(
                200,
                headers={
                    "Content-Length": str(len(content)),
                    "ETag": f'"{digest}"',
                },
                content=content,
            )
        ),
    )
    try:
        with pytest.raises(InvalidState, match="ended before"):
            with api.stream_retrieval_artifact(
                "job-1",
                collection_id=1,
                artifact_id=INPUT_ID,
                expected_bytes=len(content),
                expected_sha256=digest,
                chunk_size=2,
            ) as chunks:
                next(chunks)
    finally:
        api.close()


def test_plan_seal_replays_submitted_artifact_prefix_and_checks_sealed_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from riverhog_canonical_json import canonical_json_bytes
    from riverhog_client.workflows import CollectionWorkflowMethods

    root = {
        "collection_id": "1",
        "archive_root_sha256": "a" * 64,
        "artifact_set_identity": "b" * 64,
    }
    artifacts = [
        {"collection": root, "artifact_id": f"{i:064x}", "bytes": "1", "sha256": "c" * 64}
        for i in range(129)
    ]
    digest = hashlib.sha256(b"riverhog-claim-artifacts/v1\0")
    for artifact in artifacts:
        encoded = canonical_json_bytes(artifact)
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    identity = SimpleNamespace(count=129, total_bytes=129, sha256=digest.hexdigest())
    claim = SimpleNamespace(plan=None, artifacts=SimpleNamespace(count=128))
    client = CollectionWorkflowMethods()
    offsets = []
    monkeypatch.setattr(client, "get_processing_claim", lambda *_: claim)
    monkeypatch.setattr(
        client,
        "append_processing_claim_artifacts",
        lambda *_, **kw: offsets.append((kw["start_ordinal"], len(kw["artifacts"]))),
    )
    monkeypatch.setattr(
        client,
        "seal_processing_claim_artifacts",
        lambda *_, **kw: SimpleNamespace(identity=identity),
    )
    monkeypatch.setattr(client, "_claim_response", lambda *_, **kw: claim, raising=False)
    kwargs = dict(
        fence=1,
        execution_id=EXECUTION_ID,
        controller_evidence=CONTROLLER_EVIDENCE,
        controller_evidence_sha256=CONTROLLER_EVIDENCE_SHA256,
        operation_id="fixture.operation/v1",
        operation_sha256="d" * 64,
    )
    client.seal_processing_claim_plan("e" * 64, input_artifacts=iter(artifacts), **kwargs)
    assert offsets == [(0, 128), (128, 1)]
    # A sealed plan is an immutable authority, not a reason to skip scope verification.
    claim.plan = SimpleNamespace(artifacts=identity)
    offsets.clear()
    client.seal_processing_claim_plan("e" * 64, input_artifacts=iter(artifacts), **kwargs)
    assert offsets == []
    with pytest.raises(ValueError, match="exact requested input scope"):
        client.seal_processing_claim_plan("e" * 64, input_artifacts=iter(artifacts[:-1]), **kwargs)
