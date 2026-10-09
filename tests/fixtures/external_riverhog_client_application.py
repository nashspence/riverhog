"""Installed-wheel proof for a generic non-Stove0 Riverhog processing application."""

from __future__ import annotations

import hashlib
import sys
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import riverhog_client.producer as producer_module
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
from riverhog_client import (
    ApiClient,
    ProducerArtifactIdentity,
    ProducerStream,
    RawSourceHash,
    create_or_resume_with_initial_collection_tags,
    hash_raw_source_chunks,
)
from riverhog_client.canonical_completion import CompletionRecord
from riverhog_client.canonical_production import ProducerAttribution, build_member_journal
from riverhog_protocol import ArtifactMemberIdentityDocument, PortableCollectionInventoryPage
from riverhog_protocol.artifact_identity import ArtifactId
from riverhog_protocol.collection_completion import CollectionCompletionRecordingDocument
from riverhog_protocol.collection_workflow_transport import (
    ArtifactDispositionOutputPageDocument,
    ArtifactDispositionPageDocument,
)
from riverhog_protocol.collection_workflows import (
    ArtifactDispositionSetIdentity,
    CollectionRootIdentity,
    OperationIdentity,
    RecipeIdentity,
)
from riverhog_protocol.errors import NotFound
from riverhog_protocol.manifest import artifact_set_identity_ordered
from riverhog_protocol.output_collection_policy import OutputCollectionPolicy
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    assertion,
    create_journal,
    external_reference,
    validate_journal,
)

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


class ExternalInputHistory:
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
            self, inputs=(self.root,), work_id=WORK_ID, claim_id=CLAIM_ID, fence=1
        )
        return reader.provenance(
            ClaimedArtifact(
                self.root, self.member.artifact_id, self.member.bytes, self.member.sha256
            )
        )


_INPUT_HISTORY = ExternalInputHistory(1, INPUT_ID, INPUT_CONTENT)
INPUT_ROOT = _INPUT_HISTORY.root


class ReadApi:
    def __init__(self) -> None:
        self.closed = False

    def __getattr__(self, name: str) -> Any:
        return getattr(_INPUT_HISTORY, name)

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
                        "provenance_identity": hashlib.sha256(
                            _INPUT_HISTORY.proof.provenance_root
                        ).hexdigest(),
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
        self.bindings: dict[str, Any] = {}
        self.structures: dict[str, bytes] = {}
        self.history_inputs: dict[str, Any] = {}
        self.requirement: Any = None
        self.recording: Any = None
        self.finalized = False
        self.derivation_identity = DISPOSITIONS
        self.dispositions = [
            {
                "input": {
                    "collection_id": "1",
                    "archive_root_sha256": INPUT_ROOT.archive_root_sha256,
                    "artifact_id": INPUT_ID,
                },
                "status": "transformed",
            }
        ]
        self.output_edges = [
            {"input": self.dispositions[0]["input"], "output_artifact_id": OUTPUT_ID}
        ]
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
            "construction_identity_sha256": "c" * 64,
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

    def get_collection_upload_session_artifact(
        self, _collection_id: int, artifact_id: ArtifactId
    ) -> dict[str, Any]:
        return {**self.registered[str(artifact_id)], "custody_receipt": None}

    def upload_collection_upload_session_provenance_journal(
        self,
        _collection_id: int,
        journal_id: str,
        *,
        content: Iterator[bytes],
        byte_count: int,
        sha256: str,
        **_kwargs: Any,
    ) -> None:
        raw = b"".join(content)
        assert len(raw) == byte_count
        assert hashlib.sha256(raw).hexdigest() == sha256
        validate_journal(raw, require_profiles=False)
        self.journals[journal_id] = raw

    def get_collection_upload_session_artifact_provenance_binding(
        self, _collection_id: int, _artifact_id: ArtifactId
    ) -> object:
        try:
            return self.bindings[_artifact_id]
        except KeyError as exc:
            raise NotFound("binding absent") from exc

    def bind_collection_upload_session_artifact_provenance(
        self, _collection_id: int, batch: Any
    ) -> None:
        for binding in batch.bindings:
            assert self.bindings.setdefault(binding.artifact_id, binding) == binding

    def set_collection_upload_session_materialization_decisions(
        self, _collection_id: int, batch: Any
    ) -> None:
        self.decisions.extend(batch.model_dump(mode="json")["decisions"])

    def heartbeat_collection_upload_session(self, _collection_id: int) -> dict[str, str]:
        return {"state": "open"}

    def complete_collection_upload_session(self, _collection_id: int) -> dict[str, object]:
        self.finalized = True
        return {
            "state": "finalized",
            "artifact_set_identity": "8" * 64,
            "collection": {
                "id": "2",
                "archive_root_sha256": "f" * 64,
                "artifact_set_identity": "8" * 64,
            },
        }

    def list_processing_claim_dispositions(
        self,
        claim_id: str,
        *,
        identity_sha256: str,
        start_ordinal: int = 0,
    ) -> ArtifactDispositionPageDocument:
        assert claim_id == CLAIM_ID
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
        assert claim_id == CLAIM_ID
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

    def get_collection_upload_session(self, _collection_id: int) -> dict[str, object]:
        if self.finalized:
            return self.complete_collection_upload_session(_collection_id)
        return {"state": "open", "construction_identity_sha256": "c" * 64}

    def list_collection_upload_session_artifacts(
        self, _collection_id: int, **_kwargs: Any
    ) -> dict[str, object]:
        return {"artifacts": list(self.registered.values()), "next_page_token": None}

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
        output_id="external-output",
        source_histories=(_INPUT_HISTORY.accepted(),),
        history_extent=BOUND_HISTORY_EXTENT,
    )
    raw_evidence = b'{"optional":null,"quality":1.2300}'
    evidence_digest = hashlib.sha256(raw_evidence).hexdigest()
    completion_records = tuple(
        CompletionRecord(kind, len(raw_evidence), evidence_digest, lambda: (raw_evidence,))
        for kind in (
            "implementation",
            "invocation",
            "target-execution",
            "target-output-declarations",
            "target-result",
        )
    )
    receipt = runtime.finish_incremental_publication(
        writer,
        execution_sha256=evidence_digest,
        disposition_set=DISPOSITIONS,
        completion_records=completion_records,
        poll_seconds=0.01,
    )
    runtime.close()
    assert receipt.collection_id == 2
    assert len(upload_api.registered) == len(upload_api.bindings) == 1
    assert len(upload_api.journals) == 4  # two inherited snapshots, primary, completion
    assert upload_api.requirement is not None and upload_api.recording is not None
    assert len(upload_api.structures) >= 3
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
