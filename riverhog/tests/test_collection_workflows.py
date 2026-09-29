from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from pytest import FixtureRequest
from riverhog_client.initial_tags import prepare_initial_collection_tags
from riverhog_core.app_permissions import (
    ARCHIVES_MANAGE,
    CATALOG_READ,
    COLLECTION_TAGS_MANAGE,
    PROVENANCE_EXPORT,
    PROVENANCE_READ,
    RETRIEVAL_MANAGE,
    ApplicationAccess,
    Principal,
    tag_resource,
)
from riverhog_core.catalog_base import Base
from riverhog_core.catalog_db import SessionFactory
from riverhog_core.catalog_models import (
    CollectionArchiveCopyRecord,
    CollectionArchiveObjectRecord,
    CollectionFileRecord,
    CollectionRecord,
    CollectionUploadRecord,
)
from riverhog_core.catalog_workflow_models import (
    CollectionProcessingClaimRecord,
    CollectionProcessingConsiderationEvidenceRecord,
    CollectionProcessingConsiderationSubjectRecord,
    CollectionProcessingDispositionSetRecord,
    CollectionProcessingEffectSettlementRecord,
)
from riverhog_core.services.collection_uploads import _require_transform_output_intent
from riverhog_core.services.collection_workflows import (
    SqlAlchemyCollectionWorkflowService,
    processing_claim_blockers,
)
from riverhog_protocol.collection_tags import COLLECTION_TAG_REQUEST_MEMBERS_MAX
from riverhog_protocol.collection_workflows import (
    DERIVATION_EVIDENCE_PATH,
    PRODUCER_EVIDENCE_PATH,
    ArtifactDiscardApproval,
    ArtifactDisposition,
    ArtifactDispositionOutput,
    ArtifactDispositionSetIdentity,
    CollectionArtifactIdentity,
    CollectionDerivation,
    CollectionProcessingOutcomeIdentity,
    CollectionRootIdentity,
    OperationIdentity,
    RecipeIdentity,
    canonical_json_bytes,
    canonical_json_sha256,
    derivation_evidence_page_path,
    processing_outcome_set_identity,
)
from riverhog_protocol.effect_settlement import ExternalEffectSettlement
from riverhog_protocol.errors import BadRequest, Conflict, Forbidden, NotFound
from riverhog_protocol.no_output_settlement import NoOutputSettlement
from riverhog_protocol.output_collection_policy import OutputCollectionPolicy
from sqlalchemy import create_engine, delete, select
from sqlalchemy.orm import sessionmaker
from stove0_core.riverhog import _no_output_discard_approval
from stove0_recipe_config import (
    ArtifactFactBinding,
    RecipeSourceLossEvidenceSlot,
    RecipeSourceLossRule,
)
from time_formats import parse_utc_timestamp

from tests.unit.storage_incarnation_fixtures import seed_storage_incarnation

NOW = "2026-08-15T00:00:00Z"
WORK_ID = "b" * 64
EXECUTION_ID = "d" * 64
CONTROLLER_EVIDENCE = {
    "format": "stove0-controller-evidence/v1",
    "execution_envelope": {"execution_envelope_sha256": EXECUTION_ID},
}
CONTROLLER_EVIDENCE_SHA256 = hashlib.sha256(canonical_json_bytes(CONTROLLER_EVIDENCE)).hexdigest()


def _session_factory(tmp_path: Path, request: FixtureRequest) -> SessionFactory:
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'state.sqlite3'}")
    request.addfinalizer(engine.dispose)
    Base.metadata.create_all(engine)
    return cast(SessionFactory, sessionmaker(engine, expire_on_commit=False))


def _collection(
    session: Any,
    collection_id: int,
    *,
    creator: str,
    root: str,
    idempotency_key: str | None = None,
) -> None:
    collection = CollectionRecord(
        id=collection_id,
        creation_idempotency_key=idempotency_key or f"collection-{collection_id}",
        creation_identity_sha256=f"{collection_id:064x}",
        creation_custody_mode="producer-retained",
        content_identity=str(collection_id) * 64,
        encryption_format="age-v1-scrypt",
        passphrase_id="fixture-archive-key-v1",
        provenance_mode="omitted",
        provenance_identity=None,
        inventory_identity="f" * 64,
        ingest_source="fixture",
        created_by_principal_id=creator,
        created_by_key_id=None,
        created_at=NOW,
    )
    session.add(collection)
    session.flush()
    if collection_id == 1:
        session.add(
            CollectionFileRecord(
                collection_id=collection_id,
                path="camera/input.mov",
                bytes=4,
                sha256="8" * 64,
            )
        )
    session.add(
        CollectionArchiveCopyRecord(
            collection_id=collection_id,
            store="hot",
            incarnation_id=seed_storage_incarnation(session, "archive", "hot"),
            state="uploaded",
            archive_storage_prefix=f"collections/{collection_id}",
            last_uploaded_at=NOW,
            last_verified_at=NOW,
            failure=None,
        )
    )
    session.flush()
    session.add(
        CollectionArchiveObjectRecord(
            collection_id=collection_id,
            store="hot",
            object_id="manifest",
            object_order=1,
            kind="manifest",
            object_path=f"collections/{collection_id}/manifest.json.age",
            plaintext_bytes=1,
            stored_bytes=2,
            sha256=root,
            stored_sha256="9" * 64,
            revision=None,
            age_state_json=None,
            archive_parts_json=None,
            plan_sha256=None,
            index_sha256=None,
            uploaded_at=NOW,
            verified_at=NOW,
        )
    )


def _principal() -> Principal:
    return Principal(id="stove0", key_id="stove0-key", access=frozenset())


def _setup(factory: SessionFactory) -> CollectionRootIdentity:
    with factory() as session, session.begin():
        _collection(session, 1, creator="ftp", root="a" * 64)
    return CollectionRootIdentity(1, "a" * 64, "1" * 64)


def _work_document(root: CollectionRootIdentity) -> dict[str, object]:
    return {
        "format": "stove0-work/v1",
        "work_id": WORK_ID,
        "inputs": [root.as_dict()],
    }


def _artifact(root: CollectionRootIdentity) -> CollectionArtifactIdentity:
    return CollectionArtifactIdentity(
        collection=root,
        path="camera/input.mov",
        bytes=4,
        sha256="8" * 64,
    )


def _create_claim(
    service: SqlAlchemyCollectionWorkflowService,
    *,
    work_id: str,
    work_document: dict[str, object],
    root: CollectionRootIdentity,
    artifact: CollectionArtifactIdentity | None = None,
    purpose: str = "collection-work/v1",
) -> dict[str, object]:
    claim = service.create_or_resume_claim(
        work_id=work_id,
        work_document=work_document,
        work_document_sha256=hashlib.sha256(canonical_json_bytes(work_document)).hexdigest(),
        purpose=purpose,
        principal=_principal(),
    )
    claim_id = str(claim["id"])
    fence = int(claim["fence"])
    service.append_claim_inputs(
        claim_id,
        fence=fence,
        start_ordinal=0,
        inputs=(root,),
        principal=_principal(),
    )
    inputs = service.seal_claim_inputs(claim_id, fence=fence, principal=_principal())
    service.append_claim_artifacts(
        claim_id,
        fence=fence,
        start_ordinal=0,
        artifacts=(artifact or _artifact(root),),
        principal=_principal(),
    )
    artifacts = service.seal_claim_artifacts(claim_id, fence=fence, principal=_principal())
    result = service.get_claim(claim_id, principal=_principal())
    result["inputs"] = inputs
    result["artifacts"] = artifacts
    return result


def _operation_declaration(operation_id: str) -> dict[str, object]:
    return {
        "id": operation_id,
        "result_kind": "collection",
        "source_collection_retirement_permitted": True,
    }


def _seal_plan(
    service: SqlAlchemyCollectionWorkflowService,
    claim_id: str,
    *,
    execution_id: str = EXECUTION_ID,
    controller_evidence: dict[str, object] = CONTROLLER_EVIDENCE,
    controller_evidence_sha256: str = CONTROLLER_EVIDENCE_SHA256,
    operation_id: str = "archive-video/v1",
    source_collection_retirement_policy: str = "retain",
    output_policy: OutputCollectionPolicy | None = None,
) -> dict[str, object]:
    return service.seal_claim_plan(
        claim_id,
        fence=1,
        execution_id=execution_id,
        controller_evidence=controller_evidence,
        controller_evidence_sha256=controller_evidence_sha256,
        operation_id=operation_id,
        operation_sha256=canonical_json_sha256(_operation_declaration(operation_id)),
        operation_contract=_operation_declaration(operation_id),
        source_collection_retirement_policy=source_collection_retirement_policy,
        source_collection_retirement_grace_seconds=0,
        output_policy=output_policy,
        principal=_principal(),
    )


def _seal_dispositions(
    service: SqlAlchemyCollectionWorkflowService,
    claim_id: str,
    *,
    root: CollectionRootIdentity,
    input_path: str,
    output_path: str,
) -> ArtifactDispositionSetIdentity:
    service.record_dispositions(
        claim_id,
        fence=1,
        dispositions=(
            ArtifactDisposition(
                input_collection_id=root.collection_id,
                input_archive_root_sha256=root.archive_root_sha256,
                input_path=input_path,
                status="transformed",
            ),
        ),
        principal=_principal(),
    )
    service.record_disposition_outputs(
        claim_id,
        fence=1,
        outputs=(
            ArtifactDispositionOutput(
                input_collection_id=root.collection_id,
                input_archive_root_sha256=root.archive_root_sha256,
                input_path=input_path,
                output_path=output_path,
            ),
        ),
        principal=_principal(),
    )
    sealed = service.seal_disposition_set(
        claim_id,
        fence=1,
        principal=_principal(),
    )
    while sealed["state"] == "sealing":
        assert service.process_due_disposition_sets(limit=1) == 1
        sealed = service.get_disposition_set(claim_id, principal=_principal())
    assert sealed["state"] == "sealed"
    return ArtifactDispositionSetIdentity.from_mapping(cast(dict[str, object], sealed["identity"]))


def _issue_capability(
    service: SqlAlchemyCollectionWorkflowService,
    claim_id: str,
    root: CollectionRootIdentity,
    *,
    audience: str,
    actions: tuple[str, ...],
) -> dict[str, object]:
    capability = service.issue_capability(
        claim_id,
        fence=1,
        audience=audience,
        actions=actions,
        ttl_seconds=600,
        principal=_principal(),
    )
    service.append_capability_artifacts(
        claim_id,
        str(capability["id"]),
        fence=1,
        start_ordinal=0,
        artifacts=(_artifact(root),),
        principal=_principal(),
    )
    service.seal_capability_artifacts(
        claim_id,
        str(capability["id"]),
        fence=1,
        principal=_principal(),
    )
    return capability


def test_root_verification_capability_cannot_read_payload_or_provenance(
    tmp_path: Path, request: FixtureRequest
) -> None:
    factory = _session_factory(tmp_path, request)
    root = _setup(factory)
    service = SqlAlchemyCollectionWorkflowService(cast(Any, object()), session_factory=factory)
    claim = _create_claim(
        service,
        work_id=WORK_ID,
        work_document=_work_document(root),
        root=root,
    )
    capability = _issue_capability(
        service,
        str(claim["id"]),
        root,
        audience="fixture.observer/v1",
        actions=("read-root",),
    )
    delegated = service.authenticate_capability(str(capability["token"]))
    assert delegated is not None
    assert delegated.has_artifact_scope
    assert delegated.allows_collection(CATALOG_READ, root.collection_id)
    assert not delegated.allows_collection(RETRIEVAL_MANAGE, root.collection_id)
    assert not delegated.allows_collection(PROVENANCE_READ, root.collection_id)
    assert not delegated.allows_collection(PROVENANCE_EXPORT, root.collection_id)


def test_sealed_output_policy_grants_exact_initial_classification_only(
    tmp_path: Path, request: FixtureRequest
) -> None:
    factory = _session_factory(tmp_path, request)
    root = _setup(factory)
    service = SqlAlchemyCollectionWorkflowService(cast(Any, object()), session_factory=factory)
    claim = _create_claim(
        service,
        work_id=WORK_ID,
        work_document=_work_document(root),
        root=root,
    )
    claim_id = str(claim["id"])
    tags = tuple(f"classification/{index:04d}" for index in range(205))
    policy = OutputCollectionPolicy(
        archive_store="hot",
        use_cache=False,
        copy_to=("cold",),
        tags=tags,
    )
    sealed = _seal_plan(service, claim_id, output_policy=policy)
    assert sealed["plan"]["output_policy"] == policy.model_dump(mode="json")  # type: ignore[index]

    capability = _issue_capability(
        service,
        claim_id,
        root,
        audience="fixture.target/v1",
        actions=("read-inputs", "write-output"),
    )
    delegated = service.authenticate_capability(str(capability["token"]))
    assert delegated is not None
    assert ApplicationAccess(COLLECTION_TAGS_MANAGE, tag_resource(tags[-1])) in delegated.access
    assert all(access.permission != ARCHIVES_MANAGE for access in delegated.access)

    with prepare_initial_collection_tags(tags) as prepared:
        batches = tuple(prepared.iter_batches())
        assert tuple(len(batch) for batch in batches) == (
            COLLECTION_TAG_REQUEST_MEMBERS_MAX,
            COLLECTION_TAG_REQUEST_MEMBERS_MAX,
            5,
        )
        with factory() as session:
            intent = dict(
                initiator=delegated,
                idempotency_key=EXECUTION_ID,
                ingest_source=f"processing:{EXECUTION_ID}",
                archive_store="hot",
                use_cache=False,
                copy_to=("cold",),
                initial_tag_set_identity=prepared.tag_set_identity,
            )
            _require_transform_output_intent(session, tags=batches[0], **intent)
            with pytest.raises(Forbidden, match="sealed transform output intent"):
                _require_transform_output_intent(session, tags=batches[0][:-1], **intent)

    with pytest.raises(Conflict, match="sealed"):
        _seal_plan(service, claim_id, output_policy=OutputCollectionPolicy())


def test_disposition_batch_counts_shared_sources_once(
    tmp_path: Path,
    request: FixtureRequest,
) -> None:
    factory = _session_factory(tmp_path, request)
    root = _setup(factory)
    service = SqlAlchemyCollectionWorkflowService(
        cast(Any, object()),
        session_factory=factory,
    )
    claim = _create_claim(
        service,
        work_id=WORK_ID,
        work_document=_work_document(root),
        root=root,
    )
    claim_id = str(claim["id"])
    _seal_plan(service, claim_id)
    disposition = ArtifactDisposition(
        input_collection_id=root.collection_id,
        input_archive_root_sha256=root.archive_root_sha256,
        input_path="camera/input.mov",
        status="transformed",
    )
    service.record_dispositions(
        claim_id,
        fence=1,
        dispositions=(disposition,),
        principal=_principal(),
    )
    state = service.record_disposition_outputs(
        claim_id,
        fence=1,
        outputs=tuple(
            ArtifactDispositionOutput(
                input_collection_id=root.collection_id,
                input_archive_root_sha256=root.archive_root_sha256,
                input_path="camera/input.mov",
                output_path=path,
            )
            for path in ("video/archive.mkv", "video/archive.mkv.xmp")
        ),
        principal=_principal(),
    )

    assert state["output_edge_count"] == "2"
    assert state["output_artifact_count"] == "2"
    with factory() as session:
        counters = session.get_one(CollectionProcessingDispositionSetRecord, claim_id)
        assert counters.successor_required_count == 1
        assert counters.successor_bound_count == 1
    sealed = service.seal_disposition_set(claim_id, fence=1, principal=_principal())
    while sealed["state"] == "sealing":
        assert service.process_due_disposition_sets() == 1
        sealed = service.get_disposition_set(claim_id, principal=_principal())
    assert sealed["state"] == "sealed"


def test_claim_plan_capabilities_settlement_and_deletion_blocker(
    tmp_path: Path,
    request: FixtureRequest,
) -> None:
    factory = _session_factory(tmp_path, request)
    root = _setup(factory)
    service = SqlAlchemyCollectionWorkflowService(
        cast(Any, object()),
        session_factory=factory,
    )
    work = _work_document(root)
    claim = _create_claim(
        service,
        work_id=WORK_ID,
        work_document=work,
        root=root,
    )
    claim_id = str(claim["id"])

    observer_capability = _issue_capability(
        service,
        claim_id,
        root,
        audience="fixture.observer/v1",
        actions=("read-inputs",),
    )
    observer = service.authenticate_capability(str(observer_capability["token"]))
    assert observer_capability["audience"] == "fixture.observer/v1"
    assert observer is not None
    assert observer.id == f"claim:{claim_id}"
    assert observer.key_id == "stove0-key"
    assert observer.has_artifact_scope is True
    assert observer.artifact_scope_capability_id == observer_capability["id"]
    with pytest.raises(Conflict, match="sealed execution plan"):
        service.issue_capability(
            claim_id,
            fence=1,
            audience="fixture.target/v1",
            actions=("read-inputs", "write-output"),
            ttl_seconds=600,
            principal=_principal(),
        )

    sealed = _seal_plan(
        service,
        claim_id,
        source_collection_retirement_policy="retire-after-settlement",
    )
    assert sealed["plan"]["execution_id"] == EXECUTION_ID  # type: ignore[index]
    assert service.authenticate_capability(str(observer_capability["token"])) is None

    first = _issue_capability(
        service,
        claim_id,
        root,
        audience="fixture.target/v1",
        actions=("read-inputs", "write-output"),
    )
    second = _issue_capability(
        service,
        claim_id,
        root,
        audience="fixture.target/v1",
        actions=("read-inputs", "write-output"),
    )
    first_principal = service.authenticate_capability(str(first["token"]))
    second_principal = service.authenticate_capability(str(second["token"]))
    assert first_principal is not None and second_principal is not None
    assert first_principal.id == second_principal.id == f"processing:{EXECUTION_ID}"
    assert first_principal.key_id == second_principal.key_id == "stove0-key"

    disposition_set = _seal_dispositions(
        service,
        claim_id,
        root=root,
        input_path="camera/input.mov",
        output_path="video/output.mkv",
    )
    derivation = CollectionDerivation(
        execution_id=EXECUTION_ID,
        claim_id=claim_id,
        fence=1,
        recipe=RecipeIdentity("camera/v1", 1, "b" * 64),
        operation=OperationIdentity(
            "archive-video/v1", canonical_json_sha256(_operation_declaration("archive-video/v1"))
        ),
        input_set_sha256=cast(str, claim["inputs"]["identity"]["sha256"]),  # type: ignore[index]
        artifact_set_sha256=cast(str, claim["artifacts"]["identity"]["sha256"]),  # type: ignore[index]
        execution_envelope_sha256=EXECUTION_ID,
        execution_sha256="e" * 64,
        controller_evidence=CONTROLLER_EVIDENCE,
        controller_evidence_sha256=CONTROLLER_EVIDENCE_SHA256,
        disposition_set=disposition_set,
    )
    with factory() as session, session.begin():
        _collection(
            session,
            2,
            creator=f"processing:{EXECUTION_ID}",
            root="6" * 64,
            idempotency_key=EXECUTION_ID,
        )
        session.add_all(
            [
                CollectionFileRecord(
                    collection_id=2,
                    path="video/output.mkv",
                    bytes=4,
                    sha256="7" * 64,
                ),
                CollectionFileRecord(
                    collection_id=2,
                    path=DERIVATION_EVIDENCE_PATH,
                    bytes=len(derivation.to_json_bytes()),
                    sha256=derivation.sha256,
                ),
                CollectionFileRecord(
                    collection_id=2,
                    path=PRODUCER_EVIDENCE_PATH,
                    bytes=1,
                    sha256="5" * 64,
                ),
                CollectionFileRecord(
                    collection_id=2,
                    path=derivation_evidence_page_path("dispositions", 0),
                    bytes=1,
                    sha256="3" * 64,
                ),
                CollectionFileRecord(
                    collection_id=2,
                    path=derivation_evidence_page_path("output-edges", 0),
                    bytes=1,
                    sha256="4" * 64,
                ),
            ]
        )
        output_record = session.get(CollectionRecord, 2)
        assert output_record is not None
        output_record.file_count = 5
        output_record.file_bytes = 4 + len(derivation.to_json_bytes()) + 3

    with factory() as session, session.begin():
        stored = session.get(CollectionProcessingClaimRecord, claim_id)
        assert stored is not None
        stored.expires_at = NOW

    settled = service.settle_claim(
        claim_id,
        fence=1,
        output_collection_id=2,
        derivation=derivation.as_dict(),
        principal=_principal(),
    )
    assert settled["state"] == "settled"
    replayed = service.settle_claim(
        claim_id,
        fence=1,
        output_collection_id=2,
        derivation=derivation.as_dict(),
        principal=_principal(),
    )
    assert replayed["state"] == "settled"
    changed_derivation = derivation.as_dict()
    changed_derivation["execution_sha256"] = "f" * 64
    with pytest.raises(Conflict, match="different derivation evidence"):
        service.settle_claim(
            claim_id,
            fence=1,
            output_collection_id=2,
            derivation=changed_derivation,
            principal=_principal(),
        )
    with factory() as session, session.begin():
        stored = session.get(CollectionProcessingClaimRecord, claim_id)
        assert stored is not None
        stored.source_collection_retirement_grace_seconds = 10 * 365 * 24 * 60 * 60
    waiting = service.begin_source_collection_retirement(
        claim_id,
        fence=1,
        principal=_principal(),
    )
    assert waiting["state"] == "settled"
    with factory() as session, session.begin():
        stored = session.get(CollectionProcessingClaimRecord, claim_id)
        assert stored is not None
        stored.source_collection_retirement_grace_seconds = 0
    retiring = service.begin_source_collection_retirement(
        claim_id,
        fence=1,
        principal=_principal(),
    )
    assert retiring["state"] == "retiring"
    replayed_retiring = service.settle_claim(
        claim_id,
        fence=1,
        output_collection_id=2,
        derivation=derivation.as_dict(),
        principal=_principal(),
    )
    assert replayed_retiring["state"] == "retiring"
    assert service.authenticate_capability(str(first["token"])) is None
    with factory() as session:
        blockers = processing_claim_blockers(session, 1)
    assert blockers and claim_id in blockers[0]


def test_claim_fails_closed_on_changed_root_or_reused_work_document(
    tmp_path: Path,
    request: FixtureRequest,
) -> None:
    factory = _session_factory(tmp_path, request)
    root = _setup(factory)
    service = SqlAlchemyCollectionWorkflowService(
        cast(Any, object()),
        session_factory=factory,
    )
    work = _work_document(root)
    _create_claim(
        service,
        work_id=WORK_ID,
        work_document=work,
        root=root,
    )
    changed = {**work, "extra": True}
    with pytest.raises(Conflict, match="another request"):
        service.create_or_resume_claim(
            work_id=WORK_ID,
            work_document=changed,
            work_document_sha256=hashlib.sha256(canonical_json_bytes(changed)).hexdigest(),
            principal=_principal(),
        )
    with pytest.raises(Conflict, match="root differs"):
        _create_claim(
            service,
            work_id="f" * 64,
            work_document={"format": "test"},
            root=CollectionRootIdentity(1, "9" * 64, "1" * 64),
        )


def test_fenced_restart_advances_generation_revokes_capabilities_and_clears_plan(
    tmp_path: Path,
    request: FixtureRequest,
) -> None:
    factory = _session_factory(tmp_path, request)
    root = _setup(factory)
    service = SqlAlchemyCollectionWorkflowService(
        cast(Any, object()),
        session_factory=factory,
    )
    work = _work_document(root)
    claim = _create_claim(
        service,
        work_id=WORK_ID,
        work_document=work,
        root=root,
    )
    claim_id = str(claim["id"])
    _seal_plan(service, claim_id)
    capability = _issue_capability(
        service,
        claim_id,
        root,
        audience="fixture.target/v1",
        actions=("read-inputs", "write-output"),
    )
    assert service.authenticate_capability(str(capability["token"])) is not None

    restarted = service.restart_claim(
        claim_id,
        fence=1,
        lease_seconds=600,
        principal=_principal(),
    )

    assert restarted["state"] == "active"
    assert restarted["fence"] == "2"
    assert restarted["plan"] is None
    assert service.authenticate_capability(str(capability["token"])) is None
    with pytest.raises(Conflict, match="fence is stale"):
        service.restart_claim(
            claim_id,
            fence=1,
            lease_seconds=600,
            principal=_principal(),
        )


def test_fenced_restart_refuses_an_existing_execution_output(
    tmp_path: Path,
    request: FixtureRequest,
) -> None:
    factory = _session_factory(tmp_path, request)
    root = _setup(factory)
    service = SqlAlchemyCollectionWorkflowService(
        cast(Any, object()),
        session_factory=factory,
    )
    work = _work_document(root)
    claim = _create_claim(
        service,
        work_id=WORK_ID,
        work_document=work,
        root=root,
    )
    claim_id = str(claim["id"])
    _seal_plan(service, claim_id)
    with factory() as session, session.begin():
        _collection(
            session,
            2,
            creator=f"processing:{EXECUTION_ID}",
            root="6" * 64,
            idempotency_key=EXECUTION_ID,
        )

    with pytest.raises(Conflict, match="owns an output collection or upload"):
        service.restart_claim(
            claim_id,
            fence=1,
            lease_seconds=600,
            principal=_principal(),
        )

    with factory() as session, session.begin():
        stored = session.get(CollectionProcessingClaimRecord, claim_id)
        assert stored is not None
        stored.expires_at = NOW
    with factory() as session:
        blockers = processing_claim_blockers(session, 1)
    assert blockers and claim_id in blockers[0]
    with pytest.raises(Conflict, match="owns an output collection or upload"):
        service.create_or_resume_claim(
            work_id=WORK_ID,
            work_document=work,
            work_document_sha256=hashlib.sha256(canonical_json_bytes(work)).hexdigest(),
            principal=_principal(),
        )


def test_expired_execution_upload_remains_a_deletion_blocker(
    tmp_path: Path,
    request: FixtureRequest,
) -> None:
    factory = _session_factory(tmp_path, request)
    root = _setup(factory)
    service = SqlAlchemyCollectionWorkflowService(
        cast(Any, object()),
        session_factory=factory,
    )
    work = _work_document(root)
    claim = _create_claim(
        service,
        work_id=WORK_ID,
        work_document=work,
        root=root,
    )
    claim_id = str(claim["id"])
    _seal_plan(service, claim_id)
    with factory() as session, session.begin():
        session.add(
            CollectionUploadRecord(
                collection_id=3,
                idempotency_key=EXECUTION_ID,
                creation_identity_sha256="a" * 64,
                initial_tag_set_identity=(
                    "d99a47346b904680a2b3182b3950c159b297a427edfd0c8a23124b7bfb296ed9"
                ),
                ingest_source=f"processing:{EXECUTION_ID}",
                encryption_format="age-v1-scrypt",
                passphrase_id="fixture-archive-key-v1",
                provenance_mode="omitted",
                provenance_omission_reason="fixture",
                provenance_identity=None,
                initiated_by_principal_id=f"processing:{EXECUTION_ID}",
                initiated_by_key_id="stove0-key",
                event_context_json=None,
                state="open",
                archive_store="hot",
                archive_incarnation_id=seed_storage_incarnation(session, "archive", "hot"),
                opened_at=NOW,
                last_activity_at=NOW,
                closed_at=None,
                archive_phase="planning",
                archive_phase_updated_at=NOW,
                archive_attempt_count=0,
                archive_next_attempt_at=None,
                archive_last_attempt_at=None,
                archive_failure=None,
                archive_storage_prefix="collections/3",
                planner_checkpoint_json="{}",
            )
        )
        stored = session.get(CollectionProcessingClaimRecord, claim_id)
        assert stored is not None
        stored.expires_at = NOW

    with factory() as session:
        blockers = processing_claim_blockers(session, 1)
    assert blockers and claim_id in blockers[0]
    with pytest.raises(Conflict, match="owns an output collection or upload"):
        service.create_or_resume_claim(
            work_id=WORK_ID,
            work_document=work,
            work_document_sha256=hashlib.sha256(canonical_json_bytes(work)).hexdigest(),
            principal=_principal(),
        )
    with pytest.raises(Conflict, match="owns an output collection or upload"):
        service.abandon_claim(
            claim_id,
            fence=1,
            reason="failed: output upload requires reconciliation",
            principal=_principal(),
        )

    renewed = service.renew_claim(
        claim_id,
        fence=1,
        lease_seconds=600,
        principal=_principal(),
    )
    assert renewed["fence"] == "1"
    capability = _issue_capability(
        service,
        claim_id,
        root,
        audience="fixture.target/v1",
        actions=("read-inputs", "write-output"),
    )
    assert service.authenticate_capability(str(capability["token"])) is not None


def test_fenced_abandonment_revokes_capabilities_and_unblocks_deletion(
    tmp_path: Path,
    request: FixtureRequest,
) -> None:
    factory = _session_factory(tmp_path, request)
    root = _setup(factory)
    service = SqlAlchemyCollectionWorkflowService(
        cast(Any, object()),
        session_factory=factory,
    )
    work = _work_document(root)
    claim = _create_claim(
        service,
        work_id=WORK_ID,
        work_document=work,
        root=root,
    )
    claim_id = str(claim["id"])
    capability = _issue_capability(
        service,
        claim_id,
        root,
        audience="fixture.observer/v1",
        actions=("read-inputs",),
    )
    assert service.authenticate_capability(str(capability["token"])) is not None
    with factory() as session:
        assert processing_claim_blockers(session, 1)

    abandoned = service.abandon_claim(
        claim_id,
        fence=1,
        reason="canceled: operator requested cancellation",
        principal=_principal(),
    )
    repeated = service.abandon_claim(
        claim_id,
        fence=1,
        reason="canceled: operator requested cancellation",
        principal=_principal(),
    )

    assert abandoned["state"] == "abandoned"
    assert isinstance(abandoned["abandoned_at"], str)
    assert abandoned["abandonment_reason"] == "canceled: operator requested cancellation"
    assert repeated == abandoned
    assert service.authenticate_capability(str(capability["token"])) is None
    with factory() as session:
        assert processing_claim_blockers(session, 1) == []
    with pytest.raises(Conflict, match="fence is stale"):
        service.abandon_claim(
            claim_id,
            fence=2,
            reason="canceled: operator requested cancellation",
            principal=_principal(),
        )
    with pytest.raises(Conflict, match="another reason"):
        service.abandon_claim(
            claim_id,
            fence=1,
            reason="failed: a different terminal outcome",
            principal=_principal(),
        )


def test_expired_claim_abandonment_is_fenced_against_a_restarted_generation(
    tmp_path: Path,
    request: FixtureRequest,
) -> None:
    factory = _session_factory(tmp_path, request)
    root = _setup(factory)
    service = SqlAlchemyCollectionWorkflowService(
        cast(Any, object()),
        session_factory=factory,
    )
    work = _work_document(root)
    digest = hashlib.sha256(canonical_json_bytes(work)).hexdigest()
    claim = _create_claim(
        service,
        work_id=WORK_ID,
        work_document=work,
        root=root,
    )
    claim_id = str(claim["id"])
    with factory() as session, session.begin():
        stored = session.get(CollectionProcessingClaimRecord, claim_id)
        assert stored is not None
        stored.expires_at = NOW

    restarted = service.create_or_resume_claim(
        work_id=WORK_ID,
        work_document=work,
        work_document_sha256=digest,
        principal=_principal(),
    )
    assert restarted["fence"] == "2"
    with pytest.raises(Conflict, match="fence is stale"):
        service.abandon_claim(
            claim_id,
            fence=1,
            reason="canceled: stale worker",
            principal=_principal(),
        )


def test_expired_current_generation_can_reconcile_terminal_abandonment(
    tmp_path: Path,
    request: FixtureRequest,
) -> None:
    factory = _session_factory(tmp_path, request)
    root = _setup(factory)
    service = SqlAlchemyCollectionWorkflowService(
        cast(Any, object()),
        session_factory=factory,
    )
    work = _work_document(root)
    claim = _create_claim(
        service,
        work_id=WORK_ID,
        work_document=work,
        root=root,
    )
    claim_id = str(claim["id"])
    with factory() as session, session.begin():
        stored = session.get(CollectionProcessingClaimRecord, claim_id)
        assert stored is not None
        stored.expires_at = NOW
    with factory() as session:
        assert processing_claim_blockers(session, 1) == []
    with pytest.raises(Conflict, match="not renewable"):
        service.renew_claim(
            claim_id,
            fence=1,
            lease_seconds=600,
            principal=_principal(),
        )

    abandoned = service.abandon_claim(
        claim_id,
        fence=1,
        reason="canceled: controller reconciliation after lease expiry",
        principal=_principal(),
    )
    assert abandoned["state"] == "abandoned"


def test_multiple_processing_outcomes_retain_outputs_and_authorize_retirement(
    tmp_path: Path,
    request: FixtureRequest,
) -> None:
    factory = _session_factory(tmp_path, request)
    root = _setup(factory)
    with factory() as session, session.begin():
        session.add(
            CollectionFileRecord(
                collection_id=1,
                path="camera/sidecar.json",
                bytes=2,
                sha256="9" * 64,
            )
        )
    service = SqlAlchemyCollectionWorkflowService(
        cast(Any, object()),
        session_factory=factory,
    )

    parent_work = {"format": "fixture-multi-output-work/v1", "inputs": [root.as_dict()]}
    parent_work_id = hashlib.sha256(canonical_json_bytes(parent_work)).hexdigest()
    parent = _create_claim(
        service,
        work_id=parent_work_id,
        work_document=parent_work,
        root=root,
        purpose="fixture-multi-output/v1",
    )
    parent_id = str(parent["id"])

    required_outcomes: list[CollectionProcessingOutcomeIdentity] = []

    def settle_output(
        *,
        outcome_id: str,
        execution_id: str,
        source_path: str,
        source_bytes: int,
        source_sha256: str,
        output_collection_id: int,
        output_path: str,
    ) -> None:
        work = {
            "format": "fixture-output-work/v1",
            "outcome": outcome_id,
            "inputs": [root.as_dict()],
        }
        work_id = hashlib.sha256(canonical_json_bytes(work)).hexdigest()
        claim = _create_claim(
            service,
            work_id=work_id,
            work_document=work,
            root=root,
            artifact=CollectionArtifactIdentity(
                collection=root,
                path=source_path,
                bytes=source_bytes,
                sha256=source_sha256,
            ),
            purpose="fixture-output/v1",
        )
        claim_id = str(claim["id"])
        controller_evidence = {
            "format": "fixture-controller-evidence/v1",
            "execution_envelope": {"execution_envelope_sha256": execution_id},
        }
        controller_evidence_sha256 = hashlib.sha256(
            canonical_json_bytes(controller_evidence)
        ).hexdigest()
        _seal_plan(
            service,
            claim_id,
            execution_id=execution_id,
            controller_evidence=controller_evidence,
            controller_evidence_sha256=controller_evidence_sha256,
            operation_id="fixture.copy/v1",
        )
        disposition_set = _seal_dispositions(
            service,
            claim_id,
            root=root,
            input_path=source_path,
            output_path=output_path,
        )
        derivation = CollectionDerivation(
            execution_id=execution_id,
            claim_id=claim_id,
            fence=1,
            recipe=RecipeIdentity("fixture.branch/v1", 1, "b" * 64),
            operation=OperationIdentity(
                "fixture.copy/v1", canonical_json_sha256(_operation_declaration("fixture.copy/v1"))
            ),
            input_set_sha256=cast(str, claim["inputs"]["identity"]["sha256"]),  # type: ignore[index]
            artifact_set_sha256=cast(
                str,
                claim["artifacts"]["identity"]["sha256"],  # type: ignore[index]
            ),
            execution_envelope_sha256=execution_id,
            execution_sha256="e" * 64,
            controller_evidence=controller_evidence,
            controller_evidence_sha256=controller_evidence_sha256,
            disposition_set=disposition_set,
        )
        with factory() as session, session.begin():
            _collection(
                session,
                output_collection_id,
                creator=f"processing:{execution_id}",
                root=str(output_collection_id) * 64,
                idempotency_key=execution_id,
            )
            session.add_all(
                [
                    CollectionFileRecord(
                        collection_id=output_collection_id,
                        path=output_path,
                        bytes=source_bytes,
                        sha256=source_sha256,
                    ),
                    CollectionFileRecord(
                        collection_id=output_collection_id,
                        path=DERIVATION_EVIDENCE_PATH,
                        bytes=len(derivation.to_json_bytes()),
                        sha256=derivation.sha256,
                    ),
                    CollectionFileRecord(
                        collection_id=output_collection_id,
                        path=PRODUCER_EVIDENCE_PATH,
                        bytes=1,
                        sha256="5" * 64,
                    ),
                    CollectionFileRecord(
                        collection_id=output_collection_id,
                        path=derivation_evidence_page_path("dispositions", 0),
                        bytes=1,
                        sha256="3" * 64,
                    ),
                    CollectionFileRecord(
                        collection_id=output_collection_id,
                        path=derivation_evidence_page_path("output-edges", 0),
                        bytes=1,
                        sha256="4" * 64,
                    ),
                ]
            )
            output_record = session.get(CollectionRecord, output_collection_id)
            assert output_record is not None
            output_record.file_count = 5
            output_record.file_bytes = source_bytes + len(derivation.to_json_bytes()) + 3
        service.settle_claim(
            claim_id,
            fence=1,
            output_collection_id=output_collection_id,
            derivation=derivation.as_dict(),
            outcome_claim_id=parent_id,
            outcome_fence=1,
            outcome_id=outcome_id,
            principal=_principal(),
        )
        required_outcomes.append(
            CollectionProcessingOutcomeIdentity(
                outcome_id=outcome_id,
                source_claim_id=claim_id,
                source_fence=1,
                execution_id=execution_id,
                result_kind="collection",
                output_collection=CollectionRootIdentity(
                    output_collection_id,
                    str(output_collection_id) * 64,
                    str(output_collection_id) * 64,
                ),
                derivation_sha256=derivation.sha256,
            )
        )

    settle_output(
        outcome_id="video-copy",
        execution_id="1" * 64,
        source_path="camera/input.mov",
        source_bytes=4,
        source_sha256="8" * 64,
        output_collection_id=2,
        output_path="video/output.mkv",
    )
    settle_output(
        outcome_id="sidecar-copy",
        execution_id="2" * 64,
        source_path="camera/sidecar.json",
        source_bytes=2,
        source_sha256="9" * 64,
        output_collection_id=3,
        output_path="metadata/sidecar.json",
    )
    with factory() as session:
        assert processing_claim_blockers(session, 2)
        assert processing_claim_blockers(session, 3)

    required = processing_outcome_set_identity(required_outcomes)
    settled = service.settle_claim_outcomes(
        parent_id,
        fence=1,
        outcomes_count=len(required_outcomes),
        outcomes_sha256=str(required["sha256"]),
        source_collection_retirement_policy="retire-after-settlement",
        source_collection_retirement_grace_seconds=0,
        principal=_principal(),
    )
    while settled["state"] == "active":
        assert service.process_due_outcome_sets(limit=1) == 1
        settled = service.settle_claim_outcomes(
            parent_id,
            fence=1,
            outcomes_count=len(required_outcomes),
            outcomes_sha256=str(required["sha256"]),
            source_collection_retirement_policy="retire-after-settlement",
            source_collection_retirement_grace_seconds=0,
            principal=_principal(),
        )
    assert settled["state"] == "settled"
    assert settled["outcome_settlement"] is not None
    outcome_authority = cast(dict[str, object], settled["outcomes"])["identity"]
    assert isinstance(outcome_authority, dict)
    page = service.list_claim_outcomes(
        parent_id,
        identity_sha256=cast(str, outcome_authority["sha256"]),
        start_ordinal=0,
        principal=_principal(),
    )
    outcomes = tuple(
        CollectionProcessingOutcomeIdentity.from_mapping(item)
        for item in cast(list[dict[str, object]], page["outcomes"])
    )
    assert [item.outcome_id for item in outcomes] == [
        "sidecar-copy",
        "video-copy",
    ]
    assert (
        service.begin_source_collection_retirement(parent_id, fence=1, principal=_principal())[
            "state"
        ]
        == "retiring"
    )


# #938: receipt possession in a controller is not Riverhog settlement authority.
def _effect_claim(
    service: SqlAlchemyCollectionWorkflowService,
    root: CollectionRootIdentity,
    *,
    marker: str = "effect",
    permit: bool = True,
    retire: bool = False,
    grace: int = 0,
) -> tuple[dict[str, Any], ExternalEffectSettlement]:
    work = {"format": "fixture-work/v1", "marker": marker, "inputs": [root.as_dict()]}
    claim = _create_claim(
        service, work_id=canonical_json_sha256(work), work_document=work, root=root
    )
    operation = {
        "id": "fixture.deliver/v1",
        "result_kind": "external-effect",
        "source_collection_retirement_permitted": permit,
        "opaque_semantics": {"fixture": "not interpreted by Riverhog"},
    }
    execution_id = canonical_json_sha256({"claim": claim["id"], "marker": marker})
    evidence = {"format": "fixture-controller-evidence/v1", "execution": execution_id}
    sealed = service.seal_claim_plan(
        str(claim["id"]),
        fence=1,
        execution_id=execution_id,
        controller_evidence=evidence,
        controller_evidence_sha256=canonical_json_sha256(evidence),
        operation_id=str(operation["id"]),
        operation_sha256=canonical_json_sha256(operation),
        operation_contract=operation,
        result_kind="external-effect",
        source_collection_retirement_policy="retire-after-settlement" if retire else "retain",
        source_collection_retirement_grace_seconds=grace,
        principal=_principal(),
    )
    plan = cast(dict[str, Any], sealed["plan"])
    receipt = {
        "format": "fixture-effect-receipt/v1",
        "job": execution_id,
        "result": {"verified": True, "opaque_destination": "fixture"},
    }
    service.record_dispositions(
        str(claim["id"]),
        fence=1,
        dispositions=(
            ArtifactDisposition(
                input_collection_id=root.collection_id,
                input_archive_root_sha256=root.archive_root_sha256,
                input_path="camera/input.mov",
                status="effect-applied",
                effect_receipt_sha256=canonical_json_sha256(receipt),
            ),
        ),
        principal=_principal(),
    )
    disposition_state = service.seal_disposition_set(
        str(claim["id"]), fence=1, principal=_principal()
    )
    while disposition_state["state"] == "sealing":
        service.process_due_disposition_sets(limit=10)
        disposition_state = service.get_disposition_set(str(claim["id"]), principal=_principal())
    assert disposition_state["state"] == "sealed"
    disposition_set = ArtifactDispositionSetIdentity.from_mapping(
        cast(dict[str, object], disposition_state["identity"])
    )
    document = ExternalEffectSettlement(
        claim_id=str(claim["id"]),
        fence=1,
        execution_id=execution_id,
        execution_sha256=canonical_json_sha256({"attempt": marker}),
        operation=OperationIdentity(str(operation["id"]), canonical_json_sha256(operation)),
        input_set_sha256=str(plan["inputs"]["sha256"]),
        artifact_set_sha256=str(plan["artifacts"]["sha256"]),
        controller_evidence_sha256=str(plan["controller_evidence_sha256"]),
        receipt=receipt,
        receipt_sha256=canonical_json_sha256(receipt),
        disposition_set=disposition_set,
    )
    return cast(dict[str, Any], sealed), document


def _settle_effect(
    service: SqlAlchemyCollectionWorkflowService, document: ExternalEffectSettlement, **outcome: Any
) -> dict[str, object]:
    return service.settle_claim_effect(
        document.claim_id,
        fence=document.fence,
        settlement=document.as_dict(),
        principal=_principal(),
        **outcome,
    )


def _effect_outcome(
    label: str, document: ExternalEffectSettlement
) -> CollectionProcessingOutcomeIdentity:
    return CollectionProcessingOutcomeIdentity(
        outcome_id=label,
        source_claim_id=document.claim_id,
        source_fence=document.fence,
        execution_id=document.execution_id,
        result_kind="external-effect",
        effect_receipt_sha256=document.receipt_sha256,
        effect_settlement_sha256=document.sha256,
    )


def _no_output_claim(
    service: SqlAlchemyCollectionWorkflowService,
    root: CollectionRootIdentity,
    *,
    approved_loss: bool,
    artifact: CollectionArtifactIdentity | None = None,
    retire: bool = True,
    upload_evidence: bool = True,
    endorse_loss: bool | None = None,
    observation_verdict: object = True,
) -> NoOutputSettlement:
    selected = artifact or _artifact(root)
    work = {"format": "fixture-work/v1", "marker": "no-output", "inputs": [root.as_dict()]}
    claim = _create_claim(
        service,
        work_id=canonical_json_sha256(work),
        work_document=work,
        root=root,
        artifact=selected,
    )
    rule = {"id": "fixture.discard/v1", "verdict": "considered"}
    rule_sha256 = canonical_json_sha256(rule)
    operation: dict[str, object] = {
        "id": "fixture.no-output/v1",
        "result_kind": "no-output",
        "source_collection_retirement_permitted": True,
    }
    if approved_loss:
        operation["source_loss"] = {
            "rule_sha256": rule_sha256,
            "rule": rule,
            "evidence_slots": [
                {
                    "id": "fixture.observation/v1",
                    "contract_sha256": "6" * 64,
                    "profile_sha256": "5" * 64,
                }
            ],
        }
    execution_id = canonical_json_sha256({"claim": claim["id"], "kind": "no-output"})
    controller_evidence = {"format": "fixture-controller-evidence/v1", "execution": execution_id}
    sealed = service.seal_claim_plan(
        str(claim["id"]),
        fence=1,
        execution_id=execution_id,
        controller_evidence=controller_evidence,
        controller_evidence_sha256=canonical_json_sha256(controller_evidence),
        operation_id=str(operation["id"]),
        operation_sha256=canonical_json_sha256(operation),
        operation_contract=operation,
        result_kind="no-output",
        source_collection_retirement_policy="retire-after-settlement" if retire else "retain",
        source_collection_retirement_grace_seconds=0,
        principal=_principal(),
    )
    approval = None
    if approved_loss:
        subject = selected.as_dict()
        request_body = {
            "work_id": canonical_json_sha256(work),
            "observer_contract_id": "fixture.observation/v1",
            "observer_contract_sha256": "6" * 64,
            "subjects": [subject],
        }
        request = {**request_body, "request_id": canonical_json_sha256(request_body)}
        result_body = {
            "request_id": request["request_id"],
            "observer_contract_id": "fixture.observation/v1",
            "observer_contract_sha256": "6" * 64,
            "subjects": [subject],
            "facts_schema": {"profile_sha256": "5" * 64},
            "facts": {"considered": observation_verdict},
            "facts_sha256": canonical_json_sha256({"considered": observation_verdict}),
            "state": "observed",
        }
        result = {**result_body, "result_sha256": canonical_json_sha256(result_body)}
        observation = {"request": request, "result": result}
        observation_sha256 = canonical_json_sha256(observation)
        if upload_evidence:
            service.record_consideration_evidence(
                str(claim["id"]),
                fence=1,
                document=observation,
                sha256=observation_sha256,
                principal=_principal(),
            )
        evidence = {
            "format": "riverhog-artifact-consideration/v1",
            "subject": subject,
            "slots": [
                {
                    "id": "fixture.observation/v1",
                    "contract_sha256": "6" * 64,
                    "profile_sha256": "5" * 64,
                    "document_sha256": observation_sha256,
                }
            ],
            "reason": "The selected rule found no successor necessary.",
        }
        if endorse_loss is not False:
            approval = ArtifactDiscardApproval(
                controller_id="stove0",
                rule_sha256=rule_sha256,
                evidence_json=canonical_json_bytes(evidence).decode("utf-8"),
                evidence_sha256=canonical_json_sha256(evidence),
            )
    service.record_dispositions(
        str(claim["id"]),
        fence=1,
        dispositions=(
            ArtifactDisposition(
                input_collection_id=root.collection_id,
                input_archive_root_sha256=root.archive_root_sha256,
                input_path=selected.path,
                status="not-carried-forward",
                code="fixture.no-successor/v1",
                message="The decision produced no material output.",
                discard_approval=approval,
            ),
        ),
        principal=_principal(),
    )
    state = service.seal_disposition_set(str(claim["id"]), fence=1, principal=_principal())
    while state["state"] == "sealing":
        service.process_due_disposition_sets(limit=10)
        state = service.get_disposition_set(str(claim["id"]), principal=_principal())
    assert state["state"] == "sealed"
    plan = cast(dict[str, Any], sealed["plan"])
    decision = {"format": "fixture-no-output-decision/v1", "applies": True}
    return NoOutputSettlement(
        claim_id=str(claim["id"]),
        fence=1,
        execution_id=execution_id,
        operation=OperationIdentity(str(operation["id"]), canonical_json_sha256(operation)),
        input_set_sha256=str(plan["inputs"]["sha256"]),
        artifact_set_sha256=str(plan["artifacts"]["sha256"]),
        controller_evidence_sha256=str(plan["controller_evidence_sha256"]),
        decision=decision,
        decision_sha256=canonical_json_sha256(decision),
        disposition_set=ArtifactDispositionSetIdentity.from_mapping(
            cast(dict[str, object], state["identity"])
        ),
    )


def test_source_loss_approval_requires_retained_exact_observation(
    tmp_path: Path, request: FixtureRequest
) -> None:
    factory = _session_factory(tmp_path, request)
    root = _setup(factory)
    service = SqlAlchemyCollectionWorkflowService(cast(Any, object()), session_factory=factory)
    with pytest.raises(Conflict, match="lacks exact successful evidence"):
        _no_output_claim(service, root, approved_loss=True, upload_evidence=False)


def test_mismatched_no_output_verdict_cannot_retire_source(
    tmp_path: Path, request: FixtureRequest
) -> None:
    factory = _session_factory(tmp_path, request)
    root = _setup(factory)
    service = SqlAlchemyCollectionWorkflowService(cast(Any, object()), session_factory=factory)
    subject = _artifact(root)
    slot = RecipeSourceLossEvidenceSlot(
        observation_contract_id="fixture.observation/v1",
        observation_contract_sha256="6" * 64,
        facts_profile_sha256="5" * 64,
        artifact_facts=ArtifactFactBinding(records_pointer="/records"),
        verdict_pointer="/considered",
        verdict_value=True,
    )
    rule = RecipeSourceLossRule(id="fixture.discard/v1", evidence_slots=(slot,))
    approval = _no_output_discard_approval(
        subject,
        rule,
        cast(Any, SimpleNamespace(observations=())),
        controller_id="stove0",
        reason="No successor is needed.",
        index={
            (slot.observation_contract_id, slot.facts_profile_sha256, subject): [
                ("9" * 64, [{"considered": 1}])
            ]
        },
    )
    assert approval is None

    document = _no_output_claim(
        service,
        root,
        approved_loss=True,
        endorse_loss=approval is not None,
        observation_verdict=1,
    )
    settled = service.settle_claim_no_output(
        document.claim_id, fence=1, settlement=document.as_dict(), principal=_principal()
    )
    assert settled["state"] == "settled"
    with pytest.raises(Conflict, match="lacks a verified safe disposition"):
        service.begin_source_collection_retirement(
            document.claim_id, fence=1, principal=_principal()
        )
    with factory() as session:
        assert session.get(CollectionRecord, root.collection_id) is not None
        assert session.get(CollectionFileRecord, (root.collection_id, subject.path)) is not None


@pytest.mark.parametrize("approved_loss", [False, True])
def test_no_output_settlement_uses_shared_retirement_coverage(
    tmp_path: Path, request: FixtureRequest, approved_loss: bool
) -> None:
    factory = _session_factory(tmp_path, request)
    root = _setup(factory)
    service = SqlAlchemyCollectionWorkflowService(cast(Any, object()), session_factory=factory)
    document = _no_output_claim(service, root, approved_loss=approved_loss)
    settled = service.settle_claim_no_output(
        document.claim_id, fence=1, settlement=document.as_dict(), principal=_principal()
    )
    assert settled["state"] == "settled"
    assert settled["no_output_settlement_sha256"] == document.sha256
    assert (
        service.settle_claim_no_output(
            document.claim_id, fence=1, settlement=document.as_dict(), principal=_principal()
        )["settled_at"]
        == settled["settled_at"]
    )
    if approved_loss:
        assert (
            service.begin_source_collection_retirement(
                document.claim_id, fence=1, principal=_principal()
            )["state"]
            == "retiring"
        )
        with factory() as session:
            retained = session.scalar(
                select(CollectionProcessingConsiderationEvidenceRecord).where(
                    CollectionProcessingConsiderationEvidenceRecord.claim_id == document.claim_id
                )
            )
            assert retained is not None
            evidence_sha256 = retained.sha256
            evidence_document = json.loads(retained.document_json)
        assert service.record_consideration_evidence(
            document.claim_id,
            fence=1,
            document=evidence_document,
            sha256=evidence_sha256,
            principal=_principal(),
        ) == {"sha256": evidence_sha256}
        with factory() as session, session.begin():
            session.execute(
                delete(CollectionArchiveObjectRecord).where(
                    CollectionArchiveObjectRecord.collection_id == 1
                )
            )
            session.execute(
                delete(CollectionArchiveCopyRecord).where(
                    CollectionArchiveCopyRecord.collection_id == 1
                )
            )
            session.execute(
                delete(CollectionFileRecord).where(CollectionFileRecord.collection_id == 1)
            )
            session.execute(delete(CollectionRecord).where(CollectionRecord.id == 1))
        with factory() as session:
            assert (
                session.get(
                    CollectionProcessingConsiderationEvidenceRecord,
                    (document.claim_id, evidence_sha256),
                )
                is not None
            )
            assert (
                session.scalar(
                    select(CollectionProcessingConsiderationSubjectRecord).where(
                        CollectionProcessingConsiderationSubjectRecord.claim_id == document.claim_id
                    )
                )
                is not None
            )
        assert service.get_consideration_evidence(
            document.claim_id,
            sha256=evidence_sha256,
            principal=_principal(),
        ) == {"sha256": evidence_sha256, "document": evidence_document}
    else:
        with pytest.raises(Conflict, match="lacks a verified safe disposition"):
            service.begin_source_collection_retirement(
                document.claim_id, fence=1, principal=_principal()
            )


def _close_outcomes(
    service: SqlAlchemyCollectionWorkflowService,
    parent: str,
    outcomes: tuple[CollectionProcessingOutcomeIdentity, ...],
    *,
    retire: bool = True,
    grace: int = 0,
) -> dict[str, object]:
    identity = processing_outcome_set_identity(outcomes)
    for _ in range(5):
        state = service.settle_claim_outcomes(
            parent,
            fence=1,
            outcomes_count=len(outcomes),
            outcomes_sha256=str(identity["sha256"]),
            source_collection_retirement_policy="retire-after-settlement" if retire else "retain",
            source_collection_retirement_grace_seconds=grace,
            principal=_principal(),
        )
        if state["state"] != "active":
            return state
        service.process_due_outcome_sets(limit=10)
    raise AssertionError("bounded fixture outcome set did not settle")


def test_external_effect_commit_replays_across_service_restart_and_deletion(
    tmp_path: Path,
    request: FixtureRequest,
) -> None:
    factory = _session_factory(tmp_path, request)
    root = _setup(factory)
    service = SqlAlchemyCollectionWorkflowService(cast(Any, object()), session_factory=factory)
    claim, document = _effect_claim(service, root, retire=True)
    capability = _issue_capability(
        service, document.claim_id, root, audience="fixture.effect/v1", actions=("read-inputs",)
    )
    target = service.authenticate_capability(str(capability["token"]))
    assert target is not None and target.id == f"claim:{document.claim_id}"
    with pytest.raises((Forbidden, NotFound)):
        service.settle_claim_effect(
            document.claim_id, fence=1, settlement=document.as_dict(), principal=target
        )
    with pytest.raises((Conflict, Forbidden)):
        _issue_capability(
            service,
            document.claim_id,
            root,
            audience="fixture.effect/v1",
            actions=("read-inputs", "write-output"),
        )
    with pytest.raises(Conflict):
        service.begin_source_collection_retirement(
            document.claim_id, fence=1, principal=_principal()
        )

    committed = _settle_effect(service, document)
    assert committed["state"] == "settled" and committed["output_collection_id"] is None
    assert committed["effect_settlement_sha256"] == document.sha256
    assert service.authenticate_capability(str(capability["token"])) is None
    with factory() as session:
        retained = session.get(CollectionProcessingEffectSettlementRecord, document.claim_id)
        assert retained is not None and retained.document_sha256 == document.sha256
        assert retained.receipt_sha256 == document.receipt_sha256
        assert processing_claim_blockers(session, 1)
    # The controller died after the Riverhog commit, before persisting its ACK.
    restarted = SqlAlchemyCollectionWorkflowService(cast(Any, object()), session_factory=factory)
    assert _settle_effect(restarted, document)["settled_at"] == committed["settled_at"]
    assert (
        restarted.begin_source_collection_retirement(
            document.claim_id, fence=1, principal=_principal()
        )["state"]
        == "retiring"
    )
    with pytest.raises(Conflict):
        restarted.release_claim(document.claim_id, fence=1, principal=_principal())
    # Simulate the deletion worker having committed catalog removal then losing its ACK.
    # Real deletion-worker blocker/partial deletion coverage lives in integration tests.
    with factory() as session, session.begin():
        session.execute(
            delete(CollectionArchiveObjectRecord).where(
                CollectionArchiveObjectRecord.collection_id == 1
            )
        )
        session.execute(
            delete(CollectionArchiveCopyRecord).where(
                CollectionArchiveCopyRecord.collection_id == 1
            )
        )
        session.execute(delete(CollectionFileRecord).where(CollectionFileRecord.collection_id == 1))
        session.execute(delete(CollectionRecord).where(CollectionRecord.id == 1))
    resumed = SqlAlchemyCollectionWorkflowService(cast(Any, object()), session_factory=factory)
    assert _settle_effect(resumed, document)["state"] == "retiring"
    assert (
        resumed.begin_source_collection_retirement(
            document.claim_id, fence=1, principal=_principal()
        )["state"]
        == "retiring"
    )
    assert (
        resumed.release_claim(document.claim_id, fence=1, principal=_principal())["state"]
        == "released"
    )
    assert _settle_effect(resumed, document)["state"] == "released"
    with factory() as session:
        assert (
            session.get(CollectionProcessingEffectSettlementRecord, document.claim_id) is not None
        )


@pytest.mark.parametrize(
    "field",
    [
        "claim_id",
        "execution_id",
        "input_set_sha256",
        "artifact_set_sha256",
        "controller_evidence_sha256",
        "operation",
        "fence",
        "receipt_sha256",
        "status",
    ],
)
def test_effect_settlement_rejects_every_mismatched_authority(
    tmp_path: Path,
    request: FixtureRequest,
    field: str,
) -> None:
    factory = _session_factory(tmp_path, request)
    root = _setup(factory)
    service = SqlAlchemyCollectionWorkflowService(cast(Any, object()), session_factory=factory)
    _, document = _effect_claim(service, root, retire=True)
    changed = document.as_dict()
    changed[field] = (
        {"id": "other/v1", "sha256": "f" * 64}
        if field == "operation"
        else "2"
        if field == "fence"
        else "interrupted"
        if field == "status"
        else "f" * 64
    )
    with pytest.raises((BadRequest, Conflict)):
        service.settle_claim_effect(
            document.claim_id, fence=1, settlement=changed, principal=_principal()
        )
    assert service.get_claim(document.claim_id, principal=_principal())["state"] == "active"
    with factory() as session:
        assert session.get(CollectionProcessingEffectSettlementRecord, document.claim_id) is None
        assert session.get(CollectionRecord, 1) is not None


def test_effect_receipt_identity_cannot_be_rebound_and_replay_cannot_change_outcome_parent(
    tmp_path: Path,
    request: FixtureRequest,
) -> None:
    factory = _session_factory(tmp_path, request)
    root = _setup(factory)
    service = SqlAlchemyCollectionWorkflowService(cast(Any, object()), session_factory=factory)
    _, first = _effect_claim(service, root, marker="one")
    _, second = _effect_claim(service, root, marker="two")
    _settle_effect(service, first)
    rebound = {
        **second.as_dict(),
        "receipt": first.as_dict()["receipt"],
        "receipt_sha256": first.receipt_sha256,
    }
    with pytest.raises(Conflict, match="receipt"):
        service.settle_claim_effect(
            second.claim_id, fence=1, settlement=rebound, principal=_principal()
        )
    changed = {**first.as_dict(), "execution_sha256": "f" * 64}
    with pytest.raises(Conflict, match="different evidence"):
        service.settle_claim_effect(
            first.claim_id, fence=1, settlement=changed, principal=_principal()
        )
    with pytest.raises(Conflict):
        _settle_effect(
            service, first, outcome_claim_id=second.claim_id, outcome_fence=1, outcome_id="new"
        )


@pytest.mark.parametrize("permit", [True, False])
def test_operation_permission_never_requests_retirement(
    tmp_path: Path, request: FixtureRequest, permit: bool
) -> None:
    factory = _session_factory(tmp_path, request)
    root = _setup(factory)
    service = SqlAlchemyCollectionWorkflowService(cast(Any, object()), session_factory=factory)
    _, document = _effect_claim(service, root, permit=permit)
    _settle_effect(service, document)
    with pytest.raises(Conflict, match="not eligible"):
        service.begin_source_collection_retirement(
            document.claim_id, fence=1, principal=_principal()
        )
    with pytest.raises((BadRequest, Conflict)):
        _effect_claim(service, root, marker="unsafe", permit=False, retire=True)
    assert service.get_claim(document.claim_id, principal=_principal())["state"] == "settled"


def test_unsettled_effect_never_restarts_or_stops_blocking_on_lease_expiry(
    tmp_path: Path,
    request: FixtureRequest,
) -> None:
    factory = _session_factory(tmp_path, request)
    root = _setup(factory)
    service = SqlAlchemyCollectionWorkflowService(cast(Any, object()), session_factory=factory)
    claim, document = _effect_claim(service, root, retire=True)
    with factory() as session, session.begin():
        row = session.get(CollectionProcessingClaimRecord, document.claim_id)
        assert row is not None
        row.expires_at = NOW
    with pytest.raises(Conflict):
        service.restart_claim(document.claim_id, fence=1, lease_seconds=600, principal=_principal())
    with pytest.raises(Conflict):
        service.create_or_resume_claim(
            work_id=str(claim["work_id"]),
            work_document=claim["work_document"],
            work_document_sha256=str(claim["work_document_sha256"]),
            principal=_principal(),
        )
    with pytest.raises(Conflict):
        service.begin_source_collection_retirement(
            document.claim_id, fence=1, principal=_principal()
        )
    with factory() as session:
        assert processing_claim_blockers(session, 1)
        assert session.get(CollectionRecord, 1) is not None
    # Only an exact reconciled successful receipt can advance the old generation.
    assert _settle_effect(service, document)["state"] == "settled"


def test_effect_retirement_requires_complete_inventory_and_grace(
    tmp_path: Path,
    request: FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = _session_factory(tmp_path, request)
    root = _setup(factory)
    with factory() as session, session.begin():
        session.add_all(
            [
                CollectionFileRecord(
                    collection_id=1, path="camera/not-selected.mov", bytes=3, sha256="7" * 64
                ),
                CollectionFileRecord(
                    collection_id=1,
                    path="riverhog/not-control.json",
                    bytes=3,
                    sha256="6" * 64,
                ),
            ]
        )
    service = SqlAlchemyCollectionWorkflowService(cast(Any, object()), session_factory=factory)
    _, document = _effect_claim(service, root, retire=True, grace=60)
    settled = _settle_effect(service, document)
    with pytest.raises(Conflict, match="lacks a verified safe disposition"):
        service.begin_source_collection_retirement(
            document.claim_id, fence=1, principal=_principal()
        )
    # A separate fixture removes the uncovered entry, without pretending a partial
    # execution can rewrite its sealed scope. Catalog tampering is not a workflow API.
    with factory() as session, session.begin():
        session.execute(
            delete(CollectionFileRecord).where(
                CollectionFileRecord.path == "camera/not-selected.mov"
            )
        )
    with pytest.raises(Conflict, match="lacks a verified safe disposition"):
        service.begin_source_collection_retirement(
            document.claim_id, fence=1, principal=_principal()
        )
    with factory() as session, session.begin():
        session.execute(
            delete(CollectionFileRecord).where(
                CollectionFileRecord.path == "riverhog/not-control.json"
            )
        )
    t0 = parse_utc_timestamp(str(settled["settled_at"]))
    monkeypatch.setattr(
        "riverhog_core.services.collection_workflows.utc_epoch_ns_now", lambda: t0 + 59_000_000_000
    )
    assert (
        service.begin_source_collection_retirement(
            document.claim_id, fence=1, principal=_principal()
        )["state"]
        == "settled"
    )
    assert _settle_effect(service, document)["settled_at"] == settled["settled_at"]
    monkeypatch.setattr(
        "riverhog_core.services.collection_workflows.utc_epoch_ns_now", lambda: t0 + 60_000_000_000
    )
    assert (
        service.begin_source_collection_retirement(
            document.claim_id, fence=1, principal=_principal()
        )["state"]
        == "retiring"
    )


def test_effect_only_coordination_requires_exact_success_set_and_every_operation_permission(
    tmp_path: Path,
    request: FixtureRequest,
) -> None:
    factory = _session_factory(tmp_path, request)
    root = _setup(factory)
    service = SqlAlchemyCollectionWorkflowService(cast(Any, object()), session_factory=factory)
    parent = _create_claim(service, work_id=WORK_ID, work_document=_work_document(root), root=root)
    parent_id = str(parent["id"])
    _, first = _effect_claim(service, root, marker="permitted")
    _, second = _effect_claim(service, root, marker="not-permitted", permit=False)
    one, two = _effect_outcome("branch/a", first), _effect_outcome("branch/b", second)
    with pytest.raises(Conflict, match="settled source"):
        service.append_claim_outcomes(parent_id, fence=1, outcomes=(one,), principal=_principal())
    _settle_effect(
        service, first, outcome_claim_id=parent_id, outcome_fence=1, outcome_id="branch/a"
    )
    with pytest.raises(Conflict, match="exact required set"):
        _close_outcomes(service, parent_id, (one, two))
    _settle_effect(
        service, second, outcome_claim_id=parent_id, outcome_fence=1, outcome_id="branch/b"
    )
    result = _close_outcomes(service, parent_id, (one, two))
    assert result["state"] == "settled"
    with pytest.raises(Conflict, match="every required settled operation"):
        service.begin_source_collection_retirement(parent_id, fence=1, principal=_principal())
    with pytest.raises(Conflict, match="required processing outcomes"):
        _close_outcomes(service, parent_id, (one,))
    assert (
        _settle_effect(
            service, first, outcome_claim_id=parent_id, outcome_fence=1, outcome_id="branch/a"
        )["state"]
        == "settled"
    )
    # Parent closure is replayable, but no new rows or changed authority may appear.
    service.append_claim_outcomes(parent_id, fence=1, outcomes=(one, two), principal=_principal())
    assert _close_outcomes(service, parent_id, (one, two))["outcomes"] == result["outcomes"]


def test_retirement_refuses_a_corrupted_retained_effect_and_input_root(
    tmp_path: Path,
    request: FixtureRequest,
) -> None:
    factory = _session_factory(tmp_path, request)
    root = _setup(factory)
    service = SqlAlchemyCollectionWorkflowService(cast(Any, object()), session_factory=factory)
    _, document = _effect_claim(service, root, retire=True)
    _settle_effect(service, document)
    with factory() as session, session.begin():
        effect = session.get(CollectionProcessingEffectSettlementRecord, document.claim_id)
        assert effect is not None
        effect.document_sha256 = "f" * 64
    with pytest.raises(Conflict, match="retained processing authority"):
        service.begin_source_collection_retirement(
            document.claim_id, fence=1, principal=_principal()
        )
    with factory() as session, session.begin():
        effect = session.get(CollectionProcessingEffectSettlementRecord, document.claim_id)
        assert effect is not None
        effect.document_sha256 = document.sha256
        collection = session.get(CollectionRecord, 1)
        assert collection is not None
        collection.content_identity = "f" * 64
    with pytest.raises(Conflict):
        service.begin_source_collection_retirement(
            document.claim_id, fence=1, principal=_principal()
        )


def _settled_collection_child(
    service: SqlAlchemyCollectionWorkflowService,
    factory: SessionFactory,
    root: CollectionRootIdentity,
) -> CollectionProcessingOutcomeIdentity:
    work = {"kind": "fixture-collection-result", "inputs": [root.as_dict()]}
    child = _create_claim(
        service, work_id=canonical_json_sha256(work), work_document=work, root=root
    )
    child_id = str(child["id"])
    _seal_plan(service, child_id)
    dispositions = _seal_dispositions(
        service, child_id, root=root, input_path="camera/input.mov", output_path="out/result.bin"
    )
    derivation = CollectionDerivation(
        execution_id=EXECUTION_ID,
        claim_id=child_id,
        fence=1,
        recipe=RecipeIdentity("fixture.recipe/v1", 1, "b" * 64),
        operation=OperationIdentity(
            "archive-video/v1", canonical_json_sha256(_operation_declaration("archive-video/v1"))
        ),
        input_set_sha256=cast(Any, child)["inputs"]["identity"]["sha256"],
        artifact_set_sha256=cast(Any, child)["artifacts"]["identity"]["sha256"],
        execution_envelope_sha256=EXECUTION_ID,
        execution_sha256="e" * 64,
        controller_evidence=CONTROLLER_EVIDENCE,
        controller_evidence_sha256=CONTROLLER_EVIDENCE_SHA256,
        disposition_set=dispositions,
    )
    files = [
        ("out/result.bin", 4, "7" * 64),
        (DERIVATION_EVIDENCE_PATH, len(derivation.to_json_bytes()), derivation.sha256),
        (PRODUCER_EVIDENCE_PATH, 1, "5" * 64),
        (derivation_evidence_page_path("dispositions", 0), 1, "3" * 64),
        (derivation_evidence_page_path("output-edges", 0), 1, "4" * 64),
    ]
    with factory() as session, session.begin():
        _collection(
            session,
            2,
            creator=f"processing:{EXECUTION_ID}",
            root="6" * 64,
            idempotency_key=EXECUTION_ID,
        )
        session.add_all(
            CollectionFileRecord(collection_id=2, path=path, bytes=size, sha256=digest)
            for path, size, digest in files
        )
        output = session.get(CollectionRecord, 2)
        assert output is not None
        output.file_count = len(files)
        output.file_bytes = sum(item[1] for item in files)
    service.settle_claim(
        child_id,
        fence=1,
        output_collection_id=2,
        derivation=derivation.as_dict(),
        principal=_principal(),
    )
    service.release_claim(child_id, fence=1, principal=_principal())
    return CollectionProcessingOutcomeIdentity(
        outcome_id="branch/collection",
        source_claim_id=child_id,
        source_fence=1,
        execution_id=EXECUTION_ID,
        result_kind="collection",
        output_collection=CollectionRootIdentity(2, "6" * 64, "2" * 64),
        derivation_sha256=derivation.sha256,
    )


@pytest.mark.parametrize("mixed", [False, True])
def test_effect_only_and_mixed_exact_sets_authorize_retirement_after_all_settlements(
    tmp_path: Path,
    request: FixtureRequest,
    mixed: bool,
) -> None:
    factory = _session_factory(tmp_path, request)
    root = _setup(factory)
    service = SqlAlchemyCollectionWorkflowService(cast(Any, object()), session_factory=factory)
    parent = _create_claim(service, work_id=WORK_ID, work_document=_work_document(root), root=root)
    parent_id = str(parent["id"])
    _, effect = _effect_claim(service, root)
    _settle_effect(service, effect)
    service.release_claim(effect.claim_id, fence=1, principal=_principal())
    effects = (_effect_outcome("branch/effect", effect),)
    outcomes = (
        ((_settled_collection_child(service, factory, root),) + effects) if mixed else effects
    )
    service.append_claim_outcomes(parent_id, fence=1, outcomes=outcomes, principal=_principal())
    closed = _close_outcomes(service, parent_id, outcomes)
    identity = processing_outcome_set_identity(outcomes)
    assert cast(Any, closed)["outcomes"]["identity"] == identity
    page = service.list_claim_outcomes(
        parent_id, identity_sha256=str(identity["sha256"]), start_ordinal=0, principal=_principal()
    )
    assert page["outcomes"] == [
        item.as_dict() for item in sorted(outcomes, key=lambda item: item.outcome_id)
    ]
    restarted = SqlAlchemyCollectionWorkflowService(cast(Any, object()), session_factory=factory)
    assert _close_outcomes(restarted, parent_id, outcomes)["state"] == "settled"
    assert (
        restarted.begin_source_collection_retirement(parent_id, fence=1, principal=_principal())[
            "state"
        ]
        == "retiring"
    )
    with factory() as session:
        # Parent authorization is a retirement exemption, never blanket deletion permission.
        assert processing_claim_blockers(session, 1)
        assert not processing_claim_blockers(session, 1, exempt_claim_id=parent_id)


@pytest.mark.parametrize("material_kind", ["collection", "external-effect"])
@pytest.mark.parametrize("approved_loss", [False, True])
def test_mixed_result_retirement_uses_one_exact_disposition_denominator(
    tmp_path: Path,
    request: FixtureRequest,
    material_kind: str,
    approved_loss: bool,
) -> None:
    factory = _session_factory(tmp_path, request)
    root = _setup(factory)
    sidecar = CollectionArtifactIdentity(
        collection=root,
        path="camera/sidecar.json",
        bytes=2,
        sha256="9" * 64,
    )
    with factory() as session, session.begin():
        session.add(
            CollectionFileRecord(
                collection_id=1,
                path=sidecar.path,
                bytes=sidecar.bytes,
                sha256=sidecar.sha256,
            )
        )
    service = SqlAlchemyCollectionWorkflowService(cast(Any, object()), session_factory=factory)
    parent = _create_claim(service, work_id=WORK_ID, work_document=_work_document(root), root=root)
    parent_id = str(parent["id"])
    if material_kind == "collection":
        material = _settled_collection_child(service, factory, root)
    else:
        _, effect = _effect_claim(service, root)
        _settle_effect(service, effect)
        service.release_claim(effect.claim_id, fence=1, principal=_principal())
        material = _effect_outcome("branch/effect", effect)
    no_output = _no_output_claim(
        service, root, approved_loss=approved_loss, artifact=sidecar, retire=False
    )
    service.settle_claim_no_output(
        no_output.claim_id,
        fence=1,
        settlement=no_output.as_dict(),
        principal=_principal(),
    )
    service.release_claim(no_output.claim_id, fence=1, principal=_principal())
    empty = CollectionProcessingOutcomeIdentity(
        outcome_id="branch/no-output",
        source_claim_id=no_output.claim_id,
        source_fence=1,
        execution_id=no_output.execution_id,
        result_kind="no-output",
        no_output_settlement_sha256=no_output.sha256,
    )
    outcomes = (material, empty)
    service.append_claim_outcomes(parent_id, fence=1, outcomes=outcomes, principal=_principal())
    _close_outcomes(service, parent_id, outcomes)
    if approved_loss:
        assert (
            service.begin_source_collection_retirement(parent_id, fence=1, principal=_principal())[
                "state"
            ]
            == "retiring"
        )
    else:
        with pytest.raises(Conflict, match="lacks a verified safe disposition"):
            service.begin_source_collection_retirement(parent_id, fence=1, principal=_principal())


def test_wrong_exact_set_digest_fails_closed_in_background_seal(
    tmp_path: Path, request: FixtureRequest
) -> None:
    factory = _session_factory(tmp_path, request)
    root = _setup(factory)
    service = SqlAlchemyCollectionWorkflowService(cast(Any, object()), session_factory=factory)
    parent = _create_claim(service, work_id=WORK_ID, work_document=_work_document(root), root=root)
    _, effect = _effect_claim(service, root)
    _settle_effect(service, effect)
    service.append_claim_outcomes(
        str(parent["id"]),
        fence=1,
        outcomes=(_effect_outcome("branch/effect", effect),),
        principal=_principal(),
    )
    service.settle_claim_outcomes(
        str(parent["id"]),
        fence=1,
        outcomes_count=1,
        outcomes_sha256="f" * 64,
        source_collection_retirement_policy="retire-after-settlement",
        source_collection_retirement_grace_seconds=0,
        principal=_principal(),
    )
    service.process_due_outcome_sets(limit=10)
    current = cast(Any, service.get_claim(str(parent["id"]), principal=_principal()))
    assert current["state"] == "active" and current["outcomes"]["state"] == "failed"
    with pytest.raises(Conflict):
        service.begin_source_collection_retirement(
            str(parent["id"]), fence=1, principal=_principal()
        )


def test_nested_coordinator_adopts_verified_leaf_effect_not_a_local_parent_receipt(
    tmp_path: Path,
    request: FixtureRequest,
) -> None:
    factory = _session_factory(tmp_path, request)
    root = _setup(factory)
    service = SqlAlchemyCollectionWorkflowService(cast(Any, object()), session_factory=factory)
    parent = _create_claim(service, work_id=WORK_ID, work_document=_work_document(root), root=root)
    nested_work = {"kind": "nested", "inputs": [root.as_dict()]}
    nested = _create_claim(
        service, work_id=canonical_json_sha256(nested_work), work_document=nested_work, root=root
    )
    _, effect = _effect_claim(service, root)
    leaf = _effect_outcome("branch/effect", effect)
    _settle_effect(
        service,
        effect,
        outcome_claim_id=str(nested["id"]),
        outcome_fence=1,
        outcome_id=leaf.outcome_id,
    )
    _close_outcomes(service, str(nested["id"]), (leaf,), retire=False)
    adopted = CollectionProcessingOutcomeIdentity.from_mapping(
        {**leaf.as_dict(), "outcome_id": "nested/leaf"}
    )
    service.append_claim_outcomes(
        str(parent["id"]), fence=1, outcomes=(adopted,), principal=_principal()
    )
    assert _close_outcomes(service, str(parent["id"]), (adopted,))["state"] == "settled"
    # Adding a second parent projection must not change the original settlement binding.
    assert (
        _settle_effect(
            service,
            effect,
            outcome_claim_id=str(nested["id"]),
            outcome_fence=1,
            outcome_id=leaf.outcome_id,
        )["state"]
        == "settled"
    )


def test_outcome_append_crosses_transport_boundary_and_replays_after_restart(
    tmp_path: Path,
    request: FixtureRequest,
) -> None:
    factory = _session_factory(tmp_path, request)
    root = _setup(factory)
    service = SqlAlchemyCollectionWorkflowService(cast(Any, object()), session_factory=factory)
    parent = _create_claim(service, work_id=WORK_ID, work_document=_work_document(root), root=root)
    parent_id = str(parent["id"])
    outcomes = []
    for ordinal in range(129):
        _, effect = _effect_claim(service, root, marker=f"boundary-{ordinal:03d}")
        _settle_effect(service, effect)
        outcomes.append(_effect_outcome(f"branch/{ordinal:03d}", effect))
    expected = tuple(outcomes)
    service.append_claim_outcomes(
        parent_id, fence=1, outcomes=expected[:128], principal=_principal()
    )
    # A committed append with a lost response is replayed, not counted twice.
    restarted = SqlAlchemyCollectionWorkflowService(cast(Any, object()), session_factory=factory)
    restarted.append_claim_outcomes(
        parent_id, fence=1, outcomes=expected[:128], principal=_principal()
    )
    restarted.append_claim_outcomes(
        parent_id, fence=1, outcomes=expected[128:], principal=_principal()
    )
    identity = processing_outcome_set_identity(expected)
    restarted.settle_claim_outcomes(
        parent_id,
        fence=1,
        outcomes_count=len(expected),
        outcomes_sha256=str(identity["sha256"]),
        source_collection_retirement_policy="retain",
        source_collection_retirement_grace_seconds=0,
        principal=_principal(),
    )
    # Sealing advances in bounded durable steps, including across another restart.
    restarted.process_due_outcome_sets(limit=1)
    resumed = SqlAlchemyCollectionWorkflowService(cast(Any, object()), session_factory=factory)
    assert _close_outcomes(resumed, parent_id, expected, retire=False)["state"] == "settled"
    first = resumed.list_claim_outcomes(
        parent_id, identity_sha256=str(identity["sha256"]), start_ordinal=0, principal=_principal()
    )
    second = resumed.list_claim_outcomes(
        parent_id,
        identity_sha256=str(identity["sha256"]),
        start_ordinal=int(str(first["next_ordinal"])),
        principal=_principal(),
    )
    assert len(cast(list[Any], first["outcomes"])) == 128
    assert second["next_ordinal"] is None
    assert cast(list[Any], first["outcomes"]) + cast(list[Any], second["outcomes"]) == [
        item.as_dict() for item in expected
    ]
    # A sealed set still accepts an exact append replay, without opening its inventory.
    resumed.append_claim_outcomes(
        parent_id, fence=1, outcomes=expected[:128], principal=_principal()
    )
    assert (
        _close_outcomes(resumed, parent_id, expected, retire=False)["outcomes"]["identity"]
        == identity
    )
