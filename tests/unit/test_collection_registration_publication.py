from __future__ import annotations

import hashlib

import pytest
from riverhog_core.catalog_db import (
    dispose_session_factory,
    make_session_factory,
    session_scope,
)
from riverhog_core.catalog_models import (
    CollectionArchiveObjectUploadRecord,
    CollectionUploadArtifactRecord,
    CollectionUploadArtifactVolumeRecord,
    CollectionUploadRecord,
)
from riverhog_core.collection_plan import CollectionVolumePolicy
from riverhog_core.domain.archive import ArchiveArtifact, PackVolumePlan
from riverhog_core.incremental_emission import (
    VolumeEmissionFrontier,
    VolumeEmissionLimits,
    advance_volume_emission,
)
from riverhog_core.incremental_plan import (
    IncrementalVolumePlanBatch,
    OrderedArchiveArtifact,
    parse_incremental_volume_planner_checkpoint,
)
from riverhog_core.services import collection_uploads as owner
from riverhog_protocol import CollectionUploadRawDigestBatchDocument
from riverhog_protocol.errors import Conflict, NotFound
from riverhog_protocol.raw_ingress import ordered_raw_part_commitment
from sqlalchemy import delete, func, select, update
from sqlalchemy.orm import aliased

from tests.unit.test_collection_uploads import _CREATOR, _service_with_archive_objects

PART = 5 * 1024 * 1024


@pytest.mark.parametrize("prior_pending_pack", [False, True])
def test_staged_registration_rows_remain_hidden_from_service_and_orm_after_reopen(
    tmp_path, prior_pending_pack
):
    policy = CollectionVolumePolicy(
        pack_member_bytes=8,
        raw_volume_plaintext_bytes=PART,
        raw_part_plaintext_bytes=PART,
    )
    service, config, resumable, roots = _service_with_archive_objects(tmp_path, policy=policy)
    opened = service.create_or_resume(
        idempotency_key="hidden-registration",
        ingest_source="fixture",
        archive_store=None,
        initiator=_CREATOR,
        event_context=None,
    )
    collection_id = int(opened["collection_id"])
    prior_id = "1" * 64
    if prior_pending_pack:
        service.register_artifacts(
            collection_id,
            [{"artifact_id": prior_id, "bytes": "0", "sha256": hashlib.sha256(b"").hexdigest()}],
        )
    member_id = "2" * 64
    operation_id = "e" * 64
    size = 17 * PART - 1
    digest = hashlib.sha256(b"metadata-only fixture").hexdigest()
    factory = make_session_factory(config.database_url)
    with session_scope(factory) as session:
        session.info["include_unpublished_registration"] = True
        upload = session.get(CollectionUploadRecord, collection_id)
        original = upload.planner_checkpoint_json
        checkpoint = parse_incremental_volume_planner_checkpoint(original)
        inputs = (
            OrderedArchiveArtifact(
                checkpoint.next_artifact_order, ArchiveArtifact(member_id, size, digest)
            ),
        )
        quantum = advance_volume_emission(
            VolumeEmissionFrontier(checkpoint),
            inputs,
            limits=VolumeEmissionLimits(3, 16, 100000, 4),
        )
        assert len(quantum.volumes) == 3 and not quantum.frontier.done
        count, commitment = ordered_raw_part_commitment(("a" * 64,) * 17)
        session.add(
            CollectionUploadArtifactRecord(
                collection_id=collection_id,
                artifact_id=member_id,
                artifact_order=checkpoint.next_artifact_order,
                bytes=size,
                sha256=digest,
                raw_part_plaintext_bytes=PART,
                raw_part_count=count,
                raw_part_ordered_sha256=commitment,
                registration_id=operation_id,
            )
        )
        upload.active_registration_id = operation_id
        upload.registration_revision += 1
        owner._persist_plan_batch(
            session,
            upload=upload,
            batch=IncrementalVolumePlanBatch(
                quantum.frontier.checkpoint,
                tuple(p for p in quantum.volumes if isinstance(p, PackVolumePlan)),
                tuple(p for p in quantum.volumes if not isinstance(p, PackVolumePlan)),
            ),
            registration_id=operation_id,
        )
    stores = service._archive_stores
    service.close()
    dispose_session_factory(service._session_factory)
    dispose_session_factory(factory)
    service = owner.SqlAlchemyCollectionUploadService(config, stores, policy=policy)
    observer = make_session_factory(config.database_url)
    try:
        with session_scope(observer) as session:
            upload = session.get(CollectionUploadRecord, collection_id)
            assert upload.artifact_count == int(prior_pending_pack)
            assert upload.artifact_bytes == upload.planner_next_sequence == 0
            assert upload.planner_checkpoint_json == original
            assert len(upload.artifacts) == int(prior_pending_pack)
            assert upload.archive_objects == []
            assert session.get(CollectionUploadArtifactRecord, (collection_id, member_id)) is None
            for plan in quantum.volumes:
                assert (
                    session.get(
                        CollectionArchiveObjectUploadRecord, (collection_id, plan.volume_id)
                    )
                    is None
                )
            assert (
                session.scalar(
                    select(func.count()).select_from(CollectionUploadArtifactVolumeRecord)
                )
                == 0
            )
            alias = aliased(CollectionUploadArtifactRecord)
            assert list(session.scalars(select(alias).where(alias.artifact_id == member_id))) == []
            for model in (CollectionUploadArtifactRecord, CollectionArchiveObjectUploadRecord):
                assert (
                    session.execute(
                        update(model)
                        .where(model.registration_id == operation_id)
                        .values(registration_id="f" * 64)
                    ).rowcount
                    == 0
                )
                assert (
                    session.execute(
                        delete(model).where(model.registration_id == operation_id)
                    ).rowcount
                    == 0
                )

        assert [row["artifact_id"] for row in service.iter_artifacts(collection_id)] == (
            [prior_id] if prior_pending_pack else []
        )
        page = service.list_artifacts(collection_id, page_size=1, position=None)
        assert [row["artifact_id"] for row in page["artifacts"]] == (
            [prior_id] if prior_pending_pack else []
        )
        assert service.list_volumes(collection_id)["volumes"] == []
        work = service.acquire_work(collection_id, limit=3)
        assert work["work"] == [] and work["planning_complete"] is False
        assert work["complete"] is False
        with pytest.raises(NotFound):
            service.get_artifact(collection_id, member_id)
        with pytest.raises(NotFound):
            service.register_raw_part_digests(
                collection_id,
                CollectionUploadRawDigestBatchDocument(
                    artifact_id=member_id, first_part="0", sha256s=["a" * 64]
                ),
            )
        for plan in quantum.volumes:
            with pytest.raises(NotFound):
                service.get_volume(collection_id, plan.volume_id)
            with pytest.raises(NotFound):
                service.upload_unit(
                    collection_id,
                    plan.volume_id,
                    0,
                    principal=_CREATOR,
                    plan_sha256="a" * 64,
                    content=b"",
                )
            assert (
                service._checkpoints.load_raw_upload_checkpoint(
                    collection_id=collection_id, volume_id=plan.volume_id
                )
                is None
            )
        with pytest.raises(Conflict, match="registration is pending"):
            service.complete(collection_id)
        with pytest.raises(Conflict, match="registration is pending"):
            service.register_artifacts(
                collection_id,
                [
                    {
                        "artifact_id": "3" * 64,
                        "bytes": "0",
                        "sha256": hashlib.sha256(b"").hexdigest(),
                    }
                ],
            )

        with session_scope(observer) as session:
            session.info["include_unpublished_registration"] = True
            assert (
                session.get(
                    CollectionUploadArtifactRecord, (collection_id, member_id)
                ).registration_id
                == operation_id
            )
            for model in (
                CollectionArchiveObjectUploadRecord,
                CollectionUploadArtifactVolumeRecord,
            ):
                assert (
                    session.scalar(
                        select(func.count())
                        .select_from(model)
                        .where(model.registration_id == operation_id)
                    )
                    == 3
                )
            upload = session.get(CollectionUploadRecord, collection_id)
            assert upload.state == "open" and upload.planner_checkpoint_json == original
            assert upload.artifact_count == int(prior_pending_pack)
    finally:
        service.close()
        dispose_session_factory(service._session_factory)
        dispose_session_factory(observer)
