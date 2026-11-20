from __future__ import annotations

import hashlib

import pytest
from riverhog_core.catalog_db import make_session_factory, session_scope
from riverhog_core.catalog_models import (
    CollectionArchiveObjectUploadRecord,
    CollectionUploadArtifactRecord,
    CollectionUploadArtifactVolumeRecord,
    CollectionUploadRecord,
)
from riverhog_core.collection_plan import CollectionVolumePolicy
from riverhog_core.domain.archive import ArchiveArtifact, PackVolumePlan
from riverhog_core.incremental_plan import (
    OrderedArchiveArtifact,
    advance_incremental_volume_plan,
    new_incremental_volume_planner,
    parse_incremental_volume_planner_checkpoint,
)
from riverhog_core.pack_volume import pack_volume_plan_bytes
from riverhog_core.raw_volume import raw_volume_plan_bytes
from riverhog_core.services import collection_uploads as owner
from riverhog_protocol import CollectionUploadRawDigestBatchDocument
from riverhog_protocol.errors import Conflict
from riverhog_protocol.raw_ingress import ordered_raw_part_commitment
from sqlalchemy import event, func, select
from sqlalchemy.orm import Session

from tests.unit.test_collection_uploads import _CREATOR, _service_with_archive_objects


def registration_fixture(tmp_path, mode, members):
    policy = CollectionVolumePolicy(
        pack_artifacts=1,
        pack_member_bytes=256 if mode == "pack" else 1,
    )
    service, config, *_ = _service_with_archive_objects(tmp_path, policy=policy)
    opened = service.create_or_resume(
        idempotency_key="registration-work",
        ingest_source="fixture",
        archive_store=None,
        initiator=_CREATOR,
        event_context=None,
    )
    artifacts = []
    raw_digests = {}
    for index in range(members):
        content = b"x" * (128 // members + (index < 128 % members))
        artifact_id = f"{index + 1:064x}"
        sha256 = hashlib.sha256(content).hexdigest()
        row = {"artifact_id": artifact_id, "bytes": str(len(content)), "sha256": sha256}
        if len(content) >= policy.pack_member_bytes:
            count, commitment = ordered_raw_part_commitment((sha256,))
            row["raw_parts"] = {
                "part_plaintext_bytes": str(policy.raw_part_plaintext_bytes),
                "part_count": str(count),
                "ordered_sha256": commitment,
            }
            raw_digests[artifact_id] = sha256
        artifacts.append(row)
    return service, config, int(opened["collection_id"]), policy, artifacts, raw_digests


@pytest.mark.parametrize("members", [1, 16, 17, 129, 1000])
@pytest.mark.parametrize("mode", ["pack", "mixed"])
def test_registration_and_exact_retries_bound_prior_plan_materialization(tmp_path, mode, members):
    service, config, collection_id, policy, artifacts, raw_digests = registration_fixture(
        tmp_path, mode, members
    )
    loaded = 0

    def observe(_session, record):
        nonlocal loaded
        if isinstance(record, CollectionArchiveObjectUploadRecord):
            assert record.collection_id == collection_id
            loaded += 1

    event.listen(Session, "loaded_as_persistent", observe)
    calls = 0
    try:
        for retry in (False, True):
            for start in range(0, members, 16):
                requested = artifacts[start : start + 16]
                before = loaded
                reply = service.register_artifacts(collection_id, requested)
                assert [
                    (row["artifact_id"], row["bytes"], row["sha256"]) for row in reply["artifacts"]
                ] == [(row["artifact_id"], row["bytes"], row["sha256"]) for row in requested]
                if retry:
                    assert reply["volumes"] == []
                calls += 1
                assert loaded - before <= 1
        assert loaded <= calls
    finally:
        event.remove(Session, "loaded_as_persistent", observe)

    with pytest.raises(Conflict, match="different metadata"):
        service.register_artifacts(collection_id, [dict(artifacts[0], sha256="f" * 64)])
    for artifact_id, sha256 in raw_digests.items():
        service.register_raw_part_digests(
            collection_id,
            CollectionUploadRawDigestBatchDocument(
                artifact_id=artifact_id, first_part="0", sha256s=[sha256]
            ),
        )
    service.complete(collection_id)
    expected = advance_incremental_volume_plan(
        new_incremental_volume_planner(policy=policy),
        tuple(
            OrderedArchiveArtifact(
                order=index,
                artifact=ArchiveArtifact(
                    artifact_id=row["artifact_id"],
                    bytes=int(row["bytes"]),
                    sha256=row["sha256"],
                ),
            )
            for index, row in enumerate(artifacts)
        ),
        final=True,
    )
    with session_scope(make_session_factory(config.database_url)) as session:
        upload = session.get(CollectionUploadRecord, collection_id)
        checkpoint = parse_incremental_volume_planner_checkpoint(upload.planner_checkpoint_json)
        assert checkpoint == expected.checkpoint
        assert upload.artifact_count == members and upload.artifact_bytes == 128
        assert upload.archive_phase == "uploading"
        records = list(
            session.scalars(
                select(CollectionUploadArtifactRecord)
                .where(CollectionUploadArtifactRecord.collection_id == collection_id)
                .order_by(CollectionUploadArtifactRecord.artifact_order)
            )
        )
        assert [(row.artifact_id, str(row.bytes), row.sha256) for row in records] == [
            (row["artifact_id"], row["bytes"], row["sha256"]) for row in artifacts
        ]
        volumes = list(
            session.scalars(
                select(CollectionArchiveObjectUploadRecord)
                .where(CollectionArchiveObjectUploadRecord.collection_id == collection_id)
                .order_by(CollectionArchiveObjectUploadRecord.sequence)
            )
        )
        assert [row.sequence for row in volumes] == [plan.sequence for plan in expected.volumes]
        assert [row.plan_json for row in volumes] == [
            (
                pack_volume_plan_bytes(plan)
                if isinstance(plan, PackVolumePlan)
                else raw_volume_plan_bytes(plan)
            ).decode("utf-8")
            for plan in expected.volumes
        ]


@pytest.mark.parametrize("mode", ["pack", "mixed"])
def test_plan_and_coverage_insertion_roll_back_as_one_registration(tmp_path, mode):
    service, config, collection_id, _policy, artifacts, _digests = registration_fixture(
        tmp_path, mode, 2
    )

    def refuse(_mapper, _connection, _record):
        raise OSError("fixture coverage insert failure")

    event.listen(CollectionUploadArtifactVolumeRecord, "before_insert", refuse)
    try:
        with pytest.raises(OSError, match="fixture coverage insert failure"):
            service.register_artifacts(collection_id, artifacts)
    finally:
        event.remove(CollectionUploadArtifactVolumeRecord, "before_insert", refuse)
    with session_scope(make_session_factory(config.database_url)) as session:
        upload = session.get(CollectionUploadRecord, collection_id)
        assert upload.artifact_count == upload.artifact_bytes == 0
        checkpoint = parse_incremental_volume_planner_checkpoint(upload.planner_checkpoint_json)
        assert checkpoint.artifacts_seen == checkpoint.next_sequence == 0
        for model in (
            CollectionUploadArtifactRecord,
            CollectionArchiveObjectUploadRecord,
            CollectionUploadArtifactVolumeRecord,
        ):
            assert session.scalar(select(func.count()).select_from(model)) == 0
    reply = service.register_artifacts(collection_id, artifacts)
    assert [row["artifact_id"] for row in reply["artifacts"]] == [
        row["artifact_id"] for row in artifacts
    ]
    assert reply["volumes"]


def test_closing_tail_refreshes_an_already_loaded_archive_object_collection(tmp_path):
    service, config, collection_id, _policy, artifacts, _digests = registration_fixture(
        tmp_path, "pack", 2
    )
    service.register_artifacts(collection_id, artifacts)
    with session_scope(make_session_factory(config.database_url)) as session:
        upload = session.get(CollectionUploadRecord, collection_id)
        assert [row.sequence for row in upload.archive_objects] == [0]
        checkpoint = parse_incremental_volume_planner_checkpoint(upload.planner_checkpoint_json)
        owner._seal_open_collection_upload(session, upload, checkpoint=checkpoint, config=config)
        assert [row.sequence for row in upload.archive_objects] == [0, 1]
        assert upload.state == upload.archive_phase == "uploading"
