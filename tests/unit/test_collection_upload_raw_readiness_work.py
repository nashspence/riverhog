from __future__ import annotations

import hashlib
from contextlib import contextmanager

import pytest
from riverhog_age import CHUNK_SIZE
from riverhog_core.catalog_db import make_session_factory, session_scope
from riverhog_core.catalog_models import (
    CollectionArchiveObjectUploadRecord,
    CollectionUploadArtifactRecord,
    CollectionUploadRawPartDigestRecord,
)
from riverhog_core.collection_plan import CollectionVolumePolicy
from riverhog_core.services.collection_uploads import SqlAlchemyCollectionUploadService
from riverhog_protocol import CollectionUploadRawDigestBatchDocument
from riverhog_protocol.errors import BadRequest
from riverhog_protocol.raw_ingress import ordered_raw_part_commitment
from sqlalchemy import event, func, select

from tests.unit.test_collection_registration_work import registration_fixture
from tests.unit.test_collection_uploads import _CREATOR, _service_with_archive_objects


def waiting_raw_fixture(tmp_path, pending):
    service, config, collection_id, policy, artifacts, _digests = registration_fixture(
        tmp_path, "mixed", pending + 2
    )
    raw_sha = hashlib.sha256(b"x").hexdigest()
    empty_sha = hashlib.sha256(b"").hexdigest()
    count, commitment = ordered_raw_part_commitment((raw_sha,))
    artifacts = [
        {
            "artifact_id": row["artifact_id"],
            "bytes": "1",
            "sha256": raw_sha,
            "raw_parts": {
                "part_plaintext_bytes": str(policy.raw_part_plaintext_bytes),
                "part_count": str(count),
                "ordered_sha256": commitment,
            },
        }
        if index < pending
        else {"artifact_id": row["artifact_id"], "bytes": "0", "sha256": empty_sha}
        for index, row in enumerate(artifacts)
    ]
    for start in range(0, len(artifacts), 16):
        service.register_artifacts(collection_id, artifacts[start : start + 16])
    return service, config, collection_id, policy, artifacts


@contextmanager
def sqlite_work(service):
    engine = service._session_factory.kw["bind"]
    observation = {"vm_steps": 0}
    connections = []

    def tick():
        observation["vm_steps"] += 1
        return 0

    def checkout(raw, *_):
        connections.append(raw)
        raw.set_progress_handler(tick, 1)

    event.listen(engine, "checkout", checkout)
    try:
        yield observation
    finally:
        event.remove(engine, "checkout", checkout)
        for raw in connections:
            raw.set_progress_handler(None, 0)


@pytest.mark.parametrize("pending", [0, 1, 16, 17, 129, 1000])
def test_acquisition_skips_raw_volumes_without_accepted_digests(tmp_path, pending, record_property):
    service, _config, collection_id, _policy, _artifacts = waiting_raw_fixture(tmp_path, pending)
    with session_scope(service._session_factory) as session:
        volumes = list(
            session.scalars(
                select(CollectionArchiveObjectUploadRecord)
                .where(CollectionArchiveObjectUploadRecord.collection_id == collection_id)
                .order_by(CollectionArchiveObjectUploadRecord.sequence)
            )
        )
        assert len(volumes) == pending + 1
        assert [volume.kind for volume in volumes] == ["segment"] * pending + ["pack"]
        assert all(volume.state == "planned" for volume in volumes)
        expected_id = volumes[-1].object_id
        expected_plan = volumes[-1].plan_sha256
    with sqlite_work(service) as work:
        reply = service.acquire_work(collection_id, limit=1)
    assert len(reply["work"]) == 1
    assignment = reply["work"][0]
    assert assignment["volume"]["volume_id"] == expected_id
    assert assignment["plan_sha256"] == expected_plan
    assert assignment["unit"] == service.get_unit(collection_id, expected_id, 0)
    assert reply["planning_complete"] is reply["complete"] is False
    record_property("pending_raw_prefix", pending)
    record_property("returned_units", 1)
    record_property("sqlite_vm_steps", work["vm_steps"])
    assert work["vm_steps"] <= 350, (pending, work)


def raw_parts_fixture(tmp_path, parts, *, wrong_commitment=False):
    policy = CollectionVolumePolicy(
        pack_member_bytes=1,
        raw_part_plaintext_bytes=CHUNK_SIZE,
        raw_volume_plaintext_bytes=CHUNK_SIZE,
    )
    service, config, *_ = _service_with_archive_objects(tmp_path, policy=policy)
    collection_id = int(
        service.create_or_resume(
            idempotency_key="raw-readiness",
            ingest_source="fixture",
            archive_store=None,
            initiator=_CREATOR,
            event_context=None,
        )["collection_id"]
    )
    block = b"x" * CHUNK_SIZE
    source_hash = hashlib.sha256()
    for _ in range(parts):
        source_hash.update(block)
    digests = [hashlib.sha256(block).hexdigest()] * parts
    count, commitment = ordered_raw_part_commitment(digests)
    artifact_id = "a" * 64
    service.register_artifacts(
        collection_id,
        [
            {
                "artifact_id": artifact_id,
                "bytes": str(parts * CHUNK_SIZE),
                "sha256": source_hash.hexdigest(),
                "raw_parts": {
                    "part_plaintext_bytes": str(CHUNK_SIZE),
                    "part_count": str(count),
                    "ordered_sha256": "f" * 64 if wrong_commitment else commitment,
                },
            }
        ],
    )
    return service, config, collection_id, policy, artifact_id, digests


@pytest.mark.parametrize("parts", [1, 16, 17, 129, 1000])
def test_digest_frontier_promotes_only_newly_ready_volumes(tmp_path, parts, record_property):
    service, config, collection_id, policy, artifact_id, digests = raw_parts_fixture(
        tmp_path, parts
    )
    factory = service._session_factory
    engine = factory.kw["bind"]
    with session_scope(factory) as session:
        volumes = list(
            session.scalars(
                select(CollectionArchiveObjectUploadRecord)
                .where(CollectionArchiveObjectUploadRecord.collection_id == collection_id)
                .order_by(CollectionArchiveObjectUploadRecord.sequence)
            )
        )
        assert len(volumes) == parts
        assert [volume.source_last_part for volume in volumes] == list(range(1, parts + 1))
        assert all(not volume.work_ready for volume in volumes)
        ids = [volume.object_id for volume in volumes]
        plans = [(volume.object_id, volume.plan_json) for volume in volumes]
    promoted = []

    def observe(_connection, cursor, statement, _parameters, _context, _many):
        if statement.startswith("UPDATE collection_archive_object_uploads SET work_ready="):
            promoted.append(cursor.rowcount)

    event.listen(engine, "after_cursor_execute", observe)
    digest_steps = []
    acquisition_steps = []
    try:
        for start in range(0, parts, 16):
            batch = digests[start : start + 16]
            before = len(promoted)
            with sqlite_work(service) as work:
                service.register_raw_part_digests(
                    collection_id,
                    CollectionUploadRawDigestBatchDocument(
                        artifact_id=artifact_id, first_part=str(start), sha256s=batch
                    ),
                )
            digest_steps.append(work["vm_steps"])
            assert promoted[before:] == [len(batch)]
            assert work["vm_steps"] <= 1200 + 300 * len(batch), (parts, start, work)
            if start == 0:
                with session_scope(service._session_factory) as session:
                    artifact = session.get(
                        CollectionUploadArtifactRecord, (collection_id, artifact_id)
                    )
                    retry = {
                        "artifact_id": artifact_id,
                        "bytes": str(artifact.bytes),
                        "sha256": artifact.sha256,
                        "raw_parts": {
                            "part_plaintext_bytes": str(artifact.raw_part_plaintext_bytes),
                            "part_count": str(artifact.raw_part_count),
                            "ordered_sha256": artifact.raw_part_ordered_sha256,
                        },
                    }
                assert service.register_artifacts(collection_id, [retry])["volumes"] == []
                event.remove(engine, "after_cursor_execute", observe)
                service = SqlAlchemyCollectionUploadService(
                    config,
                    service._archive_stores,
                    policy=policy,
                    session_factory=make_session_factory(config.database_url),
                )
                engine = service._session_factory.kw["bind"]
                event.listen(engine, "after_cursor_execute", observe)
            with sqlite_work(service) as work:
                reply = service.acquire_work(collection_id, limit=16)
            acquisition_steps.append(work["vm_steps"])
            assert [row["volume"]["volume_id"] for row in reply["work"]] == ids[start : start + 16]
            assert work["vm_steps"] <= 350 + 100 * len(batch), (parts, start, work)
            with session_scope(service._session_factory) as session:
                current = list(
                    session.scalars(
                        select(CollectionArchiveObjectUploadRecord).where(
                            CollectionArchiveObjectUploadRecord.collection_id == collection_id,
                            CollectionArchiveObjectUploadRecord.object_id.in_(
                                ids[start : start + 16]
                            ),
                        )
                    )
                )
                assert all(volume.work_ready for volume in current)
                # Model completed catalog state only; do not manufacture custody receipts.
                for volume in current:
                    volume.state = "sealed"
                    volume.uploaded_units = volume.total_units
        before = len(promoted)
        with sqlite_work(service) as work:
            service.register_raw_part_digests(
                collection_id,
                CollectionUploadRawDigestBatchDocument(
                    artifact_id=artifact_id, first_part="0", sha256s=digests[:16]
                ),
            )
        assert promoted[before:] == []
        assert work["vm_steps"] <= 1200 + 300 * len(digests[:16])
    finally:
        event.remove(engine, "after_cursor_execute", observe)
    assert sum(promoted) == parts
    with session_scope(service._session_factory) as session:
        artifact = session.get(CollectionUploadArtifactRecord, (collection_id, artifact_id))
        assert artifact.raw_parts_accepted == parts
        assert artifact.raw_part_commitment_sha256 == artifact.raw_part_ordered_sha256
        assert (
            session.scalar(select(func.count()).select_from(CollectionUploadRawPartDigestRecord))
            == parts
        )
        assert [
            (volume.object_id, volume.plan_json)
            for volume in session.scalars(
                select(CollectionArchiveObjectUploadRecord)
                .where(CollectionArchiveObjectUploadRecord.collection_id == collection_id)
                .order_by(CollectionArchiveObjectUploadRecord.sequence)
            )
        ] == plans
    record_property("raw_parts", parts)
    record_property("frontier_calls", len(digest_steps))
    record_property("volume_rows_promoted", sum(promoted))
    record_property("max_digest_sqlite_vm_steps", max(digest_steps))
    record_property("max_acquisition_sqlite_vm_steps", max(acquisition_steps))


def test_readiness_promotion_rolls_back_with_digest_progress(tmp_path):
    service, _config, collection_id, _policy, artifact_id, digests = raw_parts_fixture(tmp_path, 17)
    engine = service._session_factory.kw["bind"]

    def refuse(_connection, _cursor, statement, _parameters, _context, _many):
        if statement.startswith("UPDATE collection_archive_object_uploads SET work_ready="):
            raise OSError("fixture readiness update failure")

    event.listen(engine, "before_cursor_execute", refuse)
    batch = CollectionUploadRawDigestBatchDocument(
        artifact_id=artifact_id, first_part="0", sha256s=digests[:16]
    )
    try:
        with pytest.raises(OSError, match="fixture readiness update failure"):
            service.register_raw_part_digests(collection_id, batch)
    finally:
        event.remove(engine, "before_cursor_execute", refuse)
    with session_scope(service._session_factory) as session:
        artifact = session.get(CollectionUploadArtifactRecord, (collection_id, artifact_id))
        assert artifact.raw_parts_accepted == 0
        assert (
            session.scalar(select(func.count()).select_from(CollectionUploadRawPartDigestRecord))
            == 0
        )
        assert not any(
            volume.work_ready
            for volume in session.scalars(select(CollectionArchiveObjectUploadRecord))
        )
    assert service.acquire_work(collection_id, limit=16)["work"] == []
    service.register_raw_part_digests(collection_id, batch)
    assert len(service.acquire_work(collection_id, limit=16)["work"]) == 16


def test_wrong_final_digest_commitment_does_not_promote_work(tmp_path):
    service, _config, collection_id, _policy, artifact_id, digests = raw_parts_fixture(
        tmp_path, 17, wrong_commitment=True
    )
    service.register_raw_part_digests(
        collection_id,
        CollectionUploadRawDigestBatchDocument(
            artifact_id=artifact_id, first_part="0", sha256s=digests[:16]
        ),
    )
    with pytest.raises(BadRequest, match="differs from its registered authority"):
        service.register_raw_part_digests(
            collection_id,
            CollectionUploadRawDigestBatchDocument(
                artifact_id=artifact_id, first_part="16", sha256s=digests[16:]
            ),
        )
    with session_scope(service._session_factory) as session:
        artifact = session.get(CollectionUploadArtifactRecord, (collection_id, artifact_id))
        assert artifact.raw_parts_accepted == 16
        assert (
            session.scalar(select(func.count()).select_from(CollectionUploadRawPartDigestRecord))
            == 16
        )
        volumes = list(
            session.scalars(
                select(CollectionArchiveObjectUploadRecord).order_by(
                    CollectionArchiveObjectUploadRecord.sequence
                )
            )
        )
        assert [volume.work_ready for volume in volumes] == [True] * 16 + [False]


def test_readiness_projection_cannot_replace_raw_digest_eligibility(tmp_path):
    service, _config, collection_id, _policy, _artifacts = waiting_raw_fixture(tmp_path, 1)
    with session_scope(service._session_factory) as session:
        volumes = list(
            session.scalars(
                select(CollectionArchiveObjectUploadRecord).order_by(
                    CollectionArchiveObjectUploadRecord.sequence
                )
            )
        )
        assert [volume.kind for volume in volumes] == ["segment", "pack"]
        volumes[0].work_ready = True
        expected = volumes[1].object_id
    reply = service.acquire_work(collection_id, limit=64)
    assert [row["volume"]["volume_id"] for row in reply["work"]] == [expected]


def test_digest_readiness_update_is_scoped_to_collection_and_artifact(tmp_path):
    service, _config, collection_id, _policy, artifacts = waiting_raw_fixture(tmp_path, 2)
    foreign_id = int(
        service.create_or_resume(
            idempotency_key="foreign-readiness",
            ingest_source="fixture",
            archive_store=None,
            initiator=_CREATOR,
            event_context=None,
        )["collection_id"]
    )
    service.register_artifacts(foreign_id, artifacts)
    first_id = artifacts[0]["artifact_id"]
    second_id = artifacts[1]["artifact_id"]
    service.register_raw_part_digests(
        collection_id,
        CollectionUploadRawDigestBatchDocument(
            artifact_id=first_id,
            first_part="0",
            sha256s=[hashlib.sha256(b"x").hexdigest()],
        ),
    )
    with session_scope(service._session_factory) as session:
        volumes = list(
            session.scalars(
                select(CollectionArchiveObjectUploadRecord).where(
                    CollectionArchiveObjectUploadRecord.kind == "segment"
                )
            )
        )
        assert {
            (volume.collection_id, volume.source_artifact_id): volume.work_ready
            for volume in volumes
        } == {
            (collection_id, first_id): True,
            (collection_id, second_id): False,
            (foreign_id, first_id): False,
            (foreign_id, second_id): False,
        }
        members = list(
            session.scalars(
                select(CollectionUploadArtifactRecord).where(
                    CollectionUploadArtifactRecord.artifact_id.in_([first_id, second_id])
                )
            )
        )
        assert {
            (member.collection_id, member.artifact_id): member.raw_parts_accepted
            for member in members
        } == {
            (collection_id, first_id): 1,
            (collection_id, second_id): 0,
            (foreign_id, first_id): 0,
            (foreign_id, second_id): 0,
        }
