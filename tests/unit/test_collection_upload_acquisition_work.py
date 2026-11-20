from __future__ import annotations

import hashlib

import pytest
from riverhog_age import CHUNK_SIZE
from riverhog_core.catalog_db import make_session_factory, session_scope
from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord
from riverhog_core.collection_plan import CollectionVolumePolicy
from riverhog_core.services.collection_uploads import SqlAlchemyCollectionUploadService
from riverhog_protocol import CollectionUploadRawDigestBatchDocument
from riverhog_protocol.errors import Conflict
from riverhog_protocol.raw_ingress import ordered_raw_part_commitment
from sqlalchemy import event, select

from tests.unit.test_collection_registration_work import registration_fixture
from tests.unit.test_collection_uploads import _CREATOR, _service_with_archive_objects


@pytest.mark.parametrize("sealed_prefix", [0, 1, 16, 17, 129, 1000])
def test_acquiring_one_tail_unit_does_not_revisit_the_sealed_prefix(
    tmp_path, sealed_prefix, record_property
):
    service, _config, collection_id, _policy, artifacts, _digests = registration_fixture(
        tmp_path, "pack", sealed_prefix + 2
    )
    for start in range(0, len(artifacts), 16):
        service.register_artifacts(collection_id, artifacts[start : start + 16])
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
        assert len(volumes) == sealed_prefix + 1
        expected_volume = volumes[-1].object_id
        expected_plan_sha256 = volumes[-1].plan_sha256
        # Model catalog state only; this does not establish payload custody.
        for volume in volumes[:-1]:
            volume.state = "sealed"
            volume.uploaded_units = volume.total_units

    steps = 0
    connections = []

    def tick():
        nonlocal steps
        steps += 1
        return 0

    def checkout(raw, *_):
        connections.append(raw)
        raw.set_progress_handler(tick, 1)

    event.listen(engine, "checkout", checkout)
    try:
        reply = service.acquire_work(collection_id, limit=1)
    finally:
        event.remove(engine, "checkout", checkout)
        for raw in connections:
            raw.set_progress_handler(None, 0)

    work = reply["work"]
    assert len(work) == 1
    assert work[0]["volume"]["volume_id"] == expected_volume
    assert work[0]["plan_sha256"] == expected_plan_sha256
    assert work[0]["unit"] == service.get_unit(collection_id, expected_volume, 0)
    assert reply["planning_complete"] is reply["complete"] is False
    record_property("sealed_prefix", sealed_prefix)
    record_property("returned_units", len(work))
    record_property("sqlite_vm_steps", steps)
    assert steps <= 350, (sealed_prefix, steps)


@pytest.mark.parametrize("limit", [1, 2, 64])
def test_acquisition_preserves_raw_readiness_order_progress_and_retries(tmp_path, limit):
    policy = CollectionVolumePolicy(
        pack_artifacts=1,
        pack_member_bytes=2,
        raw_part_plaintext_bytes=CHUNK_SIZE,
        raw_volume_plaintext_bytes=2 * CHUNK_SIZE,
    )
    service, config, *_ = _service_with_archive_objects(tmp_path, policy=policy)
    opened = service.create_or_resume(
        idempotency_key="mixed-work",
        ingest_source="fixture",
        archive_store=None,
        initiator=_CREATOR,
        event_context=None,
    )
    collection_id = int(opened["collection_id"])
    contents = {
        f"{1:064x}": b"x",
        f"{2:064x}": b"r" * CHUNK_SIZE + b"s",
        f"{3:064x}": b"y",
        f"{4:064x}": b"tt",
    }
    artifacts = []
    digests = {}
    for artifact_id, content in contents.items():
        row = {
            "artifact_id": artifact_id,
            "bytes": str(len(content)),
            "sha256": hashlib.sha256(content).hexdigest(),
        }
        if len(content) >= policy.pack_member_bytes:
            digests[artifact_id] = [
                hashlib.sha256(content[start : start + CHUNK_SIZE]).hexdigest()
                for start in range(0, len(content), CHUNK_SIZE)
            ]
            count, commitment = ordered_raw_part_commitment(digests[artifact_id])
            row["raw_parts"] = {
                "part_plaintext_bytes": str(CHUNK_SIZE),
                "part_count": str(count),
                "ordered_sha256": commitment,
            }
        artifacts.append(row)
    service.register_artifacts(collection_id, artifacts)
    volumes = service.list_volumes(collection_id)["volumes"]
    assert [volume["kind"] for volume in volumes] == ["pack", "segment", "pack", "segment"]
    original_plans = {
        volume["volume_id"]: service.get_volume(collection_id, volume["volume_id"])["plan_sha256"]
        for volume in volumes
    }

    foreign_id = int(
        service.create_or_resume(
            idempotency_key="foreign-work",
            ingest_source="fixture",
            archive_store=None,
            initiator=_CREATOR,
            event_context=None,
        )["collection_id"]
    )
    service.register_artifacts(
        foreign_id,
        [
            {
                "artifact_id": f"{index:064x}",
                "bytes": "1",
                "sha256": hashlib.sha256(value).hexdigest(),
            }
            for index, value in ((5, b"z"), (6, b"w"))
        ],
    )

    def expected(volume, unit=0):
        return {
            "volume": {key: volume[key] for key in ("volume_id", "sequence", "kind")},
            "plan_sha256": original_plans[volume["volume_id"]],
            "unit": volume["units"][unit],
        }

    def upload(assignment, *, plan_sha256=None):
        unit = assignment["unit"]
        content = b"".join(
            contents[source["artifact_id"]][
                int(source["offset"]) : int(source["offset"]) + int(source["bytes"])
            ]
            for source in unit["sources"]
        )
        return service.upload_unit(
            collection_id,
            assignment["volume"]["volume_id"],
            int(unit["unit"]),
            principal=_CREATOR,
            plan_sha256=plan_sha256 or assignment["plan_sha256"],
            content=content,
        )

    packs = [volume for volume in volumes if volume["kind"] == "pack"]
    assert service.acquire_work(collection_id, limit=limit)["work"] == [
        expected(volume) for volume in packs[:limit]
    ]
    raw_id = f"{2:064x}"
    service.register_raw_part_digests(
        collection_id,
        CollectionUploadRawDigestBatchDocument(
            artifact_id=raw_id, first_part="0", sha256s=digests[raw_id][:1]
        ),
    )
    assert service.acquire_work(collection_id, limit=limit)["work"] == [
        expected(volume) for volume in packs[:limit]
    ]
    first = expected(volumes[0])
    with pytest.raises(Conflict):
        upload(first, plan_sha256="f" * 64)
    assert service.acquire_work(collection_id, limit=1)["work"] == [first]
    assert upload(first)["state"] == "committed"
    assert upload(first)["state"] == "committed"
    assert service.acquire_work(collection_id, limit=1)["work"] == [expected(volumes[2])]

    service.register_raw_part_digests(
        collection_id,
        CollectionUploadRawDigestBatchDocument(
            artifact_id=raw_id, first_part="1", sha256s=digests[raw_id][1:]
        ),
    )
    raw = expected(volumes[1])
    assert (
        service.acquire_work(collection_id, limit=limit)["work"]
        == [raw, expected(volumes[2])][:limit]
    )
    assert upload(raw)["state"] == "committed"
    service = SqlAlchemyCollectionUploadService(
        config,
        service._archive_stores,
        policy=policy,
        session_factory=make_session_factory(config.database_url),
    )
    raw_tail = expected(volumes[1], unit=1)
    assert service.acquire_work(collection_id, limit=1)["work"] == [raw_tail]
    assert upload(raw_tail)["state"] == "committed"
    assert upload(expected(volumes[2]))["state"] == "committed"
    paused = service.acquire_work(collection_id, limit=limit)
    assert paused["work"] == []
    assert paused["planning_complete"] is paused["complete"] is False

    raw_id = f"{4:064x}"
    service.register_raw_part_digests(
        collection_id,
        CollectionUploadRawDigestBatchDocument(
            artifact_id=raw_id, first_part="0", sha256s=digests[raw_id]
        ),
    )
    service.complete(collection_id)
    last = expected(volumes[3])
    assert service.acquire_work(collection_id, limit=limit)["work"] == [last]
    assert upload(last)["state"] == "committed"
    finished = service.acquire_work(collection_id, limit=limit)
    assert finished["work"] == []
    assert finished["planning_complete"] is finished["complete"] is True
    assert {
        volume["volume_id"]: volume["plan_sha256"]
        for volume in service.list_volumes(collection_id)["volumes"]
    } == original_plans
