from __future__ import annotations

import hashlib
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from riverhog_core.catalog_archive_write_models import ArchiveWriteEffectRecord
from riverhog_core.catalog_db import dispose_session_factory, session_scope
from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord, CollectionUploadRecord
from riverhog_core.collection_plan import CollectionVolumePolicy
from riverhog_core.raw_volume import parse_raw_volume_plan
from riverhog_core.services.archive_write_admission import (
    ArchiveWriteAdmission,
    ClosedArchiveWrite,
)
from riverhog_protocol import CollectionUploadRawDigestBatchDocument
from riverhog_protocol.errors import Conflict
from riverhog_protocol.raw_ingress import ordered_raw_part_commitment
from sqlalchemy import event

from tests.unit.test_archive_write_admission import GRANTS, _guard
from tests.unit.test_collection_uploads import _CREATOR, _service_with_archive_objects

PART = 5 * 1024 * 1024


@contextmanager
def _source_case(tmp_path, *, database_url=None, concurrent_parts=False):
    service, config, provider, roots = _service_with_archive_objects(
        tmp_path,
        policy=CollectionVolumePolicy(
            pack_member_bytes=8,
            raw_volume_plaintext_bytes=2 * PART if concurrent_parts else PART,
            raw_part_plaintext_bytes=PART,
        ),
        database_url=database_url,
    )
    try:
        created = service.create_or_resume(
            idempotency_key="checkpoint-settlement",
            ingest_source="fixture",
            archive_store=None,
            initiator=_CREATOR,
            event_context=None,
        )
        collection_id = int(created["collection_id"])
        content = (
            b"x" * PART + b"y" * (PART - 1)
            if concurrent_parts
            else b"actual raw checkpoint fixture"
        )
        digest = hashlib.sha256(content).hexdigest()
        part_digests = tuple(
            hashlib.sha256(content[offset : offset + PART]).hexdigest()
            for offset in range(0, len(content), PART)
        )
        count, commitment = ordered_raw_part_commitment(part_digests)
        service.register_artifacts(
            collection_id,
            [
                {
                    "artifact_id": "1" * 64,
                    "bytes": str(len(content)),
                    "sha256": digest,
                    "raw_parts": {
                        "part_plaintext_bytes": str(PART),
                        "part_count": str(count),
                        "ordered_sha256": commitment,
                    },
                }
            ],
        )
        service.register_raw_part_digests(
            collection_id,
            CollectionUploadRawDigestBatchDocument(
                artifact_id="1" * 64, first_part="0", sha256s=list(part_digests)
            ),
        )
        volume = service.list_volumes(collection_id)["volumes"][0]
        object_id = str(volume["volume_id"])
        with session_scope(service._session_factory) as session:
            record = session.get(CollectionArchiveObjectUploadRecord, (collection_id, object_id))
            upload = session.get(CollectionUploadRecord, collection_id)
            plan = parse_raw_volume_plan(record.plan_json)
            identity = {
                "kind": record.kind,
                "object_path": record.object_path,
                "plan_sha256": record.plan_sha256,
            }
            relative_path = record.relative_path
            passphrase_id = upload.passphrase_id
            incarnation = upload.archive_incarnation_id
        raw = service._raw_uploader(provider, passphrase_id=passphrase_id)
        initial = raw.open(
            collection_id=collection_id,
            plan=plan,
            object_path=identity["object_path"],
            relative_path=relative_path,
            target_part_plaintext_bytes=PART,
            expected_part_sha256s=part_digests,
        )
        yield SimpleNamespace(
            service=service,
            factory=service._session_factory,
            epoch=ArchiveWriteAdmission(service._session_factory).bind_epoch(
                collection_id, guard=_guard
            ),
            provider=provider,
            collection_id=collection_id,
            object_id=object_id,
            identity=identity,
            initial=initial,
            content=content,
            digest=digest,
            incarnation=incarnation,
        )
    finally:
        service.close()
        dispose_session_factory(service._session_factory)


def _assert_checkpoint_lock_order(case):
    statements = []
    engine = case.factory.kw["bind"]

    def record(connection, cursor, statement, parameters, context, executemany):
        if statement.lstrip().upper().startswith(("SELECT", "BEGIN")):
            statements.append(statement)

    event.listen(engine, "before_cursor_execute", record)
    try:
        case.service._checkpoints.merge_raw_upload_checkpoint(
            collection_id=case.collection_id,
            volume_id=case.object_id,
            checkpoint_json=case.initial.to_json(),
        )
    finally:
        event.remove(engine, "before_cursor_execute", record)
    selects = [s for s in statements if s.lstrip().upper().startswith("SELECT")]
    assert "FROM collection_uploads " in selects[0]
    assert "FROM collection_archive_object_uploads " in selects[-1]
    assert "planner_checkpoint_json" not in selects[0]
    if engine.dialect.name == "sqlite":
        assert statements[0].strip().upper() == "BEGIN IMMEDIATE"
    else:
        assert "FOR UPDATE" in selects[0] and "FOR UPDATE" in selects[-1]


def test_checkpoint_application_locks_construction_before_volume(tmp_path):
    with _source_case(tmp_path) as case:
        _assert_checkpoint_lock_order(case)


def _assert_lost_checkpoint_report(case, monkeypatch):
    owner = ArchiveWriteAdmission(case.factory)
    owner.prepare(
        case.collection_id,
        case.object_id,
        epoch=case.epoch,
        identity=case.identity,
        preparation={"age_state": json.loads(case.initial.age_state_json)},
        grants=GRANTS,
        guard=_guard,
    )
    ticket = owner.arm(
        case.collection_id,
        case.object_id,
        epoch=case.epoch,
        kind="part",
        resource="part:1",
        arguments={"number": 1, "plaintext_sha256": case.digest},
        executor_id="e" * 64,
        result_capacity_bytes=4096,
        grants=GRANTS,
        guard=_guard,
    )
    pending = []
    writes = []
    original_write = case.provider.write_segment

    def write_once(**arguments):
        writes.append(arguments["number"])
        return original_write(**arguments)

    def lost_checkpoint(**arguments):
        pending.append(arguments["checkpoint_json"])
        raise OSError("injected checkpoint response loss")

    with monkeypatch.context() as patch:
        patch.setattr(case.provider, "write_segment", write_once)
        patch.setattr(case.service._checkpoints, "merge_raw_upload_checkpoint", lost_checkpoint)
        with pytest.raises(OSError, match="checkpoint response loss"):
            ticket.dispatch(
                lambda: case.service.upload_unit(
                    case.collection_id,
                    case.object_id,
                    0,
                    plan_sha256=case.identity["plan_sha256"],
                    content=case.content,
                )
            )
    assert len(pending) == 1 and len(writes) == 1
    owner.record_unknown(ticket)
    owner.select_terminal(
        case.collection_id,
        kind="cancel",
        request={"selection": "exact-original"},
        principal_id=_CREATOR.id,
        grants=GRANTS,
        guard=_guard,
        validate_selection=lambda session, upload: None,
    )
    with session_scope(case.factory) as session:
        effect = session.get(ArchiveWriteEffectRecord, ticket.call_id)
        evidence = ClosedArchiveWrite(
            call_id=ticket.call_id,
            request_sha256=effect.request_sha256,
            executor_id=effect.executor_id,
            target_incarnation=case.incarnation,
            result={"checkpoint": pending[0]},
        )

    def apply(session, upload, record, result):
        case.service._checkpoints.apply_raw_upload_checkpoint(
            session=session,
            upload=upload,
            record=record,
            checkpoint_json=result["checkpoint"],
        )

    def apply_then_fail(session, upload, record, result):
        apply(session, upload, record, result)
        session.flush()
        raise OSError("injected atomic report rollback")

    with pytest.raises(OSError, match="atomic report rollback"):
        owner.settle_closed(ticket, evidence, apply_result=apply_then_fail)
    with session_scope(case.factory) as session:
        volume = session.get(
            CollectionArchiveObjectUploadRecord, (case.collection_id, case.object_id)
        )
        unchanged = volume.checkpoint_json == case.initial.to_json()
        assert unchanged, "checkpoint changed despite report rollback"
        assert volume.uploaded_units == 0
        assert session.get(CollectionUploadRecord, case.collection_id).uploaded_payload_bytes == 0
        assert session.get(ArchiveWriteEffectRecord, ticket.call_id).unresolved
    owner.settle_closed(ticket, evidence, apply_result=apply)
    owner.settle_closed(
        ticket,
        evidence,
        apply_result=lambda *args: pytest.fail("settled report cannot apply twice"),
    )
    assert len(writes) == 1
    with session_scope(case.factory) as session:
        volume = session.get(
            CollectionArchiveObjectUploadRecord, (case.collection_id, case.object_id)
        )
        assert volume.uploaded_units == 1
        assert session.get(
            CollectionUploadRecord, case.collection_id
        ).uploaded_payload_bytes == len(case.content)
        assert not session.get(ArchiveWriteEffectRecord, ticket.call_id).unresolved
    with pytest.raises(Conflict, match="admission is closed"):
        owner.arm(
            case.collection_id,
            case.object_id,
            epoch=case.epoch,
            kind="complete",
            resource="complete",
            arguments={},
            executor_id="e" * 64,
            result_capacity_bytes=1000,
            grants=GRANTS,
            guard=_guard,
        )


def test_actual_upload_unit_retries_lost_checkpoint_report_without_resending_part(
    tmp_path, monkeypatch
):
    with _source_case(tmp_path) as case:
        _assert_lost_checkpoint_report(case, monkeypatch)


def _assert_parallel_checkpoint_progress(case, monkeypatch):
    ready = threading.Barrier(3)
    releases = {1: threading.Event(), 2: threading.Event()}
    write = case.provider.write_segment

    def held_write(**arguments):
        ready.wait(15)
        if not releases[arguments["number"]].wait(15):
            raise AssertionError("provider fixture release deadline")
        return write(**arguments)

    with monkeypatch.context() as patch, ThreadPoolExecutor(max_workers=2) as pool:
        patch.setattr(case.provider, "write_segment", held_write)
        futures = [
            pool.submit(
                case.service.upload_unit,
                case.collection_id,
                case.object_id,
                unit,
                plan_sha256=case.identity["plan_sha256"],
                content=case.content[unit * PART : (unit + 1) * PART],
            )
            for unit in range(2)
        ]
        try:
            ready.wait(15)
            other = case.service.create_or_resume(
                idempotency_key="independent-during-provider-wait",
                ingest_source="fixture",
                archive_store=None,
                initiator=_CREATOR,
                event_context=None,
            )
            assert int(other["collection_id"]) != case.collection_id
            releases[2].set()
            assert futures[1].result(timeout=20)["state"] == "committed"
            statements = []
            engine = case.factory.kw["bind"]

            def record_sql(connection, cursor, statement, parameters, context, executemany):
                if statement.lstrip().upper().startswith("SELECT"):
                    statements.append(statement)

            event.listen(engine, "before_cursor_execute", record_sql)
            try:
                assert (
                    case.service.get_unit(case.collection_id, case.object_id, 0)["state"]
                    == "pending"
                )
                assert (
                    case.service.get_unit(case.collection_id, case.object_id, 1)["state"]
                    == "committed"
                )
                work = case.service.acquire_work(case.collection_id, limit=1)["work"]
                assert len(work) == 1 and work[0]["unit"]["unit"] == "0"
                assert work[0]["unit"]["state"] == "pending"
            finally:
                event.remove(engine, "before_cursor_execute", record_sql)
            assert all(
                "collection_archive_object_uploads.checkpoint_json" not in statement
                for statement in statements
            )
            point_reads = [
                statement
                for statement in statements
                if "FROM collection_archive_object_upload_units " in statement
            ]
            assert len(point_reads) == 2 and all(
                "collection_archive_object_upload_units.unit =" in statement
                for statement in point_reads
            )
        finally:
            for release in releases.values():
                release.set()
        results = [future.result(timeout=20) for future in futures]
    assert all(reply["state"] == "committed" for reply in results)
    with session_scope(case.factory) as session:
        volume = session.get(
            CollectionArchiveObjectUploadRecord, (case.collection_id, case.object_id)
        )
        assert volume.uploaded_units == 2
        assert session.get(
            CollectionUploadRecord, case.collection_id
        ).uploaded_payload_bytes == len(case.content)


def test_concurrent_part_checkpoints_preserve_progress_and_independent_construction(
    tmp_path, monkeypatch
):
    with _source_case(tmp_path, concurrent_parts=True) as case:
        _assert_parallel_checkpoint_progress(case, monkeypatch)
