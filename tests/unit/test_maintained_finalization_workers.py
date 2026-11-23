from __future__ import annotations

import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor
from time import monotonic
from types import SimpleNamespace

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionRecord
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services import archive_finalizer_registry as registry_module
from riverhog_core.services.archive_write_admission import _writer
from riverhog_core.services.physical_execution import PhysicalExecutionKey
from riverhog_protocol import CollectionUploadRawDigestBatchDocument
from riverhog_protocol.raw_ingress import ordered_raw_part_commitment
from sqlalchemy import select

from tests.support.finalization import drive_finalization_turn
from tests.unit.test_archive_provenance_journal_writer import _lose_ack
from tests.unit.test_collection_finalization_plan import _metadata_case, _until
from tests.unit.test_collection_uploads import _CREATOR, _bind_primary
from tests.unit.test_finalization_claim_readiness import _claim_snapshot, _seal


def _join(case):
    for slot in tuple(case.service._archive_execution._slots.values()):
        slot.worker.join(timeout=10)
        assert not slot.worker.is_alive()


def _published(case, collection_id=None):
    with session_scope(case.factory) as session:
        collection = session.get(CollectionRecord, collection_id or case.collection_id)
        return collection is not None and collection.is_published


@pytest.mark.parametrize("delayed_receiving", (False, True))
@pytest.mark.parametrize("kind", ("segment", "pack"))
def test_maintained_coordinator_publishes_through_joined_original_workers(
    tmp_path, monkeypatch, kind, delayed_receiving, database_url=None
):
    count = 2 if kind == "pack" else 1
    with _metadata_case(
        tmp_path, monkeypatch, kind=kind, count=count, database_url=database_url
    ) as (case, previous):
        service = case.service
        service._requeue_finalization_step(previous)
        claims = []
        create = service._finalization_owner

        def original(claim, *, admission):
            claims.append(claim.claim_id)
            return create(claim, admission=admission)

        monkeypatch.setattr(service, "_finalization_owner", original)
        clock = [monotonic()]
        monkeypatch.setattr(registry_module, "monotonic", lambda: clock[0])
        interrupted = []
        if delayed_receiving:
            import sqlite3

            from riverhog_core.services.immutable_archive_admission import ImmutableArchiveAdmission
            from sqlalchemy.exc import OperationalError

            report = ImmutableArchiveAdmission.report

            def delayed_report(admission, *args, **kwargs):
                if not interrupted:
                    context = service._archive_finalizers._contexts[case.collection_id]
                    interrupted.append(context.owner)
                    context.resume_after = clock[0] + 3600
                    raise OperationalError(
                        "controlled original report contention",
                        {},
                        sqlite3.OperationalError("database is locked"),
                    )
                return report(admission, *args, **kwargs)

            monkeypatch.setattr(ImmutableArchiveAdmission, "report", delayed_report)
        for _ in range(256):
            drive_finalization_turn(service)
            # Tail maintenance can submit an original continuation after the
            # driver's first join. Receive that actual worker before observing
            # metadata, then advance only this fixture's retry clock.
            _join(case)
            context = service._archive_finalizers._contexts.get(case.collection_id)
            if context is not None:
                with context.lock:
                    if context.owner is not None and context.owner.has_pending:
                        clock[0] = max(clock[0], context.resume_after)
            with session_scope(case.factory) as session:
                upload = session.get(Upload, case.collection_id)
                if upload is None:
                    collection = session.get(CollectionRecord, case.collection_id)
                    assert collection.is_published and collection.artifact_count == count
                    assert collection.archive_root_sha256 and collection.provenance_identity
                    assert collection.description_identity and collection.tag_head_identity
                    break
        else:
            pytest.fail(
                "maintained worker turns did not publish the collection: "
                + repr(_routing_snapshot(case))
            )
        service.process_due_finalizations()
        assert not service.has_unreceived_archive_work()
        assert service._archive_finalizers._contexts == {}
        assert len(claims) > 10 and len(set(claims)) == len(claims) and case.calls
        assert bool(interrupted) is delayed_receiving
        assert all(not owner.has_pending for owner in interrupted)


@pytest.mark.parametrize("committed", (False, True))
def test_maintained_failure_keeps_original_claim_and_receives_exact_report(
    tmp_path, monkeypatch, committed, database_url=None
):
    with _metadata_case(tmp_path, monkeypatch, count=1, database_url=database_url) as (case, prior):
        _until(case, prior, lambda upload: upload.archive_tree_sha256 is not None)
        service = case.service
        service._requeue_finalization_step(prior)
        with _lose_ack(monkeypatch, "report", committed) as fault:
            deadline = monotonic() + 120
            while True:
                assert service.process_due_finalizations() >= 0
                _join(case)
                if fault["fired"]:
                    break
                assert monotonic() < deadline, "the original report was not reached: " + repr(
                    _routing_snapshot(case)
                )
        context = service._archive_finalizers._contexts[case.collection_id]
        original = context.owner
        pipeline = original._original
        child = pipeline._original
        assert original.has_pending and pipeline.has_pending and child.has_pending
        before = _claim_snapshot(case)
        assert before[0:2] == ("finalizing", "finalizing")
        assert before[3] is None and before[6] not in (None, prior.claim_id)
        assert before[5].startswith("OSError:")
        calls = list(case.calls)
        clock = [monotonic()]
        monkeypatch.setattr(registry_module, "monotonic", lambda: clock[0])
        context.resume_after = clock[0] + 60
        assert service.has_unreceived_archive_work()
        assert service.process_due_finalizations() >= 1
        _join(case)
        assert context.owner is original and pipeline._original is child
        assert original.has_pending and pipeline.has_pending and child.has_pending
        assert case.calls == calls and _claim_snapshot(case) == before

        # Receiving the failed Future does not make the original retry due.
        # Advance only this test-owned scheduling clock; join actual workers
        # and require their original semantic receiver to finish.
        deadline = monotonic() + 120
        while original.has_pending:
            clock[0] = max(clock[0], context.resume_after)
            assert service.process_due_finalizations() >= 0
            _join(case)
            assert context.owner is original and pipeline._original is child
            assert case.calls == calls
            assert monotonic() < deadline, "the original report was not received: " + repr(
                _routing_snapshot(case)
            )
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            assert upload.archive_phase == "finalization_queued"
            assert upload.archive_claim_id is None and upload.archive_failure is None
        # Successful publication of the Future still needs actual receiving.
        assert service.has_unreceived_archive_work()


def _sibling(case, monkeypatch):
    service = case.service
    store = service._archive_stores.require("archive").store
    with session_scope(case.factory) as session:
        original_prefix = session.get(Upload, case.collection_id).archive_storage_prefix
    with monkeypatch.context() as patch:
        patch.setattr(
            store,
            "new_collection_archive_storage_prefix",
            lambda: original_prefix + "-independent",
        )
        opened = service.create_or_resume(
            idempotency_key="independent-maintained-collection",
            ingest_source="fixture",
            archive_store=None,
            use_cache=False,
            custody_mode="producer-retained",
            initiator=_CREATOR,
            event_context=None,
        )
    collection_id = int(opened["collection_id"])
    artifact_id = "2" * 64
    digest = hashlib.sha256(case.content).hexdigest()
    count, commitment = ordered_raw_part_commitment((digest,))
    unit_size = service._policy.raw_part_plaintext_bytes
    assert len(case.content) <= unit_size
    service.register_artifacts(
        collection_id,
        [
            {
                "artifact_id": artifact_id,
                "bytes": str(len(case.content)),
                "sha256": digest,
                "raw_parts": {
                    "part_plaintext_bytes": str(unit_size),
                    "part_count": str(count),
                    "ordered_sha256": commitment,
                },
            }
        ],
    )
    service.register_raw_part_digests(
        collection_id,
        CollectionUploadRawDigestBatchDocument(
            artifact_id=artifact_id, first_part="0", sha256s=[digest]
        ),
    )
    _bind_primary(service, collection_id, artifact_id, case.content)
    sibling = SimpleNamespace(**{**vars(case), "collection_id": collection_id})
    _seal(sibling)
    return sibling


def _routing_snapshot(case):
    service = case.service
    with session_scope(case.factory) as session:
        upload = session.get(Upload, case.collection_id)
        upload_state = (
            None
            if upload is None
            else {
                "state": upload.state,
                "phase": upload.archive_phase,
                "failure": upload.archive_failure,
                "has_claim": upload.archive_claim_id is not None,
                "due": upload.archive_next_attempt_at,
            }
        )
    context = service._archive_finalizers._contexts.get(case.collection_id)
    retained = []
    if context is not None and context.lock.acquire(blocking=False):
        try:
            owner = context.owner
            while owner is not None:
                retained.append(
                    {
                        "owner": type(owner).__name__,
                        "pending": owner.has_pending,
                        "index": getattr(owner, "_index", None),
                        "result": getattr(owner, "_result", None),
                    }
                )
                owner = getattr(owner, "_original", None)
        finally:
            context.lock.release()
    return {
        "upload": upload_state,
        "retained": retained,
        "physical": service._archive_execution.status(context.key) if context else None,
        "resume_after": context.resume_after if context else None,
    }


@pytest.mark.parametrize("delayed_receiving", (False, True))
def test_maintained_provider_wait_leaves_an_independent_collection_progressing(
    tmp_path, monkeypatch, delayed_receiving, database_url=None
):
    with _metadata_case(tmp_path, monkeypatch, count=1, database_url=database_url) as (case, prior):
        service = case.service
        service._requeue_finalization_step(prior)
        clock = [monotonic()]
        monkeypatch.setattr(registry_module, "monotonic", lambda: clock[0])
        interrupted = []
        if delayed_receiving:
            import sqlite3

            from riverhog_core.services.immutable_archive_admission import ImmutableArchiveAdmission
            from sqlalchemy.exc import OperationalError

            report = ImmutableArchiveAdmission.report

            def delayed_report(admission, admitted, *args, **kwargs):
                if admitted.prepared.collection_id == case.collection_id and not interrupted:
                    context = service._archive_finalizers._contexts[case.collection_id]
                    interrupted.append(context.owner)
                    context.resume_after = clock[0] + 3600
                    raise OperationalError(
                        "controlled original report contention",
                        {},
                        sqlite3.OperationalError("database is locked"),
                    )
                return report(admission, admitted, *args, **kwargs)

            monkeypatch.setattr(ImmutableArchiveAdmission, "report", delayed_report)
        other = _sibling(case, monkeypatch)
        with session_scope(case.factory) as session:
            prefix = session.get(Upload, case.collection_id).archive_storage_prefix
        entered, release = threading.Event(), threading.Event()
        execute = case.physical.execute_write
        blocked = []

        def hold(invocation, *, content):
            if invocation.operation.request.object_path.startswith(prefix + "/") and not blocked:
                blocked.append(invocation)
                entered.set()
                assert release.wait(120), "test did not release its original provider"
            return execute(invocation, content=content)

        monkeypatch.setattr(case.physical, "execute_write", hold)

        def advance_retry_clock():
            # Retry due times schedule the original; they do not receive it.
            # A held provider retains its own context lock, so never wait here.
            for context in tuple(service._archive_finalizers._contexts.values()):
                if context.lock.acquire(blocking=False):
                    try:
                        if context.owner is not None and context.owner.has_pending:
                            clock[0] = max(clock[0], context.resume_after)
                    finally:
                        context.lock.release()

        def join_independent():
            for slot in tuple(service._archive_execution._slots.values()):
                if (
                    isinstance(slot.key, PhysicalExecutionKey)
                    and slot.key.collection_id == case.collection_id
                ):
                    deadline = monotonic() + 10
                    while slot.worker.is_alive() and not entered.is_set():
                        slot.worker.join(timeout=0.01)
                        assert monotonic() < deadline
                else:
                    slot.worker.join(timeout=10)
                    assert not slot.worker.is_alive()
            advance_retry_clock()

        try:
            for _ in range(256):
                service.process_due_finalizations()
                join_independent()
                if entered.is_set():
                    break
            else:
                pytest.fail(
                    "the actual metadata provider was not entered: " + repr(_routing_snapshot(case))
                )
            claim = _claim_snapshot(case)[6]
            with session_scope(case.factory) as session:
                _writer(session, defer_if_busy=True)
                assert (
                    session.scalar(
                        select(Upload.collection_id)
                        .where(Upload.collection_id == case.collection_id)
                        .with_for_update(nowait=True)
                    )
                    == case.collection_id
                )
            with ThreadPoolExecutor(max_workers=1) as workers:
                assert workers.submit(service.process_due_finalizations).result(timeout=5) >= 0
            for _ in range(256):
                service.process_due_finalizations()
                join_independent()
                if _published(case, other.collection_id):
                    break
            else:
                pytest.fail(
                    "independent collection was stalled by a held provider: "
                    + repr(_routing_snapshot(other))
                )
            assert not release.is_set() and len(blocked) == 1
            assert _claim_snapshot(case)[6] == claim and not _published(case)
        finally:
            release.set()
            _join(case)
        for _ in range(256):
            drive_finalization_turn(service)
            advance_retry_clock()
            if _published(case):
                break
        else:
            pytest.fail(
                "the original collection did not resume after provider receiving: "
                + repr(_routing_snapshot(case))
            )
        assert bool(interrupted) is delayed_receiving
        assert all(not owner.has_pending for owner in interrupted)


def test_maintained_coordinator_defers_real_parent_writer_contention(
    tmp_path, monkeypatch, database_url=None
):
    with _metadata_case(tmp_path, monkeypatch, count=1, database_url=database_url) as (case, prior):
        service = case.service
        service._requeue_finalization_step(prior)
        before = _claim_snapshot(case)
        with ThreadPoolExecutor(max_workers=1) as workers:
            with session_scope(case.factory) as session:
                _writer(session)
                session.scalar(
                    select(Upload.collection_id)
                    .where(Upload.collection_id == case.collection_id)
                    .with_for_update()
                )
                assert workers.submit(service.process_due_finalizations).result(timeout=5) == 0
        assert _claim_snapshot(case) == before
        assert service._archive_finalizers._contexts == {} and case.calls == []
