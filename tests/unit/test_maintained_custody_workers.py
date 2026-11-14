from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord as Control
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadArtifactRecord as Artifact
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services.archive_write_admission import _writer
from sqlalchemy import select

from tests.support.finalization import drive_custody_turn
from tests.unit.test_canonical_upload_custody import _construction, _seal_payload
from tests.unit.test_collection_finalization_plan import _metadata_case
from tests.unit.test_maintained_finalization_workers import _sibling


def _receipt(case, collection_id=None):
    with session_scope(case.factory) as session:
        return session.scalar(
            select(Artifact.custody_receipt_json)
            .where(Artifact.collection_id == (collection_id or case.collection_id))
            .order_by(Artifact.artifact_id)
            .limit(1)
        )


def _join(service, *, except_collection=None):
    for slot in tuple(service._archive_execution._slots.values()):
        if slot.key.collection_id == except_collection:
            continue
        slot.worker.join(timeout=60)
        assert not slot.worker.is_alive(), "test-owned custody worker did not return"
        slot.future.result()


def _finish(service):
    for _ in range(64):
        drive_custody_turn(service, limit=4)
        with session_scope(service._session_factory) as session:
            due = session.scalar(
                select(Control.collection_id).where(Control.custody_due_count > 0).limit(1)
            )
        if due is None:
            service.process_due_archive_uploads(limit=4)
            assert not service.has_unreceived_archive_work()
            return
    pytest.fail("retained early custody did not finish its finite fixture")


def test_custody_uses_actual_joined_original_workers(tmp_path, monkeypatch, database_url=None):
    case = _construction(tmp_path, database_url=database_url)
    try:
        _seal_payload(case)
        created = []
        create = case.service._artifact_custody_owner
        coordinator = threading.get_ident()

        def construct(collection_id, artifact_id, *, forward_guard):
            created.append((collection_id, artifact_id, threading.get_ident()))
            return create(collection_id, artifact_id, forward_guard=forward_guard)

        def legacy_write(**kwargs):
            pytest.fail("maintained custody bypassed native immutable execution")

        monkeypatch.setattr(case.service, "_artifact_custody_owner", construct)
        monkeypatch.setattr(case.store, "put_immutable_object", legacy_write)
        _finish(case.service)
        assert len(created) == 2
        assert all(
            collection == 1 and artifact == case.member_id and thread != coordinator
            for collection, artifact, thread in created
        )
        with session_scope(case.factory) as session:
            row = session.get(Artifact, (1, case.member_id))
            upload = session.get(Upload, 1)
            assert row.custody_receipt_json is not None
            assert upload.final_authority_json is None
        assert case.service._archive_finalizers._contexts == {}
    finally:
        case.service._archive_execution.close()
        case.factory.kw["bind"].dispose()


@pytest.mark.parametrize("mutation", ("closed", "orphaned"))
def test_stale_custody_hint_cannot_register_forward_work(
    tmp_path, monkeypatch, mutation, database_url=None
):
    case = _construction(tmp_path, database_url=database_url)
    try:
        _seal_payload(case)
        hints = case.service._custody_due.select(limit=1)
        assert hints == ((1, case.member_id),)

        def stale(*, limit):
            with session_scope(case.factory) as session:
                _writer(session)
                if mutation == "closed":
                    session.get(Control, 1).gate = "closed"
                else:
                    session.get(Upload, 1).state = "orphaned"
            return hints

        monkeypatch.setattr(case.service._custody_due, "select", stale)
        assert case.service.process_due_custody_receipts() == 0
        assert case.service._archive_execution._slots == {}
        assert case.service._archive_finalizers._contexts == {}
        assert not case.store.objects
        with session_scope(case.factory) as session:
            assert session.get(Control, 1).custody_after_artifact_id is None
    finally:
        case.service._archive_execution.close()
        case.factory.kw["bind"].dispose()


def test_custody_defers_real_parent_writer_contention(tmp_path, database_url=None):
    case = _construction(tmp_path, database_url=database_url)
    try:
        _seal_payload(case)
        with ThreadPoolExecutor(max_workers=1) as workers:
            with session_scope(case.factory) as session:
                _writer(session)
                session.scalar(
                    select(Upload.collection_id).where(Upload.collection_id == 1).with_for_update()
                )
                assert (
                    workers.submit(case.service.process_due_custody_receipts).result(timeout=5) == 0
                )
        assert case.service._archive_finalizers._contexts == {} and not case.store.objects
        with session_scope(case.factory) as session:
            assert session.get(Control, 1).custody_after_artifact_id is None
        _finish(case.service)
    finally:
        case.service._archive_execution.close()
        case.factory.kw["bind"].dispose()


def test_pending_custody_constructor_keeps_its_original_member_cursor(
    tmp_path, monkeypatch, database_url=None
):
    with _metadata_case(tmp_path, monkeypatch, count=2, database_url=database_url) as (case, _):
        service = case.service
        create = service._artifact_custody_owner
        entered, release = threading.Event(), threading.Event()
        created = []

        def construct(collection_id, artifact_id, *, forward_guard):
            created.append((collection_id, artifact_id))
            if len(created) == 1:
                entered.set()
                assert release.wait(120), "test did not release its original constructor"
            return create(collection_id, artifact_id, forward_guard=forward_guard)

        monkeypatch.setattr(service, "_artifact_custody_owner", construct)
        try:
            assert service.process_due_custody_receipts() >= 1
            assert entered.wait(30), "original custody constructor was not entered"
            with session_scope(case.factory) as session:
                selected = session.get(Control, case.collection_id).custody_after_artifact_id
            assert selected == created[0][1]
            with ThreadPoolExecutor(max_workers=1) as workers:
                for _ in range(3):
                    assert (
                        workers.submit(service.process_due_custody_receipts).result(timeout=5) == 0
                    )
            with session_scope(case.factory) as session:
                assert (
                    session.get(Control, case.collection_id).custody_after_artifact_id == selected
                )
            assert len(created) == 1 and case.calls == []
        finally:
            release.set()
            _join(service)
        _finish(service)
        with session_scope(case.factory) as session:
            receipts = list(
                session.scalars(
                    select(Artifact.custody_receipt_json).where(
                        Artifact.collection_id == case.collection_id
                    )
                )
            )
        assert len(receipts) == 2 and all(receipts)


def test_blocked_custody_provider_keeps_independent_construction_progressing(
    tmp_path, monkeypatch, database_url=None
):
    with _metadata_case(tmp_path, monkeypatch, count=1, database_url=database_url) as (case, _):
        service = case.service
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
        try:
            assert service.process_due_custody_receipts() >= 1
            assert entered.wait(30), "actual custody provider was not entered"
            original = service._archive_finalizers._contexts[case.collection_id].owner
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
                for _ in range(32):
                    workers.submit(service.process_due_custody_receipts).result(timeout=5)
                    _join(service, except_collection=case.collection_id)
                    if _receipt(case, other.collection_id) is not None:
                        break
                else:
                    pytest.fail("held provider stalled an independent custody construction")
            assert not release.is_set() and len(blocked) == 1
            assert _receipt(case) is None
            assert service._archive_finalizers._contexts[case.collection_id].owner is original
        finally:
            release.set()
            _join(service)
        _finish(service)
        assert _receipt(case) is not None and _receipt(case, other.collection_id) is not None
