from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services import archive_finalizer_work as work_module
from riverhog_core.services import collection_uploads as service_module
from riverhog_core.services.archive_write_admission import _writer
from sqlalchemy import select
from time_formats import utc_timestamp_now

from tests.unit.test_archive_finalizer_registration import _terminal
from tests.unit.test_archive_finalizer_registry import _receive
from tests.unit.test_collection_finalization_plan import _metadata_case
from tests.unit.test_finalization_claim_readiness import _claim_snapshot


@contextmanager
def _queued(tmp_path, monkeypatch, *, database_url=None):
    with _metadata_case(tmp_path, monkeypatch, count=1, database_url=database_url) as (case, claim):
        case.service._requeue_finalization_step(claim)
        yield case, claim


def _spy_native(case, monkeypatch):
    create = service_module.OriginalNativeArchiveSeal
    calls = []

    def construct(service, collection_id):
        with session_scope(case.factory) as session:
            upload = session.get(Upload, collection_id)
            assert upload.archive_phase == "finalizing"
            assert upload.archive_claim_id is not None
            calls.append((upload.archive_claim_id, threading.get_ident()))
        return create(service, collection_id)

    monkeypatch.setattr(service_module, "OriginalNativeArchiveSeal", construct)
    return calls


@pytest.mark.parametrize("committed", (False, True))
def test_actual_claim_registration_resolves_rollback_and_lost_commit_reply(
    tmp_path, monkeypatch, committed, database_url=None
):
    with _queued(tmp_path, monkeypatch, database_url=database_url) as (case, previous):
        service = case.service
        constructors = _spy_native(case, monkeypatch)
        before = _claim_snapshot(case)
        actual = work_module.session_scope
        registered = []

        @contextmanager
        def lost_reply(factory):
            with actual(factory) as session:
                yield session
                upload = session.get(Upload, case.collection_id)
                assert upload.archive_phase == "finalizing"
                assert upload.archive_claim_id not in (None, previous.claim_id)
                assert case.collection_id in service._archive_finalizers._contexts
                assert constructors == [] and case.calls == []
                registered.append(upload.archive_claim_id)
                if not committed:
                    raise OSError("claim registration acknowledgement")
            raise OSError("claim registration acknowledgement")

        with monkeypatch.context() as patch:
            patch.setattr(work_module, "session_scope", lost_reply)
            with pytest.raises(OSError, match="claim registration acknowledgement"):
                service._submit_finalization_if_due(case.collection_id, now=utc_timestamp_now())
        (completed,) = _receive(service._archive_finalizers, service._archive_execution)
        assert completed.result().progressed is committed
        assert len(registered) == 1
        if committed:
            assert constructors
            assert {value[0] for value in constructors} == set(registered)
            assert all(value[1] != threading.get_ident() for value in constructors)
            assert _claim_snapshot(case)[1] == "finalization_queued"
        else:
            assert constructors == [] and _claim_snapshot(case) == before
        assert case.calls == []
        context = service._archive_finalizers._contexts[case.collection_id]
        assert not context.owner.has_pending


@pytest.mark.parametrize("selection", ("not-due", "closed", "registry-busy"))
def test_deferred_claim_does_not_mutate_durable_work_or_install_children(
    tmp_path, monkeypatch, selection
):
    with _queued(tmp_path, monkeypatch) as (case, previous):
        service = case.service
        constructors = _spy_native(case, monkeypatch)
        if selection == "not-due":
            with session_scope(case.factory) as session:
                session.get(Upload, case.collection_id).archive_next_attempt_at = None
        elif selection == "closed":
            _terminal(case)
        before = _claim_snapshot(case)
        if selection == "registry-busy":
            with service._archive_finalizers._lock:
                outcome = service._submit_finalization_if_due(
                    case.collection_id, now=utc_timestamp_now()
                )
        else:
            outcome = service._submit_finalization_if_due(
                case.collection_id, now=utc_timestamp_now()
            )
        assert outcome == {"not-due": None, "closed": "closed", "registry-busy": "busy"}[selection]
        assert _claim_snapshot(case) == before
        assert service._archive_finalizers._contexts == {}
        assert constructors == [] and case.calls == []


def test_claim_registration_defers_contended_parent_without_installing_worker(
    tmp_path, monkeypatch, database_url=None
):
    with _queued(tmp_path, monkeypatch, database_url=database_url) as (case, previous):
        service = case.service
        constructors = _spy_native(case, monkeypatch)
        before = _claim_snapshot(case)
        with ThreadPoolExecutor(max_workers=1) as workers:
            with session_scope(case.factory) as session:
                _writer(session)
                session.scalar(
                    select(Upload.collection_id)
                    .where(Upload.collection_id == case.collection_id)
                    .with_for_update()
                )
                future = workers.submit(
                    service._submit_finalization_if_due,
                    case.collection_id,
                    now=utc_timestamp_now(),
                )
                assert future.result(timeout=5) == "busy"
        assert _claim_snapshot(case) == before
        assert service._archive_finalizers._contexts == {}
        assert constructors == [] and case.calls == []


def test_fresh_claim_waits_for_unacknowledged_original_workflow_receiver(tmp_path, monkeypatch):
    with _metadata_case(tmp_path, monkeypatch, count=1) as (case, claim):
        service = case.service
        receive = service._requeue_finalization_step
        received = []

        def lose_reply(selected):
            received.append(selected)
            receive(selected)
            if len(received) == 1:
                raise OSError("original workflow acknowledgement")

        monkeypatch.setattr(service, "_requeue_finalization_step", lose_reply)
        assert service._submit_finalization(claim) == "submitted"
        (failed,) = _receive(service._archive_finalizers, service._archive_execution)
        with pytest.raises(OSError, match="original workflow acknowledgement"):
            failed.result()
        context = service._archive_finalizers._contexts[case.collection_id]
        original = context.owner
        assert original.has_pending
        before, calls = _claim_snapshot(case), list(case.calls)
        assert (
            service._submit_finalization_if_due(case.collection_id, now=utc_timestamp_now())
            == "original-pending"
        )
        assert context.owner is original and received == [claim]
        assert _claim_snapshot(case) == before and case.calls == calls
        assert (
            service._archive_finalizers.resume(context.key, target_incarnation=context.target)
            == "submitted"
        )
        (completed,) = _receive(service._archive_finalizers, service._archive_execution)
        assert completed.result().progressed and received == [claim, claim]
        assert not original.has_pending and _claim_snapshot(case) == before
        assert (
            service._submit_finalization_if_due(case.collection_id, now=utc_timestamp_now())
            == "submitted"
        )
        (fresh,) = _receive(service._archive_finalizers, service._archive_execution)
        assert fresh.result().progressed and context.owner is not original
