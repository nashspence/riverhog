from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission, _writer
from riverhog_protocol.errors import Conflict
from sqlalchemy import select

from tests.unit.test_archive_finalizer_registry import _receive, _scope
from tests.unit.test_collection_finalization_plan import _metadata_case


def _terminal(case):
    return ArchiveWriteAdmission(case.factory).select_terminal(
        case.collection_id,
        kind="cancel",
        request={"fixture": "metadata-registration"},
        principal_id="uploader",
        grants=case.service._config.archive_write,
        guard=lambda session: None,
        validate_selection=lambda session, upload: None,
    )


def test_closed_terminal_selection_cannot_install_a_fresh_metadata_constructor(
    tmp_path, monkeypatch
):
    with _metadata_case(tmp_path, monkeypatch, count=1) as (case, claim):
        operation = _terminal(case)
        assert operation and case.service._archive_finalizers._contexts == {}
        with pytest.raises(Conflict, match="closed|unavailable"):
            case.service._submit_finalization(claim)
        key, _ = _scope(case)
        assert case.service._archive_finalizers._contexts == {}
        assert case.service._archive_execution.status(key) == "untracked"
        assert case.calls == []


def _assert_registration_blocked_by_parent(case, claim):
    service = case.service
    with ThreadPoolExecutor(max_workers=1) as workers:
        with session_scope(case.factory) as session:
            _writer(session)
            session.scalar(
                select(Upload.collection_id)
                .where(Upload.collection_id == case.collection_id)
                .with_for_update()
            )
            attempt = workers.submit(service._submit_finalization, claim)
            result = attempt.result(timeout=5)
        assert result == "busy"
    assert service._archive_finalizers._contexts == {}
    key, _ = _scope(case)
    assert service._archive_execution.status(key) == "untracked"


def test_contended_parent_defers_metadata_registration_without_installing_an_owner(
    tmp_path, monkeypatch
):
    with _metadata_case(tmp_path, monkeypatch, count=1) as (case, claim):
        _assert_registration_blocked_by_parent(case, claim)


def _assert_closed_registration(case, claim):
    _terminal(case)
    with pytest.raises(Conflict, match="closed|unavailable"):
        case.service._submit_finalization(claim)
    assert case.service._archive_finalizers._contexts == {}


def test_upload_lock_covers_original_registration_before_terminal_selection(tmp_path, monkeypatch):
    with _metadata_case(tmp_path, monkeypatch, count=1) as (case, claim):
        registry = case.service._archive_finalizers
        entered, release, started, selected = (threading.Event() for _ in range(4))
        actual = registry.submit
        errors, results = [], []

        def hold(*args, **kwargs):
            entered.set()
            assert release.wait(10)
            return actual(*args, **kwargs)

        def submit():
            try:
                results.append(case.service._submit_finalization(claim))
            except BaseException as error:
                errors.append(error)

        def terminal():
            started.set()
            try:

                def validate(session, upload):
                    assert case.collection_id in registry._contexts
                    selected.set()

                ArchiveWriteAdmission(case.factory).select_terminal(
                    case.collection_id,
                    kind="cancel",
                    request={"fixture": "interlocked-metadata-registration"},
                    principal_id="uploader",
                    grants=case.service._config.archive_write,
                    guard=lambda session: None,
                    validate_selection=validate,
                )
            except BaseException as error:
                errors.append(error)

        monkeypatch.setattr(registry, "submit", hold)
        forward = threading.Thread(target=submit)
        closing = threading.Thread(target=terminal)
        try:
            forward.start()
            assert entered.wait(10)
            closing.start()
            assert started.wait(10)
            assert not selected.wait(0.1)
        finally:
            release.set()
            forward.join(10)
            if closing.ident is not None:
                closing.join(10)
        assert not forward.is_alive() and not closing.is_alive() and not errors
        assert results == ["submitted"] and selected.is_set()


def test_closed_gate_resumes_only_original_unacknowledged_workflow_feedback(tmp_path, monkeypatch):
    with _metadata_case(tmp_path, monkeypatch, count=1) as (case, claim):
        service = case.service
        receive = service._requeue_finalization_step
        calls = []

        def lost(selected):
            calls.append(selected)
            receive(selected)
            if len(calls) == 1:
                raise OSError("injected original workflow acknowledgement")

        monkeypatch.setattr(service, "_requeue_finalization_step", lost)
        assert service._submit_finalization(claim) == "submitted"
        (failed,) = _receive(service._archive_finalizers, service._archive_execution)
        with pytest.raises(OSError, match="workflow acknowledgement"):
            failed.result()
        context = service._archive_finalizers._contexts[case.collection_id]
        original = context.owner
        assert original.has_pending
        _terminal(case)

        def forbidden(*args, **kwargs):
            pytest.fail("closed admission cannot construct or advance a fresh child")

        monkeypatch.setattr(service, "_finalization_owner", forbidden)
        monkeypatch.setattr(original._original, "advance", forbidden)
        assert service._submit_finalization(claim) == "submitted"
        (completed,) = _receive(service._archive_finalizers, service._archive_execution)
        assert completed.result().progressed and calls == [claim, claim]
        assert context.owner is original and not original.has_pending and case.calls == []
