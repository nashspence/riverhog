from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord as Control
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission, _writer
from riverhog_protocol.errors import Conflict
from sqlalchemy import select

from tests.unit.test_archive_finalizer_registration import _terminal
from tests.unit.test_archive_finalizer_registry import Finalizer, _receive, _scope
from tests.unit.test_archive_write_admission import _case


def _idle(case):
    service = case.service
    key, target = _scope(case)
    original = Finalizer()
    assert (
        service._archive_finalizers.submit(
            key,
            target_incarnation=target,
            selection="original",
            create=lambda: original,
            advance=lambda owner: owner.advance(),
        )
        == "submitted"
    )
    (receipt,) = _receive(service._archive_finalizers, service._archive_execution)
    assert receipt.result().progressed
    return key, target, service._archive_finalizers._contexts[key.collection_id]


def _assert_contended_retirement(case, table):
    key, target, context = _idle(case)
    maintenance = case.service._archive_upload_maintenance
    with ThreadPoolExecutor(max_workers=1) as workers:
        with session_scope(case.factory) as session:
            _writer(session)
            column = table.collection_id
            session.scalar(select(column).where(column == case.collection_id).with_for_update())
            attempt = workers.submit(maintenance._retire_finalizer, key, target)
            assert attempt.result(timeout=5) is False
        assert case.service._archive_finalizers._contexts[key.collection_id] is context
    assert maintenance._retire_finalizer(key, target) is True
    assert not case.service._archive_finalizers._contexts


def test_sqlite_writer_defers_idle_retirement_without_releasing_its_original(tmp_path):
    with _case(tmp_path) as case:
        _assert_contended_retirement(case, Upload)


def _assert_closed_retirement(case):
    key, target, context = _idle(case)
    operation = _terminal(case)
    assert context.terminal_id is None
    assert case.service._archive_upload_maintenance._retire_finalizer(key, target) is False
    assert case.service._archive_finalizers._contexts[key.collection_id] is context
    assert context.terminal_id is None and not context.drained and operation


def test_closed_terminal_keeps_an_idle_owner_before_its_local_fence_is_installed(tmp_path):
    with _case(tmp_path) as case:
        _assert_closed_retirement(case)


def _assert_atomic_retirement(case, monkeypatch):
    key, target, context = _idle(case)
    registry = case.service._archive_finalizers
    actual = registry.retire_idle
    entered, release, started, selected = (threading.Event() for _ in range(4))
    results, errors = [], []

    def held(*args, **kwargs):
        entered.set()
        assert release.wait(10)
        return actual(*args, **kwargs)

    def retire():
        try:
            results.append(case.service._archive_upload_maintenance._retire_finalizer(key, target))
        except BaseException as error:
            errors.append(error)

    def terminal():
        started.set()
        try:

            def validate(session, upload):
                assert key.collection_id not in registry._contexts
                selected.set()

            ArchiveWriteAdmission(case.factory).select_terminal(
                case.collection_id,
                kind="cancel",
                request={"fixture": "interlocked-finalizer-retirement"},
                principal_id="uploader",
                grants=case.service._config.archive_write,
                guard=lambda session: None,
                validate_selection=validate,
            )
        except BaseException as error:
            errors.append(error)

    monkeypatch.setattr(registry, "retire_idle", held)
    retiring = threading.Thread(target=retire)
    closing = threading.Thread(target=terminal)
    try:
        retiring.start()
        assert entered.wait(10)
        closing.start()
        assert started.wait(10)
        assert not selected.wait(0.1)
    finally:
        release.set()
        retiring.join(10)
        if closing.ident is not None:
            closing.join(10)
    assert not retiring.is_alive() and not closing.is_alive() and not errors
    assert results == [True] and selected.is_set()
    assert context.owner is not None and not registry._contexts


def test_parent_lock_covers_idle_release_before_terminal_selection(tmp_path, monkeypatch):
    with _case(tmp_path) as case:
        _assert_atomic_retirement(case, monkeypatch)


def _assert_changed_retirement(case):
    key, target, context = _idle(case)
    with session_scope(case.factory) as session:
        session.get(Control, key.collection_id).construction_json += " "
    with pytest.raises(Conflict, match="retirement construction changed"):
        case.service._archive_upload_maintenance._retire_finalizer(key, target)
    assert case.service._archive_finalizers._contexts[key.collection_id] is context


def test_idle_retirement_requires_its_original_construction_identity(tmp_path):
    with _case(tmp_path) as case:
        _assert_changed_retirement(case)


def test_catalog_retirement_callback_runs_outside_the_registry_lock(tmp_path, monkeypatch):
    with _case(tmp_path) as case:
        key, target, context = _idle(case)
        registry = case.service._archive_finalizers
        maintenance = case.service._archive_upload_maintenance
        actual, checked = maintenance._retire_finalizer, []

        def retire(chosen, incarnation):
            assert registry._lock.acquire(blocking=False)
            registry._lock.release()
            checked.append((chosen, incarnation))
            return actual(chosen, incarnation)

        monkeypatch.setattr(maintenance, "_retire_finalizer", retire)
        assert case.service.process_due_archive_uploads() == 1
        assert checked == [(key, target)] and not registry._contexts
        assert context.owner is not None
