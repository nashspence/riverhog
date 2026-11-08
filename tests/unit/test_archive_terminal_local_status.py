from __future__ import annotations

import threading
from types import SimpleNamespace

import pytest
from riverhog_core.archive_execution_tuning import ArchiveExecutionTuning
from riverhog_core.catalog_db import session_scope
from riverhog_core.services import archive_upload_execution as execution_module
from riverhog_core.services.archive_construction_retention import ArchiveConstructionRetention
from riverhog_core.services.archive_finalizer_registry import (
    ArchiveFinalizerRegistry,
    ArchiveFinalizerTurn,
)
from riverhog_core.services.archive_upload_execution import ArchiveUploadExecutionOwner
from riverhog_core.services.archive_upload_invocations import ArchiveUploadInvocationRegistry
from riverhog_core.services.archive_write_admission import _writer
from riverhog_protocol.errors import Conflict
from sqlalchemy import event

from tests.unit.test_archive_finalizer_registry import _operation, _scope
from tests.unit.test_archive_upload_invocations import Original
from tests.unit.test_archive_write_admission import _case


@pytest.fixture(params=("metadata", "payload"))
def retained(tmp_path, request):
    with _case(tmp_path) as case:
        tuning = ArchiveExecutionTuning()
        pool = ArchiveUploadExecutionOwner(tuning)
        budget = ArchiveConstructionRetention(tuning)
        registry = (
            ArchiveFinalizerRegistry(case.factory, pool, budget)
            if request.param == "metadata"
            else ArchiveUploadInvocationRegistry(case.factory, pool, budget)
        )
        key, target = _scope(case)

        def submit(create=Original):
            values = {"target_incarnation": target, "create": create, "advance": lambda owner: True}
            if request.param == "metadata":
                return registry.submit(key, selection="fixture", **values)
            return registry.submit(key, request_sha256="c" * 64, **values)

        releases = []
        try:
            yield SimpleNamespace(
                case=case,
                pool=pool,
                registry=registry,
                key=key,
                target=target,
                submit=submit,
                releases=releases,
            )
        finally:
            for release in releases:
                release.set()
            registry.close()


def _receive(f):
    for slot in tuple(f.pool._slots.values()):
        slot.worker.join(timeout=10)
        assert not slot.worker.is_alive()
    (completion,) = f.registry.collect(limit=100)
    result = completion.result()
    return result.progressed if isinstance(result, ArchiveFinalizerTurn) else result


def _status(f, operation):
    return f.registry.local_terminal_status(
        f.key, target_incarnation=f.target, operation_id=operation
    )


def test_local_observation_under_catalog_lock_never_queries_or_installs(retained):
    f = retained
    operation = _operation(f.case)
    queries = []
    engine = f.case.factory.kw["bind"]

    def observed(*args):
        queries.append(args[2])

    with session_scope(f.case.factory) as session:
        _writer(session)
        event.listen(engine, "before_cursor_execute", observed)
        try:
            assert _status(f, operation) == "untracked"
            with f.registry._lock:
                assert _status(f, operation) == "busy"
        finally:
            event.remove(engine, "before_cursor_execute", observed)
    assert queries == [] and f.pool._slots == {}
    assert f.registry._retention._reservations == {}


def test_local_observation_does_not_fence_an_existing_original(retained):
    f = retained
    assert f.submit() == "submitted" and _receive(f) is True
    operation = _operation(f.case)
    assert _status(f, operation) == "unfenced"
    assert _status(f, operation) == "unfenced"
    assert (
        f.registry.fence_terminal(f.key, target_incarnation=f.target, operation_id=operation)
        == "fenced"
    )
    assert _status(f, operation) == "fenced"


def test_closed_semantic_drain_still_waits_for_actual_worker_exit_and_receiving(
    retained, monkeypatch
):
    f = retained
    assert f.submit() == "submitted" and _receive(f) is True
    operation = _operation(f.case)
    assert (
        f.registry.fence_terminal(f.key, target_incarnation=f.target, operation_id=operation)
        == "fenced"
    )
    published, release = threading.Event(), threading.Event()
    f.releases.append(release)
    actual = execution_module._run

    def held(work, future):
        actual(work, future)
        published.set()
        assert release.wait(10)

    monkeypatch.setattr(execution_module, "_run", held)
    assert (
        f.registry.submit_terminal(f.key, target_incarnation=f.target, operation_id=operation)
        == "submitted"
    )
    assert published.wait(10)
    assert _status(f, operation) == "fenced"
    assert f.registry.collect(limit=1) == ()
    release.set()
    assert _receive(f) == "closed"
    queries = []
    engine = f.case.factory.kw["bind"]

    def observed(*args):
        queries.append(args[2])

    with session_scope(f.case.factory) as session:
        _writer(session)
        event.listen(engine, "before_cursor_execute", observed)
        try:
            assert _status(f, operation) == "drained"
            with f.pool._lock:
                assert _status(f, operation) == "busy"
            with pytest.raises(Conflict, match="another terminal request"):
                _status(f, "f" * 64 if operation != "f" * 64 else "e" * 64)
        finally:
            event.remove(engine, "before_cursor_execute", observed)
    assert queries == []


def test_unknown_constructor_keeps_local_terminal_pending_after_join(retained):
    f = retained
    calls = []

    def create():
        calls.append("constructor")
        raise OSError("injected unknown original constructor")

    assert f.submit(create) == "submitted"
    with pytest.raises(OSError, match="unknown original constructor"):
        _receive(f)
    operation = _operation(f.case)
    assert (
        f.registry.fence_terminal(f.key, target_incarnation=f.target, operation_id=operation)
        == "fenced"
    )
    assert (
        f.registry.submit_terminal(f.key, target_incarnation=f.target, operation_id=operation)
        == "submitted"
    )
    assert _receive(f) == "pending"
    assert f.pool.status(f.key) == "untracked"
    assert _status(f, operation) == "fenced" and calls == ["constructor"]
