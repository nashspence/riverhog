from __future__ import annotations

import json
import threading
from datetime import timedelta
from types import SimpleNamespace

import pytest
from riverhog_core.catalog_archive_write_models import (
    ArchiveUploadTerminalRecord as Terminal,
)
from riverhog_core.catalog_archive_write_models import (
    ArchiveWriteControlRecord as Control,
)
from riverhog_core.catalog_db import session_scope
from riverhog_core.services import archive_upload_maintenance as maintenance_module
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from sqlalchemy import event, func, insert, select
from time_formats import format_utc_timestamp, utc_now

from tests.unit.test_archive_finalizer_registry import Finalizer, _scope
from tests.unit.test_archive_finalizer_terminal_drain import _original, _terminal
from tests.unit.test_archive_finalizer_work import _service, _submit
from tests.unit.test_archive_write_admission import _case, _selected


def _clock(monkeypatch):
    now = [utc_now() + timedelta(seconds=1)]
    monkeypatch.setattr(maintenance_module, "utc_now", lambda: now[0])
    return now


def _join(service):
    for slot in tuple(service._archive_execution._slots.values()):
        slot.worker.join(timeout=10)
        assert not slot.worker.is_alive()


def _record(case, operation):
    with session_scope(case.factory) as session:
        row = session.get(Terminal, operation)
        return (row.phase, row.result_json, row.drain_next_attempt_at)


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_service_maintenance_keeps_actual_original_armed_work_until_joined_drain(
    tmp_path, monkeypatch, mode
):
    _actual_maintenance(tmp_path, monkeypatch, mode)


def _actual_maintenance(tmp_path, monkeypatch, mode, *, database_url=None):
    with (
        _original(
            tmp_path, monkeypatch, "description", mode, database_url=database_url
        ) as original,
        _service(original) as service,
    ):
        entered, release = threading.Event(), threading.Event()
        actual = original.ports.execute_write
        calls = []

        def held(invocation, *, content):
            calls.append(invocation)
            entered.set()
            assert release.wait(10)
            return actual(invocation, content=content)

        monkeypatch.setattr(original.ports, "execute_write", held)
        try:
            assert _submit(service, original) == "submitted"
            assert entered.wait(5)
            operation = _terminal(original)
            now = _clock(monkeypatch)
            key, target = _scope(original.case)
            context = service._archive_finalizers._contexts[original.case.collection_id]
            slot = service._archive_execution._slots[original.case.collection_id]

            def forbidden(*args, **kwargs):
                pytest.fail("coordination cannot join a live original worker")

            with monkeypatch.context() as patch:
                patch.setattr(slot.worker, "join", forbidden)
                assert service.process_due_archive_uploads() == 0
                assert slot.worker.is_alive() and len(calls) == 1
                assert service.process_due_archive_uploads() == 0
            assert context.terminal_id == operation and context.owner is not None
            assert _record(original.case, operation)[:2] == ("draining", None)
            release.set()
            _join(service)
            # The original forward outcome is received before another physical
            # turn can use this construction's slot.
            assert service.process_due_archive_uploads() == 1
            assert service._archive_execution.status(key) == "untracked"
            original_owner = context.owner
            now[0] += service._config.archive_upload_sweep_interval
            assert service.process_due_archive_uploads() == 1
            _join(service)
            assert service.process_due_archive_uploads() == 1
            assert context.drained and context.owner is original_owner
            assert _record(original.case, operation)[:2] == ("draining", None)
            assert service._archive_execution.status(key) == "untracked"
            assert not service._archive_finalizers.retire_idle(key, target_incarnation=target)
            # A local drained result never becomes cleanup or global success.
            assert service.process_due_archive_uploads() == 0 and len(calls) == 1
        finally:
            release.set()


@pytest.mark.parametrize("error", (OSError, KeyboardInterrupt, SystemExit))
def test_failed_original_worker_is_received_without_erasing_constructor_uncertainty(
    tmp_path, monkeypatch, caplog, error
):
    with _case(tmp_path) as case:
        service = case.service
        key, target = _scope(case)

        def failed():
            raise error("worker failure data")

        assert (
            service._archive_finalizers.submit(
                key,
                target_incarnation=target,
                selection="original",
                create=failed,
                advance=lambda owner: pytest.fail("constructor outcome is unknown"),
            )
            == "submitted"
        )
        _join(service)
        operation = _selected(case)
        now = _clock(monkeypatch)
        assert service.process_due_archive_uploads() == 2
        _join(service)
        assert service.process_due_archive_uploads() == 1
        context = service._archive_finalizers._contexts[case.collection_id]
        assert not context.drained and context.construction_failure is not None
        assert error.__name__ in caplog.text
        before = _record(case, operation)
        assert service.process_due_archive_uploads() == 0
        assert _record(case, operation) == before
        now[0] += service._config.archive_upload_sweep_interval
        assert service.process_due_archive_uploads() == 1
        _join(service)
        assert service.process_due_archive_uploads() == 1
        assert not context.drained and _record(case, operation)[:2] == ("draining", None)


def test_due_scheduling_cannot_certify_an_untracked_original_or_readjudicate_a_result(
    tmp_path, monkeypatch
):
    with _case(tmp_path) as case:
        operation = _selected(case)
        _clock(monkeypatch)
        statements = []
        engine = case.factory.kw["bind"]

        def capture(connection, cursor, statement, parameters, context, executemany):
            statements.append(statement)

        event.listen(engine, "before_cursor_execute", capture)
        try:
            assert case.service.process_due_archive_uploads() == 0
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        assert _record(case, operation)[:2] == ("draining", None)
        assert not case.service._archive_execution._slots
        updates = [s for s in statements if s.lstrip().upper().startswith("UPDATE")]
        assert len(updates) == 1 and "drain_next_attempt_at" in updates[0]
        assert "result_json" not in updates[0] and "phase=" not in updates[0]
        assert not any(s.lstrip().upper().startswith(("DELETE", "INSERT")) for s in statements)
        assert case.service.process_due_archive_uploads() == 0


@pytest.mark.parametrize("committed", (False, True))
def test_lost_due_schedule_reply_does_not_speculatively_dispatch(tmp_path, monkeypatch, committed):
    from contextlib import contextmanager

    with _case(tmp_path) as case:
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
        _join(service)
        service._archive_finalizers.collect(limit=1)[0].result()
        operation = _selected(case)
        now = _clock(monkeypatch)
        actual = maintenance_module.session_scope

        @contextmanager
        def lost(factory):
            with actual(factory) as session:
                yield session
                if not committed:
                    raise OSError("due schedule rollback")
            if committed:
                raise OSError("due schedule reply loss")

        with monkeypatch.context() as patch:
            patch.setattr(maintenance_module, "session_scope", lost)
            with pytest.raises(OSError, match="due schedule"):
                service.process_due_archive_uploads()
        assert original.drains == [] and not service._archive_execution._slots
        before = _record(case, operation)
        assert service.process_due_archive_uploads() == int(not committed)
        if committed:
            assert _record(case, operation) == before and original.drains == []
            now[0] += service._config.archive_upload_sweep_interval
            assert service.process_due_archive_uploads() == 1
        _join(service)
        service.process_due_archive_uploads()
        assert original.drains == [operation]
        assert _record(case, operation)[:2] == ("draining", None)


def test_terminal_backoff_and_capacity_leave_an_independent_construction_progressing(
    tmp_path, monkeypatch
):
    with _case(tmp_path) as case:
        service = case.service
        first_key, target = _scope(case)
        entered, release = threading.Event(), threading.Event()
        first = Finalizer()

        def held(owner):
            entered.set()
            assert release.wait(10)
            return True

        try:
            assert (
                service._archive_finalizers.submit(
                    first_key,
                    target_incarnation=target,
                    selection="held",
                    create=lambda: first,
                    advance=held,
                )
                == "submitted"
            )
            assert entered.wait(5)
            _selected(case)
            from tests.unit.test_collection_uploads import _CREATOR

            other = service.create_or_resume(
                idempotency_key="independent-terminal",
                ingest_source="fixture",
                archive_store=None,
                initiator=_CREATOR,
                event_context=None,
            )
            other_case = SimpleNamespace(
                factory=case.factory,
                collection_id=int(other["collection_id"]),
                owner=ArchiveWriteAdmission(case.factory),
            )
            other_key, other_target = _scope(other_case)
            second = Finalizer()
            assert (
                service._archive_finalizers.submit(
                    other_key,
                    target_incarnation=other_target,
                    selection="other",
                    create=lambda: second,
                    advance=lambda owner: True,
                )
                == "submitted"
            )
            other_slot = service._archive_execution._slots[other_key.collection_id]
            other_slot.worker.join(timeout=10)
            assert not other_slot.worker.is_alive()
            _selected(other_case)
            _clock(monkeypatch)
            for _ in range(3):
                service.process_due_archive_uploads(limit=2)
                for slot in tuple(service._archive_execution._slots.values()):
                    if slot.key == other_key:
                        slot.worker.join(timeout=10)
            context = service._archive_finalizers._contexts[other_key.collection_id]
            assert context.drained and second.drains
            assert entered.is_set() and not release.is_set()
            assert service._archive_execution.status(first_key) == "running"
        finally:
            release.set()


def test_due_selector_work_is_independent_of_settled_and_future_backlog(tmp_path, record_property):
    with _case(tmp_path) as case:
        operation = _selected(case)
        counts = []
        prior = 0
        for size in (16, 256, 4096):
            with session_scope(case.factory) as session:
                row = (
                    session.execute(select(Terminal.__table__).where(Terminal.id == operation))
                    .mappings()
                    .one()
                )
                rows = [
                    {
                        **dict(row),
                        "id": format(i, "064x"),
                        "phase": "done",
                        "result_json": "{}",
                        "drain_next_attempt_at": "1970-01-01T00:00:00.000000000Z",
                    }
                    for i in range(prior, size)
                ]
                rows += [
                    {
                        **dict(row),
                        "id": format(i + 50000, "064x"),
                        "phase": "draining",
                        "drain_next_attempt_at": "9999-01-01T00:00:00.000000000Z",
                    }
                    for i in range(prior, size)
                ]
                session.execute(insert(Terminal), rows)
            prior = size
            with session_scope(case.factory) as session:
                assert (
                    session.scalar(
                        select(func.count()).select_from(Terminal).where(Terminal.phase == "done")
                    )
                    == size
                )
                assert (
                    session.scalar(
                        select(func.count())
                        .select_from(Terminal)
                        .where(Terminal.drain_next_attempt_at == "9999-01-01T00:00:00.000000000Z")
                    )
                    == size
                )
            instructions = 0
            with session_scope(case.factory) as session:
                connection = session.connection().connection.driver_connection

                def step():
                    nonlocal instructions
                    instructions += 1
                    return 0

                now = format_utc_timestamp(utc_now() + timedelta(seconds=1))
                connection.set_progress_handler(step, 1)
                try:
                    selected = list(session.scalars(maintenance_module._due_query(now, 1)))
                finally:
                    connection.set_progress_handler(None, 0)
                assert len(selected) == 1 and selected[0].id == operation
                assert set(selected[0].__dict__) <= {
                    "_sa_instance_state",
                    "id",
                    "collection_id",
                    "drain_next_attempt_at",
                }
                sql = str(
                    maintenance_module._due_query(now, 1).compile(
                        session.get_bind(), compile_kwargs={"literal_binds": True}
                    )
                )
                plan = session.connection().exec_driver_sql("EXPLAIN QUERY PLAN " + sql).all()
                assert any(
                    "SEARCH" in r[3] and "ix_archive_upload_terminals_draining_due" in r[3]
                    for r in plan
                ), plan
            counts.append(instructions)
        assert max(counts) <= counts[0] + 4
        record_property("unrelated_backlog_per_kind", [16, 256, 4096])
        record_property("sqlite_due_selection_vm_instructions", counts)


def test_changed_construction_cannot_fence_or_replace_the_original_local_owner(
    tmp_path, monkeypatch
):
    from riverhog_protocol.errors import Conflict

    with _case(tmp_path) as case:
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
        _join(service)
        service._archive_finalizers.collect(limit=1)[0].result()
        operation = _selected(case)
        _clock(monkeypatch)
        with session_scope(case.factory) as session:
            row = session.get(Control, case.collection_id)
            value = json.loads(row.construction_json)
            value["archive_generation"] = "changed"
            row.construction_json = json.dumps(value, sort_keys=True, separators=(",", ":"))
        with pytest.raises(Conflict, match="another original construction"):
            service.process_due_archive_uploads()
        context = service._archive_finalizers._contexts[case.collection_id]
        assert context.owner is original and context.terminal_id is None
        assert original.drains == [] and _record(case, operation)[:2] == ("draining", None)


def test_sqlite_due_selection_defers_a_real_writer_without_changing_its_deadline(
    tmp_path, monkeypatch
):
    from concurrent.futures import ThreadPoolExecutor

    with _case(tmp_path) as case:
        operation = _selected(case)
        _clock(monkeypatch)
        before = _record(case, operation)
        engine = case.factory.kw["bind"]
        with ThreadPoolExecutor(max_workers=1) as pool:
            with engine.connect().execution_options(sqlite_immediate_writer=True) as holder:
                with holder.begin():
                    future = pool.submit(case.service.process_due_archive_uploads)
                    assert future.result(timeout=3) == 0
                    assert _record(case, operation) == before
        assert case.service.process_due_archive_uploads() == 0
        assert _record(case, operation)[2] > before[2]
        assert not case.service._archive_execution._slots


def test_unexpected_catalog_error_is_not_classified_as_busy_or_success(tmp_path, monkeypatch):
    import sqlite3

    from sqlalchemy.exc import OperationalError

    with _case(tmp_path) as case:
        operation = _selected(case)
        _clock(monkeypatch)
        before = _record(case, operation)

        def failed(*args, **kwargs):
            raise OperationalError(
                "fixture statement", {}, sqlite3.OperationalError("fixture error")
            )

        monkeypatch.setattr(maintenance_module, "_writer", failed)
        with pytest.raises(OperationalError, match="fixture error"):
            case.service.process_due_archive_uploads()
        assert _record(case, operation) == before
        assert not case.service._archive_execution._slots


def _actual_payload_maintenance(
    tmp_path, monkeypatch, kind, mode, *, database_url=None, held=False
):
    from tests.unit.test_archive_payload_unit import _select_terminal, _service_actor

    with _service_actor(tmp_path, monkeypatch, kind, mode, database_url=database_url) as f:
        entered, release = threading.Event(), threading.Event()
        execute = f.executor.execute_write
        calls = []

        def provider(invocation, *, content=b""):
            calls.append(invocation)
            if held and invocation.operation.kind == "begin":
                entered.set()
                assert release.wait(10)
            return execute(invocation, content=content)

        monkeypatch.setattr(f.executor, "execute_write", provider)
        try:
            assert f.submit() == "submitted"
            if held:
                assert entered.wait(5)
                operation = _select_terminal(f)
                now = _clock(monkeypatch)
                slot = f.execution._slots[f.key.collection_id]
                with monkeypatch.context() as patch:
                    patch.setattr(
                        slot.worker,
                        "join",
                        lambda *args: pytest.fail("maintenance joined a live payload worker"),
                    )
                    assert f.service.process_due_archive_uploads() == 0
                    assert slot.worker.is_alive()
                assert not f.payload.released
                release.set()
                _join(f.service)
                assert f.service.process_due_archive_uploads() == 1
                assert not f.payload.released
                before = list(calls)
                now[0] += f.service._config.archive_upload_sweep_interval
                assert f.service.process_due_archive_uploads() == 1
                _join(f.service)
                assert f.service.process_due_archive_uploads() == 1
                context = f.registry._invocations[f.request]
                assert context.drained and f.payload.released
                assert calls == before
                assert _record(f.case, operation)[:2] == ("draining", None)
                assert len(f.service._archive_retention._reservations) == 1
            else:
                _join(f.service)
                assert f.service.process_due_archive_uploads() == 2
                assert (
                    f.service.get_unit(f.key.collection_id, f.case.object_id, 0)["state"]
                    == "committed"
                )
                assert f.payload.released and not f.registry._invocations
                assert not f.service._archive_retention._reservations
                assert f.service.process_due_archive_uploads() == 0
        finally:
            release.set()


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("held", (False, True))
def test_actual_service_maintenance_receives_payloads_and_preserves_terminal_ownership(
    tmp_path, monkeypatch, kind, mode, held
):
    _actual_payload_maintenance(tmp_path, monkeypatch, kind, mode, held=held)


def _fair_terminal_maintenance(tmp_path, monkeypatch, *, database_url=None):
    from tests.unit.test_archive_upload_invocations import Original
    from tests.unit.test_collection_uploads import _CREATOR

    class PendingFinalizer(Finalizer):
        def drain_terminal(self, operation_id):
            self.drains.append(operation_id)
            return "pending"

    with (
        _case(tmp_path, database_url=database_url) as case,
        _service(
            SimpleNamespace(owner=case.service, case=case),
            terminal_workers_per_target=2,
        ) as service,
    ):
        cases = [case]
        other = service.create_or_resume(
            idempotency_key="fair-terminal",
            ingest_source="fixture",
            archive_store=None,
            initiator=_CREATOR,
            event_context=None,
        )
        cases.append(
            SimpleNamespace(
                collection_id=int(other["collection_id"]),
                factory=case.factory,
                owner=ArchiveWriteAdmission(case.factory),
            )
        )
        owners = []
        for number, current in enumerate(cases):
            key, target = _scope(current)
            metadata, payload = PendingFinalizer(), Original()
            metadata.pending = payload.pending = True
            assert (
                service._archive_finalizers.submit(
                    key,
                    target_incarnation=target,
                    selection="original",
                    create=lambda metadata=metadata: metadata,
                    advance=lambda a: True,
                )
                == "submitted"
            )
            _join(service)
            service._archive_finalizers.collect(limit=1)[0].result()
            request = format(number + 1, "064x")
            assert (
                service._archive_invocations.submit(
                    key,
                    target_incarnation=target,
                    request_sha256=request,
                    create=lambda payload=payload: payload,
                    advance=lambda a: True,
                )
                == "submitted"
            )
            _join(service)
            service._archive_invocations.collect(limit=1)[0].result()
            operation = _selected(current)
            owners.append((metadata, payload, operation))
        now = _clock(monkeypatch)
        for turn in range(4):
            assert service.process_due_archive_uploads(limit=2) == 2
            _join(service)
            assert service.process_due_archive_uploads(limit=2) == 2
            for metadata, payload, operation in owners:
                assert len(payload.drains) == (turn + 2) // 2
                assert len(metadata.drains) == (turn + 1) // 2
                assert _record(case, operation)[:2] == ("draining", None)
            now[0] += service._config.archive_upload_sweep_interval
        assert len(service._archive_retention._reservations) == 4


def test_each_construction_alternates_pending_payload_and_metadata_even_in_even_batches(
    tmp_path, monkeypatch
):
    _fair_terminal_maintenance(tmp_path, monkeypatch)
