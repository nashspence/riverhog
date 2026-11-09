from __future__ import annotations

import threading
from contextlib import contextmanager, nullcontext
from dataclasses import replace
from datetime import timedelta

import pytest
from riverhog_core.archive_execution_tuning import ArchiveExecutionTuning
from riverhog_core.catalog_archive_write_models import ArchiveUploadTerminalRecord as Terminal
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord as Control
from riverhog_core.catalog_db import session_scope, validate_db
from riverhog_core.services import archive_upload_maintenance as maintenance_module
from riverhog_core.services import archive_upload_terminal_due as due_module
from riverhog_core.services import archive_upload_terminal_registry as registry_module
from riverhog_core.services import physical_execution as physical
from riverhog_core.services.archive_upload_maintenance import ArchiveUploadMaintenance
from riverhog_core.services.archive_upload_terminal_due import ArchiveUploadTerminalDue
from riverhog_core.services.archive_upload_terminal_turn import ArchiveUploadTerminalTurn
from riverhog_core.services.physical_execution import PhysicalExecutionKey
from sqlalchemy import insert, select
from time_formats import format_utc_timestamp, utc_now

from tests.unit.test_archive_upload_maintenance import _join
from tests.unit.test_archive_upload_terminal_result import _state
from tests.unit.test_archive_upload_terminal_turn import _drained, _turn_case


def _clock(monkeypatch):
    now, clock = [utc_now() + timedelta(minutes=1)], [100.0]
    monkeypatch.setattr(maintenance_module, "utc_now", lambda: now[0])
    monkeypatch.setattr(due_module, "utc_now", lambda: now[0])
    monkeypatch.setattr(registry_module, "monotonic", lambda: clock[0])

    def advance():
        now[0] += timedelta(minutes=1)
        clock[0] += 60

    return now, advance


def _enable(case, *, service=None):
    service = service or case.service
    due = ArchiveUploadTerminalDue(case.factory, service._archive_terminals)
    service._archive_upload_maintenance = ArchiveUploadMaintenance(
        case.factory,
        service._archive_finalizers,
        service._archive_invocations,
        service._archive_cache_families,
        retry_after=timedelta(seconds=1),
        cache_cleanup=service._cache_cleanup,
        terminal_execution=due,
    )
    return due


def _record(case):
    with session_scope(case.factory) as session:
        terminal = session.get(Terminal, case.operation)
        return terminal.phase, terminal.execution_next_attempt_at, terminal.result_json


def _tick(case, advance):
    advance()
    count = case.service.process_due_archive_uploads(limit=1)
    _join(case.service)
    return count


def _cold(case, monkeypatch, phase):
    _drained(case)
    if phase == "needs-reconfirmation":
        with session_scope(case.factory) as session:
            session.get(Control, case.collection_id).custody_revision = "1"
    guard = case.service._terminal_write_guard(case.collection_id, case.operation)
    decision = case.service._archive_terminal_adjudication.adjudicate(
        case.collection_id, case.operation, guard=guard
    )
    assert decision.phase == ("cleanup-ready" if phase == "cleaning" else phase)
    if phase == "cleaning":
        with session_scope(case.factory) as session:
            session.get(Terminal, case.operation).phase = phase
    assert not case.service._archive_terminals._contexts
    now, advance = _clock(monkeypatch)
    _enable(case)
    original_result = _state(case, case.operation)[2]
    for _ in range(12):
        _tick(case, advance)
        with session_scope(case.factory) as session:
            assert session.get(Terminal, case.operation).original_result_json == original_result
        if phase in {"refused-sealed", "needs-reconfirmation"}:
            if not case.service._archive_retention._reservations:
                break
        else:
            contexts = case.service._archive_terminals._contexts
            if contexts and next(iter(contexts.values())).progress is not None:
                if next(iter(contexts.values())).progress.originals_retired:
                    break
    else:
        pytest.fail("cold terminal operation did not receive bounded original retirement")
    assert _record(case)[0] == phase and _record(case)[2] is None
    assert case.service._archive_retention._counts[False] == 0
    if phase in {"refused-sealed", "needs-reconfirmation"}:
        assert _record(case)[1] is None
        assert not case.service._archive_terminals._contexts
        assert case.service._archive_retention._counts[True] == 0
    else:
        assert _record(case)[1] is not None
        assert case.service._archive_retention._counts[True] == 1
    assert validate_db(case.config.database_url).condition == "current"


@pytest.mark.parametrize("phase", ("cleanup-ready", "cleaning", "needs-reconfirmation"))
def test_cold_terminal_due_resumes_after_global_decision_without_confirming_result(
    tmp_path, monkeypatch, phase
):
    with _turn_case(tmp_path, originals=3) as case:
        _cold(case, monkeypatch, phase)


def _lost_stop(case, monkeypatch):
    _drained(case)
    with session_scope(case.factory) as session:
        session.get(Control, case.collection_id).custody_revision = "1"
    guard = case.service._terminal_write_guard(case.collection_id, case.operation)
    assert (
        case.service._archive_terminal_adjudication.adjudicate(
            case.collection_id, case.operation, guard=guard
        ).phase
        == "needs-reconfirmation"
    )
    from riverhog_core.services import archive_upload_terminal_retirement as retirement_module

    original = retirement_module.session_scope
    registry = case.service._archive_terminals
    calls = []
    create = registry._create

    def make(*args):
        calls.append(args)
        return create(*args)

    monkeypatch.setattr(registry, "_create", make)
    now, advance = _clock(monkeypatch)
    due = _enable(case)

    @contextmanager
    def lose(factory):
        with original(factory) as session:
            yield session
        raise OSError("stopping commit reply lost")

    with monkeypatch.context() as patch:
        patch.setattr(retirement_module, "session_scope", lose)
        assert due.submit_due(limit=1, retry_after=timedelta(seconds=1)) == 1
        _join(case.service)
    assert _record(case)[1] is None
    assert registry.collect(limit=1) == 1
    context = next(iter(registry._contexts.values()))
    assert context.progress is None and context.received
    assert case.service._archive_retention._counts == {False: 0, True: 1}
    original_actor = context.actor
    advance()
    assert due.submit_due(limit=1, retry_after=timedelta(seconds=1)) == 1
    _join(case.service)
    assert registry.collect(limit=1) == 2
    assert context.actor is original_actor and len(calls) == 1
    assert not registry._contexts and not case.service._archive_retention._reservations
    assert _record(case) == ("needs-reconfirmation", None, None)


def test_lost_positive_stopping_reply_resumes_same_original_after_due_row_stops(
    tmp_path, monkeypatch
):
    with _turn_case(tmp_path) as case:
        _lost_stop(case, monkeypatch)


def _lost_due(case, monkeypatch, committed):
    due = _enable(case)
    now, advance = _clock(monkeypatch)
    original = due_module.session_scope
    before = _record(case)

    @contextmanager
    def lose(factory):
        with original(factory) as session:
            yield session
            if not committed:
                raise OSError("due claim rolled back")
        if committed:
            raise OSError("due claim reply lost")

    with monkeypatch.context() as patch:
        patch.setattr(due_module, "session_scope", lose)
        with pytest.raises(OSError, match="due claim"):
            due.submit_due(limit=1, retry_after=timedelta(seconds=1))
    assert not case.service._archive_execution._slots
    assert not case.service._archive_terminals._contexts
    assert case.service._archive_retention._counts[True] == 0
    after = _record(case)
    assert after[0] == before[0] and after[2] == before[2]
    assert (after[1] != before[1]) is committed
    advance()
    assert due.submit_due(limit=1, retry_after=timedelta(seconds=1)) == 1
    _join(case.service)
    assert due.collect(limit=1) == 1


@pytest.mark.parametrize("committed", (False, True))
def test_lost_due_claim_reply_dispatches_no_cold_constructor(tmp_path, monkeypatch, committed):
    with _turn_case(tmp_path, originals=0) as case:
        _lost_due(case, monkeypatch, committed)


def test_due_scheduling_and_pending_progress_never_release_an_unknown_original(
    tmp_path, monkeypatch
):
    with _turn_case(tmp_path, originals=0) as case:
        due = _enable(case)
        now, advance = _clock(monkeypatch)
        calls = []

        def unknown(*args):
            calls.append(args)
            raise OSError("original constructor unknown")

        monkeypatch.setattr(case.service._archive_terminals, "_create", unknown)
        for _ in range(4):
            _tick(case, advance)
        assert len(calls) == 1 and case.service._archive_retention._counts[True] == 1
        assert _record(case)[0] == "draining" and _record(case)[2] is None
        assert due.collect(limit=1) == 0


def test_due_uses_current_actor_even_when_durable_retry_time_is_in_the_future(
    tmp_path, monkeypatch
):
    with _turn_case(tmp_path, originals=0) as case:
        due = _enable(case)
        now, advance = _clock(monkeypatch)
        assert due.submit_due(limit=1, retry_after=timedelta(seconds=1)) == 1
        _join(case.service)
        assert due.collect(limit=1) == 1
        context = next(iter(case.service._archive_terminals._contexts.values()))
        actor = context.actor
        with session_scope(case.factory) as session:
            session.get(
                Terminal, case.operation
            ).execution_next_attempt_at = "9999-01-01T00:00:00.000000000Z"
        advance()
        assert due.submit_due(limit=1, retry_after=timedelta(seconds=1)) == 1
        _join(case.service)
        assert due.collect(limit=1) == 1 and context.actor is actor
        assert _record(case)[2] is None


@contextmanager
def _published_due(case, due, monkeypatch, *, execution_busy=False):
    actual = physical._run
    start, published, release = threading.Event(), threading.Event(), threading.Event()

    def held(work, future):
        assert start.wait(10)
        with case.service._archive_execution._lock if execution_busy else nullcontext():
            actual(work, future)
        published.set()
        assert release.wait(15)

    with monkeypatch.context() as patch:
        patch.setattr(physical, "_run", held)
        try:
            assert due.submit_due(limit=1, retry_after=timedelta(seconds=1)) == 1
            # Semantic observation may conservatively defer while submission owns
            # the shared physical lock. Select the intended interleaving explicitly.
            start.set()
            assert published.wait(10)
            (slot,) = case.service._archive_execution._slots.values()
            assert slot.worker.is_alive() and slot.future.done()
            yield slot.future
        finally:
            release.set()
            _join(case.service)


def test_stopped_semantic_receipt_cannot_retire_a_published_but_live_worker(tmp_path, monkeypatch):
    with _turn_case(tmp_path) as case:
        _drained(case)
        with session_scope(case.factory) as session:
            session.get(Control, case.collection_id).custody_revision = "1"
        due = _enable(case)
        now, advance = _clock(monkeypatch)
        with _published_due(case, due, monkeypatch) as future:
            assert future.result().execution_stopped
            assert _record(case)[1] is None
            assert due.collect(limit=1) == 0
            assert case.service._archive_retention._counts == {False: 0, True: 1}
        assert due.collect(limit=1) == 2
        assert not case.service._archive_retention._reservations
        assert _record(case) == ("needs-reconfirmation", None, None)


def test_busy_physical_observation_preserves_same_original_until_stopped_receiving(
    tmp_path, monkeypatch
):
    with _turn_case(tmp_path) as case:
        _drained(case)
        with session_scope(case.factory) as session:
            session.get(Control, case.collection_id).custody_revision = "1"
        due = _enable(case)
        now, advance = _clock(monkeypatch)
        with _published_due(case, due, monkeypatch, execution_busy=True) as future:
            progress = future.result()
            assert progress.pending_reason == "local-writers" and not progress.execution_stopped
            assert _record(case)[0] == "draining" and _record(case)[1] is not None
            assert due.collect(limit=1) == 0
            assert case.service._archive_retention._counts == {False: 1, True: 1}
        assert due.collect(limit=1) == 1
        context = next(iter(case.service._archive_terminals._contexts.values()))
        actor = context.actor
        assert context.progress is progress and context.received
        assert case.service._archive_retention._counts == {False: 1, True: 1}
        advance()
        with _published_due(case, due, monkeypatch) as future:
            assert future.result().execution_stopped and context.actor is actor
            assert due.collect(limit=1) == 0
            assert case.service._archive_retention._counts == {False: 0, True: 1}
        assert due.collect(limit=1) == 2
        assert not case.service._archive_retention._reservations
        assert _record(case) == ("needs-reconfirmation", None, None)


def test_terminal_execution_due_query_ignores_retained_and_future_backlogs(
    tmp_path, record_property
):
    with _turn_case(tmp_path, originals=0) as case:
        counts, prior = [], 0
        for size in (16, 256, 4096):
            with session_scope(case.factory) as session:
                row = dict(
                    session.execute(select(Terminal.__table__).where(Terminal.id == case.operation))
                    .mappings()
                    .one()
                )
                rows = []
                for offset, phase in (
                    (100000, "done"),
                    (200000, "needs-reconfirmation"),
                    (300000, "cleaning"),
                ):
                    for i in range(prior, size):
                        stopped = phase != "cleaning"
                        rows.append(
                            {
                                **row,
                                "id": format(offset + i, "064x"),
                                "phase": phase,
                                "original_result_json": None,
                                "result_json": "{}" if phase == "done" else None,
                                "execution_next_attempt_at": None
                                if stopped
                                else "9999-01-01T00:00:00.000000000Z",
                            }
                        )
                session.execute(insert(Terminal).execution_options(render_nulls=True), rows)
            prior = size
            instructions = 0
            with session_scope(case.factory) as session:
                connection = session.connection().connection.driver_connection

                def step():
                    nonlocal instructions
                    instructions += 1
                    return 0

                query = due_module._due_query(
                    format_utc_timestamp(utc_now() + timedelta(seconds=1)), 1
                )
                connection.set_progress_handler(step, 1)
                try:
                    (selected,) = session.scalars(query).all()
                finally:
                    connection.set_progress_handler(None, 0)
                assert selected.id == case.operation
                assert set(selected.__dict__) <= {
                    "_sa_instance_state",
                    "id",
                    "collection_id",
                    "execution_next_attempt_at",
                }
                sql = str(query.compile(session.bind, compile_kwargs={"literal_binds": True}))
                plan = " ".join(
                    str(row)
                    for row in session.connection().exec_driver_sql("EXPLAIN QUERY PLAN " + sql)
                )
                assert "ix_archive_upload_terminals_execution_due" in plan
                assert "TEMP B-TREE" not in plan
            counts.append(instructions)
        assert max(counts) <= counts[0] + 4
        record_property("unrelated_backlog_per_kind", [16, 256, 4096])
        record_property("sqlite_due_selection_vm_instructions", counts)


def _shared_fairness(tmp_path, monkeypatch, idle_polls, *, database_url=None):
    from tests.unit.test_archive_finalizer_registry import Finalizer, _scope
    from tests.unit.test_cache_population_cleanup import _abandoned
    from tests.unit.test_cache_population_cleanup_maintenance import _clock as cache_clock
    from tests.unit.test_cache_population_cleanup_maintenance import _service

    with _abandoned(tmp_path, database_url=database_url) as ready:
        cache_advance = cache_clock(monkeypatch)
        now, advance = _clock(monkeypatch)
        with _service(
            ready, monkeypatch, execution=replace(ArchiveExecutionTuning(), terminal_workers=1)
        ) as (service, provider_calls, actors):
            original = Finalizer()
            original.pending = True

            def still_pending(operation_id):
                original.drains.append(operation_id)
                return "pending"

            monkeypatch.setattr(original, "drain_terminal", still_pending)
            key, target = _scope(ready.case)
            assert (
                service._archive_finalizers.submit(
                    key,
                    target_incarnation=target,
                    selection="original",
                    create=lambda: original,
                    advance=lambda owner: False,
                )
                == "submitted"
            )
            _join(service)
            service._archive_finalizers.collect(limit=1)[0].result()

            def make(collection_id, operation_id):
                return ArchiveUploadTerminalTurn(
                    ready.case.factory,
                    collection_id,
                    operation_id,
                    guard=lambda session: session.execute(select(1)),
                    adjudication=service._archive_terminal_adjudication,
                    retirement=service._archive_terminal_retirement,
                )

            monkeypatch.setattr(service._archive_terminals, "_create", make)
            _enable(ready.case, service=service)
            actual, submit = physical._run, service._archive_execution.try_submit
            published, release = threading.Event(), threading.Event()
            accepted = []

            def held(work, future):
                actual(work, future)
                published.set()
                assert release.wait(15)

            def record(key, **kwargs):
                result = submit(key, **kwargs)
                if result == "submitted" and kwargs["terminal"]:
                    accepted.append(key)
                return result

            monkeypatch.setattr(physical, "_run", held)
            monkeypatch.setattr(service._archive_execution, "try_submit", record)
            try:
                for turn in range(4):
                    published.clear()
                    release.clear()
                    advance()
                    cache_advance()
                    service.process_due_archive_uploads(limit=1)
                    assert published.wait(10) and len(accepted) == turn + 1
                    for _ in range(idle_polls):
                        service.process_due_archive_uploads(limit=1)
                        assert len(accepted) == turn + 1
                    release.set()
                    _join(service)
                assert accepted[0] == accepted[3] == key
                assert accepted[2] == PhysicalExecutionKey(
                    ready.case.collection_id, ready.operation
                )
                assert accepted[1] not in {key, accepted[2]}
                assert len(provider_calls) == 1 and len(actors) == 1
                with session_scope(ready.case.factory) as session:
                    assert session.get(Terminal, ready.operation).phase == "draining"
                    assert session.get(Terminal, ready.operation).result_json is None
            finally:
                release.set()


@pytest.mark.parametrize("idle_polls", (1, 2, 3))
def test_one_shared_terminal_worker_rotates_only_on_actual_admission_across_three_owners(
    tmp_path, monkeypatch, idle_polls
):
    _shared_fairness(tmp_path, monkeypatch, idle_polls)


def _cold_refused(tmp_path, monkeypatch, *, database_url=None):
    from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord as Volume

    from tests.unit.test_archive_upload_terminal_retirement import _failed_forward
    from tests.unit.test_archive_upload_terminal_tags import _tagged
    from tests.unit.test_upload_cancel_authority import _select as select_cancel
    from tests.unit.test_upload_terminal_read_authority import _creator_key

    with _tagged(tmp_path, count=3, database_url=database_url) as case:
        case.registries = [_failed_forward(case, "metadata")]
        _, principal, _ = _creator_key(case)
        case.operation = select_cancel(
            case, principal, case.service._cancel_write_guard(case.collection_id, principal)
        )
        with session_scope(case.factory) as session:
            session.get(Volume, (case.collection_id, case.object_id)).state = "sealed"
        _cold(case, monkeypatch, "refused-sealed")


def test_cold_late_sealed_cancel_stops_only_after_original_receiving(tmp_path, monkeypatch):
    _cold_refused(tmp_path, monkeypatch)
