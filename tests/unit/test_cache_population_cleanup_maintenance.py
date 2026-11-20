from __future__ import annotations

import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
from functools import partial

import pytest
from riverhog_core.archive_execution_tuning import ArchiveExecutionTuning
from riverhog_core.catalog_cache_write_models import CachePopulationDispositionRecord as Disposition
from riverhog_core.catalog_db import session_scope, validate_db
from riverhog_core.catalog_models import (
    RetrievalCachePopulationRecord as Population,
)
from riverhog_core.catalog_models import (
    RetrievalCacheStoreAccountingRecord as Accounting,
)
from riverhog_core.cleanup_execution_tuning import CleanupExecutionTuning
from riverhog_core.services import archive_upload_maintenance as maintenance_module
from riverhog_core.services import cache_population_cleanup_registry as registry_module
from riverhog_core.services.collection_uploads import SqlAlchemyCollectionUploadService
from riverhog_core.services.physical_execution import PhysicalExecutionKey
from riverhog_protocol.errors import Conflict
from sqlalchemy import text
from time_formats import utc_now

from tests.unit.test_archive_upload_maintenance import _join
from tests.unit.test_cache_population_cleanup import GRANTS, _abandoned, _put, _retained_data
from tests.unit.test_cache_population_cleanup_turn import _complete, _turn


def _clock(monkeypatch):
    now, clock = [utc_now() + timedelta(minutes=1)], [100.0]
    monkeypatch.setattr(maintenance_module, "utc_now", lambda: now[0])
    monkeypatch.setattr(registry_module, "utc_now", lambda: now[0])
    monkeypatch.setattr(registry_module, "monotonic", lambda: clock[0])

    def advance():
        now[0] += timedelta(minutes=1)
        clock[0] += 60

    return advance


@contextmanager
def _service(ready, monkeypatch, *, create=None, execution=None, cleanup=None):
    case = ready.case
    config = replace(case.config, archive_execution=execution or ArchiveExecutionTuning())
    construction = create or partial(
        case.cache.native_population_cleanup_turn,
        grants=GRANTS,
        tuning=cleanup or CleanupExecutionTuning(),
    )
    calls, actors = [], []

    def make(receipt):
        assert threading.current_thread().name == "physical-execution"
        actor = construction(receipt)
        actors.append(actor)
        return actor

    execute = case.candidate.execute_cleanup

    def record(invocation):
        assert threading.current_thread().name == "physical-execution"
        calls.append(invocation)
        return execute(invocation)

    monkeypatch.setattr(case.candidate, "execute_cleanup", record)
    service = SqlAlchemyCollectionUploadService(
        config,
        case.service._archive_stores,
        retrieval_cache=case.cache,
        cache_cleanup_create=make,
        session_factory=case.factory,
    )
    try:
        yield service, calls, actors
    finally:
        service.close()


def _finished(ready, service):
    with session_scope(ready.case.factory) as session:
        return (
            session.get(
                Disposition, hashlib.sha256(ready.receipt.receipt_json.encode()).hexdigest()
            )
            is not None
            and not service._cache_cleanup._contexts
            and not service._archive_execution._slots
        )


def _drain(ready, service, advance, *, maximum_turns=16):
    for _ in range(maximum_turns):
        advance()
        service.process_due_archive_uploads(limit=1)
        _join(service)
        if _finished(ready, service):
            return
    raise AssertionError("native cache maintenance did not receive its positive disposition")


def _assert_service_cleanup(ready, monkeypatch, *, paginated=False, disabled=False):
    if paginated:
        # Two real candidates of different kinds share this exact cache leaf.
        _put(ready, ready.value["object_path"])
    if disabled:
        ready.case.cache._registrations[ready.case.candidate.name] = replace(
            ready.case.registration, admission_enabled=False
        )
        assert ready.case.cache.population_candidates() == ()
    advance = _clock(monkeypatch)
    tuning = replace(CleanupExecutionTuning(), inventory_items=1) if paginated else None
    with _service(ready, monkeypatch, cleanup=tuning) as (service, calls, actors):
        _drain(ready, service, advance)
        assert len(actors) == 1 and actors[0].outcome is not None
        assert not service._archive_retention._reservations
        assert [call.request.kind for call in calls] == (
            ["inventory", "dispose", "inventory", "dispose", "inventory"]
            if paginated
            else ["inventory", "dispose"]
        )
        assert len({call.call.call_id for call in calls}) == len(calls)
        if paginated:
            inventories = [call.request for call in calls if call.request.kind == "inventory"]
            assert inventories[0].inventory.continuation is None
            assert inventories[1].inventory.continuation is not None
            # A full final page is not exhaustion; its bounded empty successor is.
            assert len(inventories) == 3
            assert inventories[2].inventory.continuation not in {
                None,
                inventories[1].inventory.continuation,
            }
            disposed = [
                candidate
                for call in calls
                if call.request.kind == "dispose"
                for candidate in call.request.inventory.candidates
            ]
            assert len(disposed) == len(set((item.kind, item.token) for item in disposed)) == 2
        with session_scope(ready.case.factory) as session:
            assert (
                session.get(
                    Population,
                    (
                        ready.receipt.source_store,
                        ready.receipt.collection_id,
                        ready.receipt.object_id,
                    ),
                )
                is None
            )
            assert session.get(Accounting, ready.case.candidate.name).reserved_bytes == 0
        assert validate_db(ready.case.config.database_url).condition == "current"


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("paginated", (False, True))
@pytest.mark.parametrize("disabled", (False, True))
def test_service_maintenance_owns_bounded_cache_cleanup_and_real_page_progress(
    tmp_path, monkeypatch, mode, paginated, disabled
):
    with _abandoned(tmp_path, mode=mode) as ready:
        _assert_service_cleanup(ready, monkeypatch, paginated=paginated, disabled=disabled)


def _assert_lost_finish(ready, monkeypatch, *, method="finish"):
    advance = _clock(monkeypatch)
    create = partial(
        ready.case.cache.native_population_cleanup_turn,
        grants=GRANTS,
        tuning=CleanupExecutionTuning(),
    )
    failed = []

    def make(receipt):
        actor = create(receipt)
        original = getattr(actor._owner, method)

        def lose(*args, **kwargs):
            result = original(*args, **kwargs)
            if not failed:
                failed.append(True)
                raise OSError("original committed cleanup reply lost")
            return result

        monkeypatch.setattr(actor._owner, method, lose)
        return actor

    with _service(ready, monkeypatch, create=make) as (service, calls, actors):
        _drain(ready, service, advance)
        assert failed == [True] and len(actors) == 1
        assert actors[0].outcome is not None and len(calls) == 2
        assert not service._archive_retention._reservations


@pytest.mark.parametrize("method", ("finish", "acknowledge_response"))
def test_original_cleanup_resumes_after_lost_reply_even_when_population_row_is_gone(
    tmp_path, monkeypatch, method
):
    with _abandoned(tmp_path) as ready:
        _assert_lost_finish(ready, monkeypatch, method=method)


def _assert_join_before_retirement(ready, monkeypatch, *, lost_start_reply=False):
    advance = _clock(monkeypatch)
    entered, release = threading.Event(), threading.Event()
    create = partial(
        ready.case.cache.native_population_cleanup_turn,
        grants=GRANTS,
        tuning=CleanupExecutionTuning(),
    )

    def make(receipt):
        actor = create(receipt)
        run = actor.run

        def held():
            result = run()
            if result == "disposed":
                entered.set()
                assert release.wait(15)
            return result

        monkeypatch.setattr(actor, "run", held)
        return actor

    with _service(ready, monkeypatch, create=make) as (service, calls, actors):
        try:
            for _ in range(2):
                advance()
                service.process_due_archive_uploads()
                _join(service)
            advance()
            submit = service._archive_execution.try_submit
            lost = []

            def acknowledged_late(*args, **kwargs):
                result = submit(*args, **kwargs)
                if lost_start_reply and not lost and result == "submitted":
                    lost.append(True)
                    raise OSError("original worker start reply lost")
                return result

            monkeypatch.setattr(service._archive_execution, "try_submit", acknowledged_late)
            if lost_start_reply:
                with pytest.raises(OSError, match="worker start reply lost"):
                    service.process_due_archive_uploads()
            else:
                service.process_due_archive_uploads()
            assert entered.wait(5)
            context = next(iter(service._cache_cleanup._contexts.values()))
            assert actors[0].outcome is not None and len(calls) == 2
            slot = service._archive_execution._slots[actors[0].key]

            def forbidden(*args, **kwargs):
                pytest.fail("coordinator cannot join an actually live cleanup worker")

            with monkeypatch.context() as patch:
                patch.setattr(slot.worker, "join", forbidden)
                with ThreadPoolExecutor(max_workers=1) as pool:
                    assert pool.submit(service.process_due_archive_uploads).result(timeout=2) == 0
                assert slot.worker.is_alive()
                assert service._cache_cleanup._contexts[actors[0].key] is context
                assert service._archive_retention._reservations
            release.set()
            _join(service)
            advance()
            service.process_due_archive_uploads()
            assert _finished(ready, service)
        finally:
            release.set()


@pytest.mark.parametrize("lost_start_reply", (False, True))
def test_cleanup_positive_database_outcome_does_not_replace_actual_worker_join(
    tmp_path, monkeypatch, lost_start_reply
):
    with _abandoned(tmp_path) as ready:
        _assert_join_before_retirement(ready, monkeypatch, lost_start_reply=lost_start_reply)


@pytest.mark.parametrize("capacity", ("retention", "physical", "target-physical"))
def test_native_cleanup_uses_existing_service_retention_and_physical_budgets(
    tmp_path, monkeypatch, capacity
):
    with _abandoned(tmp_path) as ready:
        advance = _clock(monkeypatch)
        tuning = replace(
            ArchiveExecutionTuning(),
            terminal_workers=1 if capacity == "physical" else 2,
            retained_constructions=1 if capacity == "retention" else 16,
        )
        with _service(ready, monkeypatch, execution=tuning) as (service, calls, actors):
            key = PhysicalExecutionKey(ready.case.collection_id, "f" * 64)
            other, release, entered = object(), threading.Event(), threading.Event()
            reservation = None
            try:
                if capacity == "retention":
                    reservation = service._archive_retention.try_retain(
                        key, target_incarnation=ready.value["cache_incarnation_id"], owner=other
                    )
                    assert not isinstance(reservation, str)
                else:

                    def held():
                        entered.set()
                        assert release.wait(15)

                    assert (
                        service._archive_execution.try_submit(
                            key,
                            target_incarnation=ready.value["cache_incarnation_id"],
                            terminal=True,
                            work=held,
                            owner=other,
                        )
                        == "submitted"
                    )
                    assert entered.wait(5)
                advance()
                service.process_due_archive_uploads()
                assert calls == [] and actors == []
                _retained_data(ready)
                if reservation is not None:
                    assert service._archive_retention.retire(reservation, owner=other)
                release.set()
                _join(service)
                if capacity != "retention":
                    outcomes = service._archive_execution.collect(limit=1, owner=other)
                    assert len(outcomes) == 1
                    outcomes[0].result()
                _drain(ready, service, advance)
                assert len(calls) == 2 and len(actors) == 1
            finally:
                release.set()


@pytest.mark.parametrize("known", (False, True))
def test_cleanup_constructor_refusal_and_unknown_original_have_distinct_retirement(
    tmp_path, monkeypatch, known
):
    from riverhog_core.services.cache_population_cleanup_turn import CacheCleanupNotAdmitted

    with _abandoned(tmp_path) as ready:
        advance = _clock(monkeypatch)
        attempts = []

        def fail(receipt):
            attempts.append(receipt)
            if known:
                raise CacheCleanupNotAdmitted("no actor entered")
            raise OSError("original constructor unknown")

        with _service(ready, monkeypatch, create=fail) as (service, calls, actors):
            for _ in range(3):
                advance()
                service.process_due_archive_uploads()
                _join(service)
            assert calls == actors == []
            if known:
                assert len(attempts) >= 2
            else:
                assert len(attempts) == 1 and service._archive_retention._reservations
            _retained_data(ready)


def _assert_overlap(ready, monkeypatch):
    first, calls = _turn(ready)
    second_owner = type(ready.owner)(ready.case.factory, ready.receipt, grants=GRANTS)
    second_ready = replace_ready(ready, second_owner)
    second, other_calls = _turn(second_ready)
    selected, release = threading.Event(), threading.Event()
    select_page = ready.owner.select_page

    def held(*args, **kwargs):
        selected.set()
        assert release.wait(15)
        return select_page(*args, **kwargs)

    monkeypatch.setattr(ready.owner, "select_page", held)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(first.run)
            assert selected.wait(5)
            assert second.run() == "pending"
            release.set()
            assert pending.result(timeout=10) == "pending"
        result = _complete(second, other_calls)
        assert first.run() == "disposed" and first.outcome == result
        assert len(calls) == 1 and len(other_calls) == 2
    finally:
        release.set()


def replace_ready(ready, owner):
    from types import SimpleNamespace

    return SimpleNamespace(**{**vars(ready), "owner": owner})


def test_competing_inventory_reads_receive_their_own_original_before_following_selected_page(
    tmp_path, monkeypatch
):
    with _abandoned(tmp_path) as ready:
        _assert_overlap(ready, monkeypatch)


def test_native_cleanup_due_query_uses_its_partial_ordered_index(tmp_path):
    from riverhog_core.services.cache_population_cleanup_registry import _due_query

    with _abandoned(tmp_path) as ready:
        query = _due_query("9999-01-01T00:00:00Z", 1)
        with session_scope(ready.case.factory) as session:
            sql = str(query.compile(session.bind, compile_kwargs={"literal_binds": True}))
            plan = " ".join(str(row) for row in session.execute(text("EXPLAIN QUERY PLAN " + sql)))
        assert "ix_retrieval_cache_populations_native_cleanup_due" in plan
        assert "TEMP B-TREE" not in plan


@pytest.mark.parametrize("removed", (False, True))
def test_native_cache_binding_rechecks_current_configuration_before_new_disposal(
    tmp_path, monkeypatch, removed
):
    from types import SimpleNamespace

    with _abandoned(tmp_path) as ready:
        calls = []
        execute = ready.case.candidate.execute_cleanup

        def record(invocation):
            calls.append(invocation)
            return execute(invocation)

        monkeypatch.setattr(ready.case.candidate, "execute_cleanup", record)
        actor = ready.case.cache.native_population_cleanup_turn(
            ready.receipt, grants=GRANTS, tuning=CleanupExecutionTuning()
        )
        assert actor.run() == "pending" and len(calls) == 1
        name = ready.case.candidate.name
        original = ready.case.cache._stores.pop(name)
        if not removed:
            ready.case.cache._stores[name] = SimpleNamespace(
                incarnation_id="11111111-1111-4111-8111-111111111111"
            )
        with pytest.raises((Conflict, RuntimeError), match="target changed|store is unavailable"):
            actor.run()
        assert len(calls) == 1 and actor.outcome is None
        _retained_data(ready)
        ready.case.cache._stores[name] = original
        _complete(actor, calls)
        assert len(calls) == 2


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
def test_service_cleanup_retains_independent_owner_after_source_upload_retirement(
    tmp_path, monkeypatch, mode
):
    from riverhog_core.catalog_models import CollectionUploadRecord

    with _abandoned(tmp_path, mode=mode) as ready:
        with session_scope(ready.case.factory) as session:
            session.delete(session.get(CollectionUploadRecord, ready.case.collection_id))
        _assert_service_cleanup(ready, monkeypatch)
        from riverhog_core.catalog_archive_write_models import ArchiveUploadTerminalRecord

        with session_scope(ready.case.factory) as session:
            # The independent cache result does not adjudicate the archive terminal.
            assert session.get(ArchiveUploadTerminalRecord, ready.operation).phase == "draining"


def _assert_lost_due(ready, monkeypatch, committed):
    advance = _clock(monkeypatch)
    with _service(ready, monkeypatch) as (service, calls, actors):
        original = registry_module.session_scope

        @contextmanager
        def lose(factory):
            with original(factory) as session:
                yield session
                if not committed:
                    raise OSError("cleanup due schedule rolled back")
            if committed:
                raise OSError("cleanup due schedule reply lost")

        advance()
        with monkeypatch.context() as patch:
            patch.setattr(registry_module, "session_scope", lose)
            with pytest.raises(OSError, match="cleanup due schedule"):
                service.process_due_archive_uploads()
        assert calls == actors == []
        assert not service._archive_execution._slots
        assert not service._archive_retention._reservations
        _retained_data(ready)
        _drain(ready, service, advance)
        assert len(calls) == 2 and len(actors) == 1


@pytest.mark.parametrize("committed", (False, True))
def test_failed_native_due_schedule_reply_never_speculatively_dispatches(
    tmp_path, monkeypatch, committed
):
    with _abandoned(tmp_path) as ready:
        _assert_lost_due(ready, monkeypatch, committed)


def _assert_shared_slot_fairness(ready, monkeypatch, idle_polls):
    from tests.unit.test_archive_finalizer_registry import Finalizer, _scope

    advance = _clock(monkeypatch)
    with _service(ready, monkeypatch, execution=ArchiveExecutionTuning(terminal_workers=1)) as (
        service,
        calls,
        actors,
    ):
        original = Finalizer()
        original.pending = True
        entered, release = threading.Event(), threading.Event()
        drains = []

        def held(operation):
            drains.append(operation)
            entered.set()
            assert release.wait(15)
            return "pending"

        monkeypatch.setattr(original, "drain_terminal", held)
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
        for completion in service._archive_finalizers.collect(limit=1):
            completion.result()
        try:
            advance()
            service.process_due_archive_uploads()
            assert entered.wait(5) and len(drains) == 1 and calls == []
            # Polling cannot spend the next free-slot opportunity while the
            # original archive worker still owns the only terminal slot.
            for _ in range(idle_polls):
                service.process_due_archive_uploads()
            release.set()
            _join(service)
            advance()
            service.process_due_archive_uploads()
            _join(service)
            assert len(drains) == 1 and len(calls) == 1
            assert calls[0].request.kind == "inventory"
            assert len(actors) == 1 and actors[0].outcome is None
            _retained_data(ready)
        finally:
            release.set()


@pytest.mark.parametrize("idle_polls", (1, 2, 3))
def test_waiting_cache_scope_keeps_its_next_shared_slot_turn_across_idle_polls(
    tmp_path, monkeypatch, idle_polls
):
    with _abandoned(tmp_path) as ready:
        _assert_shared_slot_fairness(ready, monkeypatch, idle_polls)
