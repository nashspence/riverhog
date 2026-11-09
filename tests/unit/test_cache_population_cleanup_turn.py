from __future__ import annotations

import hashlib
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_core.catalog_cache_write_models import CachePopulationDispositionRecord as Disposition
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_object_cleanup_models import ObjectCleanupAttemptRecord as Attempt
from riverhog_core.cleanup_execution_tuning import CleanupExecutionTuning
from riverhog_core.services.archive_upload_drain import lock_terminal_selection
from riverhog_core.services.cache_population_cleanup import CachePopulationCleanup
from riverhog_core.services.cache_population_cleanup_turn import CachePopulationCleanupTurn
from riverhog_protocol.errors import Conflict
from riverhog_storage_adapter_protocol import unknown_cleanup_execution
from sqlalchemy import func, select

from tests.unit.test_cache_population_cleanup import (
    GRANTS,
    _abandoned,
    _guard,
    _retained_data,
)

_TUNING = CleanupExecutionTuning()


def _turn(ready, *, tuning=_TUNING, guard=_guard):
    calls = []

    def execute(invocation):
        # Provider execution must not retain a shared catalog writer.
        with session_scope(ready.case.factory) as session:
            lock_terminal_selection(
                session,
                ready.case.collection_id,
                ready.operation,
                require_upload=False,
                guard=_guard,
            )
        calls.append(invocation)
        return ready.target.execute_cleanup(invocation)

    actor = CachePopulationCleanupTurn(
        ready.owner, SimpleNamespace(execute_cleanup=execute), guard=guard, tuning=tuning
    )
    return actor, calls


def _complete(actor, calls, *, maximum_turns=8):
    for _ in range(maximum_turns):
        before = len(calls)
        status = actor.run()
        assert len(calls) - before <= 1
        if status == "disposed":
            return actor.outcome
    raise AssertionError("bounded cleanup did not receive its original positive outcome")


def _assert_turns(ready):
    actor, calls = _turn(ready)
    assert actor.key.collection_id == ready.case.collection_id
    assert actor.key.scope_sha256 == hashlib.sha256(ready.receipt.receipt_json.encode()).hexdigest()
    assert actor.target_incarnation == ready.value["cache_incarnation_id"]
    assert actor.run() == "pending" and len(calls) == 1
    assert calls[0].request.kind == "inventory"
    _retained_data(ready)
    assert actor.run() == "pending" and len(calls) == 2
    assert calls[1].request.kind == "dispose"
    _retained_data(ready)
    result = _complete(actor, calls)
    assert result.abandoned == ready.receipt and actor.run() == "disposed"
    assert len(calls) == 2 and len({call.call.call_id for call in calls}) == 2
    for call in calls:
        request = call.request
        assert request.maximum_provider_requests == _TUNING.provider_requests
        assert request.maximum_control_bytes == _TUNING.control_bytes
        assert request.maximum_in_flight == _TUNING.in_flight
    inventory = calls[0].request.inventory
    assert inventory.maximum_items == _TUNING.inventory_items
    assert inventory.maximum_response_bytes == _TUNING.inventory_bytes
    with session_scope(ready.case.factory) as session:
        assert session.scalar(select(func.count()).select_from(Attempt)) == 0
        assert session.get(Disposition, actor.key.scope_sha256).outcome_json == result.outcome_json
    return actor, calls


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
def test_cache_cleanup_executes_one_bounded_provider_turn_and_retains_positive_disposition(
    tmp_path, mode
):
    with _abandoned(tmp_path, mode=mode) as ready:
        _assert_turns(ready)


_FAULTS = (
    ("record_response", "inventory"),
    ("record_response", "dispose"),
    ("select_page", "inventory"),
    ("acknowledge_response", "inventory"),
    ("acknowledge_response", "dispose"),
    ("finish", "finish"),
)


def _assert_failed_receiving(ready, monkeypatch, method, stage, committed):
    actor, calls = _turn(ready)
    if stage in {"dispose", "finish"}:
        assert actor.run() == "pending"
    if stage == "finish":
        assert actor.run() == "pending"
    original = getattr(ready.owner, method)

    def fail(*args, **kwargs):
        if committed:
            original(*args, **kwargs)
        raise RuntimeError("original receiving reply unavailable")

    with monkeypatch.context() as patch:
        patch.setattr(ready.owner, method, fail)
        with pytest.raises(RuntimeError, match="original receiving reply"):
            actor.run()
    assert actor.outcome is None
    before = len(calls)
    assert actor.run() in {"pending", "disposed"}
    assert len(calls) == before
    result = _complete(actor, calls)
    assert result.abandoned == ready.receipt
    assert len(calls) == 2 and len({call.call.call_id for call in calls}) == 2


@pytest.mark.parametrize("method,stage", _FAULTS)
@pytest.mark.parametrize("committed", (False, True))
def test_cache_cleanup_receives_exact_original_after_database_failure_without_provider_replay(
    tmp_path, monkeypatch, method, stage, committed
):
    with _abandoned(tmp_path) as ready:
        _assert_failed_receiving(ready, monkeypatch, method, stage, committed)


def _assert_unknown(ready):
    calls = []

    def execute(invocation):
        calls.append(invocation)
        if len(calls) == 1:
            return unknown_cleanup_execution(invocation)
        return ready.target.execute_cleanup(invocation)

    actor = CachePopulationCleanupTurn(
        ready.owner, SimpleNamespace(execute_cleanup=execute), guard=_guard, tuning=_TUNING
    )
    assert actor.run() == "pending"
    assert actor.run() == "pending"
    assert actor.run() == "pending"
    assert [call.request.kind for call in calls] == ["inventory", "inventory", "dispose"]
    for _ in range(2):
        with pytest.raises(Conflict, match="original cleanup receiving"):
            actor.run()
        assert len(calls) == 3 and actor.outcome is None
    with session_scope(ready.case.factory) as session:
        assert session.scalar(select(func.count()).select_from(Attempt)) == 1
        original = session.get(Attempt, calls[0].call.call_id)
        assert original.unresolved and original.response_json is not None
        assert session.get(Disposition, actor.key.scope_sha256) is None
    _retained_data(ready)


def test_later_monotone_cleanup_cannot_supply_unknown_original_execution_closure(tmp_path):
    with _abandoned(tmp_path, grants=replace(GRANTS, control_bytes=256 * 1024)) as ready:
        _assert_unknown(ready)


@pytest.mark.parametrize("stage", ("dispose", "finish"))
def test_cache_cleanup_rechecks_current_disposal_guard_before_each_new_effect(tmp_path, stage):
    with _abandoned(tmp_path) as ready:
        permission = [True]

        def guard(session):
            _guard(session)
            if not permission[0]:
                raise Conflict("cache cleanup current permission revoked")

        actor, calls = _turn(ready, guard=guard)
        assert actor.run() == "pending"
        if stage == "finish":
            assert actor.run() == "pending"
        before = len(calls)
        permission[0] = False
        with pytest.raises(Conflict, match="permission revoked"):
            actor.run()
        assert len(calls) == before and actor.outcome is None
        _retained_data(ready)
        permission[0] = True
        _complete(actor, calls)
        assert len(calls) == 2


def test_cold_cache_cleanup_resumes_retained_page_without_repeating_inventory(tmp_path):
    with _abandoned(tmp_path) as ready:
        first, original_calls = _turn(ready)
        assert first.run() == "pending"
        ready.owner = CachePopulationCleanup(ready.case.factory, ready.receipt, grants=GRANTS)
        cold, calls = _turn(ready)
        result = _complete(cold, calls)
        assert result.abandoned == ready.receipt
        assert len(original_calls) == len(calls) == 1
        assert original_calls[0].request.kind == "inventory" and calls[0].request.kind == "dispose"
        assert first.run() == "disposed" and first.outcome == result
        assert len(original_calls) == 1


def test_cache_cleanup_capacity_refusal_dispatches_nothing_and_preserves_data(tmp_path):
    with _abandoned(tmp_path) as ready:
        actor, calls = _turn(ready, tuning=replace(_TUNING, control_bytes=1))
        with pytest.raises(ValueError, match="control grant"):
            actor.run()
        assert calls == [] and actor.outcome is None
        with session_scope(ready.case.factory) as session:
            assert session.scalar(select(func.count()).select_from(Attempt)) == 0
        _retained_data(ready)


def test_busy_cache_cleanup_turn_does_not_wait_or_dispatch(tmp_path):
    with _abandoned(tmp_path) as ready:
        actor, calls = _turn(ready)
        with actor._lock:
            assert actor.run() == "busy"
        assert calls == [] and actor.outcome is None
