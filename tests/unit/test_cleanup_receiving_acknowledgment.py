from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_core.catalog_archive_write_models import TerminalCleanupCapacityRecord as Capacity
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_object_cleanup_models import ObjectCleanupAttemptRecord as Attempt
from riverhog_core.services import object_cleanup_receiving as receiving
from riverhog_core.services.archive_upload_cleanup import ArchiveUploadCleanup
from riverhog_core.services.cache_population_cleanup import CachePopulationCleanup
from riverhog_protocol.errors import Conflict
from riverhog_storage_adapter_protocol import (
    cleanup_inventory_execution_result,
    unknown_cleanup_execution,
)

from tests.unit import test_archive_upload_cleanup as archive
from tests.unit import test_cache_population_cleanup as cache
from tests.unit.test_archive_write_admission import _case

_FIELDS = ("calls", "control_bytes", "provider_requests", "in_flight")


@contextmanager
def _receiving_case(tmp_path, owner, *, database_url=None):
    if owner == "cache":
        with cache._abandoned(tmp_path, database_url=database_url) as ready:
            yield ready
    else:
        with _case(tmp_path, database_url=database_url) as case:
            yield archive._ready(case)


def _armed(ready, owner, kind):
    if owner == "cache":
        page = cache._select(ready) if kind == "dispose" else None

        def arm():
            return (
                cache._arm_dispose(ready, page) if page is not None else cache._inventory_arm(ready)
            )
    else:
        page = archive._select(ready) if kind == "dispose" else None

        def arm():
            return (
                archive._cleanup_arm(ready, page)
                if page is not None
                else archive._inventory_arm(ready)
            )

    first, second = arm(), arm()
    calls = []

    def execute(invocation):
        calls.append(invocation)
        if owner == "cache":
            return ready.target.execute_cleanup(invocation)
        if kind == "inventory":
            return cleanup_inventory_execution_result(
                invocation,
                archive._request(ready)[1].inventory,
                provider_requests=1,
                execution="closed",
            )
        return archive._response(first, ("disposed", "disposed"))

    response = ready.owner.execute(first, SimpleNamespace(execute_cleanup=execute))
    assert response.execution == "closed" and len(calls) == 1
    return first, second, response, calls


def _capacity(ready):
    with session_scope(ready.case.factory) as session:
        row = session.get(Capacity, ready.operation)
        return tuple(int(getattr(row, field)) for field in _FIELDS)


def _assert_lost_receiving_reply(ready, monkeypatch, owner, kind, phase, committed):
    first, second, response, calls = _armed(ready, owner, kind)
    before = _capacity(ready)
    with session_scope(ready.case.factory) as session:
        row = session.get(Attempt, first.invocation.call.call_id)
        charge = (1, int(row.control_bytes), int(row.provider_requests), int(row.in_flight))
    received = (
        ready.owner.record_response(ready.case.collection_id, ready.operation, response)
        if phase == "acknowledge"
        else None
    )
    real_scope = receiving.session_scope

    @contextmanager
    def lost_reply(factory):
        with real_scope(factory) as session:
            yield session
            if not committed:
                raise RuntimeError("receiving commit refused")
        if committed:
            raise RuntimeError("receiving commit reply lost")

    with monkeypatch.context() as patch:
        patch.setattr(receiving, "session_scope", lost_reply)
        with pytest.raises(RuntimeError, match="receiving commit"):
            if phase == "record":
                ready.owner.record_response(ready.case.collection_id, ready.operation, response)
            else:
                ready.owner.acknowledge_response(received)

    with session_scope(ready.case.factory) as session:
        first_row = session.get(Attempt, first.invocation.call.call_id)
        second_row = session.get(Attempt, second.invocation.call.call_id)
        assert second_row.unresolved and second_row.response_json is None
        if phase == "acknowledge" and committed:
            assert first_row is None
        else:
            assert first_row is not None
            recorded = phase == "acknowledge" or committed
            assert first_row.unresolved is not recorded
            assert (first_row.response_json is not None) is recorded

    expected = tuple(value - used for value, used in zip(before, charge, strict=True))
    assert _capacity(ready) == (expected if phase == "acknowledge" and committed else before)
    if phase == "record":
        # Retry only this original report, never the captured provider command.
        received = ready.owner.record_response(ready.case.collection_id, ready.operation, response)
    ready.owner.acknowledge_response(received)
    assert _capacity(ready) == expected
    ready.owner.acknowledge_response(received)
    assert _capacity(ready) == expected and len(calls) == 1
    with session_scope(ready.case.factory) as session:
        assert session.get(Attempt, first.invocation.call.call_id) is None
        assert session.get(Attempt, second.invocation.call.call_id).unresolved
    if owner == "cache":
        cache._retained_data(ready)


@pytest.mark.parametrize("owner", ("archive", "cache"))
@pytest.mark.parametrize("kind", ("inventory", "dispose"))
@pytest.mark.parametrize("phase", ("record", "acknowledge"))
@pytest.mark.parametrize("committed", (False, True))
def test_original_cleanup_receiving_recovers_commit_and_reply_loss_without_redispatch(
    tmp_path, monkeypatch, owner, kind, phase, committed
):
    with _receiving_case(tmp_path, owner) as ready:
        _assert_lost_receiving_reply(ready, monkeypatch, owner, kind, phase, committed)


@pytest.mark.parametrize("owner", ("archive", "cache"))
@pytest.mark.parametrize(
    "field,value",
    (
        ("collection_id", 999999),
        ("operation_id", "e" * 64),
        ("scope_id", "f" * 64),
        ("invocation_json", "{}"),
        ("response_json", "{}"),
        ("_signature", b"invalid"),
    ),
)
def test_cleanup_receiving_evidence_binds_every_original_field(tmp_path, owner, field, value):
    with _receiving_case(tmp_path, owner) as ready:
        first, _, response, calls = _armed(ready, owner, "dispose")
        received = ready.owner.record_response(ready.case.collection_id, ready.operation, response)
        before = _capacity(ready)
        with pytest.raises(Conflict, match="original live receiving evidence"):
            ready.owner.acknowledge_response(replace(received, **{field: value}))
        assert _capacity(ready) == before and len(calls) == 1
        with session_scope(ready.case.factory) as session:
            assert session.get(Attempt, first.invocation.call.call_id) is not None


@pytest.mark.parametrize("owner", ("archive", "cache"))
def test_cold_cleanup_owner_must_receive_original_response_before_acknowledgment(tmp_path, owner):
    with _receiving_case(tmp_path, owner) as ready:
        _, _, response, calls = _armed(ready, owner, "dispose")
        received = ready.owner.record_response(ready.case.collection_id, ready.operation, response)
        cold = (
            CachePopulationCleanup(ready.case.factory, ready.receipt, grants=cache.GRANTS)
            if owner == "cache"
            else ArchiveUploadCleanup(ready.case.factory, grants=archive.GRANTS)
        )
        before = _capacity(ready)
        for value in (received, response):
            with pytest.raises(Conflict, match="original live receiving evidence"):
                cold.acknowledge_response(value)
        assert _capacity(ready) == before
        original = cold.record_response(ready.case.collection_id, ready.operation, response)
        cold.acknowledge_response(original)
        cold.acknowledge_response(original)
        assert _capacity(ready)[0] == before[0] - 1 and len(calls) == 1


@pytest.mark.parametrize("owner", ("archive", "cache"))
def test_positive_receiving_of_unknown_cleanup_does_not_supply_execution_closure(tmp_path, owner):
    with _receiving_case(tmp_path, owner) as ready:
        first, _, _, calls = _armed(ready, owner, "dispose")
        # A returned local worker cannot replace the original provider closure fact.
        response = unknown_cleanup_execution(first.invocation)
        received = ready.owner.record_response(ready.case.collection_id, ready.operation, response)
        before = _capacity(ready)
        with pytest.raises(Conflict, match="exact closed response"):
            ready.owner.acknowledge_response(received)
        assert _capacity(ready) == before and len(calls) == 1
        with session_scope(ready.case.factory) as session:
            assert session.get(Attempt, first.invocation.call.call_id).unresolved
