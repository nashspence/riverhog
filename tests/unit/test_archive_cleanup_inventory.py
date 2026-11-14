from __future__ import annotations

import json
from dataclasses import replace

import pytest
from pydantic import ValidationError
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.catalog_db import session_scope
from riverhog_core.services.archive_upload_cleanup import ArchiveUploadCleanup
from riverhog_core.services.archive_write_admission import _writer
from riverhog_protocol.errors import Conflict, NotFound
from riverhog_storage_adapter_protocol import (
    CleanupInventoryStepRequest,
    CleanupInvocation,
    WriteCallIdentity,
    cleanup_inventory_execution_result,
    cleanup_step_sha256,
    unknown_cleanup_execution,
)
from sqlalchemy import event, func, select
from sqlalchemy.exc import InvalidRequestError

from tests.unit.test_archive_upload_cleanup import (
    GRANTS,
    Attempt,
    Page,
    Upload,
    _ack,
    _cleanup_arm,
    _inventory_arm,
    _ready,
    _receive,
    _request,
    _response,
    _select,
)
from tests.unit.test_archive_write_admission import _case, _guard


def _observe(ready, ticket, page, *, execution="closed", on_call=None):
    class Inventory:
        def execute_cleanup(self, invocation):
            if on_call is not None:
                on_call(invocation)
            return cleanup_inventory_execution_result(
                invocation, page, provider_requests=1, execution=execution
            )

    return ready.owner.execute(ticket, Inventory())


def _choose(ready, ticket, inventory_request, request, *, after=None, guard=_guard):
    return ready.owner.select_page(
        ready.case.collection_id,
        ready.operation,
        inventory_call_id=ticket.invocation.call.call_id,
        inventory_request=inventory_request,
        request=request,
        after=after,
        guard=guard,
    )


def test_inventory_is_retained_before_provider_work_and_dispatches_only_once(tmp_path):
    with _case(tmp_path) as case:
        ready = _ready(case)
        inventory, request = _request(ready)
        ticket = _inventory_arm(ready, inventory)
        calls = []

        def physical(invocation):
            with session_scope(case.factory) as session:
                _writer(session)
                row = session.get(Attempt, invocation.call.call_id)
                assert row.unresolved and row.response_json is None and row.page_sha256 is None
                assert row.invocation_json == invocation.model_dump_json(exclude_none=True)
                assert int(row.control_bytes) > invocation.request.maximum_control_bytes
                assert row.provider_requests == "1" and row.in_flight == "1"
                assert session.get(Page, ready.operation) is None
            calls.append(invocation)

        response = _observe(ready, ticket, request.inventory, on_call=physical)
        assert _observe(ready, ticket, request.inventory, on_call=physical) == response
        assert len(calls) == 1
        with pytest.raises(NotFound, match="cleanup page"):
            ready.owner.current(case.collection_id, ready.operation)
        with pytest.raises(Conflict, match="no retained original"):
            _choose(ready, ticket, inventory, request)
        _receive(ready, response)
        chosen = _choose(ready, ticket, inventory, request)
        _ack(ready, response)
        assert _choose(ready, ticket, inventory, request) == chosen
        assert ready.owner.current(case.collection_id, ready.operation) == chosen


def _lost_inventory_reply(ready):
    inventory, request = _request(ready)
    original = _inventory_arm(ready, inventory)
    lost = _observe(ready, original, request.inventory)
    ready.owner = ArchiveUploadCleanup(ready.case.factory, grants=GRANTS)
    retry = _inventory_arm(ready, inventory)
    assert retry.invocation.call != original.invocation.call
    response = _observe(ready, retry, request.inventory)
    _receive(ready, response)
    chosen = _choose(ready, retry, inventory, request)
    _ack(ready, response)
    disposal = _cleanup_arm(ready, chosen)
    completed = _response(disposal, ("disposed", "disposed"))
    _receive(ready, completed)
    _ack(ready, completed)
    with session_scope(ready.case.factory) as session:
        remaining = session.scalars(select(Attempt)).all()
        assert len(remaining) == 1 and remaining[0].id == original.invocation.call.call_id
        assert remaining[0].unresolved and remaining[0].response_json is None
    _receive(ready, lost)
    _ack(ready, lost)
    assert ready.owner.current(ready.case.collection_id, ready.operation).complete
    with session_scope(ready.case.factory) as session:
        assert session.scalar(select(func.count()).select_from(Attempt)) == 0


def test_lost_inventory_reply_stays_charged_after_retry_and_disposal(tmp_path):
    with _case(tmp_path) as case:
        _lost_inventory_reply(_ready(case))


def _inventory_receiving_after_retirement(ready):
    inventory, request = _request(ready)
    ticket = _inventory_arm(ready, inventory)
    response = _observe(ready, ticket, request.inventory)
    with session_scope(ready.case.factory) as session:
        session.delete(session.get(Upload, ready.case.collection_id))
    ready.owner = ArchiveUploadCleanup(ready.case.factory, grants=GRANTS)
    _receive(ready, response)
    _receive(ready, response)
    with pytest.raises(NotFound, match="construction"):
        _choose(ready, ticket, inventory, request)
    _ack(ready, response)
    with session_scope(ready.case.factory) as session:
        assert session.scalar(select(func.count()).select_from(Attempt)) == 0
        assert session.get(Page, ready.operation) is None


def test_original_inventory_receiving_outlives_its_upload(tmp_path):
    with _case(tmp_path) as case:
        _inventory_receiving_after_retirement(_ready(case))


def test_lost_inventory_commit_reply_never_recreates_dispatch_right(tmp_path):
    with _case(tmp_path) as case:
        ready = _ready(case, grants=replace(GRANTS, calls=1))

        def lost_reply(session):
            raise RuntimeError("commit acknowledgement lost")

        event.listen(case.factory, "after_commit", lost_reply, once=True)
        with pytest.raises(InvalidRequestError, match="committed"):
            _inventory_arm(ready)
        with session_scope(case.factory) as session:
            row = session.scalar(select(Attempt))
            assert row.unresolved and row.response_json is None and row.page_sha256 is None
        with pytest.raises(Conflict, match="retention grant"):
            _inventory_arm(ready)


@pytest.mark.parametrize(
    "resource,value",
    (("calls", 1), ("control_bytes", 7000), ("provider_requests", 1), ("in_flight", 1)),
)
def test_unknown_inventory_retains_each_independent_reservation(tmp_path, resource, value):
    with _case(tmp_path) as case:
        ready = _ready(case, grants=replace(GRANTS, **{resource: value}))
        ticket = _inventory_arm(ready)
        _receive(ready, unknown_cleanup_execution(ticket.invocation))
        with pytest.raises(Conflict, match="retention grant"):
            _inventory_arm(ready)
        with session_scope(case.factory) as session:
            assert session.scalar(select(func.count()).select_from(Attempt)) == 1


def test_positive_inventory_does_not_close_its_unknown_read(tmp_path):
    with _case(tmp_path) as case:
        ready = _ready(case)
        inventory, request = _request(ready)
        ticket = _inventory_arm(ready, inventory)
        response = _observe(ready, ticket, request.inventory, execution="unknown")
        _receive(ready, response)
        _choose(ready, ticket, inventory, request)
        with pytest.raises(Conflict, match="exact closed"):
            _ack(ready, response)
        with session_scope(case.factory) as session:
            assert session.get(Attempt, ticket.invocation.call.call_id).unresolved
        closed = response.model_copy(update={"execution": "closed"})
        _receive(ready, closed)
        _ack(ready, closed)


def test_late_inventory_cannot_replace_another_selected_page(tmp_path):
    with _case(tmp_path) as case:
        ready = _ready(case)
        inventory, request = _request(ready)
        first, second = _inventory_arm(ready, inventory), _inventory_arm(ready, inventory)
        one = _observe(ready, first, request.inventory)
        two = _observe(ready, second, request.inventory)
        _receive(ready, one)
        chosen = _choose(ready, first, inventory, request)
        _ack(ready, one)
        _receive(ready, two)
        with pytest.raises(Conflict, match="different page"):
            _choose(ready, second, inventory, request)
        _ack(ready, two)
        assert ready.owner.current(case.collection_id, ready.operation) == chosen


def test_selection_cannot_substitute_an_unobserved_page_or_current_permission(tmp_path):
    with _case(tmp_path) as case:
        ready = _ready(case)
        inventory, request = _request(ready)
        ticket = _inventory_arm(ready, inventory)
        response = _observe(ready, ticket, request.inventory)
        _receive(ready, response)
        candidate = request.inventory.candidates[0].model_copy(update={"token": "another"})
        other = request.model_copy(
            update={
                "inventory": request.inventory.model_copy(
                    update={
                        "candidates": (candidate, request.inventory.candidates[1]),
                    }
                ),
            }
        )
        with pytest.raises(Conflict, match="original inventory"):
            _choose(ready, ticket, inventory, other)

        def denied(session):
            raise NotFound("permission revoked")

        with pytest.raises(NotFound, match="revoked"):
            _choose(ready, ticket, inventory, request, guard=denied)
        _receive(ready, response)
        with session_scope(case.factory) as session:
            assert session.get(Page, ready.operation) is None
            assert session.get(
                Attempt, ticket.invocation.call.call_id
            ).response_json == response.model_dump_json(exclude_none=True)


def test_full_inventory_reply_fits_the_exact_retained_byte_reservation(tmp_path):
    with _case(tmp_path) as case:
        ready = _ready(case)
        first_inventory, first_request = _request(ready, count=0)
        first_request = first_request.model_copy(
            update={
                "inventory": first_request.inventory.model_copy(
                    update={
                        "exhausted": False,
                        "continuation": "next",
                    }
                ),
            }
        )
        prior = _select(ready, inventory_request=first_inventory, request=first_request)
        inventory, disposal = _request(ready, continuation="next")
        page = disposal.inventory
        capacity = inventory.maximum_response_bytes
        delta = capacity - len(page.model_dump_json(exclude_none=True).encode())
        candidate = page.candidates[0].model_copy(
            update={
                "token": page.candidates[0].token + "x" * delta,
            }
        )
        page = page.model_copy(update={"candidates": (candidate, page.candidates[1])})
        assert len(page.model_dump_json(exclude_none=True).encode()) == capacity

        def operation(control_bytes):
            return CleanupInventoryStepRequest(
                selected_terminal_sha256=ready.operation,
                inventory=inventory,
                maximum_provider_requests=1,
                maximum_control_bytes=control_bytes,
                maximum_in_flight=1,
            )

        low, high = 1, 8192
        while low < high:
            middle = (low + high) // 2
            request = operation(middle)
            try:
                CleanupInvocation(
                    call=WriteCallIdentity(
                        call_id="0" * 64,
                        exact_request_sha256=cleanup_step_sha256(request),
                        target_incarnation=ready.incarnation,
                        dispatch_owner="d" * 64,
                    ),
                    request=request,
                )
            except ValidationError:
                low = middle + 1
            else:
                high = middle
        ticket = ready.owner.arm_inventory(
            case.collection_id,
            ready.operation,
            request=operation(low),
            after=prior.request_sha256,
            dispatch_owner="d" * 64,
            guard=_guard,
        )
        response = _observe(ready, ticket, page)
        _receive(ready, response)
        with session_scope(case.factory) as session:
            row = session.get(Attempt, ticket.invocation.call.call_id)
            header = {
                name: getattr(row, name)
                for name in (
                    "id",
                    "terminal_id",
                    "page_sha256",
                    "dispatch_sha256",
                    "unresolved",
                    "control_bytes",
                    "provider_requests",
                    "in_flight",
                )
            }
            retained_bytes = (
                len(canonical_json_bytes(header))
                + len(row.invocation_json.encode())
                + len(row.response_json.encode())
            )
            assert retained_bytes <= int(row.control_bytes)
            assert (
                json.loads(row.response_json)["result"]["inventory"]["candidates"][0]["token"]
                == candidate.token
            )
        _ack(ready, response)
