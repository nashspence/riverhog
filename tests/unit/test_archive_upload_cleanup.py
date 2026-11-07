from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.catalog_archive_write_models import (
    ArchiveCleanupAttemptRecord as Attempt,
)
from riverhog_core.catalog_archive_write_models import (
    ArchiveCleanupPageRecord as Page,
)
from riverhog_core.catalog_archive_write_models import (
    ArchiveUploadTerminalRecord as Terminal,
)
from riverhog_core.catalog_archive_write_models import (
    ArchiveWriteControlRecord as Control,
)
from riverhog_core.catalog_db import session_scope, validate_db
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services.archive_upload_cleanup import (
    ArchiveCleanupGrants,
    ArchiveUploadCleanup,
)
from riverhog_core.services.archive_write_admission import _writer
from riverhog_core.stores.storage_adapter_archive_objects import StorageAdapterArchiveObjectCleanup
from riverhog_protocol.errors import Conflict, NotFound
from riverhog_storage_adapter_protocol import (
    CleanupCandidate,
    CleanupInventoryPage,
    CleanupInventoryRequest,
    CleanupInventoryStepRequest,
    CleanupItemOutcome,
    CleanupStepRequest,
    SmallObjectWriteRequest,
    cleanup_execution_result,
    cleanup_inventory_execution_result,
    cleanup_step_sha256,
    unknown_cleanup_execution,
)
from sqlalchemy import event, func, insert, select
from sqlalchemy.exc import InvalidRequestError

from tests.unit.test_archive_write_admission import _arm, _case, _guard, _prepare, _selected

GRANTS = ArchiveCleanupGrants(4, 65536, 16, 8)


def _ready(case, *, grants=GRANTS, ready=True):
    operation = _selected(case)
    with session_scope(case.factory) as session:
        terminal = session.get(Terminal, operation)
        if ready:
            # Fixture-owned adjudication only; this is not a global quiescence proof.
            terminal.phase = "cleanup-ready"
        construction = json.loads(session.get(Control, case.collection_id).construction_json)
    return SimpleNamespace(
        case=case,
        operation=operation,
        owner=ArchiveUploadCleanup(case.factory, grants=grants),
        prefix=construction["archive_storage_prefix"],
        incarnation=construction["archive_incarnation_id"],
    )


def _request(ready, *, count=2, continuation=None):
    inventory_request = CleanupInventoryRequest(
        object_prefix=ready.prefix,
        target_incarnation=ready.incarnation,
        maximum_items=2,
        maximum_response_bytes=2048,
        continuation=continuation,
    )
    request = CleanupStepRequest(
        selected_terminal_sha256=ready.operation,
        inventory=CleanupInventoryPage(
            object_prefix=ready.prefix,
            target_incarnation=ready.incarnation,
            candidates=tuple(
                CleanupCandidate(
                    object_path=ready.prefix + f"item-{index}",
                    kind="object_revision",
                    token=f"revision-{index}",
                )
                for index in range(count)
            ),
            scanned_items=count,
            exhausted=True,
        ),
        maximum_provider_requests=2,
        maximum_control_bytes=8192,
        maximum_in_flight=1,
    )
    return inventory_request, request


def _inventory_arm(ready, inventory_request=None, *, after=None, guard=_guard):
    inventory_request = inventory_request or _request(ready)[0]
    return ready.owner.arm_inventory(
        ready.case.collection_id,
        ready.operation,
        request=CleanupInventoryStepRequest(
            selected_terminal_sha256=ready.operation,
            inventory=inventory_request,
            maximum_provider_requests=1,
            maximum_control_bytes=4096,
            maximum_in_flight=1,
        ),
        after=after,
        dispatch_owner="d" * 64,
        guard=guard,
    )


def _select(ready, *, inventory_request=None, request=None, after=None, guard=_guard):
    if request is None:
        inventory_request, request = _request(ready)
    with session_scope(ready.case.factory) as session:
        current = session.get(Page, ready.operation)
        retained = (
            current.inventory_call_id
            if current is not None and current.request_sha256 == cleanup_step_sha256(request)
            else None
        )
    response = None
    if retained is None:
        ticket = _inventory_arm(ready, inventory_request, after=after, guard=guard)

        class Inventory:
            def execute_cleanup(self, invocation):
                return cleanup_inventory_execution_result(
                    invocation, request.inventory, provider_requests=1, execution="closed"
                )

        response = ready.owner.execute(ticket, Inventory())
        ready.owner.record_response(ready.case.collection_id, ready.operation, response)
        retained = ticket.invocation.call.call_id
    page = ready.owner.select_page(
        ready.case.collection_id,
        ready.operation,
        inventory_call_id=retained,
        inventory_request=inventory_request,
        request=request,
        after=after,
        guard=guard,
    )
    if response is not None:
        ready.owner.acknowledge_response(ready.case.collection_id, ready.operation, response)
    return page


def _cleanup_arm(ready, page=None, *, guard=_guard, **attempt_grants):
    page = page or ready.owner.current(ready.case.collection_id, ready.operation)
    return ready.owner.arm(
        ready.case.collection_id,
        ready.operation,
        page_sha256=page.request_sha256,
        dispatch_owner="d" * 64,
        maximum_provider_requests=attempt_grants.get(
            "maximum_provider_requests", page.request.maximum_provider_requests
        ),
        maximum_control_bytes=attempt_grants.get(
            "maximum_control_bytes", page.request.maximum_control_bytes
        ),
        maximum_in_flight=attempt_grants.get("maximum_in_flight", page.request.maximum_in_flight),
        guard=guard,
    )


def _response(ticket, outcomes, *, execution="closed"):
    return cleanup_execution_result(
        ticket.invocation,
        tuple(
            CleanupItemOutcome(candidate=item, outcome=outcome)
            for item, outcome in zip(
                ticket.invocation.request.inventory.candidates, outcomes, strict=True
            )
        ),
        provider_requests=1,
        execution=execution,
    )


def _receive(ready, response):
    ready.owner.record_response(ready.case.collection_id, ready.operation, response)


def _ack(ready, response):
    ready.owner.acknowledge_response(ready.case.collection_id, ready.operation, response)


def _partial_retry(ready):
    page = _select(ready)
    first = _cleanup_arm(ready, page)
    partial = _response(first, ("disposed", "unknown"), execution="unknown")
    _receive(ready, partial)
    assert ready.owner.current(ready.case.collection_id, ready.operation).disposed == (0,)
    cold = ArchiveUploadCleanup(ready.case.factory, grants=GRANTS)
    ready.owner = cold
    replay = _select(ready)
    assert replay.request == page.request and replay.disposed == (0,)
    second = _cleanup_arm(ready)
    assert second.invocation.call != first.invocation.call
    assert second.invocation.request.inventory.candidates == page.request.inventory.candidates[1:]
    complete = _response(second, ("disposed",))
    _receive(ready, complete)
    _receive(ready, complete)
    _ack(ready, complete)
    assert cold.current(ready.case.collection_id, ready.operation).complete
    with session_scope(ready.case.factory) as session:
        remaining = session.scalars(select(Attempt)).all()
        assert len(remaining) == 1 and remaining[0].id == first.invocation.call.call_id
        assert remaining[0].unresolved and remaining[0].response_json == partial.model_dump_json(
            exclude_none=True
        )
        assert int(remaining[0].control_bytes) > 8192
        assert remaining[0].provider_requests == "2" and remaining[0].in_flight == "1"
        assert session.get(Terminal, ready.operation).phase == "cleaning"
    with pytest.raises(Conflict, match="exact closed"):
        _ack(ready, partial)
    with pytest.raises(Conflict, match="already complete"):
        _cleanup_arm(ready)


def test_partial_cleanup_retry_preserves_original_page_and_independent_unknown_charge(tmp_path):
    with _case(tmp_path) as case:
        _partial_retry(_ready(case))


@pytest.mark.parametrize("ready", (False, True))
def test_catalog_absence_cannot_create_cleanup_authority(tmp_path, ready):
    with _case(tmp_path) as case:
        if ready:
            _prepare(case)
            _arm(case)
        current = _ready(case, ready=ready)
        with pytest.raises(Conflict, match="unclosed forward|cleanup-ready"):
            _select(current)
        with session_scope(case.factory) as session:
            assert session.get(Page, current.operation) is None


@pytest.mark.parametrize("field", ("prefix", "incarnation", "terminal", "continuation"))
def test_selected_cleanup_requires_exact_original_scope_and_first_cursor(tmp_path, field):
    with _case(tmp_path) as case:
        ready = _ready(case)
        inventory_request, request = _request(ready)
        if field in {"prefix", "incarnation"}:
            changes = (
                {"object_prefix": "unrelated/"}
                if field == "prefix"
                else {"target_incarnation": "00000000-0000-4000-8000-000000000099"}
            )
            inventory_request = inventory_request.model_copy(update=changes)
            inventory = request.inventory.model_copy(
                update={
                    **changes,
                    "candidates": (),
                    "scanned_items": 0,
                }
            )
            request = request.model_copy(update={"inventory": inventory})
        elif field == "terminal":
            request = request.model_copy(update={"selected_terminal_sha256": "f" * 64})
        else:
            inventory_request = inventory_request.model_copy(
                update={"continuation": "not-retained"}
            )
        with pytest.raises(Conflict):
            _select(ready, inventory_request=inventory_request, request=request)


def test_current_permission_is_checked_before_new_selection_and_each_attempt(tmp_path):
    with _case(tmp_path) as case:
        ready = _ready(case)

        def denied(session):
            raise NotFound("permission revoked")

        with pytest.raises(NotFound, match="revoked"):
            _select(ready, guard=denied)
        _select(ready)
        with pytest.raises(NotFound, match="revoked"):
            _cleanup_arm(ready, guard=denied)
        with session_scope(case.factory) as session:
            assert session.scalar(select(func.count()).select_from(Attempt)) == 0


def _capacity(ready):
    _select(ready)
    first = _cleanup_arm(ready)
    _receive(ready, unknown_cleanup_execution(first.invocation))
    with pytest.raises(Conflict, match="retention grant"):
        _cleanup_arm(ready)
    with session_scope(ready.case.factory) as session:
        assert session.scalar(select(func.count()).select_from(Attempt)) == 1


@pytest.mark.parametrize(
    "resource,value",
    (("calls", 1), ("control_bytes", 14000), ("provider_requests", 3), ("in_flight", 1)),
)
def test_unknown_attempt_retains_each_independent_resource_reservation(tmp_path, resource, value):
    with _case(tmp_path) as case:
        _capacity(_ready(case, grants=replace(GRANTS, **{resource: value})))


def test_commit_acknowledgement_loss_leaves_charge_without_recreating_dispatch(tmp_path):
    with _case(tmp_path) as case:
        ready = _ready(case, grants=replace(GRANTS, calls=1))
        page = _select(ready)

        def lost_reply(session):
            raise RuntimeError("commit acknowledgement lost")

        event.listen(case.factory, "after_commit", lost_reply, once=True)
        with pytest.raises(InvalidRequestError, match="committed") as caught:
            _cleanup_arm(ready, page)
        assert isinstance(caught.value.__context__, RuntimeError)
        with session_scope(case.factory) as session:
            original = session.scalar(select(Attempt))
            assert original.unresolved and original.response_json is None
        with pytest.raises(Conflict, match="retention grant"):
            _cleanup_arm(ready)


def _receiving_after_retirement(ready):
    _select(ready)
    ticket = _cleanup_arm(ready)
    response = _response(ticket, ("disposed", "disposed"))
    with session_scope(ready.case.factory) as session:
        session.delete(session.get(Upload, ready.case.collection_id))
    _receive(ready, response)
    _receive(ready, response)
    assert ready.owner.current(ready.case.collection_id, ready.operation).complete
    _ack(ready, response)
    with session_scope(ready.case.factory) as session:
        assert session.get(Page, ready.operation) is not None
        assert session.get(Terminal, ready.operation) is not None
        assert session.scalar(select(func.count()).select_from(Attempt)) == 0


def test_original_cleanup_receiving_and_tombstone_outlive_upload_row(tmp_path):
    with _case(tmp_path) as case:
        _receiving_after_retirement(_ready(case))


def test_provider_execution_is_outside_writer_and_original_attempt_runs_only_once(tmp_path):
    with _case(tmp_path) as case:
        ready = _ready(case)
        _select(ready)
        ticket = _cleanup_arm(ready)
        calls = []

        class Target:
            def execute_cleanup(self, invocation):
                # A second connection acquires the real SQLite writer while
                # the provider callback is running.
                with session_scope(case.factory) as session:
                    _writer(session)
                    assert session.get(
                        Attempt, invocation.call.call_id
                    ).invocation_json == invocation.model_dump_json(exclude_none=True)
                calls.append(invocation)
                return _response(ticket, ("disposed", "disposed"))

        first = ready.owner.execute(ticket, Target())
        assert ready.owner.execute(ticket, Target()) == first
        assert len(calls) == 1
        _receive(ready, first)
        conflicting = _response(ticket, ("pending", "disposed"))
        with pytest.raises(Conflict, match="retained original"):
            _receive(ready, conflicting)
        assert ready.owner.current(case.collection_id, ready.operation).complete
        assert validate_db(case.config.database_url).condition == "current"


def test_complete_page_can_advance_without_recounting_confirmed_deletions(tmp_path):
    with _case(tmp_path) as case:
        ready = _ready(case)
        inventory_request, request = _request(ready)
        request = request.model_copy(
            update={
                "inventory": request.inventory.model_copy(
                    update={"continuation": "next", "exhausted": False}
                )
            }
        )
        page = _select(ready, inventory_request=inventory_request, request=request)
        first = _cleanup_arm(ready)
        _receive(ready, unknown_cleanup_execution(first.invocation))
        second = _cleanup_arm(ready)
        _receive(ready, _response(second, ("disposed", "disposed")))
        _ack(ready, _response(second, ("disposed", "disposed")))
        next_request, next_page = _request(ready, count=0, continuation="next")
        with pytest.raises(Conflict, match="different page"):
            _select(ready, inventory_request=next_request, request=next_page, after="f" * 64)
        selected = _select(
            ready, inventory_request=next_request, request=next_page, after=page.request_sha256
        )
        assert selected.complete and selected.request.inventory.exhausted
        # A late exact original response closes only its old physical charge.
        late = _response(first, ("disposed", "disposed"))
        _receive(ready, late)
        _ack(ready, late)
        assert ready.owner.current(case.collection_id, ready.operation) == selected
        with session_scope(case.factory) as session:
            assert session.scalar(select(func.count()).select_from(Page)) == 1
            assert session.scalar(select(func.count()).select_from(Attempt)) == 0


def test_filesystem_cleanup_reopens_from_retained_selection_after_lost_response(
    tmp_path, monkeypatch
):
    from uuid import UUID

    from a_riverhog_filesystem_store import (
        FilesystemStorageAdapter,
        FilesystemStorageAdapterConfig,
        incarnation,
    )

    with _case(tmp_path) as case:
        ready = _ready(case)
        root = tmp_path / "cleanup-store"
        root.mkdir()
        with monkeypatch.context() as patch:
            patch.setattr(incarnation, "uuid4", lambda: UUID(ready.incarnation))
            incarnation.provision_storage_root(root)
        config = FilesystemStorageAdapterConfig(
            root=root, segment_bytes=65536, read_chunk_bytes=65536, minimum_free_bytes=0
        )
        with FilesystemStorageAdapter(config) as adapter:
            for path in (ready.prefix + "payload", "unrelated/keep"):
                adapter.put_small_object(
                    SmallObjectWriteRequest(
                        object_path=path,
                        stored_bytes=1,
                        stored_sha256=hashlib.sha256(b"x").hexdigest(),
                        content_type="application/octet-stream",
                        required_identity_assertions={},
                        placement_policy="immediate_default",
                        mode="create_only",
                    ),
                    b"x",
                )
            target = StorageAdapterArchiveObjectCleanup(adapter)
            inventory_request = CleanupInventoryRequest(
                object_prefix=ready.prefix,
                target_incarnation=ready.incarnation,
                maximum_response_bytes=2048,
            )
            read = _inventory_arm(ready, inventory_request)
            observed = ready.owner.execute(read, target)
            _receive(ready, observed)
            inventory = observed.result.inventory
            request = _request(ready)[1].model_copy(update={"inventory": inventory})
            ready.owner.select_page(
                ready.case.collection_id,
                ready.operation,
                inventory_call_id=read.invocation.call.call_id,
                inventory_request=inventory_request,
                request=request,
                after=None,
                guard=_guard,
            )
            _ack(ready, observed)
            first = _cleanup_arm(ready)
            lost = ready.owner.execute(first, target)
            assert lost.result.selection_complete
            # Durable original intent remains; the reply never reached receiving.
        ready.owner = ArchiveUploadCleanup(case.factory, grants=GRANTS)
        with FilesystemStorageAdapter(config) as adapter:
            target = StorageAdapterArchiveObjectCleanup(adapter)
            second = _cleanup_arm(ready)
            result = ready.owner.execute(second, target)
            assert second.invocation.call.call_id != first.invocation.call.call_id
            _receive(ready, result)
            _ack(ready, result)
            assert ready.owner.current(case.collection_id, ready.operation).complete
            with session_scope(case.factory) as session:
                assert session.get(Attempt, first.invocation.call.call_id).unresolved
            _receive(ready, lost)
            _ack(ready, lost)
            untouched = target.list_cleanup_candidates(
                CleanupInventoryRequest(
                    object_prefix="unrelated/", target_incarnation=ready.incarnation
                )
            )
            assert len(untouched.candidates) == 1


def test_cleanup_reservation_seek_ignores_other_terminal_history(tmp_path, record_property):
    with _case(tmp_path) as case:
        ready = _ready(case)
        _select(ready)
        observations = []
        previous = 0
        for count in (16, 256, 4096):
            with session_scope(case.factory) as session:
                original = dict(
                    session.execute(
                        select(Terminal.__table__).where(Terminal.id == ready.operation)
                    )
                    .mappings()
                    .one()
                )
                terminals, attempts = [], []
                for index in range(previous, count):
                    value = json.loads(original["request_json"])
                    value["request"] = {"structural_history": index}
                    encoded = canonical_json_bytes(value).decode()
                    identity = hashlib.sha256(encoded.encode()).hexdigest()
                    terminals.append({**original, "id": identity, "request_json": encoded})
                    attempts.append(
                        {
                            "id": identity,
                            "terminal_id": identity,
                            "page_sha256": "a" * 64,
                            "dispatch_sha256": "b" * 64,
                            "invocation_json": "{}",
                            "unresolved": True,
                            "control_bytes": "1",
                            "provider_requests": "1",
                            "in_flight": "1",
                        }
                    )
                session.execute(insert(Terminal), terminals)
                session.execute(insert(Attempt), attempts)
            previous = count
            with session_scope(case.factory) as session:
                connection = session.connection().connection.driver_connection
                instructions = 0

                def step():
                    nonlocal instructions
                    instructions += 1
                    return 0

                connection.set_progress_handler(step, 1)
                try:
                    ready.owner._reserve(session, ready.operation, page_bytes=1)
                finally:
                    connection.set_progress_handler(None, 0)
                observations.append(instructions)
        assert max(observations) <= min(observations) + 2
        record_property("cleanup_reservation_history_sizes", [16, 256, 4096])
        record_property("cleanup_reservation_sqlite_vm_instructions", observations)
        # Structural index-cost rows are not provider execution/closure evidence.


@pytest.mark.parametrize("change", ("prefix", "incarnation"))
def test_cold_page_scope_is_rechecked_before_any_new_dispatch(tmp_path, change):
    from riverhog_storage_adapter_protocol import cleanup_step_sha256

    with _case(tmp_path) as case:
        ready = _ready(case)
        _select(ready)
        with session_scope(case.factory) as session:
            record = session.get(Page, ready.operation)
            request = CleanupStepRequest.model_validate_json(record.request_json)
            inventory = request.inventory.model_copy(
                update={
                    "candidates": (),
                    "scanned_items": 0,
                    **(
                        {"object_prefix": "other/"}
                        if change == "prefix"
                        else {"target_incarnation": "00000000-0000-4000-8000-000000000099"}
                    ),
                }
            )
            request = request.model_copy(update={"inventory": inventory})
            record.request_json = request.model_dump_json()
            record.request_sha256 = cleanup_step_sha256(request)
        with pytest.raises(Conflict, match="original archive scope"):
            _cleanup_arm(ready)


def test_retention_byte_grant_includes_page_and_attempt_record_overhead(tmp_path):
    with _case(tmp_path) as case:
        ready = _ready(case)
        page = _select(ready)
        with session_scope(case.factory) as session:
            stored = session.get(Page, ready.operation)
            page_bytes = ready.owner._page_bytes(
                ready.operation, page.request, stored.inventory_request_json
            )
        # The protocol's wire grant alone does not cover retained row metadata.
        ready.owner = ArchiveUploadCleanup(
            case.factory,
            grants=replace(GRANTS, control_bytes=page_bytes + page.request.maximum_control_bytes),
        )
        with pytest.raises(Conflict, match="retention grant"):
            _cleanup_arm(ready, page)
        with session_scope(case.factory) as session:
            assert session.scalar(select(func.count()).select_from(Attempt)) == 0


def test_new_attempt_budget_preserves_original_selection_and_every_earlier_charge(tmp_path):
    with _case(tmp_path) as case:
        ready = _ready(case)
        page = _select(ready)
        first = _cleanup_arm(ready, page)
        _receive(ready, unknown_cleanup_execution(first.invocation))
        second = _cleanup_arm(ready, page, maximum_provider_requests=5, maximum_in_flight=2)
        assert second.invocation.request.maximum_provider_requests == 5
        assert second.invocation.request.maximum_in_flight == 2
        assert ready.owner.current(case.collection_id, ready.operation).request == page.request
        assert first.invocation.request.maximum_provider_requests == 2
        assert first.invocation.call != second.invocation.call
        with session_scope(case.factory) as session:
            records = session.scalars(select(Attempt)).all()
            assert len(records) == 2 and all(row.unresolved for row in records)
            assert sum(int(row.provider_requests) for row in records) == 7
            assert sum(int(row.in_flight) for row in records) == 3


@pytest.mark.parametrize("action", ("select", "arm"))
@pytest.mark.parametrize("guard", (None, False))
def test_cleanup_admission_cannot_omit_its_runtime_disposal_guard(tmp_path, action, guard):
    with _case(tmp_path) as case:
        ready = _ready(case)
        if action == "arm":
            page = _select(ready)
        with session_scope(case.factory) as session:
            session.get(Terminal, ready.operation).phase = "draining"
        with pytest.raises(ValueError, match="requires a current disposal guard"):
            if action == "select":
                _select(ready, guard=guard)
            else:
                _cleanup_arm(ready, page, guard=guard)
        with session_scope(case.factory) as session:
            assert session.get(Terminal, ready.operation).phase == "draining"
            assert session.scalar(select(func.count()).select_from(Attempt)) == 0
            if action == "select":
                assert session.get(Page, ready.operation) is None
