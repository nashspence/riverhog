from __future__ import annotations

import hashlib
import json
import threading
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID

import pytest
from riverhog_core.catalog_archive_write_models import ArchiveUploadTerminalRecord as Terminal
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord as Control
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.catalog_object_cleanup_models import ObjectCleanupAttemptRecord as Attempt
from riverhog_core.catalog_object_cleanup_models import ObjectCleanupPageRecord as Page
from riverhog_core.cleanup_execution_tuning import CleanupExecutionTuning
from riverhog_core.services.archive_upload_cleanup import ArchiveUploadCleanup
from riverhog_core.services.archive_upload_cleanup_turn import ArchiveUploadCleanupTurn
from riverhog_core.services.archive_upload_drain import lock_terminal_selection
from riverhog_core.services.archive_upload_terminal_turn import ArchiveUploadTerminalTurn
from riverhog_core.services.physical_execution import PhysicalExecutionKey
from riverhog_core.stores.storage_adapter_archive_objects import StorageAdapterArchiveObjectCleanup
from riverhog_protocol.errors import Conflict
from riverhog_storage_adapter_protocol import (
    CleanupCandidate,
    CleanupInventoryPage,
    CleanupInventoryRequest,
    CleanupItemOutcome,
    SmallObjectWriteRequest,
    cleanup_execution_result,
    cleanup_inventory_execution_result,
    unknown_cleanup_execution,
)
from sqlalchemy import select

from tests.unit.test_archive_upload_cleanup import GRANTS
from tests.unit.test_archive_upload_maintenance import _join
from tests.unit.test_archive_upload_terminal_turn import _drained, _turn_case


class _Target:
    def __init__(self, case, prefix, guard):
        self.case, self.prefix, self.guard = case, prefix, guard
        self.calls, self.disposed = [], set()
        self.items = tuple(
            CleanupCandidate(
                object_path=prefix + str(index), kind=kind, token="original-" + str(index)
            )
            for index, kind in enumerate(("object_revision", "deletion_marker", "open_write"))
        )

    def execute_cleanup(self, invocation):
        with session_scope(self.case.factory) as session:
            lock_terminal_selection(
                session,
                self.case.collection_id,
                self.case.operation,
                nowait=True,
                guard=self.guard,
            )
        self.calls.append(invocation)
        request = invocation.request
        if request.kind == "inventory":
            inventory = request.inventory
            start = int(inventory.continuation or "0")
            end = min(len(self.items), start + inventory.maximum_items)
            exhausted = end - start < inventory.maximum_items
            page = CleanupInventoryPage(
                object_prefix=self.prefix,
                target_incarnation=self.case.incarnation,
                candidates=tuple(
                    item for item in self.items[start:end] if item.token not in self.disposed
                ),
                scanned_items=end - start,
                continuation=None if exhausted else str(end),
                exhausted=exhausted,
            )
            return cleanup_inventory_execution_result(
                invocation, page, provider_requests=1, execution="closed"
            )
        outcomes = []
        for item in request.inventory.candidates:
            assert item in self.items
            self.disposed.add(item.token)
            outcomes.append(CleanupItemOutcome(candidate=item, outcome="disposed"))
        return cleanup_execution_result(
            invocation, tuple(outcomes), provider_requests=1, execution="closed"
        )


def _bind(case, *, items=1, target=None):
    guard = case.service._terminal_write_guard(case.collection_id, case.operation)
    owner = ArchiveUploadCleanup(case.factory, grants=GRANTS)
    with session_scope(case.factory) as session:
        prefix = json.loads(session.get(Control, case.collection_id).construction_json)[
            "archive_storage_prefix"
        ]
    target = target or _Target(case, prefix, guard)
    turn = ArchiveUploadCleanupTurn(
        case.factory,
        case.collection_id,
        case.operation,
        owner=owner,
        target=target,
        guard=guard,
        tuning=replace(CleanupExecutionTuning(), inventory_items=items),
    )
    return SimpleNamespace(
        case=case, owner=owner, target=target, guard=guard, turn=turn, prefix=prefix
    )


@contextmanager
def _ready(tmp_path, *, database_url=None, items=1):
    with _turn_case(tmp_path, database_url=database_url, originals=0) as case:
        _drained(case)
        assert (
            case.service._terminal_turn(case.collection_id, case.operation)
            .advance(limit=1)
            .originals_retired
        )
        yield _bind(case, items=items)


def _complete(ready, *, maximum_turns=24):
    for _ in range(maximum_turns):
        before = len(ready.target.calls)
        status = ready.turn.advance()
        assert len(ready.target.calls) - before <= 1
        if status == "inventory-exhausted":
            return
    pytest.fail("bounded archive cleanup did not retain its complete inventory proof")


def _assert_complete(ready):
    _complete(ready)
    assert {item.token for item in ready.target.items} == ready.target.disposed
    assert len({call.call.call_id for call in ready.target.calls}) == len(ready.target.calls)
    with session_scope(ready.case.factory) as session:
        page = ready.owner.require_exhausted(
            session, ready.case.collection_id, ready.case.operation, guard=ready.guard
        )
        assert page.complete and page.request.inventory.exhausted
        terminal = session.get(Terminal, ready.case.operation)
        assert terminal.phase == "cleaning" and terminal.result_json is None
        assert terminal.original_result_json is not None
        assert session.get(Upload, ready.case.collection_id) is not None
        assert session.get(Page, ready.case.operation) is not None
        assert session.scalars(select(Attempt)).all() == []


@pytest.mark.parametrize("items", (1, 2, 4))
def test_archive_turn_owns_bounded_mixed_inventory_without_confirming_terminal(tmp_path, items):
    with _ready(tmp_path, items=items) as ready:
        _assert_complete(ready)


_FAULTS = (
    ("record_response", "inventory"),
    ("record_response", "dispose"),
    ("select_page", "inventory"),
    ("acknowledge_response", "inventory"),
    ("acknowledge_response", "dispose"),
)


def _failed_receiving(ready, monkeypatch, method, stage, committed):
    if stage == "dispose":
        assert ready.turn.advance() == "pending"
    actual = getattr(ready.owner, method)

    def lose(*args, **kwargs):
        if committed:
            actual(*args, **kwargs)
        raise OSError("original archive receiving reply lost")

    with monkeypatch.context() as patch:
        patch.setattr(ready.owner, method, lose)
        with pytest.raises(OSError, match="archive receiving"):
            ready.turn.advance()
    count = len(ready.target.calls)
    assert ready.turn.advance() == "pending"
    assert len(ready.target.calls) == count
    _assert_complete(ready)


@pytest.mark.parametrize("method,stage", _FAULTS)
@pytest.mark.parametrize("committed", (False, True))
def test_archive_receiving_retries_original_reply_without_provider_reexecution(
    tmp_path, monkeypatch, method, stage, committed
):
    with _ready(tmp_path) as ready:
        _failed_receiving(ready, monkeypatch, method, stage, committed)


def _unknown(ready, monkeypatch):
    actual = ready.target.execute_cleanup
    unknown = []

    def execute(invocation):
        if not unknown:
            unknown.append(invocation)
            ready.target.calls.append(invocation)
            return unknown_cleanup_execution(invocation)
        return actual(invocation)

    monkeypatch.setattr(ready.target, "execute_cleanup", execute)
    for _ in range(24):
        try:
            ready.turn.advance()
        except Conflict as error:
            assert "original cleanup receiving" in str(error)
            break
    else:
        pytest.fail("unknown original cleanup was incorrectly treated as complete")
    original = unknown[0]
    with session_scope(ready.case.factory) as session:
        retained = session.get(Attempt, original.call.call_id)
        assert retained.unresolved and retained.response_json is not None
        assert session.get(Terminal, ready.case.operation).result_json is None
    assert {item.token for item in ready.target.items} == ready.target.disposed
    count = len(ready.target.calls)
    with pytest.raises(Conflict, match="original cleanup receiving"):
        ready.turn.advance()
    assert len(ready.target.calls) == count


def test_exhausted_inventory_cannot_close_an_unknown_original_call(tmp_path, monkeypatch):
    with _ready(tmp_path) as ready:
        _unknown(ready, monkeypatch)


def _composed(case, monkeypatch, *, failed=False):
    _drained(case)
    ready = _bind(case)
    created = []
    registry = case.service._archive_terminals

    def cleanup():
        assert threading.current_thread().name == "physical-execution"
        created.append(True)
        if failed:
            raise OSError("original archive cleanup constructor unknown")
        ready.turn = ArchiveUploadCleanupTurn(
            case.factory,
            case.collection_id,
            case.operation,
            owner=ready.owner,
            target=ready.target,
            guard=ready.guard,
            tuning=replace(CleanupExecutionTuning(), inventory_items=1),
        )
        return ready.turn

    def original(collection_id, operation_id):
        return ArchiveUploadTerminalTurn(
            case.factory,
            collection_id,
            operation_id,
            guard=ready.guard,
            adjudication=case.service._archive_terminal_adjudication,
            retirement=case.service._archive_terminal_retirement,
            cleanup_create=cleanup,
        )

    monkeypatch.setattr(registry, "_create", original)
    key = PhysicalExecutionKey(case.collection_id, case.operation)
    from datetime import timedelta

    from riverhog_core.services import archive_upload_terminal_registry as module

    clock = [100.0]
    monkeypatch.setattr(module, "monotonic", lambda: clock[0])
    for _ in range(24 if not failed else 3):
        assert (
            registry.submit(
                key,
                target_incarnation=case.incarnation,
                work_limit=1,
                retry_after=timedelta(seconds=1),
            )
            == 1
        )
        _join(case.service)
        assert registry.collect(limit=1) == 1
        context = registry._contexts[key]
        if not failed and context.progress.archive_inventory_exhausted:
            break
        clock[0] += 2
    else:
        if not failed:
            pytest.fail("original terminal actor did not compose archive cleanup")
    assert len(created) == 1
    assert case.service._archive_retention._counts == {False: 0, True: 1}
    assert not registry._execution._slots
    with session_scope(case.factory) as session:
        assert session.get(Terminal, case.operation).result_json is None
    if failed:
        assert ready.target.calls == []
    else:
        assert registry._contexts[key].progress.originals_retired
        assert not registry._contexts[key].progress.execution_stopped
        _assert_complete(ready)


@pytest.mark.parametrize("failed", (False, True))
def test_existing_terminal_worker_composes_cleanup_and_retains_unknown_child_constructor(
    tmp_path, monkeypatch, failed
):
    with _turn_case(tmp_path, originals=2) as case:
        _composed(case, monkeypatch, failed=failed)


def _filesystem(ready, tmp_path, monkeypatch):
    from a_riverhog_filesystem_store import (
        FilesystemStorageAdapter,
        FilesystemStorageAdapterConfig,
        incarnation,
    )

    root = tmp_path / "archive-cleanup-store"
    root.mkdir()
    with monkeypatch.context() as patch:
        patch.setattr(incarnation, "uuid4", lambda: UUID(ready.case.incarnation))
        incarnation.provision_storage_root(root)
    config = FilesystemStorageAdapterConfig(
        root=root, segment_bytes=65536, read_chunk_bytes=65536, minimum_free_bytes=0
    )
    with FilesystemStorageAdapter(config) as adapter:
        for path in (ready.prefix + "payload", "independent/keep"):
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
        port = StorageAdapterArchiveObjectCleanup(adapter)
        calls = []

        def execute(invocation):
            calls.append(invocation)
            return port.execute_cleanup(invocation)

        target = SimpleNamespace(execute_cleanup=execute, calls=calls)
        bound = _bind(ready.case, target=target)
        _complete(bound)
        remaining = port.list_cleanup_candidates(
            CleanupInventoryRequest(
                object_prefix="independent/", target_incarnation=ready.case.incarnation
            )
        )
        assert len(remaining.candidates) == 1
        empty = port.list_cleanup_candidates(
            CleanupInventoryRequest(
                object_prefix=ready.prefix, target_incarnation=ready.case.incarnation
            )
        )
        assert empty.exhausted and empty.candidates == ()
        assert {call.request.kind for call in calls} == {"inventory", "dispose"}


def test_actual_filesystem_archive_cleanup_preserves_unrelated_scope(tmp_path, monkeypatch):
    with _ready(tmp_path) as ready:
        _filesystem(ready, tmp_path, monkeypatch)


@pytest.mark.parametrize("stage", ("before-next-admission", "before-inventory-selection"))
def test_current_permission_revocation_pauses_archive_disposal_and_keeps_original_reply(
    tmp_path, monkeypatch, stage
):
    from riverhog_core.app_permissions import (
        COLLECTIONS_CREATE,
        COLLECTIONS_DELETE,
        ApplicationAccess,
    )
    from riverhog_protocol.errors import NotFound

    from tests.unit.test_app_keys import BOOTSTRAP

    with _ready(tmp_path) as ready:
        case = ready.case

        def access(permission):
            case.keys.replace_access(
                app=case.principal.id,
                key_id=case.principal.key_id,
                access=(ApplicationAccess(permission),),
                grantor=BOOTSTRAP,
            )

        if stage == "before-inventory-selection":
            actual = ready.target.execute_cleanup

            def revoke(invocation):
                result = actual(invocation)
                access(COLLECTIONS_CREATE)
                return result

            with monkeypatch.context() as patch:
                patch.setattr(ready.target, "execute_cleanup", revoke)
                with pytest.raises(NotFound):
                    ready.turn.advance()
        else:
            assert ready.turn.advance() == "pending"
            access(COLLECTIONS_CREATE)
            with pytest.raises(NotFound):
                ready.turn.advance()
        assert len(ready.target.calls) == 1 and ready.target.disposed == set()
        access(COLLECTIONS_DELETE)
        before = len(ready.target.calls)
        if stage == "before-inventory-selection":
            assert ready.turn.advance() == "pending"
            assert len(ready.target.calls) == before
        _assert_complete(ready)
