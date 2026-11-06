from __future__ import annotations

import hashlib
import threading
from contextlib import ExitStack
from types import SimpleNamespace

import pytest
from riverhog_core.archive_execution_tuning import ArchiveExecutionTuning
from riverhog_core.catalog_archive_write_models import ArchiveUploadTerminalRecord
from riverhog_core.catalog_db import session_scope
from riverhog_core.services import archive_upload_execution as execution_module
from riverhog_core.services.archive_construction_retention import ArchiveConstructionRetention
from riverhog_core.services.archive_finalizer_registry import ArchiveFinalizerRegistry
from riverhog_core.services.archive_upload_execution import (
    ArchiveUploadExecutionKey,
    ArchiveUploadExecutionOwner,
)
from riverhog_core.services.archive_upload_invocations import ArchiveUploadInvocationRegistry
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from riverhog_protocol.errors import Conflict
from riverhog_storage_adapter_protocol import CleanupInventoryRequest

from tests.unit.test_archive_finalizer_registry import _scope
from tests.unit.test_archive_finalizer_registry import _submit as _finalizer_submit
from tests.unit.test_archive_upload_writes import _actor, _open, _stored, _unit
from tests.unit.test_collection_upload_preparation import _case
from tests.unit.test_collection_uploads import _CREATOR


class Original:
    def __init__(self):
        self.pending = False
        self.calls = 0
        self.drains = []

    @property
    def has_pending(self):
        return self.pending

    def drain_terminal(self, operation_id):
        self.drains.append(operation_id)
        return "pending" if self.pending else "closed"


def _key(number=1):
    return ArchiveUploadExecutionKey(number, "a" * 64)


def _request(number=1):
    return hashlib.sha256(str(number).encode()).hexdigest()


@pytest.fixture
def owned():
    registries, releases = [], []

    def make(factory=None, *, execution=None, retention=None, **values):
        tuning = ArchiveExecutionTuning(**values)
        pool = execution or ArchiveUploadExecutionOwner(tuning)
        budget = retention or ArchiveConstructionRetention(tuning)
        registry = ArchiveUploadInvocationRegistry(factory, pool, budget)
        registries.append(registry)
        return registry, pool, budget

    def event():
        release = threading.Event()
        releases.append(release)
        return release

    yield SimpleNamespace(make=make, event=event)
    for release in releases:
        release.set()
    for registry in registries:
        registry.close()


def _submit(registry, number=1, *, key=None, target="target-a", create=Original, advance=None):
    return registry.submit(
        key or _key(),
        target_incarnation=target,
        request_sha256=_request(number),
        create=create,
        advance=advance or (lambda actor: {"request": number}),
    )


def _receive(registry, pool):
    for slot in tuple(pool._slots.values()):
        slot.worker.join(timeout=10)
        assert not slot.worker.is_alive()
    return registry.collect(limit=100)


def test_successful_original_reply_is_received_repeatedly_without_dispatch_or_reconstruction(owned):
    registry, pool, _ = owned.make()
    calls = []

    def create():
        calls.append("create")
        return Original()

    def advance(owner):
        calls.append("advance")
        return {"payload": "original"}

    assert _submit(registry, create=create, advance=advance) == "submitted"
    (completion,) = _receive(registry, pool)
    assert completion.result() == {"payload": "original"}
    assert registry.completed(_request()).execution is completion.execution
    assert (
        _submit(
            registry,
            create=lambda: pytest.fail("original constructor must be retained"),
            advance=lambda actor: pytest.fail("original reply cannot redispatch"),
        )
        == "retained"
    )
    assert calls == ["create", "advance"]
    assert registry.retire_idle(completion)
    with pytest.raises(Conflict, match="original received"):
        registry.retire_idle(completion)


def test_failed_original_continuation_keeps_its_inputs_and_independent_charge(owned):
    registry, pool, _ = owned.make()
    original = Original()

    def advance(owner):
        assert owner is original
        owner.calls += 1
        owner.pending = owner.calls == 1
        if owner.pending:
            raise OSError("original reply lost")
        return "confirmed original"

    assert _submit(registry, create=lambda: original, advance=advance) == "submitted"
    (failed,) = _receive(registry, pool)
    with pytest.raises(OSError, match="reply lost"):
        failed.result()
    assert not registry.retire_idle(failed)
    assert (
        _submit(
            registry,
            create=lambda: pytest.fail("cannot reconstruct pending original"),
            advance=lambda owner: pytest.fail("cannot replace original arguments"),
        )
        == "submitted"
    )
    (confirmed,) = _receive(registry, pool)
    assert confirmed.result() == "confirmed original" and original.calls == 2
    with pytest.raises(Conflict, match="original received"):
        registry.retire_idle(failed)
    assert registry.retire_idle(confirmed)


def test_later_unit_can_run_without_releasing_an_earlier_pending_owner(owned):
    registry, pool, _ = owned.make(retained_constructions=2)
    original = Original()
    original.pending = True
    assert _submit(registry, create=lambda: original) == "submitted"
    (first,) = _receive(registry, pool)
    assert not registry.retire_idle(first)
    assert _submit(registry, 2) == "submitted"
    (second,) = _receive(registry, pool)
    assert second.request_sha256 == _request(2)
    assert _submit(registry, 3) == "retention-capacity"
    assert registry.retire_idle(second)
    assert _submit(registry, 3) == "submitted"
    _receive(registry, pool)
    assert registry._invocations[_request()].owner is original and original.pending


def test_payload_and_metadata_registries_spend_one_global_retention_budget(owned):
    registry, pool, retention = owned.make(retained_constructions=1)
    original = Original()
    original.pending = True
    assert _submit(registry, create=lambda: original) == "submitted"
    (first,) = _receive(registry, pool)
    metadata = ArchiveFinalizerRegistry(None, pool, retention)
    assert (
        _finalizer_submit(
            metadata,
            _key(2),
            target="target-b",
            create=lambda: pytest.fail("shared budget must precede metadata construction"),
        )
        == "retention-capacity"
    )
    original.pending = False
    assert registry.retire_idle(first)
    assert _finalizer_submit(metadata, _key(2), target="target-b") == "submitted"
    for slot in tuple(pool._slots.values()):
        slot.worker.join(timeout=5)
        assert not slot.worker.is_alive()
    (completed,) = metadata.collect(limit=10)
    assert completed.result().progressed
    assert metadata.retire_idle(_key(2), target_incarnation="target-b")


def test_unknown_constructor_remains_charged_and_cannot_be_recreated(owned):
    registry, pool, _ = owned.make(retained_constructions=1)
    calls = []

    def create():
        calls.append("create")
        raise OSError("constructor acknowledgement")

    assert _submit(registry, create=create) == "submitted"
    (failed,) = _receive(registry, pool)
    with pytest.raises(OSError, match="constructor acknowledgement"):
        failed.result()
    assert not registry.retire_idle(failed)
    assert not registry.retire_unstarted(_request())
    assert _submit(registry, 2) == "retention-capacity"
    assert (
        _submit(registry, create=lambda: pytest.fail("unknown original cannot be recreated"))
        == "submitted"
    )
    (retry,) = _receive(registry, pool)
    with pytest.raises(Conflict, match="constructor outcome"):
        retry.result()
    assert calls == ["create"]


def test_busy_shared_retention_never_loses_an_unstarted_original(owned, monkeypatch):
    registry, pool, retention = owned.make(forward_workers=1)
    release = owned.event()
    assert (
        pool.try_submit(
            _key(9),
            target_incarnation="target-a",
            terminal=False,
            work=lambda: release.wait(10),
        )
        == "submitted"
    )
    actual = pool.try_submit

    def refuse(*args, **kwargs):
        result = actual(*args, **kwargs)
        assert result == "capacity"
        retention._lock.acquire()
        return result

    monkeypatch.setattr(pool, "try_submit", refuse)
    try:
        assert _submit(registry, create=lambda: pytest.fail("unstarted constructor")) == "capacity"
        assert not registry.retire_unstarted(_request())
    finally:
        retention._lock.release()
    assert registry.retire_unstarted(_request())
    release.set()


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("mode", ("direct", "owned-http"))
def test_service_registry_retains_actual_begin_through_terminal_fence_and_receiving(
    tmp_path,
    monkeypatch,
    kind,
    mode,
):
    _actual_begin_receiving(tmp_path, monkeypatch, kind, mode)


def _actual_begin_receiving(tmp_path, monkeypatch, kind, mode, *, database_url=None):
    with _case(tmp_path, kind, database_url=database_url) as case, ExitStack() as resources:
        admission = ArchiveWriteAdmission(case.factory)
        guard = case.service._payload_write_guard(case.collection_id, _CREATOR)
        admission.bind_epoch(case.collection_id, guard=guard)
        key, target = _scope(case)
        registry = case.service._archive_invocations
        pool = case.service._archive_execution
        entered, release = threading.Event(), threading.Event()
        originals, checkpoints = [], []

        def create():
            # The actual service's shared reservation precedes provider/uploader construction.
            assert _request() in registry._invocations
            actor = resources.enter_context(
                _actor(tmp_path, monkeypatch, kind, mode=mode, existing_case=case)
            )
            actual = actor.executor.execute_write

            def held(invocation, *, content=b""):
                if invocation.operation.kind == "begin":
                    entered.set()
                    assert release.wait(10)
                return actual(invocation, content=content)

            monkeypatch.setattr(actor.executor, "execute_write", held)
            originals.append(actor)
            return actor.journal

        def advance(owner):
            assert owner is originals[0].journal
            checkpoint = _open(originals[0])
            checkpoints.append(checkpoint)
            return checkpoint.to_json()

        try:
            assert (
                registry.submit(
                    key,
                    target_incarnation=target,
                    request_sha256=_request(),
                    create=create,
                    advance=advance,
                )
                == "submitted"
            )
            assert entered.wait(5)
            actor = originals[0]
            preparation = actor.arguments["prepared_state"]
            before = _stored(actor)
            assert before[0] is None and len(before[1]) == 1
            operation = admission.select_terminal(
                case.collection_id,
                kind="cancel",
                request={"fixture": "retained-unit"},
                principal_id=_CREATOR.id,
                grants=case.service._config.archive_write,
                guard=guard,
                validate_selection=lambda session, upload: None,
            )
            assert (
                registry.fence_terminal(
                    key,
                    target_incarnation=target,
                    operation_id=operation,
                )
                == "fenced"
            )
            slot = pool._slots[case.collection_id]

            def forbidden(timeout=None):
                pytest.fail("point coordination cannot join the live original")

            with monkeypatch.context() as patch:
                patch.setattr(slot.worker, "join", forbidden)
                assert registry.collect(limit=1) == ()
                assert (
                    registry.submit_terminal(
                        key,
                        target_incarnation=target,
                        operation_id=operation,
                    )
                    == "busy"
                )
            with pytest.raises(Conflict, match="fenced"):
                _submit(registry, 2, key=key, target=target)
            assert _stored(actor) == before
            release.set()
            (forward,) = _receive(registry, pool)
            assert _stored(actor) == (forward.result(), [])
            assert actor.arguments["prepared_state"] is preparation
            calls = list(actor.seen)
            with pytest.raises(Conflict):
                _unit(actor, checkpoints[0])
            assert actor.seen == calls
            assert (
                registry.submit_terminal(
                    key,
                    target_incarnation=target,
                    operation_id=operation,
                )
                == "submitted"
            )
            (terminal,) = _receive(registry, pool)
            assert terminal.execution.terminal and terminal.result() == "closed"
            assert registry.completed(_request()).result() == forward.result()
            assert (
                registry.fence_terminal(
                    key,
                    target_incarnation=target,
                    operation_id=operation,
                )
                == "drained"
            )
            assert not registry.retire_idle(forward)
            with session_scope(case.factory) as session:
                assert session.get(ArchiveUploadTerminalRecord, operation).phase == "draining"
            page = actor.adapter.list_cleanup_candidates(
                CleanupInventoryRequest(
                    object_prefix=actor.arguments["object_path"],
                    target_incarnation=target,
                )
            )
            assert len(page.candidates) == 1 and page.candidates[0].kind == "open_write"
        finally:
            release.set()
            registry.close()


@pytest.mark.parametrize("change", ("operation", "construction", "target"))
def test_rejected_terminal_identity_does_not_fence_the_original_upload(tmp_path, owned, change):
    from tests.unit.test_archive_write_admission import _case as admission_case
    from tests.unit.test_archive_write_admission import _selected

    with admission_case(tmp_path) as case:
        operation = _selected(case)
        key, target = _scope(case)
        registry, pool, _ = owned.make(case.factory)
        assert _submit(registry, key=key, target=target) == "submitted"
        _receive(registry, pool)
        arguments = {"target_incarnation": target, "operation_id": operation}
        supplied_key = key
        if change == "operation":
            arguments["operation_id"] = "f" * 64
        elif change == "construction":
            supplied_key = ArchiveUploadExecutionKey(key.collection_id, "f" * 64)
        else:
            arguments["target_incarnation"] = "another-target"
        with pytest.raises(Conflict):
            registry.fence_terminal(supplied_key, **arguments)
        assert registry._constructions[key.collection_id].terminal_id is None
        assert (
            registry.fence_terminal(
                key,
                target_incarnation=target,
                operation_id=operation,
            )
            == "fenced"
        )


def test_original_payload_reply_waits_for_the_actual_worker_exit(owned, monkeypatch):
    registry, pool, _ = owned.make(retained_constructions=1)
    returned, release = threading.Event(), owned.event()

    class HeldExit(threading.Thread):
        def run(self):
            super().run()
            returned.set()
            assert release.wait(10)

    monkeypatch.setattr(execution_module, "Thread", HeldExit)
    assert _submit(registry) == "submitted"
    assert returned.wait(5)
    assert pool._slots[1].future.done()
    assert registry.completed(_request()) is None
    assert registry.collect(limit=1) == ()
    assert _submit(registry, 2, key=_key(2), target="target-b") == "retention-capacity"
    release.set()
    (received,) = _receive(registry, pool)
    assert received.result() == {"request": 1}
    assert registry.retire_idle(received)
    assert _submit(registry, 2, key=_key(2), target="target-b") == "submitted"


def test_missing_original_upload_receiver_cannot_become_terminal_drainage(tmp_path, owned):
    from tests.unit.test_archive_write_admission import _case as admission_case
    from tests.unit.test_archive_write_admission import _selected

    with admission_case(tmp_path) as case:
        operation = _selected(case)
        key, target = _scope(case)
        registry, pool, _ = owned.make(case.factory)
        assert _submit(registry, key=key, target=target, create=lambda: None) == "submitted"
        (failed,) = _receive(registry, pool)
        with pytest.raises(ValueError, match="no receiver"):
            failed.result()
        assert (
            registry.submit_terminal(
                key,
                target_incarnation=target,
                operation_id=operation,
            )
            == "submitted"
        )
        (pending,) = _receive(registry, pool)
        assert pending.result() == "pending"
        assert (
            registry.fence_terminal(
                key,
                target_incarnation=target,
                operation_id=operation,
            )
            == "fenced"
        )
