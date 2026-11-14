from __future__ import annotations

import threading
from datetime import timedelta
from types import SimpleNamespace

import pytest
from riverhog_core.app_permissions import CATALOG_READ, COLLECTIONS_DELETE, ApplicationAccess
from riverhog_core.catalog_archive_write_models import ArchiveUploadTerminalRecord as Terminal
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services import archive_upload_terminal_registry as registry_module
from riverhog_core.services.archive_upload_catalog_cleanup import ArchiveUploadCatalogCleanup
from riverhog_core.services.archive_upload_checkpoint_cleanup import ArchiveUploadCheckpointCleanup
from riverhog_core.services.archive_upload_copy_intent_cleanup import ArchiveUploadCopyIntentCleanup
from riverhog_core.services.archive_upload_projection_cleanup import ArchiveUploadProjectionCleanup
from riverhog_core.services.archive_upload_registration_cleanup import (
    ArchiveUploadRegistrationCleanup,
)
from riverhog_core.services.archive_upload_terminal_turn import ArchiveUploadTerminalTurn
from riverhog_core.services.physical_execution import PhysicalExecutionKey
from riverhog_protocol.errors import Conflict, NotFound

from tests.unit.test_app_keys import BOOTSTRAP
from tests.unit.test_archive_upload_projection_cleanup import LIMITS, _count, _projection
from tests.unit.test_archive_upload_terminal_confirmation import (
    _confirmed,
    _owner,
    _prepared,
)


def _actor(ready, monkeypatch, *, discovery_root=None):
    case = ready.case
    registry = case.service._archive_terminals
    confirmation = _owner(ready)
    created = []

    def create(collection_id, operation_id):
        created.append(threading.current_thread().name)
        return ArchiveUploadTerminalTurn(
            case.factory,
            collection_id,
            operation_id,
            guard=ready.guard,
            adjudication=case.service._archive_terminal_adjudication,
            retirement=case.service._archive_terminal_retirement,
            cleanup_create=lambda: ready.ready.turn,
            registration_cleanup=ArchiveUploadRegistrationCleanup(case.factory, ready.ready.owner),
            registration_limits=LIMITS,
            catalog_cleanup=ArchiveUploadCatalogCleanup(case.factory, ready.ready.owner),
            checkpoint_cleanup=ArchiveUploadCheckpointCleanup(case.factory, ready.ready.owner),
            copy_intent_cleanup=ArchiveUploadCopyIntentCleanup(case.factory, ready.ready.owner),
            projection_cleanup=ArchiveUploadProjectionCleanup(
                case.factory, ready.ready.owner, discovery_root=discovery_root
            ),
            confirmation=confirmation,
            result_read_guard=ready.guard.read,
        )

    monkeypatch.setattr(registry, "_create", create)
    clock = [100.0]
    monkeypatch.setattr(registry_module, "monotonic", lambda: clock[0])
    return SimpleNamespace(
        ready=ready,
        registry=registry,
        confirmation=confirmation,
        created=created,
        key=PhysicalExecutionKey(case.collection_id, case.operation),
        clock=clock,
        provider_calls=len(ready.ready.target.calls),
        original_context=None,
    )


def _submit(actor):
    case = actor.ready.case
    assert (
        actor.registry.submit(
            actor.key,
            target_incarnation=case.incarnation,
            work_limit=1,
            retry_after=timedelta(seconds=1),
        )
        == 1
    )
    context = actor.registry._contexts[actor.key]
    if actor.original_context is None:
        actor.original_context = context
    else:
        assert context is actor.original_context
    return case.service._archive_execution._slots[actor.key]


def _receive(actor, slot):
    slot.worker.join(timeout=30)
    assert not slot.worker.is_alive() and slot.future.done()
    error = slot.future.exception()
    progress = None if error is not None else slot.future.result()
    assert actor.ready.case.service._archive_retention._counts == {False: 0, True: 1}
    count = actor.registry.collect(limit=1)
    actor.clock[0] += 2
    if error is not None:
        assert count == 1
        assert actor.ready.case.service._archive_retention._counts == {False: 0, True: 1}
        raise error
    if progress.execution_stopped:
        assert count == 2 and actor.key not in actor.registry._contexts
        assert actor.ready.case.service._archive_retention._counts == {False: 0, True: 0}
    else:
        assert count == 1 and actor.key in actor.registry._contexts
        assert actor.ready.case.service._archive_retention._counts == {False: 0, True: 1}
    return progress


def _quantum(actor):
    return _receive(actor, _submit(actor))


def _drive(actor):
    ready = actor.ready
    for _ in range(_count(ready) + 100):
        progress = _quantum(actor)
        if progress.execution_stopped:
            break
        assert progress.phase in {"cleanup-ready", "cleaning"}
    else:
        pytest.fail("same original actor did not receive terminal completion")
    assert progress.phase == "done" and progress.pending_reason is None
    assert actor.created == ["physical-execution"]
    assert len(ready.ready.target.calls) == actor.provider_calls
    with session_scope(ready.case.factory) as session:
        terminal = session.get(Terminal, ready.case.operation)
        assert terminal.phase == "done" and terminal.original_result_json is None
        assert terminal.result_json == ready.original
        assert session.get(Upload, ready.case.collection_id) is None
    return progress


def test_same_original_actor_confirms_only_after_all_cleanup_and_actual_joined_receiving(
    tmp_path, monkeypatch
):
    with _projection(tmp_path, indexed=True) as ready:
        actor = _actor(ready, monkeypatch)
        _drive(actor)
        assert _confirmed(ready) == ready.original


def _lost_ack(ready, monkeypatch):
    actor = _actor(ready, monkeypatch)
    original = actor.confirmation.confirm

    def lose(*args, **kwargs):
        original(*args, **kwargs)
        raise OSError("original terminal confirmation acknowledgement lost")

    with monkeypatch.context() as patch:
        patch.setattr(actor.confirmation, "confirm", lose)
        with pytest.raises(OSError, match="acknowledgement lost"):
            _drive(actor)
    assert _confirmed(ready) == ready.original
    case = ready.case
    case.keys.replace_access(
        app=case.principal.id,
        key_id=case.principal.key_id,
        access=(ApplicationAccess(CATALOG_READ),),
        grantor=BOOTSTRAP,
    )
    with pytest.raises(NotFound):
        _confirmed(ready)
    original_context = actor.original_context
    progress = _quantum(actor)
    assert progress.phase == "done" and progress.execution_stopped
    assert actor.created == ["physical-execution"] and actor.original_context is original_context
    assert len(ready.ready.target.calls) == actor.provider_calls
    with pytest.raises(NotFound):
        _confirmed(ready)
    case.keys.replace_access(
        app=case.principal.id,
        key_id=case.principal.key_id,
        access=(ApplicationAccess(COLLECTIONS_DELETE),),
        grantor=BOOTSTRAP,
    )
    assert _confirmed(ready) == ready.original


def test_lost_ack_releases_only_joined_original_custody_when_current_read_is_revoked(
    tmp_path, monkeypatch
):
    with _projection(tmp_path, indexed=True) as ready:
        _lost_ack(ready, monkeypatch)


def _blocked_after_commit(ready, monkeypatch):
    actor = _actor(ready, monkeypatch)
    original = actor.confirmation.confirm
    committed, release, boundary = threading.Event(), threading.Event(), threading.Event()

    def blocked(*args, **kwargs):
        result = original(*args, **kwargs)
        committed.set()
        boundary.set()
        assert release.wait(30), "original receiver was not released"
        return result

    with monkeypatch.context() as patch:
        patch.setattr(actor.confirmation, "confirm", blocked)
        for _ in range(_count(ready) + 100):
            boundary.clear()
            slot = _submit(actor)
            slot.future.add_done_callback(lambda future: boundary.set())
            assert boundary.wait(30), "original worker reached no receiving boundary"
            # Waiting in this test keeps the actual final physical worker alive;
            # the production coordinator is still exercised without joining it.
            if committed.is_set():
                try:
                    assert slot.worker.is_alive() and not slot.future.done()
                    assert (
                        actor.confirmation.receive_completion(
                            ready.case.collection_id,
                            ready.case.operation,
                            target_incarnation=ready.case.incarnation,
                        )
                        is True
                    )
                    assert actor.registry.collect(limit=1) == 0
                    assert ready.case.service._archive_retention._counts == {False: 0, True: 1}
                    assert actor.key in actor.registry._contexts
                    assert _confirmed(ready) == ready.original
                finally:
                    release.set()
                progress = _receive(actor, slot)
                assert progress.execution_stopped and progress.phase == "done"
                break
            _receive(actor, slot)
        else:
            release.set()
            pytest.fail("original confirmation never committed")
    assert actor.created == ["physical-execution"]
    assert len(ready.ready.target.calls) == actor.provider_calls


def test_confirmed_database_state_cannot_release_a_still_running_original_worker(
    tmp_path, monkeypatch
):
    with _projection(tmp_path, indexed=True) as ready:
        _blocked_after_commit(ready, monkeypatch)


def test_revocation_before_confirmation_keeps_the_original_actor_and_result_unconfirmed(
    tmp_path, monkeypatch
):
    with _projection(tmp_path, indexed=True) as ready:
        actor = _actor(ready, monkeypatch)
        assert not _quantum(actor).execution_stopped
        before = _count(ready)
        case = ready.case
        case.keys.replace_access(
            app=case.principal.id,
            key_id=case.principal.key_id,
            access=(ApplicationAccess(CATALOG_READ),),
            grantor=BOOTSTRAP,
        )
        with pytest.raises(NotFound):
            _quantum(actor)
        assert _count(ready) == before
        _prepared(ready)
        case.keys.replace_access(
            app=case.principal.id,
            key_id=case.principal.key_id,
            access=(ApplicationAccess(COLLECTIONS_DELETE),),
            grantor=BOOTSTRAP,
        )
        _drive(actor)


def test_linked_discovery_custody_remains_pending_instead_of_becoming_a_done_receipt(
    tmp_path, monkeypatch
):
    with _projection(tmp_path, indexed=True, linked=True) as ready:
        actor = _actor(ready, monkeypatch)
        for _ in range(100):
            progress = _quantum(actor)
            assert not progress.execution_stopped
            if progress.pending_reason == "discovery-custody":
                break
        else:
            pytest.fail("original actor did not expose retained discovery custody")
        assert _confirmed(ready) is None
        assert (
            actor.confirmation.receive_completion(
                ready.case.collection_id,
                ready.case.operation,
                target_incarnation=ready.case.incarnation,
            )
            is False
        )
        assert ready.discovery.marker.read_bytes() == b"original independently leased bytes"
        _prepared(ready)


def test_completion_receiving_checks_original_target_without_returning_result_data(
    tmp_path, monkeypatch
):
    with _projection(tmp_path) as ready:
        actor = _actor(ready, monkeypatch)
        assert (
            actor.confirmation.receive_completion(
                ready.case.collection_id,
                ready.case.operation,
                target_incarnation=ready.case.incarnation,
            )
            is False
        )
        with pytest.raises(Conflict, match="original target"):
            actor.confirmation.receive_completion(
                ready.case.collection_id,
                ready.case.operation,
                target_incarnation="another-original-target",
            )
        _drive(actor)
        assert (
            actor.confirmation.receive_completion(
                ready.case.collection_id,
                ready.case.operation,
                target_incarnation=ready.case.incarnation,
            )
            is True
        )


def test_confirmation_composition_cannot_skip_the_original_projection_owner(tmp_path):
    with _projection(tmp_path) as ready:
        case = ready.case
        with pytest.raises(ValueError, match="projection"):
            ArchiveUploadTerminalTurn(
                case.factory,
                case.collection_id,
                case.operation,
                guard=ready.guard,
                adjudication=case.service._archive_terminal_adjudication,
                retirement=case.service._archive_terminal_retirement,
                confirmation=_owner(ready),
                result_read_guard=ready.guard.read,
            )
