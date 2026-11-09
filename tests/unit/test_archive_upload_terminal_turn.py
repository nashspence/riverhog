from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import pytest
from riverhog_core.app_permissions import COLLECTIONS_CREATE, COLLECTIONS_DELETE, ApplicationAccess
from riverhog_core.catalog_archive_write_models import ArchiveUploadTerminalRecord as Terminal
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord as Control
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord as Volume
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services import physical_execution as physical
from riverhog_core.services.archive_upload_terminal_result import ArchiveUploadTerminalResults
from riverhog_core.services.archive_write_admission import _writer
from riverhog_protocol.errors import NotFound
from sqlalchemy import event
from sqlalchemy.exc import InvalidRequestError

from tests.unit.test_app_keys import BOOTSTRAP
from tests.unit.test_archive_upload_terminal_result import _state
from tests.unit.test_archive_upload_terminal_retirement import (
    _drain,
    _failed_forward,
    _install,
    _join,
    _retained,
)
from tests.unit.test_archive_upload_terminal_tags import _tagged
from tests.unit.test_archive_write_admission import (
    _apply,
    _arm,
    _case,
    _closure,
    _prepare,
)
from tests.unit.test_upload_cancel_authority import _select as _select_cancel
from tests.unit.test_upload_discard_authority import _operator, _select
from tests.unit.test_upload_terminal_read_authority import _creator_key


@contextmanager
def _turn_case(tmp_path, *, database_url=None, originals=1, prepare=True, armed=False):
    with _case(tmp_path, database_url=database_url) as case:
        case.registries = []
        if armed:
            _prepare(case)
            case.ticket = _arm(case)
        for number in range(originals):
            registry = (
                _failed_forward(case, "metadata")
                if number == 0
                else (_install(case, "payload", number))
            )
            if number:
                _join(case, registry)[0].result()
            case.registries.append(registry)
        case.keys, case.principal = _operator(case)
        with session_scope(case.factory) as session:
            session.get(Upload, case.collection_id).state = "orphaned"
        case.operation = _select(
            case,
            case.principal,
            case.service._discard_write_guard(case.collection_id, case.principal),
        )
        if prepare:
            _original_result(case)
        yield case


def _original_result(case):
    return ArchiveUploadTerminalResults(case.factory).prepare(
        case.collection_id,
        case.operation,
        {"status": "discarded", "collection_id": str(case.collection_id), "files": 1},
        guard=case.service._terminal_write_guard(case.collection_id, case.operation),
    )


def _drained(case):
    for registry in case.registries:
        _drain(case, registry, case.operation)


def _turn(case):
    return case.service._terminal_turn(case.collection_id, case.operation)


def _advance(case, turn, *, limit=1):
    result = turn.advance(limit=limit)
    with session_scope(case.factory) as session:
        assert session.get(Terminal, case.operation).result_json is None
    return result


def _bounded_turn(case):
    _drained(case)
    turn = _turn(case)
    assert turn.key.collection_id == case.collection_id
    assert turn.key.scope_sha256 == case.operation
    assert turn.target_incarnation == case.incarnation
    for remaining in range(len(case.registries) - 1, -1, -1):
        result = _advance(case, turn, limit=1)
        assert result.phase == "cleanup-ready" and result.retired_originals == 1
        assert _retained(case) == remaining
        assert result.originals_retired is (remaining == 0)
        assert result.pending_reason == (None if remaining == 0 else "retained-originals")
    result = _advance(case, turn)
    assert result.retired_originals == 0 and result.originals_retired
    return turn


def test_original_terminal_turn_composes_global_decision_and_bounded_retirement(tmp_path):
    with _turn_case(tmp_path, originals=3) as case:
        _bounded_turn(case)


def test_execution_waits_for_original_drain_then_for_reserved_original_result(tmp_path):
    with _turn_case(tmp_path, prepare=False) as case:
        turn = _turn(case)
        result = _advance(case, turn)
        assert result.phase == "draining" and result.pending_reason == "local-writers"
        assert not result.originals_retired and _retained(case) == 1
        _drained(case)
        result = _advance(case, turn)
        assert result.pending_reason == "original-result" and _retained(case) == 1
        _original_result(case)
        result = _advance(case, turn)
        assert result.phase == "cleanup-ready" and result.originals_retired
        assert _retained(case) == 0


def test_original_terminal_turn_cannot_replace_unknown_or_unreceived_forward_work(tmp_path):
    with _turn_case(tmp_path, originals=0, armed=True) as case:
        turn = _turn(case)
        assert _advance(case, turn).pending_reason == "forward-effects"
        case.owner.settle_closed(case.ticket, _closure(case, case.ticket), apply_result=_apply)
        assert _advance(case, turn).pending_reason == "original-receiving"
        case.owner.acknowledge_report(case.ticket)
        result = _advance(case, turn)
        assert result.phase == "cleanup-ready" and result.originals_retired


def test_factory_builds_one_current_guard_and_warm_turns_do_not_reload_unchanged_grants(
    tmp_path, monkeypatch
):
    with _turn_case(tmp_path) as case:
        _drained(case)
        calls = []
        make = case.service._terminal_write_guard

        def create(*args):
            guard = make(*args)
            calls.append(guard)
            return guard

        monkeypatch.setattr(case.service, "_terminal_write_guard", create)
        turn = _turn(case)
        assert len(calls) == 1
        assert _advance(case, turn).originals_retired
        queries = []
        engine = case.factory.kw["bind"]

        def capture(*args):
            queries.append(args[2])
            assert "FROM app_key_access_grants" not in args[2]

        event.listen(engine, "before_cursor_execute", capture)
        try:
            for _ in range(3):
                result = _advance(case, turn)
                assert result.phase == "cleanup-ready" and result.originals_retired
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        assert len(calls) == 1 and any("FROM app_keys" in q for q in queries)


def _revoked_between_stages(case, monkeypatch):
    _drained(case)
    turn = _turn(case)
    owner = case.service._archive_terminal_retirement
    retire = owner.retire

    def revoked(*args, **kwargs):
        case.keys.replace_access(
            app=case.principal.id,
            key_id=case.principal.key_id,
            access=(ApplicationAccess(COLLECTIONS_CREATE),),
            grantor=BOOTSTRAP,
        )
        return retire(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(owner, "retire", revoked)
        with pytest.raises(NotFound):
            turn.advance(limit=1)
    assert _state(case, case.operation)[0] == "cleanup-ready"
    assert _retained(case) == 1
    case.keys.replace_access(
        app=case.principal.id,
        key_id=case.principal.key_id,
        access=(ApplicationAccess(COLLECTIONS_DELETE),),
        grantor=BOOTSTRAP,
    )
    result = _advance(case, turn)
    assert result.originals_retired and result.retired_originals == 1


def test_current_authority_change_between_committed_decision_and_retirement_pauses_release(
    tmp_path, monkeypatch
):
    with _turn_case(tmp_path) as case:
        _revoked_between_stages(case, monkeypatch)


def _lost_decision_reply(case):
    _drained(case)
    turn = _turn(case)

    def lost(session):
        raise RuntimeError("terminal decision commit reply lost")

    event.listen(case.factory, "after_commit", lost, once=True)
    with pytest.raises(InvalidRequestError, match="committed") as caught:
        turn.advance(limit=1)
    assert isinstance(caught.value.__context__, RuntimeError)
    assert _state(case, case.operation)[0] == "cleanup-ready" and _retained(case) == 1
    original = _state(case, case.operation)[2]
    result = _advance(case, turn)
    assert result.retired_originals == 1 and result.originals_retired
    assert _state(case, case.operation)[2:] == (original, None)


def test_lost_global_decision_reply_never_speculates_original_release(tmp_path):
    with _turn_case(tmp_path) as case:
        _lost_decision_reply(case)


def test_reconfirmed_discard_preserves_original_result_and_requires_a_fresh_operation(tmp_path):
    with _turn_case(tmp_path) as case:
        _drained(case)
        turn = _turn(case)
        original = _state(case, case.operation)[2]
        with session_scope(case.factory) as session:
            session.get(Control, case.collection_id).custody_revision = "1"
        result = _advance(case, turn)
        assert result.phase == "needs-reconfirmation" and result.originals_retired
        assert _state(case, case.operation)[2:] == (original, None)
        assert _advance(case, turn).phase == "needs-reconfirmation"
        with session_scope(case.factory) as session:
            assert session.get(Upload, case.collection_id).state == "orphaned"


def test_late_sealed_cancel_remains_refused_after_original_retirement(tmp_path):
    with _tagged(tmp_path, count=3) as case:
        registry = _failed_forward(case, "metadata")
        _, principal, _ = _creator_key(case)
        case.operation = _select_cancel(
            case, principal, case.service._cancel_write_guard(case.collection_id, principal)
        )
        _drain(case, registry, case.operation)
        turn = _turn(case)
        with session_scope(case.factory) as session:
            session.get(Volume, (case.collection_id, case.object_id)).state = "sealed"
        result = _advance(case, turn)
        assert result.phase == "refused-sealed" and result.originals_retired
        assert _retained(case) == 0 and _advance(case, turn).phase == "refused-sealed"
        with session_scope(case.factory) as session:
            assert session.get(Volume, (case.collection_id, case.object_id)).state == "sealed"


def _execution_scope(case, monkeypatch):
    _drained(case)
    turn = _turn(case)
    service = case.service
    owner = object()
    reservation = service._archive_retention.try_retain(
        turn.key, target_incarnation=turn.target_incarnation, owner=owner, terminal=True
    )
    assert not isinstance(reservation, str)
    actual = physical._run
    published, release = threading.Event(), threading.Event()

    def held(work, future):
        actual(work, future)
        published.set()
        assert release.wait(timeout=10)

    monkeypatch.setattr(physical, "_run", held)
    try:
        assert (
            service._archive_execution.try_submit(
                turn.key,
                target_incarnation=turn.target_incarnation,
                terminal=True,
                work=lambda: turn.advance(limit=1),
                owner=owner,
                identity=turn,
            )
            == "submitted"
        )
        assert published.wait(timeout=10)
        assert _retained(case) == 1 and service._archive_retention._counts == {False: 0, True: 1}
        assert service._archive_execution.collect(limit=1, owner=owner) == ()
        assert service._archive_retention._reservations[id(reservation)] is reservation
    finally:
        release.set()
    for slot in tuple(service._archive_execution._slots.values()):
        slot.worker.join(timeout=10)
        assert not slot.worker.is_alive()
    (received,) = service._archive_execution.collect(limit=1, owner=owner)
    result = received.result()
    assert result.originals_retired and result.retired_originals == 1
    assert received.key == turn.key and received.identity is turn
    assert service._archive_retention.retire(reservation, owner=owner)
    assert _retained(case) == 0


def test_actual_operation_worker_does_not_mistake_itself_for_original_construction_work(
    tmp_path, monkeypatch
):
    with _turn_case(tmp_path) as case:
        _execution_scope(case, monkeypatch)


def test_actor_contention_is_nonblocking_and_does_not_query_current_scope(tmp_path):
    with _turn_case(tmp_path) as case:
        turn = _turn(case)
        queries = []
        engine = case.factory.kw["bind"]

        def capture(*args):
            queries.append(args[2])

        event.listen(engine, "before_cursor_execute", capture)
        try:
            with turn._lock:
                result = turn.advance(limit=1)
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        assert not queries and result.phase is None and result.pending_reason == "actor-busy"


def test_busy_catalog_defers_the_original_execution_without_releasing_ownership(tmp_path):
    with _turn_case(tmp_path) as case:
        _drained(case)
        turn = _turn(case)
        with ThreadPoolExecutor(max_workers=1) as workers:
            with session_scope(case.factory) as session:
                _writer(session)
                result = workers.submit(turn.advance, limit=1).result(timeout=5)
        assert result.pending_reason == "catalog-busy" and not result.originals_retired
        assert _retained(case) == 1
        assert _advance(case, turn).originals_retired


@pytest.mark.parametrize("limit", (0, -1, True, 1.5))
def test_terminal_execution_requires_a_positive_bounded_retirement_turn(tmp_path, limit):
    with _turn_case(tmp_path) as case:
        turn = _turn(case)
        with pytest.raises(ValueError, match="positive"):
            turn.advance(limit=limit)
        assert _retained(case) == 1
