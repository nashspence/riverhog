from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from riverhog_core.catalog_archive_write_models import (
    ArchiveUploadTerminalRecord as Terminal,
)
from riverhog_core.catalog_archive_write_models import (
    ArchiveWritePreparationRecord as Preparation,
)
from riverhog_core.catalog_db import session_scope
from riverhog_core.services import physical_execution as execution_module
from riverhog_core.services.archive_write_admission import _writer
from riverhog_core.services.physical_execution import PhysicalExecutionKey
from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy import event, select
from sqlalchemy.exc import InvalidRequestError

from tests.unit.test_archive_finalizer_registry import _scope
from tests.unit.test_archive_finalizer_terminal_drain import _original, _terminal
from tests.unit.test_archive_payload_input import _body, _request
from tests.unit.test_archive_provenance_journal_writer import _lose_ack
from tests.unit.test_archive_upload_terminal_adjudication import _decision, _result
from tests.unit.test_archive_upload_terminal_result import _state
from tests.unit.test_archive_write_admission import _case, _guard, _prepare, _selected


class _Staged:
    has_pending = True

    def drain_terminal(self, operation_id):
        return "closed"


def _join(case, registry):
    for slot in tuple(case.service._archive_execution._slots.values()):
        slot.worker.join(timeout=10)
        assert not slot.worker.is_alive()
    return registry.collect(limit=100)


def _install(case, kind, number=1, *, create=_Staged, advance=None):
    registry = (
        case.service._archive_finalizers
        if kind == "metadata"
        else case.service._archive_invocations
    )
    key, target = _scope(case)
    args = dict(target_incarnation=target, create=create, advance=advance or (lambda owner: True))
    if kind == "metadata":
        status = registry.submit(key, selection="original", **args)
    else:
        status = registry.submit(key, request_sha256=f"{number:064x}", **args)
    assert status == "submitted"
    return registry


def _drain(case, registry, operation):
    key, target = _scope(case)
    assert (
        registry.submit_terminal(key, target_incarnation=target, operation_id=operation)
        == "submitted"
    )
    (received,) = _join(case, registry)
    assert received.result() == "closed"


def _retire(case, operation, guard=_guard, limit=1):
    return case.service._archive_terminal_retirement.retire(
        case.collection_id, operation, guard=guard, limit=limit
    )


def _retained(case):
    return len(case.service._archive_retention._reservations)


def _prepared(case):
    with session_scope(case.factory) as session:
        return [
            tuple(getattr(row, c.name) for c in Preparation.__table__.columns)
            for row in session.scalars(select(Preparation))
        ]


def _failed_forward(case, kind):
    def failed(owner):
        raise OSError("lost original forward acknowledgement")

    registry = _install(case, kind, advance=failed)
    with pytest.raises(OSError, match="original forward"):
        _join(case, registry)[0].result()
    return registry


def _ready_original(case, kind):
    registry = _failed_forward(case, kind)
    operation = _selected(case)
    _result(case, operation)
    _drain(case, registry, operation)
    assert _decision(case, operation).phase == "cleanup-ready"
    return registry, operation


@pytest.mark.parametrize("kind", ("metadata", "payload"))
def test_retirement_requires_committed_global_decision_and_preserves_durable_preparations(
    tmp_path, kind
):
    with _case(tmp_path) as case:
        _prepare(case)
        registry = _failed_forward(case, kind)
        operation = _selected(case)
        _result(case, operation)
        _drain(case, registry, operation)
        prepared, terminal = _prepared(case), _state(case, operation)
        assert prepared and _retained(case) == 1
        assert _retire(case, operation).pending_reason == "global-decision"
        assert _retained(case) == 1
        assert _decision(case, operation).phase == "cleanup-ready"
        result = _retire(case, operation)
        assert result.retired == 1 and result.pending_reason is None
        assert _retained(case) == 0 and _prepared(case) == prepared
        assert _state(case, operation) == ("cleanup-ready", *terminal[1:])
        assert _retire(case, operation).retired == 0


@pytest.mark.parametrize("kind", ("metadata", "payload"))
def test_current_authority_failure_precedes_retirement_probes(tmp_path, monkeypatch, kind):
    with _case(tmp_path) as case:
        _, operation = _ready_original(case, kind)

        def revoked(session):
            raise NotFound("current terminal authority revoked")

        def forbidden(*args, **kwargs):
            pytest.fail("revoked authority reached local retirement")

        monkeypatch.setattr(case.service._archive_finalizers, "local_terminal_status", forbidden)
        with pytest.raises(NotFound, match="revoked"):
            _retire(case, operation, revoked)
        assert _retained(case) == 1


@pytest.mark.parametrize("kind", ("metadata", "payload"))
def test_uncertain_constructor_never_acquires_retirement_from_a_phase_flag(tmp_path, kind):
    with _case(tmp_path) as case:

        def unknown():
            raise OSError("unknown original constructor")

        registry = _install(case, kind, create=unknown)
        with pytest.raises(OSError, match="unknown original"):
            _join(case, registry)[0].result()
        operation = _selected(case)
        _result(case, operation)
        key, target = _scope(case)
        assert (
            registry.submit_terminal(key, target_incarnation=target, operation_id=operation)
            == "submitted"
        )
        assert _join(case, registry)[0].result() == "pending"
        with session_scope(case.factory) as session:
            session.get(Terminal, operation).phase = "cleanup-ready"
        assert _retire(case, operation).pending_reason == "local-writers"
        assert _retained(case) == 1


@pytest.mark.parametrize("kind", ("metadata", "payload"))
def test_semantic_closure_waits_for_actual_worker_exit_and_receiving(tmp_path, monkeypatch, kind):
    with _case(tmp_path) as case:
        registry = _install(case, kind)
        assert _join(case, registry)[0].result()
        operation = _selected(case)
        _result(case, operation)
        published, release = threading.Event(), threading.Event()
        original = execution_module._run

        def held(work, future):
            original(work, future)
            published.set()
            assert release.wait(timeout=10)

        monkeypatch.setattr(execution_module, "_run", held)
        key, target = _scope(case)
        try:
            assert (
                registry.submit_terminal(key, target_incarnation=target, operation_id=operation)
                == "submitted"
            )
            assert published.wait(timeout=10)
            # A phase alone cannot substitute for actual original receiving.
            with session_scope(case.factory) as session:
                session.get(Terminal, operation).phase = "cleanup-ready"
            assert _retire(case, operation).pending_reason == "local-writers"
            assert not registry.collect(limit=1) and _retained(case) == 1
        finally:
            release.set()
        assert _join(case, registry)[0].result() == "closed"
        assert _retire(case, operation).retired == 1


def _bounded_retirement(case):
    metadata = _install(case, "metadata")
    _join(case, metadata)[0].result()
    for number in range(1, 4):
        payloads = _install(case, "payload", number)
        _join(case, payloads)[0].result()
    operation = _selected(case)
    _result(case, operation)
    _drain(case, metadata, operation)
    for _ in range(3):
        _drain(case, payloads, operation)
    assert _decision(case, operation).phase == "cleanup-ready"
    assert _retained(case) == 4
    from collections import deque

    class SelectedRequests(deque):
        def remove(self, request):
            pytest.fail("terminal retirement rescanned the original request group")

    group = next(iter(payloads._constructions.values()))
    group.requests = SelectedRequests(group.requests)
    for remaining in range(3, -1, -1):
        result = _retire(case, operation, limit=1)
        assert result.retired == 1 and _retained(case) == remaining
        assert (result.pending_reason is None) is (remaining == 0)
    return operation


def test_retirement_shares_one_bounded_allowance_across_original_work_kinds(tmp_path):
    with _case(tmp_path) as case:
        statements = []
        engine = case.factory.kw["bind"]

        def observe(*args):
            statements.append(args[2])

        event.listen(engine, "before_cursor_execute", observe)
        try:
            _bounded_retirement(case)
        finally:
            event.remove(engine, "before_cursor_execute", observe)
        selected = [s.upper() for s in statements if s.lstrip().upper().startswith("SELECT")]
        assert not any("COUNT(" in s or "SUM(" in s for s in selected)
        effects = [s for s in selected if "from archive_write_effects" in s.lower()]
        assert effects and all("LIMIT" in s for s in effects)


def test_original_retirement_does_not_wait_on_the_terminal_operations_own_worker(tmp_path):
    with _case(tmp_path) as case:
        _, operation = _ready_original(case, "metadata")
        construction, target = _scope(case)
        pool = case.service._archive_execution
        owner, release, entered = object(), threading.Event(), threading.Event()

        def terminal_turn():
            entered.set()
            assert release.wait(timeout=10)

        try:
            assert (
                pool.try_submit(
                    PhysicalExecutionKey(case.collection_id, operation),
                    target_incarnation=target,
                    terminal=True,
                    work=terminal_turn,
                    owner=owner,
                )
                == "submitted"
            )
            assert entered.wait(timeout=10)
            assert pool.status(construction) == "untracked"
            assert _retire(case, operation).retired == 1
        finally:
            release.set()
            for slot in tuple(pool._slots.values()):
                slot.worker.join(timeout=10)
            assert len(pool.collect(limit=1, owner=owner)) == 1


@pytest.mark.parametrize("phase", ("refused-sealed", "needs-reconfirmation", "cleaning"))
def test_stable_native_dispositions_release_only_original_local_resources(tmp_path, phase):
    from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord as Control
    from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord as Volume
    from riverhog_core.catalog_models import CollectionUploadRecord as Upload

    with _case(tmp_path) as case:
        registry = _failed_forward(case, "metadata")
        if phase == "needs-reconfirmation":
            with session_scope(case.factory) as session:
                session.get(Upload, case.collection_id).state = "orphaned"
        operation = _selected(case, kind="discard" if phase == "needs-reconfirmation" else "cancel")
        _result(case, operation)
        _drain(case, registry, operation)
        with session_scope(case.factory) as session:
            if phase == "refused-sealed":
                session.get(Volume, (case.collection_id, case.object_id)).state = "sealed"
            elif phase == "needs-reconfirmation":
                session.get(Control, case.collection_id).custody_revision = "1"
        assert _decision(case, operation).phase == (
            "cleanup-ready" if phase == "cleaning" else phase
        )
        if phase == "cleaning":
            with session_scope(case.factory) as session:
                session.get(Terminal, operation).phase = phase
        before = _state(case, operation)
        assert _retire(case, operation).retired == 1
        assert _state(case, operation) == before


def _unstarted_input(case, monkeypatch, *, unknown=False):
    registry = case.service._archive_invocations
    key, target = _scope(case)
    payload = _body(
        case.service._archive_inputs,
        _request(
            key=key,
            target_incarnation=target,
            epoch=case.epoch,
            volume_id=case.object_id,
            plan_sha256=case.identity["plan_sha256"],
        ),
    )
    entered, release = threading.Event(), threading.Event()
    original = execution_module._run
    calls = []

    def held(work, future):
        entered.set()
        assert release.wait(timeout=10)
        original(work, future)

    def create():
        calls.append("constructor")
        if unknown:
            raise OSError("unknown constructor outcome")
        pytest.fail("fenced original constructor must not enter")

    monkeypatch.setattr(execution_module, "_run", held)
    try:
        assert (
            registry.submit(
                key,
                target_incarnation=target,
                request_sha256=payload.request_sha256,
                payload=payload,
                create=create,
                advance=lambda owner: None,
            )
            == "submitted"
        )
        assert entered.wait(timeout=10)
        if unknown:
            release.set()
            with pytest.raises(OSError, match="unknown constructor"):
                _join(case, registry)[0].result()
        operation = _selected(case)
        _result(case, operation)
        registry.fence_terminal(key, target_incarnation=target, operation_id=operation)
    finally:
        release.set()
    if not unknown:
        with pytest.raises(Conflict, match="cannot start forward"):
            _join(case, registry)[0].result()
    assert not payload.released and _retained(case) == 1
    assert (
        registry.submit_terminal(key, target_incarnation=target, operation_id=operation)
        == "submitted"
    )
    # Semantic drainage cannot withdraw the body before its actual worker joins.
    for slot in tuple(case.service._archive_execution._slots.values()):
        slot.worker.join(timeout=10)
        assert not slot.worker.is_alive()
    assert not payload.released
    (received,) = registry.collect(limit=1)
    assert received.result() == ("pending" if unknown else "closed")
    assert payload.released is not unknown
    if unknown:
        assert calls == ["constructor"]
        assert _decision(case, operation).pending_reason == "local-writers"
        assert _retire(case, operation).retired == 0
    else:
        assert not calls and case.service._archive_inputs.status(key) == "untracked"
        assert _decision(case, operation).phase == "cleanup-ready"
        assert _retire(case, operation).retired == 1
    assert _retained(case) == int(unknown)


@pytest.mark.parametrize("unknown", (False, True))
def test_only_positive_unstarted_drain_and_actual_receiving_withdraw_original_input(
    tmp_path, monkeypatch, unknown
):
    with _case(tmp_path) as case:
        _unstarted_input(case, monkeypatch, unknown=unknown)


@pytest.mark.parametrize("kind", ("root", "description"))
@pytest.mark.parametrize("mode", ("direct", "http"))
def test_real_metadata_original_retires_staging_after_its_receiving_not_forward_success(
    tmp_path, monkeypatch, kind, mode
):
    with _original(tmp_path, monkeypatch, kind, mode) as original:
        case = original.case
        with _lose_ack(monkeypatch, "report", True):
            registry = _install(
                case,
                "metadata",
                create=lambda: original.owner,
                advance=lambda owner: original.advance(),
            )
            with pytest.raises(OSError, match="injected journal writer"):
                _join(case, registry)[0].result()
        operation = _terminal(original)
        _result(case, operation)
        _drain(case, registry, operation)
        assert original.owner.has_pending
        prepared = _prepared(case)
        decision = _decision(case, operation)
        assert decision.pending_reason is None
        assert decision.phase in {"cleanup-ready", "refused-sealed"}
        before = _state(case, operation)
        assert _retire(case, operation).retired == 1
        assert _retained(case) == 0 and _prepared(case) == prepared
        assert _state(case, operation) == before


def test_catalog_contention_defers_without_releasing_original(tmp_path):
    with _case(tmp_path) as case:
        _, operation = _ready_original(case, "metadata")
        with ThreadPoolExecutor(max_workers=1) as workers:
            with session_scope(case.factory) as session:
                _writer(session)
                result = workers.submit(_retire, case, operation).result(timeout=5)
        assert result.phase is None and result.pending_reason == "catalog-busy"
        assert _retained(case) == 1
        assert _retire(case, operation).retired == 1


def test_lost_retirement_reply_retries_without_releasing_another_original(tmp_path):
    with _case(tmp_path) as case:
        _, operation = _ready_original(case, "metadata")

        def lost(session):
            raise RuntimeError("original retirement commit reply lost")

        event.listen(case.factory, "after_commit", lost, once=True)
        with pytest.raises(InvalidRequestError, match="committed") as caught:
            _retire(case, operation)
        assert isinstance(caught.value.__context__, RuntimeError)
        assert _retained(case) == 0
        assert _retire(case, operation).retired == 0
        assert _state(case, operation)[0] == "cleanup-ready"


@pytest.mark.parametrize("value", (0, -1, True, 1.5))
def test_retirement_requires_a_positive_bounded_turn(tmp_path, value):
    with _case(tmp_path) as case:
        with pytest.raises(ValueError, match="positive"):
            _retire(case, "f" * 64, limit=value)
        with pytest.raises(ValueError, match="current authority"):
            _retire(case, "f" * 64, guard=None)
