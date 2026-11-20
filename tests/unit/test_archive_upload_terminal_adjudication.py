from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest
from riverhog_core.catalog_archive_write_models import (
    ArchiveUploadTerminalRecord as Terminal,
)
from riverhog_core.catalog_archive_write_models import (
    ArchiveWriteControlRecord as Control,
)
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import (
    CollectionArchiveObjectUploadRecord as Volume,
)
from riverhog_core.catalog_models import (
    CollectionUploadRecord as Upload,
)
from riverhog_core.services.archive_payload_input import ArchivePayloadInput
from riverhog_core.services.archive_upload_terminal_result import ArchiveUploadTerminalResults
from riverhog_core.services.archive_write_admission import _writer
from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy import event

from tests.unit.test_archive_finalizer_registry import Finalizer, _receive, _scope
from tests.unit.test_archive_payload_input import _request
from tests.unit.test_archive_upload_terminal_result import _state
from tests.unit.test_archive_write_admission import (
    _apply,
    _arm,
    _case,
    _closure,
    _guard,
    _prepare,
    _selected,
)


def _result(case, operation):
    return ArchiveUploadTerminalResults(case.factory).prepare(
        case.collection_id,
        operation,
        {"state": "canceled", "collection_id": str(case.collection_id), "files": 1},
        guard=_guard,
    )


def _decision(case, operation, guard=_guard):
    return case.service._archive_terminal_adjudication.adjudicate(
        case.collection_id, operation, guard=guard
    )


def _phase(case, operation):
    with session_scope(case.factory) as session:
        upload = session.get(Upload, case.collection_id)
        terminal = session.get(Terminal, operation)
        return terminal.phase, upload.state, upload.lease_expires_at, terminal.result_json


def _original_receiving(case):
    _prepare(case)
    ticket = _arm(case)
    operation = _selected(case)
    _result(case, operation)
    first = _decision(case, operation)
    assert first.phase == "draining" and first.pending_reason == "forward-effects"
    case.owner.settle_closed(ticket, _closure(case, ticket), apply_result=_apply)
    received = _decision(case, operation)
    assert received.phase == "draining" and received.pending_reason == "original-receiving"
    case.owner.acknowledge_report(ticket)
    selected = _decision(case, operation)
    assert selected.phase == "cleanup-ready" and selected.pending_reason is None
    assert _phase(case, operation)[:2] == ("cleanup-ready", "discarding")
    assert _phase(case, operation)[3] is None


def test_global_decision_keeps_unknown_and_unreceived_originals_separate(tmp_path):
    with _case(tmp_path) as case:
        _original_receiving(case)


def test_missing_result_reservation_does_not_mark_a_drained_terminal_ready(tmp_path):
    with _case(tmp_path) as case:
        operation = _selected(case)
        before = _phase(case, operation)
        result = _decision(case, operation)
        assert result.phase == "draining" and result.pending_reason == "original-result"
        assert _phase(case, operation) == before
        encoded = _result(case, operation)
        assert _decision(case, operation).phase == "cleanup-ready"
        assert _state(case, operation)[2:] == (encoded, None)
        assert _phase(case, operation)[2] == before[2]


def test_retained_input_prevents_global_adjudication_until_original_withdrawal(tmp_path):
    with _case(tmp_path) as case:
        key, target = _scope(case)
        body = case.service._archive_inputs.reserve(
            _request(
                key=key,
                target_incarnation=target,
                epoch=case.epoch,
                volume_id=case.object_id,
                plan_sha256=case.identity["plan_sha256"],
            )
        )
        assert isinstance(body, ArchivePayloadInput)
        operation = _selected(case)
        _result(case, operation)
        decision = _decision(case, operation)
        assert decision.pending_reason == "local-input"
        body.discard_unretained()
        assert _decision(case, operation).phase == "cleanup-ready"


def test_known_finalizer_requires_its_original_terminal_drain_and_actual_receiving(tmp_path):
    with _case(tmp_path) as case:
        registry = case.service._archive_finalizers
        pool = case.service._archive_execution
        key, target = _scope(case)
        assert (
            registry.submit(
                key,
                target_incarnation=target,
                selection="original",
                create=Finalizer,
                advance=lambda actor: actor.advance(),
            )
            == "submitted"
        )
        assert _receive(registry, pool)[0].result().progressed
        operation = _selected(case)
        _result(case, operation)
        assert _decision(case, operation).pending_reason == "local-writers"
        assert (
            registry.fence_terminal(key, target_incarnation=target, operation_id=operation)
            == "fenced"
        )
        assert _decision(case, operation).pending_reason == "local-writers"
        assert (
            registry.submit_terminal(key, target_incarnation=target, operation_id=operation)
            == "submitted"
        )
        for slot in tuple(pool._slots.values()):
            slot.worker.join(timeout=10)
            assert not slot.worker.is_alive()
        assert _decision(case, operation).pending_reason == "local-writers"
        assert _receive(registry, pool)[0].result() == "closed"
        assert _decision(case, operation).phase == "cleanup-ready"
        # Readiness does not retire the original actor or dispose its preparation.
        assert not registry.retire_idle(key, target_incarnation=target)


def test_positive_late_seal_refuses_cancel_without_erasing_original_state(tmp_path):
    with _case(tmp_path) as case:
        operation = _selected(case)
        _result(case, operation)
        with session_scope(case.factory) as session:
            session.get(Volume, (case.collection_id, case.object_id)).state = "sealed"
        before = _phase(case, operation)
        assert _decision(case, operation).phase == "refused-sealed"
        assert _phase(case, operation)[1:] == before[1:]
        assert _decision(case, operation).phase == "refused-sealed"
        with session_scope(case.factory) as session:
            assert session.get(Volume, (case.collection_id, case.object_id)).state == "sealed"
            assert session.get(Control, case.collection_id).gate == "closed"


@pytest.mark.parametrize("changed", (False, True))
def test_discard_compares_original_custody_before_any_disposal(tmp_path, changed):
    with _case(tmp_path) as case:
        with session_scope(case.factory) as session:
            session.get(Upload, case.collection_id).state = "orphaned"
        operation = _selected(case, kind="discard")
        _result(case, operation)
        if changed:
            with session_scope(case.factory) as session:
                session.get(Control, case.collection_id).custody_revision = "1"
        expected = "needs-reconfirmation" if changed else "cleanup-ready"
        assert _decision(case, operation).phase == expected
        assert _phase(case, operation)[1] == ("orphaned" if changed else "discarding")
        assert _decision(case, operation).phase == expected


def test_nonorphan_discard_cannot_acquire_cleanup_ready(tmp_path):
    with _case(tmp_path) as case:
        operation = _selected(case, kind="discard")
        _result(case, operation)
        before = _phase(case, operation)
        with pytest.raises(Conflict, match="original orphan"):
            _decision(case, operation)
        assert _phase(case, operation) == before


def test_current_authority_failure_precedes_any_global_resource_probe(tmp_path, monkeypatch):
    with _case(tmp_path) as case:
        operation = _selected(case)
        _result(case, operation)

        def revoked(session):
            raise NotFound("current terminal authority is unavailable")

        def forbidden(*args, **kwargs):
            pytest.fail("revoked authority reached a local resource probe")

        monkeypatch.setattr(case.service._archive_finalizers, "local_terminal_status", forbidden)
        with pytest.raises(NotFound):
            _decision(case, operation, revoked)
        assert _phase(case, operation)[:2] == ("draining", "open")


def test_catalog_contention_defers_without_a_cleanup_decision(tmp_path):
    with _case(tmp_path) as case:
        operation = _selected(case)
        _result(case, operation)
        with ThreadPoolExecutor(max_workers=1) as workers:
            with session_scope(case.factory) as session:
                _writer(session)
                deferred = workers.submit(_decision, case, operation).result(timeout=5)
        assert deferred.phase is None and deferred.pending_reason == "catalog-busy"
        assert _decision(case, operation).phase == "cleanup-ready"


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("finalizers", "busy", "local-writers"),
        ("invocations", "fenced", "local-writers"),
        ("inputs", "busy", "local-input"),
        ("execution", "running", "local-execution"),
        ("execution", "awaiting-join", "local-execution"),
        ("execution", "outcome-missing", "local-execution"),
    ],
)
def test_no_uncertain_local_status_is_reinterpreted_as_quiescence(
    tmp_path, monkeypatch, field, value, reason
):
    with _case(tmp_path) as case:
        operation = _selected(case)
        _result(case, operation)
        observer = case.service._archive_terminal_observation
        original = observer.observe

        def observe(*args, **kwargs):
            return replace(original(*args, **kwargs), **{field: value})

        monkeypatch.setattr(observer, "observe", observe)
        result = _decision(case, operation)
        assert result.phase == "draining" and result.pending_reason == reason


def test_adjudication_uses_only_indexed_bounded_facts_and_never_confirms_a_result(tmp_path):
    with _case(tmp_path) as case:
        operation = _selected(case)
        _result(case, operation)
        queries = []

        def capture(connection, cursor, statement, parameters, context, executemany):
            queries.append(statement)
            assert "COUNT(" not in statement.upper() and "SUM(" not in statement.upper()
            if "FROM archive_write_effects" in statement or "FROM cache_population_" in statement:
                assert "LIMIT" in statement.upper()

        engine = case.factory.kw["bind"]
        event.listen(engine, "before_cursor_execute", capture)
        try:
            assert _decision(case, operation).phase == "cleanup-ready"
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        assert queries
        assert _phase(case, operation)[3] is None


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_optional_cache_unknown_and_original_receiving_block_archive_adjudication(tmp_path, mode):
    from tests.unit.test_cache_population_begin import _arm as arm
    from tests.unit.test_cache_population_begin import _case as cache_case
    from tests.unit.test_cache_population_begin import _terminal

    with cache_case(tmp_path, mode=mode) as case:
        ticket = arm(case)
        operation = _terminal(case)
        _result(case, operation)
        assert _decision(case, operation).pending_reason == "forward-effects"
        response = case.owner.execute(ticket, candidate=case.candidate)
        case.owner.record_response(ticket, response.model_copy(update={"execution": "unknown"}))
        assert _decision(case, operation).pending_reason == "forward-effects"
        case.owner.record_response(ticket, response)
        assert _decision(case, operation).pending_reason == "original-receiving"
        assert _phase(case, operation)[:2] == ("draining", "open")


def test_positive_readiness_preserves_hidden_registration_for_its_later_disposal_stage(tmp_path):
    from tests.unit.test_collection_terminal_registration import (
        _case as registration_case,
    )
    from tests.unit.test_collection_terminal_registration import (
        _select,
        _snapshot,
    )

    with registration_case(tmp_path, "staging") as values:
        service, principal, cid, registration, registration_id, member = values
        operation = _select(service, principal, cid)
        guard = service._terminal_write_guard(cid, operation)
        ArchiveUploadTerminalResults(service._session_factory).prepare(
            cid, operation, {"state": "canceled", "collection_id": str(cid)}, guard=guard
        )
        before = _snapshot(service, cid, registration_id)
        assert (
            service._archive_terminal_adjudication.adjudicate(cid, operation, guard=guard).phase
            == "cleanup-ready"
        )
        after = _snapshot(service, cid, registration_id)
        assert after["operation"] == before["operation"]
        assert after["rows"] == before["rows"]
        assert after["upload"]["lease_expires_at"] == before["upload"]["lease_expires_at"]
        assert after["upload"]["active_registration_id"] == registration_id
        assert after["operation"]["disposition_json"] is None


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("mode", ("direct", "owned-http"))
def test_native_seal_before_logical_receipt_is_refused_after_original_drain(
    tmp_path, monkeypatch, kind, mode
):
    from tests.unit.test_archive_payload_unit import _select_terminal, _service_actor, _stored
    from tests.unit.test_archive_upload_invocations import _receive as receive

    with _service_actor(tmp_path, monkeypatch, kind, mode) as f:

        def lost_receipt(*args, **kwargs):
            raise OSError("original logical receipt acknowledgement missing")

        with monkeypatch.context() as patch:
            patch.setattr(f.service, "_record_sealed_volume", lost_receipt)
            assert f.submit() == "submitted"
            (forward,) = receive(f.registry, f.execution)
            with pytest.raises(OSError, match="logical receipt acknowledgement"):
                forward.result()
        assert _stored(f)[0] is not None and _stored(f)[1] is None
        operation = _select_terminal(f)
        pending = _decision(f.case, operation)
        assert pending.phase == "draining" and pending.pending_reason is not None
        calls = list(f.seen)
        assert (
            f.registry.submit_terminal(f.key, target_incarnation=f.target, operation_id=operation)
            == "submitted"
        )
        (drained,) = receive(f.registry, f.execution)
        assert drained.result() == "closed"
        assert _decision(f.case, operation).phase == "refused-sealed"
        assert f.seen == calls and _stored(f)[1] is not None
