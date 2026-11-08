from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest
from riverhog_core.catalog_db import catalog_write_contention, session_scope
from riverhog_core.services.archive_payload_input import ArchivePayloadInput
from riverhog_core.services.archive_write_admission import _writer
from riverhog_protocol.errors import Conflict
from sqlalchemy.exc import OperationalError

from tests.unit.test_archive_finalizer_registry import _receive, _scope
from tests.unit.test_archive_payload_input import _request
from tests.unit.test_archive_upload_invocations import Original
from tests.unit.test_archive_write_admission import (
    _apply,
    _arm,
    _case,
    _closure,
    _prepare,
    _selected,
)


def _observe(case, operation):
    with session_scope(case.factory) as session:
        return case.service._archive_terminal_observation.observe(
            session, case.collection_id, operation
        )


def test_original_selection_is_checked_before_local_resource_observation(tmp_path, monkeypatch):
    with _case(tmp_path) as case:
        operation = _selected(case)

        def forbidden(*args, **kwargs):
            pytest.fail("unselected request reached a local resource owner")

        monkeypatch.setattr(case.service._archive_finalizers, "local_terminal_status", forbidden)
        with pytest.raises(Conflict, match="selection is not current"):
            _observe(case, "f" * 64 if operation != "f" * 64 else "e" * 64)


def test_incoming_body_stays_visible_until_its_original_unused_input_is_withdrawn(tmp_path):
    with _case(tmp_path) as case:
        key, target = _scope(case)
        with session_scope(case.factory) as session:
            _writer(session)
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
        before = _observe(case, operation)
        assert before.inputs == "retained" and before.execution == "untracked"
        assert before.finalizers == before.invocations == "untracked"
        body.discard_unretained()
        after = _observe(case, operation)
        assert after.inputs == "untracked" and after.records == before.records


def test_durable_unknown_receiving_and_retirement_remain_distinct_from_local_absence(tmp_path):
    with _case(tmp_path) as case:
        _prepare(case)
        ticket = _arm(case)
        operation = _selected(case)
        unknown = _observe(case, operation)
        assert unknown.records.archive_unclosed and unknown.records.archive_retained
        assert unknown.finalizers == unknown.invocations == unknown.inputs == "untracked"
        assert unknown.execution == "untracked"
        case.owner.settle_closed(ticket, _closure(case, ticket), apply_result=_apply)
        received = _observe(case, operation)
        assert not received.records.archive_unclosed and received.records.archive_retained
        case.owner.acknowledge_report(ticket)
        retired = _observe(case, operation)
        assert not retired.records.archive_unclosed and not retired.records.archive_retained
        assert retired.records.operation_id == operation


def test_observation_preserves_the_original_fence_until_semantic_drain_and_join(tmp_path):
    with _case(tmp_path) as case:
        service = case.service
        assert (
            service._archive_finalizer_work._submit(
                case.collection_id, "fixture", Original, lambda owner: True, lambda result: None
            )
            == "submitted"
        )
        (completed,) = _receive(service._archive_finalizers, service._archive_execution)
        assert completed.result().progressed
        operation = _selected(case)
        assert _observe(case, operation).finalizers == "unfenced"
        key, target = _scope(case)
        assert (
            service._archive_finalizers.fence_terminal(
                key, target_incarnation=target, operation_id=operation
            )
            == "fenced"
        )
        assert _observe(case, operation).finalizers == "fenced"
        assert (
            service._archive_finalizers.submit_terminal(
                key, target_incarnation=target, operation_id=operation
            )
            == "submitted"
        )
        (closed,) = _receive(service._archive_finalizers, service._archive_execution)
        assert closed.result() == "closed"
        observed = _observe(case, operation)
        assert observed.finalizers == "drained" and observed.execution == "untracked"


def test_catalog_contention_defers_observation_before_its_resource_queries(tmp_path):
    with _case(tmp_path) as case:
        operation = _selected(case)
        with ThreadPoolExecutor(max_workers=1) as workers:
            with session_scope(case.factory) as session:
                _writer(session)
                attempt = workers.submit(_observe, case, operation)
                with pytest.raises(OperationalError) as failed:
                    attempt.result(timeout=5)
                assert catalog_write_contention(failed.value)
        assert _observe(case, operation).records.operation_id == operation


def test_local_contention_is_reported_without_nested_catalog_work_or_fencing(tmp_path):
    with _case(tmp_path) as case:
        operation = _selected(case)
        with case.service._archive_finalizers._lock:
            observed = _observe(case, operation)
        assert observed.finalizers == "busy"
        assert observed.records.operation_id == operation
        assert observed.invocations == observed.inputs == observed.execution == "untracked"
