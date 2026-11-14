from __future__ import annotations

import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_core.catalog_cache_write_models import (
    CachePopulationBeginRecord,
    CachePopulationWriteEffectRecord,
)
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import (
    CollectionUploadRecord,
    RetrievalCacheObjectRecord,
    RetrievalCachePopulationRecord,
    RetrievalCacheStoreAccountingRecord,
)
from riverhog_core.services.cache_population_writes import CachePopulationWriteAdmission
from riverhog_protocol.errors import Conflict, NotFound
from riverhog_storage_adapter_protocol import (
    CompleteWriteOperation,
    WriteCompleteRequest,
    WritePartOperation,
    WriteSegmentListRequest,
    WriteSegmentRequest,
)
from sqlalchemy import delete, event, select
from sqlalchemy.orm import Session

from tests.unit.test_cache_population_begin import (
    GRANTS,
    _case,
    _terminal,
)
from tests.unit.test_cache_population_begin import (
    _arm as _arm_begin,
)
from tests.unit.test_cache_population_begin import (
    _execute as _execute_begin,
)
from tests.unit.test_collection_uploads import _CREATOR

CONTENT = b"abc"


@contextmanager
def _writing(tmp_path, *, mode="direct", database_url=None, custody_mode="producer-retained"):
    with _case(tmp_path, mode=mode, database_url=database_url, custody_mode=custody_mode) as case:
        begin = _arm_begin(case)
        execute = case.candidate.execute_write
        try:
            # The original begin is accounted for by the actual joined filesystem
            # executor; subsequent calls exercise the selected transport profile.
            if mode == "supplied":
                case.candidate.execute_write = case.adapter.execute_write
            response = _execute_begin(case, begin)
        finally:
            case.candidate.execute_write = execute
        yield SimpleNamespace(
            case=case,
            owner=CachePopulationWriteAdmission(case.factory),
            begin=begin,
            session=response.result,
        )


def _part(writing, *, number=1, content=CONTENT):
    return WritePartOperation(
        request=WriteSegmentRequest(
            session=writing.session, number=number, stored_bytes=len(content)
        ),
        stored_sha256=hashlib.sha256(content).hexdigest(),
    )


def _complete(writing):
    case = writing.case
    page = case.candidate._adapter.list_segments(
        WriteSegmentListRequest(session=writing.session, maximum_items=1)
    )
    assert page.completion is not None
    original = case.candidate.population_write_request(
        source_store="archive",
        collection_id=case.collection_id,
        object_id=case.object_id,
        expected_bytes=3,
    )
    return CompleteWriteOperation(
        request=WriteCompleteRequest(
            session=writing.session,
            completion=page.completion,
            expected_bytes=3,
            expected_content_type=original.content_type,
            required_identity_assertions=original.required_identity_assertions,
            expected_placement_policy=original.placement_policy,
        )
    )


def _arm(writing, operation, **changes):
    case = writing.case
    arguments = dict(
        begin_call_id=writing.begin.call_id,
        operation=operation,
        owner="upload-cache",
        principal=_CREATOR,
        epoch=case.epoch,
        executor_id="d" * 64,
        candidate=case.candidate,
        grants=GRANTS,
        guard=case.guard,
        validate_population=case.validate_population,
    )
    arguments.update(changes)
    return writing.owner.arm(case.collection_id, case.object_id, **arguments)


def _stored(writing, ticket):
    case = writing.case
    with session_scope(case.factory) as session:
        row = session.get(CachePopulationWriteEffectRecord, ticket.call_id)
        return (
            row.state,
            row.unresolved,
            row.request_json,
            row.observation_json,
            row.result_json,
            row.control_bytes,
            row.cleanup_capacity_bytes,
            session.get(RetrievalCacheStoreAccountingRecord, case.candidate.name).reserved_bytes,
        )


def _execute(writing, ticket, *, content=b""):
    response = writing.owner.execute(ticket, candidate=writing.case.candidate, content=content)
    writing.owner.record_response(ticket, response)
    return response


def _assert_actual_session_parts_and_completion(writing, monkeypatch, mode):
    case = writing.case
    calls = []
    closed = []
    execute = case.adapter.execute_write

    def capture(invocation, **kwargs):
        # Every physical invocation sees its committed exact original effect,
        # while a second transaction can obtain its own writer.
        ticket = SimpleNamespace(call_id=invocation.call.call_id)
        before = _stored(writing, ticket)
        assert before[0:2] == ("armed", True)
        with session_scope(case.factory) as session:
            row = session.get(
                RetrievalCachePopulationRecord, ("archive", case.collection_id, case.object_id)
            )
            row.failure = "provider-outside-catalog-transaction"
        calls.append(invocation.operation.kind)
        response = execute(invocation, **kwargs)
        closed.append(response)
        return response

    monkeypatch.setattr(case.adapter, "execute_write", capture)
    part = _arm(writing, _part(writing))
    response = _execute(writing, part, content=CONTENT)
    assert response.outcome == "part"
    if mode == "supplied":
        assert _stored(writing, part)[0:2] == ("uncertain", True)
        with pytest.raises(Conflict, match="unresolved"):
            _arm(writing, _complete(writing))
        with pytest.raises(Conflict, match="unresolved"):
            writing.owner.acknowledge_report(part)
        writing.owner.record_response(part, closed[-1])
    assert _stored(writing, part)[0:2] == ("settled", False)
    writing.owner.acknowledge_report(part)
    with pytest.raises(NotFound):
        writing.owner.acknowledge_report(part)
    complete = _arm(writing, _complete(writing))
    response = _execute(writing, complete)
    assert response.outcome == "completed" and response.result.stored_bytes == 3
    if mode == "supplied":
        assert _stored(writing, complete)[0:2] == ("uncertain", True)
        writing.owner.record_response(complete, closed[-1])
    before = _stored(writing, complete)
    writing.owner.record_response(complete, response.model_copy(update={"execution": "unknown"}))
    assert _stored(writing, complete) == before and before[0:2] == ("settled", False)
    with pytest.raises(Conflict, match="independently owned"):
        writing.owner.acknowledge_report(complete)
    with pytest.raises(Conflict, match="not writable"):
        _arm(writing, _part(writing, number=2))
    assert calls == ["part", "complete"] and before[-1] == 3
    with session_scope(case.factory) as session:
        assert session.scalar(select(RetrievalCacheObjectRecord.object_id)) is None
        begin = session.get(CachePopulationBeginRecord, writing.begin.call_id)
        assert begin.completion_call_id == complete.call_id


@pytest.mark.parametrize("mode", ["direct", "owned-http", "supplied"])
def test_actual_cache_session_parts_complete_without_publishing_or_releasing_ownership(
    tmp_path, monkeypatch, mode
):
    with _writing(tmp_path, mode=mode) as writing:
        _assert_actual_session_parts_and_completion(writing, monkeypatch, mode)


@pytest.mark.parametrize("content", [b"abd", b"ab"])
def test_cache_part_changed_ciphertext_refuses_before_dispatch_and_can_withdraw(tmp_path, content):
    with _writing(tmp_path) as writing:
        ticket = _arm(writing, _part(writing))
        before = _stored(writing, ticket)
        with pytest.raises(Conflict, match="ciphertext"):
            _execute(writing, ticket, content=content)
        assert _stored(writing, ticket) == before
        page = writing.case.adapter.list_segments(WriteSegmentListRequest(session=writing.session))
        assert page.segments == ()
        writing.owner.settle_withdrawn(ticket.withdraw())
        assert _stored(writing, ticket)[0:2] == ("settled", False)
        assert _stored(writing, ticket)[-1] == 3


@pytest.mark.parametrize("changed", ["token", "path", "generation", "claim", "terminal"])
def test_cache_session_forward_admission_rechecks_original_ownership(tmp_path, changed):
    with _writing(tmp_path) as writing:
        case = writing.case
        operation = _part(writing)
        if changed in {"token", "path"}:
            field = "write_token" if changed == "token" else "object_path"
            session = writing.session.model_copy(update={field: "other"})
            operation = operation.model_copy(
                update={"request": operation.request.model_copy(update={"session": session})}
            )
        elif changed == "generation":
            with session_scope(case.factory) as session:
                row = session.get(
                    RetrievalCachePopulationRecord, ("archive", case.collection_id, case.object_id)
                )
                row.generation = "f" * 64
        elif changed == "claim":
            assert case.cache.release(owner="upload-cache") == 0
        else:
            _terminal(case)
        with pytest.raises((Conflict, NotFound)):
            _arm(writing, operation)
        with session_scope(case.factory) as session:
            assert session.scalar(select(CachePopulationWriteEffectRecord.id)) is None


@pytest.mark.parametrize("capacity", ["calls", "control"])
def test_cache_session_shared_capacity_includes_original_begin_and_retained_parts(
    tmp_path, capacity
):
    with _writing(tmp_path) as writing:
        case = writing.case
        if capacity == "calls":
            grants = replace(GRANTS, calls=1)
        else:
            with session_scope(case.factory) as session:
                charge = session.get(
                    CachePopulationBeginRecord, writing.begin.call_id
                ).control_bytes
            grants = replace(GRANTS, control_bytes=charge)
        with pytest.raises(Conflict, match="grant is exhausted"):
            _arm(writing, _part(writing), grants=grants)
        with session_scope(case.factory) as session:
            assert session.scalar(select(CachePopulationWriteEffectRecord.id)) is None
            assert (
                session.get(CachePopulationBeginRecord, writing.begin.call_id).completion_call_id
                is None
            )
        assert (
            case.adapter.list_segments(WriteSegmentListRequest(session=writing.session)).segments
            == ()
        )


def test_unresolved_cache_part_blocks_its_lane_and_completion_until_exact_report(tmp_path):
    with _writing(tmp_path) as writing:
        ticket = _arm(writing, _part(writing))
        response = writing.owner.execute(ticket, candidate=writing.case.candidate, content=CONTENT)
        with pytest.raises(Conflict, match="unresolved"):
            _arm(writing, _part(writing))
        with pytest.raises(Conflict, match="unresolved"):
            _arm(writing, _complete(writing))
        writing.owner.record_response(ticket, response)
        complete = _arm(writing, _complete(writing))
        with pytest.raises(Conflict, match="not writable"):
            _arm(writing, _part(writing, number=2))
        proof = complete.withdraw()
        writing.owner.settle_withdrawn(proof)
        writing.owner.settle_withdrawn(proof)
        with session_scope(writing.case.factory) as session:
            assert (
                session.get(CachePopulationBeginRecord, writing.begin.call_id).completion_call_id
                is None
            )
        writing.owner.acknowledge_report(complete)
        replacement = _arm(writing, _complete(writing))
        assert replacement.call_id != complete.call_id


def test_cache_part_report_commit_ack_loss_retains_exact_response_without_resend(
    tmp_path, monkeypatch
):
    with _writing(tmp_path) as writing:
        ticket = _arm(writing, _part(writing))
        response = writing.owner.execute(ticket, candidate=writing.case.candidate, content=CONTENT)

        def fail_commit(session):
            if any(isinstance(row, CachePopulationWriteEffectRecord) for row in session.dirty):
                raise OSError("injected cache part report commit failure")

        event.listen(Session, "before_commit", fail_commit)
        try:
            with pytest.raises(OSError, match="commit failure"):
                writing.owner.record_response(ticket, response)
        finally:
            event.remove(Session, "before_commit", fail_commit)
        assert _stored(writing, ticket)[0:2] == ("armed", True)
        with pytest.raises(OSError, match="acknowledgement loss"):
            writing.owner.record_response(ticket, response)
            raise OSError("injected cache report acknowledgement loss")
        before = _stored(writing, ticket)
        writing.owner.record_response(ticket, response)
        assert _stored(writing, ticket) == before
        page = writing.case.adapter.list_segments(WriteSegmentListRequest(session=writing.session))
        assert len(page.segments) == 1
        with pytest.raises(Conflict, match="already used"):
            _execute(writing, ticket, content=CONTENT)


def _assert_late_http_effect_survives_retirement(writing, tmp_path, monkeypatch, kind):
    case = writing.case
    if kind == "complete":
        part = _arm(writing, _part(writing))
        _execute(writing, part, content=CONTENT)
        operation = _complete(writing)
        method = "complete_write"
    else:
        operation = _part(writing)
        method = "write_segment"
    ticket = _arm(writing, operation)
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    reported = threading.Event()
    original = getattr(case.adapter, method)
    responses = []
    executor = case.adapter.execute_write

    def delayed(*args, **kwargs):
        entered.set()
        try:
            assert release.wait(timeout=10)
            return original(*args, **kwargs)
        finally:
            finished.set()

    def capture(invocation, **kwargs):
        response = executor(invocation, **kwargs)
        responses.append(response)
        reported.set()
        return response

    monkeypatch.setattr(case.adapter, method, delayed)
    monkeypatch.setattr(case.adapter, "execute_write", capture)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(_execute, writing, ticket, content=CONTENT if kind == "part" else b"")
        try:
            assert entered.wait(timeout=5)
            _terminal(case)
            assert case.cache.release(owner="upload-cache") == 0
            with session_scope(case.factory) as session:
                session.delete(session.get(CollectionUploadRecord, case.collection_id))
                session.execute(delete(RetrievalCachePopulationRecord))
            response = future.result(timeout=5)
            assert response.execution == "unknown" and response.outcome == "unknown"
            before = _stored(writing, ticket)
            assert before[0:2] == ("uncertain", True) and before[-1] == 3
            for _ in range(3):
                assert case.cache.reap_abandoned_populations() == 0
                assert _stored(writing, ticket) == before
            with pytest.raises(Conflict, match="already started"):
                ticket.withdraw()
        finally:
            release.set()
            assert finished.wait(timeout=5)
    # The original adapter executor can eventually supply the correlated closure.
    # A caller timeout, reaper pass or missing population never supplied it.
    assert reported.wait(timeout=5)
    assert len(responses) == 1 and responses[0].execution == "closed"
    assert _stored(writing, ticket) == before
    writing.owner.record_response(ticket, responses[0])
    assert _stored(writing, ticket)[0:2] == ("settled", False)
    assert _stored(writing, ticket)[-1] == 3
    with session_scope(case.factory) as session:
        assert session.get(CollectionUploadRecord, case.collection_id) is None
        assert session.scalar(select(RetrievalCachePopulationRecord.object_id)) is None
        assert session.scalar(select(RetrievalCacheObjectRecord.object_id)) is None


@pytest.mark.parametrize("kind", ["part", "complete"])
def test_actual_http_late_cache_session_effect_remains_owned_after_parent_retirement(
    tmp_path, monkeypatch, kind
):
    with _writing(tmp_path, mode="owned-http") as writing:
        _assert_late_http_effect_survives_retirement(writing, tmp_path, monkeypatch, kind)


def test_retained_cache_part_fences_replacement_and_reaping_without_begin_anchor(tmp_path):
    with _writing(tmp_path) as writing:
        case = writing.case
        ticket = _arm(writing, _part(writing))
        key = ("archive", case.collection_id, case.object_id)
        with session_scope(case.factory) as session:
            population = session.get(RetrievalCachePopulationRecord, key)
            source_incarnation = population.source_incarnation_id
            session.execute(delete(RetrievalCachePopulationRecord))
            session.delete(session.get(CachePopulationBeginRecord, writing.begin.call_id))
            session.add(
                RetrievalCachePopulationRecord(
                    source_store="archive",
                    source_incarnation_id=source_incarnation,
                    collection_id=case.collection_id,
                    object_id=case.object_id,
                    expected_bytes=3,
                    state="waiting",
                    initiated_at="fixture",
                    updated_at="fixture",
                )
            )
        assert case.cache.reap_abandoned_populations() == 0
        with pytest.raises(Conflict, match="retains admitted provider effects"):
            case.cache.admit(
                owner="replacement",
                source_store="archive",
                collection_id=case.collection_id,
                object_id=case.object_id,
                expected_bytes=3,
            )
        with pytest.raises(Conflict, match="original effect is retained"):
            _arm_begin(case)
        response = _execute(writing, ticket, content=CONTENT)
        assert response.outcome == "part" and _stored(writing, ticket)[-1] == 3
        with session_scope(case.factory) as session:
            population = session.get(RetrievalCachePopulationRecord, key)
            assert population.write_token is None and population.cache_store is None
            assert (
                population.generation
                != session.get(
                    CachePopulationWriteEffectRecord, ticket.call_id
                ).population_generation
            )


def test_cache_session_pending_and_legacy_fences_use_owned_indexes(tmp_path):
    with _writing(tmp_path) as writing:
        case = writing.case
        ticket = _arm(writing, _part(writing))
        engine = case.factory.kw["bind"]
        statements = []

        def capture(connection, cursor, statement, parameters, context, executemany):
            if "cache_population_write_effects" in statement:
                statements.append((statement, parameters))

        event.listen(engine, "before_cursor_execute", capture)
        try:
            assert writing.owner.unresolved(case.collection_id, limit=2) == (ticket.call_id,)
            assert case.cache.reap_abandoned_populations() == 0
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        plans = []
        with engine.connect() as connection:
            for statement, parameters in statements:
                plans.extend(
                    row[3]
                    for row in connection.exec_driver_sql(
                        "EXPLAIN QUERY PLAN " + statement, parameters
                    )
                )
        assert any("ix_cache_write_effects_pending" in plan for plan in plans)
        assert any("ix_cache_write_effects_object" in plan for plan in plans)
