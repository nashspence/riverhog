from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_core.cache_write_grants import CacheWriteGrants
from riverhog_core.catalog_cache_write_models import CachePopulationBeginRecord
from riverhog_core.catalog_db import dispose_session_factory, session_scope, validate_db
from riverhog_core.catalog_models import (
    CollectionArchiveObjectUploadRecord,
    CollectionUploadRecord,
    RetrievalCachePopulationClaimRecord,
    RetrievalCachePopulationRecord,
    RetrievalCacheStoreAccountingRecord,
    StorageIncarnationRecord,
)
from riverhog_core.collection_plan import CollectionVolumePolicy
from riverhog_core.provider_write_dispatch import ArmedProviderWrite
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from riverhog_core.services.cache_population_begin import (
    CacheDataReservationUnavailable,
    CachePopulationBeginAdmission,
)
from riverhog_core.services.retrieval_cache import SqlAlchemyRetrievalCache
from riverhog_protocol.errors import Conflict
from sqlalchemy import delete, event, select
from sqlalchemy.orm import Session

from tests.unit.storage_incarnation_fixtures import seed_storage_incarnation
from tests.unit.test_archive_write_execution import _executor, _filesystem
from tests.unit.test_collection_registration_operations import PART, _member
from tests.unit.test_collection_uploads import _CREATOR, _service_with_archive_objects
from tests.unit.test_retrieval_cache_coordinator import _registration
from tests.unit.test_retrieval_cache_write_execution import _candidate

GRANTS = CacheWriteGrants(calls=4, control_bytes=100000, result_bytes=10000, cleanup_bytes=1000)


@contextmanager
def _case(tmp_path, *, mode="direct", database_url=None, custody_mode="producer-retained"):
    with _filesystem(tmp_path / "cache") as adapter, _executor(adapter, mode) as executor:
        candidate = _candidate(executor)
        service, config, _, _ = _service_with_archive_objects(
            tmp_path,
            policy=CollectionVolumePolicy(
                pack_member_bytes=8, raw_volume_plaintext_bytes=PART, raw_part_plaintext_bytes=PART
            ),
            database_url=database_url,
        )
        factory = service._session_factory
        try:
            with session_scope(factory) as session:
                seed_storage_incarnation(session, "cache", candidate.name)
                session.get(
                    StorageIncarnationRecord, ("cache", candidate.name)
                ).id = candidate.incarnation_id
            registration = replace(_registration(candidate.name), admission_budget_bytes=100)
            cache = SqlAlchemyRetrievalCache(
                {candidate.name: candidate},
                {candidate.name: registration},
                session_factory=factory,
            )
            service._retrieval_cache = cache
            opened = service.create_or_resume(
                idempotency_key="cache-effect-owner",
                ingest_source="fixture",
                archive_store=None,
                use_cache=True,
                custody_mode=custody_mode,
                initiator=_CREATOR,
                event_context=None,
            )
            collection_id = int(opened["collection_id"])
            service.register_artifacts(collection_id, [_member("1", 2)])
            volume = service.list_volumes(collection_id)["volumes"][0]
            parent = ArchiveWriteAdmission(factory)
            guard = service._payload_write_guard(collection_id, _CREATOR)
            epoch = parent.bind_epoch(collection_id, guard=guard)

            def validate_population(session, upload, object_id, expected_bytes):
                assert upload.collection_id == collection_id and expected_bytes == 3
                assert (
                    session.scalar(
                        select(CollectionArchiveObjectUploadRecord.object_id)
                        .where(
                            CollectionArchiveObjectUploadRecord.collection_id == collection_id,
                            CollectionArchiveObjectUploadRecord.object_id == object_id,
                        )
                        .with_for_update()
                    )
                    == object_id
                )

            yield SimpleNamespace(
                adapter=adapter,
                candidate=candidate,
                registration=registration,
                service=service,
                factory=factory,
                owner=CachePopulationBeginAdmission(factory),
                parent=parent,
                collection_id=collection_id,
                object_id=volume["volume_id"],
                epoch=epoch,
                guard=guard,
                validate_population=validate_population,
                cache=cache,
                config=config,
            )
        finally:
            service.close()
            dispose_session_factory(factory)


def _arm(case, **changes):
    arguments = dict(
        owner="upload-cache",
        principal=_CREATOR,
        expected_bytes=3,
        epoch=case.epoch,
        executor_id="e" * 64,
        candidate=case.candidate,
        registration=case.registration,
        grants=GRANTS,
        guard=case.guard,
        validate_population=case.validate_population,
    )
    arguments.update(changes)
    return case.owner.arm(case.collection_id, case.object_id, **arguments)


def _execute(case, ticket):
    response = case.owner.execute(ticket, candidate=case.candidate)
    case.owner.record_response(ticket, response)
    return response


def _stored(case, ticket):
    with session_scope(case.factory) as session:
        row = session.get(CachePopulationBeginRecord, ticket.call_id)
        accounting = session.get(RetrievalCacheStoreAccountingRecord, case.candidate.name)
        return (
            row.state,
            row.unresolved,
            row.request_json,
            row.observation_json,
            row.result_json,
            row.control_bytes,
            row.cleanup_capacity_bytes,
            accounting.reserved_bytes,
        )


def _terminal(case):
    return case.parent.select_terminal(
        case.collection_id,
        kind="cancel",
        request={"fixture": "cache-begin"},
        principal_id=_CREATOR.id,
        grants=case.config.archive_write,
        guard=case.guard,
        validate_selection=lambda *args: None,
    )


@pytest.mark.parametrize("mode", ["direct", "owned-http", "supplied"])
def test_actual_cache_begin_commits_original_effect_before_dispatch_and_retains_charges(
    tmp_path, monkeypatch, mode
):
    with _case(tmp_path, mode=mode) as case:
        ticket = _arm(case)
        before = _stored(case, ticket)
        assert before[0:2] == ("armed", True) and before[-2:] == (1000, 3)
        assert not list((tmp_path / "cache" / "writes").iterdir())
        execute = case.candidate.execute_write
        observed = []

        def verify(invocation, **kwargs):
            observed.append(_stored(case, ticket))
            # A second independent transaction can write while the provider runs.
            with session_scope(case.factory) as session:
                population = session.get(
                    RetrievalCachePopulationRecord,
                    ("archive", case.collection_id, case.object_id),
                    with_for_update=True,
                )
                population.failure = "fixture-provider-outside-transaction"
            return execute(invocation, **kwargs)

        monkeypatch.setattr(case.candidate, "execute_write", verify)
        response = _execute(case, ticket)
        assert observed == [before] and response.outcome == "session"
        expected = ("uncertain", True) if mode == "supplied" else ("settled", False)
        assert _stored(case, ticket)[0:2] == expected
        assert case.cache.release(owner="upload-cache") == 0
        assert _stored(case, ticket)[-1] == 3
        assert len(list((tmp_path / "cache" / "writes").iterdir())) == 1
        with pytest.raises(Conflict, match="already used"):
            case.owner.execute(ticket, candidate=case.candidate)
        assert validate_db(case.config.database_url).condition == "current"


def test_actual_cache_begin_report_survives_terminal_claim_loss_and_parent_retirement(tmp_path):
    with _case(tmp_path) as case:
        ticket = _arm(case)
        _terminal(case)
        assert case.cache.release(owner="upload-cache") == 0
        with session_scope(case.factory) as session:
            session.delete(session.get(CollectionUploadRecord, case.collection_id))
        response = _execute(case, ticket)
        before = _stored(case, ticket)
        case.owner.record_response(ticket, response)
        case.owner.record_response(ticket, response.model_copy(update={"execution": "unknown"}))
        assert _stored(case, ticket) == before and before[0:2] == ("settled", False)
        with session_scope(case.factory) as session:
            assert session.get(CollectionUploadRecord, case.collection_id) is None
            assert (
                session.get(
                    RetrievalCachePopulationClaimRecord,
                    ("upload-cache", "archive", case.collection_id, case.object_id),
                )
                is None
            )
            population = session.get(
                RetrievalCachePopulationRecord, ("archive", case.collection_id, case.object_id)
            )
            assert population.write_token == response.result.write_token
        assert case.cache.reap_abandoned_populations() == 0
        assert _stored(case, ticket)[-1] == 3


def test_positive_unknown_response_needs_exact_original_closure(tmp_path, monkeypatch):
    with _case(tmp_path) as case:
        ticket = _arm(case)
        execute = case.candidate.execute_write
        closed = []

        def unknown_positive(invocation):
            response = execute(invocation)
            closed.append(response)
            return response.model_copy(update={"execution": "unknown"})

        monkeypatch.setattr(case.candidate, "execute_write", unknown_positive)
        _execute(case, ticket)
        original = _stored(case, ticket)
        assert original[0:2] == ("uncertain", True) and original[3] and original[4] is None
        assert case.owner.unresolved(case.collection_id, limit=1) == (ticket.call_id,)
        assert case.cache.release(owner="upload-cache") == 0
        assert _stored(case, ticket) == original
        case.owner.record_response(ticket, closed[0])
        assert _stored(case, ticket)[0:2] == ("settled", False)
        assert case.owner.unresolved(case.collection_id, limit=1) == ()
        assert _stored(case, ticket)[-1] == 3


@pytest.mark.parametrize("changed", ["nonce", "request", "executor", "incarnation", "object"])
def test_changed_live_cache_invocation_is_refused_before_provider_dispatch(tmp_path, changed):
    with _case(tmp_path) as case:
        original = _arm(case)
        arguments = dict(
            call_id=original.call_id,
            collection_id=original.collection_id,
            object_id=original.object_id,
            nonce=original._nonce,
            request_sha256=original.request_sha256,
            executor_id=original.executor_id,
            target_incarnation=original.target_incarnation,
        )
        key = {
            "nonce": "nonce",
            "request": "request_sha256",
            "executor": "executor_id",
            "incarnation": "target_incarnation",
            "object": "object_id",
        }[changed]
        arguments[key] = "other" if changed == "object" else "f" * 64
        changed_ticket = ArmedProviderWrite(**arguments)
        with pytest.raises(Conflict, match="original invocation changed"):
            case.owner.execute(changed_ticket, candidate=case.candidate)
        assert not list((tmp_path / "cache" / "writes").iterdir())
        assert _stored(case, original)[0:2] == ("armed", True)


@pytest.mark.parametrize("capacity", ["control", "data"])
def test_cache_begin_capacity_refusal_rolls_back_population_and_all_reservations(
    tmp_path, capacity
):
    with _case(tmp_path) as case:
        changes = (
            {"grants": replace(GRANTS, control_bytes=1)}
            if capacity == "control"
            else {"registration": replace(case.registration, admission_budget_bytes=2)}
        )
        with pytest.raises(Conflict, match="exhausted") as refused:
            _arm(case, **changes)
        assert isinstance(refused.value, CacheDataReservationUnavailable) is (capacity == "data")
        with session_scope(case.factory) as session:
            assert session.scalar(select(CachePopulationBeginRecord.id)) is None
            assert session.scalar(select(RetrievalCachePopulationRecord.object_id)) is None
            assert (
                session.get(RetrievalCacheStoreAccountingRecord, case.candidate.name).reserved_bytes
                == 0
            )
        assert not list((tmp_path / "cache" / "writes").iterdir())


def test_cache_begin_commit_failure_never_returns_live_dispatch_authority(tmp_path):
    with _case(tmp_path) as case:

        def fail_commit(session):
            if any(isinstance(row, CachePopulationBeginRecord) for row in session.new):
                raise OSError("injected cache admission commit failure")

        event.listen(Session, "before_commit", fail_commit)
        try:
            with pytest.raises(OSError, match="commit failure"):
                _arm(case)
        finally:
            event.remove(Session, "before_commit", fail_commit)
        with session_scope(case.factory) as session:
            assert session.scalar(select(CachePopulationBeginRecord.id)) is None
            assert (
                session.get(RetrievalCacheStoreAccountingRecord, case.candidate.name).reserved_bytes
                == 0
            )
        assert not list((tmp_path / "cache" / "writes").iterdir())


def test_cache_report_does_not_attach_original_token_to_recreated_population(tmp_path):
    with _case(tmp_path) as case:
        ticket = _arm(case)
        key = ("archive", case.collection_id, case.object_id)
        with session_scope(case.factory) as session:
            population = session.get(RetrievalCachePopulationRecord, key)
            source_incarnation = population.source_incarnation_id
            session.execute(delete(RetrievalCachePopulationRecord))
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
        response = _execute(case, ticket)
        with session_scope(case.factory) as session:
            population = session.get(RetrievalCachePopulationRecord, key)
            assert population.write_token is None and population.cache_store is None
            assert (
                population.generation
                != session.get(CachePopulationBeginRecord, ticket.call_id).population_generation
            )
        assert response.outcome == "session" and _stored(case, ticket)[-1] == 3
        with pytest.raises(Conflict, match="retains admitted provider effects"):
            case.cache.admit(
                owner="replacement",
                source_store="archive",
                collection_id=case.collection_id,
                object_id=case.object_id,
                expected_bytes=3,
            )
        assert case.cache.reap_abandoned_populations() == 0


def test_actual_http_cache_begin_timeout_preserves_unknown_and_reservation_after_late_creation(
    tmp_path, monkeypatch
):
    with _case(tmp_path, mode="owned-http") as case:
        ticket = _arm(case)
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()
        original = case.adapter.begin_write
        calls = []

        def delayed(request):
            calls.append(request)
            entered.set()
            try:
                assert release.wait(timeout=10)
                return original(request)
            finally:
                finished.set()

        monkeypatch.setattr(case.adapter, "begin_write", delayed)
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(_execute, case, ticket)
            try:
                assert entered.wait(timeout=5)
                _terminal(case)
                assert case.cache.release(owner="upload-cache") == 0
                for _ in range(3):
                    assert (
                        case.candidate.find_completed_population(
                            source_store="archive",
                            collection_id=case.collection_id,
                            object_id=case.object_id,
                            expected_bytes=3,
                        )
                        is None
                    )
                    assert case.cache.reap_abandoned_populations() == 0
                response = future.result(timeout=5)
                assert response.execution == "unknown" and response.outcome == "unknown"
                unknown = _stored(case, ticket)
                assert unknown[0:2] == ("uncertain", True) and unknown[-1] == 3
                assert not list((tmp_path / "cache" / "writes").iterdir())
                release.set()
                assert finished.wait(timeout=5) and len(calls) == 1
                assert len(list((tmp_path / "cache" / "writes").iterdir())) == 1
                assert _stored(case, ticket) == unknown
                assert case.owner.unresolved(case.collection_id, limit=1) == (ticket.call_id,)
                assert case.cache.reap_abandoned_populations() == 0
            finally:
                release.set()
                assert finished.wait(timeout=5)


def test_cache_report_commit_ack_loss_retries_the_same_response_without_provider_resend(
    tmp_path, monkeypatch
):
    with _case(tmp_path) as case:
        ticket = _arm(case)
        response = case.owner.execute(ticket, candidate=case.candidate)
        report = case.owner.record_response
        attempts = []

        def lose_ack(original_ticket, original_response):
            report(original_ticket, original_response)
            attempts.append(original_ticket.call_id)
            if len(attempts) == 1:
                raise OSError("injected cache report acknowledgement loss")

        monkeypatch.setattr(case.owner, "record_response", lose_ack)
        with pytest.raises(OSError, match="acknowledgement loss"):
            case.owner.record_response(ticket, response)
        before = _stored(case, ticket)
        case.owner.record_response(ticket, response)
        assert attempts == [ticket.call_id, ticket.call_id] and _stored(case, ticket) == before
        assert len(list((tmp_path / "cache" / "writes").iterdir())) == 1
        assert case.cache.release(owner="upload-cache") == 0


def test_managed_cache_begin_fences_legacy_put_resumable_and_ready_publication(tmp_path):
    from riverhog_core.ports.retrieval_cache import RetrievalCacheReceipt
    from riverhog_core.services.retrieval_cache import register_cache_ready

    with _case(tmp_path) as case:
        ticket = _arm(case)
        response = _execute(case, ticket)
        with session_scope(case.factory) as session:
            population = session.get(
                RetrievalCachePopulationRecord,
                ("archive", case.collection_id, case.object_id),
            )
            admission = case.cache._admission(population, owner="upload-cache")
        assert not case.cache.is_current(admission=admission)
        with pytest.raises(Conflict, match="retains admitted provider effects"):
            case.cache.put(admission=admission, content=(b"abc",))
        with pytest.raises(Conflict, match="retains admitted provider effects"):
            case.cache.resumable_object_store(admission=admission)
        with pytest.raises(Conflict, match="retains admitted provider effects"):
            with session_scope(case.factory) as session:
                register_cache_ready(
                    session,
                    source_store="archive",
                    collection_id=case.collection_id,
                    object_id=case.object_id,
                    receipt=RetrievalCacheReceipt(
                        cache_store=case.candidate.name,
                        object_path=response.result.object_path,
                        revision=None,
                        stored_bytes=3,
                        stored_sha256=None,
                        cached_at="fixture",
                        verified_at="fixture",
                    ),
                )
        assert _stored(case, ticket)[-1] == 3


def test_cache_pending_status_and_reaper_use_owned_indexes(tmp_path):
    with _case(tmp_path) as case:
        ticket = _arm(case)
        statements = []
        engine = case.factory.kw["bind"]

        def capture(connection, cursor, statement, parameters, context, executemany):
            if (
                "FROM retrieval_cache_populations" in statement
                or "FROM cache_population_begins" in statement
            ):
                statements.append((statement, parameters))

        event.listen(engine, "before_cursor_execute", capture)
        try:
            assert case.owner.unresolved(case.collection_id, limit=1) == (ticket.call_id,)
            assert case.cache.release(owner="upload-cache") == 0
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        with engine.connect() as connection:
            plans = [
                " ".join(
                    str(row[-1])
                    for row in connection.exec_driver_sql("EXPLAIN QUERY PLAN " + sql, parameters)
                )
                for sql, parameters in statements
            ]
        assert any("ix_cache_population_begins_pending" in plan for plan in plans)
        assert any("ix_retrieval_cache_populations_reap" in plan for plan in plans)


def _assert_withdrawal_survives_owner_retirement(case, tmp_path):
    ticket = _arm(case)
    before = _stored(case, ticket)
    terminal = _terminal(case)
    assert case.cache.release(owner="upload-cache") == 0
    with session_scope(case.factory) as session:
        session.delete(session.get(CollectionUploadRecord, case.collection_id))
        session.execute(delete(RetrievalCachePopulationRecord))
    proof = ticket.withdraw()
    case.owner.settle_withdrawn(proof)
    CachePopulationBeginAdmission(case.factory).settle_withdrawn(proof)
    after = _stored(case, ticket)
    assert after[0:2] == ("settled", False)
    assert after[2:4] == before[2:4] and after[5:] == before[5:]
    assert json.loads(after[4]) == {"dispatch": "never-started"}
    assert case.owner.unresolved(case.collection_id, limit=1) == ()
    assert not list((tmp_path / "cache" / "writes").iterdir())
    assert case.cache.reap_abandoned_populations() == 0
    from riverhog_core.catalog_archive_write_models import ArchiveUploadTerminalRecord

    with session_scope(case.factory) as session:
        assert session.get(ArchiveUploadTerminalRecord, terminal).phase == "draining"
        assert session.scalar(select(RetrievalCachePopulationRecord.object_id)) is None
        assert session.scalar(select(RetrievalCachePopulationClaimRecord.owner)) is None
    with pytest.raises(Conflict, match="already used"):
        case.owner.execute(ticket, candidate=case.candidate)


def test_cache_withdrawal_survives_terminal_claim_loss_and_parent_population_retirement(tmp_path):
    with _case(tmp_path) as case:
        _assert_withdrawal_survives_owner_retirement(case, tmp_path)


def _assert_withdrawal_commit_and_ack_failures(case, tmp_path):
    ticket = _arm(case)
    before = _stored(case, ticket)
    proof = ticket.withdraw()

    def fail_commit(session):
        if any(isinstance(row, CachePopulationBeginRecord) for row in session.dirty):
            raise OSError("injected withdrawal commit failure")

    event.listen(Session, "before_commit", fail_commit)
    try:
        with pytest.raises(OSError, match="withdrawal commit failure"):
            case.owner.settle_withdrawn(proof)
    finally:
        event.remove(Session, "before_commit", fail_commit)
    assert _stored(case, ticket) == before and ticket.withdraw() is proof
    with pytest.raises(OSError, match="withdrawal acknowledgement loss"):
        case.owner.settle_withdrawn(proof)
        raise OSError("injected withdrawal acknowledgement loss")
    settled = _stored(case, ticket)
    case.owner.settle_withdrawn(proof)
    assert _stored(case, ticket) == settled and settled[-1] == 3
    assert not list((tmp_path / "cache" / "writes").iterdir())


def test_cache_withdrawal_commit_and_ack_failures_keep_exact_proof_and_reservation(tmp_path):
    with _case(tmp_path) as case:
        _assert_withdrawal_commit_and_ack_failures(case, tmp_path)


@pytest.mark.parametrize(
    "field,value",
    [
        ("_nonce", "changed"),
        ("request_sha256", "f" * 64),
        ("executor_id", "f" * 64),
        ("target_incarnation", "changed"),
    ],
)
def test_cache_withdrawal_rechecks_original_invocation(tmp_path, field, value):
    with _case(tmp_path) as case:
        ticket = _arm(case)
        setattr(ticket, field, value)
        with pytest.raises(Conflict, match="original invocation changed"):
            case.owner.settle_withdrawn(ticket.withdraw())
        assert _stored(case, ticket)[0:2] == ("armed", True)
        assert not list((tmp_path / "cache" / "writes").iterdir())


def test_actual_cache_invocation_with_unknown_closure_cannot_be_withdrawn(tmp_path):
    with _case(tmp_path, mode="supplied") as case:
        ticket = _arm(case)
        response = _execute(case, ticket)
        assert response.execution == "unknown" and response.outcome == "session"
        with pytest.raises(Conflict, match="already started"):
            ticket.withdraw()
        assert _stored(case, ticket)[0:2] == ("uncertain", True)
        assert _stored(case, ticket)[-1] == 3
        assert len(list((tmp_path / "cache" / "writes").iterdir())) == 1


def test_full_optional_cache_does_not_hide_selected_terminal_authority(tmp_path):
    with _case(tmp_path) as case:
        _terminal(case)
        with pytest.raises(Conflict) as refused:
            _arm(case, registration=replace(case.registration, admission_budget_bytes=2))
        assert not isinstance(refused.value, CacheDataReservationUnavailable)
        with session_scope(case.factory) as session:
            assert session.scalar(select(CachePopulationBeginRecord.id)) is None
            assert (
                session.get(RetrievalCacheStoreAccountingRecord, case.candidate.name).reserved_bytes
                == 0
            )
        assert not list((tmp_path / "cache" / "writes").iterdir())
