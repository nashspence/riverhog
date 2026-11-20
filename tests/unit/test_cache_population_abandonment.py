from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace

import pytest
from riverhog_core.catalog_cache_write_models import (
    CachePopulationBeginRecord,
    CachePopulationWriteEffectRecord,
)
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import (
    CollectionArchiveObjectUploadRecord,
    CollectionUploadRecord,
    RetrievalCachePopulationClaimRecord,
    RetrievalCachePopulationRecord,
    RetrievalCacheStoreAccountingRecord,
)
from riverhog_core.services import cache_population_abandonment as abandonment_module
from riverhog_core.services.cache_population_abandonment import CachePopulationAbandonment
from riverhog_core.services.cache_population_writes import CachePopulationWriteAdmission
from riverhog_core.services.retrieval_cache import _require_unmanaged_population
from riverhog_protocol.errors import Conflict
from sqlalchemy import select
from time_formats import utc_timestamp_now

from tests.unit.test_cache_population_begin import GRANTS, _arm, _case, _execute, _terminal
from tests.unit.test_cache_population_writes import _part
from tests.unit.test_collection_uploads import _CREATOR

HANDOFF_GRANTS = replace(GRANTS, control_bytes=600000, cleanup_bytes=65536)


def _begun(case, *, withdrawn=False, grants=HANDOFF_GRANTS):
    ticket = _arm(case, grants=grants)
    if withdrawn:
        case.owner.settle_withdrawn(ticket.withdraw())
    else:
        _execute(case, ticket)
    return ticket


def _accept(case, ticket, operation):
    return case.cache.abandonment.accept_report(
        case.collection_id,
        case.object_id,
        ticket.call_id,
        operation_id=operation,
        guard=case.guard,
    )


def _population(case, session):
    return session.scalar(
        select(RetrievalCachePopulationRecord).where(
            RetrievalCachePopulationRecord.collection_id == case.collection_id,
            RetrievalCachePopulationRecord.object_id == case.object_id,
        )
    )


def _assert_handoff(tmp_path, monkeypatch, mode, *, database_url=None, withdrawn=False):
    with _case(tmp_path, mode=mode, database_url=database_url) as case:
        ticket = _begun(case, withdrawn=withdrawn)
        operation = _terminal(case)

        def forbidden(*args, **kwargs):
            raise AssertionError("handoff must not execute provider work")

        monkeypatch.setattr(case.candidate, "execute_write", forbidden)
        receipt = _accept(case, ticket, operation)
        owner = CachePopulationAbandonment(case.factory)
        assert (
            owner.load(
                receipt.source_store,
                receipt.collection_id,
                receipt.object_id,
                operation_id=operation,
            )
            == receipt
        )
        with session_scope(case.factory) as session:
            source = session.get(
                CollectionArchiveObjectUploadRecord, (case.collection_id, case.object_id)
            )
            population = _population(case, session)
            assert (
                source.cache_abandonment_json
                == population.abandonment_receipt_json
                == receipt.receipt_json
            )
            assert population.state == "abandoning" and population.generation == receipt.generation
            assert population.closed_receipt_json is population.publication_json is None
            assert session.get(CachePopulationBeginRecord, ticket.call_id) is not None
            assert (
                session.get(RetrievalCacheStoreAccountingRecord, case.candidate.name).reserved_bytes
                == 3
            )
        owner.acknowledge(receipt)
        owner.acknowledge(receipt)
        assert _accept(case, ticket, operation) == receipt
        assert case.cache.release(owner="upload-cache") == 0
        with session_scope(case.factory) as session:
            population = _population(case, session)
            assert session.get(CachePopulationBeginRecord, ticket.call_id) is None
            assert population.begin_call_id == ticket.call_id
            assert population.state == "abandoning"
            assert (population.write_token is None) is withdrawn
            assert (
                session.get(RetrievalCacheStoreAccountingRecord, case.candidate.name).reserved_bytes
                == 3
            )
            with pytest.raises(Conflict, match="retains admitted"):
                _require_unmanaged_population(
                    session, (receipt.source_store, receipt.collection_id, receipt.object_id)
                )


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("withdrawn", (False, True))
def test_original_closed_cache_population_enters_independent_abandoning_owner_without_disposal(
    tmp_path, monkeypatch, mode, withdrawn
):
    _assert_handoff(tmp_path, monkeypatch, mode, withdrawn=withdrawn)


@pytest.mark.parametrize("committed", (False, True))
@pytest.mark.parametrize("phase", ("accept", "acknowledge"))
def test_cache_abandonment_commit_and_reply_loss_retry_only_original_receiving(
    tmp_path, monkeypatch, committed, phase
):
    with _case(tmp_path) as case:
        ticket = _begun(case)
        operation = _terminal(case)
        owner = case.cache.abandonment
        receipt = _accept(case, ticket, operation) if phase == "acknowledge" else None
        transaction = abandonment_module.session_scope
        attempts = []

        @contextmanager
        def rollback(factory):
            with transaction(factory) as session:
                yield session
                if not committed and len(attempts) == 1:
                    raise OSError("injected abandonment reply loss")

        def receive():
            attempts.append(True)
            result = (
                _accept(case, ticket, operation)
                if phase == "accept"
                else owner.acknowledge(receipt)
            )
            if committed and len(attempts) == 1:
                raise OSError("injected abandonment reply loss")
            return result

        monkeypatch.setattr(abandonment_module, "session_scope", rollback)
        with pytest.raises(OSError, match="reply loss"):
            receive()
        received = receive()
        if phase == "accept":
            receipt = received
            owner.acknowledge(receipt)
        assert receipt is not None
        assert (
            owner.load(
                receipt.source_store,
                receipt.collection_id,
                receipt.object_id,
                operation_id=operation,
            )
            == receipt
        )
        with session_scope(case.factory) as session:
            assert session.get(CachePopulationBeginRecord, ticket.call_id) is None
            assert _population(case, session).state == "abandoning"


def test_cache_abandonment_receiving_outlives_original_upload_and_volume(tmp_path):
    with _case(tmp_path) as case:
        ticket = _begun(case)
        operation = _terminal(case)
        receipt = _accept(case, ticket, operation)
        with session_scope(case.factory) as session:
            session.delete(session.get(CollectionUploadRecord, case.collection_id))
        cold = CachePopulationAbandonment(case.factory)
        restored = cold.load(
            receipt.source_store, receipt.collection_id, receipt.object_id, operation_id=operation
        )
        assert restored == receipt
        cold.acknowledge(restored)
        with session_scope(case.factory) as session:
            assert session.get(CachePopulationBeginRecord, ticket.call_id) is None
            assert _population(case, session).state == "abandoning"


def test_unknown_original_cache_begin_cannot_transfer_ownership(tmp_path):
    with _case(tmp_path) as case:
        ticket = _arm(case, grants=HANDOFF_GRANTS)
        case.owner.record_unknown(ticket)
        operation = _terminal(case)
        with pytest.raises(Conflict, match="original closed begin"):
            _accept(case, ticket, operation)
        with session_scope(case.factory) as session:
            assert session.get(CachePopulationBeginRecord, ticket.call_id).unresolved
            assert _population(case, session).state == "admitting"


def test_cache_abandonment_waits_for_actual_child_receiving(tmp_path):
    from types import SimpleNamespace

    with _case(tmp_path) as case:
        ticket = _begun(case)
        with session_scope(case.factory) as session:
            population = _population(case, session)
            from riverhog_storage_adapter_protocol import WriteSession

            cache_session = WriteSession(
                object_path=population.object_path,
                write_token=population.write_token,
                expected_bytes=3,
            )
        children = CachePopulationWriteAdmission(case.factory)
        child = children.arm(
            case.collection_id,
            case.object_id,
            begin_call_id=ticket.call_id,
            operation=_part(SimpleNamespace(session=cache_session)),
            owner="upload-cache",
            principal=_CREATOR,
            epoch=case.epoch,
            executor_id="d" * 64,
            candidate=case.candidate,
            grants=HANDOFF_GRANTS,
            guard=case.guard,
            validate_population=case.validate_population,
        )
        children.settle_withdrawn(child.withdraw())
        operation = _terminal(case)
        with pytest.raises(Conflict, match="unreceived population"):
            _accept(case, ticket, operation)
        with session_scope(case.factory) as session:
            assert session.get(CachePopulationWriteEffectRecord, child.call_id) is not None
        children.acknowledge_report(child)
        assert _accept(case, ticket, operation).operation_id == operation


def test_cache_abandonment_cannot_take_another_owner_claim(tmp_path):
    with _case(tmp_path) as case:
        ticket = _begun(case)
        operation = _terminal(case)
        with session_scope(case.factory) as session:
            population = _population(case, session)
            session.add(
                RetrievalCachePopulationClaimRecord(
                    owner="independent",
                    source_store=population.source_store,
                    collection_id=case.collection_id,
                    object_id=case.object_id,
                    created_at=utc_timestamp_now(),
                )
            )
        with pytest.raises(Conflict, match="another population owner"):
            _accept(case, ticket, operation)


def test_cache_abandonment_receipt_must_fit_original_reserved_cleanup_capacity(tmp_path):
    with _case(tmp_path) as case:
        ticket = _begun(case, grants=GRANTS)
        operation = _terminal(case)
        with pytest.raises(Conflict, match="reserved byte grant"):
            _accept(case, ticket, operation)
        with session_scope(case.factory) as session:
            assert _population(case, session).state == "admitted"
            assert session.get(CachePopulationBeginRecord, ticket.call_id) is not None


@pytest.mark.parametrize("field", ("generation", "operation_id", "source_store", "receipt_json"))
def test_cache_abandonment_acknowledgement_requires_exact_original_acceptance(tmp_path, field):
    with _case(tmp_path) as case:
        ticket = _begun(case)
        receipt = _accept(case, ticket, _terminal(case))
        changed = replace(
            receipt,
            **{field: "{}" if field == "receipt_json" else "a" * 64},
        )
        with pytest.raises(Conflict):
            case.cache.abandonment.acknowledge(changed)
        with session_scope(case.factory) as session:
            assert session.get(CachePopulationBeginRecord, ticket.call_id) is not None
