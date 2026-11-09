from __future__ import annotations

import json
from contextlib import contextmanager

import pytest
from riverhog_core.catalog_cache_write_models import CachePopulationDispositionRecord as Disposition
from riverhog_core.catalog_db import session_scope, validate_db
from riverhog_core.catalog_models import (
    CollectionUploadRecord,
    RetrievalCachePopulationClaimRecord,
    RetrievalCachePopulationRecord,
    RetrievalCacheStoreAccountingRecord,
)
from riverhog_core.catalog_object_cleanup_models import ObjectCleanupAttemptRecord as Attempt
from riverhog_core.catalog_object_cleanup_models import ObjectCleanupPageRecord as Page
from riverhog_core.services import cache_population_cleanup as cleanup_module
from riverhog_core.services.cache_population_abandonment import CachePopulationAbandonment
from riverhog_core.services.cache_population_cleanup import CachePopulationCleanup
from riverhog_protocol.errors import Conflict, NotFound
from riverhog_storage_adapter_protocol import CleanupStepRequest
from sqlalchemy import func, select
from time_formats import utc_timestamp_now

from tests.unit.test_cache_population_abandonment import _population
from tests.unit.test_cache_population_cleanup import (
    GRANTS,
    _abandoned,
    _arm_dispose,
    _guard,
    _inventory_arm,
    _inventory_request,
    _put,
    _receive,
    _retained_data,
    _select,
)


def _complete(ready):
    page = _select(ready)
    ticket = _arm_dispose(ready, page)
    result = ready.owner.execute(ticket, ready.target)
    assert result.execution == "closed"
    _receive(ready, result)
    assert ready.owner.current(ready.case.collection_id, ready.operation).complete
    return page


def _assert_disposition(tmp_path, mode, *, database_url=None, retire_source=False):
    with _abandoned(tmp_path, mode=mode, database_url=database_url) as ready:
        if retire_source:
            with session_scope(ready.case.factory) as session:
                session.delete(session.get(CollectionUploadRecord, ready.case.collection_id))
        with pytest.raises(NotFound):
            ready.owner.outcome()
        page = _complete(ready)
        _retained_data(ready)
        receipt = ready.owner.finish(guard=_guard)
        assert receipt.abandoned == ready.receipt
        payload = json.loads(receipt.outcome_json)
        assert payload["state"] == "disposed" and payload["data_bytes"] == 3
        assert payload["last_page_sha256"] == page.request_sha256
        cold = CachePopulationCleanup(ready.case.factory, ready.receipt, grants=GRANTS)
        assert cold.outcome() == cold.finish(guard=_guard) == receipt
        handoff = CachePopulationAbandonment(ready.case.factory)
        assert (
            handoff.load(
                ready.receipt.source_store,
                ready.case.collection_id,
                ready.case.object_id,
                operation_id=ready.operation,
            )
            == ready.receipt
        )
        handoff.acknowledge(ready.receipt)
        with session_scope(ready.case.factory) as session:
            assert _population(ready.case, session) is None
            assert session.get(Page, ready.owner._scope_id(ready.operation)) is None
            assert session.scalar(select(func.count()).select_from(Attempt)) == 0
            outcome = session.get(Disposition, ready.owner._scope_id(ready.operation))
            assert outcome.receipt_json == ready.receipt.receipt_json
            assert outcome.outcome_json == receipt.outcome_json
            assert len(cold._disposition_bytes(outcome)) <= ready.value["capacity_bytes"]
            accounting = session.get(RetrievalCacheStoreAccountingRecord, ready.case.candidate.name)
            assert accounting.reserved_bytes == 0 and accounting.committed_bytes == 0
        assert validate_db(ready.case.config.database_url).condition == "current"
        # Finished original scopes cannot grant a new physical mutation.
        with pytest.raises(Conflict, match="current exact abandoned owner"):
            _inventory_arm(ready)


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("retire_source", (False, True))
def test_positive_cache_disposition_retains_exact_outcome_after_source_and_population_retirement(
    tmp_path, mode, retire_source
):
    _assert_disposition(tmp_path, mode, retire_source=retire_source)


def _assert_receiving_blocks(ready, recorded):
    page = _select(ready)
    first = _arm_dispose(ready, page)
    original = ready.owner.execute(first, ready.target)
    second = _arm_dispose(ready, page)
    _receive(ready, ready.owner.execute(second, ready.target))
    if recorded:
        _receive(ready, original, acknowledge=False)
    assert ready.owner.current(ready.case.collection_id, ready.operation).complete
    with pytest.raises(Conflict, match="original cleanup receiving"):
        ready.owner.finish(guard=_guard)
    _retained_data(ready)
    with session_scope(ready.case.factory) as session:
        assert session.get(Attempt, first.invocation.call.call_id) is not None
        assert session.get(Disposition, ready.owner._scope_id(ready.operation)) is None
    _receive(ready, original)
    return ready.owner.finish(guard=_guard)


@pytest.mark.parametrize("recorded", (False, True))
def test_later_cleanup_and_closed_but_unacknowledged_original_cannot_release_data(
    tmp_path, recorded
):
    with _abandoned(tmp_path) as ready:
        _assert_receiving_blocks(ready, recorded)


def test_missing_or_incomplete_cache_cleanup_proof_cannot_release_data(tmp_path):
    with _abandoned(tmp_path) as ready:
        with pytest.raises(Conflict, match="no retained cleanup proof"):
            ready.owner.finish(guard=_guard)
        page = _select(ready)
        with pytest.raises(Conflict, match="proof is incomplete"):
            ready.owner.finish(guard=_guard)
        ticket = _arm_dispose(ready, page)
        result = ready.owner.execute(ticket, ready.target)
        _receive(ready, result, acknowledge=False)
        with pytest.raises(Conflict, match="original cleanup receiving"):
            ready.owner.finish(guard=_guard)
        _retained_data(ready)


def test_completed_but_nonexhausted_cache_page_cannot_release_data(tmp_path):
    with _abandoned(tmp_path) as ready:
        # Two actual entries share the exact leaf: one completed revision and one session.
        _put(ready, ready.value["object_path"])
        request = _inventory_request(ready)
        request = request.model_copy(
            update={"inventory": request.inventory.model_copy(update={"maximum_items": 1})}
        )
        read = _inventory_arm(ready, request=request)
        observed = ready.owner.execute(read, ready.target)
        assert observed.execution == "closed" and not observed.result.inventory.exhausted
        _receive(ready, observed, acknowledge=False)
        page = ready.owner.select_page(
            ready.case.collection_id,
            ready.operation,
            inventory_call_id=read.invocation.call.call_id,
            inventory_request=request.inventory,
            request=CleanupStepRequest(
                selected_terminal_sha256=ready.operation,
                inventory=observed.result.inventory,
                maximum_provider_requests=3,
                maximum_control_bytes=8192,
                maximum_in_flight=1,
            ),
            after=None,
            guard=_guard,
        )
        ready.owner.acknowledge_response(
            ready.owner.record_response(ready.case.collection_id, ready.operation, observed)
        )
        ticket = _arm_dispose(ready, page)
        _receive(ready, ready.owner.execute(ticket, ready.target))
        assert ready.owner.current(ready.case.collection_id, ready.operation).complete
        with pytest.raises(Conflict, match="proof is incomplete"):
            ready.owner.finish(guard=_guard)
        _retained_data(ready)


@pytest.mark.parametrize("mutation", ("claim", "size", "target", "permission"))
def test_cache_disposition_rechecks_current_owner_and_disposal_permission(tmp_path, mutation):
    with _abandoned(tmp_path) as ready:
        _complete(ready)
        with session_scope(ready.case.factory) as session:
            if mutation == "claim":
                session.add(
                    RetrievalCachePopulationClaimRecord(
                        owner="other",
                        source_store=ready.receipt.source_store,
                        collection_id=ready.case.collection_id,
                        object_id=ready.case.object_id,
                        created_at=utc_timestamp_now(),
                    )
                )
            elif mutation == "size":
                _population(ready.case, session).expected_bytes = 4
            elif mutation == "target":
                from tests.unit.storage_incarnation_fixtures import seed_storage_incarnation

                population = _population(ready.case, session)
                population.cache_incarnation_id = seed_storage_incarnation(
                    session, "cache", "replacement-cache"
                )
                population.cache_store = "replacement-cache"

        def guard(session):
            _guard(session)
            if mutation == "permission":
                raise Conflict("disposal permission revoked")

        with pytest.raises(Conflict):
            ready.owner.finish(guard=guard)
        _retained_data(ready)
        with session_scope(ready.case.factory) as session:
            assert session.get(Disposition, ready.owner._scope_id(ready.operation)) is None


@pytest.mark.parametrize("committed", (False, True))
def test_cache_disposition_commit_and_reply_loss_release_data_exactly_once(
    tmp_path, monkeypatch, committed
):
    with _abandoned(tmp_path) as ready:
        _complete(ready)
        transaction = cleanup_module.session_scope
        attempts = []

        @contextmanager
        def lose_reply(factory):
            with transaction(factory) as session:
                yield session
                if not committed and len(attempts) == 1:
                    raise OSError("injected disposition reply loss")

        monkeypatch.setattr(cleanup_module, "session_scope", lose_reply)

        def finish():
            attempts.append(True)
            outcome = ready.owner.finish(guard=_guard)
            if committed and len(attempts) == 1:
                raise OSError("injected disposition reply loss")
            return outcome

        with pytest.raises(OSError, match="reply loss"):
            finish()
        with session_scope(ready.case.factory) as session:
            assert (_population(ready.case, session) is None) is committed
            assert session.get(
                RetrievalCacheStoreAccountingRecord, ready.case.candidate.name
            ).reserved_bytes == (0 if committed else 3)
        result = finish()
        assert ready.owner.outcome() == result
        with session_scope(ready.case.factory) as session:
            assert session.scalar(select(func.count()).select_from(Disposition)) == 1
            assert (
                session.get(
                    RetrievalCacheStoreAccountingRecord, ready.case.candidate.name
                ).reserved_bytes
                == 0
            )


def test_cache_disposition_original_outcome_cannot_release_a_new_generation(tmp_path):
    with _abandoned(tmp_path) as ready:
        with session_scope(ready.case.factory) as session:
            original = _population(ready.case, session)
            replacement = {
                column.name: getattr(original, column.name)
                for column in RetrievalCachePopulationRecord.__table__.columns
            }
        _complete(ready)
        outcome = ready.owner.finish(guard=_guard)
        with session_scope(ready.case.factory) as session:
            session.delete(session.get(CollectionUploadRecord, ready.case.collection_id))
            # An independent receiver owns the next generation; the old scope has no rights.
            replacement.update(
                generation="f" * 64, begin_call_id="e" * 64, abandonment_receipt_json=None
            )
            session.add(RetrievalCachePopulationRecord(**replacement))
            session.get(
                RetrievalCacheStoreAccountingRecord, ready.case.candidate.name
            ).reserved_bytes += 3
        assert ready.owner.outcome() == ready.owner.finish(guard=_guard) == outcome
        handoff = CachePopulationAbandonment(ready.case.factory)
        assert (
            handoff.load(
                ready.receipt.source_store,
                ready.case.collection_id,
                ready.case.object_id,
                operation_id=ready.operation,
            )
            == ready.receipt
        )
        handoff.acknowledge(ready.receipt)
        with pytest.raises(Conflict, match="current exact abandoned owner"):
            _inventory_arm(ready)
        with session_scope(ready.case.factory) as session:
            assert _population(ready.case, session).generation == "f" * 64
            assert (
                session.get(
                    RetrievalCacheStoreAccountingRecord, ready.case.candidate.name
                ).reserved_bytes
                == 3
            )


def test_cache_disposition_releases_only_its_own_data_and_page_contribution(tmp_path):
    from riverhog_core.catalog_archive_write_models import TerminalCleanupCapacityRecord as Capacity

    from tests.unit.test_cache_population_cleanup import _archive_scope

    with _abandoned(tmp_path) as ready:
        _complete(ready)
        archive, request = _archive_scope(ready)
        retained = archive.arm_inventory(
            ready.case.collection_id,
            ready.operation,
            request=request,
            after=None,
            dispatch_owner="a" * 64,
            guard=_guard,
        )
        with session_scope(ready.case.factory) as session:
            session.get(
                RetrievalCacheStoreAccountingRecord, ready.case.candidate.name
            ).reserved_bytes += 7  # Separate fixture-owned data reservation.
            before = session.get(Capacity, ready.operation).control_bytes
        ready.owner.finish(guard=_guard)
        with session_scope(ready.case.factory) as session:
            capacity = session.get(Capacity, ready.operation)
            assert capacity.calls == "1" and int(capacity.control_bytes) < int(before)
            assert session.get(Attempt, retained.invocation.call.call_id).unresolved
            assert (
                session.get(
                    RetrievalCacheStoreAccountingRecord, ready.case.candidate.name
                ).reserved_bytes
                == 7
            )


def test_cache_disposition_refuses_an_outcome_that_exceeds_original_receiving_grant(tmp_path):
    from dataclasses import replace

    from tests.unit.test_cache_population_abandonment import HANDOFF_GRANTS

    (tmp_path / "measure").mkdir()
    (tmp_path / "bounded").mkdir()
    with _abandoned(tmp_path / "measure") as measured:
        capacity = len(measured.receipt.receipt_json.encode()) + 128
    with _abandoned(
        tmp_path / "bounded", handoff_grants=replace(HANDOFF_GRANTS, cleanup_bytes=capacity)
    ) as ready:
        assert len(ready.receipt.receipt_json.encode()) <= capacity
        _complete(ready)
        with pytest.raises(Conflict, match="reserved receiving byte grant"):
            ready.owner.finish(guard=_guard)
        _retained_data(ready)
        with session_scope(ready.case.factory) as session:
            assert session.get(Page, ready.owner._scope_id(ready.operation)) is not None
            assert session.get(Disposition, ready.owner._scope_id(ready.operation)) is None
