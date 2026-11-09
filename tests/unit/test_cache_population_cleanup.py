from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_core.archive_cleanup_grants import ArchiveCleanupGrants
from riverhog_core.catalog_archive_write_models import ArchiveUploadTerminalRecord as Terminal
from riverhog_core.catalog_archive_write_models import TerminalCleanupCapacityRecord as Capacity
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import (
    CollectionUploadRecord,
    RetrievalCachePopulationClaimRecord,
    RetrievalCacheStoreAccountingRecord,
)
from riverhog_core.catalog_object_cleanup_models import ObjectCleanupAttemptRecord as Attempt
from riverhog_core.services.archive_upload_cleanup import ArchiveUploadCleanup
from riverhog_core.services.cache_population_writes import CachePopulationWriteAdmission
from riverhog_protocol.errors import Conflict, NotFound
from riverhog_storage_adapter_protocol import (
    CleanupInventoryProgress,
    CleanupInventoryRequest,
    CleanupInventoryStepRequest,
    CleanupProgress,
    CleanupStepRequest,
    SmallObjectWriteRequest,
    WriteSession,
)
from sqlalchemy import func, select

from tests.unit.test_cache_population_abandonment import (
    HANDOFF_GRANTS,
    _accept,
    _begun,
    _population,
)
from tests.unit.test_cache_population_begin import _case, _terminal
from tests.unit.test_cache_population_writes import _part
from tests.unit.test_collection_uploads import _CREATOR

GRANTS = ArchiveCleanupGrants(4, 65536, 16, 8)


def _guard(session):
    # Fixture-owned cache disposal permission survives source upload retirement.
    session.execute(select(1))


@contextmanager
def _abandoned(
    tmp_path,
    *,
    mode="direct",
    database_url=None,
    acknowledged=True,
    grants=GRANTS,
    handoff_grants=HANDOFF_GRANTS,
):
    with _case(tmp_path, mode=mode, database_url=database_url) as case:
        begin = _begun(case, grants=handoff_grants)
        with session_scope(case.factory) as session:
            population = _population(case, session)
            cache_session = WriteSession(
                object_path=population.object_path,
                write_token=population.write_token,
                expected_bytes=3,
            )
        writes = CachePopulationWriteAdmission(case.factory)
        part = writes.arm(
            case.collection_id,
            case.object_id,
            begin_call_id=begin.call_id,
            operation=_part(SimpleNamespace(session=cache_session)),
            owner="upload-cache",
            principal=_CREATOR,
            epoch=case.epoch,
            executor_id="d" * 64,
            candidate=case.candidate,
            grants=handoff_grants,
            guard=case.guard,
            validate_population=case.validate_population,
        )
        response = writes.execute(part, candidate=case.candidate, content=b"abc")
        assert response.execution == "closed"
        writes.record_response(part, response)
        writes.acknowledge_report(part)
        operation = _terminal(case)
        receipt = _accept(case, begin, operation)
        if acknowledged:
            case.cache.abandonment.acknowledge(receipt)
        owner, target = case.cache.native_population_cleanup(receipt, grants=grants)
        yield SimpleNamespace(
            case=case,
            begin=begin,
            receipt=receipt,
            operation=operation,
            owner=owner,
            target=target,
            value=json.loads(receipt.receipt_json),
        )


def _inventory_request(ready):
    return CleanupInventoryStepRequest(
        selected_terminal_sha256=ready.operation,
        inventory=CleanupInventoryRequest(
            object_prefix=ready.value["object_path"],
            target_incarnation=ready.value["cache_incarnation_id"],
            maximum_items=4,
            maximum_response_bytes=4096,
        ),
        maximum_provider_requests=2,
        maximum_control_bytes=8192,
        maximum_in_flight=1,
    )


def _inventory_arm(ready, *, guard=_guard, request=None):
    return ready.owner.arm_inventory(
        ready.case.collection_id,
        ready.operation,
        request=request or _inventory_request(ready),
        after=None,
        dispatch_owner="c" * 64,
        guard=guard,
    )


def _receive(ready, response, *, acknowledge=True):
    ready.owner.record_response(ready.case.collection_id, ready.operation, response)
    if acknowledge:
        ready.owner.acknowledge_response(ready.case.collection_id, ready.operation, response)


def _observe(ready):
    read = _inventory_arm(ready)
    response = ready.owner.execute(read, ready.target)
    assert response.execution == "closed" and isinstance(response.result, CleanupInventoryProgress)
    _receive(ready, response, acknowledge=False)
    return read, response


def _select(ready):
    read, response = _observe(ready)
    page = ready.owner.select_page(
        ready.case.collection_id,
        ready.operation,
        inventory_call_id=read.invocation.call.call_id,
        inventory_request=read.invocation.request.inventory,
        request=CleanupStepRequest(
            selected_terminal_sha256=ready.operation,
            inventory=response.result.inventory,
            maximum_provider_requests=3,
            maximum_control_bytes=8192,
            maximum_in_flight=1,
        ),
        after=None,
        guard=_guard,
    )
    assert page.request.inventory.exhausted and page.request.inventory.candidates
    ready.owner.acknowledge_response(ready.case.collection_id, ready.operation, response)
    return page


def _arm_dispose(ready, page):
    return ready.owner.arm(
        ready.case.collection_id,
        ready.operation,
        page_sha256=page.request_sha256,
        dispatch_owner="c" * 64,
        maximum_provider_requests=3,
        maximum_control_bytes=8192,
        maximum_in_flight=1,
        guard=_guard,
    )


def _retained_data(ready):
    with session_scope(ready.case.factory) as session:
        assert _population(ready.case, session).state == "abandoning"
        assert (
            session.get(
                RetrievalCacheStoreAccountingRecord, ready.case.candidate.name
            ).reserved_bytes
            == 3
        )


def _put(ready, path):
    ready.case.adapter.put_small_object(
        SmallObjectWriteRequest(
            object_path=path,
            stored_bytes=1,
            stored_sha256=hashlib.sha256(b"x").hexdigest(),
            content_type="application/octet-stream",
            required_identity_assertions={},
            placement_policy="immediate_default",
            mode="create_only",
        ),
        b"x",
    )


def _assert_cleanup(tmp_path, monkeypatch, mode, *, database_url=None, retire_source=False):
    with _abandoned(tmp_path, mode=mode, database_url=database_url) as ready:
        case = ready.case
        if retire_source:
            with session_scope(case.factory) as session:
                session.delete(session.get(CollectionUploadRecord, case.collection_id))
        _put(ready, "unrelated/keep")
        execute = ready.target.execute_cleanup
        physical = []

        def capture(invocation):
            with session_scope(case.factory) as session:
                attempt = session.get(Attempt, invocation.call.call_id)
                assert attempt.unresolved and attempt.scope_id == ready.owner._scope_id(
                    ready.operation
                )
                assert session.get(Capacity, ready.operation).calls == "1"
                # An independent catalog writer is available during actual provider work.
                _population(case, session).failure = "provider-outside-catalog-transaction"
            physical.append(invocation)
            return execute(invocation)

        monkeypatch.setattr(ready.target, "execute_cleanup", capture)
        assert list((tmp_path / "cache" / "writes").iterdir())
        page = _select(ready)
        assert {item.object_path for item in page.request.inventory.candidates} == {
            ready.value["object_path"]
        }
        with session_scope(case.factory) as session:
            assert session.get(Terminal, ready.operation).phase == "draining"
        ticket = _arm_dispose(ready, page)
        response = ready.owner.execute(ticket, ready.target)
        assert response.execution == "closed" and isinstance(response.result, CleanupProgress)
        assert all(item.outcome == "disposed" for item in response.result.items)
        _receive(ready, response)
        assert ready.owner.current(case.collection_id, ready.operation).complete
        assert not list((tmp_path / "cache" / "writes").iterdir())
        assert len(physical) == 2
        assert (
            len(
                case.adapter.list_cleanup_candidates(
                    CleanupInventoryRequest(
                        object_prefix="unrelated/",
                        target_incarnation=ready.value["cache_incarnation_id"],
                    )
                ).candidates
            )
            == 1
        )
        _retained_data(ready)
        with session_scope(case.factory) as session:
            assert session.get(Terminal, ready.operation).phase == "draining"
            assert session.scalar(select(func.count()).select_from(Attempt)) == 0


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("retire_source", (False, True))
def test_cache_cleanup_uses_own_exact_scope_and_keeps_data_owned(
    tmp_path, monkeypatch, mode, retire_source
):
    _assert_cleanup(tmp_path, monkeypatch, mode, retire_source=retire_source)


def _assert_lost_reply(tmp_path, *, database_url=None):
    with _abandoned(tmp_path, database_url=database_url) as ready:
        page = _select(ready)
        first = _arm_dispose(ready, page)
        original = ready.owner.execute(first, ready.target)
        assert original.execution == "closed"
        second = _arm_dispose(ready, page)
        later = ready.owner.execute(second, ready.target)
        _receive(ready, later)
        assert ready.owner.current(ready.case.collection_id, ready.operation).complete
        with session_scope(ready.case.factory) as session:
            assert session.get(Attempt, first.invocation.call.call_id).unresolved
            assert session.get(Capacity, ready.operation).calls == "1"
        # Deliver the genuinely captured first response; later progress did not close it.
        _receive(ready, original)
        with session_scope(ready.case.factory) as session:
            assert session.get(Attempt, first.invocation.call.call_id) is None
            assert session.get(Capacity, ready.operation).calls == "0"
        _retained_data(ready)


def test_later_cache_cleanup_progress_does_not_release_an_unreceived_original(tmp_path):
    _assert_lost_reply(tmp_path)


def test_cache_cleanup_refuses_forward_receiving_until_handoff_acknowledgment(tmp_path):
    with _abandoned(tmp_path, acknowledged=False) as ready:
        with pytest.raises(Conflict, match="claimant ownership|forward receiving"):
            _inventory_arm(ready)
        ready.case.cache.abandonment.acknowledge(ready.receipt)
        assert _inventory_arm(ready).invocation.request == _inventory_request(ready)


@pytest.mark.parametrize("mutation", ("claim", "size", "incarnation"))
def test_cache_cleanup_rechecks_current_population_ownership(tmp_path, mutation):
    with _abandoned(tmp_path) as ready:
        with session_scope(ready.case.factory) as session:
            if mutation == "claim":
                from time_formats import utc_timestamp_now

                now = utc_timestamp_now()
                session.add(
                    RetrievalCachePopulationClaimRecord(
                        owner="other",
                        source_store=ready.receipt.source_store,
                        collection_id=ready.case.collection_id,
                        object_id=ready.case.object_id,
                        created_at=now,
                    )
                )
            elif mutation == "size":
                _population(ready.case, session).expected_bytes = 4
            else:
                from tests.unit.storage_incarnation_fixtures import seed_storage_incarnation

                population = _population(ready.case, session)
                population.cache_incarnation_id = seed_storage_incarnation(
                    session, "cache", "replacement-cache"
                )
                population.cache_store = "replacement-cache"
        with pytest.raises(Conflict):
            _inventory_arm(ready)
        with session_scope(ready.case.factory) as session:
            assert session.scalar(select(func.count()).select_from(Attempt)) == 0
        _retained_data(ready)


def test_cache_cleanup_requires_current_disposal_guard_and_original_target(tmp_path):
    with _abandoned(tmp_path) as ready:

        def revoked(session):
            _guard(session)
            raise Conflict("current disposal permission revoked")

        with pytest.raises(Conflict, match="permission revoked"):
            _inventory_arm(ready, guard=revoked)
        original = _inventory_request(ready)
        for key, value in (
            ("object_prefix", "another/"),
            ("target_incarnation", "11111111-1111-4111-8111-111111111111"),
        ):
            request = original.model_copy(
                update={"inventory": original.inventory.model_copy(update={key: value})}
            )
            with pytest.raises(Conflict, match="original target scope"):
                _inventory_arm(ready, request=request)
        with session_scope(ready.case.factory) as session:
            assert session.scalar(select(func.count()).select_from(Attempt)) == 0


def test_cache_cleanup_never_disposes_a_prefix_neighbor(tmp_path):
    with _abandoned(tmp_path) as ready:
        neighbor = ready.value["object_path"] + "-neighbor"
        _put(ready, neighbor)
        read, response = _observe(ready)
        assert neighbor in {item.object_path for item in response.result.inventory.candidates}
        request = CleanupStepRequest(
            selected_terminal_sha256=ready.operation,
            inventory=response.result.inventory,
            maximum_provider_requests=3,
            maximum_control_bytes=8192,
            maximum_in_flight=1,
        )
        with pytest.raises(Conflict, match="original target scope"):
            ready.owner.select_page(
                ready.case.collection_id,
                ready.operation,
                inventory_call_id=read.invocation.call.call_id,
                inventory_request=read.invocation.request.inventory,
                request=request,
                after=None,
                guard=_guard,
            )
        ready.owner.acknowledge_response(ready.case.collection_id, ready.operation, response)
        assert (
            len(
                ready.case.adapter.list_cleanup_candidates(
                    CleanupInventoryRequest(
                        object_prefix=neighbor,
                        target_incarnation=ready.value["cache_incarnation_id"],
                    )
                ).candidates
            )
            == 1
        )
        assert list((tmp_path / "cache" / "writes").iterdir())
        _retained_data(ready)


def _archive_scope(ready, grants=GRANTS):
    with session_scope(ready.case.factory) as session:
        # Fixture-owned archive adjudication; cache scope never selects this decision.
        session.get(Terminal, ready.operation).phase = "cleanup-ready"
    owner = ArchiveUploadCleanup(ready.case.factory, grants=grants)
    with session_scope(ready.case.factory) as session:
        scope = owner._context(session, ready.case.collection_id, ready.operation)
    request = _inventory_request(ready)
    return owner, request.model_copy(
        update={
            "inventory": request.inventory.model_copy(
                update={
                    "object_prefix": scope.object_prefix,
                    "target_incarnation": scope.target_incarnation,
                }
            )
        }
    )


@pytest.mark.parametrize("first", ("cache", "archive"))
def test_archive_and_cache_scopes_share_terminal_capacity_without_lending_permission(
    tmp_path, first
):
    grants = replace(GRANTS, calls=1)
    with _abandoned(tmp_path, grants=grants) as ready:
        archive = ArchiveUploadCleanup(ready.case.factory, grants=grants)
        with pytest.raises(Conflict, match="cleanup-ready"):
            archive.arm_inventory(
                ready.case.collection_id,
                ready.operation,
                request=_inventory_request(ready),
                after=None,
                dispatch_owner="a" * 64,
                guard=_guard,
            )
        archive, request = _archive_scope(ready, grants)

        def arm_archive():
            return archive.arm_inventory(
                ready.case.collection_id,
                ready.operation,
                request=request,
                after=None,
                dispatch_owner="a" * 64,
                guard=_guard,
            )

        arms = (
            (_inventory_arm, lambda unused: arm_archive())
            if first == "cache"
            else (
                lambda unused: arm_archive(),
                _inventory_arm,
            )
        )
        original = arms[0](ready)
        with pytest.raises(Conflict, match="retention grant"):
            arms[1](ready)
        with session_scope(ready.case.factory) as session:
            row = session.get(Attempt, original.invocation.call.call_id)
            assert row.scope_id == (
                ready.owner._scope_id(ready.operation) if first == "cache" else ready.operation
            )
            assert session.get(Capacity, ready.operation).calls == "1"
            assert session.scalar(select(func.count()).select_from(Attempt)) == 1


def test_cache_page_is_independent_of_archive_page_for_the_same_terminal(tmp_path):
    with _abandoned(tmp_path) as ready:
        page = _select(ready)
        archive, _ = _archive_scope(ready)
        with pytest.raises(NotFound):
            archive.current(ready.case.collection_id, ready.operation)
        assert ready.owner.current(ready.case.collection_id, ready.operation) == page
