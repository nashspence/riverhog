from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_core.cache_write_grants import CacheWriteGrants
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import (
    CollectionArchiveObjectUploadRecord,
    CollectionRecord,
    CollectionUploadRecord,
    RetrievalCachePopulationRecord,
    StorageIncarnationRecord,
)
from riverhog_core.services import cache_population_closure as closure_module
from riverhog_core.services.cache_population_closure import CachePopulationClosure
from riverhog_core.services.cache_population_publication import CachePopulationPublication
from riverhog_core.services.retrieval_cache import (
    SqlAlchemyRetrievalCache,
    _require_unmanaged_population,
)
from riverhog_protocol.errors import Conflict, NotFound

from tests.unit.storage_incarnation_fixtures import seed_storage_incarnation
from tests.unit.test_archive_upload_writes import _open, _stored, _unit
from tests.unit.test_archive_write_execution import _filesystem
from tests.unit.test_cache_population_publication import _accept_archive, _state
from tests.unit.test_collection_upload_preparation import _case
from tests.unit.test_mirrored_archive_begin import _terminal
from tests.unit.test_mirrored_archive_upload_writes import _writing
from tests.unit.test_retrieval_cache_coordinator import _registration
from tests.unit.test_retrieval_cache_write_execution import _candidate


@contextmanager
def _sealed(tmp_path, monkeypatch, kind="pack", *, mode="direct", database_url=None):
    with _writing(tmp_path, monkeypatch, kind, mode=mode, database_url=database_url) as joint:
        opened = _open(joint.actor)
        joint.completed = _unit(joint.actor, opened)
        assert joint.completed.completed is not None
        joint.publisher = CachePopulationPublication(joint.actor.case.factory)
        joint.completion = next(
            effect.ticket for effect in joint.writer._effects.values() if effect.kind == "complete"
        )
        yield joint


def _assert_closure(tmp_path, monkeypatch, kind, mode, *, database_url=None):
    with _sealed(tmp_path, monkeypatch, kind, mode=mode, database_url=database_url) as joint:
        before = _state(joint)
        physical, checkpoint = list(joint.actor.seen), _stored(joint.actor)
        closed = joint.publisher.closure.accept(joint.completion)
        accepted = _state(joint)
        assert accepted["reserved"] == before["reserved"] > 0
        assert accepted["committed"] == 0 and accepted["ready"] == []
        assert accepted["begins"] == before["begins"]
        assert accepted["children"] == before["children"]
        assert accepted["populations"] == [("sealed", closed.generation)]
        with session_scope(joint.actor.case.factory) as session:
            source = session.get(
                CollectionArchiveObjectUploadRecord,
                (closed.collection_id, closed.object_id),
            )
            population = session.get(
                RetrievalCachePopulationRecord,
                (closed.source_store, closed.collection_id, closed.object_id),
            )
            assert (
                source.cache_receipt_json == population.closed_receipt_json == closed.receipt_json
            )
            assert session.get(CollectionRecord, closed.collection_id) is None
        with pytest.raises(Conflict, match="accepted original archive"):
            joint.publisher.publish(closed)
        assert _state(joint) == accepted
        joint.publisher.closure.acknowledge(closed)
        after = _state(joint)
        assert after["begins"] == after["children"] == []
        assert after["reserved"] == before["reserved"] and after["committed"] == 0
        assert after["generation"] == before["generation"]
        assert after["populations"] == accepted["populations"] and after["ready"] == []
        assert joint.publisher.closure.accept(joint.completion) == closed
        assert (
            joint.publisher.closure.load(
                closed.source_store, closed.collection_id, closed.object_id
            )
            == closed
        )
        assert joint.actor.case.service._retrieval_cache.release(owner="joint-cache") == 0
        with session_scope(joint.actor.case.factory) as session:
            with pytest.raises(Conflict, match="retains admitted"):
                _require_unmanaged_population(
                    session, (closed.source_store, closed.collection_id, closed.object_id)
                )
        assert _state(joint) == after
        assert joint.actor.seen == physical and _stored(joint.actor) == checkpoint


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("mode", ("direct", "owned-http"))
def test_actual_closed_receipt_enters_independent_sealed_owner_before_archive_visibility(
    tmp_path, monkeypatch, kind, mode
):
    _assert_closure(tmp_path, monkeypatch, kind, mode)


def _assert_transfer_loss(tmp_path, monkeypatch, committed, *, phase="accept", database_url=None):
    with _sealed(tmp_path, monkeypatch, database_url=database_url) as joint:
        owner = joint.publisher.closure
        original = getattr(owner, phase)
        transaction, attempts = closure_module.session_scope, []
        physical, checkpoint = list(joint.actor.seen), _stored(joint.actor)
        before = _state(joint)

        @contextmanager
        def rollback(factory):
            with transaction(factory) as session:
                yield session
                if not committed and len(attempts) == 1:
                    raise OSError("injected closed-receipt reply loss")

        def lost(argument):
            attempts.append(argument)
            result = original(argument)
            if committed and len(attempts) == 1:
                raise OSError("injected closed-receipt reply loss")
            return result

        monkeypatch.setattr(closure_module, "session_scope", rollback)
        monkeypatch.setattr(owner, phase, lost)
        with pytest.raises(OSError, match="reply loss"):
            joint.writer.transfer_cache(joint.publisher)
        held = _state(joint)
        assert held["reserved"] == before["reserved"] and held["committed"] == 0
        if phase == "accept" or not committed:
            assert held["begins"] and held["children"]
        else:
            assert held["begins"] == held["children"] == []
        result = joint.writer.transfer_cache(joint.publisher)
        assert result is not None
        after = _state(joint)
        assert after["begins"] == after["children"] == []
        assert after["reserved"] == before["reserved"] and after["committed"] == 0
        assert after["generation"] == before["generation"]
        assert all(argument is attempts[0] for argument in attempts)
        assert joint.writer.transfer_cache(joint.publisher) is result
        assert joint.actor.seen == physical and _stored(joint.actor) == checkpoint


@pytest.mark.parametrize("committed", (False, True))
@pytest.mark.parametrize("phase", ("accept", "acknowledge"))
def test_closed_receipt_commit_and_reply_loss_retries_original_without_redispatch(
    tmp_path, monkeypatch, committed, phase
):
    _assert_transfer_loss(tmp_path, monkeypatch, committed, phase=phase)


@pytest.mark.parametrize("accepted", (False, True))
def test_cold_report_reader_completes_transfer_without_reconstructing_dispatch(
    tmp_path, monkeypatch, accepted
):
    with _sealed(tmp_path, monkeypatch) as joint:
        owner = joint.publisher.closure
        if accepted:
            owner.accept(joint.completion)
        physical, checkpoint = list(joint.actor.seen), _stored(joint.actor)
        case = joint.actor.case
        # A fresh owner consumes already settled original reports. It has no
        # live ticket, executor callback, provider, or ability to redispatch.
        cold = CachePopulationClosure(case.factory)
        closed = cold.accept_report(case.collection_id, case.object_id, joint.completion.call_id)
        cold.acknowledge(closed)
        assert _state(joint)["begins"] == _state(joint)["children"] == []
        assert cold.load(closed.source_store, closed.collection_id, closed.object_id) == closed
        assert joint.actor.seen == physical and _stored(joint.actor) == checkpoint


@pytest.mark.parametrize("acknowledged", (False, True))
def test_closed_receipt_survives_source_actor_and_parent_retirement(
    tmp_path, monkeypatch, acknowledged
):
    with _sealed(tmp_path, monkeypatch) as joint:
        _accept_archive(joint, joint.completed.completed)
        closed = (
            joint.writer.transfer_cache(joint.publisher)
            if acknowledged
            else joint.publisher.closure.accept(joint.completion)
        )
        physical = list(joint.actor.seen)
        before = _state(joint)
        with session_scope(joint.actor.case.factory) as session:
            session.delete(session.get(CollectionUploadRecord, closed.collection_id))
        cold = CachePopulationPublication(joint.actor.case.factory)
        retained = cold.closure.load(closed.source_store, closed.collection_id, closed.object_id)
        assert retained == closed
        assert _state(joint) == before
        cold.closure.acknowledge(retained)
        assert _state(joint)["begins"] == _state(joint)["children"] == []
        published = cold.publish(retained)
        assert published.receipt.stored_bytes == before["reserved"]
        cold.acknowledge(published)
        assert _state(joint)["reserved"] == 0
        assert _state(joint)["committed"] == before["reserved"]
        assert joint.actor.seen == physical


def test_terminal_selection_keeps_closed_cache_data_with_its_receiving_owner(tmp_path, monkeypatch):
    with _sealed(tmp_path, monkeypatch) as joint:
        _terminal(joint)
        before = _state(joint)
        assert joint.writer.has_pending and joint.mirror.has_pending
        closed = joint.writer.transfer_cache(joint.publisher)
        assert closed is not None
        assert not joint.writer.has_pending and not joint.mirror.has_pending
        selected = _terminal(joint)
        assert joint.writer.drain_terminal(selected) == "closed"
        after = _state(joint)
        assert after["begins"] == after["children"] == []
        assert after["reserved"] == before["reserved"] > 0
        assert after["ready"] == [] and after["committed"] == 0
        assert joint.actor.case.service._retrieval_cache.release(owner="joint-cache") == 0
        assert _state(joint) == after


@pytest.mark.parametrize("changed", ("receipt", "checkpoint", "generation"))
def test_closed_transfer_rejects_changed_evidence_without_freeing_calls(
    tmp_path, monkeypatch, changed
):
    with _sealed(tmp_path, monkeypatch) as joint:
        owner = joint.publisher.closure
        if changed == "checkpoint":
            with session_scope(joint.actor.case.factory) as session:
                source = session.get(
                    CollectionArchiveObjectUploadRecord,
                    (joint.actor.case.collection_id, joint.actor.case.object_id),
                )
                value = json.loads(source.checkpoint_json)
                value["completed"] = None
                source.checkpoint_json = json.dumps(value)
            before = _state(joint)
            with pytest.raises(Conflict, match="different or unacknowledged"):
                owner.accept(joint.completion)
        else:
            closed = owner.accept(joint.completion)
            before = _state(joint)
            if changed == "receipt":
                value = json.loads(closed.receipt_json)
                value["completion_result_sha256"] = "e" * 64
                altered = replace(closed, receipt_json=json.dumps(value))
            else:
                altered = replace(closed, generation="e" * 64)
            with pytest.raises((Conflict, NotFound)):
                owner.acknowledge(altered)
        assert _state(joint) == before and before["begins"] and before["children"]


def _assert_many_volumes(
    tmp_path, monkeypatch, kind, mode, *, database_url=None, budget_absence=False
):
    calls, count = 3, 6
    grants = CacheWriteGrants(
        calls=calls, control_bytes=100000, result_bytes=10000, cleanup_bytes=1000
    )
    case_path = tmp_path / "case"
    case_path.mkdir()

    def configure(service):
        candidate = _candidate(cache_adapter)
        registration = _registration(candidate.name)
        with session_scope(service._session_factory) as session:
            seed_storage_incarnation(session, "cache", candidate.name)
            session.get(
                StorageIncarnationRecord, ("cache", candidate.name)
            ).id = candidate.incarnation_id
        service._retrieval_cache = SqlAlchemyRetrievalCache(
            {candidate.name: candidate},
            {candidate.name: registration},
            session_factory=service._session_factory,
        )

    with (
        _filesystem(tmp_path / "shared-cache") as cache_adapter,
        _case(
            case_path,
            kind,
            database_url=database_url,
            use_cache=True,
            artifact_count=count,
            configure_service=configure,
        ) as base,
    ):
        base.service.complete(base.collection_id)
        volumes = base.service.list_volumes(base.collection_id)["volumes"]
        assert len(volumes) == count > calls
        completed, closed_receipts, budget = [], [], 1000000
        for index, volume in enumerate(volumes):
            case = SimpleNamespace(
                **{
                    **vars(base),
                    "object_id": volume["volume_id"],
                    "plan_sha256": volume["plan_sha256"],
                }
            )
            with _writing(
                tmp_path,
                monkeypatch,
                kind,
                mode=mode,
                existing_case=case,
                archive_root=tmp_path / "shared-archive",
                cache_adapter=cache_adapter,
                grants=grants,
                budget=budget,
            ) as joint:
                opened = _open(joint.actor)
                joint.completed = _unit(joint.actor, opened)
                assert joint.completed.completed is not None
                publisher = CachePopulationPublication(base.factory)
                original = list(joint.actor.seen)
                closed = joint.writer.transfer_cache(publisher)
                state = _state(joint)
                assert state["begins"] == state["children"] == []
                assert state["committed"] == 0 and state["ready"] == []
                assert _stored(joint.actor) == (joint.completed.to_json(), [])
                assert joint.actor.seen == original
                if not budget_absence or index == 0:
                    assert closed is not None
                    closed_receipts.append(closed)
                    completed.append(joint)
                    if budget_absence:
                        budget = state["reserved"]
                else:
                    assert closed is None
                assert len(state["populations"]) == (1 if budget_absence else index + 1)
        assert len(closed_receipts) == (1 if budget_absence else count)
        if budget_absence:
            return
        # The normal accepted archive authority is supplied independently after
        # every original volume has progressed under the same finite grants.
        for joint in completed:
            _accept_archive(joint, joint.completed.completed)
        cold = CachePopulationPublication(base.factory)
        for closed in closed_receipts:
            retained = cold.closure.load(
                closed.source_store, closed.collection_id, closed.object_id
            )
            publication = cold.publish(retained)
            cold.acknowledge(publication)
        final = _state(completed[-1])
        assert final["reserved"] == 0 and final["committed"] > 0
        assert len(final["ready"]) == count and final["populations"] == []


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("mode", ("direct", "owned-http"))
def test_one_collection_progresses_more_volumes_than_fixed_call_grants(
    tmp_path, monkeypatch, kind, mode
):
    _assert_many_volumes(tmp_path, monkeypatch, kind, mode)


def test_fixed_grants_continue_archive_when_only_optional_cache_data_budget_is_full(
    tmp_path, monkeypatch
):
    _assert_many_volumes(tmp_path, monkeypatch, "pack", "direct", budget_absence=True)


def _assert_begin_receiving_loss(tmp_path, monkeypatch, committed, *, database_url=None):
    with _sealed(tmp_path, monkeypatch, database_url=database_url) as joint:
        acknowledge = joint.mirror.acknowledge_transfer
        attempts = []

        def lose(receipt):
            attempts.append(receipt)
            if len(attempts) == 1:
                if committed:
                    acknowledge(receipt)
                raise OSError("injected original begin receiving reply loss")
            return acknowledge(receipt)

        monkeypatch.setattr(joint.mirror, "acknowledge_transfer", lose)
        physical = list(joint.actor.seen)
        with pytest.raises(OSError, match="begin receiving"):
            joint.writer.transfer_cache(joint.publisher)
        assert joint.writer.has_pending and not joint.writer._closure_acknowledged
        selected = _terminal(joint)
        assert joint.writer.drain_terminal(selected) == "pending"
        closed = joint.writer.transfer_cache(joint.publisher)
        assert attempts[0] is attempts[1] and closed is attempts[0]
        assert joint.writer.drain_terminal(selected) == "closed"
        assert not joint.writer.has_pending and not joint.mirror.has_pending
        assert joint.actor.seen == physical
        state = _state(joint)
        assert state["reserved"] > 0 and state["committed"] == 0
        assert state["begins"] == state["children"] == [] and state["populations"]


@pytest.mark.parametrize("committed", (False, True))
def test_original_begin_receiving_acknowledgement_loss_retains_complete_actor(
    tmp_path, monkeypatch, committed
):
    _assert_begin_receiving_loss(tmp_path, monkeypatch, committed)


@pytest.mark.parametrize("changed", ("call", "request", "incarnation", "durability"))
def test_begin_receiving_requires_its_original_call_and_actual_durable_evidence(
    tmp_path, monkeypatch, changed
):
    with _sealed(tmp_path, monkeypatch) as joint:
        closed = joint.publisher.closure.accept(joint.completion)
        if changed == "durability":
            with session_scope(joint.actor.case.factory) as session:
                source = session.get(
                    CollectionArchiveObjectUploadRecord,
                    (closed.collection_id, closed.object_id),
                )
                receiver = session.get(
                    RetrievalCachePopulationRecord,
                    (closed.source_store, closed.collection_id, closed.object_id),
                )
                source.cache_receipt_json = receiver.closed_receipt_json = None
                receiver.state = "admitted"
        else:
            value = json.loads(closed.receipt_json)
            key = {
                "call": "begin_call_id",
                "request": "begin_request_sha256",
                "incarnation": "cache_incarnation_id",
            }[changed]
            value[key] = "f" * 64
            closed = replace(closed, receipt_json=json.dumps(value))
        with pytest.raises(Conflict, match="another original call|not durable"):
            joint.mirror.acknowledge_transfer(closed)
        assert joint.mirror.has_pending and joint.writer.has_pending


def test_original_begin_receiving_replay_rejects_changed_receipt_scope(tmp_path, monkeypatch):
    with _sealed(tmp_path, monkeypatch) as joint:
        closed = joint.writer.transfer_cache(joint.publisher)
        assert closed is not None and not joint.mirror.has_pending
        joint.mirror.acknowledge_transfer(closed)
        with pytest.raises(Conflict, match="receiving acknowledgement changed"):
            joint.mirror.acknowledge_transfer(
                replace(closed, collection_id=closed.collection_id + 1)
            )
        assert not joint.mirror.has_pending
