from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import replace

import pytest
from riverhog_core.catalog_cache_write_models import (
    CachePopulationBeginRecord,
    CachePopulationWriteEffectRecord,
)
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import (
    CollectionArchiveCopyRecord,
    CollectionArchiveObjectRecord,
    CollectionRecord,
    RetrievalCacheObjectRecord,
    RetrievalCachePopulationRecord,
    RetrievalCacheStoreAccountingRecord,
    StorageIncarnationRecord,
)
from riverhog_core.services import cache_population_publication as publication_module
from riverhog_core.services.cache_population_publication import CachePopulationPublication
from riverhog_protocol.errors import Conflict
from sqlalchemy import select

from tests.unit.test_archive_upload_writes import _open, _stored, _unit
from tests.unit.test_mirrored_archive_upload_writes import _writing


def _accept_archive(joint, completed):
    case = joint.actor.case
    with session_scope(case.factory) as session:
        begin = session.get(CachePopulationBeginRecord, joint.mirror.cache_begin_call_id)
        original = json.loads(begin.request_json)
        construction = original["construction"]
        collection = session.get(CollectionRecord, case.collection_id)
        if collection is None:
            collection = CollectionRecord(
                id=case.collection_id,
                creation_idempotency_key=construction["idempotency_key"],
                creation_identity_sha256=construction["creation_identity"],
                creation_custody_mode=construction["custody_mode"],
                archive_generation=construction["archive_generation"],
                delivery_context_id=construction["delivery_context_id"],
                artifact_set_identity="1" * 64,
                encryption_format=construction["encryption_format"],
                passphrase_id=construction["passphrase_id"],
                provenance_identity="2" * 64,
                inventory_identity="3" * 64,
                created_by_principal_id=construction["principal_id"],
                created_at=completed.completed_at,
                artifact_count=1,
                artifact_bytes=len(case.content),
            )
            session.add(collection)
            session.flush()
        collection.is_published = True
        collection.archive_root_sha256 = "4" * 64
        copy = session.get(
            CollectionArchiveCopyRecord, (case.collection_id, construction["archive_store"])
        )
        if copy is None:
            copy = CollectionArchiveCopyRecord(
                collection_id=case.collection_id,
                store=construction["archive_store"],
                incarnation_id=construction["archive_incarnation_id"],
                archive_storage_prefix=construction["archive_storage_prefix"],
            )
            session.add(copy)
            session.flush()
        copy.state = "uploaded"
        copy.last_uploaded_at = copy.last_verified_at = completed.completed_at
        session.add(
            CollectionArchiveObjectRecord(
                collection_id=case.collection_id,
                store=construction["archive_store"],
                object_id=case.object_id,
                object_order=0,
                kind=joint.actor.kind,
                object_path=joint.completed.object_path,
                plaintext_bytes=len(case.content),
                stored_bytes=completed.bytes,
                plan_sha256=case.plan_sha256,
                revision=completed.revision,
                uploaded_at=completed.completed_at,
                verified_at=completed.completed_at,
            )
        )


@contextmanager
def _completed(tmp_path, monkeypatch, kind="pack", *, mode="direct", database_url=None):
    with _writing(tmp_path, monkeypatch, kind, mode=mode, database_url=database_url) as joint:
        opened = _open(joint.actor)
        joint.completed = _unit(joint.actor, opened)
        assert joint.completed.completed is not None
        _accept_archive(joint, joint.completed.completed)
        joint.publisher = CachePopulationPublication(joint.actor.case.factory)
        joint.completion = next(
            effect.ticket for effect in joint.writer._effects.values() if effect.kind == "complete"
        )
        yield joint


def _state(joint):
    with session_scope(joint.actor.case.factory) as session:
        accounting = session.get(RetrievalCacheStoreAccountingRecord, joint.candidate.name)
        return {
            "reserved": accounting.reserved_bytes,
            "committed": accounting.committed_bytes,
            "generation": accounting.generation,
            "begins": list(session.scalars(select(CachePopulationBeginRecord.id))),
            "children": list(session.scalars(select(CachePopulationWriteEffectRecord.id))),
            "populations": list(
                session.execute(
                    select(
                        RetrievalCachePopulationRecord.state,
                        RetrievalCachePopulationRecord.generation,
                    )
                )
            ),
            "ready": list(
                session.execute(
                    select(
                        RetrievalCacheObjectRecord.object_path,
                        RetrievalCacheObjectRecord.stored_bytes,
                    )
                )
            ),
        }


def _assert_publication(tmp_path, monkeypatch, kind, mode, *, database_url=None):
    with _completed(tmp_path, monkeypatch, kind, mode=mode, database_url=database_url) as joint:
        before = _state(joint)
        assert before["reserved"] > 0 and before["committed"] == 0
        physical = list(joint.actor.seen)
        checkpoint = _stored(joint.actor)
        published = joint.publisher.publish(joint.completion)
        current = _state(joint)
        assert current["reserved"] == 0 and current["committed"] == before["reserved"]
        assert current["generation"] == before["generation"] + 1
        assert current["begins"] == before["begins"] and current["children"] == before["children"]
        assert current["populations"][0][0] == "published"
        assert current["ready"] == [(published.receipt.object_path, published.receipt.stored_bytes)]
        assert published.ticket is joint.completion
        assert joint.publisher.publish(joint.completion) == published
        assert _state(joint) == current
        assert joint.actor.seen == physical and _stored(joint.actor) == checkpoint
        assert published.receipt.object_path != joint.completed.object_path
        assert published.receipt.stored_sha256 is None
        assert joint.actor.case.service._retrieval_cache.release(owner="joint-cache") == 0
        with pytest.raises(Conflict, match="independently owned"):
            joint.writes.acknowledge_report(joint.completion)
        joint.publisher.acknowledge(published)
        after = _state(joint)
        assert after["reserved"] == 0 and after["committed"] == before["reserved"]
        assert after["generation"] == current["generation"]
        assert after["begins"] == after["children"] == after["populations"] == []
        assert after["ready"] == current["ready"]
        assert joint.actor.seen == physical and _stored(joint.actor) == checkpoint


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("mode", ("direct", "owned-http"))
def test_original_cache_publication_moves_data_once_before_retiring_exact_controls(
    tmp_path, monkeypatch, kind, mode
):
    _assert_publication(tmp_path, monkeypatch, kind, mode)


def _assert_publication_reply_loss(tmp_path, monkeypatch, committed, *, database_url=None):
    with _completed(tmp_path, monkeypatch, database_url=database_url) as joint:
        publish, attempts = joint.publisher.publish, []
        physical, checkpoint = list(joint.actor.seen), _stored(joint.actor)

        adjust = publication_module.adjust_cache_committed_bytes

        def rollback(accounting, *, delta):
            adjust(accounting, delta=delta)
            if not committed and len(attempts) == 1:
                raise OSError("injected publication reply loss")

        def lost(ticket):
            attempts.append(ticket)
            result = publish(ticket)
            if committed and len(attempts) == 1:
                raise OSError("injected publication reply loss")
            return result

        monkeypatch.setattr(publication_module, "adjust_cache_committed_bytes", rollback)
        monkeypatch.setattr(joint.publisher, "publish", lost)
        before = _state(joint)
        with pytest.raises(OSError, match="reply loss"):
            joint.writer.publish_cache(joint.publisher)
        held = _state(joint)
        assert held["begins"] and held["children"] and held["populations"]
        assert held["committed"] == (before["reserved"] if committed else 0)
        receipt = joint.writer.publish_cache(joint.publisher)
        after = _state(joint)
        assert receipt is not None and after["ready"]
        assert after["committed"] == before["reserved"] and after["reserved"] == 0
        assert after["generation"] == before["generation"] + 1
        assert after["begins"] == after["children"] == after["populations"] == []
        assert attempts == [joint.completion, joint.completion]
        assert joint.writer.publish_cache(joint.publisher) is receipt
        assert joint.actor.seen == physical and _stored(joint.actor) == checkpoint


def _assert_publication_retirement_loss(tmp_path, monkeypatch, committed, *, database_url=None):
    with _completed(tmp_path, monkeypatch, database_url=database_url) as joint:
        acknowledge, attempts = joint.publisher.acknowledge, []
        physical = list(joint.actor.seen)

        transaction = publication_module.session_scope

        @contextmanager
        def rollback(factory):
            with transaction(factory) as session:
                yield session
                if not committed and len(attempts) == 1:
                    raise OSError("injected publication retirement reply loss")

        def lost(publication):
            attempts.append(publication)
            acknowledge(publication)
            if committed and len(attempts) == 1:
                raise OSError("injected publication retirement reply loss")

        monkeypatch.setattr(publication_module, "session_scope", rollback)
        monkeypatch.setattr(joint.publisher, "acknowledge", lost)
        with pytest.raises(OSError, match="reply loss"):
            joint.writer.publish_cache(joint.publisher)
        held = _state(joint)
        receipt = joint.writer.publish_cache(joint.publisher)
        assert receipt is not None and _state(joint)["committed"] == receipt.stored_bytes
        assert _state(joint)["generation"] == held["generation"]
        assert all(attempt is attempts[0] for attempt in attempts)
        assert joint.actor.seen == physical


@pytest.mark.parametrize(
    "changed",
    (
        "unpublished",
        "generation",
        "source_prefix",
        "copy_state",
        "copy_verification",
        "object_bytes",
        "object_verification",
        "population_generation",
        "population_state",
        "cache_disabled",
        "source_disabled",
    ),
)
def test_publication_refuses_unaccepted_or_changed_original_archive_and_population(
    tmp_path, monkeypatch, changed
):
    with _completed(tmp_path, monkeypatch) as joint:
        case = joint.actor.case
        with session_scope(case.factory) as session:
            begin = session.get(CachePopulationBeginRecord, joint.mirror.cache_begin_call_id)
            key = (begin.source_store, case.collection_id, case.object_id)
            collection = session.get(CollectionRecord, case.collection_id)
            copy = session.get(
                CollectionArchiveCopyRecord, (case.collection_id, begin.source_store)
            )
            obj = session.get(
                CollectionArchiveObjectRecord,
                (case.collection_id, begin.source_store, case.object_id),
            )
            population = session.get(RetrievalCachePopulationRecord, key)
            if changed == "unpublished":
                collection.is_published = False
            elif changed == "generation":
                collection.archive_generation = "e" * 64
            elif changed == "source_prefix":
                copy.archive_storage_prefix += "/changed"
            elif changed == "copy_state":
                copy.state = "failed"
            elif changed == "copy_verification":
                copy.last_verified_at = None
            elif changed == "object_bytes":
                obj.stored_bytes += 1
            elif changed == "object_verification":
                obj.verified_at = None
            elif changed == "population_generation":
                population.generation = "e" * 64
            elif changed == "population_state":
                population.state = "abandoning"
            elif changed == "cache_disabled":
                session.get(
                    StorageIncarnationRecord, ("cache", begin.cache_store)
                ).state = "disabled"
            elif changed == "source_disabled":
                session.get(StorageIncarnationRecord, ("archive", copy.store)).state = "disabled"
        before = _state(joint)
        with pytest.raises(Conflict):
            joint.publisher.publish(joint.completion)
        assert _state(joint) == before


def test_publication_acknowledgement_refuses_changed_receipt_without_releasing_controls(
    tmp_path, monkeypatch
):
    with _completed(tmp_path, monkeypatch) as joint:
        published = joint.publisher.publish(joint.completion)
        before = _state(joint)
        changed = replace(published, receipt=replace(published.receipt, stored_bytes=1))
        with pytest.raises(Conflict, match="receipt"):
            joint.publisher.acknowledge(changed)
        assert _state(joint) == before


def _assert_publication_after_eviction(tmp_path, monkeypatch, *, database_url=None):
    with _completed(tmp_path, monkeypatch, database_url=database_url) as joint:
        published = joint.publisher.publish(joint.completion)
        physical = list(joint.actor.seen)
        coordinator = joint.actor.case.service._retrieval_cache
        assert coordinator._evict_one(cache_store=joint.candidate.name)
        evicted = _state(joint)
        assert evicted["reserved"] == evicted["committed"] == 0 and evicted["ready"] == []
        assert evicted["begins"] and evicted["children"]
        assert joint.publisher.publish(joint.completion) == published
        assert _state(joint) == evicted and joint.actor.seen == physical
        joint.publisher.acknowledge(published)
        after = _state(joint)
        assert after["reserved"] == after["committed"] == 0 and after["ready"] == []
        assert after["begins"] == after["children"] == after["populations"] == []


@pytest.mark.parametrize("outcome", ("unknown", "positive-unknown"))
def test_actual_uncertain_cache_completion_never_publishes_or_releases_original_charge(
    tmp_path, monkeypatch, outcome
):
    with _writing(tmp_path, monkeypatch) as joint:
        execute = joint.candidate.execute_write

        def uncertain(invocation, **kwargs):
            response = execute(invocation, **kwargs)
            if invocation.operation.kind != "complete":
                return response
            fields = {"execution": "unknown"}
            if outcome == "unknown":
                fields.update(outcome="unknown", result=None)
            return response.model_copy(update=fields)

        monkeypatch.setattr(joint.candidate, "execute_write", uncertain)
        opened = _open(joint.actor)
        joint.completed = _unit(joint.actor, opened)
        _accept_archive(joint, joint.completed.completed)
        publisher = CachePopulationPublication(joint.actor.case.factory)
        ticket = next(
            effect.ticket for effect in joint.writer._effects.values() if effect.kind == "complete"
        )
        before = _state(joint)
        with pytest.raises(Conflict, match="closed completion"):
            publisher.publish(ticket)
        with pytest.raises(Conflict, match="confirmed original completion"):
            joint.writer.publish_cache(publisher)
        assert _state(joint) == before and before["reserved"] > 0 and before["ready"] == []


def test_publication_acknowledgement_uses_its_original_reserved_byte_grant(tmp_path, monkeypatch):
    with _writing(tmp_path, monkeypatch) as joint:
        arm = joint.cache.arm

        def smaller(*args, **kwargs):
            kwargs["grants"] = replace(kwargs["grants"], result_bytes=800)
            return arm(*args, **kwargs)

        monkeypatch.setattr(joint.cache, "arm", smaller)
        opened = _open(joint.actor)
        joint.completed = _unit(joint.actor, opened)
        _accept_archive(joint, joint.completed.completed)
        publisher = CachePopulationPublication(joint.actor.case.factory)
        before = _state(joint)
        with pytest.raises(Conflict, match="reserved byte grant"):
            joint.writer.publish_cache(publisher)
        assert _state(joint) == before and before["reserved"] > 0 and before["ready"] == []


@pytest.mark.parametrize("committed", (False, True))
def test_joint_publication_reply_loss_retains_original_completion_and_transfers_once(
    tmp_path, monkeypatch, committed
):
    _assert_publication_reply_loss(tmp_path, monkeypatch, committed)


@pytest.mark.parametrize("committed", (False, True))
def test_joint_publication_control_retirement_reply_loss_retries_same_transfer(
    tmp_path, monkeypatch, committed
):
    _assert_publication_retirement_loss(tmp_path, monkeypatch, committed)


def test_publication_replay_after_receiving_owner_eviction_never_recreates_placement(
    tmp_path, monkeypatch
):
    _assert_publication_after_eviction(tmp_path, monkeypatch)


def test_publication_requires_every_original_part_control_acknowledged(tmp_path, monkeypatch):
    with _writing(tmp_path, monkeypatch) as joint:
        acknowledge = joint.writes.acknowledge_report
        retained = []

        def capture(ticket):
            with session_scope(joint.actor.case.factory) as session:
                row = session.get(CachePopulationWriteEffectRecord, ticket.call_id)
                retained.append(
                    {column.key: getattr(row, column.key) for column in row.__table__.columns}
                )
            return acknowledge(ticket)

        monkeypatch.setattr(joint.writes, "acknowledge_report", capture)
        opened = _open(joint.actor)
        joint.completed = _unit(joint.actor, opened)
        _accept_archive(joint, joint.completed.completed)
        assert len(retained) == 1 and retained[0]["state"] == "settled"
        # Restore the authentic closed part as an unacknowledged control row.
        with session_scope(joint.actor.case.factory) as session:
            session.add(CachePopulationWriteEffectRecord(**retained[0]))
        publisher = CachePopulationPublication(joint.actor.case.factory)
        before = _state(joint)
        with pytest.raises(Conflict, match="unacknowledged"):
            joint.writer.publish_cache(publisher)
        assert _state(joint) == before and before["ready"] == []


def test_original_publication_acknowledgement_survives_population_retirement(tmp_path, monkeypatch):
    with _completed(tmp_path, monkeypatch) as joint:
        published = joint.publisher.publish(joint.completion)
        with session_scope(joint.actor.case.factory) as session:
            population = session.scalar(select(RetrievalCachePopulationRecord))
            session.delete(population)
        before = _state(joint)
        assert before["populations"] == [] and before["begins"] and before["children"]
        assert joint.publisher.publish(joint.completion) == published
        assert _state(joint) == before
        joint.publisher.acknowledge(published)
        after = _state(joint)
        assert after["reserved"] == 0 and after["committed"] == before["committed"]
        assert after["generation"] == before["generation"]
        assert after["begins"] == after["children"] == []
