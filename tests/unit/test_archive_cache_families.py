from __future__ import annotations

import threading
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_core.cache_write_grants import CacheWriteGrants
from riverhog_core.services.archive_cache_families import ArchiveCacheFamilies
from riverhog_core.services.mirrored_archive_upload_writes import MirroredArchiveUploadWrites
from riverhog_protocol.errors import Conflict

from tests.unit.test_archive_payload_input import _body, _request, _submit
from tests.unit.test_archive_upload_invocations import _receive
from tests.unit.test_archive_upload_invocations import owned as owned


class _InputActor:
    def __init__(self, payload, factory):
        self.payload = payload
        self.factory = factory
        self.mirror = None
        payload.bind_actor(self)

    @property
    def has_pending(self):
        return self.mirror is not None and self.mirror.has_pending

    def advance(self):
        if self.mirror is None:
            request = self.payload.request
            archive = SimpleNamespace(
                scope=(request.key.collection_id, request.volume_id, request.epoch),
                has_pending=False,
            )
            self.mirror = self.factory(archive, lambda session: None)
        self.payload.consumed(self)
        return self.mirror

    def drain_terminal(self, operation_id):
        self.payload.consumed(self)
        return "closed"


def _families(owned):
    registry, pool, budget = owned.make(
        retained_constructions=4, retained_constructions_per_target=4
    )
    families = ArchiveCacheFamilies(registry)
    begin = SimpleNamespace(has_pending=True)
    calls = []

    def first(archive, guard):
        calls.append(archive)
        return MirroredArchiveUploadWrites(
            archive=archive,
            begin=begin,
            cache=None,
            source_store="source",
            owner="owner",
            principal=_request().principal,
            grants=CacheWriteGrants(
                calls=4, control_bytes=100000, result_bytes=10000, cleanup_bytes=1000
            ),
            guard=guard,
            validate_population=lambda *args: None,
        )

    def submit(number, *, request=None, create=first):
        payload = _body(request=request or _request(unit=number))
        factory = families.factory(
            payload,
            create=create,
            grants=CacheWriteGrants(
                calls=4, control_bytes=100000, result_bytes=10000, cleanup_bytes=1000
            ),
            validate_population=lambda *args: None,
        )
        actor = _InputActor(payload, factory)
        status = _submit(registry, payload, create=lambda: actor, advance=lambda a: a.advance())
        return payload, actor, status

    return SimpleNamespace(
        registry=registry,
        pool=pool,
        budget=budget,
        families=families,
        begin=begin,
        calls=calls,
        create=first,
        submit=submit,
    )


@pytest.mark.parametrize("children", (8, 32, 128))
def test_one_charged_parent_allows_many_healthy_children_to_retire(owned, children):
    f = _families(owned)
    payload, first, status = f.submit(0)
    assert status == "submitted"
    (parent,) = _receive(f.registry, f.pool)
    original = parent.result().writes
    assert payload.released and len(f.budget._reservations) == 1
    assert not f.registry.retire_idle(parent) and not f.families.retire(payload)
    for number in range(1, children + 1):
        incoming, child, status = f.submit(number)
        assert status == "submitted"
        (received,) = _receive(f.registry, f.pool)
        mirror = received.result()
        assert mirror.writes._begin is original._begin
        assert incoming.released and not child.has_pending
        assert len(f.budget._reservations) == 2 and len(f.families._families) == 1
        assert f.registry.retire_idle(received)
        assert len(f.budget._reservations) == 1
    assert len(f.calls) == 1 and first.has_pending
    # A complete source still cannot release an unsettled original cache begin.
    family = next(iter(f.families._families.values()))
    family.complete = True
    assert not f.families.retire(payload)
    f.begin.has_pending = False
    assert f.families.retire(payload)
    assert not f.families._families and not family.pin.active
    assert not first.has_pending and f.registry.retire_idle(parent)
    assert not f.budget._reservations


def test_family_parent_stays_charged_until_actual_worker_join_and_map_removal(owned, monkeypatch):
    f = _families(owned)
    payload, actor, status = f.submit(0)
    assert status == "submitted"
    (parent,) = _receive(f.registry, f.pool)
    parent.result()
    family = next(iter(f.families._families.values()))
    f.begin.has_pending = False
    family.complete = True
    entered, release = threading.Event(), owned.event()

    def held():
        entered.set()
        assert release.wait(10)

    assert (
        f.pool.try_submit(
            payload.request.key,
            target_incarnation=payload.request.target_incarnation,
            terminal=False,
            work=held,
        )
        == "submitted"
    )
    assert entered.wait(10)
    assert not f.families.retire(payload) and family.pin.active
    assert not f.registry.retire_idle(parent)
    release.set()
    for slot in tuple(f.pool._slots.values()):
        slot.worker.join(timeout=10)
        assert not slot.worker.is_alive()
    f.pool.collect(owner=None, limit=100)
    original = f.registry._release_pin

    def release_pin(pin, *, owner):
        assert not f.families._families
        assert len(f.budget._reservations) == 1
        return original(pin, owner=owner)

    monkeypatch.setattr(f.registry, "_release_pin", release_pin)
    assert f.families.retire(payload)
    assert f.registry.retire_idle(parent)


def test_uninstalled_refusal_never_enters_family_constructor(owned):
    f = _families(owned)
    with f.registry._lock:
        payload, actor, status = f.submit(0)
        assert status == "busy"
    assert not f.calls and not f.families._families
    assert payload.released


@pytest.mark.parametrize("change", ("key", "target_incarnation", "epoch", "volume_id"))
def test_pin_requires_exact_original_scope_and_owner(owned, change):
    f = _families(owned)
    payload, actor, status = f.submit(0)
    assert status == "submitted"
    (parent,) = _receive(f.registry, f.pool)
    parent.result()
    request = payload.request
    values = dict(
        key=request.key,
        target_incarnation=request.target_incarnation,
        epoch=request.epoch,
        volume_id=request.volume_id,
    )
    values[change] = replace(request.key, scope_sha256="c" * 64) if change == "key" else "another"
    with pytest.raises(Conflict, match="scope"):
        f.registry.pin_family(payload.request_sha256, owner=object(), **values)
    with pytest.raises(Conflict, match="owner"):
        f.registry.pin_family(
            payload.request_sha256,
            key=request.key,
            target_incarnation=request.target_incarnation,
            epoch=request.epoch,
            volume_id=request.volume_id,
            owner=object(),
        )
    assert not f.registry.retire_idle(parent)


def test_unknown_child_keeps_its_independent_charge(owned):
    f = _families(owned)
    payload, _, status = f.submit(0)
    assert status == "submitted"
    (parent,) = _receive(f.registry, f.pool)
    parent.result()
    for number in range(1, 4):
        _, child, status = f.submit(number)
        assert status == "submitted"
        (received,) = _receive(f.registry, f.pool)
        received.result()
        child.mirror.writes._cache_admission_pending = True
        assert child.has_pending and not f.registry.retire_idle(received)
    _, _, status = f.submit(4)
    assert status == "retention-capacity"
    assert len(f.calls) == 1 and len(f.budget._reservations) == 4
    assert not f.families.retire(payload)


def test_closed_family_cannot_accept_fresh_child_or_changed_plan(owned):
    f = _families(owned)
    payload, _, status = f.submit(0)
    assert status == "submitted"
    (parent,) = _receive(f.registry, f.pool)
    parent.result()
    _, _, status = f.submit(1, request=_request(unit=1, plan_sha256="d" * 64))
    assert status == "submitted"
    (changed,) = _receive(f.registry, f.pool)
    with pytest.raises(Conflict, match="plan"):
        changed.result()
    family = next(iter(f.families._families.values()))
    family.complete = True
    _, _, status = f.submit(2)
    assert status == "submitted"
    (closed,) = _receive(f.registry, f.pool)
    with pytest.raises(Conflict, match="closed"):
        closed.result()
    assert len(f.calls) == 1


def _service_family_units(
    tmp_path,
    monkeypatch,
    mode,
    unknown,
    *,
    database_url=None,
    lose_transfer=False,
    refuse_parent=False,
    terminal_unknown=False,
    maintained=False,
    terminal_after_first=False,
    lose_abandonment=False,
):
    from riverhog_age import CHUNK_SIZE, ResumableAgeScryptSession
    from riverhog_core.archive_store_registry import ArchiveStoreRegistry
    from riverhog_core.catalog_cache_write_models import (
        CachePopulationBeginRecord,
        CachePopulationWriteEffectRecord,
    )
    from riverhog_core.catalog_db import session_scope
    from riverhog_core.catalog_models import (
        CollectionArchiveObjectUploadRecord,
        CollectionUploadRecord,
    )
    from riverhog_core.services.archive_payload_cache import ArchivePayloadCache
    from riverhog_core.services.archive_payload_input import ArchivePayloadRequest
    from riverhog_core.services.cache_population_abandonment import CachePopulationAbandonment
    from riverhog_core.services.cache_population_closure import CachePopulationClosure
    from riverhog_storage_adapter_protocol import unknown_write_execution
    from sqlalchemy import func, select

    from tests.unit.test_archive_finalizer_registry import _scope
    from tests.unit.test_collection_uploads import _CREATOR
    from tests.unit.test_mirrored_archive_upload_writes import _writing

    content = b"a" * CHUNK_SIZE + b"b" * CHUNK_SIZE
    profile = ResumableAgeScryptSession.create("fixture profile", log_n=1)
    segment_bytes = profile.age_aligned_unit_plans(len(content), chunks_per_unit=1)[
        0
    ].ciphertext_len
    with _writing(
        tmp_path,
        monkeypatch,
        "segment",
        mode=mode,
        content=content,
        database_url=database_url,
        segment_bytes=segment_bytes,
    ) as joint:
        actor = joint.actor
        case, service = actor.case, actor.case.service
        with session_scope(case.factory) as session:
            name = session.get(CollectionUploadRecord, case.collection_id).archive_store
        binding = service._archive_stores.require(name)
        service._archive_stores = ArchiveStoreRegistry(
            {
                name: replace(
                    binding, resumable_objects=actor.provider, resumable_writes=actor.executor
                )
            }
        )
        key, target = _scope(case)
        registry, execution = service._archive_invocations, service._archive_execution
        cache_calls, built, originals, received_units = [], [], [], []
        cache_execute = joint.candidate.execute_write

        def cache(invocation, **kwargs):
            result = cache_execute(invocation, **kwargs)
            cache_calls.append(invocation)
            if unknown and invocation.operation.kind == "part":
                return unknown_write_execution(invocation)
            return result

        monkeypatch.setattr(joint.candidate, "execute_write", cache)

        original_create = ArchivePayloadCache.create

        def create(builder, archive, guard):
            value = original_create(builder, archive, guard)
            built.append(value)
            return value

        monkeypatch.setattr(ArchivePayloadCache, "create", create)

        ack, acknowledgements = CachePopulationClosure.acknowledge, []

        def acknowledge(owner, receipt):
            ack(owner, receipt)
            acknowledgements.append(receipt)
            if lose_transfer and len(acknowledgements) == 1:
                raise OSError("injected original closed-cache acknowledgement loss")

        monkeypatch.setattr(CachePopulationClosure, "acknowledge", acknowledge)
        abandonment_ack = CachePopulationAbandonment.acknowledge
        abandonment_receipts = []

        def acknowledge_abandonment(owner, receipt):
            abandonment_ack(owner, receipt)
            abandonment_receipts.append(receipt)
            if lose_abandonment and len(abandonment_receipts) == 1:
                raise OSError("injected abandonment acknowledgement loss")

        monkeypatch.setattr(CachePopulationAbandonment, "acknowledge", acknowledge_abandonment)
        pin, pin_attempts = registry.pin_family, []

        def pin_family(*args, **kwargs):
            pin_attempts.append(True)
            if refuse_parent and len(pin_attempts) == 1:
                return "busy"
            return pin(*args, **kwargs)

        monkeypatch.setattr(registry, "pin_family", pin_family)
        for number in range(2):
            chunk = content[number * CHUNK_SIZE : (number + 1) * CHUNK_SIZE]
            payload = _body(
                service._archive_inputs,
                ArchivePayloadRequest(
                    key,
                    target,
                    actor.journal.scope[2],
                    case.object_id,
                    number,
                    case.plan_sha256,
                    len(chunk),
                    _CREATOR,
                ),
                chunk,
            )
            owner = service._payload_unit_invocation(
                key=key,
                target_incarnation=target,
                volume_id=case.object_id,
                unit=number,
                plan_sha256=case.plan_sha256,
                payload=payload,
                principal=_CREATOR,
            )
            originals.append(owner)

            def submit(payload=payload, owner=owner):
                return registry.submit(
                    key,
                    target_incarnation=target,
                    request_sha256=payload.request_sha256,
                    payload=payload,
                    create=lambda: owner,
                    advance=lambda a: a.advance(),
                )

            assert submit() == "submitted"
            (received,) = _receive(registry, execution)
            if refuse_parent and number == 0:
                from riverhog_core.services.archive_cache_families import CacheFamilyNotAdmitted

                with pytest.raises(CacheFamilyNotAdmitted):
                    received.result()
                assert not built and not owner._mirror_unresolved
                assert not service._archive_cache_families._families and not actor.seen
                assert submit() == "submitted"
                (received,) = _receive(registry, execution)
            if lose_transfer and number == 1:
                with pytest.raises(OSError, match="acknowledgement loss"):
                    received.result()
                before = list(actor.seen), list(cache_calls)
                assert not service._archive_cache_families.retire(payload)
                assert not registry.retire_idle(received)
                assert submit() == "submitted"
                (received,) = _receive(registry, execution)
                assert (actor.seen, cache_calls) == before
                assert acknowledgements[0] is acknowledgements[1]
            assert received.result()["state"] == "committed"
            assert payload.released
            received_units.append(received)
            assert len(built) == 1
            if number == 0:
                assert owner.has_pending and not registry.retire_idle(received)
                assert not service._archive_cache_families.retire(payload)
            if terminal_after_first:
                assert number == 0
                operation = actor.admission.select_terminal(
                    case.collection_id,
                    kind="cancel",
                    request={"fixture": "partial cache family"},
                    principal_id=_CREATOR.id,
                    key_id=_CREATOR.key_id,
                    grants=service._config.archive_write,
                    guard=service._payload_write_guard(case.collection_id, _CREATOR),
                    validate_selection=lambda session, upload: None,
                )
                before = list(actor.seen), list(cache_calls)
                assert (
                    registry.submit_terminal(key, target_incarnation=target, operation_id=operation)
                    == "submitted"
                )
                (terminal,) = _receive(registry, execution)
                if lose_abandonment:
                    with pytest.raises(OSError, match="abandonment acknowledgement loss"):
                        terminal.result()
                    assert owner.has_pending and not service._archive_cache_families.retire(payload)
                    assert (
                        registry.submit_terminal(
                            key, target_incarnation=target, operation_id=operation
                        )
                        == "submitted"
                    )
                    (terminal,) = _receive(registry, execution)
                    assert abandonment_receipts[0] is abandonment_receipts[1]
                assert terminal.result() == ("pending" if unknown else "closed")
                assert (actor.seen, cache_calls) == before
                with session_scope(case.factory) as session:
                    observation = service._archive_terminal_observation.observe(
                        session, case.collection_id, operation
                    )
                    assert not observation.records.archive_unclosed
                    assert not observation.records.cache_begin_unclosed
                    assert observation.records.cache_write_unclosed is unknown
                    assert observation.records.cache_begin_retained is unknown
                    assert not observation.records.payload_sealed
                    assert session.get(CollectionUploadRecord, case.collection_id) is not None
                    assert session.scalar(
                        select(func.count()).select_from(CachePopulationBeginRecord)
                    ) == int(unknown)
                assert observation.invocations == ("fenced" if unknown else "drained")
                assert observation.inputs == observation.execution == "untracked"
                assert owner.has_pending and payload.released
                assert len(service._archive_retention._reservations) == 1
                assert len(service._archive_cache_families._families) == 1
                pin = next(iter(service._archive_cache_families._families.values())).pin
                assert pin.active
                assert service._archive_cache_families.retire(payload) is not unknown
                if unknown:
                    assert pin.active and owner.has_pending
                else:
                    assert not pin.active and not owner.has_pending
                    assert not service._archive_cache_families._families
                    receipt = abandonment_receipts[-1]
                    from riverhog_core.catalog_models import RetrievalCachePopulationRecord

                    with session_scope(case.factory) as session:
                        population = session.get(
                            RetrievalCachePopulationRecord,
                            (receipt.source_store, case.collection_id, case.object_id),
                        )
                        assert population.state == "abandoning"
                        assert population.abandonment_receipt_json == receipt.receipt_json
                        assert population.begin_call_id is not None
                    # Global terminal ownership still retains the construction charge.
                    assert len(service._archive_retention._reservations) == 1
                with pytest.raises(Conflict):
                    service._reserve_payload_input(
                        case.collection_id,
                        case.object_id,
                        1,
                        plan_sha256=case.plan_sha256,
                        principal=_CREATOR,
                    )
                assert (actor.seen, cache_calls) == before
                assert service._archive_inputs._bytes == 0
                return

        assert sum(call.operation.kind == "begin" for call in cache_calls) == 1
        assert originals[0]._writes._begin is originals[1]._writes._begin
        assert originals[0]._archive is not originals[1]._archive
        assert not originals[1]._mirror_binding._first
        with session_scope(case.factory) as session:
            source = session.get(
                CollectionArchiveObjectUploadRecord, (case.collection_id, case.object_id)
            )
            assert source.sealed_receipt_json is not None
            assert (source.cache_receipt_json is not None) is not unknown
            assert (
                session.scalar(select(func.count()).select_from(CachePopulationBeginRecord)) == 0
            ) is not unknown
            assert (
                session.scalar(select(func.count()).select_from(CachePopulationWriteEffectRecord))
                == 0
            ) is not unknown
        if maintained:
            before = list(actor.seen), list(cache_calls)
            for _ in range(2):
                service.process_due_archive_uploads(limit=1)
            assert (actor.seen, cache_calls) == before
            assert service._archive_inputs._bytes == 0
            assert len(service._archive_retention._reservations) == int(unknown)
            assert len(service._archive_cache_families._families) == int(unknown)
            if unknown:
                assert originals[0].has_pending
                assert received_units[0].request_sha256 in registry._invocations
            else:
                assert not registry._invocations
                assert all(not owner.has_pending for owner in originals)
            return
        if terminal_unknown:
            assert unknown
            operation = actor.admission.select_terminal(
                case.collection_id,
                kind="cancel",
                request={"fixture": "family terminal"},
                principal_id=_CREATOR.id,
                key_id=_CREATOR.key_id,
                grants=service._config.archive_write,
                guard=service._payload_write_guard(case.collection_id, _CREATOR),
                validate_selection=lambda session, upload: None,
            )
            results = []
            for _ in range(2):
                assert (
                    registry.submit_terminal(key, target_incarnation=target, operation_id=operation)
                    == "submitted"
                )
                (received,) = _receive(registry, execution)
                results.append(received.result())
            assert results == ["pending", "closed"]
            assert not service._archive_cache_families.retire(payload)
            assert registry._invocations[received_units[1].request_sha256].drained
            assert not registry._invocations[received_units[0].request_sha256].drained
            assert len(service._archive_retention._reservations) == 2
            return
        if unknown:
            assert not service._archive_cache_families.retire(payload)
            assert originals[0].has_pending and not registry.retire_idle(received_units[0])
            assert not originals[1].has_pending and registry.retire_idle(received_units[1])
        else:
            assert service._archive_cache_families.retire(payload)
            assert not service._archive_cache_families._families
            assert all(not owner.has_pending for owner in originals)
            assert all(registry.retire_idle(received) for received in received_units)
            assert not service._archive_retention._reservations
        assert service._archive_inputs._bytes == 0


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("unknown", (False, True))
def test_service_units_share_original_cache_family_and_transfer_before_publication(
    tmp_path, monkeypatch, mode, unknown
):
    _service_family_units(tmp_path, monkeypatch, mode, unknown)


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
def test_service_cache_receipt_acknowledgement_loss_retries_only_original_receiving(
    tmp_path, monkeypatch, mode
):
    _service_family_units(tmp_path, monkeypatch, mode, False, lose_transfer=True)


def test_service_can_retry_a_parent_pin_refusal_without_reconstructing_unknown_work(
    tmp_path, monkeypatch
):
    _service_family_units(tmp_path, monkeypatch, "direct", False, refuse_parent=True)


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
def test_terminal_child_receives_its_work_while_unknown_parent_remains_charged(
    tmp_path, monkeypatch, mode
):
    _service_family_units(tmp_path, monkeypatch, mode, True, terminal_unknown=True)


def test_held_family_constructor_keeps_pin_without_blocking_an_independent_family(owned):
    f = _families(owned)
    entered, release = threading.Event(), owned.event()

    def held(archive, guard):
        assert len(f.families._families) == 1
        assert len(f.budget._reservations) == 1
        entered.set()
        assert release.wait(10)
        return f.create(archive, guard)

    payload, first, status = f.submit(0, create=held)
    assert status == "submitted" and entered.wait(10)
    assert not f.families.retire(payload)
    other_request = _request(key=replace(payload.request.key, collection_id=2))
    other, second, status = f.submit(1, request=other_request)
    assert status == "submitted"
    slot = next(slot for slot in f.pool._slots.values() if slot.key == other.request.key)
    slot.worker.join(timeout=10)
    assert not slot.worker.is_alive()
    (received,) = f.registry.collect(limit=100)
    received.result()
    assert second.mirror is not None and first.mirror is None
    assert len(f.families._families) == 2 and len(f.budget._reservations) == 2
    release.set()
    (received,) = _receive(f.registry, f.pool)
    received.result()
    assert first.mirror is not None


def test_parent_pin_cannot_be_released_by_a_different_receiver(owned):
    f = _families(owned)
    payload, _, status = f.submit(0)
    assert status == "submitted"
    (received,) = _receive(f.registry, f.pool)
    received.result()
    family = next(iter(f.families._families.values()))
    with pytest.raises(Conflict, match="receiving owner"):
        family.pin.release(owner=object())
    assert family.pin.active and len(f.budget._reservations) == 1
    assert not f.families.retire(payload) and not f.registry.retire_idle(received)


@pytest.mark.parametrize("after_release", (False, True))
def test_parent_release_error_keeps_map_only_while_original_pin_is_charged(
    owned, monkeypatch, after_release
):
    f = _families(owned)
    payload, actor, status = f.submit(0)
    assert status == "submitted"
    (received,) = _receive(f.registry, f.pool)
    received.result()
    family = next(iter(f.families._families.values()))
    family.complete = True
    f.begin.has_pending = False
    original = f.registry._release_pin

    def release(pin, *, owner):
        assert not f.families._families
        if after_release:
            assert original(pin, owner=owner)
        raise OSError("injected local parent-release interruption")

    monkeypatch.setattr(f.registry, "_release_pin", release)
    with pytest.raises(OSError, match="parent-release"):
        f.families.retire(payload)
    assert bool(f.families._families) is not after_release
    assert family.pin.active is not after_release
    assert len(f.budget._reservations) == 1
    assert actor.has_pending is not after_release
    monkeypatch.setattr(f.registry, "_release_pin", original)
    assert f.families.retire(payload)
    assert f.registry.retire_idle(received)
    assert not f.budget._reservations


def test_bounded_parent_retirement_rotates_past_pending_family(owned, monkeypatch):
    f = _families(owned)
    first, _, status = f.submit(0)
    assert status == "submitted"
    _receive(f.registry, f.pool)[0].result()
    other, owner, status = f.submit(1, request=_request(unit=1, volume_id="other"))
    assert status == "submitted"
    _receive(f.registry, f.pool)[0].result()
    family = owner.mirror._family
    owner.mirror.writes._begin = SimpleNamespace(has_pending=False)
    family.complete = True
    visits = []
    actual = f.families._retire_family

    def visit(scope, original):
        visits.append(scope)
        return actual(scope, original)

    monkeypatch.setattr(f.families, "_retire_family", visit)
    assert f.families.retire_ready(limit=1) == 0 and len(visits) == 1
    assert f.families.retire_ready(limit=1) == 1 and len(visits) == 2
    assert family.pin.active is False and owner.has_pending is False
    assert not f.families.retire(first)
    assert f.registry.retire_ready(limit=2) == 1
    assert len(f.budget._reservations) == len(f.families._families) == 1
    assert f.registry.completed(other.request_sha256) is None


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("unknown", (False, True))
def test_service_maintenance_retires_closed_families_but_preserves_unknown_cache(
    tmp_path, monkeypatch, mode, unknown
):
    _service_family_units(tmp_path, monkeypatch, mode, unknown, maintained=True)


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("unknown", (False, True))
def test_partial_cache_family_closes_only_confirmed_forward_execution_and_retains_ownership(
    tmp_path, monkeypatch, mode, unknown
):
    _service_family_units(tmp_path, monkeypatch, mode, unknown, terminal_after_first=True)


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
def test_partial_cache_family_retries_original_abandonment_ack_without_provider_redispatch(
    tmp_path, monkeypatch, mode
):
    _service_family_units(
        tmp_path,
        monkeypatch,
        mode,
        False,
        terminal_after_first=True,
        lose_abandonment=True,
    )
