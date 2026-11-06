from __future__ import annotations

import hashlib
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_age import CHUNK_SIZE, ResumableAgeScryptSession
from riverhog_core.catalog_archive_write_models import ArchiveWriteEffectRecord
from riverhog_core.catalog_cache_write_models import (
    CachePopulationBeginRecord,
    CachePopulationWriteEffectRecord,
)
from riverhog_core.catalog_db import session_scope
from riverhog_core.provider_write_dispatch import ProviderWriteAdmissionRefused
from riverhog_core.services.archive_upload_writes import ArchiveUploadWrites
from riverhog_core.services.cache_population_publication import CachePopulationPublication
from riverhog_core.stores.executing_archive_objects import ExecutingArchiveResumableObjectStore
from riverhog_protocol.errors import Conflict
from riverhog_storage_adapter_protocol import unknown_write_execution
from sqlalchemy import select

from tests.unit.test_archive_finalizer_registry import _scope
from tests.unit.test_archive_upload_invocations import _receive
from tests.unit.test_archive_upload_writes import _open, _stored
from tests.unit.test_collection_uploads import _CREATOR
from tests.unit.test_mirrored_archive_upload_writes import _writing


def _new_archive(joint):
    actor = joint.actor
    return ArchiveUploadWrites(
        actor.case.factory,
        admission=actor.admission,
        checkpoints=actor.case.service._checkpoints,
        executor=actor.executor,
        collection_id=actor.case.collection_id,
        object_id=actor.case.object_id,
        epoch=actor.journal.scope[2],
        grants=replace(actor.case.service._config.archive_write, control_bytes=8 * 1024 * 1024),
        guard=actor.case.service._payload_write_guard(actor.case.collection_id, _CREATOR),
    )


def _request(number):
    return hashlib.sha256(str(number).encode()).hexdigest()


def _continue(joint, archive, *, guard=None, grants=None):
    return joint.writer.continue_invocation(
        archive=archive,
        principal=_CREATOR,
        grants=joint.writer._grants if grants is None else grants,
        guard=guard
        if guard is not None
        else joint.actor.case.service._payload_write_guard(
            joint.actor.case.collection_id, _CREATOR
        ),
        validate_population=joint.writer._validate_population,
    )


def _two_units(
    tmp_path,
    monkeypatch,
    mode,
    unknown,
    *,
    database_url=None,
    lose_checkpoint=False,
    refuse_next=None,
):
    content = b"a" * CHUNK_SIZE + b"b" * CHUNK_SIZE
    profile = ResumableAgeScryptSession.create("fixture profile", log_n=1)
    # Use a real fixed-segment profile matching the first nonfinal unit;
    # the second unit is its permitted short final segment.
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
        case = actor.case
        registry, pool = case.service._archive_invocations, case.service._archive_execution
        key, target = _scope(case)
        cache_calls = []
        execute = joint.candidate.execute_write

        def cache(invocation, **kwargs):
            result = execute(invocation, **kwargs)
            cache_calls.append(invocation)
            if unknown and invocation.operation.kind == "part":
                return unknown_write_execution(invocation)
            return result

        monkeypatch.setattr(joint.candidate, "execute_write", cache)

        def first(owner):
            assert owner is joint.writer
            checkpoint = _open(actor)
            return actor.uploader.upload_unit(
                plan=actor.arguments["plan"],
                checkpoint=checkpoint,
                unit_number=1,
                plaintext=content[:CHUNK_SIZE],
            )

        assert (
            registry.submit(
                key,
                target_incarnation=target,
                request_sha256=_request(1),
                create=lambda: joint.writer,
                advance=first,
            )
            == "submitted"
        )
        (received_first,) = _receive(registry, pool)
        checkpoint = received_first.result()
        assert checkpoint.completed is None and checkpoint.next_part == 1
        assert not registry.retire_idle(received_first)
        begin_id = joint.mirror.cache_begin_call_id
        assert begin_id is not None
        originals = []
        guard_calls = []
        original_physical = list(actor.seen)
        original_cache_calls = list(cache_calls)

        def current_guard(session):
            guard_calls.append("current unit")
            raise Conflict("current unit permission revoked")

        def construct():
            archive = _new_archive(joint)
            continuation = _continue(
                joint,
                archive,
                guard=current_guard if refuse_next == "guard" else None,
                grants=replace(joint.writer._grants, calls=1)
                if refuse_next == "capacity"
                else None,
            )
            originals.append((archive, continuation))
            return continuation

        attempted = []
        apply = case.service._checkpoints.apply_raw_upload_checkpoint

        def checkpoint_reply(**kwargs):
            attempted.append(kwargs["checkpoint_json"])
            if lose_checkpoint and len(attempted) == 1:
                raise OSError("original next-unit checkpoint acknowledgement")
            return apply(**kwargs)

        monkeypatch.setattr(
            case.service._checkpoints, "apply_raw_upload_checkpoint", checkpoint_reply
        )

        def second(owner):
            assert owner is originals[0][1]
            store = ExecutingArchiveResumableObjectStore(
                actor.provider, owner, placement_policy="archive_default"
            )
            uploader = case.service._raw_uploader(
                store,
                passphrase_id=case.service._config.archive_active_passphrase_id,
                checkpoint_store=owner,
            )
            opened = uploader.open(**actor.arguments)
            assert opened.to_json() == checkpoint.to_json()
            return uploader.upload_unit(
                plan=actor.arguments["plan"],
                checkpoint=opened,
                unit_number=2,
                plaintext=content[CHUNK_SIZE:],
            )

        assert (
            registry.submit(
                key,
                target_incarnation=target,
                request_sha256=_request(2),
                create=construct,
                advance=second,
            )
            == "submitted"
        )
        (received_second,) = _receive(registry, pool)
        archive, continuation = originals[0]
        assert archive is not actor.journal
        assert archive._executor_id != actor.journal._executor_id
        assert continuation._begin is joint.mirror
        assert joint.mirror.cache_begin_call_id == begin_id

        if refuse_next is not None:
            message = "current unit permission" if refuse_next == "guard" else "grant is exhausted"
            with pytest.raises(ProviderWriteAdmissionRefused, match=message):
                received_second.result()
            assert actor.seen[: len(original_physical)] == original_physical
            assert [call.operation.kind for call in actor.seen[len(original_physical) :]] == [
                "lookup"
            ]
            assert cache_calls == original_cache_calls
            assert guard_calls == (["current unit"] if refuse_next == "guard" else [])
            assert not archive.has_pending
            assert joint.writer.has_pending
            assert not registry.retire_idle(received_first)
            return

        if lose_checkpoint:
            with pytest.raises(OSError, match="checkpoint acknowledgement"):
                received_second.result()
            assert archive.has_pending
            seen = list(actor.seen)
            operation = actor.admission.select_terminal(
                case.collection_id,
                kind="cancel",
                request={"fixture": "next-unit-receiving"},
                principal_id=_CREATOR.id,
                grants=case.service._config.archive_write,
                guard=case.service._payload_write_guard(case.collection_id, _CREATOR),
                validate_selection=lambda session, upload: None,
            )
            for _ in range(2):
                assert (
                    registry.submit_terminal(key, target_incarnation=target, operation_id=operation)
                    == "submitted"
                )
                (drained,) = _receive(registry, pool)
                assert drained.result() == "pending"
            assert not archive.has_pending
            assert actor.seen == seen
            assert len(attempted) >= 2 and all(value == attempted[0] for value in attempted)
            assert _stored(actor)[0] == attempted[0]
            with pytest.raises(Conflict, match="fenced"):
                _continue(joint, _new_archive(joint))
            with pytest.raises(Conflict, match="stopped"):
                continuation.execute_write_operation(actor.seen[-1].operation)
            with session_scope(case.factory) as session:
                assert session.scalar(select(ArchiveWriteEffectRecord.id)) is None
            return

        completed = received_second.result()
        assert completed.completed is not None
        assert _stored(actor)[0] == completed.to_json()
        assert not registry.retire_idle(received_first)
        assert not registry.retire_idle(received_second)
        assert sum(call.operation.kind == "begin" for call in cache_calls) == 1
        with session_scope(case.factory) as session:
            assert session.get(CachePopulationBeginRecord, begin_id) is not None

        publisher = CachePopulationPublication(case.factory)
        if unknown:
            assert sum(call.operation.kind == "part" for call in cache_calls) == 1
            assert not any(call.operation.kind == "complete" for call in cache_calls)
            assert joint.writer.has_pending and continuation.has_pending
            with pytest.raises(Conflict, match="confirmed original completion"):
                continuation.transfer_cache(publisher)
            with session_scope(case.factory) as session:
                assert (
                    session.scalar(
                        select(CachePopulationWriteEffectRecord.id).where(
                            CachePopulationWriteEffectRecord.unresolved.is_(True)
                        )
                    )
                    is not None
                )
            assert not registry.retire_idle(received_first)
            return

        assert sum(call.operation.kind == "complete" for call in cache_calls) == 1
        closed = continuation.transfer_cache(publisher)
        assert closed is not None
        assert not joint.writer.has_pending and not continuation.has_pending
        assert registry.retire_idle(received_first)
        assert registry.retire_idle(received_second)
        with session_scope(case.factory) as session:
            assert session.scalar(select(CachePopulationBeginRecord.id)) is None
            assert session.scalar(select(CachePopulationWriteEffectRecord.id)) is None


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("unknown", (False, True))
def test_actual_next_unit_uses_original_cache_begin_and_keeps_unknown_siblings_charged(
    tmp_path, monkeypatch, mode, unknown
):
    _two_units(tmp_path, monkeypatch, mode, unknown)


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
def test_terminal_receives_the_continuation_own_archive_checkpoint_without_redispatch(
    tmp_path, monkeypatch, mode
):
    _two_units(tmp_path, monkeypatch, mode, False, lose_checkpoint=True)


@pytest.mark.parametrize("change", ("collection", "object", "epoch", "same-owner"))
def test_joint_continuation_rejects_another_scope_or_the_same_original(
    tmp_path, monkeypatch, change
):
    with _writing(tmp_path, monkeypatch) as joint:
        scope = list(joint.actor.journal.scope)
        if change == "collection":
            scope[0] += 1
        elif change == "object":
            scope[1] += "-foreign"
        elif change == "epoch":
            scope[2] = "f" * 64
        foreign = (
            joint.actor.journal if change == "same-owner" else SimpleNamespace(scope=tuple(scope))
        )
        with pytest.raises(Conflict, match="another original invocation"):
            _continue(joint, foreign)


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("refusal", ("guard", "capacity"))
def test_continuation_uses_current_guard_and_grants_before_either_payload_child_dispatch(
    tmp_path, monkeypatch, mode, refusal
):
    _two_units(tmp_path, monkeypatch, mode, False, refuse_next=refusal)
