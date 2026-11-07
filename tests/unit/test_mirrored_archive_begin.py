from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_core.cache_write_grants import CacheWriteGrants
from riverhog_core.catalog_archive_write_models import ArchiveWriteEffectRecord
from riverhog_core.catalog_cache_write_models import CachePopulationBeginRecord
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import (
    CollectionArchiveObjectUploadRecord,
    CollectionUploadRecord,
    RetrievalCacheObjectRecord,
    RetrievalCacheStoreAccountingRecord,
    StorageIncarnationRecord,
)
from riverhog_core.services.cache_population_begin import CachePopulationBeginAdmission
from riverhog_core.services.mirrored_archive_begin import MirroredArchiveBegin
from riverhog_core.services.retrieval_cache import SqlAlchemyRetrievalCache
from riverhog_protocol.errors import Conflict
from riverhog_storage_adapter_protocol import BeginWriteOperation
from sqlalchemy import select

from tests.unit.storage_incarnation_fixtures import seed_storage_incarnation
from tests.unit.test_archive_upload_writes import _actor, _open, _stored
from tests.unit.test_archive_write_execution import _executor, _filesystem
from tests.unit.test_collection_uploads import _CREATOR
from tests.unit.test_retrieval_cache_coordinator import _registration
from tests.unit.test_retrieval_cache_write_execution import _candidate


@contextmanager
def _joint(
    tmp_path,
    monkeypatch,
    kind="pack",
    *,
    mode="direct",
    database_url=None,
    budget=1000000,
    content=None,
    existing_case=None,
    archive_root=None,
    cache_adapter=None,
    grants=None,
    segment_bytes=64 * 1024,
):
    with (
        (
            nullcontext(cache_adapter)
            if cache_adapter is not None
            else _filesystem(tmp_path / "cache", segment_bytes=segment_bytes)
        ) as adapter,
        _executor(adapter, mode) as transport,
    ):
        candidate = _candidate(transport)
        registration = replace(_registration(candidate.name), admission_budget_bytes=budget)

        def configure(service):
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

        with _actor(
            tmp_path,
            monkeypatch,
            kind,
            # Archive lookup needs qualified absence; the supplied profile
            # exercises the independent cache child.
            mode="direct" if mode == "supplied" else mode,
            database_url=database_url,
            content=content,
            configure_service=configure,
            use_cache=True,
            existing_case=existing_case,
            archive_root=archive_root,
            segment_bytes=segment_bytes,
        ) as actor:
            case = actor.case
            cache = CachePopulationBeginAdmission(case.factory)

            def validate(session, upload, object_id, expected_bytes):
                volume = session.get(
                    CollectionArchiveObjectUploadRecord,
                    (case.collection_id, object_id),
                    with_for_update=True,
                )
                assert upload.collection_id == case.collection_id
                assert object_id == case.object_id and volume is not None
                # Bind the selected cache population to the same original volume
                # begin already admitted by the archive owner.
                effects = list(
                    session.scalars(
                        select(ArchiveWriteEffectRecord)
                        .where(
                            ArchiveWriteEffectRecord.collection_id == case.collection_id,
                            ArchiveWriteEffectRecord.object_id == object_id,
                            ArchiveWriteEffectRecord.kind == "begin",
                            ArchiveWriteEffectRecord.state == "armed",
                        )
                        .limit(2)
                    )
                )
                assert len(effects) == 1
                original = json.loads(effects[0].request_json)["arguments"]["operation"]["request"]
                assert original["object_path"] == volume.object_path
                assert int(original["expected_bytes"]) == expected_bytes

            mirror = MirroredArchiveBegin(
                actor.case.factory,
                archive=actor.journal,
                cache=cache,
                candidate=candidate,
                registration=registration,
                owner="joint-cache",
                principal=_CREATOR,
                grants=grants
                or CacheWriteGrants(
                    calls=4,
                    control_bytes=100000,
                    result_bytes=10000,
                    cleanup_bytes=1000,
                ),
                guard=case.service._payload_write_guard(case.collection_id, _CREATOR),
                validate_population=validate,
            )
            execute = actor.journal.execute_write_operation

            def dispatch(operation, *, content=b""):
                if isinstance(operation, BeginWriteOperation):
                    assert not content
                    return mirror.begin(operation)
                return execute(operation, content=content)

            monkeypatch.setattr(actor.journal, "execute_write_operation", dispatch)
            yield SimpleNamespace(
                actor=actor,
                mirror=mirror,
                cache=cache,
                candidate=candidate,
                adapter=adapter,
                registration=registration,
            )


def _cache_state(joint):
    case = joint.actor.case
    with session_scope(case.factory) as session:
        row = session.get(CachePopulationBeginRecord, joint.mirror.cache_begin_call_id)
        assert row is not None
        return (
            row.state,
            row.unresolved,
            row.request_json,
            row.observation_json,
            row.result_json,
            session.get(RetrievalCacheStoreAccountingRecord, joint.candidate.name).reserved_bytes,
        )


def _terminal(joint):
    actor = joint.actor
    return actor.admission.select_terminal(
        actor.case.collection_id,
        kind="cancel",
        request={"fixture": "joint-begin"},
        principal_id=_CREATOR.id,
        grants=actor.case.service._config.archive_write,
        guard=actor.case.service._payload_write_guard(actor.case.collection_id, _CREATOR),
        validate_selection=lambda *args: None,
    )


def _assert_joint_begin(tmp_path, monkeypatch, kind, mode, *, database_url=None):
    with _joint(tmp_path, monkeypatch, kind, mode=mode, database_url=database_url) as joint:
        actor = joint.actor
        physical = []

        def inspect():
            with session_scope(actor.case.factory) as session:
                if session.get_bind().dialect.name == "sqlite":
                    session.connection(execution_options={"sqlite_immediate_writer": True})
                assert (
                    session.scalar(
                        select(CollectionUploadRecord)
                        .where(CollectionUploadRecord.collection_id == actor.case.collection_id)
                        .with_for_update(nowait=True)
                    )
                    is not None
                )
                archive = session.scalar(
                    select(ArchiveWriteEffectRecord).where(ArchiveWriteEffectRecord.kind == "begin")
                )
                cache = session.get(CachePopulationBeginRecord, joint.mirror.cache_begin_call_id)
                assert archive is not None and cache is not None
                assert archive.state == cache.state == "armed"
                assert archive.unresolved and cache.unresolved
                assert archive.id != cache.id

        archive_execute = actor.executor.execute_write
        cache_execute = joint.candidate.execute_write

        def archive_call(invocation, **kwargs):
            if invocation.operation.kind == "begin":
                inspect()
                physical.append(("archive", invocation))
            return archive_execute(invocation, **kwargs)

        def cache_call(invocation, **kwargs):
            inspect()
            physical.append(("cache", invocation))
            return cache_execute(invocation, **kwargs)

        monkeypatch.setattr(actor.executor, "execute_write", archive_call)
        monkeypatch.setattr(joint.candidate, "execute_write", cache_call)
        opened = _open(actor)
        original = joint.mirror.archive_response
        assert original is not None and opened.write_token == original.result.write_token
        assert opened.object_path == original.result.object_path
        assert [side for side, _ in physical] == ["archive", "cache"]
        archive_op, cache_op = (inv.operation for _, inv in physical)
        assert archive_op.request.object_path != cache_op.request.object_path
        assert archive_op.request.expected_bytes == cache_op.request.expected_bytes
        assert cache_op.request.content_type == "application/octet-stream"
        assert cache_op.request.placement_policy == "immediate_default"
        assert (
            cache_op.request.required_identity_assertions
            != archive_op.request.required_identity_assertions
        )
        cache_state = _cache_state(joint)
        assert cache_state[-1] == original.result.expected_bytes
        if mode == "supplied":
            assert cache_state[:2] == ("uncertain", True) and joint.mirror.cache_session is None
        else:
            assert cache_state[:2] == ("settled", False)
            session = joint.mirror.cache_session
            assert session is not None and session.write_token != opened.write_token
            assert session.object_path == cache_op.request.object_path
            assert _stored(actor) == (opened.to_json(), [])
        with session_scope(actor.case.factory) as session:
            assert session.scalar(select(RetrievalCacheObjectRecord.object_id)) is None
        before = list(physical)
        with pytest.raises(Conflict, match="already attempted"):
            joint.mirror.begin(archive_op)
        assert physical == before


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("mode", ("direct", "owned-http", "supplied"))
def test_joint_begin_commits_both_original_intents_and_keeps_archive_checkpoint_identity(
    tmp_path,
    monkeypatch,
    kind,
    mode,
):
    _assert_joint_begin(tmp_path, monkeypatch, kind, mode)


def test_optional_data_absence_continues_archive_without_cache_dispatch(tmp_path, monkeypatch):
    with _joint(tmp_path, monkeypatch, budget=1) as joint:
        opened = _open(joint.actor)
        assert opened.write_token == joint.mirror.archive_response.result.write_token
        assert joint.mirror.cache_begin_call_id is None
        with session_scope(joint.actor.case.factory) as session:
            assert session.scalar(select(CachePopulationBeginRecord.id)) is None
            assert (
                session.get(
                    RetrievalCacheStoreAccountingRecord, joint.candidate.name
                ).reserved_bytes
                == 0
            )
        assert not list((tmp_path / "cache" / "writes").iterdir())


def test_sibling_authority_refusal_withdraws_unused_archive_before_physical_begin(
    tmp_path, monkeypatch
):
    with _joint(tmp_path, monkeypatch) as joint:
        arm = joint.cache.arm

        def terminal(*args, **kwargs):
            _terminal(joint)
            return arm(*args, **kwargs)

        monkeypatch.setattr(joint.cache, "arm", terminal)
        with pytest.raises(Conflict):
            _open(joint.actor)
        assert [x.operation.kind for x in joint.actor.seen] == ["lookup"]
        assert _stored(joint.actor)[1] == []
        assert joint.mirror.archive_response is None
        assert not list((tmp_path / "provider" / "writes").iterdir())
        assert not list((tmp_path / "cache" / "writes").iterdir())


@pytest.mark.parametrize("committed", (False, True))
def test_sibling_refusal_retains_exact_unused_archive_proof_across_settlement_loss(
    tmp_path,
    monkeypatch,
    committed,
):
    with _joint(tmp_path, monkeypatch) as joint:
        proofs = []
        settle = joint.actor.admission.settle_withdrawn

        def refused(*args, **kwargs):
            raise Conflict("injected selected cache authority refusal")

        def lost(proof):
            proofs.append(proof)
            if committed or len(proofs) > 1:
                settle(proof)
            if len(proofs) == 1:
                raise OSError("injected withdrawal acknowledgement loss")

        monkeypatch.setattr(joint.cache, "arm", refused)
        monkeypatch.setattr(joint.actor.admission, "settle_withdrawn", lost)
        with pytest.raises(OSError, match="acknowledgement loss"):
            _open(joint.actor)
        assert [x.operation.kind for x in joint.actor.seen] == ["lookup"]
        joint.actor.journal.flush_withdrawals()
        assert len(proofs) == 2 and proofs[0] is proofs[1]
        assert _stored(joint.actor)[1] == []
        assert not list((tmp_path / "provider" / "writes").iterdir())


def test_lost_archive_begin_response_withdraws_unused_cache_without_releasing_reservation(
    tmp_path,
    monkeypatch,
):
    with _joint(tmp_path, monkeypatch) as joint:
        execute = joint.actor.executor.execute_write

        def lost(invocation, **kwargs):
            response = execute(invocation, **kwargs)
            if invocation.operation.kind == "begin":
                raise OSError("injected lost archive begin response")
            return response

        monkeypatch.setattr(joint.actor.executor, "execute_write", lost)
        with pytest.raises(OSError, match="lost archive begin"):
            _open(joint.actor)
        cache = _cache_state(joint)
        assert cache[:2] == ("settled", False) and cache[-1] > 0
        assert json.loads(cache[4]) == {"dispatch": "never-started"}
        assert len(list((tmp_path / "provider" / "writes").iterdir())) == 1
        assert not list((tmp_path / "cache" / "writes").iterdir())
        assert joint.actor.admission.unresolved(joint.actor.case.collection_id, limit=1)


def _assert_cache_report_retry(tmp_path, monkeypatch, committed, *, database_url=None):
    with _joint(tmp_path, monkeypatch, database_url=database_url) as joint:
        report = joint.cache.record_response
        calls = []

        def lost(ticket, response):
            calls.append((ticket, response))
            if committed or len(calls) > 1:
                report(ticket, response)
            if len(calls) == 1:
                raise OSError("injected cache report acknowledgement loss")

        monkeypatch.setattr(joint.cache, "record_response", lost)
        opened = _open(joint.actor)
        assert opened.write_token == joint.mirror.archive_response.result.write_token
        assert joint.mirror.cache_session is None
        physical = list(joint.actor.seen)
        joint.mirror.flush_cache_report()
        assert len(calls) == 2 and calls[0][0] is calls[1][0] and calls[0][1] is calls[1][1]
        assert joint.actor.seen == physical
        assert joint.mirror.cache_session is not None
        assert _stored(joint.actor) == (opened.to_json(), [])
        assert _cache_state(joint)[:2] == ("settled", False)
        assert len(list((tmp_path / "cache" / "writes").iterdir())) == 1


@pytest.mark.parametrize("committed", (False, True))
def test_cache_report_loss_keeps_original_archive_checkpoint_and_retries_without_resend(
    tmp_path,
    monkeypatch,
    committed,
):
    _assert_cache_report_retry(tmp_path, monkeypatch, committed)


def _assert_late_cache_begin(tmp_path, monkeypatch, *, database_url=None):
    with _joint(tmp_path, monkeypatch, mode="owned-http", database_url=database_url) as joint:
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()
        begin = joint.adapter.begin_write
        calls = []

        def held(request):
            calls.append(request)
            entered.set()
            try:
                assert release.wait(timeout=10)
                return begin(request)
            finally:
                finished.set()

        monkeypatch.setattr(joint.adapter, "begin_write", held)
        with ThreadPoolExecutor(max_workers=1) as pool:
            opening = pool.submit(_open, joint.actor)
            try:
                assert entered.wait(timeout=5)
                _terminal(joint)
                assert joint.actor.case.service._retrieval_cache.release(owner="joint-cache") == 0
                opened = opening.result(timeout=5)
                assert opened.write_token == joint.mirror.archive_response.result.write_token
                assert _stored(joint.actor) == (opened.to_json(), [])
                before = _cache_state(joint)
                assert before[:2] == ("uncertain", True) and before[-1] > 0
                assert joint.mirror.cache_session is None
                assert not list((tmp_path / "cache" / "writes").iterdir())
                release.set()
                assert finished.wait(timeout=5) and len(calls) == 1
                assert len(list((tmp_path / "cache" / "writes").iterdir())) == 1
                assert _cache_state(joint) == before
                assert joint.cache.unresolved(joint.actor.case.collection_id, limit=1) == (
                    joint.mirror.cache_begin_call_id,
                )
            finally:
                release.set()
                assert finished.wait(timeout=5)


def test_late_http_cache_creation_cannot_erase_independently_checkpointed_archive_begin(
    tmp_path,
    monkeypatch,
):
    _assert_late_cache_begin(tmp_path, monkeypatch)


def test_prephysical_cache_refusal_is_not_optional_and_preserves_original_archive_outcome(
    tmp_path,
    monkeypatch,
):
    with _joint(tmp_path, monkeypatch) as joint:

        def refused(*args, **kwargs):
            raise Conflict("injected original cache invocation refusal")

        monkeypatch.setattr(joint.cache, "execute", refused)
        with pytest.raises(Conflict, match="invocation refusal"):
            _open(joint.actor)
        assert joint.mirror.archive_response is not None
        pending = joint.actor.journal._pending
        assert len(pending) == 1 and pending[0][1] is joint.mirror.archive_response
        state = _cache_state(joint)
        assert state[:2] == ("settled", False)
        assert json.loads(state[4]) == {"dispatch": "never-started"}
        assert state[-1] > 0 and not list((tmp_path / "cache" / "writes").iterdir())


def _assert_cache_failure_after_dispatch(
    tmp_path,
    monkeypatch,
    interrupted,
    *,
    database_url=None,
):
    with _joint(tmp_path, monkeypatch, database_url=database_url) as joint:
        execute = joint.candidate.execute_write
        error = (
            KeyboardInterrupt("injected cache execution interrupt")
            if interrupted
            else OSError("injected lost cache begin result")
        )
        calls = []

        def failed(invocation, **kwargs):
            calls.append(invocation)
            execute(invocation, **kwargs)
            raise error

        monkeypatch.setattr(joint.candidate, "execute_write", failed)
        if interrupted:
            with pytest.raises(KeyboardInterrupt) as caught:
                _open(joint.actor)
            assert caught.value is error
            assert joint.actor.journal._pending[0][1] is joint.mirror.archive_response
        else:
            opened = _open(joint.actor)
            assert opened.write_token == joint.mirror.archive_response.result.write_token
            assert _stored(joint.actor) == (opened.to_json(), [])
        assert joint.mirror.archive_response is not None
        assert joint.mirror.cache_session is None
        state = _cache_state(joint)
        assert state[:2] == ("uncertain", True) and state[-1] > 0
        assert state[3:5] == (None, None)
        assert len(calls) == 1
        assert len(list((tmp_path / "provider" / "writes").iterdir())) == 1
        assert len(list((tmp_path / "cache" / "writes").iterdir())) == 1


@pytest.mark.parametrize("interrupted", (False, True))
def test_cache_failure_after_physical_dispatch_keeps_original_archive_and_interrupt(
    tmp_path,
    monkeypatch,
    interrupted,
):
    _assert_cache_failure_after_dispatch(tmp_path, monkeypatch, interrupted)
