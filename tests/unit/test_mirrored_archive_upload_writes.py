from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import pytest
from riverhog_core.cache_write_grants import CacheWriteGrants
from riverhog_core.catalog_archive_write_models import ArchiveWriteEffectRecord
from riverhog_core.catalog_cache_write_models import (
    CachePopulationBeginRecord,
    CachePopulationWriteEffectRecord,
)
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import (
    CollectionArchiveObjectUploadRecord,
    CollectionUploadRecord,
    RetrievalCacheObjectRecord,
    RetrievalCacheStoreAccountingRecord,
)
from riverhog_core.services.cache_population_writes import CachePopulationWriteAdmission
from riverhog_core.services.mirrored_archive_upload_writes import MirroredArchiveUploadWrites
from riverhog_protocol.errors import Conflict
from sqlalchemy import select

from tests.unit.test_archive_upload_writes import _open, _stored, _unit
from tests.unit.test_collection_uploads import _CREATOR
from tests.unit.test_mirrored_archive_begin import _cache_state, _joint, _terminal


@contextmanager
def _writing(tmp_path, monkeypatch, kind="pack", *, mode="direct", database_url=None, content=None):
    with _joint(
        tmp_path, monkeypatch, kind, mode=mode, database_url=database_url, content=content
    ) as joint:
        actor = joint.actor
        with session_scope(actor.case.factory) as session:
            upload = session.get(CollectionUploadRecord, actor.case.collection_id)
            source_store, passphrase_id = upload.archive_store, upload.passphrase_id

        def validate(session, upload, object_id, expected_bytes):
            volume = session.get(
                CollectionArchiveObjectUploadRecord,
                (actor.case.collection_id, object_id),
                with_for_update=True,
            )
            assert volume is not None and object_id == actor.case.object_id
            assert upload.collection_id == actor.case.collection_id
            assert expected_bytes == joint.mirror.cache_session.expected_bytes

        joint.writes = CachePopulationWriteAdmission(actor.case.factory)
        joint.writer = MirroredArchiveUploadWrites(
            archive=actor.journal,
            begin=joint.mirror,
            cache=joint.writes,
            candidate=joint.candidate,
            source_store=source_store,
            owner="joint-cache",
            principal=_CREATOR,
            grants=CacheWriteGrants(
                calls=4, control_bytes=100000, result_bytes=10000, cleanup_bytes=1000
            ),
            guard=actor.case.service._payload_write_guard(actor.case.collection_id, _CREATOR),
            validate_population=validate,
        )
        actor.store._executor = joint.writer
        actor.uploader = (
            actor.case.service._pack_uploader(
                actor.store, passphrase_id=passphrase_id, checkpoint_store=joint.writer
            )
            if kind == "pack"
            else actor.case.service._raw_uploader(
                actor.store, passphrase_id=passphrase_id, checkpoint_store=joint.writer
            )
        )
        yield joint


def _effects(joint):
    with session_scope(joint.actor.case.factory) as session:
        return [
            (row.id, row.kind, row.state, row.unresolved, row.result_json, row.observation_json)
            for row in session.scalars(
                select(CachePopulationWriteEffectRecord).order_by(
                    CachePopulationWriteEffectRecord.id
                )
            )
        ]


def _merge(joint, checkpoint_json):
    method = (
        joint.writer.merge_pack_upload_checkpoint
        if joint.actor.kind == "pack"
        else joint.writer.merge_raw_upload_checkpoint
    )
    return method(
        collection_id=joint.actor.case.collection_id,
        volume_id=joint.actor.case.object_id,
        checkpoint_json=checkpoint_json,
    )


def _assert_joint_upload(tmp_path, monkeypatch, kind, mode, *, database_url=None):
    with _writing(tmp_path, monkeypatch, kind, mode=mode, database_url=database_url) as joint:
        actor = joint.actor
        physical = []
        archive_execute, cache_execute = actor.executor.execute_write, joint.candidate.execute_write
        list_segments, complete_write = joint.adapter.list_segments, joint.adapter.complete_write
        issued = {}

        def cache_page(request):
            page = list_segments(request)
            if page.completion is None:
                return page
            native = page.completion
            exposed = native.model_copy(update={"state_token": "cache:" + native.state_token})
            issued[request.session.write_token] = (native, exposed)
            return page.model_copy(update={"completion": exposed})

        def cache_complete(request):
            native, exposed = issued[request.session.write_token]
            assert request.completion == exposed
            return complete_write(request.model_copy(update={"completion": native}))

        # The selected cache issues an opaque token in its own namespace, while
        # real filesystem writes and owned HTTP transport still perform the work.
        monkeypatch.setattr(joint.adapter, "list_segments", cache_page)
        monkeypatch.setattr(joint.adapter, "complete_write", cache_complete)

        def inspect(invocation):
            if invocation.operation.kind not in {"part", "complete"}:
                return
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
                    select(ArchiveWriteEffectRecord).where(
                        ArchiveWriteEffectRecord.kind == invocation.operation.kind,
                        ArchiveWriteEffectRecord.unresolved.is_(True),
                    )
                )
                cache = session.scalar(
                    select(CachePopulationWriteEffectRecord).where(
                        CachePopulationWriteEffectRecord.kind == invocation.operation.kind,
                        CachePopulationWriteEffectRecord.unresolved.is_(True),
                    )
                )
                assert archive is not None and cache is not None
                assert archive.state == cache.state == "armed" and archive.id != cache.id

        def archive(invocation, **kwargs):
            inspect(invocation)
            physical.append(("archive", invocation))
            return archive_execute(invocation, **kwargs)

        def cache(invocation, **kwargs):
            inspect(invocation)
            physical.append(("cache", invocation))
            return cache_execute(invocation, **kwargs)

        monkeypatch.setattr(actor.executor, "execute_write", archive)
        monkeypatch.setattr(joint.candidate, "execute_write", cache)
        opened = _open(actor)
        completed = _unit(actor, opened)
        assert completed.completed is not None and _stored(actor) == (completed.to_json(), [])
        assert [(side, inv.operation.kind) for side, inv in physical] == [
            ("archive", "lookup"),
            ("archive", "begin"),
            ("cache", "begin"),
            ("archive", "part"),
            ("cache", "part"),
            ("archive", "complete"),
            ("cache", "complete"),
        ]
        archive_part, cache_part = (inv.operation for _, inv in physical[3:5])
        assert archive_part.request.session.write_token == opened.write_token
        assert cache_part.request.session == joint.mirror.cache_session
        assert archive_part.stored_sha256 == cache_part.stored_sha256
        assert archive_part.request.number == cache_part.request.number
        archive_complete, cache_complete = (inv.operation for _, inv in physical[5:7])
        assert archive_complete.request.completion.state_token != (
            cache_complete.request.completion.state_token
        )
        assert cache_complete.request.expected_content_type == "application/octet-stream"
        assert cache_complete.request.expected_placement_policy == "immediate_default"
        assert cache_complete.request.required_identity_assertions != (
            archive_complete.request.required_identity_assertions
        )
        effects = _effects(joint)
        assert len(effects) == 1 and effects[0][1:4] == ("complete", "settled", False)
        assert json.loads(effects[0][4])["outcome"] == "completed"
        assert _cache_state(joint)[-1] == joint.mirror.archive_response.result.expected_bytes
        with session_scope(actor.case.factory) as session:
            assert session.scalar(select(RetrievalCacheObjectRecord.object_id)) is None
            begin = session.get(CachePopulationBeginRecord, joint.mirror.cache_begin_call_id)
            assert begin.completion_call_id == effects[0][0]


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("mode", ("direct", "owned-http"))
def test_actual_joint_parts_complete_use_independent_intents_tokens_and_checkpoints(
    tmp_path, monkeypatch, kind, mode
):
    _assert_joint_upload(tmp_path, monkeypatch, kind, mode)


def test_unknown_supplied_cache_begin_does_not_create_forward_cache_children(tmp_path, monkeypatch):
    with _writing(tmp_path, monkeypatch, mode="supplied") as joint:
        opened = _open(joint.actor)
        completed = _unit(joint.actor, opened)
        assert completed.completed is not None
        assert _stored(joint.actor) == (completed.to_json(), [])
        assert joint.mirror.cache_session is None and _effects(joint) == []
        assert _cache_state(joint)[:2] == ("uncertain", True)


def _assert_archive_checkpoint_retry(tmp_path, monkeypatch, committed, *, database_url=None):
    with _writing(tmp_path, monkeypatch, database_url=database_url) as joint:
        opened = _open(joint.actor)
        apply = joint.actor.admission.apply_reports
        save = joint.actor.case.service._checkpoints.apply_pack_upload_checkpoint
        attempts, checkpoints = [], []

        def checkpoint(**arguments):
            checkpoints.append(arguments["checkpoint_json"])
            if not committed and len(checkpoints) == 1:
                raise OSError("injected archive checkpoint acknowledgement loss")
            return save(**arguments)

        def lost(reports, *, apply_result):
            attempts.append(reports)
            apply(reports, apply_result=apply_result)
            if committed and len(attempts) == 1:
                raise OSError("injected archive checkpoint acknowledgement loss")

        monkeypatch.setattr(
            joint.actor.case.service._checkpoints, "apply_pack_upload_checkpoint", checkpoint
        )
        monkeypatch.setattr(joint.actor.admission, "apply_reports", lost)
        with pytest.raises(OSError, match="acknowledgement loss"):
            _unit(joint.actor, opened)
        effects = _effects(joint)
        assert len(effects) == 1 and effects[0][1:4] == ("part", "settled", False)
        before = list(joint.actor.seen)
        joint.writer.flush_cache_reports()
        assert _effects(joint) == effects and joint.actor.seen == before
        assert _merge(joint, checkpoints[0]) == checkpoints[0]
        assert _effects(joint) == [] and joint.actor.seen == before
        assert len(attempts) == 2
        assert attempts[0][0].ticket is attempts[1][0].ticket
        assert len(checkpoints) == (1 if committed else 2)
        assert all(value == checkpoints[0] for value in checkpoints)
        assert _stored(joint.actor) == (checkpoints[0], [])


def _assert_cache_report_retry(tmp_path, monkeypatch, committed, *, database_url=None):
    with _writing(tmp_path, monkeypatch, database_url=database_url) as joint:
        opened = _open(joint.actor)
        report, attempts = joint.writes.record_response, []
        held = True

        def lost(ticket, response):
            attempts.append((ticket, response))
            if committed or not held:
                report(ticket, response)
            if held:
                raise OSError("injected cache part report acknowledgement loss")

        monkeypatch.setattr(joint.writes, "record_response", lost)
        with pytest.raises(OSError, match="acknowledgement loss"):
            _unit(joint.actor, opened)
        saved, archive_effects = _stored(joint.actor)
        assert saved != opened.to_json() and archive_effects == []
        effects = _effects(joint)
        assert len(effects) == 1 and effects[0][1] == "part"
        assert effects[0][2:4] == (("settled", False) if committed else ("armed", True))
        before = list(joint.actor.seen)
        held = False
        joint.writer.flush_cache_reports()
        assert _effects(joint) == [] and _stored(joint.actor) == (saved, [])
        assert joint.actor.seen == before
        assert len(attempts) >= 2
        assert all(ticket is attempts[0][0] for ticket, _ in attempts)
        assert all(response is attempts[0][1] for _, response in attempts)


def _assert_cache_retirement_retry(tmp_path, monkeypatch, committed, *, database_url=None):
    with _writing(tmp_path, monkeypatch, database_url=database_url) as joint:
        opened = _open(joint.actor)
        acknowledge, attempts = joint.writes.acknowledge_report, []
        held = True

        def lost(ticket):
            attempts.append(ticket)
            assert _stored(joint.actor)[1] == []
            if held:
                if committed and len(attempts) == 1:
                    acknowledge(ticket)
                raise OSError("injected cache retirement acknowledgement loss")
            return acknowledge(ticket)

        monkeypatch.setattr(joint.writes, "acknowledge_report", lost)
        with pytest.raises(OSError, match="acknowledgement loss"):
            _unit(joint.actor, opened)
        saved = _stored(joint.actor)
        assert len(_effects(joint)) == (0 if committed else 1)
        before = list(joint.actor.seen)
        held = False
        joint.writer.flush_cache_reports()
        assert _effects(joint) == [] and _stored(joint.actor) == saved
        assert joint.actor.seen == before
        assert len(attempts) >= 2 and all(ticket is attempts[0] for ticket in attempts)


@pytest.mark.parametrize("phase", ("part", "complete"))
def test_joint_sibling_admission_refusal_withdraws_archive_before_either_physical_call(
    tmp_path, monkeypatch, phase
):
    with _writing(tmp_path, monkeypatch) as joint:
        opened = _open(joint.actor)
        arm = joint.writes.arm

        def refused(*args, **kwargs):
            if kwargs["operation"].kind == phase:
                _terminal(joint)
            return arm(*args, **kwargs)

        monkeypatch.setattr(joint.writes, "arm", refused)
        with pytest.raises(Conflict):
            _unit(joint.actor, opened)
        assert phase not in [inv.operation.kind for inv in joint.actor.seen]
        assert _stored(joint.actor)[1] == []
        assert _effects(joint) == []
        assert _cache_state(joint)[-1] > 0


def _assert_cache_child_failure(tmp_path, monkeypatch, phase, interrupted, *, database_url=None):
    with _writing(tmp_path, monkeypatch, database_url=database_url) as joint:
        opened = _open(joint.actor)
        execute = joint.candidate.execute_write
        error = (
            KeyboardInterrupt("injected cache child interruption")
            if interrupted
            else OSError("injected lost cache child result")
        )
        calls = []

        def failed(invocation, **kwargs):
            calls.append(invocation)
            result = execute(invocation, **kwargs)
            if invocation.operation.kind == phase:
                raise error
            return result

        monkeypatch.setattr(joint.candidate, "execute_write", failed)
        if interrupted:
            with pytest.raises(KeyboardInterrupt) as caught:
                _unit(joint.actor, opened)
            assert caught.value is error
            assert joint.actor.journal._pending
        else:
            completed = _unit(joint.actor, opened)
            assert completed.completed is not None
            assert _stored(joint.actor) == (completed.to_json(), [])
        effects = _effects(joint)
        assert len(effects) == 1 and effects[0][1:4] == (phase, "uncertain", True)
        assert effects[0][4:] == (None, None)
        assert [inv.operation.kind for inv in calls] == (
            ["part"] if phase == "part" else ["part", "complete"]
        )
        assert _cache_state(joint)[-1] == joint.mirror.archive_response.result.expected_bytes
        with session_scope(joint.actor.case.factory) as session:
            assert session.scalar(select(RetrievalCacheObjectRecord.object_id)) is None
            assert (
                session.get(
                    RetrievalCacheStoreAccountingRecord, joint.candidate.name
                ).reserved_bytes
                > 0
            )


@pytest.mark.parametrize("phase", ("part", "complete"))
@pytest.mark.parametrize("interrupted", (False, True))
def test_cache_child_physical_failure_keeps_original_archive_and_interruption(
    tmp_path, monkeypatch, phase, interrupted
):
    _assert_cache_child_failure(tmp_path, monkeypatch, phase, interrupted)


@pytest.mark.parametrize("phase", ("part", "complete"))
def test_prephysical_cache_child_refusal_preserves_archive_and_settles_unused_sibling(
    tmp_path, monkeypatch, phase
):
    with _writing(tmp_path, monkeypatch) as joint:
        opened = _open(joint.actor)
        execute = joint.writes.execute

        def refused(ticket, **kwargs):
            with session_scope(joint.actor.case.factory) as session:
                kind = session.get(CachePopulationWriteEffectRecord, ticket.call_id).kind
            if kind == phase:
                raise Conflict("injected prephysical cache child refusal")
            return execute(ticket, **kwargs)

        monkeypatch.setattr(joint.writes, "execute", refused)
        with pytest.raises(Conflict, match="prephysical"):
            _unit(joint.actor, opened)
        assert joint.actor.journal._pending
        assert joint.actor.journal._pending[-1][1].outcome == (
            "part" if phase == "part" else "completed"
        )
        assert _effects(joint) == []
        assert _cache_state(joint)[-1] > 0


def test_changed_joint_ciphertext_withdraws_both_unused_original_invocations(tmp_path, monkeypatch):
    with _writing(tmp_path, monkeypatch) as joint:
        opened = _open(joint.actor)
        execute = joint.writer.execute_write_operation

        def changed(operation, *, content=b""):
            if operation.kind == "part":
                content += b"changed"
            return execute(operation, content=content)

        monkeypatch.setattr(joint.writer, "execute_write_operation", changed)
        with pytest.raises(Conflict, match="ciphertext"):
            _unit(joint.actor, opened)
        assert [inv.operation.kind for inv in joint.actor.seen] == ["lookup", "begin"]
        assert _stored(joint.actor) == (opened.to_json(), [])
        assert _effects(joint) == []


def test_cache_completion_extent_mismatch_refuses_before_either_completion_dispatch(
    tmp_path, monkeypatch
):
    with _writing(tmp_path, monkeypatch) as joint:
        opened = _open(joint.actor)
        listing = joint.adapter.list_segments

        def changed(request):
            page = listing(request)
            if page.completion is None:
                return page
            return page.model_copy(
                update={
                    "completion": page.completion.model_copy(
                        update={"stored_bytes": page.completion.stored_bytes + 1}
                    )
                }
            )

        monkeypatch.setattr(joint.adapter, "list_segments", changed)
        with pytest.raises(Conflict, match="extent"):
            _unit(joint.actor, opened)
        assert "complete" not in [inv.operation.kind for inv in joint.actor.seen]
        assert _stored(joint.actor)[1] == [] and _effects(joint) == []
        assert _cache_state(joint)[-1] > 0


def _assert_late_cache_child(tmp_path, monkeypatch, phase, *, database_url=None):
    with _writing(tmp_path, monkeypatch, mode="owned-http", database_url=database_url) as joint:
        opened = _open(joint.actor)
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()
        name = "write_segment" if phase == "part" else "complete_write"
        execute = getattr(joint.adapter, name)
        calls = []

        def held(*args, **kwargs):
            calls.append((args, kwargs))
            entered.set()
            try:
                assert release.wait(timeout=10)
                return execute(*args, **kwargs)
            finally:
                finished.set()

        monkeypatch.setattr(joint.adapter, name, held)
        with ThreadPoolExecutor(max_workers=1) as pool:
            work = pool.submit(_unit, joint.actor, opened)
            try:
                assert entered.wait(timeout=5)
                _terminal(joint)
                assert joint.actor.case.service._retrieval_cache.release(owner="joint-cache") == 0
                if phase == "part":
                    # The original part checkpoint may settle, but terminal
                    # selection refuses the next archive completion admission.
                    with pytest.raises(Conflict):
                        work.result(timeout=5)
                else:
                    completed = work.result(timeout=5)
                    assert completed.completed is not None
                before = _effects(joint)
                assert len(before) == 1 and before[0][1:4] == (phase, "uncertain", True)
                original_checkpoint = _stored(joint.actor)
                assert original_checkpoint[1] == []
                release.set()
                assert finished.wait(timeout=5) and len(calls) == 1
                assert _effects(joint) == before and _stored(joint.actor) == original_checkpoint
                assert joint.writes.unresolved(joint.actor.case.collection_id, limit=1) == (
                    before[0][0],
                )
                assert _cache_state(joint)[-1] > 0
            finally:
                release.set()
                assert finished.wait(timeout=5)


@pytest.mark.parametrize("phase", ("part", "complete"))
def test_late_http_cache_child_keeps_original_charge_and_archive_checkpoint_after_terminal(
    tmp_path, monkeypatch, phase
):
    _assert_late_cache_child(tmp_path, monkeypatch, phase)


@pytest.mark.parametrize("committed", (False, True))
def test_archive_checkpoint_loss_keeps_cache_part_until_exact_acknowledgement(
    tmp_path, monkeypatch, committed
):
    _assert_archive_checkpoint_retry(tmp_path, monkeypatch, committed)


@pytest.mark.parametrize("committed", (False, True))
def test_cache_report_loss_after_archive_checkpoint_retries_original_part_without_resend(
    tmp_path, monkeypatch, committed
):
    _assert_cache_report_retry(tmp_path, monkeypatch, committed)


@pytest.mark.parametrize("committed", (False, True))
def test_cache_retirement_loss_retries_only_after_both_exact_confirmations(
    tmp_path, monkeypatch, committed
):
    _assert_cache_retirement_retry(tmp_path, monkeypatch, committed)


@pytest.mark.parametrize("kind", ("pack", "segment"))
def test_one_logical_checkpoint_confirms_all_original_cache_segments_once(
    tmp_path, monkeypatch, kind
):
    with _writing(tmp_path, monkeypatch, kind, content=b"x" * 65536) as joint:
        opened = _open(joint.actor)
        report = joint.writes.record_response
        acknowledge = joint.writes.acknowledge_report
        parts, retired = [], []

        def reported(ticket, response):
            if response.outcome == "part":
                parts.append(ticket.call_id)
            return report(ticket, response)

        def confirmed(ticket):
            assert _stored(joint.actor)[1] == []
            assert _stored(joint.actor)[0] != opened.to_json()
            retired.append(ticket.call_id)
            return acknowledge(ticket)

        monkeypatch.setattr(joint.writes, "record_response", reported)
        monkeypatch.setattr(joint.writes, "acknowledge_report", confirmed)
        completed = _unit(joint.actor, opened)
        assert completed.completed is not None
        assert len(parts) > 1 and len(parts) == len(set(parts))
        assert retired == parts
        assert [row[1] for row in _effects(joint)] == ["complete"]
        assert _stored(joint.actor) == (completed.to_json(), [])
