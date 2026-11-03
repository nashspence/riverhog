from __future__ import annotations

import hashlib
import json
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
from dataclasses import replace
from types import SimpleNamespace

import pytest
from a_riverhog_filesystem_store import FilesystemStorageAdapter, FilesystemStorageAdapterConfig
from a_riverhog_filesystem_store import incarnation as filesystem_incarnation
from riverhog_core.catalog_archive_write_models import ArchiveWriteEffectRecord
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord, CollectionUploadRecord
from riverhog_core.pack_volume import parse_pack_volume_plan
from riverhog_core.provider_write_dispatch import (
    ClosedProviderWrite,
    ProviderWriteReport,
)
from riverhog_core.raw_volume import parse_raw_volume_plan
from riverhog_core.services.archive_upload_writes import ArchiveUploadWrites
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from riverhog_core.stores.executing_archive_objects import ExecutingArchiveResumableObjectStore
from riverhog_core.stores.storage_adapter_archive_objects import (
    StorageAdapterArchiveResumableObjectStore,
)
from riverhog_protocol.errors import Conflict
from riverhog_storage_adapter_protocol import (
    CompletedWriteLookupRequest,
    unknown_write_execution,
    write_operation_sha256,
)
from sqlalchemy import select

from tests.unit.test_archive_write_execution import _executor
from tests.unit.test_collection_upload_preparation import _case
from tests.unit.test_collection_uploads import _CREATOR


@contextmanager
def _actor(
    tmp_path,
    monkeypatch,
    kind,
    *,
    mode="direct",
    database_url=None,
    content=None,
    configure_service=None,
    use_cache=False,
    existing_case=None,
    archive_root=None,
):
    with (
        nullcontext(existing_case)
        if existing_case is not None
        else _case(
            tmp_path,
            kind,
            database_url=database_url,
            content=content,
            configure_service=configure_service,
            use_cache=use_cache,
        )
    ) as case:
        if existing_case is not None and configure_service is not None:
            configure_service(case.service)
        with session_scope(case.factory) as session:
            row = session.get(
                CollectionArchiveObjectUploadRecord, (case.collection_id, case.object_id)
            )
            upload = session.get(CollectionUploadRecord, case.collection_id)
            incarnation = upload.archive_incarnation_id
            identity = {
                "kind": row.kind,
                "object_path": row.object_path,
                "plan_sha256": row.plan_sha256,
            }
            arguments = {
                "collection_id": case.collection_id,
                "object_path": row.object_path,
                "relative_path": row.relative_path,
            }
            if kind == "pack":
                arguments["plan"] = parse_pack_volume_plan(row.plan_json)
            else:
                arguments["plan"] = parse_raw_volume_plan(row.plan_json)
                arguments["target_part_plaintext_bytes"] = row.unit_plaintext_bytes
                arguments["expected_part_sha256s"] = (hashlib.sha256(case.content).hexdigest(),)
            passphrase_id = upload.passphrase_id
        root = archive_root or tmp_path / "provider"
        root.mkdir(exist_ok=archive_root is not None)
        with monkeypatch.context() as patch:
            patch.setattr(filesystem_incarnation, "uuid4", lambda: uuid.UUID(incarnation))
            assert filesystem_incarnation.provision_storage_root(root) == incarnation
        with (
            FilesystemStorageAdapter(
                FilesystemStorageAdapterConfig(
                    root=root,
                    segment_bytes=64 * 1024,
                    read_chunk_bytes=64 * 1024,
                    minimum_free_bytes=0,
                )
            ) as adapter,
            _executor(adapter, mode) as transport,
        ):
            provider = StorageAdapterArchiveResumableObjectStore(transport)
            admission = ArchiveWriteAdmission(case.factory)
            guard = case.service._payload_write_guard(case.collection_id, _CREATOR)
            epoch = admission.bind_epoch(case.collection_id, guard=guard)
            seen = []

            class AuditedExecutor:
                def execute_write(self, invocation, *, content=b""):
                    # A separate NOWAIT writer proves dispatch owns no catalog transaction.
                    with session_scope(case.factory) as session:
                        if session.get_bind().dialect.name == "sqlite":
                            session.connection(execution_options={"sqlite_immediate_writer": True})
                        upload = session.scalar(
                            select(CollectionUploadRecord)
                            .where(CollectionUploadRecord.collection_id == case.collection_id)
                            .with_for_update(nowait=True)
                        )
                        effect = session.get(ArchiveWriteEffectRecord, invocation.call.call_id)
                        assert upload is not None and effect.state == "armed" and effect.unresolved
                        stored = json.loads(effect.request_json)["arguments"]
                        assert stored["operation"] == invocation.operation.model_dump(mode="json")
                        assert stored["provider_request_sha256"] == write_operation_sha256(
                            invocation.operation
                        )
                        assert effect.request_sha256 != invocation.call.exact_request_sha256
                        assert effect.executor_id == invocation.call.dispatch_owner
                    seen.append(invocation)
                    return provider.execute_write(invocation, content=content)

            executor = AuditedExecutor()
            journal = ArchiveUploadWrites(
                admission=admission,
                checkpoints=case.service._checkpoints,
                executor=executor,
                collection_id=case.collection_id,
                object_id=case.object_id,
                epoch=epoch,
                grants=replace(case.service._config.archive_write, control_bytes=8 * 1024 * 1024),
                guard=guard,
            )
            store = ExecutingArchiveResumableObjectStore(
                provider, journal, placement_policy="archive_default"
            )
            uploader = (
                case.service._pack_uploader(
                    store, passphrase_id=passphrase_id, checkpoint_store=journal
                )
                if kind == "pack"
                else case.service._raw_uploader(
                    store, passphrase_id=passphrase_id, checkpoint_store=journal
                )
            )
            preparation = uploader.prepare(plan=arguments["plan"])
            admission.prepare(
                case.collection_id,
                case.object_id,
                epoch=epoch,
                identity=identity,
                preparation={"age_state": json.loads(preparation.to_json_bytes())},
                grants=case.service._config.archive_write,
                guard=guard,
            )
            arguments["prepared_state"] = preparation
            yield SimpleNamespace(
                case=case,
                kind=kind,
                admission=admission,
                journal=journal,
                store=store,
                uploader=uploader,
                arguments=arguments,
                executor=executor,
                seen=seen,
                provider=provider,
                adapter=adapter,
            )


def _open(actor):
    return actor.uploader.open(**actor.arguments)


def _unit(actor, checkpoint):
    arguments = {
        "plan": actor.arguments["plan"],
        "checkpoint": checkpoint,
        "unit_number": 0 if actor.kind == "pack" else 1,
    }
    if actor.kind == "pack":
        arguments["payload_chunks"] = (actor.case.content,)
    else:
        arguments["plaintext"] = actor.case.content
    return actor.uploader.upload_unit(**arguments)


def _stored(actor):
    case = actor.case
    with session_scope(case.factory) as session:
        row = session.get(CollectionArchiveObjectUploadRecord, (case.collection_id, case.object_id))
        effects = list(session.scalars(select(ArchiveWriteEffectRecord)))
        return row.checkpoint_json, [
            (e.id, e.state, e.unresolved, e.result_json, e.observation_json) for e in effects
        ]


@pytest.mark.parametrize("kind", ["pack", "segment"])
@pytest.mark.parametrize("mode", ["direct", "owned-http"])
def test_actual_uploader_commits_admitted_filesystem_outcomes_with_checkpoints(
    tmp_path, monkeypatch, kind, mode
):
    with _actor(tmp_path, monkeypatch, kind, mode=mode) as actor:
        opened = _open(actor)
        assert _stored(actor) == (opened.to_json(), [])
        completed = _unit(actor, opened)
        assert completed.completed is not None
        assert _stored(actor) == (completed.to_json(), [])
        assert [inv.operation.kind for inv in actor.seen] == ["lookup", "begin", "part", "complete"]
        assert len({inv.call.call_id for inv in actor.seen}) == 4


@pytest.mark.parametrize("kind", ["pack", "segment"])
def test_checkpoint_report_retry_after_lost_commit_ack_never_resends_provider(
    tmp_path, monkeypatch, kind
):
    with _actor(tmp_path, monkeypatch, kind) as actor:
        apply = actor.admission.apply_reports
        calls = []

        def lost(reports, *, apply_result):
            calls.append(reports)
            apply(reports, apply_result=apply_result)
            if len(calls) == 2:
                raise OSError("injected committed checkpoint acknowledgement loss")

        monkeypatch.setattr(actor.admission, "apply_reports", lost)
        with pytest.raises(OSError, match="acknowledgement loss"):
            _open(actor)
        pending = tuple(actor.journal._pending)
        assert len(pending) == 1
        stored, effects = _stored(actor)
        assert stored is not None and len(effects) == 1 and effects[0][1:3] == ("settled", False)
        before = list(actor.seen)
        assert actor.journal._report(stored, kind=kind) == stored
        assert actor.seen == before and _stored(actor) == (stored, [])


@pytest.mark.parametrize("kind", ["pack", "segment"])
def test_checkpoint_and_all_segment_reports_roll_back_together(tmp_path, monkeypatch, kind):
    with _actor(tmp_path, monkeypatch, kind) as actor:
        opened = _open(actor)
        original = (
            actor.case.service._checkpoints.apply_pack_upload_checkpoint
            if kind == "pack"
            else actor.case.service._checkpoints.apply_raw_upload_checkpoint
        )
        selected = (
            "apply_pack_upload_checkpoint" if kind == "pack" else "apply_raw_upload_checkpoint"
        )
        attempted = []

        def rollback(**arguments):
            attempted.append(arguments["checkpoint_json"])
            original(**arguments)
            raise OSError("injected atomic checkpoint rollback")

        with monkeypatch.context() as patch:
            patch.setattr(actor.case.service._checkpoints, selected, rollback)
            with pytest.raises(OSError, match="checkpoint rollback"):
                _unit(actor, opened)
        stored, effects = _stored(actor)
        assert stored == opened.to_json() and effects and all(e[2] for e in effects)
        before = list(actor.seen)
        assert actor.journal._report(attempted[0], kind=kind) is not None
        assert actor.seen == before and _stored(actor)[1] == []


@pytest.mark.parametrize("kind", ["pack", "segment"])
def test_unknown_absence_never_supplies_begin_authority(tmp_path, monkeypatch, kind):
    with _actor(tmp_path, monkeypatch, kind, mode="supplied") as actor:
        # Supplied transport cannot claim owned closure even for a positive reply.
        # The initial absent lookup itself must remain unresolved and prevent begin.
        with pytest.raises(Conflict, match="remains unknown"):
            _open(actor)
        stored, effects = _stored(actor)
        assert stored is None and len(effects) == 1 and effects[0][1:3] == ("uncertain", True)
        assert [inv.operation.kind for inv in actor.seen] == ["lookup"]
        with pytest.raises(Conflict, match="already unresolved"):
            _open(actor)
        assert len(actor.seen) == 1


@pytest.mark.parametrize("kind", ["pack", "segment"])
def test_unknown_begin_keeps_exact_armed_request_and_blocks_cold_resend(
    tmp_path, monkeypatch, kind
):
    with _actor(tmp_path, monkeypatch, kind) as actor:
        execute = actor.executor.execute_write
        invocations = []

        def lose(invocation, *, content=b""):
            response = execute(invocation, content=content)
            invocations.append(invocation)
            return (
                unknown_write_execution(invocation)
                if invocation.operation.kind == "begin"
                else response
            )

        monkeypatch.setattr(actor.executor, "execute_write", lose)
        with pytest.raises(Conflict, match="remains unknown"):
            _open(actor)
        stored, effects = _stored(actor)
        assert stored is None and len(effects) == 1 and effects[0][2]
        original = effects[0][0]
        with pytest.raises(Conflict, match="already unresolved"):
            _open(actor)
        assert _stored(actor)[1][0][0] == original
        assert [inv.operation.kind for inv in invocations] == ["lookup", "begin"]


@pytest.mark.parametrize("kind", ["pack", "segment"])
def test_positive_unknown_begin_retains_original_checkpoint_and_late_closure(
    tmp_path, monkeypatch, kind
):
    with _actor(tmp_path, monkeypatch, kind) as actor:
        execute = actor.executor.execute_write
        returned = []
        reports = []
        apply = actor.admission.apply_reports

        def unknown_positive(invocation, *, content=b""):
            response = execute(invocation, content=content)
            if invocation.operation.kind == "begin":
                returned.append(response)
                return response.model_copy(update={"execution": "unknown"})
            return response

        def capture(batch, *, apply_result):
            reports.extend(batch)
            return apply(batch, apply_result=apply_result)

        monkeypatch.setattr(actor.executor, "execute_write", unknown_positive)
        monkeypatch.setattr(actor.admission, "apply_reports", capture)
        opened = _open(actor)
        stored, effects = _stored(actor)
        assert stored == opened.to_json() and len(effects) == 1
        assert effects[0][1:3] == ("uncertain", True) and effects[0][3] is None
        original = reports[-1]
        assert original.closure is None
        assert json.loads(effects[0][4]) == original.result
        with pytest.raises(Conflict, match="already unresolved"):
            _unit(actor, opened)
        assert len(actor.seen) == 2
        ticket = original.ticket
        closure = ClosedProviderWrite(
            call_id=ticket.call_id,
            request_sha256=ticket.request_sha256,
            executor_id=ticket.executor_id,
            target_incarnation=ticket.target_incarnation,
            result=actor.journal._report_result(
                returned[0], hashlib.sha256(stored.encode()).hexdigest()
            ),
        )
        applied = []
        actor.admission.apply_reports(
            (ProviderWriteReport(ticket, original.result, closure),),
            apply_result=lambda *args: applied.append("closed"),
        )
        assert applied == ["closed"] and _stored(actor)[1][0][1:3] == ("settled", False)
        assert _stored(actor)[0] == stored and len(actor.seen) == 2


@pytest.mark.parametrize("kind", ["pack", "segment"])
def test_multiple_provider_segments_settle_with_one_logical_checkpoint(tmp_path, monkeypatch, kind):
    with _actor(tmp_path, monkeypatch, kind, content=b"x" * 65536) as actor:
        opened = _open(actor)
        apply = actor.admission.apply_reports
        batches = []

        def capture(reports, *, apply_result):
            batches.append(tuple(reports))
            return apply(reports, apply_result=apply_result)

        monkeypatch.setattr(actor.admission, "apply_reports", capture)
        completed = _unit(actor, opened)
        assert completed.completed is not None and _stored(actor)[1] == []
        part_batch = batches[0]
        assert len(part_batch) > 1
        assert len({r.result["checkpoint_sha256"] for r in part_batch}) == 1
        assert all(r.result["outcome"] == "part" and r.closure for r in part_batch)
        assert len({r.ticket.call_id for r in part_batch}) == len(part_batch)


@pytest.mark.parametrize("kind", ["pack", "segment"])
@pytest.mark.parametrize("phase", ["begin", "complete"])
def test_terminal_selection_retains_exact_late_provider_outcome(tmp_path, monkeypatch, kind, phase):
    with _actor(tmp_path, monkeypatch, kind) as actor:
        execute = actor.executor.execute_write
        selected = []

        def select_before_effect(invocation, *, content=b""):
            if invocation.operation.kind == phase:
                selected.append(
                    actor.admission.select_terminal(
                        actor.case.collection_id,
                        kind="cancel",
                        request={"fixture": phase},
                        principal_id=_CREATOR.id,
                        grants=actor.case.service._config.archive_write,
                        guard=actor.case.service._payload_write_guard(
                            actor.case.collection_id, _CREATOR
                        ),
                        validate_selection=lambda *args: None,
                    )
                )
            return execute(invocation, content=content)

        monkeypatch.setattr(actor.executor, "execute_write", select_before_effect)
        checkpoint = _open(actor)
        if phase == "complete":
            checkpoint = _unit(actor, checkpoint)
            assert checkpoint.completed is not None
        else:
            with pytest.raises(Conflict, match="admission is closed"):
                _unit(actor, checkpoint)
        assert selected and _stored(actor) == (checkpoint.to_json(), [])
        assert actor.admission.unresolved(actor.case.collection_id, limit=1) == ()
        with pytest.raises(Conflict, match="admission is closed"):
            actor.admission.bind_epoch(
                actor.case.collection_id,
                guard=actor.case.service._payload_write_guard(actor.case.collection_id, _CREATOR),
            )


@pytest.mark.parametrize("kind", ["pack", "segment"])
def test_lost_retirement_ack_retries_only_the_confirmed_exact_checkpoint(
    tmp_path, monkeypatch, kind
):
    with _actor(tmp_path, monkeypatch, kind) as actor:
        acknowledge = actor.admission.acknowledge_report
        acknowledgements = []

        def lose(ticket):
            acknowledge(ticket)
            acknowledgements.append(ticket.call_id)
            if len(acknowledgements) == 2:
                raise OSError("injected retirement acknowledgement loss")

        monkeypatch.setattr(actor.admission, "acknowledge_report", lose)
        with pytest.raises(OSError, match="retirement acknowledgement"):
            _open(actor)
        stored, effects = _stored(actor)
        assert stored is not None and effects == [] and actor.journal._retire
        before = list(actor.seen)
        with pytest.raises(Conflict, match="no admitted write outcome"):
            actor.journal._report(stored + " ", kind=kind)
        assert actor.journal._report(stored, kind=kind) == stored
        assert actor.seen == before and not actor.journal._retire


@pytest.mark.parametrize("kind", ["pack", "segment"])
def test_mixed_closed_and_unknown_segments_commit_one_checkpoint_and_keep_unknown_charge(
    tmp_path, monkeypatch, kind
):
    with _actor(tmp_path, monkeypatch, kind, content=b"x" * 65536) as actor:
        opened = _open(actor)
        execute = actor.executor.execute_write

        def unknown_first(invocation, *, content=b""):
            response = execute(invocation, content=content)
            if invocation.operation.kind == "part" and invocation.operation.request.number == 1:
                return response.model_copy(update={"execution": "unknown"})
            return response

        monkeypatch.setattr(actor.executor, "execute_write", unknown_first)
        with pytest.raises(Conflict, match="already unresolved"):
            _unit(actor, opened)
        stored, effects = _stored(actor)
        document = json.loads(stored)
        assert len(document["archive_parts"]) == 1 and document["completed"] is None
        assert len(effects) == 1 and effects[0][1:3] == ("uncertain", True)
        assert json.loads(effects[0][4])["outcome"] == "part"
        assert [inv.operation.kind for inv in actor.seen] == ["lookup", "begin", "part", "part"]


@pytest.mark.parametrize("kind", ["pack", "segment"])
def test_changed_provider_target_is_refused_before_dispatch(tmp_path, monkeypatch, kind):
    with _actor(tmp_path, monkeypatch, kind) as actor:
        opened = _open(actor)
        original = actor.seen[-1].operation
        changed = original.model_copy(
            update={
                "request": original.request.model_copy(update={"object_path": "other/volume.age"})
            }
        )
        with pytest.raises(Conflict, match="differs from the original volume"):
            actor.journal.execute_write_operation(changed)
        assert len(actor.seen) == 2 and _stored(actor) == (opened.to_json(), [])


@pytest.mark.parametrize("kind", ["pack", "segment"])
def test_retained_capacity_refusal_precedes_provider_access(tmp_path, monkeypatch, kind):
    with _actor(tmp_path, monkeypatch, kind) as actor:
        actor.journal._grants = replace(actor.journal._grants, control_bytes=1)
        with pytest.raises(Conflict, match="grant is exhausted"):
            _open(actor)
        assert actor.seen == [] and _stored(actor) == (None, [])


@pytest.mark.parametrize("kind", ["pack", "segment"])
@pytest.mark.parametrize("phase", ["begin", "complete"])
def test_actual_http_timeout_and_negative_observation_do_not_close_late_admitted_write(
    tmp_path, monkeypatch, kind, phase
):
    with _actor(tmp_path, monkeypatch, kind, mode="owned-http") as actor:
        opened = _open(actor) if phase == "complete" else None
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()
        name = "begin_write" if phase == "begin" else "complete_write"
        original = getattr(actor.adapter, name)
        calls = []

        def delayed(request):
            calls.append(request)
            entered.set()
            try:
                assert release.wait(timeout=10)
                return original(request)
            finally:
                finished.set()

        monkeypatch.setattr(actor.adapter, name, delayed)
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = (
                pool.submit(_open, actor) if opened is None else pool.submit(_unit, actor, opened)
            )
            try:
                assert entered.wait(timeout=5)
                invocation = actor.seen[-1]
                assert invocation.operation.kind == phase
                selected = actor.admission.select_terminal(
                    actor.case.collection_id,
                    kind="cancel",
                    request={"fixture": "actual-http-" + phase},
                    principal_id=_CREATOR.id,
                    grants=actor.case.service._config.archive_write,
                    guard=actor.case.service._payload_write_guard(
                        actor.case.collection_id, _CREATOR
                    ),
                    validate_selection=lambda *args: None,
                )
                assert selected and not finished.is_set()
                request = invocation.operation.request
                lookup = CompletedWriteLookupRequest(
                    object_path=request.object_path
                    if phase == "begin"
                    else request.session.object_path,
                    expected_bytes=request.expected_bytes,
                    expected_content_type=request.content_type
                    if phase == "begin"
                    else request.expected_content_type,
                    required_identity_assertions=request.required_identity_assertions,
                    expected_placement_policy=request.placement_policy
                    if phase == "begin"
                    else request.expected_placement_policy,
                )
                for _ in range(3):
                    assert actor.adapter.find_completed_write(lookup) is None
                    actor.admission.observe(
                        actor.case.collection_id,
                        invocation.call.call_id,
                        None,
                        apply_observation=lambda *args: pytest.fail(
                            "absence cannot apply a result"
                        ),
                    )
                with pytest.raises(Conflict, match="remains unknown"):
                    future.result(timeout=5)
                stored, effects = _stored(actor)
                assert len(effects) == 1 and effects[0][1:3] == ("uncertain", True)
                assert effects[0][0] == invocation.call.call_id and effects[0][3:] == (None, None)
                if phase == "begin":
                    assert (
                        stored is None and list((tmp_path / "provider" / "writes").iterdir()) == []
                    )
                else:
                    assert json.loads(stored)["completed"] is None
                release.set()
                assert finished.wait(timeout=5) and len(calls) == 1
                if phase == "begin":
                    assert len(list((tmp_path / "provider" / "writes").iterdir())) == 1
                else:
                    assert actor.adapter.find_completed_write(lookup) is not None
                # Actual remote completion after HTTP exit supplies no report to
                # this actor. Earlier absence never authorized releasing its charge.
                assert _stored(actor) == (stored, effects)
                assert actor.admission.unresolved(actor.case.collection_id, limit=1) == (
                    invocation.call.call_id,
                )
            finally:
                release.set()
                assert finished.wait(timeout=5)


def test_archive_actor_admits_before_dispatch_and_withdraws_on_sibling_refusal(
    tmp_path, monkeypatch
):
    with _actor(tmp_path, monkeypatch, "segment") as actor:
        _open(actor)
        original = _stored(actor)
        operation = next(call.operation for call in actor.seen if call.operation.kind == "begin")
        seen = len(actor.seen)
        admitted = actor.journal.arm_write_operation(operation)
        assert len(actor.seen) == seen
        with session_scope(actor.case.factory) as session:
            row = session.get(ArchiveWriteEffectRecord, admitted.ticket.call_id)
            assert row.state == "armed" and row.unresolved
        with pytest.raises(Conflict, match="sibling refused"):
            try:
                raise Conflict("sibling refused before either provider started")
            finally:
                actor.journal.withdraw_admitted_write(admitted)
        assert len(actor.seen) == seen and _stored(actor) == original


def test_archive_actor_keeps_withdrawal_proof_after_settlement_acknowledgement_loss(
    tmp_path, monkeypatch
):
    with _actor(tmp_path, monkeypatch, "segment") as actor:
        _open(actor)
        original = _stored(actor)
        operation = next(call.operation for call in actor.seen if call.operation.kind == "begin")
        admitted = actor.journal.arm_write_operation(operation)
        report = actor.admission.settle_withdrawn
        reports = []

        def lose_ack(proof):
            report(proof)
            reports.append(proof)
            if len(reports) == 1:
                raise OSError("injected withdrawal acknowledgement loss")

        monkeypatch.setattr(actor.admission, "settle_withdrawn", lose_ack)
        seen = len(actor.seen)
        with pytest.raises(OSError, match="acknowledgement loss"):
            actor.journal.withdraw_admitted_write(admitted)
        assert _stored(actor)[1][0][1:3] == ("settled", False)
        actor.journal.flush_withdrawals()
        assert reports[0] is reports[1]
        assert len(actor.seen) == seen and _stored(actor) == original
        with pytest.raises(Conflict, match="live actor"):
            actor.journal.execute_admitted_write(admitted)


def test_archive_actor_refuses_copied_or_another_actors_admission(tmp_path, monkeypatch):
    with _actor(tmp_path, monkeypatch, "segment") as actor:
        _open(actor)
        operation = actor.seen[0].operation
        admitted = actor.journal.arm_write_operation(operation)
        seen = len(actor.seen)
        other = ArchiveUploadWrites(
            admission=actor.admission,
            checkpoints=actor.case.service._checkpoints,
            executor=actor.executor,
            collection_id=actor.case.collection_id,
            object_id=actor.case.object_id,
            epoch=actor.journal._epoch,
            grants=actor.journal._grants,
            guard=actor.journal._guard,
        )
        for target, invocation in ((actor.journal, replace(admitted)), (other, admitted)):
            with pytest.raises(Conflict, match="live actor"):
                target.execute_admitted_write(invocation)
            with pytest.raises(Conflict, match="live actor"):
                target.withdraw_admitted_write(invocation)
        actor.journal.withdraw_admitted_write(admitted)
        assert len(actor.seen) == seen


def test_archive_actor_executes_admitted_original_once(tmp_path, monkeypatch):
    with _actor(tmp_path, monkeypatch, "segment") as actor:
        _open(actor)
        admitted = actor.journal.arm_write_operation(actor.seen[0].operation)
        seen = len(actor.seen)
        response = actor.journal.execute_admitted_write(admitted)
        assert response.outcome == "no_effect" and len(actor.seen) == seen + 1
        with pytest.raises(Conflict, match="live actor"):
            actor.journal.execute_admitted_write(admitted)


def test_archive_actor_changed_ciphertext_refuses_before_dispatch(tmp_path, monkeypatch):
    with _actor(tmp_path, monkeypatch, "segment") as actor:
        checkpoint = _open(actor)
        content = []
        execute = actor.executor.execute_write

        def capture(invocation, **kwargs):
            if invocation.operation.kind == "part":
                content.append(kwargs["content"])
            return execute(invocation, **kwargs)

        monkeypatch.setattr(actor.executor, "execute_write", capture)
        _unit(actor, checkpoint)
        operation = next(call.operation for call in actor.seen if call.operation.kind == "part")
        admitted = actor.journal.arm_write_operation(operation)
        seen = len(actor.seen)
        with pytest.raises(Conflict, match="ciphertext"):
            actor.journal.execute_admitted_write(admitted, content=b"changed")
        assert len(actor.seen) == seen and content
        actor.journal.withdraw_admitted_write(admitted)


def test_archive_actor_running_admission_cannot_execute_twice_or_be_withdrawn(
    tmp_path, monkeypatch
):
    with _actor(tmp_path, monkeypatch, "segment") as actor:
        _open(actor)
        admitted = actor.journal.arm_write_operation(actor.seen[0].operation)
        entered, release = threading.Event(), threading.Event()
        execute = actor.executor.execute_write
        seen = len(actor.seen)

        def held(invocation, **kwargs):
            entered.set()
            assert release.wait(timeout=5)
            return execute(invocation, **kwargs)

        monkeypatch.setattr(actor.executor, "execute_write", held)
        with ThreadPoolExecutor(max_workers=1) as pool:
            running = pool.submit(actor.journal.execute_admitted_write, admitted)
            try:
                assert entered.wait(timeout=5)
                with pytest.raises(Conflict, match="live actor"):
                    actor.journal.execute_admitted_write(admitted)
                with pytest.raises(Conflict, match="live actor"):
                    actor.journal.withdraw_admitted_write(admitted)
                with session_scope(actor.case.factory) as session:
                    row = session.get(ArchiveWriteEffectRecord, admitted.ticket.call_id)
                    assert row.state == "armed" and row.unresolved
            finally:
                release.set()
            assert running.result(timeout=5).outcome == "no_effect"
        assert len(actor.seen) == seen + 1
