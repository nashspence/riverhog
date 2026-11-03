from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from threading import Event
from types import SimpleNamespace

import pytest
from riverhog_core.catalog_archive_write_models import (
    ArchiveUploadTerminalRecord,
    ArchiveWriteControlRecord,
    ArchiveWriteEffectRecord,
)
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord, StorageIncarnationRecord
from riverhog_core.services import immutable_archive_admission as admission_module
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from riverhog_core.services.immutable_archive_admission import ImmutableArchiveAdmission
from riverhog_core.stores.storage_adapter_archive_objects import (
    StorageAdapterImmutableArchiveObjectStore,
)
from riverhog_protocol.errors import Conflict, NotFound, ServiceUnavailable
from riverhog_storage_adapter_protocol import unknown_write_execution, write_operation_sha256
from sqlalchemy import event, func, select

from tests.unit.test_archive_write_execution import _executor
from tests.unit.test_collection_uploads import _CREATOR
from tests.unit.test_immutable_archive_preparation import _metadata, _prepare


def _arm(case, owner, prepared=None, **changes):
    arguments = dict(
        epoch=case.epoch,
        executor_id="e" * 64,
        grants=case.grants,
        guard=case.guard,
        validate_metadata=case.validate,
    )
    arguments.update(changes)
    return owner.arm(prepared or _prepare(case), **arguments)


def _effect(case, admitted):
    with session_scope(case.actor.case.factory) as session:
        effect = session.get(ArchiveWriteEffectRecord, admitted.ticket.call_id)
        return (
            None
            if effect is None
            else SimpleNamespace(
                state=effect.state,
                unresolved=effect.unresolved,
                observation=effect.observation_json,
                result=effect.result_json,
                request=effect.request_json,
                control_bytes=effect.control_bytes,
            )
        )


def _checkpoint(session, prepared, receipt):
    upload = session.get(CollectionUploadRecord, prepared.collection_id)
    assert upload is not None
    assert receipt.object_path == prepared.operation.request.object_path
    upload.archive_terminal_receipt_json = receipt.model_dump_json()


def _verify_checkpoint(session, prepared, receipt):
    upload = session.get(CollectionUploadRecord, prepared.collection_id)
    if upload is None or upload.archive_terminal_receipt_json != receipt.model_dump_json():
        raise Conflict("original metadata receipt has no durable receiving owner")


def _select(case):
    return ArchiveWriteAdmission(case.actor.case.factory).select_terminal(
        case.actor.case.collection_id,
        kind="cancel",
        request={"original": "metadata-owner"},
        principal_id=_CREATOR.id,
        grants=case.grants,
        guard=case.guard,
        validate_selection=lambda session, upload: None,
    )


def _assert_actual_metadata_dispatch(tmp_path, monkeypatch, mode, *, database_url=None):
    with (
        _metadata(tmp_path, monkeypatch, database_url=database_url) as case,
        _executor(case.actor.adapter, mode) as transport,
    ):
        factory = case.actor.case.factory
        owner = ImmutableArchiveAdmission(factory)
        admitted = _arm(case, owner)
        physical = StorageAdapterImmutableArchiveObjectStore(transport)
        calls = []

        class Executor:
            def execute_write(self, invocation, *, content):
                # A separate NOWAIT writer proves physical dispatch owns no
                # construction transaction and observes the committed intent.
                with session_scope(factory) as session:
                    if session.get_bind().dialect.name == "sqlite":
                        session.connection(execution_options={"sqlite_immediate_writer": True})
                    upload = session.scalar(
                        select(CollectionUploadRecord)
                        .where(
                            CollectionUploadRecord.collection_id == case.actor.case.collection_id
                        )
                        .with_for_update(nowait=True)
                    )
                    effect = session.get(ArchiveWriteEffectRecord, invocation.call.call_id)
                    assert upload is not None and effect.kind == "put"
                    assert effect.state == "armed" and effect.unresolved
                    arguments = json.loads(effect.request_json)["arguments"]
                    assert arguments["operation"] == invocation.operation.model_dump(mode="json")
                    assert arguments["provider_request_sha256"] == write_operation_sha256(
                        invocation.operation
                    )
                    assert content == admitted.prepared.content
                    assert effect.object_id != case.actor.case.object_id
                calls.append(invocation)
                return physical.execute_write(invocation, content=content)

        response = owner.execute(admitted, Executor())
        assert response.execution == "closed" and response.outcome == "completed"
        assert response.result.stored_sha256 == admitted.prepared.operation.request.stored_sha256
        owner.report(admitted, response, apply_receipt=_checkpoint)
        owner.report(admitted, response, apply_receipt=lambda *args: pytest.fail("duplicate apply"))
        assert _effect(case, admitted).state == "settled"
        with session_scope(factory) as session:
            upload = session.get(CollectionUploadRecord, admitted.prepared.collection_id)
            assert upload.archive_terminal_receipt_json == response.result.model_dump_json()
        owner.acknowledge_report(admitted, verify_receipt=_verify_checkpoint)
        assert _effect(case, admitted) is None and len(calls) == 1
        with pytest.raises(Conflict, match="used live actor"):
            owner.execute(admitted, Executor())


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_actual_metadata_dispatch_is_admitted_once_and_checkpointed_atomically(
    tmp_path, monkeypatch, mode
):
    _assert_actual_metadata_dispatch(tmp_path, monkeypatch, mode)


@pytest.mark.parametrize("committed", (False, True))
def test_uncertain_admission_commit_creates_no_live_dispatch_right(
    tmp_path, monkeypatch, committed
):
    with _metadata(tmp_path, monkeypatch) as case:
        owner = ImmutableArchiveAdmission(case.actor.case.factory)
        prepared = _prepare(case)
        transaction = admission_module.session_scope

        @contextmanager
        def lost(factory):
            with transaction(factory) as session:
                yield session
                if not committed:
                    raise OSError("injected admission reply loss")
            if committed:
                raise OSError("injected admission reply loss")

        with monkeypatch.context() as patch:
            patch.setattr(admission_module, "session_scope", lost)
            with pytest.raises(OSError, match="reply loss"):
                _arm(case, owner, prepared)
        assert owner._live == {}
        with session_scope(case.actor.case.factory) as session:
            count = session.scalar(select(func.count()).select_from(ArchiveWriteEffectRecord))
        assert count == int(committed)
        if committed:
            with pytest.raises(Conflict, match="already unresolved"):
                _arm(case, owner, prepared)
        assert case.actor.seen == []


@pytest.mark.parametrize("committed", (False, True))
def test_report_rollback_and_reply_loss_retain_original_response_without_resend(
    tmp_path, monkeypatch, committed
):
    with _metadata(tmp_path, monkeypatch) as case:
        owner = ImmutableArchiveAdmission(case.actor.case.factory)
        admitted = _arm(case, owner)
        physical = StorageAdapterImmutableArchiveObjectStore(case.actor.adapter)
        response = owner.execute(admitted, physical)
        transaction = admission_module.session_scope
        applications = []

        def apply(session, prepared, receipt):
            applications.append(receipt)
            _checkpoint(session, prepared, receipt)

        @contextmanager
        def lost(factory):
            with transaction(factory) as session:
                yield session
                if not committed:
                    raise OSError("injected checkpoint reply loss")
            if committed:
                raise OSError("injected checkpoint reply loss")

        with monkeypatch.context() as patch:
            patch.setattr(admission_module, "session_scope", lost)
            with pytest.raises(OSError, match="reply loss"):
                owner.report(admitted, response, apply_receipt=apply)
        effect = _effect(case, admitted)
        assert effect.unresolved is not committed
        with session_scope(case.actor.case.factory) as session:
            upload = session.get(CollectionUploadRecord, admitted.prepared.collection_id)
            assert (upload.archive_terminal_receipt_json is not None) is committed
        owner.report(admitted, response, apply_receipt=apply)
        assert len(applications) == (1 if committed else 2)
        owner.acknowledge_report(admitted, verify_receipt=_verify_checkpoint)
        with pytest.raises(Conflict, match="used live actor"):
            owner.execute(admitted, physical)


@pytest.mark.parametrize("retire_parent", (False, True))
def test_original_metadata_report_survives_terminal_selection_and_parent_retirement(
    tmp_path, monkeypatch, retire_parent
):
    with _metadata(tmp_path, monkeypatch) as case:
        factory = case.actor.case.factory
        owner = ImmutableArchiveAdmission(factory)
        admitted = _arm(case, owner)
        physical = StorageAdapterImmutableArchiveObjectStore(case.actor.adapter)
        response = owner.execute(admitted, physical)
        selected = _select(case)
        if retire_parent:
            with session_scope(factory) as session:
                session.delete(session.get(CollectionUploadRecord, admitted.prepared.collection_id))
        with pytest.raises((Conflict, NotFound)):
            _arm(case, owner, admitted.prepared)
        applications = []

        def apply(session, prepared, receipt):
            applications.append(receipt)
            assert prepared == admitted.prepared
            assert session.get(ArchiveUploadTerminalRecord, selected).phase == "draining"
            if not retire_parent:
                _checkpoint(session, prepared, receipt)

        owner.report(admitted, response, apply_receipt=apply)
        owner.report(admitted, response, apply_receipt=apply)
        assert len(applications) == 1 and not _effect(case, admitted).unresolved
        if retire_parent:
            with pytest.raises(Conflict, match="durable receiving owner"):
                owner.acknowledge_report(admitted, verify_receipt=_verify_checkpoint)
            assert _effect(case, admitted).result is not None
        else:
            owner.acknowledge_report(admitted, verify_receipt=_verify_checkpoint)


def test_positive_unknown_outcome_cannot_release_execution_charge(tmp_path, monkeypatch):
    with _metadata(tmp_path, monkeypatch) as case:
        owner = ImmutableArchiveAdmission(case.actor.case.factory)
        admitted = _arm(case, owner)
        response = owner.execute(
            admitted, StorageAdapterImmutableArchiveObjectStore(case.actor.adapter)
        )
        unknown = response.model_copy(update={"execution": "unknown"})
        owner.report(admitted, unknown, apply_receipt=_checkpoint)
        assert _effect(case, admitted).unresolved
        with pytest.raises(Conflict, match="retain its reservation"):
            owner.acknowledge_report(admitted, verify_receipt=_verify_checkpoint)
        _select(case)
        # Only actual original closure evidence upgrades this same observation.
        owner.report(admitted, response, apply_receipt=lambda *args: pytest.fail("reapply"))
        assert not _effect(case, admitted).unresolved
        owner.acknowledge_report(admitted, verify_receipt=_verify_checkpoint)


@pytest.mark.parametrize("failure", ("exception", "unknown"))
def test_missing_execution_outcome_retains_original_charge(tmp_path, monkeypatch, failure):
    with _metadata(tmp_path, monkeypatch) as case:
        owner = ImmutableArchiveAdmission(case.actor.case.factory)
        admitted = _arm(case, owner)

        class Executor:
            def execute_write(self, invocation, *, content):
                if failure == "exception":
                    raise OSError("injected provider response loss")
                return unknown_write_execution(invocation)

        if failure == "exception":
            with pytest.raises(OSError, match="provider response loss"):
                owner.execute(admitted, Executor())
        else:
            response = owner.execute(admitted, Executor())
            owner.report(admitted, response, apply_receipt=lambda *args: pytest.fail("no receipt"))
        effect = _effect(case, admitted)
        assert effect.state == "uncertain" and effect.unresolved
        with pytest.raises(Conflict, match="used live actor"):
            owner.withdraw(admitted)
        with pytest.raises(Conflict, match="already unresolved"):
            _arm(case, owner, admitted.prepared)


def test_different_existing_ciphertext_is_not_execution_closure(tmp_path, monkeypatch):
    with _metadata(tmp_path, monkeypatch) as case:
        owner = ImmutableArchiveAdmission(case.actor.case.factory)
        admitted = _arm(case, owner)
        response = owner.execute(
            admitted, StorageAdapterImmutableArchiveObjectStore(case.actor.adapter)
        )
        changed = response.model_copy(
            update={"result": response.result.model_copy(update={"stored_sha256": "f" * 64})}
        )
        with pytest.raises(ValueError):
            owner.report(
                admitted, changed, apply_receipt=lambda *args: pytest.fail("false receipt")
            )
        assert _effect(case, admitted).unresolved
        owner.report(admitted, response, apply_receipt=_checkpoint)


def test_prepared_facts_and_cold_rows_cannot_recreate_live_dispatch(tmp_path, monkeypatch):
    with _metadata(tmp_path, monkeypatch) as case:
        owner = ImmutableArchiveAdmission(case.actor.case.factory)
        admitted = _arm(case, owner)
        cold = ImmutableArchiveAdmission(case.actor.case.factory)
        physical = StorageAdapterImmutableArchiveObjectStore(case.actor.adapter)
        for actor, value in ((cold, admitted), (owner, replace(admitted))):
            with pytest.raises(Conflict, match="used live actor"):
                actor.execute(value, physical)
        proof = owner.withdraw(admitted)
        _select(case)
        with session_scope(case.actor.case.factory) as session:
            session.delete(session.get(CollectionUploadRecord, admitted.prepared.collection_id))
        owner.settle_withdrawal(proof)
        owner.settle_withdrawal(proof)
        assert not _effect(case, admitted).unresolved
        owner.acknowledge_report(admitted, verify_receipt=_verify_checkpoint)


@pytest.mark.parametrize("lane", ("payload-first", "metadata-first"))
def test_payload_and_metadata_share_one_finite_retained_call_grant(tmp_path, monkeypatch, lane):
    with _metadata(tmp_path, monkeypatch) as case:
        owner = ImmutableArchiveAdmission(case.actor.case.factory)
        prepared = _prepare(case)
        grants = replace(case.grants, calls=1, control_bytes=1_000_000)

        def payload():
            return case.actor.admission.arm(
                case.actor.case.collection_id,
                case.actor.case.object_id,
                epoch=case.epoch,
                kind="begin",
                resource="begin",
                arguments={"original": "shared-capacity"},
                executor_id="a" * 64,
                result_capacity_bytes=grants.result_bytes,
                grants=grants,
                guard=case.guard,
            )

        def metadata():
            return _arm(case, owner, prepared, grants=grants)

        first, second = (payload, metadata) if lane == "payload-first" else (metadata, payload)
        first()
        with pytest.raises(Conflict, match="grant is exhausted"):
            second()
        with session_scope(case.actor.case.factory) as session:
            assert session.scalar(select(func.count()).select_from(ArchiveWriteEffectRecord)) == 1


@pytest.mark.parametrize(
    "refusal", ("permission", "epoch", "binding", "changed-preparation", "bytes")
)
def test_metadata_admission_rechecks_current_authority_identity_and_grants(
    tmp_path, monkeypatch, refusal
):
    with _metadata(tmp_path, monkeypatch) as case:
        owner = ImmutableArchiveAdmission(case.actor.case.factory)
        prepared = _prepare(case)
        changes = {}
        if refusal == "permission":

            def revoked(session):
                raise Conflict("current owner permission revoked")

            changes["guard"] = revoked
        elif refusal == "epoch":
            changes["epoch"] = "f" * 64
        elif refusal == "binding":
            with session_scope(case.actor.case.factory) as session:
                row = session.scalar(
                    select(StorageIncarnationRecord).where(
                        StorageIncarnationRecord.kind == "archive",
                        StorageIncarnationRecord.name == case.source_store,
                    )
                )
                row.state = "disabled"
        elif refusal == "changed-preparation":
            prepared = replace(prepared, preparation_json=prepared.preparation_json + " ")
        else:
            changes["grants"] = replace(case.grants, control_bytes=1)
        with pytest.raises((Conflict, ServiceUnavailable)):
            _arm(case, owner, prepared, **changes)
        with session_scope(case.actor.case.factory) as session:
            assert session.scalar(select(func.count()).select_from(ArchiveWriteEffectRecord)) == 0


def test_fresh_admission_epoch_preserves_original_preparation_identity(tmp_path, monkeypatch):
    with _metadata(tmp_path, monkeypatch) as case:
        owner = ImmutableArchiveAdmission(case.actor.case.factory)
        prepared = _prepare(case)
        # A resumed owner may bind a new epoch only after its separate terminal
        # adjudication. This fixture changes just that already-granted epoch.
        with session_scope(case.actor.case.factory) as session:
            session.get(ArchiveWriteControlRecord, prepared.collection_id).epoch = "d" * 64
        replay = _prepare(case, epoch="d" * 64)
        assert replay == prepared
        admitted = _arm(case, owner, replay, epoch="d" * 64)
        with session_scope(case.actor.case.factory) as session:
            assert session.get(ArchiveWriteEffectRecord, admitted.ticket.call_id).epoch == "d" * 64
        assert admitted.prepared.epoch == case.epoch
        proof = owner.withdraw(admitted)
        owner.settle_withdrawal(proof)
        owner.acknowledge_report(admitted, verify_receipt=_verify_checkpoint)


def test_running_metadata_dispatch_keeps_terminal_pending_until_original_return(
    tmp_path, monkeypatch
):
    with _metadata(tmp_path, monkeypatch) as case:
        owner = ImmutableArchiveAdmission(case.actor.case.factory)
        admitted = _arm(case, owner)
        physical = StorageAdapterImmutableArchiveObjectStore(case.actor.adapter)
        entered, release = Event(), Event()

        class Held:
            def execute_write(self, invocation, *, content):
                entered.set()
                assert release.wait(timeout=10)
                return physical.execute_write(invocation, content=content)

        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(owner.execute, admitted, Held())
            try:
                assert entered.wait(timeout=10)
                selected = _select(case)
                assert _effect(case, admitted).unresolved
                with pytest.raises(Conflict, match="still running"):
                    owner.report(
                        admitted,
                        unknown_write_execution(admitted.invocation),
                        apply_receipt=lambda *args: pytest.fail("running"),
                    )
                with session_scope(case.actor.case.factory) as session:
                    assert session.get(ArchiveUploadTerminalRecord, selected).phase == "draining"
            finally:
                release.set()
            response = future.result(timeout=10)
        owner.report(admitted, response, apply_receipt=_checkpoint)
        owner.acknowledge_report(admitted, verify_receipt=_verify_checkpoint)


@pytest.mark.parametrize("interruption", (False, True))
def test_original_executor_failure_survives_unknown_status_write_failure(
    tmp_path, monkeypatch, interruption
):
    with _metadata(tmp_path, monkeypatch) as case:
        owner = ImmutableArchiveAdmission(case.actor.case.factory)
        admitted = _arm(case, owner)
        original = (
            KeyboardInterrupt("original executor exit") if interruption else OSError("original")
        )

        class Executor:
            def execute_write(self, invocation, *, content):
                raise original

        def unavailable(value):
            raise RuntimeError("secondary status write failure")

        monkeypatch.setattr(owner, "record_unknown", unavailable)
        with pytest.raises(type(original)) as caught:
            owner.execute(admitted, Executor())
        assert caught.value is original
        assert _effect(case, admitted).state == "armed" and _effect(case, admitted).unresolved
        _select(case)


@pytest.mark.parametrize("committed", (False, True))
def test_original_settled_report_is_retained_across_retirement_reply_loss(
    tmp_path, monkeypatch, committed
):
    with _metadata(tmp_path, monkeypatch) as case:
        owner = ImmutableArchiveAdmission(case.actor.case.factory)
        admitted = _arm(case, owner)
        response = owner.execute(
            admitted, StorageAdapterImmutableArchiveObjectStore(case.actor.adapter)
        )
        owner.report(admitted, response, apply_receipt=_checkpoint)
        transaction = admission_module.session_scope

        @contextmanager
        def lost(factory):
            with transaction(factory) as session:
                yield session
                if not committed:
                    raise OSError("injected retirement reply loss")
            if committed:
                raise OSError("injected retirement reply loss")

        with monkeypatch.context() as patch:
            patch.setattr(admission_module, "session_scope", lost)
            with pytest.raises(OSError, match="retirement reply loss"):
                owner.acknowledge_report(admitted, verify_receipt=_verify_checkpoint)
        assert (_effect(case, admitted) is None) is committed
        if committed:
            # Absence is meaningful only to this actor, which already received
            # the exact original report acknowledgement before retirement.
            with pytest.raises(NotFound):
                owner.acknowledge_report(admitted, verify_receipt=_verify_checkpoint)
        else:
            owner.acknowledge_report(admitted, verify_receipt=_verify_checkpoint)
        with pytest.raises(Conflict, match="used live actor"):
            owner.execute(admitted, StorageAdapterImmutableArchiveObjectStore(case.actor.adapter))


@pytest.mark.parametrize("count", (1, 8, 32))
def test_metadata_progress_uses_fixed_call_grants_and_indexed_point_reports(
    tmp_path, monkeypatch, count
):
    with _metadata(tmp_path, monkeypatch) as case:
        factory = case.actor.case.factory
        engine = factory.kw["bind"]
        owner = ImmutableArchiveAdmission(factory)
        physical = StorageAdapterImmutableArchiveObjectStore(case.actor.adapter)
        grants = replace(case.grants, calls=1, control_bytes=200_000)
        statements = []

        def capture(connection, cursor, statement, parameters, context, executemany):
            if (
                statement.lstrip().startswith("SELECT")
                and "FROM archive_write_effects" in statement
            ):
                statements.append(statement)

        event.listen(engine, "before_cursor_execute", capture)
        try:
            for number in range(count):
                request = case.operation.request.model_copy(
                    update={"object_path": case.prefix + f"/metadata/{number}.age"}
                )
                operation = case.operation.model_copy(update={"request": request})
                prepared = _prepare(case, operation=operation)
                admitted = _arm(case, owner, prepared, grants=grants)
                response = owner.execute(admitted, physical)
                owner.report(admitted, response, apply_receipt=_checkpoint)
                owner.acknowledge_report(admitted, verify_receipt=_verify_checkpoint)
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        assert len(statements) == 4 * count
        assert sum("LIMIT" in statement for statement in statements) == 2 * count
        with session_scope(factory) as session:
            assert session.scalar(select(func.count()).select_from(ArchiveWriteEffectRecord)) == 0


def test_completed_metadata_control_requires_exact_positive_receiving_owner_proof(
    tmp_path, monkeypatch
):
    with _metadata(tmp_path, monkeypatch) as case:
        factory = case.actor.case.factory
        owner = ImmutableArchiveAdmission(factory)
        admitted = _arm(case, owner)
        response = owner.execute(
            admitted, StorageAdapterImmutableArchiveObjectStore(case.actor.adapter)
        )
        # An outcome can be retained while its source checkpoint is unavailable.
        owner.report(admitted, response, apply_receipt=lambda *args: None)
        with pytest.raises(Conflict, match="no receiving proof"):
            owner.acknowledge_report(admitted)
        with pytest.raises(Conflict, match="durable receiving owner"):
            owner.acknowledge_report(admitted, verify_receipt=_verify_checkpoint)
        assert not _effect(case, admitted).unresolved
        assert _effect(case, admitted).result is not None
        with session_scope(factory) as session:
            _checkpoint(session, admitted.prepared, response.result)
        owner.acknowledge_report(admitted, verify_receipt=_verify_checkpoint)
        assert _effect(case, admitted) is None
