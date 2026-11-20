from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_core.catalog_archive_write_models import (
    ArchiveUploadTerminalRecord,
    ArchiveWriteEffectRecord,
    ArchiveWritePreparationRecord,
)
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord
from riverhog_core.services import immutable_archive_admission as admission_module
from riverhog_core.services.immutable_archive_admission import (
    ImmutableArchiveAdmission,
    SettledImmutableArchiveWrite,
)
from riverhog_core.stores.storage_adapter_archive_objects import (
    StorageAdapterImmutableArchiveObjectStore,
)
from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy import event, func, select

from tests.unit.test_archive_write_execution import _executor
from tests.unit.test_immutable_archive_admission import (
    _arm,
    _checkpoint,
    _effect,
    _select,
    _verify_checkpoint,
)
from tests.unit.test_immutable_archive_observation import _seed
from tests.unit.test_immutable_archive_preparation import _metadata, _prepare


@contextmanager
def _closed_source(tmp_path, monkeypatch, outcome="executed", *, mode="direct", database_url=None):
    with (
        _metadata(tmp_path, monkeypatch, database_url=database_url) as case,
        _executor(case.actor.adapter, mode) as transport,
    ):
        owner = ImmutableArchiveAdmission(case.actor.case.factory)
        physical = StorageAdapterImmutableArchiveObjectStore(transport)
        if outcome == "observed":
            _seed(case, physical)
        admitted = _arm(case, owner)
        calls = []

        class Executor:
            def execute_write(self, invocation, *, content):
                calls.append(invocation)
                return physical.execute_write(invocation, content=content)

        receipt = None
        if outcome in {"executed", "positive-unknown"}:
            response = owner.execute(admitted, Executor())
            receipt = response.result
            if outcome == "positive-unknown":
                response = response.model_copy(update={"execution": "unknown"})
            owner.report(admitted, response, apply_receipt=lambda *args: None)
        elif outcome == "observed":
            receipt = physical.observe_immutable_object(admitted.prepared.operation.request)
            proof = owner.withdraw(admitted)
            owner.accept_existing(admitted, proof, receipt, apply_receipt=lambda *args: None)
        elif outcome == "unused":
            owner.settle_withdrawal(owner.withdraw(admitted))
        elif outcome != "armed":
            raise AssertionError("invalid fixture outcome")
        yield SimpleNamespace(
            case=case,
            factory=case.actor.case.factory,
            owner=owner,
            admitted=admitted,
            prepared=admitted.prepared,
            physical=physical,
            receipt=receipt,
            calls=calls,
        )


def _load(source, owner):
    return owner.load_next_report(
        source.prepared.collection_id, source.prepared.operation.request.object_path
    )


def _assert_cold_receiving(tmp_path, monkeypatch, mode, *, database_url=None):
    with _closed_source(tmp_path, monkeypatch, mode=mode, database_url=database_url) as source:
        selected = _select(source.case)
        # The receiver has no access to the original live owner's registry.
        receiver = ImmutableArchiveAdmission(source.factory)
        settled = _load(source, receiver)
        assert isinstance(settled, SettledImmutableArchiveWrite)
        assert settled.receipt == source.receipt
        assert settled.prepared == source.prepared and receiver._live == {}
        assert not any(hasattr(settled, name) for name in ("ticket", "nonce", "dispatch"))
        with session_scope(source.factory) as session:
            assert session.get(ArchiveUploadTerminalRecord, selected).phase == "draining"
        receiver.receive_settled(
            settled, apply_receipt=_checkpoint, verify_receipt=_verify_checkpoint
        )
        receiver.receive_settled(
            settled, apply_receipt=_checkpoint, verify_receipt=_verify_checkpoint
        )
        receiver.acknowledge_settled(settled, verify_receipt=_verify_checkpoint)
        assert _load(source, receiver) is None
        assert _effect(source.case, source.admitted) is None and len(source.calls) == 1
        with pytest.raises(Conflict):
            _arm(source.case, receiver, source.prepared)


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_cold_receiver_consumes_original_closed_report_without_provider_reexecution(
    tmp_path, monkeypatch, mode
):
    _assert_cold_receiving(tmp_path, monkeypatch, mode)


def test_cold_existing_object_receipt_keeps_unused_dispatch_separate(tmp_path, monkeypatch):
    with _closed_source(tmp_path, monkeypatch, "observed") as source:
        receiver = ImmutableArchiveAdmission(source.factory)
        settled = _load(source, receiver)
        assert settled.receipt == source.receipt
        assert settled.receipt.stored_sha256 != settled.prepared.operation.request.stored_sha256
        value = json.loads(settled.result_json)
        assert value["dispatch"] == "never-started" and "call" not in value
        receiver.receive_settled(
            settled, apply_receipt=_checkpoint, verify_receipt=_verify_checkpoint
        )
        receiver.acknowledge_settled(settled, verify_receipt=_verify_checkpoint)
        assert source.calls == [] and _effect(source.case, source.admitted) is None


def test_cold_unused_control_retires_after_terminal_and_parent_retirement(tmp_path, monkeypatch):
    with _closed_source(tmp_path, monkeypatch, "unused") as source:
        _select(source.case)
        with session_scope(source.factory) as session:
            session.delete(session.get(CollectionUploadRecord, source.prepared.collection_id))
        receiver = ImmutableArchiveAdmission(source.factory)
        settled = _load(source, receiver)
        assert settled.receipt is None

        def no_receipt(*args):
            pytest.fail("unused dispatch has no object receipt")

        receiver.receive_settled(settled, apply_receipt=no_receipt, verify_receipt=no_receipt)
        receiver.acknowledge_settled(settled)
        assert _load(source, receiver) is None
        assert source.calls == []


@pytest.mark.parametrize("outcome", ("armed", "positive-unknown"))
def test_cold_rows_never_upgrade_unresolved_original_execution(tmp_path, monkeypatch, outcome):
    with _closed_source(tmp_path, monkeypatch, outcome) as source:
        receiver = ImmutableArchiveAdmission(source.factory)
        assert _load(source, receiver) is None
        fake = SettledImmutableArchiveWrite(
            source.prepared,
            source.admitted.ticket.call_id,
            source.admitted.invocation_json,
            "{}",
            "f" * 64,
        )
        with pytest.raises(Conflict, match="retain its reservation"):
            receiver.acknowledge_settled(fake, verify_receipt=_verify_checkpoint)
        assert _effect(source.case, source.admitted).unresolved


def test_cold_positive_report_requires_actual_receiving_ownership(tmp_path, monkeypatch):
    with _closed_source(tmp_path, monkeypatch) as source:
        receiver = ImmutableArchiveAdmission(source.factory)
        settled = _load(source, receiver)
        with pytest.raises(Conflict, match="no receiving proof"):
            receiver.acknowledge_settled(settled)
        with pytest.raises(Conflict, match="durable receiving owner"):
            receiver.acknowledge_settled(settled, verify_receipt=_verify_checkpoint)
        _select(source.case)
        with session_scope(source.factory) as session:
            session.delete(session.get(CollectionUploadRecord, source.prepared.collection_id))
        receiver = ImmutableArchiveAdmission(source.factory)
        recovered = _load(source, receiver)
        assert recovered == settled
        with pytest.raises(Conflict, match="durable receiving owner"):
            receiver.receive_settled(
                recovered, apply_receipt=lambda *args: None, verify_receipt=_verify_checkpoint
            )
        assert _effect(source.case, source.admitted).result == settled.result_json


@pytest.mark.parametrize("committed", (False, True))
def test_cold_receiving_commit_and_reply_loss_replay_idempotent_owned_checkpoint(
    tmp_path, monkeypatch, committed
):
    with _closed_source(tmp_path, monkeypatch) as source:
        receiver = ImmutableArchiveAdmission(source.factory)
        settled = _load(source, receiver)
        transaction = admission_module.session_scope

        def apply(session, prepared, receipt):
            upload = session.get(CollectionUploadRecord, prepared.collection_id)
            if upload.archive_terminal_receipt_json is None:
                _checkpoint(session, prepared, receipt)
                upload.archive_volume_next_sequence += 1
            else:
                _verify_checkpoint(session, prepared, receipt)

        @contextmanager
        def lost(factory):
            with transaction(factory) as session:
                yield session
                if not committed:
                    raise OSError("injected receiving reply loss")
            if committed:
                raise OSError("injected receiving reply loss")

        with monkeypatch.context() as patch:
            patch.setattr(admission_module, "session_scope", lost)
            with pytest.raises(OSError, match="receiving reply loss"):
                receiver.receive_settled(
                    settled, apply_receipt=apply, verify_receipt=_verify_checkpoint
                )
        assert _effect(source.case, source.admitted) is not None
        with session_scope(source.factory) as session:
            upload = session.get(CollectionUploadRecord, source.prepared.collection_id)
            assert (upload.archive_terminal_receipt_json is not None) is committed
            assert upload.archive_volume_next_sequence == int(committed)
        receiver.receive_settled(settled, apply_receipt=apply, verify_receipt=_verify_checkpoint)
        with session_scope(source.factory) as session:
            assert (
                session.get(
                    CollectionUploadRecord, source.prepared.collection_id
                ).archive_volume_next_sequence
                == 1
            )
        receiver.acknowledge_settled(settled, verify_receipt=_verify_checkpoint)
        assert len(source.calls) == 1


@pytest.mark.parametrize("committed", (False, True))
def test_cold_retirement_reply_loss_preserves_original_receiver_proof(
    tmp_path, monkeypatch, committed
):
    with _closed_source(tmp_path, monkeypatch) as source:
        receiver = ImmutableArchiveAdmission(source.factory)
        settled = _load(source, receiver)
        receiver.receive_settled(
            settled, apply_receipt=_checkpoint, verify_receipt=_verify_checkpoint
        )
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
                receiver.acknowledge_settled(settled, verify_receipt=_verify_checkpoint)
        assert (_effect(source.case, source.admitted) is None) is committed
        if committed:
            # This receiver already received exact durable checkpoint acceptance.
            with pytest.raises(NotFound):
                receiver.acknowledge_settled(settled, verify_receipt=_verify_checkpoint)
        else:
            receiver.acknowledge_settled(settled, verify_receipt=_verify_checkpoint)
        assert len(source.calls) == 1


@pytest.mark.parametrize("change", ("result", "invocation", "preparation", "record", "call"))
def test_cold_receiving_rejects_changed_original_proof(tmp_path, monkeypatch, change):
    with _closed_source(tmp_path, monkeypatch) as source:
        receiver = ImmutableArchiveAdmission(source.factory)
        settled = _load(source, receiver)
        updates = {
            "result": {"result_json": "{}"},
            "invocation": {"invocation_json": "{}"},
            "preparation": {
                "prepared": replace(
                    settled.prepared, preparation_json=settled.prepared.preparation_json + " "
                )
            },
            "record": {"record_sha256": "f" * 64},
            "call": {"call_id": "f" * 64},
        }
        changed = replace(settled, **updates[change])
        with pytest.raises((Conflict, NotFound)):
            receiver.receive_settled(
                changed,
                apply_receipt=lambda *args: pytest.fail("changed proof"),
                verify_receipt=_verify_checkpoint,
            )
        assert _effect(source.case, source.admitted).result == settled.result_json


@pytest.mark.parametrize("change", ("result", "request", "capacity", "observation"))
def test_cold_reader_validates_retained_original_shape_identity_and_capacity(
    tmp_path, monkeypatch, change
):
    with _closed_source(tmp_path, monkeypatch) as source:
        with session_scope(source.factory) as session:
            effect = session.get(ArchiveWriteEffectRecord, source.admitted.ticket.call_id)
            if change == "result":
                effect.result_json = "[]"
            elif change == "request":
                effect.request_json += " "
            elif change == "capacity":
                effect.result_capacity_bytes = 100
            else:
                effect.observation_json = "{}"
        receiver = ImmutableArchiveAdmission(source.factory)
        with pytest.raises(Conflict):
            _load(source, receiver)
        assert _effect(source.case, source.admitted) is not None


def test_receiving_validator_gets_fresh_original_assertions_after_mutating_callback(
    tmp_path, monkeypatch
):
    with _closed_source(tmp_path, monkeypatch) as source:
        receiver = ImmutableArchiveAdmission(source.factory)
        settled = _load(source, receiver)

        def mutate(session, prepared, receipt):
            receipt.verified_identity_assertions["unexpected"] = "callback mutation"
            _checkpoint(session, prepared, receipt)

        with pytest.raises(Conflict, match="durable receiving owner"):
            receiver.receive_settled(
                settled, apply_receipt=mutate, verify_receipt=_verify_checkpoint
            )
        with session_scope(source.factory) as session:
            assert (
                session.get(
                    CollectionUploadRecord, source.prepared.collection_id
                ).archive_terminal_receipt_json
                is None
            )
        assert _effect(source.case, source.admitted).result == settled.result_json


def test_absent_cold_report_creates_neither_preparation_nor_provider_authority(
    tmp_path, monkeypatch
):
    with _metadata(tmp_path, monkeypatch) as case:
        receiver = ImmutableArchiveAdmission(case.actor.case.factory)
        assert (
            receiver.load_next_report(
                case.actor.case.collection_id, case.operation.request.object_path
            )
            is None
        )
        assert receiver._live == {}
        with session_scope(case.actor.case.factory) as session:
            assert (
                session.get(
                    ArchiveWritePreparationRecord,
                    (
                        case.actor.case.collection_id,
                        case.owner.object_id(case.operation.request.object_path),
                    ),
                )
                is None
            )
        assert case.actor.seen == []


@pytest.mark.parametrize("count", (1, 8, 32))
def test_cold_metadata_receiving_progresses_under_fixed_one_call_grants(
    tmp_path, monkeypatch, count
):
    with _metadata(tmp_path, monkeypatch) as case:
        factory = case.actor.case.factory
        engine = factory.kw["bind"]
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
                prepared = _prepare(
                    case, operation=case.operation.model_copy(update={"request": request})
                )
                writer = ImmutableArchiveAdmission(factory)
                admitted = _arm(case, writer, prepared, grants=grants)
                response = writer.execute(admitted, physical)
                writer.report(admitted, response, apply_receipt=lambda *args: None)
                receiver = ImmutableArchiveAdmission(factory)
                settled = receiver.load_next_report(prepared.collection_id, request.object_path)
                receiver.receive_settled(
                    settled, apply_receipt=_checkpoint, verify_receipt=_verify_checkpoint
                )
                receiver.acknowledge_settled(settled, verify_receipt=_verify_checkpoint)
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        assert len(statements) <= 8 * count
        assert all("LIMIT" in s or "archive_write_effects.id =" in s for s in statements)
        with session_scope(factory) as session:
            assert session.scalar(select(func.count()).select_from(ArchiveWriteEffectRecord)) == 0
