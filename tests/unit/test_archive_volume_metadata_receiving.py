from __future__ import annotations

import hashlib
import json
import uuid
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from a_riverhog_filesystem_store import FilesystemStorageAdapter, FilesystemStorageAdapterConfig
from a_riverhog_filesystem_store import incarnation as filesystem_incarnation
from riverhog_age import encrypt_age_scrypt
from riverhog_archive_contracts import update_archive_sequence_commitment
from riverhog_core.catalog_archive_write_models import ArchiveWriteEffectRecord
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.ports.archive_objects import ArchiveObjectIdentityConflict
from riverhog_core.services import immutable_archive_admission as admission_module
from riverhog_core.services.archive_volume_metadata_checkpoint import ArchiveVolumeMetadataSource
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from riverhog_core.services.immutable_archive_admission import ImmutableArchiveAdmission
from riverhog_core.services.immutable_archive_preparation import ImmutableArchivePreparation
from riverhog_core.stores.storage_adapter_archive_objects import (
    StorageAdapterImmutableArchiveObjectStore,
)
from riverhog_protocol.errors import Conflict, NotFound
from riverhog_storage_adapter_protocol import PutObjectOperation
from sqlalchemy import event, func, select

from tests.unit.test_archive_volume_metadata_checkpoint import _snapshot, _source
from tests.unit.test_archive_write_execution import _executor
from tests.unit.test_collection_upload_preparation import _case
from tests.unit.test_finalization_claim_readiness import _fresh_coordinator, _seal


@contextmanager
def _metadata_source(
    tmp_path, monkeypatch, kind="segment", mode="direct", count=2, *, database_url=None
):
    with _case(tmp_path, kind, artifact_count=count, database_url=database_url) as case:
        _seal(case)
        initial = _source(case, 0)
        assert case.service._claim_due_finalization() == case.collection_id
        epoch = ArchiveWriteAdmission(case.factory).bind_epoch(
            case.collection_id, guard=initial.guard
        )
        with session_scope(case.factory) as session:
            incarnation = session.get(Upload, case.collection_id).archive_incarnation_id
        root = tmp_path / "metadata-provider"
        root.mkdir()
        with monkeypatch.context() as patch:
            patch.setattr(filesystem_incarnation, "uuid4", lambda: uuid.UUID(incarnation))
            assert filesystem_incarnation.provision_storage_root(root) == incarnation
        with (
            FilesystemStorageAdapter(
                FilesystemStorageAdapterConfig(root=root, minimum_free_bytes=0)
            ) as adapter,
            _executor(adapter, mode) as transport,
        ):
            yield SimpleNamespace(
                case=case,
                physical=StorageAdapterImmutableArchiveObjectStore(transport),
                epoch=epoch,
                grants=replace(case.service._config.archive_write, calls=1, control_bytes=200_000),
            )


def _admit(fixture, sequence, *, owner=None):
    case = fixture.case
    with session_scope(case.factory) as session:
        source = ArchiveVolumeMetadataSource.load(session, case.collection_id, sequence)
    content = encrypt_age_scrypt(source.publication.plaintext, "fixture", log_n=1)
    prepared = ImmutableArchivePreparation(case.factory).prepare(
        case.collection_id,
        operation=PutObjectOperation(request=source.publication.request(content)),
        content=content,
        epoch=fixture.epoch,
        grants=fixture.grants,
        guard=source.guard,
        validate_metadata=source.validate_metadata,
    )
    owner = owner or ImmutableArchiveAdmission(case.factory)
    admitted = owner.arm(
        prepared,
        epoch=fixture.epoch,
        executor_id="e" * 64,
        grants=fixture.grants,
        guard=source.guard,
        validate_metadata=source.validate_metadata,
    )
    assert prepared.object_id != source.object_id
    return source, owner, admitted


def _count(case):
    with session_scope(case.factory) as session:
        return session.scalar(select(func.count()).select_from(ArchiveWriteEffectRecord))


def _reported(fixture, sequence, *, observed=False, receive=True):
    source, owner, admitted = _admit(fixture, sequence)
    apply = source.apply_prepared_receipt if receive else lambda *args: None
    if observed:
        ciphertext = encrypt_age_scrypt(source.publication.plaintext, "fixture", log_n=1)
        fixture.physical.put_immutable_object(
            object_path=source.publication.object_path,
            content=ciphertext,
            content_type=admitted.prepared.operation.request.content_type,
            required_identity_assertions=source.publication.required_identity_assertions,
            placement_policy="immediate_default",
        )
        receipt = fixture.physical.observe_immutable_object(admitted.prepared.operation.request)
        assert receipt.stored_sha256 != admitted.prepared.operation.request.stored_sha256
        owner.accept_existing(admitted, owner.withdraw(admitted), receipt, apply_receipt=apply)
    else:
        response = owner.execute(admitted, fixture.physical)
        assert response.execution == "closed"
        owner.report(admitted, response, apply_receipt=apply)
        receipt = response.result
    return source, owner, admitted, receipt


@pytest.mark.parametrize("kind", ["pack", "segment"])
@pytest.mark.parametrize("mode", ["direct", "http"])
def test_actual_cold_finalizer_drains_original_receipt_after_pointer_advanced(
    tmp_path, monkeypatch, kind, mode
):
    with _metadata_source(tmp_path, monkeypatch, kind, mode) as fixture:
        case = fixture.case
        source, owner, admitted, receipt = _reported(fixture, 0)
        before = _snapshot(case)
        assert before[0] == 1 and _count(case) == 1
        cold = _fresh_coordinator(case)
        assert cold._drain_archive_volume_metadata_report(case.collection_id)
        assert _snapshot(case) == before and _count(case) == 0
        assert not cold._drain_archive_volume_metadata_report(case.collection_id)
        # Cold recovery selected the original object rather than next sequence 1.
        assert owner._live == {}
        with session_scope(case.factory) as session:
            source.verify_prepared_receipt(session, admitted.prepared, receipt)


@pytest.mark.parametrize("observed", [False, True])
def test_cold_receiving_preserves_actual_ciphertext_and_once_only_terminal_commitment(
    tmp_path, monkeypatch, observed
):
    with _metadata_source(tmp_path, monkeypatch) as fixture:
        case = fixture.case
        expected = hashlib.sha256()
        for sequence in range(3):
            source, owner, admitted, receipt = _reported(fixture, sequence, observed=observed)
            update_archive_sequence_commitment(expected, source.document)
            before = _snapshot(case)
            if sequence == 2:
                with session_scope(case.factory) as session:
                    upload = session.get(Upload, case.collection_id)
                    upload.state = upload.archive_phase = "orphaned"
            assert _fresh_coordinator(case)._drain_archive_volume_metadata_report(
                case.collection_id
            )
            assert _snapshot(case) == before and _count(case) == 0
            with session_scope(case.factory) as session:
                source.verify_prepared_receipt(session, admitted.prepared, receipt)
        assert _snapshot(case)[3] == expected.hexdigest()
        assert _snapshot(case)[1] is None


@pytest.mark.parametrize("phase", ["receiving", "retirement"])
@pytest.mark.parametrize("committed", [False, True])
def test_actual_cold_receiving_retries_transaction_and_reply_loss_without_another_put(
    tmp_path, monkeypatch, phase, committed
):
    with _metadata_source(tmp_path, monkeypatch) as fixture:
        case = fixture.case
        # Simulate an original closed report whose receiving checkpoint was not
        # retained by an earlier receiver. The actual source owner must prove it.
        _reported(fixture, 0, receive=phase != "receiving")
        transaction = admission_module.session_scope
        calls = 0

        @contextmanager
        def lost(factory):
            nonlocal calls
            calls += 1
            # Collection selection, receive, then retirement are separate steps.
            selected = calls == (2 if phase == "receiving" else 3)
            with transaction(factory) as session:
                yield session
                if selected and not committed:
                    raise OSError("injected cold receiving rollback")
            if selected and committed:
                raise OSError("injected cold receiving commit reply loss")

        with monkeypatch.context() as patch:
            patch.setattr(admission_module, "session_scope", lost)
            with pytest.raises(OSError, match="injected cold receiving"):
                _fresh_coordinator(case)._drain_archive_volume_metadata_report(case.collection_id)
        assert _count(case) == (0 if phase == "retirement" and committed else 1)
        assert _snapshot(case)[0] == (int(committed) if phase == "receiving" else 1)
        _fresh_coordinator(case)._drain_archive_volume_metadata_report(case.collection_id)
        assert _count(case) == 0 and _snapshot(case)[0] == 1


@pytest.mark.parametrize("change", ["parent", "tree", "receipt"])
def test_missing_or_changed_receiving_source_retains_original_closed_control(
    tmp_path, monkeypatch, change
):
    from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord as Volume

    with _metadata_source(tmp_path, monkeypatch) as fixture:
        case = fixture.case
        source, _, admitted, _ = _reported(fixture, 0)
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            if change == "parent":
                session.delete(upload)
            elif change == "tree":
                upload.archive_tree_sha256 = "c" * 64
            else:
                row = session.get(Volume, (case.collection_id, source.object_id))
                value = json.loads(row.metadata_receipt_json)
                value["stored_sha256"] = "f" * 64
                row.metadata_receipt_json = json.dumps(value, sort_keys=True, separators=(",", ":"))
        with pytest.raises((Conflict, NotFound)):
            _fresh_coordinator(case)._drain_archive_volume_metadata_report(case.collection_id)
        assert _count(case) == 1
        with session_scope(case.factory) as session:
            assert not session.get(ArchiveWriteEffectRecord, admitted.ticket.call_id).unresolved


@pytest.mark.parametrize(
    "field",
    [
        "object_path",
        "verified_content_type",
        "verified_identity_assertions",
        "verified_placement_policy",
    ],
)
def test_source_bridge_refuses_changed_receipt_authority(tmp_path, monkeypatch, field):
    with _metadata_source(tmp_path, monkeypatch) as fixture:
        case = fixture.case
        source, _, admitted, receipt = _reported(fixture, 0)
        changed = receipt.model_copy(
            update={field: {} if field == "verified_identity_assertions" else "changed"}
        )
        before = _snapshot(case)
        with pytest.raises(ArchiveObjectIdentityConflict):
            with session_scope(case.factory) as session:
                source.verify_prepared_receipt(session, admitted.prepared, changed)
        assert _snapshot(case) == before and _count(case) == 1


@pytest.mark.parametrize("count", [1, 4, 8])
def test_actual_metadata_draining_progresses_under_fixed_one_call_grant(
    tmp_path, monkeypatch, record_property, count
):
    with _metadata_source(tmp_path, monkeypatch, count=count) as fixture:
        case = fixture.case
        statements = []
        engine = case.factory.kw["bind"]

        def capture(connection, cursor, statement, parameters, context, executemany):
            if (
                statement.lstrip().startswith("SELECT")
                and "FROM archive_write_effects" in statement
            ):
                statements.append(statement)

        event.listen(engine, "before_cursor_execute", capture)
        try:
            expected = hashlib.sha256()
            for sequence in range(count + 1):
                source, _, _, _ = _reported(fixture, sequence)
                update_archive_sequence_commitment(expected, source.document)
                assert _fresh_coordinator(case)._drain_archive_volume_metadata_report(
                    case.collection_id
                )
            assert _count(case) == 0 and _snapshot(case)[3] == expected.hexdigest()
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        # All retained effects share one call slot. Every replay lookup is a
        # bounded pool selection or exact original call lookup, never history.
        # Fresh admission now checks predecessor closure before and after its
        # waits. Account for that additional authority work separately while
        # preserving the receiving/replay bound.
        closure_checks = [
            statement
            for statement in statements
            if "archive_write_effects.unresolved IS 1" in statement
            and "archive_write_effects.object_id =" not in statement
        ]
        receiving_checks = [
            statement for statement in statements if statement not in closure_checks
        ]
        assert len(receiving_checks) <= 10 * (count + 1) + 1
        assert 0 < len(closure_checks) <= 4 * (count + 1)
        assert len(statements) <= 14 * (count + 1) + 1
        assert all(
            "LIMIT" in s or "archive_write_effects.id =" in s or "count(" in s for s in statements
        )
        record_property(
            "metadata_receiving_work",
            {
                "volumes": count,
                "metadata_objects": count + 1,
                "effect_selects": len(statements),
                "receiving_effect_selects": len(receiving_checks),
                "admission_closure_selects": len(closure_checks),
                "call_grant": 1,
            },
        )


def test_cold_unused_metadata_control_retires_without_fabricating_a_source_receipt(
    tmp_path, monkeypatch
):
    with _metadata_source(tmp_path, monkeypatch) as fixture:
        case = fixture.case
        _, owner, admitted = _admit(fixture, 0)
        owner.settle_withdrawal(owner.withdraw(admitted))
        with session_scope(case.factory) as session:
            session.delete(session.get(Upload, case.collection_id))
        assert _fresh_coordinator(case)._drain_archive_volume_metadata_report(case.collection_id)
        assert _count(case) == 0


def test_positive_unknown_metadata_remains_charged_without_cold_closure(tmp_path, monkeypatch):
    with _metadata_source(tmp_path, monkeypatch) as fixture:
        case = fixture.case
        source, owner, admitted = _admit(fixture, 0)
        response = owner.execute(admitted, fixture.physical)
        owner.report(
            admitted,
            response.model_copy(update={"execution": "unknown"}),
            apply_receipt=source.apply_prepared_receipt,
        )
        before = _snapshot(case)
        assert before[0] == 1
        assert not _fresh_coordinator(case)._drain_archive_volume_metadata_report(
            case.collection_id
        )
        with pytest.raises(Conflict, match="unresolved provider writes"):
            _admit(fixture, 1)
        assert _count(case) == 1 and _snapshot(case) == before
        with session_scope(case.factory) as session:
            assert session.get(ArchiveWriteEffectRecord, admitted.ticket.call_id).unresolved
        # Original closure, when actually returned by the selected executor,
        # can close this same original observation without applying it twice.
        owner.report(admitted, response, apply_receipt=source.apply_prepared_receipt)
        assert _fresh_coordinator(case)._drain_archive_volume_metadata_report(case.collection_id)
        assert _count(case) == 0 and _snapshot(case) == before


def test_a_different_metadata_owner_cannot_be_acknowledged_as_a_volume_source(
    tmp_path, monkeypatch
):
    from tests.unit.test_immutable_archive_receiving import _closed_source

    with _closed_source(tmp_path, monkeypatch) as original:
        case = original.case.actor.case
        with pytest.raises(Conflict, match="original sequence is invalid"):
            _fresh_coordinator(case)._drain_archive_volume_metadata_report(case.collection_id)
        assert _count(case) == 1
