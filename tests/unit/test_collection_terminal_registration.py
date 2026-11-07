from __future__ import annotations

import json
from contextlib import contextmanager

import pytest
from riverhog_core.catalog_archive_write_models import (
    ArchiveUploadTerminalRecord,
    ArchiveWriteControlRecord,
    ArchiveWriteEffectRecord,
)
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import (
    CollectionArchiveObjectUploadRecord,
    CollectionUploadArtifactRecord,
    CollectionUploadArtifactVolumeRecord,
    CollectionUploadRecord,
)
from riverhog_core.catalog_registration_models import CollectionRegistrationRecord
from riverhog_core.services import archive_write_admission
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from riverhog_core.services.collection_registration import CollectionRegistrationOwner
from riverhog_core.services.collection_registration_cleanup import advance_registration_cleanup
from riverhog_protocol.errors import Conflict
from sqlalchemy import func, select

from tests.unit.test_collection_registration_operations import (
    LIMITS,
    _close,
    _member,
    _open,
    _until,
)


@contextmanager
def _case(tmp_path, phase):
    service, keys, principal, _, collection_id = _open(tmp_path, custody_mode="custody-transfer")
    try:
        service.register_artifacts(collection_id, [_member("1", 1)])
        registration = CollectionRegistrationOwner(service)
        member = _member("2", 17)
        operation_id = registration.admit(collection_id, [member], principal=principal)[
            "operation_id"
        ]
        _until(service, collection_id, operation_id, phase)
        yield service, principal, collection_id, registration, operation_id, member
    finally:
        _close(service, keys)


def _snapshot(service, collection_id, operation_id):
    with session_scope(service._session_factory) as session:
        session.info["include_unpublished_registration"] = True
        upload = session.get(CollectionUploadRecord, collection_id)
        operation = session.get(CollectionRegistrationRecord, operation_id)
        control = session.get(ArchiveWriteControlRecord, collection_id)
        return {
            "operation": {
                column.name: getattr(operation, column.name)
                for column in operation.__table__.columns
            },
            "upload": {
                name: getattr(upload, name)
                for name in (
                    "state",
                    "active_registration_id",
                    "registration_revision",
                    "planner_checkpoint_json",
                    "planner_next_sequence",
                    "artifact_count",
                    "artifact_bytes",
                    "lease_expires_at",
                )
            },
            "control": (control.gate, control.epoch, control.terminal_id),
            "rows": tuple(
                session.scalar(
                    select(func.count())
                    .select_from(model)
                    .where(model.collection_id == collection_id)
                )
                for model in (
                    CollectionUploadArtifactRecord,
                    CollectionArchiveObjectUploadRecord,
                    CollectionUploadArtifactVolumeRecord,
                )
            ),
            "terminals": session.scalar(
                select(func.count()).select_from(ArchiveUploadTerminalRecord)
            ),
        }


def _select(service, principal, collection_id, *, kind="cancel"):
    return ArchiveWriteAdmission(service._session_factory).select_terminal(
        collection_id,
        kind=kind,
        request={"fixture": "exact-original-terminal"},
        principal_id=principal.id,
        grants=service._config.archive_write,
        guard=service._payload_write_guard(collection_id, principal),
        validate_selection=lambda *args: None,
    )


@pytest.mark.parametrize("phase", ["staging", "verifying", "auditing", "ready"])
@pytest.mark.parametrize("kind", ["cancel", "discard"])
def test_terminal_selection_freezes_hidden_registration_without_cleanup(tmp_path, phase, kind):
    with _case(tmp_path, phase) as case:
        service, principal, collection_id, registration, operation_id, member = case
        before = _snapshot(service, collection_id, operation_id)
        selected = _select(service, principal, collection_id, kind=kind)
        after = _snapshot(service, collection_id, operation_id)
        assert after["operation"]["execution_epoch"] != before["operation"]["execution_epoch"]
        assert after["operation"]["due_at"] is None
        assert after["operation"]["phase"] == phase
        assert after["operation"]["disposition_json"] is None
        assert after["operation"]["cleanup_phase"] is None
        assert after["upload"] == before["upload"] and after["rows"] == before["rows"]
        for name in before["operation"].keys() - {"execution_epoch", "due_at", "updated_at"}:
            assert after["operation"][name] == before["operation"][name]
        assert after["control"][0] == "closed" and after["control"][1] != before["control"][1]
        assert after["control"][2] == selected and after["terminals"] == 1
        assert _select(service, principal, collection_id, kind=kind) == selected
        assert _snapshot(service, collection_id, operation_id) == after
        with pytest.raises(Conflict, match="execution changed"):
            registration.advance(
                collection_id,
                operation_id,
                limits=LIMITS,
                expected_epoch=before["operation"]["execution_epoch"],
            )
        with pytest.raises(Conflict, match="admission is closed"):
            registration.advance(collection_id, operation_id, limits=LIMITS)
        with pytest.raises(Conflict, match="admission is closed"):
            registration.admit(collection_id, [member], principal=principal)
        with pytest.raises(Conflict, match="admission is closed"):
            registration.admit(collection_id, [_member("3", 1)], principal=principal)
        assert registration.process_due(limit=4) == 0
        with session_scope(service._session_factory) as session:
            upload = session.get(CollectionUploadRecord, collection_id)
            operation = session.get(CollectionRegistrationRecord, operation_id)
            with pytest.raises(Conflict, match="selected terminal owner"):
                advance_registration_cleanup(session, upload, operation, limits=LIMITS)
        assert _snapshot(service, collection_id, operation_id) == after
        assert service._archive_stores.require("archive").resumable_objects.uploads == {}


def test_terminal_freeze_rolls_back_with_its_selection_transaction(tmp_path, monkeypatch):
    with _case(tmp_path, "staging") as case:
        service, principal, collection_id, registration, operation_id, _ = case
        before = _snapshot(service, collection_id, operation_id)
        freeze = archive_write_admission.freeze_registration_execution

        def interrupted(*args, **kwargs):
            freeze(*args, **kwargs)
            raise OSError("injected selected terminal transaction rollback")

        with monkeypatch.context() as patch:
            patch.setattr(archive_write_admission, "freeze_registration_execution", interrupted)
            with pytest.raises(OSError, match="transaction rollback"):
                _select(service, principal, collection_id)
        assert _snapshot(service, collection_id, operation_id) == before
        registration.advance(
            collection_id,
            operation_id,
            limits=LIMITS,
            expected_epoch=before["operation"]["execution_epoch"],
        )
        assert (
            _snapshot(service, collection_id, operation_id)["operation"]["stage_frontier_json"]
            != (before["operation"]["stage_frontier_json"])
        )


@pytest.mark.parametrize("changed", ["revision", "checkpoint", "construction"])
def test_terminal_selection_refuses_changed_active_registration_reservation(tmp_path, changed):
    with _case(tmp_path, "staging") as case:
        service, principal, collection_id, _, operation_id, _ = case
        with session_scope(service._session_factory) as session:
            operation = session.get(CollectionRegistrationRecord, operation_id)
            if changed == "revision":
                operation.base_revision = str(int(operation.base_revision) + 1)
            elif changed == "checkpoint":
                operation.base_checkpoint_json += " "
            else:
                content = json.loads(operation.construction_json)
                content["creation_identity_sha256"] = "f" * 64
                operation.construction_json = json.dumps(content)
        before = _snapshot(service, collection_id, operation_id)
        with pytest.raises(Conflict, match="freeze reservation changed"):
            _select(service, principal, collection_id)
        assert _snapshot(service, collection_id, operation_id) == before


def test_terminal_freeze_preserves_published_acknowledgement_replay(tmp_path):
    with _case(tmp_path, "ready") as case:
        service, principal, collection_id, registration, operation_id, member = case
        acknowledgement = registration.advance(collection_id, operation_id, limits=LIMITS)
        assert acknowledgement["phase"] == "published"
        before = _snapshot(service, collection_id, operation_id)
        _select(service, principal, collection_id)
        selected = _snapshot(service, collection_id, operation_id)
        assert selected["operation"] == before["operation"]
        assert selected["upload"] == before["upload"] and selected["rows"] == before["rows"]
        assert registration.admit(collection_id, [member], principal=principal) == acknowledgement
        assert (
            registration.status(collection_id, operation_id, principal=principal) == acknowledgement
        )
        assert _snapshot(service, collection_id, operation_id) == selected
        with pytest.raises(Conflict, match="admission is closed"):
            registration.admit(collection_id, [_member("3", 1)], principal=principal)


def test_terminal_freeze_keeps_original_unresolved_effect_and_hidden_rows(tmp_path):
    with _case(tmp_path, "staging") as case:
        service, principal, collection_id, _, operation_id, _ = case
        guard = service._payload_write_guard(collection_id, principal)
        admission = ArchiveWriteAdmission(service._session_factory)
        epoch = admission.bind_epoch(collection_id, guard=guard)
        with session_scope(service._session_factory) as session:
            volume = session.scalar(
                select(CollectionArchiveObjectUploadRecord)
                .where(CollectionArchiveObjectUploadRecord.collection_id == collection_id)
                .order_by(CollectionArchiveObjectUploadRecord.sequence)
                .limit(1)
            )
            object_id = volume.object_id
            identity = {
                "kind": volume.kind,
                "object_path": volume.object_path,
                "plan_sha256": volume.plan_sha256,
            }
        admission.prepare(
            collection_id,
            object_id,
            epoch=epoch,
            identity=identity,
            preparation={"original": "retained-test-preparation"},
            grants=service._config.archive_write,
            guard=guard,
        )
        ticket = admission.arm(
            collection_id,
            object_id,
            epoch=epoch,
            kind="begin",
            resource="begin",
            arguments={"original": "retained-test-request"},
            executor_id="e" * 64,
            result_capacity_bytes=1024,
            grants=service._config.archive_write,
            guard=guard,
        )
        admission.record_unknown(ticket)
        before = _snapshot(service, collection_id, operation_id)
        selected = _select(service, principal, collection_id)
        after = _snapshot(service, collection_id, operation_id)
        assert after["rows"] == before["rows"] and after["upload"] == before["upload"]
        assert admission.unresolved(collection_id, limit=1) == (ticket.call_id,)
        with session_scope(service._session_factory) as session:
            effect = session.get(ArchiveWriteEffectRecord, ticket.call_id)
            terminal = session.get(ArchiveUploadTerminalRecord, selected)
            assert effect.unresolved and effect.state == "uncertain" and effect.epoch == epoch
            assert terminal.phase == "draining" and terminal.frontier == "1"
