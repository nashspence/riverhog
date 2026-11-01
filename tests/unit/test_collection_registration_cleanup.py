from __future__ import annotations

import hashlib
import json
from dataclasses import replace

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import (
    CollectionArchiveObjectUploadRecord,
    CollectionUploadArtifactRecord,
    CollectionUploadArtifactVolumeRecord,
    CollectionUploadRawPartDigestRecord,
    CollectionUploadRecord,
)
from riverhog_core.catalog_registration_models import (
    CollectionRegistrationRecord,
    CollectionRegistrationVolumeRecord,
)
from riverhog_core.incremental_emission import VolumeEmissionLimits
from riverhog_core.registration_tuning import RegistrationTuning
from riverhog_core.services import collection_registration_cleanup as cleanup
from riverhog_core.services import collection_uploads as uploads
from riverhog_core.services.app_keys import SqlAlchemyAppKeyService
from riverhog_core.services.collection_registration import CollectionRegistrationOwner
from riverhog_protocol.errors import Conflict
from sqlalchemy import func, select

from tests.unit.test_collection_registration_operations import LIMITS, _close, _member, _open

CLEANUP = VolumeEmissionLimits(2, 2, 10000, 1)


def _snapshot(service, operation_id):
    with session_scope(service._session_factory) as session:
        session.info["include_unpublished_registration"] = True
        op = session.get(CollectionRegistrationRecord, operation_id)
        upload = session.get(CollectionUploadRecord, op.collection_id)
        counts = {}
        for model in (
            CollectionUploadArtifactVolumeRecord,
            CollectionArchiveObjectUploadRecord,
            CollectionUploadArtifactRecord,
            CollectionRegistrationVolumeRecord,
        ):
            counts[model.__tablename__] = session.scalar(
                select(func.count()).select_from(model).where(model.registration_id == operation_id)
            )
        return {
            "phase": op.phase,
            "cleanup": op.cleanup_phase,
            "epoch": op.execution_epoch,
            "disposition": op.disposition_json,
            "failure": op.failure_json,
            "checkpoint": upload.planner_checkpoint_json,
            "accepted_members": upload.artifact_count,
            "accepted_sequence": upload.planner_next_sequence,
            "active": upload.active_registration_id,
            "counts": counts,
        }


def _fence(service, collection_id, *, identity="e" * 64, kind="orphan-discard"):
    with session_scope(service._session_factory) as session:
        if session.get_bind().dialect.name == "sqlite":
            session.connection(execution_options={"sqlite_immediate_writer": True})
        upload = session.scalar(
            select(CollectionUploadRecord)
            .where(CollectionUploadRecord.collection_id == collection_id)
            .with_for_update()
        )
        upload.state = "discarding"
        operation = cleanup.fence_registration_disposal(
            session, upload, kind=kind, identity=identity
        )
        assert operation is not None
        return operation.execution_epoch


def _staged(tmp_path, *, prior_pack=False):
    service, keys, principal, created, collection_id = _open(tmp_path)
    prior = (
        {"artifact_id": "1" * 64, "bytes": "0", "sha256": hashlib.sha256(b"").hexdigest()}
        if prior_pack
        else _member("1", 1)
    )
    service.register_artifacts(collection_id, [prior])
    owner = CollectionRegistrationOwner(service)
    operation = owner.admit(collection_id, [_member()], principal=principal)["operation_id"]
    owner.advance(collection_id, operation, limits=LIMITS)
    owner.advance(collection_id, operation, limits=LIMITS)
    return service, keys, principal, created, collection_id, operation, prior


@pytest.mark.parametrize("prior_pack", [False, True])
def test_cleanup_cold_resumes_bounded_windows_and_preserves_the_accepted_prefix(
    tmp_path, prior_pack
):
    service, keys, principal, created, collection_id, operation, prior = _staged(
        tmp_path, prior_pack=prior_pack
    )
    before = _snapshot(service, operation)
    epoch = _fence(service, collection_id)
    assert epoch != before["epoch"]
    owner = CollectionRegistrationOwner(service)
    with pytest.raises(Conflict, match="execution changed"):
        owner.advance(collection_id, operation, limits=LIMITS, expected_epoch=before["epoch"])
    first = _snapshot(service, operation)
    assert first["phase"] == "cleaning" and first["active"] == operation
    owner.advance(collection_id, operation, limits=CLEANUP, expected_epoch=epoch)
    config, stores, policy = service._config, service._archive_stores, service._policy
    _close(service, keys)
    service = uploads.SqlAlchemyCollectionUploadService(config, stores, policy=policy)
    keys = SqlAlchemyAppKeyService(config)
    try:
        owner = CollectionRegistrationOwner(service)
        previous = _snapshot(service, operation)
        for _ in range(40):
            reply = owner.advance(collection_id, operation, limits=CLEANUP, expected_epoch=epoch)
            current = _snapshot(service, operation)
            assert current["checkpoint"] == before["checkpoint"]
            assert current["accepted_members"] == before["accepted_members"]
            assert current["accepted_sequence"] == before["accepted_sequence"]
            assert all(
                0 <= previous["counts"][name] - count <= 2
                for name, count in current["counts"].items()
            )
            if current["phase"] == "disposed":
                break
            assert current["active"] == operation
            previous = current
        else:
            pytest.fail("bounded cleanup did not finish")
        assert set(current["counts"].values()) == {0}
        assert current["active"] is None and current["cleanup"] == "done"
        assert json.loads(current["disposition"]) == {"kind": "orphan-discard", "sha256": "e" * 64}
        assert (
            owner.advance(collection_id, operation, limits=CLEANUP, expected_epoch=epoch) == reply
        )
        with session_scope(service._session_factory) as session:
            accepted = session.get(
                CollectionUploadArtifactRecord, (collection_id, prior["artifact_id"])
            )
            assert accepted is not None
            assert accepted.registration_id is None
            assert session.scalar(
                select(func.count())
                .select_from(CollectionArchiveObjectUploadRecord)
                .where(CollectionArchiveObjectUploadRecord.collection_id == collection_id)
            ) == (0 if prior_pack else 1)
    finally:
        _close(service, keys)


def test_cleanup_transaction_interruption_rolls_back_deletes_and_progress(tmp_path, monkeypatch):
    service, keys, principal, created, collection_id, operation, prior = _staged(tmp_path)
    try:
        _fence(service, collection_id)
        before = _snapshot(service, operation)
        original = cleanup._delete

        def interrupted(*args, **kwargs):
            original(*args, **kwargs)
            raise RuntimeError("fixture interruption after deletion")

        monkeypatch.setattr(cleanup, "_delete", interrupted)
        with pytest.raises(RuntimeError, match="fixture interruption"):
            CollectionRegistrationOwner(service).advance(collection_id, operation, limits=CLEANUP)
        assert _snapshot(service, operation) == before
        monkeypatch.setattr(cleanup, "_delete", original)
        CollectionRegistrationOwner(service).advance(collection_id, operation, limits=CLEANUP)
        assert _snapshot(service, operation)["counts"] != before["counts"]
    finally:
        _close(service, keys)


def test_cleanup_capacity_preserves_disposition_and_cursor_and_backs_off(tmp_path):
    service, keys, principal, created, collection_id, operation, prior = _staged(tmp_path)
    try:
        _fence(service, collection_id)
        before = _snapshot(service, operation)
        service._config = replace(
            service._config,
            registration=RegistrationTuning(
                plans_per_step=2,
                coverage_rows_per_step=2,
                metadata_bytes_per_step=1,
                input_transitions_per_step=1,
            ),
        )
        assert CollectionRegistrationOwner(service).process_due() == 1
        current = _snapshot(service, operation)
        assert current["phase"] == "cleaning"
        assert current["cleanup"] == before["cleanup"]
        assert current["disposition"] == before["disposition"]
        assert current["counts"] == before["counts"]
        assert json.loads(current["failure"])["kind"] == "capacity"
        assert CollectionRegistrationOwner(service).process_due() == 0
        CollectionRegistrationOwner(service).advance(collection_id, operation, limits=CLEANUP)
        assert _snapshot(service, operation)["failure"] is None
    finally:
        _close(service, keys)


def test_cleanup_refuses_payload_state_before_removing_coverage(tmp_path):
    service, keys, principal, created, collection_id, operation, prior = _staged(tmp_path)
    try:
        _fence(service, collection_id)
        with session_scope(service._session_factory) as session:
            session.info["include_unpublished_registration"] = True
            row = session.scalar(
                select(CollectionArchiveObjectUploadRecord)
                .where(CollectionArchiveObjectUploadRecord.registration_id == operation)
                .order_by(CollectionArchiveObjectUploadRecord.sequence)
                .limit(1)
            )
            row.uploaded_bytes = 1
        before = _snapshot(service, operation)
        with pytest.raises(RuntimeError, match="payload work"):
            CollectionRegistrationOwner(service).advance(collection_id, operation, limits=CLEANUP)
        assert _snapshot(service, operation) == before
    finally:
        _close(service, keys)


def test_cleanup_refuses_member_children_instead_of_using_fk_cascade(tmp_path):
    service, keys, principal, created, collection_id, operation, prior = _staged(tmp_path)
    try:
        _fence(service, collection_id)
        owner = CollectionRegistrationOwner(service)
        for _ in range(20):
            current = _snapshot(service, operation)
            if current["cleanup"] == "members":
                break
            owner.advance(collection_id, operation, limits=CLEANUP)
        assert current["cleanup"] == "members"
        with session_scope(service._session_factory) as session:
            session.add(
                CollectionUploadRawPartDigestRecord(
                    collection_id=collection_id,
                    artifact_id="2" * 64,
                    part_number=0,
                    sha256="a" * 64,
                )
            )
        before = _snapshot(service, operation)
        with pytest.raises(RuntimeError, match="dependent state"):
            owner.advance(collection_id, operation, limits=CLEANUP)
        assert _snapshot(service, operation) == before
        with session_scope(service._session_factory) as session:
            assert (
                session.get(CollectionUploadRawPartDigestRecord, (collection_id, "2" * 64, 0))
                is not None
            )
    finally:
        _close(service, keys)


@pytest.mark.parametrize("kind", ["cancel", "orphan-discard"])
def test_cleanup_reservation_prevents_new_registration_and_changed_disposal(tmp_path, kind):
    service, keys, principal, created, collection_id, operation, prior = _staged(tmp_path)
    try:
        epoch = _fence(service, collection_id, kind=kind)
        assert _fence(service, collection_id, kind=kind) == epoch
        with pytest.raises(Conflict, match="selection differs"):
            _fence(service, collection_id, identity="f" * 64, kind=kind)
        with pytest.raises(Conflict, match="selection differs"):
            _fence(
                service,
                collection_id,
                kind="cancel" if kind == "orphan-discard" else "orphan-discard",
            )
        with session_scope(service._session_factory) as session:
            upload = session.get(CollectionUploadRecord, collection_id)
            upload.state = "open"
        with pytest.raises(Conflict, match="pending"):
            CollectionRegistrationOwner(service).admit(
                collection_id, [_member("3", 1)], principal=principal
            )
        assert _snapshot(service, operation)["active"] == operation
    finally:
        _close(service, keys)


@pytest.mark.parametrize("changed", ["construction", "owner", "disposition"])
def test_cleanup_refuses_a_changed_construction_or_terminal_selection(tmp_path, changed):
    service, keys, principal, created, collection_id, operation, prior = _staged(tmp_path)
    try:
        _fence(service, collection_id)
        with session_scope(service._session_factory) as session:
            upload = session.get(CollectionUploadRecord, collection_id)
            op = session.get(CollectionRegistrationRecord, operation)
            if changed == "construction":
                upload.archive_generation = "f" * 64
            elif changed == "owner":
                upload.state = "orphaned"
            else:
                op.disposition_json = "{}"
        before = _snapshot(service, operation)
        with pytest.raises((Conflict, RuntimeError), match="reservation|terminal owner|selection"):
            CollectionRegistrationOwner(service).advance(collection_id, operation, limits=CLEANUP)
        assert _snapshot(service, operation) == before
    finally:
        _close(service, keys)
