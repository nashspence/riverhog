from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

import pytest
from riverhog_core.app_permissions import COLLECTIONS_CREATE, ApplicationAccess
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord
from riverhog_core.catalog_registration_models import CollectionRegistrationRecord
from riverhog_core.incremental_emission import parse_volume_emission_frontier
from riverhog_core.registration_tuning import RegistrationTuning
from riverhog_core.services import collection_registration as registration
from riverhog_core.services.collection_registration import CollectionRegistrationOwner
from riverhog_protocol.errors import Conflict
from sqlalchemy import func, select
from time_formats import format_utc_timestamp, utc_now

from tests.unit.test_app_keys import BOOTSTRAP
from tests.unit.test_collection_registration_operations import (
    _close,
    _member,
    _open,
    _read,
    _until,
)


def _tune(service, **overrides):
    service._config = replace(
        service._config,
        registration=RegistrationTuning(
            plans_per_step=3,
            coverage_rows_per_step=16,
            metadata_bytes_per_step=overrides.get("metadata_bytes_per_step", 100000),
            input_transitions_per_step=4,
        ),
    )


def _other(service, principal):
    return int(
        service.create_or_resume(
            idempotency_key="another-construction",
            ingest_source="fixture",
            archive_store=None,
            initiator=principal,
            event_context=None,
        )["collection_id"]
    )


def _sequence(service, operation):
    return parse_volume_emission_frontier(
        _read(service, operation)["stage"]
    ).checkpoint.next_sequence


def test_due_turn_is_one_configured_quantum_per_construction_and_rotates_fairly(tmp_path):
    service, keys, principal, created, first = _open(tmp_path)
    try:
        _tune(service)
        second = _other(service, principal)
        owner = CollectionRegistrationOwner(service)
        a = owner.admit(first, [_member()], principal=principal)["operation_id"]
        b = owner.admit(second, [_member("3", 4)], principal=principal)["operation_id"]
        assert CollectionRegistrationOwner(service).process_due(limit=2) == 2
        assert (_sequence(service, a), _sequence(service, b)) == (3, 3)
        assert CollectionRegistrationOwner(service).process_due(limit=100) == 2
        assert (_sequence(service, a), _sequence(service, b)) == (6, 4)
        assert service.list_volumes(first)["volumes"] == []
        assert service.list_volumes(second)["volumes"] == []
    finally:
        _close(service, keys)


def test_due_resource_refusal_preserves_progress_and_backs_off_until_retry(tmp_path, monkeypatch):
    service, keys, principal, created, collection_id = _open(tmp_path)
    try:
        _tune(service, metadata_bytes_per_step=1)
        owner = CollectionRegistrationOwner(service)
        pending = owner.admit(collection_id, [_member()], principal=principal)
        operation = pending["operation_id"]
        before = _read(service, operation)
        assert CollectionRegistrationOwner(service).process_due() == 1
        after = _read(service, operation)
        assert after["stage"] == before["stage"] and after["epoch"] == before["epoch"]
        status = owner.status(collection_id, operation, principal=principal)
        assert status["failure"]["kind"] == "capacity"
        assert CollectionRegistrationOwner(service).process_due() == 0
        with session_scope(service._session_factory) as session:
            session.info["include_unpublished_registration"] = True
            assert (
                session.scalar(
                    select(func.count())
                    .select_from(CollectionArchiveObjectUploadRecord)
                    .where(CollectionArchiveObjectUploadRecord.registration_id == operation)
                )
                == 0
            )
        _tune(service)
        later = utc_now() + timedelta(minutes=1)
        monkeypatch.setattr(registration, "utc_now", lambda: later)
        monkeypatch.setattr(registration, "utc_timestamp_now", lambda: format_utc_timestamp(later))
        assert CollectionRegistrationOwner(service).process_due() == 1
        assert _sequence(service, operation) == 3
        assert "failure" not in owner.status(collection_id, operation, principal=principal)
    finally:
        _close(service, keys)


def test_due_authority_pause_fences_old_worker_and_exact_admission_resumes_cursor(tmp_path):
    service, keys, principal, created, collection_id = _open(tmp_path)
    try:
        _tune(service)
        owner = CollectionRegistrationOwner(service)
        operation = owner.admit(collection_id, [_member()], principal=principal)["operation_id"]
        assert CollectionRegistrationOwner(service).process_due() == 1
        before = _read(service, operation)
        keys.replace_access(
            app=principal.id,
            key_id=principal.key_id,
            access=(ApplicationAccess(COLLECTIONS_CREATE),),
            grantor=BOOTSTRAP,
        )
        assert CollectionRegistrationOwner(service).process_due() == 1
        paused = _read(service, operation)
        assert paused["phase"] == "paused"
        assert paused["epoch"] != before["epoch"]
        assert paused["stage"] == before["stage"]
        assert CollectionRegistrationOwner(service).process_due() == 0
        renewed = keys.authenticate(str(created["token"]))
        reply = owner.admit(collection_id, [_member()], principal=renewed)
        assert reply["phase"] == "staging"
        assert _read(service, operation)["stage"] == before["stage"]
        with pytest.raises(Conflict, match="execution changed"):
            owner.advance(
                collection_id,
                operation,
                limits=service._config.registration.emission_limits(),
                expected_epoch=before["epoch"],
            )
        assert CollectionRegistrationOwner(service).process_due() == 1
        assert _sequence(service, operation) == 6
    finally:
        _close(service, keys)


def test_due_integrity_failure_is_retained_without_publishing_or_hot_looping(tmp_path):
    service, keys, principal, created, collection_id = _open(tmp_path)
    try:
        _tune(service)
        owner = CollectionRegistrationOwner(service)
        operation = owner.admit(collection_id, [_member()], principal=principal)["operation_id"]
        _until(service, collection_id, operation, "ready")
        with session_scope(service._session_factory) as session:
            record = session.get(CollectionRegistrationRecord, operation)
            record.verify_chain = "f" * 64
        assert CollectionRegistrationOwner(service).process_due() == 1
        assert _read(service, operation)["phase"] == "failed"
        assert CollectionRegistrationOwner(service).process_due() == 0
        status = owner.status(collection_id, operation, principal=principal)
        assert status["failure"] == {"kind": "integrity"}
        assert service.list_volumes(collection_id)["volumes"] == []
    finally:
        _close(service, keys)


def test_sqlite_due_writer_contention_defers_without_touching_the_cursor(tmp_path):
    service, keys, principal, created, collection_id = _open(tmp_path)
    try:
        _tune(service)
        owner = CollectionRegistrationOwner(service)
        operation = owner.admit(collection_id, [_member()], principal=principal)["operation_id"]
        before = _read(service, operation)
        with service._session_factory.kw["bind"].connect() as writer:
            with writer.execution_options(sqlite_immediate_writer=True).begin():
                assert CollectionRegistrationOwner(service).process_due() == 0
        assert _read(service, operation) == before
        assert CollectionRegistrationOwner(service).process_due() == 1
        assert _sequence(service, operation) == 3
    finally:
        _close(service, keys)
