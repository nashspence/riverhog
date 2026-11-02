from __future__ import annotations

from contextlib import contextmanager

import pytest
from riverhog_core.catalog_archive_write_models import (
    ArchiveUploadTerminalRecord,
    ArchiveWriteControlRecord,
)
from riverhog_core.catalog_db import dispose_session_factory, session_scope
from riverhog_core.catalog_models import CollectionUploadRecord
from riverhog_core.catalog_workflow_models import CollectionProcessingClaimRecord
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from riverhog_core.services.collection_upload_authority import (
    retain_upload_claim,
    retain_upload_key,
)
from riverhog_core.services.collection_workflows import _execution_output_exists
from riverhog_protocol.errors import Conflict
from sqlalchemy import event, select

from tests.unit.test_collection_registration_delegation import _delegated
from tests.unit.test_collection_registration_operations import _close, _open


@contextmanager
def _case(tmp_path, *, delegated):
    workflows = None
    if delegated:
        service, keys, workflows, principal, collection_id = _delegated(tmp_path)
        identity = principal.id.removeprefix("processing:")
        source = principal.id
    else:
        service, keys, principal, _, collection_id = _open(tmp_path)
        identity, source = "bounded-registration", "fixture"
    try:
        request = {
            "idempotency_key": identity,
            "ingest_source": source,
            "archive_store": None,
            "initiator": principal,
            "event_context": None,
        }
        yield service, principal, collection_id, request
    finally:
        if workflows is not None:
            dispose_session_factory(workflows._session_factory)
        _close(service, keys)


def _select(service, principal, collection_id, kind):
    # The fixture selects the lower-level owner without granting cleanup.
    def guard(session):
        retain_upload_key(session, principal)
        retain_upload_claim(session, principal.id, principal.key_id)

    return ArchiveWriteAdmission(service._session_factory).select_terminal(
        collection_id,
        kind=kind,
        request={"fixture": "original-terminal"},
        principal_id=principal.id,
        grants=service._config.archive_write,
        guard=guard,
        validate_selection=lambda *args: None,
    )


def _snapshot(service, collection_id):
    with session_scope(service._session_factory) as session:
        upload = session.get(CollectionUploadRecord, collection_id)
        control = session.get(ArchiveWriteControlRecord, collection_id)
        terminal = session.get(ArchiveUploadTerminalRecord, control.terminal_id)
        return (
            tuple(getattr(upload, column.name) for column in upload.__table__.columns),
            tuple(getattr(control, column.name) for column in control.__table__.columns),
            tuple(getattr(terminal, column.name) for column in terminal.__table__.columns),
        )


@pytest.mark.parametrize("delegated", [False, True])
@pytest.mark.parametrize("state", ["open", "orphaned"])
@pytest.mark.parametrize("kind", ["cancel", "discard"])
def test_existing_output_resume_cannot_cross_selected_terminal(tmp_path, delegated, state, kind):
    with _case(tmp_path, delegated=delegated) as case:
        service, principal, collection_id, request = case
        with session_scope(service._session_factory) as session:
            upload = session.get(CollectionUploadRecord, collection_id)
            upload.state = state
            if state == "orphaned":
                upload.orphaned_at = "2000-01-01T00:00:00Z"
                upload.archive_phase = "orphaned"
        selected = _select(service, principal, collection_id, kind)
        before = _snapshot(service, collection_id)
        with pytest.raises(Conflict, match="admission is closed"):
            service.create_or_resume(**request)
        assert _snapshot(service, collection_id) == before
        assert _select(service, principal, collection_id, kind) == selected
        assert _snapshot(service, collection_id) == before


@pytest.mark.parametrize("kind", ["cancel", "discard"])
def test_selected_terminal_keeps_independent_creation_authority(tmp_path, kind):
    with _case(tmp_path, delegated=False) as case:
        service, principal, collection_id, request = case
        _select(service, principal, collection_id, kind)
        before = _snapshot(service, collection_id)
        opened = service.create_or_resume(**{**request, "idempotency_key": "independent"})
        other_id = int(opened["collection_id"])
        assert other_id != collection_id
        assert service.create_or_resume(**{**request, "idempotency_key": "independent"})["resumed"]
        with session_scope(service._session_factory) as session:
            assert session.get(ArchiveWriteControlRecord, other_id).gate == "open"
        assert _snapshot(service, collection_id) == before


def test_processing_output_existence_is_an_indexed_point_seek(tmp_path):
    with _case(tmp_path, delegated=True) as case:
        service, principal, _, _ = case
        with session_scope(service._session_factory) as session:
            claim = session.scalar(
                select(CollectionProcessingClaimRecord).where(
                    CollectionProcessingClaimRecord.execution_id
                    == principal.id.removeprefix("processing:")
                )
            )
            queries = []

            def capture(connection, cursor, statement, parameters, context, executemany):
                if statement.lstrip().upper().startswith("SELECT"):
                    queries.append((statement, parameters))

            engine = session.get_bind()
            event.listen(engine, "before_cursor_execute", capture)
            try:
                assert _execution_output_exists(session, claim)
            finally:
                event.remove(engine, "before_cursor_execute", capture)
            assert len(queries) == 3
            for statement, parameters in queries:
                assert "COUNT(" not in statement.upper()
                assert "LIMIT" in statement.upper()
                plan = (
                    session.connection()
                    .exec_driver_sql("EXPLAIN QUERY PLAN " + statement, parameters)
                    .all()
                )
                assert not any("SCAN " in row[-1].upper() for row in plan)
                assert any("SEARCH " in row[-1].upper() for row in plan)


@pytest.mark.parametrize("state", ["open", "orphaned"])
def test_output_claim_expiry_after_upload_lock_is_rechecked(tmp_path, monkeypatch, state):
    from riverhog_core.services import collection_upload_authority, collection_uploads
    from riverhog_protocol.errors import NotFound
    from time_formats import format_utc_ns, parse_utc_timestamp

    with _case(tmp_path, delegated=True) as case:
        service, _, collection_id, request = case
        with session_scope(service._session_factory) as session:
            upload = session.get(CollectionUploadRecord, collection_id)
            upload.state = state
            if state == "orphaned":
                upload.orphaned_at = "2000-01-01T00:00:00Z"
                upload.archive_phase = "orphaned"
            original = tuple(getattr(upload, column.name) for column in upload.__table__.columns)
        clock = [parse_utc_timestamp("2026-11-01T00:00:00Z")]
        monkeypatch.setattr(collection_uploads, "utc_epoch_ns_now", lambda: clock[0])
        monkeypatch.setattr(
            collection_upload_authority, "utc_timestamp_now", lambda: format_utc_ns(clock[0])
        )
        observations = []

        def after_lock(connection, cursor, statement, parameters, context, executemany):
            if (
                "FROM collection_uploads" in statement
                and "collection_uploads.idempotency_key =" in statement
            ):
                clock[0] = parse_utc_timestamp("2999-01-02T00:00:00Z")
                observations.append(statement)

        engine = service._session_factory.kw["bind"]
        event.listen(engine, "after_cursor_execute", after_lock)
        try:
            with pytest.raises(NotFound, match="execution authority"):
                service.create_or_resume(**request)
        finally:
            event.remove(engine, "after_cursor_execute", after_lock)
        assert observations
        with session_scope(service._session_factory) as session:
            upload = session.get(CollectionUploadRecord, collection_id)
            assert (
                tuple(getattr(upload, column.name) for column in upload.__table__.columns)
                == original
            )
            assert session.get(ArchiveWriteControlRecord, collection_id).gate == "open"
