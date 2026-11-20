from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace

import pytest
from riverhog_core.app_permissions import COLLECTIONS_CREATE, COLLECTIONS_DELETE, ApplicationAccess
from riverhog_core.catalog_archive_write_models import ArchiveUploadTerminalRecord as Terminal
from riverhog_core.catalog_base import Base
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_initial_metadata_models import (
    CollectionUploadInitialTagPublicationRecord,
)
from riverhog_core.catalog_journal_validation_models import JournalValidationJobRecord as Job
from riverhog_core.catalog_models import CollectionUploadProvenanceJournalRecord as Journal
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.catalog_object_cleanup_models import ObjectCleanupPageRecord as Page
from riverhog_core.catalog_registration_models import (
    CollectionRegistrationRecord as Registration,
)
from riverhog_core.catalog_registration_models import (
    CollectionRegistrationVolumeRecord as Summary,
)
from riverhog_core.checkpoint_sha256 import CheckpointSHA256
from riverhog_core.incremental_emission import VolumeEmissionCapacity
from riverhog_core.services import archive_upload_catalog_cleanup as module
from riverhog_core.services.archive_upload_catalog_cleanup import ArchiveUploadCatalogCleanup
from riverhog_core.services.catalog_retirement import metadata_row_bytes
from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy import Column, ForeignKey, Integer, Table, event, func, select

from tests.unit.test_app_keys import BOOTSTRAP
from tests.unit.test_archive_upload_registration_cleanup import (
    _complete_registration,
    _composed_registration,
    _prepared,
)
from tests.unit.test_collection_registration_cleanup import CLEANUP
from tests.unit.test_upload_journal_validation_retirement_catalog_growth import fields

LIMITS = replace(CLEANUP, input_transitions=2)


def _owner(ready):
    return ArchiveUploadCatalogCleanup(ready.case.factory, ready.ready.owner)


def _advance(ready, *, limits=LIMITS):
    return _owner(ready).advance(
        ready.case.collection_id, ready.case.operation, guard=ready.guard, limits=limits
    )


def _counts(ready):
    with session_scope(ready.case.factory) as session:
        return {
            name: session.scalar(
                select(func.count())
                .select_from(Base.metadata.tables[name])
                .where(Base.metadata.tables[name].c.collection_id == ready.case.collection_id)
            )
            for name in module.UPLOAD_CHILD_TABLES
        }


def _receipts(ready):
    with session_scope(ready.case.factory) as session:
        operations = session.scalars(
            select(Registration)
            .where(Registration.collection_id == ready.case.collection_id)
            .order_by(Registration.id)
        ).all()
        return (
            [
                tuple(getattr(row, c.name) for c in Registration.__table__.columns)
                for row in operations
            ],
            [
                tuple(row)
                for row in session.execute(
                    select(Summary.__table__)
                    .join(Registration)
                    .where(Registration.collection_id == ready.case.collection_id)
                    .order_by(Summary.registration_id, Summary.sequence)
                )
            ],
        )


def _finish_children(ready, *, expected_extra=0):
    prior = _counts(ready)
    receipts = _receipts(ready)
    removed = 0
    for _ in range(expected_extra + 100):
        progress = _advance(ready)
        current = _counts(ready)
        assert 0 <= progress.removed <= LIMITS.input_transitions
        assert progress.metadata_bytes <= LIMITS.metadata_bytes
        assert sum(prior.values()) - sum(current.values()) == progress.removed
        if progress.upload_children_exhausted:
            assert progress.table is None and progress.removed == 0
            assert set(current.values()) == {0}
            break
        assert progress.table is not None and progress.removed > 0
        removed += progress.removed
        prior = current
    else:
        pytest.fail("bounded upload child cleanup did not exhaust its exact scope")
    assert _receipts(ready) == receipts
    with session_scope(ready.case.factory) as session:
        assert session.get(Upload, ready.case.collection_id) is not None
        terminal = session.get(Terminal, ready.case.operation)
        assert terminal.original_result_json == ready.original
        assert terminal.result_json is None and terminal.phase == "cleaning"
        assert session.get(Page, ready.case.operation) is not None
    assert _advance(ready).upload_children_exhausted
    return removed


def test_upload_child_routes_cover_current_schema_and_reject_unreviewed_cascades():
    module.validate_upload_cleanup_routes()
    extra = Table(
        "unrouted_upload_child",
        Base.metadata,
        Column(
            "collection_id",
            Integer,
            ForeignKey("collection_uploads.collection_id"),
            primary_key=True,
        ),
    )
    try:
        with pytest.raises(RuntimeError, match="routes differ"):
            module.validate_upload_cleanup_routes()
    finally:
        Base.metadata.remove(extra)
    module.validate_upload_cleanup_routes()


def test_upload_children_cold_resume_without_parent_or_independent_receipt_retirement(
    tmp_path, monkeypatch
):
    with _prepared(tmp_path, monkeypatch) as ready:
        _complete_registration(ready)
        assert _finish_children(ready) > 0


@pytest.mark.parametrize("provider_ready", (False, True))
def test_catalog_disposal_requires_archive_proof_and_completed_hidden_registration(
    tmp_path, monkeypatch, provider_ready
):
    with _prepared(tmp_path, monkeypatch, provider_ready=provider_ready) as ready:
        before = _counts(ready)
        with pytest.raises(Conflict, match="hidden registration|inventory proof"):
            _advance(ready)
        assert _counts(ready) == before


def _revoked_children(ready):
    assert _advance(ready).removed > 0
    before = _counts(ready)
    case = ready.case
    case.keys.replace_access(
        app=case.principal.id,
        key_id=case.principal.key_id,
        access=(ApplicationAccess(COLLECTIONS_DELETE),),
        grantor=BOOTSTRAP,
    )
    with pytest.raises(NotFound):
        _advance(ready)
    assert _counts(ready) == before
    case.keys.replace_access(
        app=case.principal.id,
        key_id=case.principal.key_id,
        access=(ApplicationAccess(COLLECTIONS_CREATE),),
        grantor=BOOTSTRAP,
    )
    _finish_children(ready)


def test_each_catalog_window_requires_current_terminal_rights(tmp_path, monkeypatch):
    with _prepared(tmp_path, monkeypatch) as ready:
        _complete_registration(ready)
        _revoked_children(ready)


def _lost_children(ready, monkeypatch, committed):
    actual = module.session_scope

    @contextmanager
    def lose(factory):
        with actual(factory) as session:
            yield session
            if not committed:
                raise OSError("catalog cleanup rolled back")
        raise OSError("catalog cleanup commit reply lost")

    before = _counts(ready)
    with monkeypatch.context() as patch:
        patch.setattr(module, "session_scope", lose)
        with pytest.raises(OSError, match="catalog cleanup"):
            _advance(ready)
    assert (_counts(ready) == before) is not committed
    _finish_children(ready)


@pytest.mark.parametrize("committed", (False, True))
def test_catalog_cleanup_retries_first_live_rows_after_original_sql_interruption(
    tmp_path, monkeypatch, committed
):
    with _prepared(tmp_path, monkeypatch) as ready:
        _complete_registration(ready)
        _lost_children(ready, monkeypatch, committed)


def _seed_tags(ready, count, *, width=17):
    with session_scope(ready.case.factory) as session:
        session.add_all(
            CollectionUploadInitialTagPublicationRecord(
                collection_id=ready.case.collection_id,
                object_id=f"{number:064x}",
                role="state",
                document_bytes=b"x" * width,
                source_json='{"source":"retained"}',
                source_sha256="a" * 64,
            )
            for number in range(count)
        )


def test_full_opaque_metadata_bytes_refuse_capacity_without_deleting_the_row(tmp_path, monkeypatch):
    with _prepared(tmp_path, monkeypatch) as ready:
        _complete_registration(ready)
        _seed_tags(ready, 1, width=10001)
        for _ in range(100):
            before = _counts(ready)
            try:
                _advance(ready)
            except VolumeEmissionCapacity as error:
                assert error.dimension == "metadata_bytes" and error.required > 10001
                assert _counts(ready) == before
                break
        else:
            pytest.fail("opaque metadata was omitted from catalog admission")
        _finish_with_larger = replace(LIMITS, metadata_bytes=20000)
        assert _advance(ready, limits=_finish_with_larger).removed == 1
        _finish_children(ready)
    assert metadata_row_bytes({"collection_id": 2**60, "sequence": 2**256 - 1}) > 90


def _seed_validation(ready):
    identity = "urn:uuid:00000000-0000-0000-0000-000000000001"
    values = fields("a" * 32, linked=identity, width=4097, epoch=2**60)
    values["collection_id"] = ready.case.collection_id
    with session_scope(ready.case.factory) as session:
        session.add(
            Journal(
                collection_id=ready.case.collection_id,
                journal_id=identity,
                bytes=0,
                sha256="b" * 64,
                content_hash_state=CheckpointSHA256().export_state(),
            )
        )
        session.flush()
        session.add(Job(**values))
    return values


def _validation_detachment(ready):
    values = _seed_validation(ready)
    _finish_children(ready)
    with session_scope(ready.case.factory) as session:
        job = session.get(Job, values["workspace_id"])
        assert job is not None and job.state == "retiring"
        assert job.collection_id is None and job.journal_id is None and job.failure is None
        assert job.version != values["version"]
        for name in (
            "source_binding_json",
            "certificate_binding_json",
            "worker_epoch",
            "attempt_id",
            "progress_json",
            "outcome_json",
        ):
            assert getattr(job, name) == values[name]


def test_catalog_journal_retirement_preserves_independent_native_workspace_receiving(
    tmp_path, monkeypatch
):
    with _prepared(tmp_path, monkeypatch) as ready:
        _complete_registration(ready)
        _validation_detachment(ready)


@pytest.mark.parametrize("count", (17, 65, 129))
def test_actual_first_live_catalog_windows_have_linear_indexed_query_work(
    tmp_path, monkeypatch, count, record_property
):
    with _prepared(tmp_path, monkeypatch) as ready:
        _complete_registration(ready)
        _seed_tags(ready, count)
        statements = []

        def observe(connection, cursor, statement, parameters, context, executemany):
            compiled = getattr(context, "compiled", None)
            if compiled is not None:
                assert getattr(compiled.statement, "_offset_clause", None) is None
            statements.append(statement)

        engine = ready.case.factory.kw["bind"]
        event.listen(engine, "before_cursor_execute", observe)
        try:
            turns, removed = 0, 0
            while True:
                progress = _advance(ready)
                turns += 1
                removed += progress.removed
                if progress.upload_children_exhausted:
                    break
                assert turns <= count + 100
        finally:
            event.remove(engine, "before_cursor_execute", observe)
        assert removed >= count
        assert len(statements) <= 100 * (count + 20)
        assert all("COUNT(" not in sql.upper() for sql in statements)
        child_reads = [
            sql
            for sql in statements
            if sql.lstrip().upper().startswith("SELECT")
            and "FROM collection_upload_initial_tag_publications" in sql
        ]
        assert child_reads and all("WHERE" in sql and "LIMIT" in sql for sql in child_reads)
        record_property("catalog_rows", removed)
        record_property("catalog_turns", turns)
        record_property("catalog_sql_statements", len(statements))


def _composed_children(ready, monkeypatch, *, checkpoints=False, copy_intents=False):
    quantum, created = _composed_registration(
        ready, monkeypatch, catalog=True, checkpoints=checkpoints, copy_intents=copy_intents
    )
    before = _counts(ready)
    assert sum(before.values()) > 0
    receipts = _receipts(ready)
    calls = len(ready.ready.target.calls)
    for _ in range(100):
        progress = quantum()
        current = _counts(ready)
        assert progress.archive_inventory_exhausted and progress.registration_disposed
        assert 0 <= sum(before.values()) - sum(current.values()) <= CLEANUP.input_transitions
        if progress.upload_children_exhausted:
            break
        assert progress.pending_reason == "catalog-cleanup"
        before = current
    else:
        pytest.fail("original terminal did not compose bounded catalog cleanup")
    assert set(current.values()) == {0}
    if checkpoints:
        assert progress.pending_reason in {None, "checkpoint-cleanup"}
    else:
        assert progress.pending_reason is None
    assert not progress.execution_stopped
    assert created == [True] and len(ready.ready.target.calls) == calls
    assert _receipts(ready) == receipts
    assert ready.case.service._archive_retention._counts == {False: 0, True: 1}
    with session_scope(ready.case.factory) as session:
        terminal = session.get(Terminal, ready.case.operation)
        assert terminal.original_result_json == ready.original and terminal.result_json is None
        assert session.get(Upload, ready.case.collection_id) is not None
    return quantum, created


def test_original_terminal_worker_composes_catalog_only_after_archive_and_registration(
    tmp_path, monkeypatch
):
    with _prepared(tmp_path, monkeypatch, provider_ready=False) as ready:
        _composed_children(ready, monkeypatch)
