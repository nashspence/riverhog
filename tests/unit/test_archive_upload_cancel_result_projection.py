from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.catalog_archive_write_models import ArchiveUploadTerminalRecord as Terminal
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import ArchiveCopyJobRecord as CopyJob
from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord as Volume
from riverhog_core.catalog_models import CollectionUploadCopyIntentRecord as Intent
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.incremental_emission import VolumeEmissionCapacity
from riverhog_core.services import archive_upload_cancel_result_projection as module
from riverhog_core.services import collection_uploads
from riverhog_core.services.archive_upload_cancel_result_projection import (
    ArchiveUploadCancelResultProjection,
    decode_cancel_terminal_result,
)
from riverhog_core.services.archive_upload_terminal_control import terminal_control_bytes
from riverhog_core.services.archive_upload_terminal_tags import load_terminal_tag_selector
from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy import delete, event, select

from tests.unit.test_archive_upload_projection_cleanup import LIMITS as SMALL_LIMITS
from tests.unit.test_archive_upload_terminal_tags import _tagged
from tests.unit.test_collection_uploads import _CREATOR
from tests.unit.test_upload_cancel_authority import _select
from tests.unit.test_upload_terminal_read_authority import _creator_key

LIMITS = replace(SMALL_LIMITS, metadata_bytes=100000)


@contextmanager
def _cancel(tmp_path, *, count=17, tags=3, copies=0, visible=None, database_url=None):
    destinations = [f"target{index:05d}" for index in range(copies)]
    with _tagged(tmp_path, count=tags, database_url=database_url, copy_to=destinations) as case:
        with session_scope(case.factory) as session:
            original = session.get(Volume, (case.collection_id, case.object_id))
            values = {
                column.name: getattr(original, column.name)
                for column in Volume.__table__.columns
                if column.computed is None
            }
            session.execute(delete(Volume).where(Volume.collection_id == case.collection_id))
            for index in range(count):
                session.add(
                    Volume(
                        **{
                            **values,
                            "object_id": f"{index + 1:064x}",
                            "sequence": index,
                            "uploaded_bytes": 1,
                            "uploaded_units": 1,
                            "total_units": 2,
                        }
                    )
                )
            upload = session.get(Upload, case.collection_id)
            upload.planner_next_sequence = count if visible is None else visible
            for name in destinations:
                intent = session.get(Intent, (case.collection_id, name))
                intent.state = "failed"
                intent.attempts = 3
                intent.failure_code = "original-copy-failure"
                intent.job_created = False
        if tags:
            case.keys, case.principal, _ = _creator_key(case)
        else:
            from riverhog_core.app_permissions import COLLECTIONS_CREATE, ApplicationAccess
            from riverhog_core.services.app_keys import SqlAlchemyAppKeyService

            from tests.unit.test_app_keys import create_key

            case.keys = SqlAlchemyAppKeyService(case.config, session_factory=case.factory)
            created = create_key(
                case.keys, app=_CREATOR.id, access=(ApplicationAccess(COLLECTIONS_CREATE),)
            )
            case.principal = case.keys.authenticate(str(created["token"]))
            assert case.principal is not None
        with session_scope(case.factory) as session:
            expected = collection_uploads._upload_payload(
                session, session.get(Upload, case.collection_id), state="canceled"
            )
        expected.update(latest_failure=None, archive_phase="canceled", archive_next_attempt_at=None)
        case.operation = _select(
            case,
            case.principal,
            case.service._cancel_write_guard(case.collection_id, case.principal),
        )
        guard = case.service._terminal_write_guard(case.collection_id, case.operation)
        yield SimpleNamespace(
            case=case,
            guard=guard,
            expected=expected,
            count=count if visible is None else visible,
            tags=tags,
            copies=copies,
        )


def _state(ready):
    with session_scope(ready.case.factory) as session:
        terminal = session.get(Terminal, ready.case.operation)
        return (
            terminal.phase,
            terminal.result_preparation_json,
            terminal.original_result_json,
            terminal.result_json,
        )


def _advance(ready, *, limits=LIMITS):
    return ArchiveUploadCancelResultProjection(
        ready.case.factory, ready.case.service._archive_terminal_observation
    ).advance(ready.case.collection_id, ready.case.operation, guard=ready.guard, limits=limits)


def _counts_done(ready):
    for _ in range((ready.count + ready.tags) // LIMITS.input_transitions + 8):
        state = _state(ready)[1]
        if state is not None and json.loads(state)["stage"] == "result":
            return
        progress = _advance(ready)
        assert not progress.prepared and progress.rows <= LIMITS.input_transitions
    pytest.fail("cancel counters did not finish")


def _finish(ready):
    windows = []
    for _ in range((ready.count + ready.tags) // LIMITS.input_transitions + 8):
        progress = _advance(ready)
        windows.append(progress)
        assert progress.rows <= LIMITS.input_transitions
        assert progress.metadata_bytes <= LIMITS.metadata_bytes
        assert progress.phase == "draining" and progress.pending_reason is None
        if progress.prepared:
            break
    else:
        pytest.fail("original rich cancel response did not finish")
    phase, preparation, encoded, result = _state(ready)
    assert phase == "draining" and preparation is None and result is None
    actual = decode_cancel_terminal_result(encoded)
    expected = {**ready.expected, "archive_phase_updated_at": actual["archive_phase_updated_at"]}
    assert actual == expected
    from riverhog_api.schemas.collections import CollectionUploadSessionOut

    CollectionUploadSessionOut.model_validate(actual)
    assert _advance(ready).prepared and _state(ready)[2] == encoded
    with session_scope(ready.case.factory) as session:
        assert session.get(Upload, ready.case.collection_id) is not None
        assert session.get(Terminal, ready.case.operation).result_json is None
    return windows, encoded


@pytest.mark.parametrize("count,tags,copies", ((0, 0, 0), (1, 1, 1), (17, 3, 17), (65, 17, 0)))
def test_cancel_original_response_resumes_exact_rich_fields_before_cleanup(
    tmp_path, count, tags, copies
):
    with _cancel(tmp_path, count=count, tags=tags, copies=copies) as ready:
        windows, _ = _finish(ready)
        assert sum(window.rows for window in windows) == count + tags
        assert sum(window.copy_rows for window in windows) == copies
        assert (
            ready.case.service._archive_terminal_adjudication.adjudicate(
                ready.case.collection_id, ready.case.operation, guard=ready.guard
            ).phase
            == "cleanup-ready"
        )


@pytest.mark.parametrize("committed", (False, True))
@pytest.mark.parametrize("complete", (False, True))
def test_cancel_projection_keeps_original_progress_on_rollback_or_lost_ack(
    tmp_path, monkeypatch, committed, complete
):
    with _cancel(tmp_path) as ready:
        if complete:
            _counts_done(ready)
        actual = module.session_scope

        @contextmanager
        def lose(factory):
            with actual(factory) as session:
                yield session
                if not committed:
                    raise OSError("cancel projection rollback")
            raise OSError("cancel projection lost reply")

        before = _state(ready)
        with monkeypatch.context() as patch:
            patch.setattr(module, "session_scope", lose)
            with pytest.raises(OSError, match="cancel projection"):
                _advance(ready)
        assert (_state(ready) != before) is committed
        _finish(ready)


def _revoked(ready):
    assert not _advance(ready).prepared
    before = _state(ready)
    ready.case.keys.revoke(app=ready.case.principal.id, key_id=ready.case.principal.key_id)
    with pytest.raises(NotFound):
        _advance(ready)
    assert _state(ready) == before


def test_cancel_projection_cold_resume_rechecks_current_original_rights(tmp_path):
    with _cancel(tmp_path) as ready:
        _revoked(ready)


@pytest.mark.parametrize(
    "damage", ("description", "frontier", "source", "stage", "counter", "cursor")
)
def test_cancel_projection_rejects_changed_source_or_durable_preparation(tmp_path, damage):
    with _cancel(tmp_path, tags=17) as ready:
        assert not _advance(ready).prepared
        with session_scope(ready.case.factory) as session:
            terminal = session.get(Terminal, ready.case.operation)
            state = json.loads(terminal.result_preparation_json)
            if damage == "description":
                session.get(Upload, ready.case.collection_id).description = "changed"
            elif damage == "frontier":
                session.get(Upload, ready.case.collection_id).planner_next_sequence += 1
            elif damage == "counter":
                state["tag_count"] = "02"
            else:
                state[{"source": "source", "stage": "stage", "cursor": "after"}[damage]] = "changed"
            terminal.result_preparation_json = canonical_json_bytes(state).decode()
        before = _state(ready)
        with pytest.raises(Conflict, match="preparation changed|cursor changed"):
            _advance(ready)
        assert _state(ready) == before


def test_cancel_projection_complete_output_width_has_explicit_byte_refusal_and_retry(tmp_path):
    with _cancel(tmp_path, copies=65) as ready:
        _counts_done(ready)
        before = _state(ready)
        with pytest.raises(VolumeEmissionCapacity):
            _advance(ready, limits=replace(LIMITS, metadata_bytes=9000))
        assert _state(ready) == before
        windows, _ = _finish(ready)
        assert windows[-1].copy_rows == 65 > LIMITS.input_transitions


def test_cancel_projection_complete_output_uses_the_original_control_reservation(tmp_path):
    with _cancel(tmp_path, copies=17) as ready:
        _counts_done(ready)
        with session_scope(ready.case.factory) as session:
            terminal = session.get(Terminal, ready.case.operation)
            terminal.control_capacity_bytes = (
                terminal_control_bytes(
                    terminal, load_terminal_tag_selector(session, ready.case.operation)
                )
                + 1
            )
        before = _state(ready)
        with pytest.raises(Conflict, match="byte grant"):
            _advance(ready)
        assert _state(ready) == before


def test_cancel_projection_visible_original_frontier_excludes_staged_suffix(tmp_path):
    with _cancel(tmp_path, count=17, visible=3) as ready:
        windows, encoded = _finish(ready)
        assert sum(window.rows for window in windows) == 3 + ready.tags
        actual = decode_cancel_terminal_result(encoded)
        assert actual["archive_uploaded_bytes"] == 3 and actual["archive_total_units"] == 6


@pytest.mark.parametrize("large", (2**53 + 1, 2**63 - 1))
def test_cancel_result_internal_integer_codecs_preserve_large_public_counters(tmp_path, large):
    with _cancel(tmp_path, count=1, tags=0) as ready:
        with session_scope(ready.case.factory) as session:
            upload = session.get(Upload, ready.case.collection_id)
            upload.artifact_count = large
            upload.artifact_bytes = large
            upload.payload_sealed_artifact_count = 0
            upload.payload_sealed_artifact_bytes = 0
            upload.safe_release_artifact_count = 0
            upload.safe_release_artifact_bytes = 0
            ready.expected.update(
                files_total=large,
                bytes_total=large,
                custody={"state": "pending", "files": 0, "bytes": 0},
            )
        _, encoded = _finish(ready)
        assert decode_cancel_terminal_result(encoded)["files_total"] == large


@pytest.mark.parametrize("count", (17, 65, 129))
def test_cancel_result_actual_sql_counts_grow_with_source_windows(tmp_path, count, record_property):
    with _cancel(tmp_path, count=count, tags=1, copies=3) as ready:
        statements = []

        def capture(connection, cursor, statement, parameters, context, executemany):
            statements.append(statement)
            assert "COUNT(" not in statement.upper() and "SUM(" not in statement.upper()

        engine = ready.case.factory.kw["bind"]
        event.listen(engine, "before_cursor_execute", capture)
        try:
            windows, _ = _finish(ready)
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        assert len(statements) < 35 * count + 300
        record_property("visible_volume_rows", count)
        record_property("projection_windows", len(windows))
        record_property("projection_sql_statements", len(statements))


def _handoff(ready):
    from riverhog_core.catalog_models import CollectionArchiveCopyRecord as Copy
    from riverhog_core.catalog_models import CollectionRecord as Collection

    with session_scope(ready.case.factory) as session:
        upload = session.get(Upload, ready.case.collection_id)
        session.add(
            Collection(
                id=upload.collection_id,
                creation_idempotency_key=upload.idempotency_key,
                creation_identity_sha256=upload.creation_identity_sha256,
                creation_custody_mode=upload.custody_mode,
                creation_archive_store=upload.archive_store,
                creation_use_cache=upload.use_cache,
                creation_copy_to_json=upload.copy_to_json,
                archive_generation=upload.archive_generation,
                delivery_context_id=upload.delivery_context_id,
                encryption_format=upload.encryption_format,
                passphrase_id=upload.passphrase_id,
                created_by_principal_id=upload.initiated_by_principal_id,
                created_by_key_id=upload.initiated_by_key_id,
                artifact_set_identity="2" * 64,
                inventory_identity="4" * 64,
                provenance_identity="3" * 64,
                archive_root_sha256="5" * 64,
                created_at=upload.opened_at,
                is_published=False,
                artifact_count=upload.artifact_count,
                artifact_bytes=upload.artifact_bytes,
            )
        )
        session.flush()
        session.add(
            Copy(
                collection_id=upload.collection_id,
                store=upload.archive_store,
                incarnation_id=upload.archive_incarnation_id,
                archive_storage_prefix=upload.archive_storage_prefix,
                state="uploaded",
            )
        )
        session.flush()
        intent = session.get(Intent, (upload.collection_id, "target00000"))
        intent.state = "handed_off"
        intent.job_created = True
        session.add(
            CopyJob(
                collection_id=upload.collection_id,
                destination_store=intent.destination_store,
                destination_incarnation_id=intent.destination_incarnation_id,
                destination_storage_prefix="independent-copy-prefix",
                source_store=intent.source_store,
                source_incarnation_id=intent.source_incarnation_id,
                use_cache=False,
                initiated_by_app=intent.initiated_by_app,
                initiated_by_key_id=intent.initiated_by_key_id,
                state="waiting",
                requested_at=upload.opened_at,
            )
        )
    ready.expected["copy_intents"][0].update(job_state="waiting", job_created=True)


def _handoff_preserved(ready):
    _handoff(ready)
    with session_scope(ready.case.factory) as session:
        before = dict(session.execute(select(CopyJob.__table__)).mappings().one())
    _, encoded = _finish(ready)
    assert decode_cancel_terminal_result(encoded)["copy_intents"][0]["job_state"] == "waiting"
    with session_scope(ready.case.factory) as session:
        assert dict(session.execute(select(CopyJob.__table__)).mappings().one()) == before
        assert session.get(Intent, (ready.case.collection_id, "target00000")).state == "handed_off"


def test_cancel_result_preserves_independent_handed_off_job_and_original_failure_receiving(
    tmp_path,
):
    with _cancel(tmp_path, copies=3) as ready:
        _handoff_preserved(ready)
