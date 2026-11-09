from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.catalog_archive_write_models import ArchiveUploadTerminalRecord as Terminal
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord as Volume
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.incremental_emission import VolumeEmissionCapacity
from riverhog_core.services import archive_upload_discard_result_projection as module
from riverhog_core.services.archive_upload_discard_result_projection import (
    ArchiveUploadDiscardResultProjection,
    decode_discard_terminal_result,
)
from riverhog_core.services.archive_upload_terminal_control import (
    require_prepared_terminal_result,
    terminal_control_bytes,
)
from riverhog_core.services.archive_upload_terminal_result import ArchiveUploadTerminalResults
from riverhog_core.services.archive_upload_terminal_tags import load_terminal_tag_selector
from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy import delete, event

from tests.unit.test_archive_upload_projection_cleanup import LIMITS
from tests.unit.test_archive_write_admission import _case
from tests.unit.test_upload_discard_authority import _operator, _select


@contextmanager
def _discard(tmp_path, *, database_url=None, count=17, visible=None):
    with _case(tmp_path, database_url=database_url) as case:
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
                        }
                    )
                )
            upload = session.get(Upload, case.collection_id)
            upload.state = "orphaned"
            upload.planner_next_sequence = count if visible is None else visible
        case.keys, case.principal = _operator(case)
        plan = case.service.plan_orphan_discard(case.collection_id)
        expected = {
            "status": "discarded",
            **{
                key: plan[key]
                for key in ("collection_id", "files", "bytes", "custody", "archive_objects")
            },
        }
        case.operation = _select(
            case,
            case.principal,
            case.service._discard_write_guard(case.collection_id, case.principal),
        )
        guard = case.service._terminal_write_guard(case.collection_id, case.operation)
        yield SimpleNamespace(
            case=case, guard=guard, expected=expected, count=count if visible is None else visible
        )


def _advance(ready, *, limits=LIMITS):
    return ArchiveUploadDiscardResultProjection(
        ready.case.factory, ready.case.service._archive_terminal_observation
    ).advance(ready.case.collection_id, ready.case.operation, guard=ready.guard, limits=limits)


def _state(ready):
    with session_scope(ready.case.factory) as session:
        terminal = session.get(Terminal, ready.case.operation)
        return (
            terminal.phase,
            terminal.result_preparation_json,
            terminal.original_result_json,
            terminal.result_json,
        )


def _finish(ready):
    total = 0
    for _ in range(ready.count // LIMITS.input_transitions + 3):
        progress = _advance(ready)
        assert progress.rows <= LIMITS.input_transitions
        assert progress.metadata_bytes <= LIMITS.metadata_bytes
        total += progress.rows
        assert progress.phase == "draining" and progress.pending_reason is None
        if progress.prepared:
            break
    else:
        pytest.fail("original discard result did not finish")
    phase, preparation, encoded, result = _state(ready)
    assert phase == "draining" and preparation is None and result is None
    assert decode_discard_terminal_result(encoded) == ready.expected
    assert _advance(ready).prepared and _state(ready)[2] == encoded
    return total, encoded


@pytest.mark.parametrize("count", (0, 1, 2, 17, 65))
def test_discard_original_result_cold_resumes_exact_public_counts_before_cleanup(tmp_path, count):
    with _discard(tmp_path, count=count) as ready:
        total, encoded = _finish(ready)
        assert total == count
        from riverhog_api.schemas.collections import CollectionUploadDiscardResultOut

        projected = CollectionUploadDiscardResultOut.model_validate(
            decode_discard_terminal_result(encoded)
        )
        assert projected.files == ready.expected["files"]
        with session_scope(ready.case.factory) as session:
            assert session.get(Upload, ready.case.collection_id) is not None
        assert (
            ready.case.service._archive_terminal_adjudication.adjudicate(
                ready.case.collection_id, ready.case.operation, guard=ready.guard
            ).phase
            == "cleanup-ready"
        )


@pytest.mark.parametrize("committed", (False, True))
def test_discard_result_preparation_retries_original_rollback_or_lost_commit_reply(
    tmp_path, monkeypatch, committed
):
    with _discard(tmp_path) as ready:
        actual = module.session_scope

        @contextmanager
        def lose(factory):
            with actual(factory) as session:
                yield session
                if not committed:
                    raise OSError("original result rollback")
            raise OSError("original result commit reply lost")

        before = _state(ready)
        with monkeypatch.context() as patch:
            patch.setattr(module, "session_scope", lose)
            with pytest.raises(OSError, match="original result"):
                _advance(ready)
        assert (_state(ready) != before) is committed
        total, _ = _finish(ready)
        assert total == ready.count - (LIMITS.input_transitions if committed else 0)


def _revoked(ready):
    from riverhog_core.app_permissions import (
        COLLECTIONS_CREATE,
        COLLECTIONS_DELETE,
        ApplicationAccess,
    )

    from tests.unit.test_app_keys import BOOTSTRAP

    assert not _advance(ready).prepared
    before = _state(ready)
    case = ready.case
    case.keys.replace_access(
        app=case.principal.id,
        key_id=case.principal.key_id,
        access=(ApplicationAccess(COLLECTIONS_CREATE),),
        grantor=BOOTSTRAP,
    )
    with pytest.raises(NotFound):
        _advance(ready)
    assert _state(ready) == before
    case.keys.replace_access(
        app=case.principal.id,
        key_id=case.principal.key_id,
        access=(ApplicationAccess(COLLECTIONS_DELETE),),
        grantor=BOOTSTRAP,
    )
    total, _ = _finish(ready)
    assert total == ready.count - LIMITS.input_transitions


def test_discard_result_preparation_reloads_current_terminal_rights_on_cold_resume(tmp_path):
    with _discard(tmp_path) as ready:
        _revoked(ready)


def test_partial_projection_is_not_a_prepared_or_confirmed_result(tmp_path):
    with _discard(tmp_path) as ready:
        assert not _advance(ready).prepared
        before = _state(ready)
        with session_scope(ready.case.factory) as session:
            terminal = session.get(Terminal, ready.case.operation)
            with pytest.raises(Conflict, match="original result reservation"):
                require_prepared_terminal_result(
                    terminal, load_terminal_tag_selector(session, ready.case.operation)
                )
        with pytest.raises(Conflict, match="projection is in progress"):
            ArchiveUploadTerminalResults(ready.case.factory).prepare(
                ready.case.collection_id, ready.case.operation, ready.expected, guard=ready.guard
            )
        decision = ready.case.service._archive_terminal_adjudication.adjudicate(
            ready.case.collection_id, ready.case.operation, guard=ready.guard
        )
        assert decision.pending_reason == "original-result" and _state(ready) == before
        _finish(ready)


def test_projection_quantum_and_original_control_reservation_include_complete_progress(tmp_path):
    with _discard(tmp_path) as ready:
        before = _state(ready)
        with pytest.raises(VolumeEmissionCapacity):
            _advance(ready, limits=replace(LIMITS, metadata_bytes=1))
        assert _state(ready) == before
        with session_scope(ready.case.factory) as session:
            terminal = session.get(Terminal, ready.case.operation)
            terminal.control_capacity_bytes = (
                terminal_control_bytes(
                    terminal, load_terminal_tag_selector(session, ready.case.operation)
                )
                + 1
            )
        with pytest.raises(Conflict, match="byte grant"):
            _advance(ready)
        assert _state(ready) == before


@pytest.mark.parametrize(
    "damage",
    ("format", "operation_id", "custody_revision", "after", "counter", "source", "frontier"),
)
def test_changed_original_projection_or_source_refuses_without_advancing(tmp_path, damage):
    with _discard(tmp_path) as ready:
        assert not _advance(ready).prepared
        with session_scope(ready.case.factory) as session:
            terminal = session.get(Terminal, ready.case.operation)
            value = json.loads(terminal.result_preparation_json)
            if damage == "source":
                session.get(Upload, ready.case.collection_id).artifact_count += 1
            elif damage == "frontier":
                session.get(Upload, ready.case.collection_id).planner_next_sequence += 1
            elif damage == "counter":
                value["payload"]["archive_objects"] = "02"
            else:
                value[damage] = {
                    "format": "different",
                    "operation_id": "0" * 64,
                    "custody_revision": "77",
                    "after": "../outside",
                }[damage]
            terminal.result_preparation_json = canonical_json_bytes(value).decode()
        before = _state(ready)
        with pytest.raises(Conflict, match="preparation changed"):
            _advance(ready)
        assert _state(ready) == before


@pytest.mark.parametrize("number", (2**53 + 1, 2**63 - 1))
def test_original_public_result_preserves_exact_large_sql_counters(tmp_path, number):
    with _discard(tmp_path, count=1) as ready:
        with session_scope(ready.case.factory) as session:
            upload = session.get(Upload, ready.case.collection_id)
            upload.artifact_count = number
            upload.artifact_bytes = number
            upload.payload_sealed_artifact_count = number - 1
            upload.payload_sealed_artifact_bytes = number - 1
            upload.safe_release_artifact_count = number - 1
            upload.safe_release_artifact_bytes = number - 1
        ready.expected.update(
            files=number,
            bytes=number,
            custody={"state": "pending", "files": number - 1, "bytes": number - 1},
        )
        _, encoded = _finish(ready)
        assert json.loads(encoded)["payload"]["files"] == str(number)


@pytest.mark.parametrize("count", (17, 65, 129))
def test_original_discard_projection_has_linear_keyset_work_without_aggregate_rescans(
    tmp_path, count, record_property
):
    with _discard(tmp_path, count=count) as ready:
        statements = []

        def observe(connection, cursor, statement, parameters, context, many):
            statements.append(statement)

        event.listen(ready.case.factory.kw["bind"], "before_cursor_execute", observe)
        try:
            total, _ = _finish(ready)
        finally:
            event.remove(ready.case.factory.kw["bind"], "before_cursor_execute", observe)
        scans = [
            sql
            for sql in statements
            if "FROM collection_archive_object_uploads" in sql and "object_id >" in sql
        ]
        assert total == count and len(scans) <= count // LIMITS.input_transitions + 2
        assert not any("count(" in sql.lower() or "sum(" in sql.lower() for sql in statements)
        record_property("projection_rows", total)
        record_property("sql_statements", len(statements))


def _native_ready(case):
    with session_scope(case.factory) as session:
        session.get(Upload, case.collection_id).state = "orphaned"
    case.keys, case.principal = _operator(case)
    plan = case.service.plan_orphan_discard(case.collection_id)
    expected = {
        "status": "discarded",
        **{
            key: plan[key]
            for key in ("collection_id", "files", "bytes", "custody", "archive_objects")
        },
    }
    case.operation = _select(
        case, case.principal, case.service._discard_write_guard(case.collection_id, case.principal)
    )
    return SimpleNamespace(
        case=case,
        guard=case.service._terminal_write_guard(case.collection_id, case.operation),
        expected=expected,
        count=1,
    )


def test_actual_original_effect_and_receiving_prevent_discard_result_snapshot(tmp_path):
    from tests.unit.test_archive_write_admission import _apply, _arm, _closure, _prepare

    with _case(tmp_path) as case:
        _prepare(case)
        ticket = _arm(case)
        ready = _native_ready(case)
        before = _state(ready)
        assert _advance(ready).pending_reason == "forward-effects" and _state(ready) == before
        case.owner.settle_closed(ticket, _closure(case, ticket), apply_result=_apply)
        assert _advance(ready).pending_reason == "original-receiving" and _state(ready) == before
        case.owner.acknowledge_report(ticket)
        first = _advance(ready, limits=replace(LIMITS, input_transitions=1))
        assert not first.prepared and first.rows == 1
        # The exact current object's opaque ID is not required to be a digest.
        _finish(ready)


def test_actual_retained_input_prevents_original_result_preparation(tmp_path):
    from tests.unit.test_archive_finalizer_registry import _scope
    from tests.unit.test_archive_payload_input import _request

    with _case(tmp_path) as case:
        key, target = _scope(case)
        body = case.service._archive_inputs.reserve(
            _request(
                key=key,
                target_incarnation=target,
                epoch=case.epoch,
                volume_id=case.object_id,
                plan_sha256=case.identity["plan_sha256"],
            )
        )
        try:
            ready = _native_ready(case)
            before = _state(ready)
            assert _advance(ready).pending_reason == "local-input" and _state(ready) == before
            body.discard_unretained()
            _finish(ready)
        finally:
            body.discard_unretained()


def test_changed_selected_custody_leaves_partial_snapshot_for_fresh_adjudication(tmp_path):
    from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord as Control

    with _discard(tmp_path) as ready:
        assert not _advance(ready).prepared
        before = _state(ready)
        with session_scope(ready.case.factory) as session:
            session.get(Control, ready.case.collection_id).custody_revision = "7"
        progress = _advance(ready)
        assert progress.pending_reason == "original-result" and progress.rows == 0
        assert _state(ready) == before
        decision = ready.case.service._archive_terminal_adjudication.adjudicate(
            ready.case.collection_id, ready.case.operation, guard=ready.guard
        )
        assert decision.phase == "needs-reconfirmation"
        assert _state(ready)[1:] == before[1:]


def test_original_public_count_preserves_the_accepted_registration_frontier(tmp_path):
    with _discard(tmp_path, count=17, visible=1) as ready:
        total, encoded = _finish(ready)
        assert total == 1 and decode_discard_terminal_result(encoded)["archive_objects"] == 1


def test_prepared_original_readback_still_charges_complete_projection_metadata(tmp_path):
    with _discard(tmp_path, count=1) as ready:
        _finish(ready)
        before = _state(ready)
        with pytest.raises(VolumeEmissionCapacity):
            _advance(ready, limits=replace(LIMITS, metadata_bytes=1))
        assert _state(ready) == before
