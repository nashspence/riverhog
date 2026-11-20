from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace

import pytest
from riverhog_core.app_permissions import COLLECTIONS_CREATE, COLLECTIONS_DELETE, ApplicationAccess
from riverhog_core.catalog_archive_write_models import ArchiveUploadTerminalRecord as Terminal
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadCopyIntentRecord as Intent
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.incremental_emission import VolumeEmissionCapacity
from riverhog_core.services import archive_upload_copy_intent_cleanup as module
from riverhog_core.services.archive_upload_copy_intent_cleanup import ArchiveUploadCopyIntentCleanup
from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy import event, select

from tests.unit.test_app_keys import BOOTSTRAP
from tests.unit.test_archive_upload_registration_cleanup import _complete_registration, _prepared
from tests.unit.test_collection_registration_cleanup import CLEANUP

LIMITS = replace(CLEANUP, input_transitions=2)
DESTINATIONS = tuple(f"target{n:05d}" for n in range(17))


def _rows(ready, *, scoped=True):
    with session_scope(ready.case.factory) as session:
        statement = select(Intent.__table__).order_by(
            Intent.collection_id, Intent.destination_store
        )
        if scoped:
            statement = statement.where(Intent.collection_id == ready.case.collection_id)
        return [dict(row) for row in session.execute(statement).mappings()]


def _advance(ready, *, limits=LIMITS):
    return ArchiveUploadCopyIntentCleanup(ready.case.factory, ready.ready.owner).advance(
        ready.case.collection_id, ready.case.operation, guard=ready.guard, limits=limits
    )


def _finish(ready, *, limits=LIMITS):
    original = _rows(ready)
    previous = original
    for _ in range(len(original) + 2):
        progress = _advance(ready, limits=limits)
        current = _rows(ready)
        changed = sum(a != b for a, b in zip(previous, current, strict=True))
        assert changed == progress.canceled <= limits.input_transitions
        assert progress.metadata_bytes <= limits.metadata_bytes
        if progress.exhausted:
            break
        previous = current
    else:
        pytest.fail("bounded original copy-intent cancellation did not finish")
    assert current == [{**row, "state": "canceled", "next_attempt_at": None} for row in original]
    assert _advance(ready).exhausted
    with session_scope(ready.case.factory) as session:
        terminal = session.get(Terminal, ready.case.operation)
        assert terminal.original_result_json == ready.original and terminal.result_json is None
        assert terminal.phase == "cleaning"
        assert session.get(Upload, ready.case.collection_id) is not None


@pytest.mark.parametrize("count", (0, 1, 17, 65))
def test_original_accepted_copy_intents_cold_resume_bounded_windows_and_keep_receipts(
    tmp_path, monkeypatch, count
):
    destinations = tuple(f"target{n:05d}" for n in range(count))
    with _prepared(tmp_path, monkeypatch, copy_to=destinations) as ready:
        _complete_registration(ready)
        assert len(_rows(ready)) == count
        _finish(ready)


@pytest.mark.parametrize("provider_ready", (False, True))
def test_copy_intents_survive_until_positive_archive_and_hidden_registration_proof(
    tmp_path, monkeypatch, provider_ready
):
    with _prepared(
        tmp_path, monkeypatch, copy_to=DESTINATIONS, provider_ready=provider_ready
    ) as ready:
        before = _rows(ready)
        with pytest.raises(Conflict, match="inventory proof|hidden registration"):
            _advance(ready)
        assert _rows(ready) == before


def test_complete_copy_intent_bytes_refuse_an_insufficient_metadata_grant(tmp_path, monkeypatch):
    destination = "target" + "x" * 10001
    with _prepared(tmp_path, monkeypatch, copy_to=(destination,)) as ready:
        _complete_registration(ready)
        before = _rows(ready)
        with pytest.raises(VolumeEmissionCapacity) as refusal:
            _advance(ready)
        assert refusal.value.required > 10001 and _rows(ready) == before
        _finish(ready, limits=replace(LIMITS, metadata_bytes=20000))


def _revoked(ready):
    assert _advance(ready).canceled == 2
    before = _rows(ready)
    case = ready.case
    case.keys.replace_access(
        app=case.principal.id,
        key_id=case.principal.key_id,
        access=(ApplicationAccess(COLLECTIONS_DELETE),),
        grantor=BOOTSTRAP,
    )
    with pytest.raises(NotFound):
        _advance(ready)
    assert _rows(ready) == before
    case.keys.replace_access(
        app=case.principal.id,
        key_id=case.principal.key_id,
        access=(ApplicationAccess(COLLECTIONS_CREATE),),
        grantor=BOOTSTRAP,
    )
    _finish(ready)


def test_copy_intent_disposal_requires_current_terminal_rights_for_each_window(
    tmp_path, monkeypatch
):
    with _prepared(tmp_path, monkeypatch, copy_to=DESTINATIONS) as ready:
        _complete_registration(ready)
        _revoked(ready)


def _lost(ready, monkeypatch, committed):
    actual = module.session_scope

    @contextmanager
    def lose(factory):
        with actual(factory) as session:
            yield session
            if not committed:
                raise OSError("copy-intent cancellation rollback")
        raise OSError("copy-intent cancellation commit reply lost")

    before = _rows(ready)
    with monkeypatch.context() as patch:
        patch.setattr(module, "session_scope", lose)
        with pytest.raises(OSError, match="copy-intent cancellation"):
            _advance(ready)
    assert (_rows(ready) == before) is not committed
    _finish(ready)


@pytest.mark.parametrize("committed", (False, True))
def test_original_copy_intent_windows_retry_sql_interruption_without_losing_receipts(
    tmp_path, monkeypatch, committed
):
    with _prepared(tmp_path, monkeypatch, copy_to=DESTINATIONS) as ready:
        _complete_registration(ready)
        _lost(ready, monkeypatch, committed)


def _independent(ready, state):
    with session_scope(ready.case.factory) as session:
        session.get(Intent, (ready.case.collection_id, DESTINATIONS[-1])).state = state
    before = _rows(ready)
    with pytest.raises(Conflict, match="independently runnable handoff"):
        _advance(ready)
    assert _rows(ready) == before


@pytest.mark.parametrize("state", ("pending", "handed_off", "failed"))
def test_original_terminal_cannot_dispose_an_independent_copy_handoff(tmp_path, monkeypatch, state):
    with _prepared(tmp_path, monkeypatch, copy_to=DESTINATIONS) as ready:
        _complete_registration(ready)
        _independent(ready, state)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("initiated_by_app", "different-writer"),
        ("initiated_by_key_id", "different-key"),
        ("event_context_json", "{}"),
        ("use_cache", True),
        ("attempts", 1),
        ("next_attempt_at", "2026-01-01T00:00:00.000000000Z"),
        ("handed_off_at", "2026-01-01T00:00:00.000000000Z"),
        ("job_created", True),
        ("failure_code", "source_unavailable"),
        ("source", None),
    ),
)
def test_changed_original_intent_identity_or_started_receiving_preserves_the_entire_window(
    tmp_path, monkeypatch, field, value
):
    with _prepared(tmp_path, monkeypatch, copy_to=DESTINATIONS) as ready:
        _complete_registration(ready)
        with session_scope(ready.case.factory) as session:
            intent = session.get(Intent, (ready.case.collection_id, DESTINATIONS[1]))
            if field == "source":
                destination = session.get(Intent, (ready.case.collection_id, DESTINATIONS[0]))
                intent.source_store = destination.destination_store
                intent.source_incarnation_id = destination.destination_incarnation_id
            else:
                setattr(intent, field, value)
        before = _rows(ready)
        with pytest.raises(Conflict, match="differs from the original"):
            _advance(ready)
        assert _rows(ready) == before


def test_copy_retirement_preserves_unrelated_intents_and_ignores_current_destination_aliases(
    tmp_path, monkeypatch
):
    with _prepared(tmp_path, monkeypatch, copy_to=DESTINATIONS) as ready:
        _complete_registration(ready)
        originals = _rows(ready)
        with session_scope(ready.case.factory) as session:
            for row in originals:
                session.add(Intent(**{**row, "collection_id": ready.case.collection_id + 1000000}))

        # SQL-only cancellation must use the recorded source/destination preimage;
        # it needs neither current destination availability nor provider calls.
        def unexpected(*args, **kwargs):
            raise AssertionError("copy-intent metadata retirement contacted a destination")

        monkeypatch.setattr(ready.case.service._archive_stores, "require", unexpected)
        _finish(ready)
        unrelated = [
            row
            for row in _rows(ready, scoped=False)
            if row["collection_id"] != ready.case.collection_id
        ]
        assert unrelated == [
            {**row, "collection_id": ready.case.collection_id + 1000000} for row in originals
        ]


@pytest.mark.parametrize("count", (17, 65, 129))
def test_actual_copy_intent_retirement_has_linear_sql_work_and_bounded_row_windows(
    tmp_path, monkeypatch, count, record_property
):
    destinations = tuple(f"target{n:05d}" for n in range(count))
    with _prepared(tmp_path, monkeypatch, copy_to=destinations) as ready:
        _complete_registration(ready)
        statements = []
        engine = ready.case.factory.kw["bind"]

        def observed(connection, cursor, statement, parameters, context, executemany):
            statements.append(statement)

        event.listen(engine, "before_cursor_execute", observed)
        try:
            canceled = turns = 0
            for _ in range(count + 2):
                progress = _advance(ready)
                turns += 1
                canceled += progress.canceled
                assert progress.canceled <= LIMITS.input_transitions
                if progress.exhausted:
                    break
            else:
                pytest.fail("copy-intent retirement did not exhaust its first-live windows")
        finally:
            event.remove(engine, "before_cursor_execute", observed)
        assert canceled == count and turns == (count + 1) // 2 + 1
        assert len(statements) <= 30 * turns
        record_property("copy_intents_canceled", canceled)
        record_property("copy_intent_turns", turns)
        record_property("copy_intent_sql_statements", len(statements))


def _composed_intents(ready, monkeypatch, *, revoked=False):
    import threading

    from tests.unit.test_archive_upload_checkpoint_cleanup import _composed_checkpoints

    original = ArchiveUploadCopyIntentCleanup.advance
    workers = []

    def advance(owner, *args, **kwargs):
        assert kwargs["guard"] is ready.guard and kwargs["limits"] is CLEANUP
        workers.append(threading.current_thread().name)
        return original(owner, *args, **kwargs)

    monkeypatch.setattr(ArchiveUploadCopyIntentCleanup, "advance", advance)
    quantum, created = _composed_checkpoints(ready, monkeypatch, copy_intents=True)
    before = _rows(ready)
    assert sum(row["state"] == "canceled" for row in before) == CLEANUP.input_transitions
    assert workers == ["physical-execution"]
    calls = len(ready.ready.target.calls)
    case = ready.case
    if revoked:
        case.keys.replace_access(
            app=case.principal.id,
            key_id=case.principal.key_id,
            access=(ApplicationAccess(COLLECTIONS_DELETE),),
            grantor=BOOTSTRAP,
        )
        for _ in range(2):
            quantum()
        assert _rows(ready) == before and len(workers) == 1
        case.keys.replace_access(
            app=case.principal.id,
            key_id=case.principal.key_id,
            access=(ApplicationAccess(COLLECTIONS_CREATE),),
            grantor=BOOTSTRAP,
        )
    for _ in range(len(before) + 2):
        progress = quantum()
        current = _rows(ready)
        assert (
            sum(a != b for a, b in zip(before, current, strict=True)) <= CLEANUP.input_transitions
        )
        assert progress.upload_children_exhausted and progress.checkpoints_exhausted
        if progress.copy_intents_exhausted:
            break
        assert progress.pending_reason == "copy-intent-cleanup"
        before = current
    else:
        pytest.fail("original worker did not finish bounded copy-intent cancellation")
    assert all(row["state"] == "canceled" for row in current)
    assert progress.pending_reason is None and not progress.execution_stopped
    assert set(workers) == {"physical-execution"} and created == [True]
    assert len(ready.ready.target.calls) == calls
    assert case.service._archive_retention._counts == {False: 0, True: 1}
    with session_scope(case.factory) as session:
        terminal = session.get(Terminal, case.operation)
        assert terminal.original_result_json == ready.original and terminal.result_json is None
        assert session.get(Upload, case.collection_id) is not None


@pytest.mark.parametrize("revoked", (False, True))
def test_original_terminal_worker_composes_copy_cancellation_after_checkpoint_retirement(
    tmp_path, monkeypatch, revoked
):
    with _prepared(tmp_path, monkeypatch, copy_to=DESTINATIONS, provider_ready=False) as ready:
        _composed_intents(ready, monkeypatch, revoked=revoked)
