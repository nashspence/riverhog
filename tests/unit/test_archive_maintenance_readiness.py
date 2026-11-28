from __future__ import annotations

from contextlib import contextmanager

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord as Volume
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services import collection_uploads as owner
from sqlalchemy import event
from time_formats import utc_timestamp_now

from tests.unit import test_finalization_terminal_admission as terminal_tests
from tests.unit.test_collection_upload_preparation import _case
from tests.unit.test_finalization_claim_readiness import _seal


def _ready_again(case):
    with session_scope(case.factory) as session:
        upload = session.get(Upload, case.collection_id)
        upload.state, upload.archive_phase = "uploading", "uploading"
        upload.archive_next_attempt_at = None


def _state(case):
    with session_scope(case.factory) as session:
        upload = session.get(Upload, case.collection_id)
        return upload.state, upload.archive_phase, upload.archive_next_attempt_at


def test_readiness_hint_follows_original_transaction_commit_and_close(tmp_path, monkeypatch):
    with _case(tmp_path, "segment", artifact_count=1) as case:
        _seal(case)
        _ready_again(case)
        original_scope = owner.session_scope
        trace = []
        captured = []

        @contextmanager
        def observe_scope(factory):
            with original_scope(factory) as session:
                captured.append(session)
                original_close = session.close

                def close():
                    original_close()
                    trace.append("closed")

                session.close = close
                yield session
            trace.append("exited")

        def wake():
            assert trace == ["closed", "exited"]
            assert len(captured) == 1 and not captured[0].in_transaction()
            state, phase, due = _state(case)
            assert (state, phase) == ("finalizing", "finalization_queued")
            assert due is not None
            trace.append("hint")

        with monkeypatch.context() as patch:
            patch.setattr(owner, "session_scope", observe_scope)
            with case.service.archive_maintenance_wakeup(wake):
                case.service._schedule_finalization_if_ready(case.collection_id)
        assert trace == ["closed", "exited", "hint"]
        assert case.service._finalization_due.select(utc_timestamp_now(), limit=1) == (
            case.collection_id,
        )


@pytest.mark.parametrize("failure", ("rollback", "lost-reply"))
def test_unconfirmed_transition_has_no_hint_and_timer_can_rediscover(
    tmp_path, monkeypatch, failure
):
    with _case(tmp_path, "segment", artifact_count=1) as case:
        _seal(case)
        _ready_again(case)
        original_scope = owner.session_scope
        hints = []

        @contextmanager
        def interrupted_scope(factory):
            with original_scope(factory) as session:
                yield session
                if failure == "rollback":
                    raise OSError("readiness transaction rolled back")
            raise OSError("committed readiness reply lost")

        with case.service.archive_maintenance_wakeup(lambda: hints.append("hint")):
            with monkeypatch.context() as patch:
                patch.setattr(owner, "session_scope", interrupted_scope)
                with pytest.raises(OSError):
                    case.service._schedule_finalization_if_ready(case.collection_id)
            assert hints == []
            if failure == "rollback":
                assert _state(case) == ("uploading", "uploading", None)
                assert case.service._finalization_due.select(utc_timestamp_now(), limit=1) == ()
                assert case.service._schedule_ready_finalizations(limit=1) == 1
            else:
                assert _state(case)[:2] == ("finalizing", "finalization_queued")
            assert case.service._finalization_due.select(utc_timestamp_now(), limit=1) == (
                case.collection_id,
            )
        assert hints == []


@pytest.mark.parametrize("change", ("terminal", "readiness", "planner"))
def test_lost_atomic_readiness_race_does_not_emit_another_hint(tmp_path, monkeypatch, change):
    original_case = terminal_tests._case
    hints = []

    @contextmanager
    def observed_case(*args, **kwargs):
        with original_case(*args, **kwargs) as case:
            with case.service.archive_maintenance_wakeup(lambda: hints.append("hint")):
                yield case

    monkeypatch.setattr(terminal_tests, "_case", observed_case)
    terminal_tests.test_point_readiness_matches_current_facts_in_its_atomic_update(
        tmp_path, monkeypatch, change
    )
    # The existing race witness first completes one real upload, then changes
    # current facts after the point readiness read and before its atomic update.
    assert hints == ["hint"]


@pytest.mark.parametrize("missing", ("completion", "payload", "count", "bytes", "volume"))
def test_incomplete_source_never_announces_finalization_readiness(tmp_path, missing):
    with _case(tmp_path, "segment", artifact_count=1) as case:
        if missing == "payload":
            case.service.complete(case.collection_id)
        elif missing != "completion":
            _seal(case)
            _ready_again(case)
            with session_scope(case.factory) as session:
                upload = session.get(Upload, case.collection_id)
                if missing == "count":
                    upload.artifact_count += 1
                elif missing == "bytes":
                    upload.payload_sealed_artifact_bytes = 0
                else:
                    session.get(Volume, (case.collection_id, case.object_id)).state = "planned"
        hints = []
        with case.service.archive_maintenance_wakeup(lambda: hints.append("hint")):
            case.service._schedule_finalization_if_ready(case.collection_id)
        assert hints == []
        assert case.service._finalization_due.select(utc_timestamp_now(), limit=1) == ()


def test_selected_terminal_never_announces_fresh_finalization(tmp_path):
    with terminal_tests._terminal_case(tmp_path, phase="ready") as case:
        hints = []
        before = _state(case)
        with case.service.archive_maintenance_wakeup(lambda: hints.append("hint")):
            case.service._schedule_finalization_if_ready(case.collection_id)
        assert hints == [] and _state(case) == before


def test_hint_adds_no_catalog_work_and_duplicate_does_not_notify(tmp_path, record_property):
    with _case(tmp_path, "segment", artifact_count=1) as case:
        _seal(case)
        engine = case.factory.kw["bind"]
        statements = []
        hints = []

        def statement(_connection, _cursor, text, *_args):
            statements.append(text)

        observations = []
        for bound in (False, True):
            _ready_again(case)
            statements.clear()
            event.listen(engine, "before_cursor_execute", statement)
            try:
                if bound:
                    with case.service.archive_maintenance_wakeup(
                        lambda: hints.append(len(statements))
                    ):
                        case.service._schedule_finalization_if_ready(case.collection_id)
                else:
                    case.service._schedule_finalization_if_ready(case.collection_id)
            finally:
                event.remove(engine, "before_cursor_execute", statement)
            observations.append(tuple(statements))
        assert observations[0] == observations[1]
        assert len(observations[0]) == 3
        assert hints == [3]
        with case.service.archive_maintenance_wakeup(lambda: hints.append(-1)):
            case.service._schedule_finalization_if_ready(case.collection_id)
        assert hints == [3]
        record_property("readiness_sql_statements_without_and_with_hint", [3, 3])


def test_callback_failure_preserves_committed_success_and_unbinding(tmp_path, caplog):
    with _case(tmp_path, "segment", artifact_count=1) as case:
        _seal(case)
        _ready_again(case)
        failure = OSError("local wake receiver unavailable")
        attempts = []

        def broken():
            attempts.append("original")
            raise failure

        with case.service.archive_maintenance_wakeup(broken):
            case.service._schedule_finalization_if_ready(case.collection_id)
        assert attempts == ["original"]
        assert _state(case)[:2] == ("finalizing", "finalization_queued")
        assert any(record.exc_info and record.exc_info[1] is failure for record in caplog.records)
        _ready_again(case)
        case.service._schedule_finalization_if_ready(case.collection_id)
        assert attempts == ["original"]
        _ready_again(case)
        with case.service.archive_maintenance_wakeup(lambda: attempts.append("replacement")):
            case.service._schedule_finalization_if_ready(case.collection_id)
        assert attempts == ["original", "replacement"]


def test_binding_rejects_another_live_receiver_and_detaches_after_failure():
    service = object.__new__(owner.SqlAlchemyCollectionUploadService)

    def first():
        pass

    def second():
        pass

    with pytest.raises(OSError, match="owner scope failed"):
        with service.archive_maintenance_wakeup(first):
            with pytest.raises(RuntimeError):
                with service.archive_maintenance_wakeup(second):
                    pytest.fail("another live receiver replaced the original")
            raise OSError("owner scope failed")
    with service.archive_maintenance_wakeup(second):
        pass
