from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from sqlalchemy import event, select, text

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_collection_upload_preparation import _case
from tests.unit.test_finalization_claim_readiness import (
    _assert_claims_reuse,
    _fresh_coordinator,
    _seal,
)
from tests.unit.test_finalization_terminal_admission import (
    _closed_catalog_stage,
    _closed_claim,
    _closed_readiness,
)

pytestmark = pytest.mark.integration


def test_postgres_claims_reuse_accepted_readiness(database_url, tmp_path, record_property):
    _assert_claims_reuse(tmp_path, record_property, 8, database_url=database_url)


@pytest.mark.parametrize("callback", ["retry", "requeue"])
@pytest.mark.parametrize("change", ("orphan", "new-claim"))
def test_postgres_stale_callback_rechecks_state_after_parent_lock(
    database_url, tmp_path, callback, change
):
    with _case(tmp_path, "segment", artifact_count=1, database_url=database_url) as case:
        _seal(case)
        claim = case.service._claim_due_finalization()
        assert claim is not None
        engine = case.factory.kw["bind"]
        waiting = threading.Event()
        lane = threading.local()
        pids = {}

        def capture(connection, cursor, statement, parameters, context, executemany):
            if (
                getattr(lane, "contender", False)
                and "FROM collection_uploads" in statement
                and "FOR UPDATE" in statement
            ):
                cursor.execute("SELECT pg_backend_pid()")
                pids["contender"] = cursor.fetchone()[0]
                waiting.set()

        def compete():
            lane.contender = True
            current = _fresh_coordinator(case)
            if callback == "retry":
                current._record_finalization_retry(claim, OSError("late report"))
            else:
                current._requeue_finalization_step(claim)

        def blocked():
            deadline = time.monotonic() + 10
            with engine.connect() as observer:
                while time.monotonic() < deadline:
                    blockers = observer.scalar(
                        text("SELECT pg_blocking_pids(:pid)"), {"pid": pids["contender"]}
                    )
                    if pids["holder"] in blockers:
                        return True
                    time.sleep(0.01)
            return False

        event.listen(engine, "before_cursor_execute", capture)
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                with session_scope(case.factory) as session:
                    upload = session.scalar(
                        select(Upload)
                        .where(Upload.collection_id == case.collection_id)
                        .with_for_update()
                    )
                    pids["holder"] = session.scalar(text("SELECT pg_backend_pid()"))
                    if change == "orphan":
                        upload.state = upload.archive_phase = "orphaned"
                    else:
                        upload.archive_claim_id = "f" * 64
                        assert upload.archive_claim_id != claim.claim_id
                    upload.archive_next_attempt_at = None
                    upload.archive_failure = "current owner"
                    session.flush()
                    future = pool.submit(compete)
                    assert waiting.wait(timeout=10)
                    assert blocked() and not future.done()
                future.result(timeout=10)
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            assert (
                upload.state
                == upload.archive_phase
                == ("orphaned" if change == "orphan" else "finalizing")
            )
            if change == "new-claim":
                assert upload.archive_claim_id == "f" * 64
            assert upload.archive_next_attempt_at is None
            assert upload.archive_failure == "current owner"


@pytest.mark.parametrize("phase", ("queued", "retry"))
def test_postgres_selected_terminal_excludes_fresh_finalization_claims(
    database_url, tmp_path, phase
):
    _closed_claim(tmp_path, phase, database_url=database_url)


@pytest.mark.parametrize("entry", ("batch", "point", "startup"))
def test_postgres_selected_terminal_preserves_closed_workflow_state(database_url, tmp_path, entry):
    _closed_readiness(tmp_path, entry, database_url=database_url)


@pytest.mark.parametrize(
    "stage",
    (
        "_stage_next_member_histories",
        "_advance_provenance_closure_validation",
        "_advance_archive_tree_checkpoint",
        "_advance_catalog_projection",
    ),
)
def test_postgres_selected_terminal_fences_forward_catalog_steps(database_url, tmp_path, stage):
    _closed_catalog_stage(tmp_path, stage, database_url=database_url)


def test_postgres_point_readiness_rechecks_gate_after_terminal_lock_wait(
    database_url, tmp_path, monkeypatch
):
    _point_readiness_after_terminal_lock(tmp_path, monkeypatch, database_url=database_url)


def _point_readiness_after_terminal_lock(tmp_path, monkeypatch, *, database_url):
    from contextlib import contextmanager

    from riverhog_core.services import archive_write_admission as admission
    from riverhog_core.services import collection_uploads as owner

    with _case(tmp_path, "segment", artifact_count=1, database_url=database_url) as case:
        _seal(case)
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            upload.state, upload.archive_phase = "uploading", "uploading"
            upload.archive_next_attempt_at = None
        from tests.unit.test_finalization_claim_readiness import _claim_snapshot

        before = _claim_snapshot(case)
        held, release, point_waiting = (threading.Event() for _ in range(3))
        lane, pids, operations, errors, calls = threading.local(), {}, [], [], []
        engine = case.factory.kw["bind"]
        actual_scope, actual_readiness = admission.session_scope, owner._ready_for_finalization

        @contextmanager
        def held_selection(factory):
            with actual_scope(factory) as session:
                pids["holder"] = session.scalar(text("SELECT pg_backend_pid()"))
                yield session
                session.flush()
                held.set()
                assert release.wait(10)

        def observe_readiness(session, upload):
            calls.append(upload.collection_id)
            return actual_readiness(session, upload)

        def capture(connection, cursor, statement, parameters, context, executemany):
            if getattr(lane, "point", False) and (
                "FROM collection_uploads" in statement and "FOR UPDATE" in statement
            ):
                cursor.execute("SELECT pg_backend_pid()")
                pids["point"] = cursor.fetchone()[0]
                point_waiting.set()

        def terminal():
            try:
                operations.append(
                    admission.ArchiveWriteAdmission(case.factory).select_terminal(
                        case.collection_id,
                        kind="cancel",
                        request={"fixture": "point-readiness-wait"},
                        principal_id="uploader",
                        key_id=None,
                        grants=case.service._config.archive_write,
                        guard=lambda session: None,
                        validate_selection=lambda session, upload: None,
                    )
                )
            except BaseException as error:
                errors.append(error)

        def point():
            lane.point = True
            try:
                case.service._schedule_finalization_if_ready(case.collection_id)
            except BaseException as error:
                errors.append(error)

        monkeypatch.setattr(admission, "session_scope", held_selection)
        monkeypatch.setattr(owner, "_ready_for_finalization", observe_readiness)
        event.listen(engine, "before_cursor_execute", capture)
        selected, pending = threading.Thread(target=terminal), threading.Thread(target=point)
        try:
            selected.start()
            assert held.wait(10)
            pending.start()
            assert point_waiting.wait(10)
            deadline, blocked = time.monotonic() + 10, False
            with engine.connect() as observer:
                while time.monotonic() < deadline:
                    blockers = observer.scalar(
                        text("SELECT pg_blocking_pids(:pid)"), {"pid": pids["point"]}
                    )
                    if pids["holder"] in blockers:
                        blocked = True
                        break
                    time.sleep(0.01)
            assert blocked
        finally:
            release.set()
            selected.join(10)
            if pending.ident is not None:
                pending.join(10)
            event.remove(engine, "before_cursor_execute", capture)
        assert not selected.is_alive() and not pending.is_alive() and not errors
        assert len(operations) == 1
        # This original lookup saw the pre-commit gate while its parent lock
        # waited. The fresh UPDATE snapshot must still exclude the terminal.
        assert calls == [case.collection_id]
        assert _claim_snapshot(case) == before


@pytest.mark.parametrize("kind", ("member-histories", "provenance-closure", "artifact-tree"))
@pytest.mark.parametrize("committed", (False, True))
def test_postgres_original_catalog_batch_survives_commit_reply_loss(
    database_url, tmp_path, monkeypatch, kind, committed
):
    from riverhog_core.runtime_config import RuntimeConfig

    from tests.unit.test_archive_catalog_finalizer import _assert_original_catalog_batch

    original_config = RuntimeConfig.for_testing

    def config_for_case(**values):
        return original_config(
            journal_validation_workspace=tmp_path / "journal-validation", **values
        )

    monkeypatch.setattr(RuntimeConfig, "for_testing", staticmethod(config_for_case))
    _assert_original_catalog_batch(
        tmp_path, monkeypatch, kind, committed, database_url=database_url
    )


@pytest.mark.parametrize("phase", ("artifact-set-identity", "artifacts", "terminal"))
@pytest.mark.parametrize("committed", (False, True))
def test_postgres_exact_catalog_publication_survives_commit_reply_loss(
    database_url, tmp_path, monkeypatch, phase, committed
):
    from tests.unit.test_archive_catalog_publication_receiving import _assert_publication_reply

    _assert_publication_reply(tmp_path, monkeypatch, phase, committed, database_url=database_url)
