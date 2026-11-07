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
