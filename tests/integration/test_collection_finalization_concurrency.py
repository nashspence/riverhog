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

pytestmark = pytest.mark.integration


def test_postgres_claims_reuse_accepted_readiness(database_url, tmp_path, record_property):
    _assert_claims_reuse(tmp_path, record_property, 8, database_url=database_url)


@pytest.mark.parametrize("callback", ["retry", "requeue"])
def test_postgres_stale_callback_rechecks_state_after_parent_lock(database_url, tmp_path, callback):
    with _case(tmp_path, "segment", artifact_count=1, database_url=database_url) as case:
        _seal(case)
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
                current._record_finalization_retry(case.collection_id, OSError("late report"))
            else:
                current._requeue_finalization_step(case.collection_id)

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
                    upload.state = upload.archive_phase = "orphaned"
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
            assert upload.state == upload.archive_phase == "orphaned"
            assert upload.archive_next_attempt_at is None
            assert upload.archive_failure == "current owner"
