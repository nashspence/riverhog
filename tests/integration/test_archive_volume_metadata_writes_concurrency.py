from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_protocol.errors import Conflict
from sqlalchemy import event, select, text

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_volume_metadata_receiving import _count, _metadata_source
from tests.unit.test_archive_volume_metadata_writes import _actor, _publish

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("mode", ["direct", "http"])
def test_postgres_actual_metadata_publisher_observes_committed_intent_without_parent_lock(
    database_url, tmp_path, monkeypatch, mode
):
    with _metadata_source(tmp_path, monkeypatch, mode=mode, database_url=database_url) as fixture:
        original = _actor(fixture)
        published = _publish(original)
        assert _publish(original) == published
        assert len(original.calls["execute"]) == len(original.calls["observe"]) == 1
        assert not original.actor.has_pending and _count(original.case) == 0


def test_postgres_actual_metadata_actor_rechecks_finalizer_claim_after_parent_wait(
    database_url, tmp_path, monkeypatch
):
    with _metadata_source(tmp_path, monkeypatch, database_url=database_url) as fixture:
        original = _actor(fixture)
        case = original.case
        engine = case.factory.kw["bind"]
        lane = threading.local()
        waiting = threading.Event()
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
            return _publish(original)

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
                    session.flush()
                    future = pool.submit(compete)
                    assert waiting.wait(10)
                    assert blocked() and not future.done()
                with pytest.raises(Conflict, match="claim is not current"):
                    future.result(timeout=10)
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        assert original.calls == {"observe": [], "execute": []} and _count(case) == 0
