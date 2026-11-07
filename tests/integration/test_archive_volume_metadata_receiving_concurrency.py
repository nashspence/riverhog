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
from tests.unit.test_archive_volume_metadata_receiving import _count, _metadata_source, _reported
from tests.unit.test_finalization_claim_readiness import _fresh_coordinator

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("received", [False, True])
def test_postgres_cold_receiving_rechecks_original_source_after_real_parent_wait(
    database_url, tmp_path, monkeypatch, received
):
    with _metadata_source(tmp_path, monkeypatch, database_url=database_url) as fixture:
        case = fixture.case
        _reported(fixture, 0, receive=received)
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
            return _fresh_coordinator(case)._drain_archive_immutable_report(case.collection_id)

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
                    upload.archive_tree_sha256 = "c" * 64
                    session.flush()
                    future = pool.submit(compete)
                    assert waiting.wait(10)
                    assert blocked() and not future.done()
                with pytest.raises(Conflict, match="request differs"):
                    future.result(timeout=10)
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        assert _count(case) == 1
        with session_scope(case.factory) as session:
            assert session.get(Upload, case.collection_id).archive_volume_next_sequence == int(
                received
            )
