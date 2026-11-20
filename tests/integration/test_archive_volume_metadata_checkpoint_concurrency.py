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
from tests.unit.test_archive_volume_metadata_checkpoint import _publish, _snapshot, _source
from tests.unit.test_collection_upload_preparation import _case
from tests.unit.test_finalization_claim_readiness import _seal

pytestmark = pytest.mark.integration


def test_postgres_receives_each_volume_and_terminal_once(database_url, tmp_path):
    with _case(tmp_path, "segment", artifact_count=2, database_url=database_url) as case:
        _seal(case)
        for sequence in range(2):
            source = _source(case, sequence)
            published = _publish(source)
            with session_scope(case.factory) as session:
                source.apply_receipt(session, published)
                source.verify_receipt(session, published)
                source.apply_receipt(session, published)
        source = _source(case, 2, terminal=True)
        published = _publish(source)
        with session_scope(case.factory) as session:
            source.apply_receipt(session, published)
            source.verify_receipt(session, published)
        before = _snapshot(case)
        cold = _source(case, 2, terminal=True)
        with session_scope(case.factory) as session:
            cold.apply_receipt(session, published)
            cold.verify_receipt(session, published)
        assert _snapshot(case) == before and before[3] is not None and before[1] is None


@pytest.mark.parametrize("method", ["apply_receipt", "verify_receipt"])
def test_postgres_receiving_rechecks_original_source_after_parent_wait(
    database_url, tmp_path, method
):
    with _case(tmp_path, "segment", artifact_count=2, database_url=database_url) as case:
        _seal(case)
        source = _source(case, 0)
        published = _publish(source)
        if method == "verify_receipt":
            with session_scope(case.factory) as session:
                source.apply_receipt(session, published)
        before = _snapshot(case)
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
            with session_scope(case.factory) as session:
                getattr(source, method)(session, published)

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
                    assert waiting.wait(timeout=10)
                    assert blocked() and not future.done()
                with pytest.raises(Conflict, match="construction changed"):
                    future.result(timeout=10)
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        assert _snapshot(case) == before
