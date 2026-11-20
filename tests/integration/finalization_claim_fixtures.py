from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_protocol.errors import Conflict
from sqlalchemy import event, select, text

from tests.unit.finalization_claim_fixtures import _different_claim


def assert_replaced_claim_refused_after_parent_wait(case, claim, advance):
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
        return advance()

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
                assert upload.archive_claim_id == claim.claim_id
                pids["holder"] = session.scalar(text("SELECT pg_backend_pid()"))
                upload.archive_claim_id = _different_claim(claim)
                session.flush()
                future = pool.submit(compete)
                assert waiting.wait(10) and blocked() and not future.done()
            with pytest.raises(Conflict, match="original finalization claim"):
                future.result(timeout=10)
    finally:
        event.remove(engine, "before_cursor_execute", capture)
