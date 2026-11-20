from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import AppKeyRecord, CollectionUploadRecord
from riverhog_core.services.archive_write_admission import _writer
from riverhog_protocol.errors import NotFound
from sqlalchemy import event, text
from sqlalchemy.exc import OperationalError

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_upload_terminal_tags import _tagged
from tests.unit.test_upload_cancel_authority import _control, _observe, _select
from tests.unit.test_upload_terminal_read_authority import _creator_key

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("held_row", ("key", "upload"))
def test_postgres_creator_cancel_defers_on_current_key_or_parent_contention(
    database_url, tmp_path, held_row
):
    with _tagged(tmp_path, count=3, database_url=database_url) as case:
        _, principal, _ = _creator_key(case)
        guard = case.service._cancel_write_guard(case.collection_id, principal)
        original = _control(case)
        with session_scope(case.factory) as holder:
            if held_row == "key":
                holder.get(AppKeyRecord, principal.key_id, with_for_update=True)
            else:
                holder.get(CollectionUploadRecord, case.collection_id, with_for_update=True)
            with pytest.raises(OperationalError) as failure:
                _select(case, principal, guard)
            assert failure.value.orig.sqlstate == "55P03"
        assert _control(case) == original
        operation = _select(case, principal, guard)
        assert _observe(case, operation, guard).records.operation_id == operation


def test_postgres_current_cancel_key_fence_interlocks_actual_revocation(database_url, tmp_path):
    with _tagged(tmp_path, count=3, database_url=database_url) as case:
        keys, principal, _ = _creator_key(case)
        guard = case.service._cancel_write_guard(case.collection_id, principal)
        operation = _select(case, principal, guard)
        original = _control(case)
        engine = case.factory.kw["bind"]
        lane = threading.local()
        waiting = threading.Event()
        pids = {}

        def capture(connection, cursor, statement, parameters, context, executemany):
            if (
                getattr(lane, "role", None) == "revoker"
                and "FROM app_keys" in statement
                and "FOR UPDATE" in statement
            ):
                cursor.execute("SELECT pg_backend_pid()")
                pids["revoker"] = cursor.fetchone()[0]
                waiting.set()

        def revoke():
            lane.role = "revoker"
            keys.revoke(app=principal.id, key_id=principal.key_id)

        def blocked_by(holder):
            deadline = time.monotonic() + 5
            with engine.connect() as observer:
                while time.monotonic() < deadline:
                    if holder in observer.scalar(
                        text("SELECT pg_blocking_pids(:pid)"), {"pid": pids["revoker"]}
                    ):
                        return True
                    time.sleep(0.01)
            return False

        event.listen(engine, "before_cursor_execute", capture)
        try:
            with ThreadPoolExecutor(max_workers=1) as workers:
                with session_scope(case.factory) as holder:
                    _writer(holder)
                    guard(holder)
                    pid = holder.scalar(text("SELECT pg_backend_pid()"))
                    future = workers.submit(revoke)
                    assert waiting.wait(10) and blocked_by(pid) and not future.done()
                future.result(timeout=10)
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        with pytest.raises(NotFound, match="application key"):
            _observe(case, operation, guard)
        assert _control(case) == original
