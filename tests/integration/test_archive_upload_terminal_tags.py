from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.services.catalog_sync import _tag_node_has_live_owner
from sqlalchemy import event, text

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_upload_terminal_tags import (
    _read,
    _reap,
    _select,
    _tagged,
    _teardown_upload,
)
from tests.unit.test_archive_write_admission import GRANTS

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("count", (0, 32))
def test_postgres_original_terminal_selector_survives_upload_teardown_and_exact_retry(
    database_url, tmp_path, count
):
    with _tagged(tmp_path, count=count, database_url=database_url) as case:
        operation = _select(case)
        original = _read(case, operation)
        _teardown_upload(case)
        _reap(case)

        def forbidden(*args):
            pytest.fail("original terminal retry attempted fresh selection")

        assert (
            _select(
                case,
                grants=replace(GRANTS, terminal_bytes=1),
                validate_selection=forbidden,
            )
            == operation
        )
        assert _read(case, operation) == original


def test_postgres_upload_teardown_waits_for_atomic_terminal_selector_retention(
    database_url, tmp_path
):
    with _tagged(tmp_path, count=16, database_url=database_url) as case:
        engine = case.factory.kw["bind"]
        lane = threading.local()
        retaining = threading.Event()
        deleting = threading.Event()
        release = threading.Event()
        pids = {}

        def capture(connection, cursor, statement, parameters, context, executemany):
            if getattr(lane, "role", None) == "selector" and statement.startswith(
                "INSERT INTO archive_upload_terminal_tags"
            ):
                cursor.execute("SELECT pg_backend_pid()")
                pids["selector"] = cursor.fetchone()[0]
                retaining.set()
                assert release.wait(10)
            if getattr(lane, "role", None) == "deleter" and statement.startswith(
                "DELETE FROM collection_uploads"
            ):
                cursor.execute("SELECT pg_backend_pid()")
                pids["deleter"] = cursor.fetchone()[0]
                deleting.set()

        def select():
            lane.role = "selector"
            return _select(case)

        def teardown():
            lane.role = "deleter"
            _teardown_upload(case)

        def blocked():
            deadline = time.monotonic() + 5
            with engine.connect() as observer:
                while time.monotonic() < deadline:
                    if pids["selector"] in observer.scalar(
                        text("SELECT pg_blocking_pids(:pid)"), {"pid": pids["deleter"]}
                    ):
                        return True
                    time.sleep(0.01)
            return False

        event.listen(engine, "before_cursor_execute", capture)
        try:
            with ThreadPoolExecutor(max_workers=2) as workers:
                try:
                    selecting = workers.submit(select)
                    assert retaining.wait(10)
                    with session_scope(case.factory) as observer:
                        from riverhog_core.catalog_models import CollectionUploadRecord

                        root = observer.get(
                            CollectionUploadRecord, case.collection_id
                        ).tag_staging_root_sha256
                        assert _tag_node_has_live_owner(observer, root)
                    tearing_down = workers.submit(teardown)
                    assert deleting.wait(10) and blocked() and not tearing_down.done()
                finally:
                    release.set()
                operation = selecting.result(timeout=10)
                tearing_down.result(timeout=10)
        finally:
            release.set()
            event.remove(engine, "before_cursor_execute", capture)
        _reap(case)
        assert _read(case, operation)[1] == set(case.tags)
