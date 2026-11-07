from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_upload_drain import (
    _archive_transition,
    _cache_begin_transition,
    _cache_write_transition,
)
from tests.unit.test_archive_write_admission import _case
from tests.unit.test_cache_population_begin import _case as _cache_case
from tests.unit.test_cache_population_writes import _writing

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("unknown", (False, True))
def test_postgres_archive_drain_facts_preserve_original_closure(database_url, tmp_path, unknown):
    with _case(tmp_path, database_url=database_url) as case:
        _archive_transition(case, unknown)


@pytest.mark.parametrize("unknown", (False, True))
def test_postgres_cache_begin_drain_facts_are_independent(database_url, tmp_path, unknown):
    with _cache_case(tmp_path, database_url=database_url) as case:
        _cache_begin_transition(case, unknown)


@pytest.mark.parametrize("unknown", (False, True))
def test_postgres_cache_child_drain_facts_keep_original_receiving(database_url, tmp_path, unknown):
    with _writing(tmp_path, database_url=database_url) as writing:
        _cache_write_transition(writing, unknown)


def test_postgres_terminal_record_inspection_waits_for_parent_and_rechecks_selection(
    database_url, tmp_path
):
    import threading
    import time
    from concurrent.futures import ThreadPoolExecutor

    from riverhog_core.catalog_archive_write_models import ArchiveUploadTerminalRecord
    from riverhog_core.catalog_db import session_scope
    from riverhog_core.catalog_models import CollectionUploadRecord as Upload
    from riverhog_protocol.errors import Conflict
    from sqlalchemy import event, select, text

    from tests.unit.test_archive_upload_drain import _inspect
    from tests.unit.test_archive_write_admission import _selected

    with _case(tmp_path, database_url=database_url) as case:
        operation = _selected(case)
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

        def inspect():
            lane.contender = True
            return _inspect(case, operation)

        def blocked():
            deadline = time.monotonic() + 10
            with engine.connect() as observer:
                while time.monotonic() < deadline:
                    if pids["holder"] in observer.scalar(
                        text("SELECT pg_blocking_pids(:pid)"), {"pid": pids["contender"]}
                    ):
                        return True
                    time.sleep(0.01)
            return False

        event.listen(engine, "before_cursor_execute", capture)
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                with session_scope(case.factory) as session:
                    session.scalar(
                        select(Upload)
                        .where(Upload.collection_id == case.collection_id)
                        .with_for_update()
                    )
                    pids["holder"] = session.scalar(text("SELECT pg_backend_pid()"))
                    terminal = session.get(ArchiveUploadTerminalRecord, operation)
                    terminal.frontier = "999"
                    session.flush()
                    future = pool.submit(inspect)
                    assert waiting.wait(10) and blocked() and not future.done()
                with pytest.raises(Conflict, match="original selection changed"):
                    future.result(timeout=10)
        finally:
            event.remove(engine, "before_cursor_execute", capture)


@pytest.mark.parametrize("change", ("control", "construction"))
def test_postgres_terminal_inspection_refreshes_previously_loaded_owner_rows(
    database_url, tmp_path, change
):
    from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord
    from riverhog_core.catalog_db import session_scope
    from riverhog_core.catalog_models import CollectionUploadRecord as Upload
    from riverhog_core.services.archive_upload_drain import inspect_terminal_write_records
    from riverhog_protocol.errors import Conflict

    from tests.unit.test_archive_write_admission import _selected

    with _case(tmp_path, database_url=database_url) as case:
        operation = _selected(case)
        with session_scope(case.factory) as stale:
            upload = stale.get(Upload, case.collection_id)
            control = stale.get(ArchiveWriteControlRecord, case.collection_id)
            assert upload is not None and control.next_call == "0"
            with session_scope(case.factory) as current:
                if change == "control":
                    current.get(ArchiveWriteControlRecord, case.collection_id).next_call = "1"
                else:
                    current.get(Upload, case.collection_id).archive_storage_prefix += "-changed"
            with pytest.raises(Conflict):
                inspect_terminal_write_records(stale, case.collection_id, operation)
