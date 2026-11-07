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
from tests.unit.test_archive_provenance_structure_checkpoint import _structure_source
from tests.unit.test_archive_provenance_structure_writer import (
    _closure,
    _cold_and_live,
    _complete_all,
    _owner,
)
from tests.unit.test_archive_volume_metadata_receiving import _count

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_postgres_actual_retained_structural_scheduler_publishes_each_pending_kind(
    database_url, tmp_path, monkeypatch, mode
):
    with _structure_source(tmp_path, monkeypatch, mode=mode, database_url=database_url) as f:
        _complete_all(f)


@pytest.mark.parametrize("kind", ("history", "record-page", "source-proof"))
def test_postgres_live_and_cold_structural_writers_receive_one_original_result(
    database_url, tmp_path, monkeypatch, kind
):
    with _structure_source(tmp_path, monkeypatch, kind, database_url=database_url) as f:
        _cold_and_live(f, monkeypatch)


@pytest.mark.parametrize("change", ("orphan", "closure"))
def test_postgres_pending_structure_selection_rechecks_authority_after_observed_parent_wait(
    database_url, tmp_path, monkeypatch, change
):
    with _structure_source(tmp_path, monkeypatch, database_url=database_url) as f:
        _closure(f)
        original = _owner(f)
        engine = f.case.factory.kw["bind"]
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

        def advance():
            lane.contender = True
            return original.owner.advance()

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
                with session_scope(f.case.factory) as session:
                    upload = session.scalar(
                        select(Upload)
                        .where(Upload.collection_id == f.case.collection_id)
                        .with_for_update()
                    )
                    pids["holder"] = session.scalar(text("SELECT pg_backend_pid()"))
                    if change == "orphan":
                        upload.state = upload.archive_phase = "orphaned"
                    else:
                        upload.provenance_closure_validated = False
                    session.flush()
                    future = pool.submit(advance)
                    assert waiting.wait(10) and blocked() and not future.done()
                if change == "orphan":
                    with pytest.raises(Conflict):
                        future.result(timeout=10)
                else:
                    assert future.result(timeout=10) is False
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        assert not original.owner.has_pending and _count(f.case) == 0
        assert f.calls == {"observe": 0, "execute": 0}
