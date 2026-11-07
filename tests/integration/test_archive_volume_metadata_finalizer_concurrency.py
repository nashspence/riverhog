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
from tests.unit.test_archive_volume_metadata_checkpoint import _snapshot
from tests.unit.test_archive_volume_metadata_finalizer import _fresh, _owner
from tests.unit.test_archive_volume_metadata_receiving import _count, _metadata_source
from tests.unit.test_finalization_claim_readiness import _current_claim, _fresh_coordinator

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("mode", ["direct", "http"])
def test_postgres_explicit_finalizer_composes_actual_requeued_metadata_steps(
    database_url, tmp_path, monkeypatch, mode
):
    with _metadata_source(tmp_path, monkeypatch, mode=mode, database_url=database_url) as fixture:
        original = _owner(fixture)
        for _ in range(3):
            assert original.owner.advance()
            coordinator = _fresh_coordinator(original.case)
            coordinator._requeue_finalization_step(_current_claim(original.case))
            assert (
                coordinator._claim_due_finalization().collection_id == original.case.collection_id
            )
        assert not original.owner.advance()
        assert not original.owner.has_pending and _count(original.case) == 0
        assert _snapshot(original.case)[3] is not None
        assert len(original.calls["execute"]) == len(original.calls["observe"]) == 3


@pytest.mark.parametrize("change", ["unresolved", "claim"])
def test_postgres_finalizer_checks_closure_and_claim_under_actual_parent_lock(
    database_url, tmp_path, monkeypatch, change
):
    with _metadata_source(tmp_path, monkeypatch, database_url=database_url) as fixture:
        original = _owner(fixture)
        case = original.case
        if change == "unresolved":
            execute = original.ports.execute_write

            def unknown(invocation, *, content):
                return execute(invocation, content=content).model_copy(
                    update={"execution": "unknown"}
                )

            monkeypatch.setattr(original.ports, "execute_write", unknown)
            with pytest.raises(Conflict, match="execution remains unknown"):
                original.owner.advance()
            contender = _fresh(original)
        else:
            contender = original.owner
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
            return contender.advance()

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
                    if change == "claim":
                        upload.state = upload.archive_phase = "orphaned"
                        session.flush()
                    future = pool.submit(compete)
                    assert waiting.wait(10)
                    assert blocked() and not future.done()
                with pytest.raises(
                    Conflict,
                    match="unresolved provider writes"
                    if change == "unresolved"
                    else "claim is not current",
                ):
                    future.result(timeout=10)
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        expected = int(change == "unresolved")
        assert _count(case) == _snapshot(case)[0] == expected
        assert len(original.calls["execute"]) == len(original.calls["observe"]) == expected
