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
from tests.unit.test_archive_provenance_volume_checkpoint import _volume_source
from tests.unit.test_archive_provenance_volume_progress import (
    _commit,
    _journal_pair,
    _ready,
    _snapshot,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("change", ("committed", "orphan", "cursor"))
def test_postgres_corpus_progress_observes_parent_wait_and_rechecks_original_authority(
    database_url, tmp_path, monkeypatch, change
):
    with _volume_source(tmp_path, monkeypatch, database_url=database_url) as f:
        _ready(f)
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

        def commit():
            lane.contender = True
            return _commit(f)

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
                    if change == "committed":
                        assert f.metadata.commit_volume(session)
                        upload = session.get(Upload, f.case.collection_id)
                    else:
                        upload = session.scalar(
                            select(Upload)
                            .where(Upload.collection_id == f.case.collection_id)
                            .with_for_update()
                        )
                        if change == "orphan":
                            upload.state = upload.archive_phase = "orphaned"
                        else:
                            upload.provenance_archive_next_artifact_order += 1
                    pids["holder"] = session.scalar(text("SELECT pg_backend_pid()"))
                    session.flush()
                    future = pool.submit(commit)
                    assert waiting.wait(10) and blocked() and not future.done()
                if change == "committed":
                    assert future.result(timeout=10) is False
                    assert _snapshot(f)[0] == 1 and _snapshot(f)[-1] == 1
                else:
                    with pytest.raises(Conflict):
                        future.result(timeout=10)
                    assert _snapshot(f)[0] == 0 and _snapshot(f)[-1] == 0
        finally:
            event.remove(engine, "before_cursor_execute", capture)


def test_postgres_actual_journal_pair_advances_its_original_journal_cursor(
    database_url, tmp_path, monkeypatch
):
    with _volume_source(tmp_path, monkeypatch, kind="journal", database_url=database_url) as f:
        _journal_pair(f, f.metadata)
        assert _commit(f)
        before = _snapshot(f)
        assert before[0] == 1 and before[4] == f.metadata.document.journal_id
        assert before[5:7] == (None, 0) and before[-1] == 1
        assert not _commit(f) and _snapshot(f) == before
        assert f.calls == {"observe": 2, "execute": 2}
