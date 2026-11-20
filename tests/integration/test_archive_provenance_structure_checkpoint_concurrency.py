from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import (
    CollectionUploadProvenanceStructureRecord as Structure,
)
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services.immutable_archive_receiving import drain_original_immutable_report
from riverhog_protocol.errors import Conflict
from sqlalchemy import event, select, text

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_provenance_structure_checkpoint import (
    _admit,
    _received,
    _structure_source,
)
from tests.unit.test_archive_volume_metadata_receiving import _count

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("kind", ("history", "record-page", "source-proof"))
@pytest.mark.parametrize("mode", ("direct", "http"))
def test_postgres_actual_staged_structure_receiving_and_retirement(
    database_url, tmp_path, monkeypatch, kind, mode
):
    with _structure_source(tmp_path, monkeypatch, kind, mode, database_url=database_url) as f:
        admitted, response = _admit(f)
        f.owner.report(admitted, response, apply_receipt=lambda *args: None)
        assert _received(f) is None and _count(f.case) == 1
        assert drain_original_immutable_report(f.case.factory, f.case.collection_id)
        assert _received(f).stored_sha256 == response.result.stored_sha256
        assert _count(f.case) == 0 and f.calls == {"observe": 0, "execute": 1}
        with session_scope(f.case.factory) as session:
            f.source.verify_prepared_receipt(session, admitted.prepared, response.result)


@pytest.mark.parametrize("change", ("orphan", "construction", "source"))
def test_postgres_original_structure_rechecks_source_after_observed_parent_lock_wait(
    database_url, tmp_path, monkeypatch, change
):
    with _structure_source(tmp_path, monkeypatch, database_url=database_url) as f:
        admitted, response = _admit(f)
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

        def receive():
            lane.contender = True
            return f.owner.report(admitted, response, apply_receipt=f.source.apply_prepared_receipt)

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
                    elif change == "construction":
                        upload.archive_generation = "f" * 64
                    else:
                        row = session.get(Structure, (f.case.collection_id, f.source.object_id))
                        row.content = row.content.replace(b'"bytes":"7"', b'"bytes":"8"')
                        assert row.content != f.source.publication.plaintext
                    session.flush()
                    future = pool.submit(receive)
                    assert waiting.wait(10) and blocked() and not future.done()
                if change != "orphan":
                    refusal = (
                        "archive write construction identity changed"
                        if change == "construction"
                        else "provenance structure staged identity changed"
                    )
                    with pytest.raises(Conflict, match=refusal):
                        future.result(timeout=10)
                else:
                    future.result(timeout=10)
                    f.owner.acknowledge_report(
                        admitted, verify_receipt=f.source.verify_prepared_receipt
                    )
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        assert _count(f.case) == (0 if change == "orphan" else 1)
        if change == "orphan":
            assert _received(f).stored_sha256 == response.result.stored_sha256
