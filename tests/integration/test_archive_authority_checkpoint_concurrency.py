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
from tests.unit.test_archive_authority_checkpoint import (
    _admit,
    _authority_source,
    _snapshot,
    _source,
    _write,
)
from tests.unit.test_archive_volume_metadata_receiving import _count

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("mode", ["direct", "http"])
def test_postgres_actual_root_then_exact_recovery_progress_under_one_call(
    database_url, tmp_path, monkeypatch, mode
):
    with _authority_source(tmp_path, monkeypatch, mode, database_url=database_url) as fixture:
        root = _source(fixture, "root")
        owner, admitted, receipt = _write(fixture, root)
        assert _snapshot(fixture)[0] is not None and _snapshot(fixture)[1] is None
        owner.acknowledge_report(admitted, verify_receipt=root.verify_prepared_receipt)
        assert _count(fixture.case) == 0
        recovery = _source(fixture, "recovery")
        assert recovery.publication.descriptor.root.stored_sha256 == receipt.stored_sha256
        owner, admitted, _ = _write(fixture, recovery)
        owner.acknowledge_report(admitted, verify_receipt=recovery.verify_prepared_receipt)
        assert _snapshot(fixture)[1] is not None and _count(fixture.case) == 0


@pytest.mark.parametrize("kind", ["root", "recovery"])
@pytest.mark.parametrize("change", ["orphan", "source"])
def test_postgres_original_authority_receiving_rechecks_source_after_real_parent_wait(
    database_url, tmp_path, monkeypatch, kind, change
):
    with _authority_source(tmp_path, monkeypatch, database_url=database_url) as fixture:
        root = _source(fixture, "root")
        if kind == "recovery":
            owner, admitted, _ = _write(fixture, root)
            owner.acknowledge_report(admitted, verify_receipt=root.verify_prepared_receipt)
        source = _source(fixture, kind)
        owner, admitted = _admit(fixture, source)
        response = owner.execute(admitted, fixture.physical)
        assert response.execution == "closed"
        engine = fixture.case.factory.kw["bind"]
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
            return owner.report(admitted, response, apply_receipt=source.apply_prepared_receipt)

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
                with session_scope(fixture.case.factory) as session:
                    upload = session.scalar(
                        select(Upload)
                        .where(Upload.collection_id == fixture.case.collection_id)
                        .with_for_update()
                    )
                    pids["holder"] = session.scalar(text("SELECT pg_backend_pid()"))
                    if change == "orphan":
                        upload.state = upload.archive_phase = "orphaned"
                    else:
                        upload.archive_ordered_volume_sha256 = "f" * 64
                    session.flush()
                    future = pool.submit(receive)
                    assert waiting.wait(10) and blocked() and not future.done()
                if change == "source":
                    with pytest.raises(Conflict, match="original construction changed"):
                        future.result(timeout=10)
                else:
                    future.result(timeout=10)
                    owner.acknowledge_report(
                        admitted, verify_receipt=source.verify_prepared_receipt
                    )
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        if change == "source":
            assert _count(fixture.case) == 1
            assert _snapshot(fixture)[0 if kind == "root" else 1] is None
        else:
            assert _count(fixture.case) == 0
            assert _snapshot(fixture)[0 if kind == "root" else 1] is not None
            with session_scope(fixture.case.factory) as session:
                with pytest.raises(Conflict, match="claim is not current"):
                    source.guard(session)
