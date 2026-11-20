from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.catalog_provenance_publication_models import (
    CollectionUploadProvenanceAuthorityPublicationRecord as Authority,
)
from riverhog_protocol.errors import Conflict
from sqlalchemy import event, select, text

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_provenance_authority_checkpoint import (
    _authority_source,
    _cold_receive,
    _complete_pair,
    _original,
    _received,
)
from tests.unit.test_archive_provenance_structure_checkpoint import _admit
from tests.unit.test_archive_volume_metadata_receiving import _count

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("role", ("terminal", "root"))
@pytest.mark.parametrize("mode", ("direct", "http"))
def test_postgres_actual_authority_receiving_and_once_only_progress(
    database_url, tmp_path, monkeypatch, role, mode
):
    with _authority_source(tmp_path, monkeypatch, role, mode, database_url=database_url) as f:
        _complete_pair(f)


@pytest.mark.parametrize("role", ("terminal", "root"))
@pytest.mark.parametrize("mode", ("direct", "http"))
def test_postgres_actual_cold_authority_receiving_after_current_claim_loss(
    database_url, tmp_path, monkeypatch, role, mode
):
    with _authority_source(tmp_path, monkeypatch, role, mode, database_url=database_url) as f:
        _cold_receive(f)


@pytest.mark.parametrize("role", ("terminal", "root"))
@pytest.mark.parametrize("change", ("orphan", "cursor", "source"))
def test_postgres_original_authority_receiving_observes_parent_wait_and_rechecks_source(
    database_url, tmp_path, monkeypatch, role, change
):
    with _authority_source(tmp_path, monkeypatch, role, database_url=database_url) as f:
        admitted, response = _admit(_original(f))
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
            return f.owner.report(
                admitted, response, apply_receipt=f.authority.apply_prepared_receipt
            )

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
                    elif change == "cursor":
                        upload.provenance_archive_next_sequence += 1
                    else:
                        session.get(Authority, (f.case.collection_id, role)).document_json += " "
                    session.flush()
                    future = pool.submit(receive)
                    assert waiting.wait(10) and blocked() and not future.done()
                if change == "source":
                    with pytest.raises(Conflict, match="original document changed"):
                        future.result(timeout=10)
                else:
                    future.result(timeout=10)
                    f.owner.acknowledge_report(
                        admitted, verify_receipt=f.authority.verify_prepared_receipt
                    )
                    assert _received(f).stored_sha256 == response.result.stored_sha256
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        assert _count(f.case) == (1 if change == "source" else 0)
