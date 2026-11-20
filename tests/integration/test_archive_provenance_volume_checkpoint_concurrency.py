from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.catalog_provenance_publication_models import (
    CollectionUploadProvenanceVolumePublicationRecord as Volume,
)
from riverhog_protocol.errors import Conflict
from sqlalchemy import event, select, text

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_provenance_structure_checkpoint import _admit
from tests.unit.test_archive_provenance_volume_checkpoint import (
    _cold_receiving,
    _publish,
    _received,
    _select,
    _volume_source,
)
from tests.unit.test_archive_volume_metadata_receiving import _count

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("role", ("bindings", "metadata"))
@pytest.mark.parametrize("mode", ("direct", "http"))
def test_postgres_original_corpus_receiving_after_forward_loss(
    database_url, tmp_path, monkeypatch, role, mode
):
    with _volume_source(tmp_path, monkeypatch, mode, database_url=database_url) as fixture:
        _cold_receiving(fixture, role)


@pytest.mark.parametrize("role", ("bindings", "metadata"))
@pytest.mark.parametrize("change", ("orphan", "document"))
def test_postgres_corpus_receiving_rechecks_original_source_after_observed_parent_wait(
    database_url, tmp_path, monkeypatch, role, change
):
    with _volume_source(tmp_path, monkeypatch, database_url=database_url) as f:
        if role == "metadata":
            _publish(f, "bindings")
        source = _select(f, role)
        admitted, response = _admit(source)
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
                admitted, response, apply_receipt=source.source.apply_prepared_receipt
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
                    else:
                        session.get(Volume, (f.case.collection_id, 0)).document_json += " "
                    session.flush()
                    future = pool.submit(receive)
                    assert waiting.wait(10) and blocked() and not future.done()
                if change == "document":
                    with pytest.raises(Conflict, match="original document changed"):
                        future.result(timeout=10)
                    assert _count(f.case) == 1
                else:
                    future.result(timeout=10)
                    assert _received(f, role) is not None and _count(f.case) == 1
                    f.owner.acknowledge_report(
                        admitted, verify_receipt=source.source.verify_prepared_receipt
                    )
                    assert _count(f.case) == 0
        finally:
            event.remove(engine, "before_cursor_execute", capture)
