from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_initial_metadata_models import (
    CollectionUploadInitialDescriptionRecord as Description,
)
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_protocol.errors import Conflict
from sqlalchemy import event, select, text

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.finalization_claim_fixtures import _different_claim
from tests.unit.test_archive_initial_description_checkpoint import (
    _admit,
    _cold_receive,
    _complete,
    _description_source,
    _received,
)
from tests.unit.test_archive_initial_description_finalizer import _cold_and_live, _owner
from tests.unit.test_archive_volume_metadata_receiving import _count
from tests.unit.test_finalization_claim_readiness import _current_claim

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("description", (None, "present"))
@pytest.mark.parametrize("mode", ("direct", "http"))
def test_postgres_actual_initial_description_receiving_and_progress(
    database_url, tmp_path, monkeypatch, description, mode
):
    with _description_source(
        tmp_path, monkeypatch, description, mode, database_url=database_url
    ) as f:
        _complete(f)


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_postgres_cold_initial_description_receives_after_current_selection_loss(
    database_url, tmp_path, monkeypatch, mode
):
    with _description_source(tmp_path, monkeypatch, mode=mode, database_url=database_url) as f:
        _cold_receive(f)


def test_postgres_retained_live_and_cold_description_owners_converge(
    database_url, tmp_path, monkeypatch
):
    with _description_source(tmp_path, monkeypatch, database_url=database_url) as f:
        _cold_and_live(f, monkeypatch)


@pytest.mark.parametrize("change", ("orphan", "selection", "source", "claim"))
def test_postgres_description_receiving_waits_for_parent_and_rechecks_original_facts(
    database_url, tmp_path, monkeypatch, change
):
    with _description_source(tmp_path, monkeypatch, database_url=database_url) as f:
        claim = _current_claim(f.case)
        calls_before = {k: len(v) for k, v in f.calls.items()}
        if change == "claim":
            contender = _owner(f, forward_guard=claim.guard)
        else:
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
            if change == "claim":
                return contender.advance()
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
                    elif change == "selection":
                        upload.description = "different current selection"
                        upload.final_authority_json = None
                    elif change == "claim":
                        upload.archive_claim_id = _different_claim(claim)
                    else:
                        session.get(Description, f.case.collection_id).document_json += " "
                    session.flush()
                    future = pool.submit(receive)
                    assert waiting.wait(10) and blocked() and not future.done()
                if change in {"source", "claim"}:
                    with pytest.raises(
                        Conflict,
                        match="original finalization claim"
                        if change == "claim"
                        else "original document changed",
                    ):
                        future.result(timeout=10)
                else:
                    future.result(timeout=10)
                    f.owner.acknowledge_report(
                        admitted, verify_receipt=f.source.verify_prepared_receipt
                    )
                    assert _received(f).stored_sha256 == response.result.stored_sha256
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        assert _count(f.case) == (1 if change == "source" else 0)
        if change == "claim":
            assert not contender.has_pending
            assert {k: len(v) for k, v in f.calls.items()} == calls_before
