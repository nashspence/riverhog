from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadProvenanceJournalRecord as Journal
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.catalog_provenance_publication_models import (
    CollectionUploadProvenanceJournalPublicationRecord as Publication,
)
from riverhog_core.services.archive_provenance_journal_checkpoint import (
    ArchiveProvenanceJournalSource as Source,
)
from riverhog_core.services.immutable_archive_receiving import drain_original_immutable_report
from riverhog_protocol.errors import Conflict
from sqlalchemy import event, func, select, text

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_provenance_journal_checkpoint import (
    _JOURNAL,
    _OTHER,
    _admit,
    _journal_source,
    _received,
    _stage_journal,
)
from tests.unit.test_archive_volume_metadata_receiving import _count
from tests.unit.test_collection_uploads import _CREATOR

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_postgres_actual_original_journal_cold_receiving_and_retirement(
    database_url, tmp_path, monkeypatch, mode
):
    with _journal_source(tmp_path, monkeypatch, mode, database_url=database_url) as f:
        admitted, response = _admit(f)
        f.owner.report(admitted, response, apply_receipt=lambda *args: None)
        assert _received(f) is None and _count(f.case) == 1
        assert drain_original_immutable_report(f.case.factory, f.case.collection_id)
        assert _received(f).stored_sha256 == response.result.stored_sha256
        assert _count(f.case) == 0 and f.calls == {"observe": 0, "execute": 1}
        with session_scope(f.case.factory) as session:
            f.source.verify_prepared_receipt(session, admitted.prepared, response.result)


@pytest.mark.parametrize("change", ("orphan", "construction", "journal", "origin"))
def test_postgres_journal_receiving_rechecks_original_source_after_observed_parent_wait(
    database_url, tmp_path, monkeypatch, change
):
    with _journal_source(tmp_path, monkeypatch, database_url=database_url) as f:
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
                    elif change == "journal":
                        session.get(Journal, (f.case.collection_id, _JOURNAL)).sha256 = "f" * 64
                    else:
                        row = session.get(
                            Publication,
                            (f.case.collection_id, f.source.publication.plaintext_sha256),
                        )
                        row.source_json += " "
                    session.flush()
                    future = pool.submit(receive)
                    assert waiting.wait(10) and blocked() and not future.done()
                if change != "orphan":
                    refusal = {
                        "construction": "archive write construction identity changed",
                        "journal": "provenance journal original source changed",
                        "origin": "provenance journal original source context changed",
                    }[change]
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


def test_postgres_concurrent_shared_bytes_choose_one_retained_original_position(
    database_url, tmp_path, monkeypatch
):
    with _journal_source(tmp_path, monkeypatch, database_url=database_url) as f:
        _stage_journal(f, _OTHER, b"other---other---")
        _stage_journal(f, "urn:uuid:33333333-3333-4333-8333-333333333333", b"other---other---")
        barrier = threading.Barrier(2)

        def stage(journal):
            guard = f.case.service._payload_write_guard(f.case.collection_id, _CREATOR)
            barrier.wait(timeout=10)
            with session_scope(f.case.factory) as session:
                return Source.stage(
                    session,
                    f.case.collection_id,
                    journal_id=journal,
                    offset=8,
                    size=8,
                    grants=f.original_grants,
                    forward_guard=guard,
                )

        with ThreadPoolExecutor(max_workers=2) as pool:
            a = pool.submit(stage, _OTHER)
            b = pool.submit(stage, "urn:uuid:33333333-3333-4333-8333-333333333333")
            first, second = a.result(timeout=15), b.result(timeout=15)
        assert first.source_json == second.source_json
        assert first.journal_range.journal_id in {
            _OTHER,
            "urn:uuid:33333333-3333-4333-8333-333333333333",
        }
        assert first.journal_range.offset == 8
        with session_scope(f.case.factory) as session:
            assert session.scalar(select(func.count()).select_from(Publication)) == 2
        assert f.calls == {"observe": 0, "execute": 0} and _count(f.case) == 0
