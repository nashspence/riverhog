from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import pytest
from riverhog_core.services import archive_write_admission as admission_module
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from riverhog_protocol.errors import Conflict
from sqlalchemy import event, text

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_artifact_custody_receipt import (
    _assert_receipt_reply,
    _counts,
    _original,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("committed", (False, True))
def test_postgres_original_custody_receipt_survives_commit_reply_loss(
    database_url, tmp_path, monkeypatch, committed
):
    _assert_receipt_reply(tmp_path, monkeypatch, committed, database_url=database_url)


def test_postgres_custody_acknowledgement_rechecks_terminal_after_parent_wait(
    database_url, tmp_path, monkeypatch
):
    with _original(tmp_path, monkeypatch, database_url=database_url) as (
        case,
        source,
        owner,
        selections,
    ):
        engine = case.factory.kw["bind"]
        held, release, waiting = threading.Event(), threading.Event(), threading.Event()
        lane = threading.local()
        pids, operations, failures = {}, [], []
        actual_scope = admission_module.session_scope

        @contextmanager
        def hold_terminal(factory):
            with actual_scope(factory) as session:
                yield session
                pids["holder"] = session.scalar(text("SELECT pg_backend_pid()"))
                held.set()
                assert release.wait(10)

        def select_terminal():
            operations.append(
                ArchiveWriteAdmission(case.factory).select_terminal(
                    1,
                    kind="cancel",
                    request={"fixture": "custody-acknowledgement-wait"},
                    principal_id="uploader",
                    key_id=None,
                    grants=case.config.archive_write,
                    guard=lambda session: None,
                    validate_selection=lambda *args: None,
                )
            )

        def capture(connection, cursor, statement, parameters, context, executemany):
            if (
                getattr(lane, "receipt", False)
                and "FROM collection_uploads" in statement
                and "FOR UPDATE" in statement
            ):
                cursor.execute("SELECT pg_backend_pid()")
                pids["receipt"] = cursor.fetchone()[0]
                waiting.set()

        def acknowledge():
            lane.receipt = True
            try:
                owner.advance()
            except BaseException as exc:
                failures.append(exc)

        monkeypatch.setattr(admission_module, "session_scope", hold_terminal)
        event.listen(engine, "before_cursor_execute", capture)
        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                terminal = pool.submit(select_terminal)
                assert held.wait(10)
                receipt = pool.submit(acknowledge)
                try:
                    assert waiting.wait(10)
                    deadline, blocked = time.monotonic() + 10, False
                    with engine.connect() as observer:
                        while time.monotonic() < deadline:
                            blockers = observer.scalar(
                                text("SELECT pg_blocking_pids(:pid)"), {"pid": pids["receipt"]}
                            )
                            if pids["holder"] in blockers:
                                blocked = True
                                break
                            time.sleep(0.01)
                    assert blocked and not receipt.done()
                finally:
                    release.set()
                terminal.result(timeout=10)
                receipt.result(timeout=10)
        finally:
            release.set()
            event.remove(engine, "before_cursor_execute", capture)
        assert len(operations) == 1 and len(failures) == 1
        assert isinstance(failures[0], Conflict) and "admission is closed" in str(failures[0])
        assert owner.has_pending and owner._source is source and selections == [source]
        assert _counts(case) == (0, 0)
