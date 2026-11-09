from __future__ import annotations

import pytest
from riverhog_core.catalog_archive_write_models import ArchiveUploadTerminalRecord as Terminal
from riverhog_core.catalog_db import session_scope
from riverhog_core.services.archive_upload_terminal_due import _due_query
from sqlalchemy import select

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_upload_terminal_due import (
    _cold,
    _lost_due,
    _lost_stop,
    _shared_fairness,
)
from tests.unit.test_archive_upload_terminal_turn import _turn_case

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("phase", ("cleanup-ready", "cleaning", "needs-reconfirmation"))
def test_postgres_cold_terminal_due_preserves_original_receiving(
    database_url, tmp_path, monkeypatch, phase
):
    with _turn_case(tmp_path, database_url=database_url, originals=3) as case:
        _cold(case, monkeypatch, phase)


def test_postgres_lost_stopping_reply_keeps_same_original_without_a_due_row(
    database_url, tmp_path, monkeypatch
):
    with _turn_case(tmp_path, database_url=database_url) as case:
        _lost_stop(case, monkeypatch)


@pytest.mark.parametrize("committed", (False, True))
def test_postgres_failed_due_claim_never_dispatches_speculatively(
    database_url, tmp_path, monkeypatch, committed
):
    with _turn_case(tmp_path, database_url=database_url, originals=0) as case:
        _lost_due(case, monkeypatch, committed)


def test_postgres_actual_three_owner_shared_worker_fairness(database_url, tmp_path, monkeypatch):
    _shared_fairness(tmp_path, monkeypatch, 3, database_url=database_url)


def test_postgres_execution_due_selector_uses_index_and_skips_a_held_terminal(
    database_url, tmp_path
):
    with _turn_case(tmp_path, database_url=database_url, originals=0) as case:
        with session_scope(case.factory) as held:
            held.scalar(select(Terminal).where(Terminal.id == case.operation).with_for_update())
            with session_scope(case.factory) as other:
                query = _due_query("9999-01-01T00:00:00.000000000Z", 1)
                assert other.scalars(query).all() == []
        with session_scope(case.factory) as session:
            session.connection().exec_driver_sql("SET LOCAL enable_seqscan=off")
            query = _due_query("9999-01-01T00:00:00.000000000Z", 1)
            sql = str(query.compile(session.bind, compile_kwargs={"literal_binds": True}))
            plan = " ".join(
                row[0] for row in session.connection().exec_driver_sql("EXPLAIN " + sql)
            )
            assert "ix_archive_upload_terminals_execution_due" in plan
            assert "Sort" not in plan


def test_postgres_cold_refused_cancel_keeps_positive_custody_and_stops_scheduling(
    database_url, tmp_path, monkeypatch
):
    from tests.unit.test_archive_upload_terminal_due import _cold_refused

    _cold_refused(tmp_path, monkeypatch, database_url=database_url)
