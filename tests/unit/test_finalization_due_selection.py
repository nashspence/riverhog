from __future__ import annotations

from contextlib import contextmanager

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services.archive_finalization_due import ArchiveFinalizationDue
from sqlalchemy import insert, select

from tests.unit.test_collection_upload_preparation import _case
from tests.unit.test_collection_upload_raw_readiness_work import sqlite_work

_NOW = "2026-11-09T00:00:00Z"
_DUE = "2026-11-08T00:00:00Z"


@contextmanager
def _due_catalog(tmp_path, prefix, pending, *, database_url=None):
    # Synthetic workflow hints qualify selection only. They contain no readiness,
    # construction admission or archive custody, and cannot authorize a claim.
    with _case(tmp_path, "segment", artifact_count=1, database_url=database_url) as case:
        with session_scope(case.factory) as session:
            table = Upload.__table__
            template = dict(
                session.execute(select(table).where(table.c.collection_id == case.collection_id))
                .mappings()
                .one()
            )
            template = {
                key: value for key, value in template.items() if table.c[key].computed is None
            }
            rows = []
            for offset in range(2 * prefix + pending):
                identity = 100 + offset
                row = {
                    **template,
                    "collection_id": identity,
                    "idempotency_key": f"due-selector-{identity}",
                    "archive_storage_prefix": f"due-selector/{identity}",
                    "state": "finalizing",
                    "archive_phase": "finalization_queued" if offset % 2 else "retry_wait",
                    "archive_next_attempt_at": _DUE,
                }
                if offset < prefix:
                    row.update(
                        state="orphaned", archive_phase="orphaned", archive_next_attempt_at=None
                    )
                elif offset < 2 * prefix:
                    row["archive_next_attempt_at"] = "9999-12-31T00:00:00Z"
                rows.append(row)
            if rows:
                session.execute(insert(table), rows)
        expected = list(range(100 + 2 * prefix, 100 + 2 * prefix + pending))
        yield case, expected


@pytest.mark.parametrize("prefix", (0, 32, 1000))
@pytest.mark.parametrize("pending", (0, 1, 7))
def test_due_selection_skips_inactive_and_future_backlogs(
    tmp_path, record_property, prefix, pending
):
    with _due_catalog(tmp_path, prefix, pending) as (case, expected):
        owner = ArchiveFinalizationDue(case.factory)
        with sqlite_work(case.service) as work:
            selected = owner.select(_NOW, limit=2)
        assert selected == tuple(expected[:2])
        assert work["vm_steps"] <= 300, (prefix, pending, work)
        record_property("inactive_parents", prefix)
        record_property("future_parents", prefix)
        record_property("due_parents", pending)
        record_property("selected_rows", len(selected))
        record_property("sqlite_vm_steps", work["vm_steps"])


@pytest.mark.parametrize("pending", (1, 17, 129))
def test_rotating_due_hints_give_each_busy_candidate_a_bounded_fair_visit(
    tmp_path, record_property, pending
):
    with _due_catalog(tmp_path, 32, pending) as (case, expected):
        owner = ArchiveFinalizationDue(case.factory)
        found = []
        total = 0
        for _ in range(2 * pending):
            with sqlite_work(case.service) as work:
                selected = owner.select(_NOW, limit=1)
            assert len(selected) == 1
            found.extend(selected)
            total += work["vm_steps"]
        assert found == expected + expected
        assert total <= 100 * len(found) + 500, (pending, total)
        # A lost volatile hint changes no durable state or admission authority.
        assert ArchiveFinalizationDue(case.factory).select(_NOW, limit=1) == (expected[0],)
        with owner._lock:
            assert owner.select(_NOW, limit=1) == ()
        record_property("due_parents", pending)
        record_property("fair_visits", len(found))
        record_property("selection_sqlite_vm_steps", total)


def test_due_pages_cross_timestamp_groups_and_wrap_with_bounded_queries(tmp_path, record_property):
    from sqlalchemy import event, update

    with _due_catalog(tmp_path, 32, 7) as (case, expected):
        with session_scope(case.factory) as session:
            for ids, when in (
                (expected[:3], "2026-11-07T00:00:00Z"),
                (expected[-2:], "2026-11-08T12:00:00Z"),
            ):
                session.execute(
                    update(Upload)
                    .where(Upload.collection_id.in_(ids))
                    .values(archive_next_attempt_at=when)
                )
        owner = ArchiveFinalizationDue(case.factory)
        engine = case.factory.kw["bind"]
        queries = []

        def observe(connection, cursor, statement, parameters, context, executemany):
            if statement.lstrip().upper().startswith("SELECT"):
                queries.append(statement)

        event.listen(engine, "before_cursor_execute", observe)
        try:
            counts, pages = [], []
            for _ in range(3):
                before = len(queries)
                pages.append(owner.select(_NOW, limit=4))
                counts.append(len(queries) - before)
        finally:
            event.remove(engine, "before_cursor_execute", observe)
        assert pages == [tuple(expected[:4]), tuple(expected[4:]), tuple(expected[:4])]
        assert counts == [1, 2, 3]
        record_property("selected_page_sizes", [len(page) for page in pages])
        record_property("bounded_select_counts", counts)
