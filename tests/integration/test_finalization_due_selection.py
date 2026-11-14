from __future__ import annotations

import pytest
from psycopg import sql as pgsql
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services.archive_finalization_due import finalization_due_query
from sqlalchemy import update
from sqlalchemy.dialects import postgresql

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_finalization_due_selection import _DUE, _NOW, _due_catalog

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("pending", (0, 1, 7))
@pytest.mark.parametrize("selection", ("initial", "same-time", "later-time"))
def test_postgres_due_selection_uses_its_partial_index_with_generic_plans(
    database_url, tmp_path, record_property, pending, selection
):
    with _due_catalog(tmp_path, 1000, pending, database_url=database_url) as (case, expected):
        if selection == "later-time" and expected:
            with session_scope(case.factory) as session:
                session.execute(
                    update(Upload)
                    .where(Upload.collection_id.in_(expected[1:]))
                    .values(archive_next_attempt_at="2026-11-08T12:00:00Z")
                )
        cursor = (_DUE, expected[0] if expected else 1)
        statement = finalization_due_query(
            _NOW,
            limit=2,
            at=cursor if selection == "same-time" else None,
            after_time=_DUE if selection == "later-time" else None,
        )
        compiled = statement.compile(dialect=postgresql.dialect(paramstyle="numeric_dollar"))
        values = tuple(compiled.params[name] for name in compiled.positiontup)
        with case.factory.kw["bind"].begin() as connection:
            connection.exec_driver_sql("ANALYZE collection_uploads")
            connection.exec_driver_sql("SET LOCAL plan_cache_mode = force_generic_plan")
            connection.exec_driver_sql("PREPARE finalization_due_hints AS " + str(compiled))
            execute = (
                pgsql.SQL("EXECUTE finalization_due_hints(")
                + pgsql.SQL(", ").join(pgsql.Literal(value) for value in values)
                + pgsql.SQL(")")
            ).as_string(connection.connection.driver_connection)
            plan = connection.exec_driver_sql(
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + execute
            ).scalar_one()[0]["Plan"]
            rows = connection.exec_driver_sql(execute).all()
            connection.exec_driver_sql("DEALLOCATE finalization_due_hints")

        def walk(node):
            yield node
            for child in node.get("Plans", ()):
                yield from walk(child)

        nodes = list(walk(plan))
        selected = [
            node
            for node in nodes
            if node.get("Index Name") == "ix_collection_uploads_finalization_due"
        ]
        assert len(selected) == 1, plan
        assert not any(node["Node Type"] == "Sort" for node in nodes), plan
        assert selected[0].get("Rows Removed by Filter", 0) == 0, plan
        wanted = expected[1:] if selection == "same-time" and expected else expected
        if selection == "later-time":
            wanted = expected[1:]
        assert [row.collection_id for row in rows] == wanted[:2]
        assert selected[0]["Actual Rows"] == min(len(wanted), 2), plan
        record_property("inactive_parents", 1000)
        record_property("future_parents", 1000)
        record_property("due_parents", pending)
        record_property("selection", selection)
        record_property("index_rows", selected[0]["Actual Rows"])
