from __future__ import annotations

import pytest
from psycopg import sql as pgsql
from riverhog_core.services.archive_custody_due import (
    custody_member_query,
    custody_parent_query,
)
from sqlalchemy.dialects import postgresql

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_custody_due_selection import _due_catalog

pytestmark = pytest.mark.integration


def _index_probe(connection, statement, *, name, index):
    compiled = statement.compile(dialect=postgresql.dialect(paramstyle="numeric_dollar"))
    values = tuple(compiled.params[key] for key in compiled.positiontup)
    connection.exec_driver_sql("PREPARE " + name + " AS " + str(compiled))
    try:
        execute = (
            pgsql.SQL("EXECUTE ")
            + pgsql.Identifier(name)
            + pgsql.SQL("(")
            + pgsql.SQL(", ").join(pgsql.Literal(value) for value in values)
            + pgsql.SQL(")")
        ).as_string(connection.connection.driver_connection)
        plan = connection.exec_driver_sql(
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + execute
        ).scalar_one()[0]["Plan"]
        rows = connection.exec_driver_sql(execute).all()
    finally:
        connection.exec_driver_sql("DEALLOCATE " + name)

    def walk(node):
        yield node
        for child in node.get("Plans", ()):
            yield from walk(child)

    nodes = list(walk(plan))
    selected = [node for node in nodes if node.get("Index Name") == index]
    assert len(selected) == 1, plan
    assert not any(node["Node Type"] in {"Sort", "Seq Scan"} for node in nodes), plan
    assert selected[0].get("Rows Removed by Filter", 0) == 0, plan
    return rows, selected[0]["Actual Rows"]


@pytest.mark.parametrize("pending", (0, 1, 7))
@pytest.mark.parametrize("history", (False, True))
@pytest.mark.parametrize("after", (False, True))
def test_postgres_custody_parent_and_member_seeks_use_partial_generic_indexes(
    database_url, tmp_path, record_property, pending, history, after
):
    with _due_catalog(tmp_path, 1000, pending, history=history, database_url=database_url) as (
        case,
        expected,
    ):
        collection_id, members = expected[0]
        parent = custody_parent_query(limit=2, after=collection_id if after else None)
        member = custody_member_query(
            collection_id,
            requires_history=history,
            after=(members[0] if members else "0" * 64) if after else None,
        )
        with case.factory.kw["bind"].begin() as connection:
            connection.exec_driver_sql("ANALYZE archive_write_controls")
            connection.exec_driver_sql("ANALYZE collection_upload_artifacts")
            connection.exec_driver_sql("SET LOCAL plan_cache_mode = force_generic_plan")
            parents, parent_rows = _index_probe(
                connection,
                parent,
                name="custody_parents",
                index="ix_archive_write_controls_custody_due",
            )
            artifacts, member_rows = _index_probe(
                connection,
                member,
                name="custody_members",
                index=(
                    "ix_upload_members_custody_history"
                    if history
                    else "ix_upload_members_custody_primary"
                ),
            )
        assert [row.collection_id for row in parents] == (
            [collection_id] if pending and not after else []
        )
        wanted = members[1:] if after else members
        assert [row.artifact_id for row in artifacts] == wanted[:1]
        assert parent_rows == int(bool(pending) and not after)
        assert member_rows == min(len(wanted), 1)
        record_property("disjoint_prefix_members", 1000)
        record_property("ineligible_parents", 3000)
        record_property("requires_history", history)
        record_property("after_cursor", after)
        record_property("parent_index_rows", parent_rows)
        record_property("member_index_rows", member_rows)
