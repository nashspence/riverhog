from __future__ import annotations

import pytest
from riverhog_core.catalog_models import CollectionUploadTagPublicationFrontierRecord as Frontier
from riverhog_core.services.collection_upload_tag_frontier import pending_initial_tag_query
from sqlalchemy import delete, insert, select, text

from tests.unit.test_immutable_archive_preparation import _metadata


def _completed(row):
    row.published = True
    row.object_path = "archives/fixture/tags/" + row.node_digest
    row.provider_revision = "original"
    row.stored_bytes = 123
    row.stored_sha256 = "a" * 64
    row.published_at = "2026-01-01T00:00:00Z"


def _rows(collection_id, count, work):
    for number in range(2 * count):
        row = {
            "collection_id": collection_id,
            "node_digest": f"{number:064x}",
            "expanded": number < count if work == "expand" else True,
            "published": work == "publish" and number < count,
            "object_path": None,
            "provider_revision": None,
            "stored_bytes": None,
            "stored_sha256": None,
            "published_at": None,
        }
        if row["published"]:
            row.update(
                object_path="archives/fixture/tags/" + row["node_digest"],
                provider_revision="original",
                stored_bytes=123,
                stored_sha256="a" * 64,
                published_at="2026-01-01T00:00:00Z",
            )
        yield row


def _plan(session, query):
    dialect = session.get_bind().dialect
    sql = str(query.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))
    if dialect.name == "sqlite":
        return [row[3] for row in session.execute(text("EXPLAIN QUERY PLAN " + sql))]
    plan = session.scalar(text("EXPLAIN (FORMAT JSON) " + sql))
    indices = []

    def walk(node):
        if "Index Name" in node:
            indices.append(node["Index Name"])
        for child in node.get("Plans", ()):
            walk(child)

    walk(plan[0]["Plan"])
    return indices


def _qualify(factory, collection_id, work):
    observations = []
    for count in (31, 127, 511):
        with factory() as session, session.begin():
            session.execute(delete(Frontier).where(Frontier.collection_id == collection_id))
            session.execute(insert(Frontier), list(_rows(collection_id, count, work)))
        with factory() as session, session.begin():
            session.execute(text("ANALYZE collection_upload_tag_publication_frontier"))
            query = pending_initial_tag_query(collection_id, work=work)
            expected = "ix_upload_tag_frontier_" + work + "_pending"
            plans = _plan(session, query)
            assert any(expected in name for name in plans), plans
            assert not any("TEMP B-TREE" in name for name in plans), plans
            driver = session.connection().connection.driver_connection
            steps = 0

            def tick():
                nonlocal steps
                steps += 1
                return 0

            sqlite = session.get_bind().dialect.name == "sqlite"
            if sqlite:
                driver.set_progress_handler(tick, 1)
            try:
                for number in range(count, count * 2):
                    frontier = session.scalar(query)
                    assert frontier is not None and frontier.node_digest == f"{number:064x}"
                    if work == "expand":
                        frontier.expanded = True
                    else:
                        _completed(frontier)
                    session.flush()
                assert session.scalar(query) is None
            finally:
                if sqlite:
                    driver.set_progress_handler(None, 0)
            if sqlite:
                assert steps > 0
            observations.append({"nodes": count, "sqlite_vm_steps": steps, "plan": plans})
            if work == "publish":
                receipt_query = (
                    select(Frontier)
                    .where(Frontier.collection_id == collection_id, Frontier.published.is_(True))
                    .order_by(Frontier.node_digest)
                    .limit(1)
                )
                receipt_plan = _plan(session, receipt_query)
                assert any("ix_upload_tag_frontier_receipts" in name for name in receipt_plan)
                assert session.scalar(receipt_query).node_digest == "0" * 64
    if observations[0]["sqlite_vm_steps"]:
        first, last = observations[0], observations[-1]
        # This counts VM instructions, not B-tree comparison or physical I/O costs.
        # Completed-prefix scans or repeated pending-frontier sorts grow quadratically.
        assert last["sqlite_vm_steps"] * first["nodes"] <= (
            first["sqlite_vm_steps"] * last["nodes"] * 2
        ), observations
    return observations


@pytest.mark.parametrize("work", ("expand", "publish"))
def test_actual_pending_tag_frontier_uses_ordered_indexes_across_completed_prefixes(
    tmp_path, monkeypatch, record_property, work
):
    with _metadata(tmp_path, monkeypatch) as f:
        observations = _qualify(f.actor.case.factory, f.actor.case.collection_id, work)
        record_property("tag_frontier_observations", observations)


def test_frontier_work_selection_is_explicit():
    with pytest.raises(ValueError, match="unsupported"):
        pending_initial_tag_query(1, work="other")
