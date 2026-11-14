from __future__ import annotations

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import (
    CollectionUploadCopyIntentRecord as Intent,
)
from riverhog_core.catalog_models import (
    CollectionUploadRecord as Upload,
)
from riverhog_core.catalog_models import (
    StorageIncarnationRecord as Incarnation,
)
from sqlalchemy import select

from tests.unit.test_archive_upload_registration_cleanup import _prepared


def _copy_prefix(ready, count):
    now = "2026-01-01T00:00:00.000000000Z"
    with session_scope(ready.case.factory) as session:
        source = session.get(Upload, ready.case.collection_id).archive_store
        names = [f"target{n:05d}" for n in range(count + 1)]
        session.add_all(
            Incarnation(
                id=ready.case.incarnation,
                kind="archive",
                name=name,
                state="bound",
                created_at=now,
            )
            for name in names
        )
        session.flush()
        session.add_all(
            Intent(
                collection_id=ready.case.collection_id,
                destination_store=name,
                destination_incarnation_id=ready.case.incarnation,
                source_store=source,
                source_incarnation_id=ready.case.incarnation,
                initiated_by_app=ready.case.principal.id,
                initiated_by_key_id=ready.case.principal.key_id,
                use_cache=False,
                state="accepted" if n == count else "canceled",
                accepted_at=now,
                attempts=0,
            )
            for n, name in enumerate(names)
        )
        session.add_all(
            Intent(
                collection_id=ready.case.collection_id + 1000000 + n,
                destination_store=name,
                destination_incarnation_id=ready.case.incarnation,
                source_store=source,
                source_incarnation_id=ready.case.incarnation,
                initiated_by_app=ready.case.principal.id,
                initiated_by_key_id=ready.case.principal.key_id,
                use_cache=False,
                state="accepted",
                accepted_at=now,
                attempts=0,
            )
            for n, name in enumerate(names)
        )
    return names[-1]


@pytest.mark.parametrize("retired", (17, 129, 1025))
def test_actual_copy_intent_first_live_seek_skips_retired_and_unrelated_prefixes(
    tmp_path, monkeypatch, retired, record_property
):
    with _prepared(tmp_path, monkeypatch) as ready:
        expected = _copy_prefix(ready, retired)
        engine = ready.case.factory.kw["bind"]
        query = (
            select(Intent.destination_store)
            .where(
                Intent.collection_id == ready.case.collection_id,
                Intent.state == "accepted",
            )
            .order_by(Intent.destination_store)
            .limit(2)
        )
        with engine.connect() as connection:
            plan = connection.exec_driver_sql(
                "EXPLAIN QUERY PLAN SELECT destination_store FROM collection_upload_copy_intents "
                "WHERE collection_id=? AND state='accepted' ORDER BY destination_store LIMIT 2",
                (ready.case.collection_id,),
            ).all()
            details = [row[3] for row in plan]
            assert any(
                "ix_collection_upload_copy_intents_terminal_cleanup" in row for row in details
            )
            assert all("TEMP B-TREE" not in row for row in details)
            raw = connection.connection.driver_connection
            steps = [0]

            def progress():
                steps[0] += 1
                return 0

            raw.set_progress_handler(progress, 1)
            try:
                result = connection.execute(query).scalars().all()
                other_live = (
                    connection.execute(
                        select(Intent.destination_store)
                        .where(
                            Intent.collection_id == ready.case.collection_id,
                            Intent.state.in_(("pending", "handed_off", "failed")),
                        )
                        .limit(1)
                    )
                    .scalars()
                    .all()
                )
            finally:
                raw.set_progress_handler(None, 0)
        assert result == [expected] and other_live == [] and steps[0] <= 300
        record_property("retired_copy_intents", retired)
        record_property("unrelated_copy_intents", retired + 1)
        record_property("copy_intent_seek_vm_steps", steps[0])
