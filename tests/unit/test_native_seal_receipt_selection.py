from __future__ import annotations

from contextlib import contextmanager

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord as Volume
from riverhog_core.services.archive_native_seal_receiving import native_seal_receipts_query
from sqlalchemy import select, update

from tests.unit.test_collection_upload_preparation import _case
from tests.unit.test_collection_upload_raw_readiness_work import sqlite_work


@contextmanager
def _pending_native_seals(tmp_path, prefix, pending, *, database_url=None):
    total = max(1, 2 * prefix + pending)
    with _case(tmp_path, "segment", artifact_count=total, database_url=database_url) as case:
        with session_scope(case.factory) as session:
            ids = list(
                session.scalars(
                    select(Volume.object_id)
                    .where(Volume.collection_id == case.collection_id)
                    .order_by(Volume.sequence)
                )
            )
            assert len(ids) == total
            # Synthetic selectors exercise catalog work only, not archive custody.
            session.execute(
                update(Volume)
                .where(Volume.collection_id == case.collection_id)
                .values(state="sealed", sealed_receipt_json="{}", checkpoint_json="selector")
            )
            if prefix:
                session.execute(
                    update(Volume)
                    .where(
                        Volume.collection_id == case.collection_id,
                        Volume.object_id.in_(ids[prefix : 2 * prefix]),
                    )
                    .values(state="planned", sealed_receipt_json=None)
                )
            selected = ids[2 * prefix : 2 * prefix + pending]
            if selected:
                session.execute(
                    update(Volume)
                    .where(
                        Volume.collection_id == case.collection_id,
                        Volume.object_id.in_(selected),
                    )
                    .values(sealed_receipt_json=None)
                )
        yield case, selected


@pytest.mark.parametrize("prefix", (0, 32, 1000))
@pytest.mark.parametrize("pending", (0, 1, 7))
def test_native_seal_selection_skips_completed_and_unsealed_prefixes(
    tmp_path, record_property, prefix, pending
):
    with _pending_native_seals(tmp_path, prefix, pending) as (case, expected):
        with sqlite_work(case.service) as work:
            with session_scope(case.factory) as session:
                rows = list(
                    session.execute(native_seal_receipts_query(case.collection_id, limit=2))
                )
        assert [row.object_id for row in rows] == expected[:2]
        assert work["vm_steps"] <= 300, (prefix, pending, work)
        record_property("completed_seal_prefix", prefix)
        record_property("unsealed_prefix", prefix)
        record_property("pending_receipts", pending)
        record_property("selected_rows", len(rows))
        record_property("sqlite_vm_steps", work["vm_steps"])


@pytest.mark.parametrize("pending", (1, 17, 129, 1000))
def test_native_seal_selections_have_linear_total_query_work(tmp_path, record_property, pending):
    with _pending_native_seals(tmp_path, pending, pending) as (case, expected):
        found = []
        query_steps = 0
        while True:
            with sqlite_work(case.service) as work:
                with session_scope(case.factory) as session:
                    rows = list(
                        session.execute(native_seal_receipts_query(case.collection_id, limit=16))
                    )
            query_steps += work["vm_steps"]
            assert len(rows) <= 16
            if not rows:
                break
            ids = [row.object_id for row in rows]
            found.extend(ids)
            # Fixture acknowledgement is separate from the measured selector.
            # These rows exercise no receipt validation or archive custody.
            with session_scope(case.factory) as session:
                session.execute(
                    update(Volume)
                    .where(
                        Volume.collection_id == case.collection_id,
                        Volume.object_id.in_(ids),
                    )
                    .values(sealed_receipt_json="{}")
                )
        assert found == expected
        assert len(set(found)) == pending
        assert query_steps <= 30 * pending + 500, (pending, query_steps)
        record_property("pending_receipts", pending)
        record_property("selected_rows", len(found))
        record_property("selection_sqlite_vm_steps", query_steps)


@pytest.mark.parametrize("prefix", (0, 32, 1000))
def test_native_source_preflight_and_capture_skip_irrelevant_prefixes(
    tmp_path, record_property, prefix
):
    from riverhog_core.services.archive_native_seal_receiving import NativeArchiveSealSource

    # These catalog selectors deliberately carry no valid native checkpoint or
    # archive custody; only source selection and byte admission run here.
    with _pending_native_seals(tmp_path, prefix, 1) as (case, expected):
        with sqlite_work(case.service) as work:
            source = NativeArchiveSealSource.load(
                case.factory, case.collection_id, case.service._config.archive_write
            )
        assert source is not None and source.object_id == expected[0]
        assert source.checkpoint_json == "selector"
        assert source.reserved_bytes <= case.service._config.archive_write.native_receipt_bytes
        assert work["vm_steps"] <= 512, (prefix, work)
        record_property("completed_seal_prefix", prefix)
        record_property("unsealed_prefix", prefix)
        record_property("native_source_sqlite_vm_steps", work["vm_steps"])
        record_property("native_source_reserved_bytes", source.reserved_bytes)
