from __future__ import annotations

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.services.immutable_archive_receiving import drain_original_immutable_report

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_immutable_source_receiving import _original_report
from tests.unit.test_archive_volume_metadata_receiving import _count

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("kind", ["volume", "root", "recovery"])
def test_postgres_cold_source_router_receives_original_closed_fact(
    database_url, tmp_path, monkeypatch, kind
):
    with _original_report(tmp_path, monkeypatch, kind, database_url=database_url) as original:
        fixture, source, owner, admitted, receipt = original
        assert drain_original_immutable_report(fixture.case.factory, fixture.case.collection_id)
        assert _count(fixture.case) == 0
        with session_scope(fixture.case.factory) as session:
            source.verify_prepared_receipt(session, admitted.prepared, receipt)


@pytest.mark.parametrize("kind", ("volume", "root", "recovery"))
@pytest.mark.parametrize("phase", ("receive", "retire"))
@pytest.mark.parametrize("committed", (False, True))
def test_postgres_original_report_preserves_source_through_lost_acknowledgement(
    database_url, tmp_path, monkeypatch, kind, phase, committed
):
    from tests.unit.test_original_immutable_archive_report import _assert_original_report

    _assert_original_report(
        tmp_path, monkeypatch, kind, "direct", phase, committed, database_url=database_url
    )
