from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_finalizer_work import _actual_service_retry

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("kind", ("description", "journal"))
@pytest.mark.parametrize("committed", (False, True))
@pytest.mark.parametrize("mode", ("direct", "http"))
def test_postgres_service_retains_original_finalizer_through_report_ack_loss(
    tmp_path, monkeypatch, database_url, kind, committed, mode
):
    _actual_service_retry(tmp_path, monkeypatch, kind, committed, mode, database_url=database_url)
