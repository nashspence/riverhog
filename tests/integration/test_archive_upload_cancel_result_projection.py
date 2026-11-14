from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_upload_cancel_result_projection import (
    _cancel,
    _finish,
    _handoff_preserved,
    _revoked,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("count,copies", ((0, 0), (17, 17)))
def test_postgres_original_cancel_result_resumes_exact_rich_response(
    database_url, tmp_path, count, copies
):
    with _cancel(tmp_path, database_url=database_url, count=count, copies=copies) as ready:
        _finish(ready)


def test_postgres_original_cancel_result_rechecks_current_rights(database_url, tmp_path):
    with _cancel(tmp_path, database_url=database_url) as ready:
        _revoked(ready)


def test_postgres_original_cancel_result_preserves_independent_handed_off_job(
    database_url, tmp_path
):
    with _cancel(tmp_path, database_url=database_url, copies=3) as ready:
        _handoff_preserved(ready)
