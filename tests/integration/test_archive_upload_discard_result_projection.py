from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_upload_discard_result_projection import _discard, _finish, _revoked

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("count", (0, 17))
def test_postgres_original_discard_result_resumes_complete_public_counts(
    database_url, tmp_path, count
):
    with _discard(tmp_path, database_url=database_url, count=count) as ready:
        total, _ = _finish(ready)
        assert total == count


def test_postgres_original_discard_result_keeps_current_authority_on_resume(database_url, tmp_path):
    with _discard(tmp_path, database_url=database_url) as ready:
        _revoked(ready)
