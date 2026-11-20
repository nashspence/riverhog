from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_upload_discard_projection_composition import _compose
from tests.unit.test_archive_upload_discard_result_projection import _discard

pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "revoked,committed", ((False, None), (True, None), (False, False), (False, True))
)
def test_postgres_same_original_actor_prepares_complete_discard_result(
    database_url, tmp_path, monkeypatch, revoked, committed
):
    with _discard(tmp_path, database_url=database_url) as ready:
        _compose(ready, monkeypatch, revoked=revoked, committed=committed)
