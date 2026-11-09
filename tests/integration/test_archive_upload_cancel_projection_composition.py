from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_upload_cancel_projection_composition import _compose
from tests.unit.test_archive_upload_cancel_result_projection import _cancel

pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "revoked,committed", ((False, None), (True, None), (False, False), (False, True))
)
def test_postgres_same_original_actor_prepares_complete_cancel_result(
    database_url, tmp_path, monkeypatch, revoked, committed
):
    with _cancel(tmp_path, database_url=database_url, copies=3) as ready:
        _compose(ready, monkeypatch, revoked=revoked, committed=committed)


def test_postgres_same_original_actor_receives_sealed_refusal_without_disposing_upload(
    database_url,
    tmp_path,
    monkeypatch,
):
    from tests.unit.test_archive_upload_cancel_projection_composition import _sealed_refusal

    with _cancel(tmp_path, database_url=database_url, count=3, tags=0) as ready:
        _sealed_refusal(ready, monkeypatch)
