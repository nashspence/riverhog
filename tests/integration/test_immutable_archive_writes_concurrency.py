from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_authority_checkpoint import _authority_source
from tests.unit.test_immutable_archive_writes import _original_pair

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("mode", ["direct", "http"])
def test_postgres_original_root_recovery_actors_keep_results_across_report_reply_loss(
    database_url, tmp_path, monkeypatch, mode
):
    with _authority_source(tmp_path, monkeypatch, mode, database_url=database_url) as fixture:
        _original_pair(fixture, monkeypatch)
