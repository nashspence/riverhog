from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_provenance_journal_checkpoint import _journal_source
from tests.unit.test_archive_provenance_journal_writer import _cold_and_live

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_postgres_actual_journal_writer_and_cold_receiving_converge_after_forward_loss(
    database_url, tmp_path, monkeypatch, mode
):
    with _journal_source(tmp_path, monkeypatch, mode, database_url=database_url) as fixture:
        _cold_and_live(fixture, monkeypatch)
