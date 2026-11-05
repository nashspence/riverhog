from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_provenance_volume_checkpoint import _volume_source
from tests.unit.test_archive_provenance_volume_finalizer import _cold_and_live, _two_volumes

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_postgres_actual_retained_corpus_finalizer_covers_binding_and_journal_sequences(
    database_url, tmp_path, monkeypatch, mode
):
    with _volume_source(tmp_path, monkeypatch, mode, database_url=database_url) as f:
        _two_volumes(f)


@pytest.mark.parametrize("phase", ("bindings", "journal", "metadata"))
def test_postgres_live_corpus_finalizer_receives_original_after_cold_retirement(
    database_url, tmp_path, monkeypatch, phase
):
    with _volume_source(
        tmp_path,
        monkeypatch,
        kind="journal" if phase == "journal" else "bindings",
        database_url=database_url,
    ) as f:
        _cold_and_live(f, monkeypatch, phase)
