from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_finalizer_terminal_drain import (
    _busy,
    _late_result,
    _original,
    _unknown,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "kind",
    (
        "archive-volume",
        "archive-terminal",
        "description",
        "tag-node",
        "provenance-authority",
        "root",
        "recovery",
        "structure",
        "journal",
        "volume-bindings",
        "volume-journal",
        "volume-metadata",
    ),
)
def test_postgres_retained_finalizer_drains_original_reports_without_releasing_inputs(
    database_url, tmp_path, monkeypatch, kind
):
    with _original(tmp_path, monkeypatch, kind, database_url=database_url) as original:
        _late_result(original, monkeypatch)


@pytest.mark.parametrize("mode", ("direct", "http"))
@pytest.mark.parametrize("before_admission", (False, True))
def test_postgres_retained_finalizer_drain_observes_busy_workers_without_waiting(
    database_url, tmp_path, monkeypatch, mode, before_admission
):
    with _original(
        tmp_path, monkeypatch, "description", mode, database_url=database_url
    ) as original:
        _busy(original, monkeypatch, before_admission)


@pytest.mark.parametrize("returned", (False, True))
def test_postgres_retained_finalizer_drain_keeps_unknown_execution(
    database_url, tmp_path, monkeypatch, returned
):
    with _original(tmp_path, monkeypatch, "journal", "http", database_url=database_url) as original:
        _unknown(original, monkeypatch, returned)
