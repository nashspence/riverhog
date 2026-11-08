from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit import test_maintained_finalization_workers as checks

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("kind", ("segment", "pack"))
def test_postgres_maintained_workers_publish(tmp_path, monkeypatch, database_url, kind):
    checks.test_maintained_coordinator_publishes_through_joined_original_workers(
        tmp_path, monkeypatch, kind, database_url=database_url
    )


@pytest.mark.parametrize("committed", (False, True))
def test_postgres_maintained_original_receives_failed_report(
    tmp_path, monkeypatch, database_url, committed
):
    checks.test_maintained_failure_keeps_original_claim_and_receives_exact_report(
        tmp_path, monkeypatch, committed, database_url=database_url
    )


def test_postgres_maintained_wait_allows_independent_publication(
    tmp_path, monkeypatch, database_url
):
    checks.test_maintained_provider_wait_leaves_an_independent_collection_progressing(
        tmp_path, monkeypatch, database_url=database_url
    )


def test_postgres_maintained_parent_contention_defers(tmp_path, monkeypatch, database_url):
    checks.test_maintained_coordinator_defers_real_parent_writer_contention(
        tmp_path, monkeypatch, database_url=database_url
    )
