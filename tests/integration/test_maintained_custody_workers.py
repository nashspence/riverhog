from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_maintained_custody_workers import (
    test_blocked_custody_provider_keeps_independent_construction_progressing as _independent,
)
from tests.unit.test_maintained_custody_workers import (
    test_custody_defers_real_parent_writer_contention as _contention,
)
from tests.unit.test_maintained_custody_workers import (
    test_pending_custody_constructor_keeps_its_original_member_cursor as _constructor,
)

pytestmark = pytest.mark.integration


def test_postgres_custody_defers_parent_writer_contention(tmp_path, database_url):
    _contention(tmp_path, database_url=database_url)


def test_postgres_pending_custody_constructor_retains_its_cursor(
    tmp_path, monkeypatch, database_url
):
    _constructor(tmp_path, monkeypatch, database_url=database_url)


def test_postgres_blocked_custody_provider_allows_independent_progress(
    tmp_path, monkeypatch, database_url
):
    _independent(tmp_path, monkeypatch, database_url=database_url)
