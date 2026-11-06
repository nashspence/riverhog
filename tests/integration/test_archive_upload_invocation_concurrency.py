from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_upload_invocations import _actual_begin_receiving

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("mode", ("direct", "owned-http"))
def test_postgres_actual_upload_invocation_retains_original_begin_and_terminal_receiving(
    database_url,
    tmp_path,
    monkeypatch,
    kind,
    mode,
):
    _actual_begin_receiving(
        tmp_path,
        monkeypatch,
        kind,
        mode,
        database_url=database_url,
    )
