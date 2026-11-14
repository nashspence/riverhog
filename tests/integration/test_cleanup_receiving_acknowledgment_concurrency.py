from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_cleanup_receiving_acknowledgment import (
    _assert_lost_receiving_reply,
    _receiving_case,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("owner", ("archive", "cache"))
@pytest.mark.parametrize("kind", ("inventory", "dispose"))
@pytest.mark.parametrize("phase", ("record", "acknowledge"))
@pytest.mark.parametrize("committed", (False, True))
def test_postgres_original_cleanup_receiving_survives_commit_and_reply_loss(
    database_url, tmp_path, monkeypatch, owner, kind, phase, committed
):
    with _receiving_case(tmp_path, owner, database_url=database_url) as ready:
        _assert_lost_receiving_reply(ready, monkeypatch, owner, kind, phase, committed)
