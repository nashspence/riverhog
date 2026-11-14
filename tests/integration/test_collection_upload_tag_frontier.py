from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_collection_upload_tag_frontier import _qualify
from tests.unit.test_immutable_archive_preparation import _metadata

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("work", ("expand", "publish"))
def test_postgres_pending_initial_tag_frontier_traverses_ordered_partial_indexes(
    database_url, tmp_path, monkeypatch, record_property, work
):
    with _metadata(tmp_path, monkeypatch, database_url=database_url) as f:
        observations = _qualify(f.actor.case.factory, f.actor.case.collection_id, work)
        record_property("tag_frontier_observations", observations)
