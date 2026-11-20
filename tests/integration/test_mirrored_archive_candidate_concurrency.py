from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_mirrored_archive_begin import (
    test_confirmed_data_refusal_tries_the_next_original_cache_candidate as _next_candidate,
)
from tests.unit.test_mirrored_archive_begin import (
    test_uncertain_cache_admission_never_tries_another_candidate as _unknown_admission,
)

pytestmark = pytest.mark.integration


def test_postgres_confirmed_capacity_refusal_selects_the_next_cache(
    tmp_path, monkeypatch, database_url
):
    _next_candidate(tmp_path, monkeypatch, "direct", database_url=database_url)


@pytest.mark.parametrize("committed", (False, True))
@pytest.mark.parametrize("returned", (False, True))
def test_postgres_uncertain_admission_retains_the_original_cache_selection(
    tmp_path, monkeypatch, database_url, committed, returned
):
    _unknown_admission(tmp_path, monkeypatch, committed, returned, database_url=database_url)
