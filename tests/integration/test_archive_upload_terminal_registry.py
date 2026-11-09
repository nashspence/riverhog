from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_upload_terminal_registry import (
    _bounded_original,
    _joined_receiving,
    _unknown_constructor,
)
from tests.unit.test_archive_upload_terminal_turn import _turn_case

pytestmark = pytest.mark.integration


def test_postgres_registry_retains_one_original_through_bounded_turns(
    database_url, tmp_path, monkeypatch
):
    with _turn_case(tmp_path, database_url=database_url, originals=3) as case:
        _bounded_original(case, monkeypatch)


def test_postgres_registry_receives_actual_worker_after_lost_start_acknowledgement(
    database_url, tmp_path, monkeypatch
):
    with _turn_case(tmp_path, database_url=database_url) as case:
        _joined_receiving(case, monkeypatch, lost_start=True)


def test_postgres_registry_never_reconstructs_an_unknown_original(
    database_url, tmp_path, monkeypatch
):
    with _turn_case(tmp_path, database_url=database_url, originals=0) as case:
        _unknown_constructor(case, monkeypatch, OSError)


def test_postgres_registry_resumes_original_after_later_thread_nonentry(
    database_url, tmp_path, monkeypatch
):
    from tests.unit.test_archive_upload_terminal_registry import _resume_start_failure

    with _turn_case(tmp_path, database_url=database_url) as case:
        _resume_start_failure(case, monkeypatch)
