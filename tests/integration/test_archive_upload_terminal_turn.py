from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_upload_terminal_turn import (
    _bounded_turn,
    _execution_scope,
    _lost_decision_reply,
    _revoked_between_stages,
    _turn_case,
)

pytestmark = pytest.mark.integration


def test_postgres_original_execution_composes_bounded_decision_and_retirement(
    database_url, tmp_path
):
    with _turn_case(tmp_path, database_url=database_url, originals=3) as case:
        _bounded_turn(case)


def test_postgres_terminal_execution_preserves_originals_after_lost_decision_reply(
    database_url, tmp_path
):
    with _turn_case(tmp_path, database_url=database_url) as case:
        _lost_decision_reply(case)


def test_postgres_terminal_execution_rechecks_revoked_authority_between_stages(
    database_url, tmp_path, monkeypatch
):
    with _turn_case(tmp_path, database_url=database_url) as case:
        _revoked_between_stages(case, monkeypatch)


def test_postgres_actual_terminal_worker_uses_operation_scope(database_url, tmp_path, monkeypatch):
    with _turn_case(tmp_path, database_url=database_url) as case:
        _execution_scope(case, monkeypatch)
