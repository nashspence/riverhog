from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_payload_continuation import _two_units
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


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("unknown", (False, True))
def test_postgres_unit_continuation_retains_original_cache_begin_and_separate_journals(
    database_url, tmp_path, monkeypatch, mode, unknown
):
    _two_units(tmp_path, monkeypatch, mode, unknown, database_url=database_url)


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
def test_postgres_terminal_receives_continuation_checkpoint_without_redispatch(
    database_url, tmp_path, monkeypatch, mode
):
    _two_units(tmp_path, monkeypatch, mode, False, database_url=database_url, lose_checkpoint=True)


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("refusal", ("guard", "capacity"))
def test_postgres_continuation_checks_current_guard_and_capacity_before_payload_dispatch(
    database_url, tmp_path, monkeypatch, mode, refusal
):
    _two_units(tmp_path, monkeypatch, mode, False, database_url=database_url, refuse_next=refusal)
