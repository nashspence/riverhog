from __future__ import annotations

import pytest
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord as Control
from riverhog_core.catalog_models import CollectionUploadRecord as Upload

from tests.integration.test_archive_finalizer_work import _registration_case
from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_finalizer_retirement import (
    _assert_atomic_retirement,
    _assert_changed_retirement,
    _assert_closed_retirement,
    _assert_contended_retirement,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("table", (Upload, Control))
def test_postgres_contended_parent_or_control_keeps_the_original_idle_owner(
    tmp_path, monkeypatch, database_url, table
):
    with _registration_case(tmp_path, monkeypatch, database_url) as (case, claim, owner):
        _assert_contended_retirement(case, table)


def test_postgres_closed_gate_prevents_retirement_before_local_fencing(
    tmp_path, monkeypatch, database_url
):
    with _registration_case(tmp_path, monkeypatch, database_url) as (case, claim, owner):
        _assert_closed_retirement(case)


def test_postgres_parent_lock_covers_idle_retirement_before_terminal_selection(
    tmp_path, monkeypatch, database_url
):
    with _registration_case(tmp_path, monkeypatch, database_url) as (case, claim, owner):
        _assert_atomic_retirement(case, monkeypatch)


def test_postgres_retirement_preserves_a_different_original_construction(
    tmp_path, monkeypatch, database_url
):
    with _registration_case(tmp_path, monkeypatch, database_url) as (case, claim, owner):
        _assert_changed_retirement(case)
