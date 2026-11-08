from __future__ import annotations

from contextlib import contextmanager

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_finalizer_work import (
    _actual_feedback,
    _actual_gate_closed_feedback,
    _actual_service_retry,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("kind", ("description", "journal"))
@pytest.mark.parametrize("committed", (False, True))
@pytest.mark.parametrize("mode", ("direct", "http"))
def test_postgres_service_retains_original_finalizer_through_report_ack_loss(
    tmp_path, monkeypatch, database_url, kind, committed, mode
):
    _actual_service_retry(tmp_path, monkeypatch, kind, committed, mode, database_url=database_url)


@pytest.mark.parametrize("kind", ("description", "journal"))
@pytest.mark.parametrize("committed", (False, True))
@pytest.mark.parametrize("mode", ("direct", "http"))
@pytest.mark.parametrize("terminal", (False, True))
def test_postgres_original_finalizer_feedback_survives_commit_reply_loss(
    tmp_path, monkeypatch, database_url, kind, committed, mode, terminal
):
    _actual_feedback(
        tmp_path,
        monkeypatch,
        kind,
        mode,
        committed,
        terminal=terminal,
        database_url=database_url,
    )


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_postgres_terminal_selection_prevents_feedback_requeue(
    tmp_path, monkeypatch, database_url, mode
):
    _actual_gate_closed_feedback(tmp_path, monkeypatch, mode, database_url=database_url)


@pytest.mark.parametrize("mode", ("direct", "http"))
@pytest.mark.parametrize("committed", (False, True))
def test_postgres_original_feedback_cannot_requeue_another_claim(
    database_url, tmp_path, monkeypatch, mode, committed
):
    from tests.unit.test_archive_finalizer_work import _actual_claim_feedback

    _actual_claim_feedback(
        tmp_path, monkeypatch, "description", mode, committed, database_url=database_url
    )


@contextmanager
def _registration_case(tmp_path, monkeypatch, database_url):
    from riverhog_core.runtime_config import RuntimeConfig

    from tests.unit.test_archive_catalog_finalizer import _original

    configured = RuntimeConfig.for_testing

    def config(**values):
        return configured(journal_validation_workspace=tmp_path / "journal-validation", **values)

    with monkeypatch.context() as patch:
        patch.setattr(RuntimeConfig, "for_testing", staticmethod(config))
        with _original(
            tmp_path, monkeypatch, "artifact-tree", database_url=database_url
        ) as original:
            yield original


def test_postgres_contended_parent_defers_finalizer_registration(
    tmp_path, monkeypatch, database_url
):
    from tests.unit.test_archive_finalizer_registration import (
        _assert_registration_blocked_by_parent,
    )

    with _registration_case(tmp_path, monkeypatch, database_url) as (case, claim, owner):
        _assert_registration_blocked_by_parent(case, claim)


def test_postgres_closed_terminal_cannot_install_fresh_finalizer(
    tmp_path, monkeypatch, database_url
):
    from tests.unit.test_archive_finalizer_registration import _assert_closed_registration

    with _registration_case(tmp_path, monkeypatch, database_url) as (case, claim, owner):
        _assert_closed_registration(case, claim)
