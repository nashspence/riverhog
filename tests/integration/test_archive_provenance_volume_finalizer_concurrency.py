from __future__ import annotations

import pytest

from tests.integration.finalization_claim_fixtures import (
    assert_replaced_claim_refused_after_parent_wait,
)
from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_provenance_volume_checkpoint import _volume_source
from tests.unit.test_archive_provenance_volume_finalizer import (
    _cold_and_live,
    _owner,
    _prior,
    _two_volumes,
)
from tests.unit.test_archive_volume_metadata_receiving import _count
from tests.unit.test_finalization_claim_readiness import _current_claim

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_postgres_actual_retained_corpus_finalizer_covers_binding_and_journal_sequences(
    database_url, tmp_path, monkeypatch, mode
):
    with _volume_source(tmp_path, monkeypatch, mode, database_url=database_url) as f:
        _two_volumes(f)


@pytest.mark.parametrize("phase", ("bindings", "journal", "metadata"))
def test_postgres_live_corpus_finalizer_receives_original_after_cold_retirement(
    database_url, tmp_path, monkeypatch, phase
):
    with _volume_source(
        tmp_path,
        monkeypatch,
        kind="journal" if phase == "journal" else "bindings",
        database_url=database_url,
    ) as f:
        _cold_and_live(f, monkeypatch, phase)


@pytest.mark.parametrize("phase", ("bindings", "journal", "metadata"))
def test_postgres_provenance_volume_rechecks_original_claim_after_parent_wait(
    database_url, tmp_path, monkeypatch, phase
):
    with _volume_source(
        tmp_path,
        monkeypatch,
        kind="journal" if phase == "journal" else "bindings",
        database_url=database_url,
    ) as f:
        claim = _current_claim(f.case)
        original = _owner(f, forward_guard=claim.guard)
        _prior(original.owner, phase)
        calls = dict(f.calls)
        assert_replaced_claim_refused_after_parent_wait(f.case, claim, original.owner.advance)
        assert not original.owner.has_pending and _count(f.case) == 0 and f.calls == calls
