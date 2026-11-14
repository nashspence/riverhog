from __future__ import annotations

import pytest

from tests.integration.finalization_claim_fixtures import (
    assert_replaced_claim_refused_after_parent_wait,
)
from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_provenance_authority_checkpoint import _authority_source
from tests.unit.test_archive_provenance_authority_finalizer import (
    _cold_and_live,
    _complete_owner,
    _owner,
)
from tests.unit.test_archive_volume_metadata_receiving import _count
from tests.unit.test_finalization_claim_readiness import _current_claim

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("role", ("terminal", "root"))
@pytest.mark.parametrize("mode", ("direct", "http"))
def test_postgres_actual_retained_authority_finalizer_completes_original_steps(
    database_url, tmp_path, monkeypatch, role, mode
):
    with _authority_source(tmp_path, monkeypatch, role, mode, database_url=database_url) as f:
        _complete_owner(f)


@pytest.mark.parametrize("role", ("terminal", "root"))
def test_postgres_original_authority_receiving_converges_between_live_and_cold_owners(
    database_url, tmp_path, monkeypatch, role
):
    with _authority_source(tmp_path, monkeypatch, role, database_url=database_url) as f:
        _cold_and_live(f, monkeypatch)


@pytest.mark.parametrize("role", ("terminal", "root"))
def test_postgres_provenance_authority_rechecks_original_claim_after_parent_wait(
    database_url, tmp_path, monkeypatch, role
):
    with _authority_source(tmp_path, monkeypatch, role, database_url=database_url) as f:
        claim = _current_claim(f.case)
        original = _owner(f, forward_guard=claim.guard)
        calls = dict(f.calls)
        assert_replaced_claim_refused_after_parent_wait(f.case, claim, original.owner.advance)
        assert not original.owner.has_pending and _count(f.case) == 0 and f.calls == calls
