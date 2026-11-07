from __future__ import annotations

from contextlib import contextmanager

import pytest
from riverhog_core.catalog_archive_write_models import ArchiveWriteEffectRecord
from riverhog_core.catalog_db import session_scope
from riverhog_core.services.immutable_archive_receiving import drain_original_immutable_report
from riverhog_protocol.errors import Conflict
from sqlalchemy import select

from tests.unit.test_archive_authority_checkpoint import _authority_source, _source, _write
from tests.unit.test_archive_volume_metadata_receiving import _count, _metadata_source, _reported
from tests.unit.test_finalization_claim_readiness import _fresh_coordinator
from tests.unit.test_immutable_archive_receiving import _closed_source


@contextmanager
def _original_report(
    tmp_path, monkeypatch, kind, mode="direct", receive=False, *, database_url=None
):
    context = _metadata_source if kind == "volume" else _authority_source
    with context(tmp_path, monkeypatch, mode=mode, database_url=database_url) as fixture:
        if kind == "volume":
            source, owner, admitted, receipt = _reported(fixture, 0, receive=receive)
        else:
            if kind == "recovery":
                root = _source(fixture, "root")
                owner, admitted, _ = _write(fixture, root)
                owner.acknowledge_report(admitted, verify_receipt=root.verify_prepared_receipt)
            source = _source(fixture, kind)
            owner, admitted, receipt = _write(fixture, source, receive=receive)
        yield fixture, source, owner, admitted, receipt


@pytest.mark.parametrize("kind", ["volume", "root", "recovery"])
@pytest.mark.parametrize("mode", ["direct", "http"])
@pytest.mark.parametrize("receive", [False, True])
def test_maintained_coordinator_routes_original_closed_report_to_exact_source(
    tmp_path, monkeypatch, kind, mode, receive
):
    with _original_report(tmp_path, monkeypatch, kind, mode, receive) as original:
        fixture, source, owner, admitted, receipt = original
        assert _count(fixture.case) == 1
        cold = _fresh_coordinator(fixture.case)
        assert cold._drain_archive_immutable_report(fixture.case.collection_id)
        assert _count(fixture.case) == 0
        assert not cold._drain_archive_immutable_report(fixture.case.collection_id)
        with session_scope(fixture.case.factory) as session:
            source.verify_prepared_receipt(session, admitted.prepared, receipt)


@pytest.mark.parametrize("outcome", ["executed", "observed"])
def test_unknown_source_format_keeps_original_positive_report_charged(
    tmp_path, monkeypatch, outcome
):
    with _closed_source(tmp_path, monkeypatch, outcome) as source:
        with session_scope(source.factory) as session:
            before = session.scalar(select(ArchiveWriteEffectRecord)).result_json
        with pytest.raises(Conflict, match="no source receiver"):
            drain_original_immutable_report(source.factory, source.prepared.collection_id)
        with session_scope(source.factory) as session:
            record = session.scalar(select(ArchiveWriteEffectRecord))
            assert record is not None and record.result_json == before


def test_original_unused_proof_needs_no_fabricated_positive_format_receiver(tmp_path, monkeypatch):
    with _closed_source(tmp_path, monkeypatch, "unused") as source:
        assert drain_original_immutable_report(source.factory, source.prepared.collection_id)
        with session_scope(source.factory) as session:
            assert session.scalar(select(ArchiveWriteEffectRecord)) is None


def test_unknown_execution_is_not_a_closed_report_for_any_source_router(tmp_path, monkeypatch):
    with _closed_source(tmp_path, monkeypatch, "positive-unknown") as source:
        assert not drain_original_immutable_report(source.factory, source.prepared.collection_id)
        with session_scope(source.factory) as session:
            record = session.scalar(select(ArchiveWriteEffectRecord))
            assert record is not None and record.unresolved
