from __future__ import annotations

from contextlib import contextmanager

import pytest
from riverhog_core.catalog_archive_write_models import ArchiveWriteEffectRecord
from riverhog_core.catalog_db import session_scope
from riverhog_core.services import immutable_archive_admission as admission_module
from riverhog_core.services.immutable_archive_receiving import OriginalImmutableArchiveReport
from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy import select

from tests.unit.test_archive_immutable_source_receiving import _original_report
from tests.unit.test_archive_volume_metadata_receiving import _count, _reported
from tests.unit.test_immutable_archive_receiving import _closed_source


@contextmanager
def _lose_reply(monkeypatch, owner, phase, committed):
    name = "receive_settled" if phase == "receive" else "acknowledge_settled"
    original = getattr(owner._admission, name)
    actual_scope = admission_module.session_scope

    @contextmanager
    def lose(factory):
        with actual_scope(factory) as session:
            yield session
            if not committed:
                raise OSError("synthetic original report acknowledgement")
        raise OSError("synthetic original report acknowledgement")

    def fail(*args, **kwargs):
        with monkeypatch.context() as patch:
            patch.setattr(admission_module, "session_scope", lose)
            return original(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(owner._admission, name, fail)
        yield


@pytest.mark.parametrize("kind", ("volume", "root", "recovery"))
@pytest.mark.parametrize("mode", ("direct", "http"))
@pytest.mark.parametrize("phase", ("receive", "retire"))
@pytest.mark.parametrize("committed", (False, True))
def test_original_report_retry_keeps_exact_source_and_closed_outcome(
    tmp_path, monkeypatch, kind, mode, phase, committed
):
    _assert_original_report(tmp_path, monkeypatch, kind, mode, phase, committed)


def _assert_original_report(
    tmp_path, monkeypatch, kind, mode, phase, committed, *, database_url=None
):
    with _original_report(
        tmp_path, monkeypatch, kind, mode, receive=False, database_url=database_url
    ) as (fixture, source, admission, admitted, receipt):
        case = fixture.case
        owner = OriginalImmutableArchiveReport(case.factory, case.collection_id)
        with _lose_reply(monkeypatch, owner, phase, committed):
            with pytest.raises(OSError, match="original report acknowledgement"):
                owner.advance()
        selected = owner._selected
        assert selected.call_id == admitted.ticket.call_id
        assert owner.has_pending
        assert _count(case) == (0 if phase == "retire" and committed else 1)

        def fresh(*args):
            pytest.fail("an original report retry must not select another closed call")

        monkeypatch.setattr(owner._admission, "load_next_collection_report", fresh)
        assert owner.advance() is True and not owner.has_pending
        assert owner._selected is selected and _count(case) == 0
        assert owner.advance() is True and _count(case) == 0
        with session_scope(case.factory) as session:
            source.verify_prepared_receipt(session, admitted.prepared, receipt)


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_lost_retirement_reply_does_not_consume_a_later_report(tmp_path, monkeypatch, mode):
    with _original_report(tmp_path, monkeypatch, "volume", mode, receive=False) as (
        fixture,
        source,
        admission,
        admitted,
        receipt,
    ):
        case = fixture.case
        owner = OriginalImmutableArchiveReport(case.factory, case.collection_id)
        with _lose_reply(monkeypatch, owner, "retire", True):
            with pytest.raises(OSError):
                owner.advance()
        assert owner.has_pending and _count(case) == 0
        later, _, later_admitted, later_receipt = _reported(fixture, 1, receive=False)
        assert _count(case) == 1
        assert owner.advance() is True and not owner.has_pending
        assert _count(case) == 1
        with session_scope(case.factory) as session:
            row = session.scalar(select(ArchiveWriteEffectRecord))
            assert row.id == later_admitted.ticket.call_id
            source.verify_prepared_receipt(session, admitted.prepared, receipt)
        replacement = OriginalImmutableArchiveReport(case.factory, case.collection_id)
        assert replacement.advance() is True and _count(case) == 0
        with session_scope(case.factory) as session:
            later.verify_prepared_receipt(session, later_admitted.prepared, later_receipt)


@pytest.mark.parametrize("committed", (False, True))
def test_original_unused_report_retains_its_exact_never_started_result(
    tmp_path, monkeypatch, committed
):
    with _closed_source(tmp_path, monkeypatch, "unused") as source:
        owner = OriginalImmutableArchiveReport(source.factory, source.prepared.collection_id)
        with _lose_reply(monkeypatch, owner, "retire", committed):
            with pytest.raises(OSError):
                owner.advance()
        selected = owner._selected
        assert selected.receipt is None and owner.has_pending
        assert owner.advance() is True and owner._selected is selected
        assert not owner.has_pending
        with session_scope(source.factory) as session:
            assert session.scalar(select(ArchiveWriteEffectRecord)) is None


def test_unknown_execution_is_never_a_report_or_a_closure_fact(tmp_path, monkeypatch):
    with _closed_source(tmp_path, monkeypatch, "positive-unknown") as source:
        owner = OriginalImmutableArchiveReport(source.factory, source.prepared.collection_id)
        assert owner.advance() is False and not owner.has_pending
        with session_scope(source.factory) as session:
            row = session.scalar(select(ArchiveWriteEffectRecord))
            assert row is not None and row.unresolved


@pytest.mark.parametrize("outcome", ("executed", "observed"))
def test_unsupported_positive_source_stays_original_and_charged(tmp_path, monkeypatch, outcome):
    with _closed_source(tmp_path, monkeypatch, outcome) as source:
        owner = OriginalImmutableArchiveReport(source.factory, source.prepared.collection_id)
        with pytest.raises(Conflict, match="no source receiver"):
            owner.advance()
        selected = owner._selected
        assert owner.has_pending
        with pytest.raises(Conflict, match="no source receiver"):
            owner.advance()
        assert owner.has_pending and owner._selected is selected
        with session_scope(source.factory) as session:
            assert session.scalar(select(ArchiveWriteEffectRecord)) is not None


def test_missing_receiver_cannot_retire_a_still_present_original_effect(tmp_path, monkeypatch):
    with _original_report(tmp_path, monkeypatch, "volume", receive=False) as (
        fixture,
        source,
        admission,
        admitted,
        receipt,
    ):
        owner = OriginalImmutableArchiveReport(fixture.case.factory, fixture.case.collection_id)

        def missing(*args, **kwargs):
            raise NotFound("synthetic unavailable receiver")

        monkeypatch.setattr(owner._admission, "receive_settled", missing)
        with pytest.raises(NotFound, match="unavailable receiver"):
            owner.advance()
        assert owner.has_pending and _count(fixture.case) == 1
