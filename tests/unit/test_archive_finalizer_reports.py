from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.services import immutable_archive_admission as admission_module
from riverhog_core.services.archive_authority_finalizer import ArchiveAuthorityFinalizer
from riverhog_core.services.archive_initial_description_finalizer import (
    ArchiveInitialDescriptionFinalizer,
)
from riverhog_core.services.archive_initial_tag_finalizer import ArchiveInitialTagFinalizer
from riverhog_core.services.archive_provenance_authority_finalizer import (
    ArchiveProvenanceAuthorityFinalizer,
)
from riverhog_core.services.archive_provenance_journal_writer import ArchiveProvenanceJournalWriter
from riverhog_core.services.archive_provenance_structure_writer import (
    ArchiveProvenanceStructureWriter,
)
from riverhog_core.services.archive_provenance_volume_finalizer import (
    ArchiveProvenanceVolumeFinalizer,
)
from riverhog_core.services.archive_volume_metadata_finalizer import ArchiveVolumeMetadataFinalizer
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from riverhog_protocol.errors import Conflict

from tests.unit.test_archive_finalizer_registry import _receive
from tests.unit.test_archive_immutable_source_receiving import _original_report
from tests.unit.test_archive_volume_metadata_receiving import _count, _reported

_OWNERS = {
    "archive-volume": ArchiveVolumeMetadataFinalizer,
    "root": ArchiveAuthorityFinalizer,
    "provenance-authority": ArchiveProvenanceAuthorityFinalizer,
    "provenance-volume": ArchiveProvenanceVolumeFinalizer,
    "tag": ArchiveInitialTagFinalizer,
    "description": ArchiveInitialDescriptionFinalizer,
    "structure": ArchiveProvenanceStructureWriter,
    "journal": ArchiveProvenanceJournalWriter,
}


def _owner(case, kind):
    kwargs = {}
    if kind in {"structure", "journal"}:
        kwargs["forward_guard"] = lambda session: pytest.fail("fresh semantic selection")
    return _OWNERS[kind](
        case.factory,
        collection_id=case.collection_id,
        config=None,
        archive_stores=None,
        **kwargs,
    )


def _advance(owner, kind):
    if kind == "structure":
        return owner.advance(selected_object_id="unselected")
    if kind == "journal":
        return owner.advance(journal_id="unselected", offset=0, size=1)
    return owner.advance()


@contextmanager
def _lose_report_reply(monkeypatch, phase, committed):
    cls = admission_module.ImmutableArchiveAdmission
    name = "receive_settled" if phase == "receive" else "acknowledge_settled"
    original = getattr(cls, name)
    scope = admission_module.session_scope

    @contextmanager
    def lose(factory):
        with scope(factory) as session:
            yield session
            if not committed:
                raise OSError("synthetic metadata report acknowledgement")
        raise OSError("synthetic metadata report acknowledgement")

    def failed(receiver, *args, **kwargs):
        with monkeypatch.context() as patch:
            patch.setattr(admission_module, "session_scope", lose)
            return original(receiver, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(cls, name, failed)
        yield


def _forbidden(*args, **kwargs):
    pytest.fail("a retained report must precede fresh selection")


@pytest.mark.parametrize("kind", tuple(_OWNERS))
@pytest.mark.parametrize(
    "phase,committed", (("receive", False), ("receive", True), ("retire", False), ("retire", True))
)
def test_metadata_owner_retains_report_reply_before_any_semantic_actor(
    tmp_path, monkeypatch, kind, phase, committed
):
    _assert_metadata_report(tmp_path, monkeypatch, kind, phase, committed)


def _assert_metadata_report(tmp_path, monkeypatch, kind, phase, committed, *, database_url=None):
    with _original_report(
        tmp_path, monkeypatch, "volume", receive=False, database_url=database_url
    ) as (fixture, source, admission, admitted, receipt):
        case = fixture.case
        owner = _owner(case, kind)
        assert not owner.has_pending and owner._actor is None
        with _lose_report_reply(monkeypatch, phase, committed):
            with pytest.raises(OSError, match="metadata report acknowledgement"):
                _advance(owner, kind)
        original = owner._reports._original
        selected = original._selected
        assert owner.has_pending and owner._actor is None
        assert selected.call_id == admitted.ticket.call_id
        retired = phase == "retire" and committed
        assert _count(case) == (0 if retired else 1)
        if retired:
            _reported(fixture, 1, receive=False)
        with monkeypatch.context() as patch:
            patch.setattr(original._admission, "load_next_collection_report", _forbidden)
            assert _advance(owner, kind) is True
        assert not owner.has_pending and owner._actor is None and owner._reports._original is None
        assert original._selected is selected and not original.has_pending
        assert _count(case) == int(retired)
        with session_scope(case.factory) as session:
            source.verify_prepared_receipt(session, admitted.prepared, receipt)


@pytest.mark.parametrize("kind", tuple(_OWNERS))
def test_terminal_drains_the_metadata_owners_original_report(tmp_path, monkeypatch, kind):
    with _original_report(tmp_path, monkeypatch, "volume", receive=False) as (
        fixture,
        source,
        admission,
        admitted,
        receipt,
    ):
        case = fixture.case
        owner = _owner(case, kind)
        with _lose_report_reply(monkeypatch, "receive", False):
            with pytest.raises(OSError):
                _advance(owner, kind)
        original = owner._reports._original
        selected = original._selected
        operation = ArchiveWriteAdmission(case.factory).select_terminal(
            case.collection_id,
            kind="cancel",
            request={"fixture": "metadata-report"},
            principal_id="uploader",
            grants=case.service._config.archive_write,
            guard=lambda session: None,
            validate_selection=lambda *args: None,
        )
        monkeypatch.setattr(original._admission, "load_next_collection_report", _forbidden)
        assert owner.drain_terminal(operation) == "closed"
        assert owner._reports._original is original and original._selected is selected
        assert owner.has_pending and not original.has_pending and _count(case) == 0
        assert owner.drain_terminal(operation) == "closed"
        with pytest.raises(Conflict, match="stopped for its terminal"):
            _advance(owner, kind)
        with session_scope(case.factory) as session:
            source.verify_prepared_receipt(session, admitted.prepared, receipt)


@pytest.mark.parametrize("kind", ("structure", "journal"))
def test_registry_keeps_a_metadata_report_before_an_unrelated_finalizer(
    tmp_path, monkeypatch, kind
):
    with _original_report(tmp_path, monkeypatch, "volume", receive=False) as (
        fixture,
        source,
        admission,
        admitted,
        receipt,
    ):
        case = fixture.case
        service = case.service
        owner = _owner(case, kind)
        received = []

        def completed():
            (result,) = _receive(service._archive_finalizers, service._archive_execution)
            return result.result()

        with _lose_report_reply(monkeypatch, "retire", True):
            assert (
                service._archive_finalizer_work._submit(
                    case.collection_id,
                    "metadata-report",
                    lambda: owner,
                    lambda current: _advance(current, kind),
                    received.append,
                )
                == "submitted"
            )
            with pytest.raises(OSError):
                completed()
        context = service._archive_finalizers._contexts[case.collection_id]
        retained = context.owner
        original = owner._reports._original
        assert retained.has_pending and owner.has_pending and owner._actor is None
        assert _count(case) == 0 and received == []
        _reported(fixture, 1, receive=False)
        monkeypatch.setattr(original._admission, "load_next_collection_report", _forbidden)
        assert (
            service._archive_finalizer_work._submit(
                case.collection_id,
                "another-kind",
                _forbidden,
                _forbidden,
                _forbidden,
            )
            == "submitted"
        )
        turn = completed()
        assert turn.selection == "metadata-report" and turn.progressed
        assert context.owner is retained and not retained.has_pending and received == [True]
        assert not owner.has_pending and _count(case) == 1
        assert service._archive_finalizers.retire_idle(
            context.key, target_incarnation=context.target
        )
        with session_scope(case.factory) as session:
            source.verify_prepared_receipt(session, admitted.prepared, receipt)


def test_metadata_report_terminal_poll_stays_responsive_during_receiving(tmp_path, monkeypatch):
    with _original_report(tmp_path, monkeypatch, "volume", receive=False) as (
        fixture,
        source,
        admission,
        admitted,
        receipt,
    ):
        case = fixture.case
        owner = _owner(case, "archive-volume")
        entered, release = threading.Event(), threading.Event()
        cls = admission_module.ImmutableArchiveAdmission
        actual = cls.receive_settled

        def held(receiver, *args, **kwargs):
            entered.set()
            assert release.wait(10)
            return actual(receiver, *args, **kwargs)

        monkeypatch.setattr(cls, "receive_settled", held)
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                active = pool.submit(owner.advance)
                assert entered.wait(5) and owner.has_pending
                assert owner.drain_terminal("not-selected") == "busy"
                release.set()
                assert active.result(timeout=5) is True
        finally:
            release.set()
        assert not owner.has_pending and _count(case) == 0
