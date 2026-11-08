from __future__ import annotations

import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from a_riverhog_filesystem_store import FilesystemStorageAdapter, FilesystemStorageAdapterConfig
from a_riverhog_filesystem_store import incarnation as filesystem_incarnation
from riverhog_archive_contracts import (
    MEMBER_HISTORY_IMPORTS_SCHEMA,
    RecordPage,
    RecordSetCommitment,
)
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadProvenanceJournalChunkRecord as Chunk
from riverhog_core.catalog_models import CollectionUploadProvenanceJournalRecord as Journal
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services import archive_artifact_custody as module
from riverhog_core.services.archive_artifact_custody_receipt import (
    ArchiveArtifactCustodyReceiptSource as Receipt,
)
from riverhog_core.services.collection_upload_authority import require_open_upload_writes
from riverhog_core.stores.storage_adapter_archive_objects import (
    StorageAdapterImmutableArchiveObjectStore,
)
from riverhog_protocol import ArtifactId
from riverhog_protocol.collection_completion import (
    COMPLETION_REQUIRED_RECORD_KINDS,
    CollectionCompletionRequirementDocument,
)
from riverhog_protocol.errors import Conflict

from tests.unit.test_archive_artifact_custody_receipt import (
    _counts,
    _terminal,
)
from tests.unit.test_archive_artifact_custody_receipt import (
    _lose_reply as _lose_receipt_reply,
)
from tests.unit.test_archive_finalizer_registry import _receive
from tests.unit.test_archive_provenance_journal_writer import _lose_ack
from tests.unit.test_archive_write_execution import _executor
from tests.unit.test_canonical_upload_custody import _construction, _seal_payload


@contextmanager
def _case(tmp_path, monkeypatch, *, mode="direct"):
    requirement = CollectionCompletionRequirementDocument(
        execution_id="a" * 64,
        execution_envelope_sha256="b" * 64,
        controller_evidence_sha256="c" * 64,
        record_kinds=COMPLETION_REQUIRED_RECORD_KINDS,
    )
    case = _construction(tmp_path, requirement)
    _seal_payload(case)
    inputs = RecordSetCommitment(MEMBER_HISTORY_IMPORTS_SCHEMA).ref()
    case.service.stage_history_structure(1, RecordPage(inputs, 0, (), True).to_json_bytes())
    case.service.set_member_history_inputs(1, ArtifactId(case.member_id), inputs)
    with session_scope(case.factory) as session:
        incarnation = session.get(Upload, 1).archive_incarnation_id
    root = tmp_path / "custody-provider"
    root.mkdir()
    with monkeypatch.context() as patch:
        patch.setattr(filesystem_incarnation, "uuid4", lambda: uuid.UUID(incarnation))
        assert filesystem_incarnation.provision_storage_root(root) == incarnation
    try:
        with (
            FilesystemStorageAdapter(
                FilesystemStorageAdapterConfig(root=root, minimum_free_bytes=0)
            ) as adapter,
            _executor(adapter, mode) as transport,
        ):
            physical = StorageAdapterImmutableArchiveObjectStore(transport)
            calls = {"observe": 0, "execute": 0}

            class Ports:
                def observe_immutable_object(self, request):
                    calls["observe"] += 1
                    return physical.observe_immutable_object(request)

                def execute_write(self, invocation, *, content):
                    calls["execute"] += 1
                    return physical.execute_write(invocation, content=content)

            ports = Ports()
            binding = SimpleNamespace(
                immutable_objects=physical, require_immutable_writes=lambda: ports
            )

            def require_incarnation(name, selected):
                assert name == "archive" and selected == incarnation
                return binding

            case.service._archive_stores = SimpleNamespace(
                require_incarnation=require_incarnation, require=lambda name: binding
            )
            case.calls = calls
            case.ports = ports
            yield case
    finally:
        case.factory.kw["bind"].dispose()


def _owner(case):
    def guard(session):
        require_open_upload_writes(session, session.get(Upload, 1))

    return case.service._artifact_custody_owner(1, case.member_id, forward_guard=guard)


def _kind(selection):
    if selection.pending_structure is not None:
        return "structure"
    return "journal" if selection.pending_segment is not None else "receipt"


def _prime(case, kind):
    for _ in range(4):
        selected = case.service._select_artifact_custody(1, case.member_id)
        assert selected is not None
        if _kind(selected) == kind:
            return selected
        assert _owner(case).advance()
    pytest.fail("the verified custody stage was not reached")


def _forbidden(*args, **kwargs):
    pytest.fail("original custody work cannot select or construct a replacement")


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_verified_member_custody_uses_actual_retained_dependencies_before_the_receipt(
    tmp_path, monkeypatch, mode
):
    with _case(tmp_path, monkeypatch, mode=mode) as case:
        for kind in ("structure", "journal", "receipt"):
            assert _kind(case.service._select_artifact_custody(1, case.member_id)) == kind
            owner = _owner(case)
            assert not owner.has_pending
            assert owner.advance() is True and not owner.has_pending
            calls = dict(case.calls)
            owner._select_source = _forbidden
            assert owner.advance() is True and case.calls == calls
        assert _counts(case) == (1, 3)
        empty = _owner(case)
        assert empty.advance() is False and not empty.has_pending
        empty._select_source = _forbidden
        assert empty.advance() is False
        assert case.calls == {"observe": 2, "execute": 2}


@pytest.mark.parametrize(
    ("kind", "phase"),
    (
        ("structure", "report"),
        ("structure", "retirement"),
        ("journal", "report"),
        ("journal", "retirement"),
        ("journal", "association"),
        ("receipt", "receipt"),
    ),
)
@pytest.mark.parametrize("committed", (False, True))
def test_custody_selection_and_child_survive_actual_acknowledgement_loss(
    tmp_path, monkeypatch, kind, phase, committed
):
    with _case(tmp_path, monkeypatch) as case:
        _prime(case, kind)
        owner = _owner(case)
        fault = (
            _lose_receipt_reply(monkeypatch, committed)
            if kind == "receipt"
            else _lose_ack(monkeypatch, phase, committed)
        )
        with fault:
            with pytest.raises(OSError):
                owner.advance()
        selected, original = owner._selection, owner._original
        assert selected is not None and original is not None and owner.has_pending
        assert original.has_pending
        calls = dict(case.calls)
        owner._select_source = _forbidden
        owner._config = owner._archive_stores = None
        monkeypatch.setattr(case.service, "_select_artifact_custody", _forbidden)
        assert owner.advance() is True and not owner.has_pending
        assert owner._selection is selected and owner._original is original
        assert owner.advance() is True and case.calls == calls
        assert _counts(case) == ((1, 3) if kind == "receipt" else (0, 0))


@pytest.mark.parametrize("change", ("sha256", "content"))
def test_verified_journal_selection_cannot_be_replaced_before_child_execution(
    tmp_path, monkeypatch, change
):
    with _case(tmp_path, monkeypatch) as case:
        verified = _prime(case, "journal")
        original = case.service._select_artifact_custody
        selections = []

        def select(*args):
            selected = original(*args)
            selections.append(selected)
            journal = verified.pending_segment.selected
            with session_scope(case.factory) as session:
                if change == "sha256":
                    session.get(Journal, (1, journal.journal_id)).sha256 = "f" * 64
                else:
                    chunk = session.get(Chunk, (1, journal.journal_id, 0))
                    chunk.content = b"!" + chunk.content[1:]
            return selected

        monkeypatch.setattr(case.service, "_select_artifact_custody", select)
        owner = _owner(case)
        calls = dict(case.calls)
        with pytest.raises(Conflict, match="verified artifact custody journal selection changed"):
            owner.advance()
        assert owner.has_pending and len(selections) == 1 and case.calls == calls
        owner._select_source = _forbidden
        with pytest.raises(Conflict, match="verified artifact custody journal selection changed"):
            owner.advance()
        assert case.calls == calls


@pytest.mark.parametrize("returned", (False, True))
def test_unconfirmed_custody_child_constructor_cannot_be_replayed_or_drained_as_closed(
    tmp_path, monkeypatch, returned
):
    with _case(tmp_path, monkeypatch) as case:
        owner = _owner(case)
        constructed = []

        def unknown(*args, **kwargs):
            constructed.append(1)
            if returned:
                return None
            raise OSError("synthetic unconfirmed custody constructor")

        with monkeypatch.context() as patch:
            patch.setattr(module, "ArchiveProvenanceStructureWriter", unknown)
            with pytest.raises(ValueError if returned else OSError):
                owner.advance()
        owner._select_source = _forbidden
        with pytest.raises(Conflict, match="constructor remains unresolved"):
            owner.advance()
        assert constructed == [1] and owner.has_pending
        operation = _terminal(case)
        assert owner.drain_terminal(operation) == "pending"
        assert owner.drain_terminal(operation) == "pending"
        assert case.calls == {"observe": 0, "execute": 0}


def test_custody_terminal_drain_keeps_the_original_physical_report(tmp_path, monkeypatch):
    with _case(tmp_path, monkeypatch) as case:
        _prime(case, "journal")
        owner = _owner(case)
        with _lose_ack(monkeypatch, "report", True):
            with pytest.raises(OSError):
                owner.advance()
        selected, original = owner._selection, owner._original
        owner._select_source = _forbidden
        operation = _terminal(case)
        assert owner.drain_terminal(operation) == "closed"
        assert owner._selection is selected and owner._original is original and owner.has_pending
        with pytest.raises(Conflict, match="stopped for its terminal"):
            owner.advance()
        assert _counts(case) == (0, 0)


def test_custody_terminal_poll_does_not_wait_for_original_logical_receiving(tmp_path, monkeypatch):
    with _case(tmp_path, monkeypatch) as case:
        _prime(case, "receipt")
        owner = _owner(case)
        entered, release = threading.Event(), threading.Event()
        actual = Receipt.apply

        def held(source, session):
            entered.set()
            assert release.wait(10)
            return actual(source, session)

        monkeypatch.setattr(Receipt, "apply", held)
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                active = pool.submit(owner.advance)
                assert entered.wait(5) and owner.has_pending
                assert owner.drain_terminal("not-selected") == "busy"
                release.set()
                assert active.result(timeout=5) is True
        finally:
            release.set()
        assert _counts(case) == (1, 3)


def test_registry_receives_the_original_custody_turn_without_another_verification_walk(
    tmp_path, monkeypatch
):
    with _case(tmp_path, monkeypatch) as case:
        _prime(case, "journal")
        owner = _owner(case)
        received = []

        def feedback(progressed):
            received.append(progressed)
            if len(received) == 1:
                raise OSError("synthetic custody workflow reply loss")

        def completed():
            (result,) = _receive(case.service._archive_finalizers, case.service._archive_execution)
            return result.result()

        work = case.service._archive_finalizer_work
        assert (
            work._submit(
                1, "member-custody", lambda: owner, lambda current: current.advance(), feedback
            )
            == "submitted"
        )
        with pytest.raises(OSError, match="workflow reply loss"):
            completed()
        owner._select_source = _forbidden
        calls = dict(case.calls)
        assert work._submit(1, "fresh-selection", _forbidden, _forbidden, _forbidden) == "submitted"
        result = completed()
        assert result.selection == "member-custody" and result.progressed
        assert received == [True, True] and case.calls == calls and _counts(case) == (0, 0)
