from __future__ import annotations

import hashlib
import sqlite3
from contextlib import closing
from dataclasses import FrozenInstanceError

import pytest
from native_units import unit_budget
from riverhog_provenance._native_epoch import EpochContext
from riverhog_provenance._native_frame import NativeFrame, own_frame
from riverhog_provenance._native_work import NativeWorkCapacity, WorkAmount, WorkQuantum
from riverhog_provenance.errors import IncompleteSourceError
from riverhog_provenance.journal import JournalFrame
from riverhog_provenance.journal_session import JournalValidationSession
from sqlite_admission import SqliteProgressConnection
from test_native_epoch_integration import _reach
from test_retained_journal_session import OwnedSource, directory, open_session


def observe_frames(monkeypatch):
    decoded, hashed = [], []
    decode, digest = JournalFrame.document.fget, JournalFrame.sha256.fget

    def document(frame):
        if isinstance(frame, NativeFrame):
            decoded.append(frame)
        return decode(frame)

    def sha256(frame):
        if isinstance(frame, NativeFrame):
            hashed.append(frame)
        return digest(frame)

    monkeypatch.setattr(JournalFrame, "document", property(document))
    monkeypatch.setattr(JournalFrame, "sha256", property(sha256))
    return decoded, hashed


@pytest.mark.parametrize("retained", [False, True])
@pytest.mark.parametrize("rows", [1, 64])
def test_actual_children_borrow_one_owned_parse_and_hash_until_original_publication(
    tmp_path, journal, catalog, journal_constructions, monkeypatch, retained, rows
):
    decoded, hashed = observe_frames(monkeypatch)
    context = (
        open_session(
            directory(tmp_path),
            OwnedSource(journal, catalog),
            catalog,
            create=True,
            journal_constructions=journal_constructions,
        )
        if retained
        else JournalValidationSession((journal,), attempt_id="a" * 32, catalog=catalog)
    )
    with context as session:
        _reach(session, "entry-admission")
        current = session._replay._current
        frame = current.frame
        assert isinstance(frame, NativeFrame)
        assert current.document is current.prepared.document is frame.document
        expected = JournalFrame(frame.json_bytes).document
        assert decoded == hashed == [frame]
        phases = set()
        for _ in range(4096):
            phases.add(session.progress().phase)
            progress = session.advance(unit_budget(rows=rows))
            if progress.accepted_frames:
                break
        else:
            pytest.fail("small exact frame did not complete its original publication")
        assert {"entry", "staging", "finish", "publish"} <= phases
        assert session._replay._current is None
        assert session._ledger.epoch.quiescent
        assert decoded == hashed == [frame]
        assert frame.document == expected
        assert frame.sha256 == hashlib.sha256(frame.json_bytes).hexdigest()
    assert session.storage_received


def test_cold_current_atom_is_hashed_before_parsing_and_reuses_that_hash(
    journal, catalog, monkeypatch
):
    with JournalValidationSession((journal,), attempt_id="a" * 32, catalog=catalog) as session:
        _reach(session, "entry")
        before = session._replay._current.frame
        decoded, hashed = observe_frames(monkeypatch)
        session._replay._current = None
        session.advance(unit_budget())
        after = session._replay._current.frame
        assert after is not before and after.json_bytes == before.json_bytes
        assert decoded == hashed == [after]
        assert after.document == before.document and after.sha256 == before.sha256
        assert decoded == hashed == [after]
    assert session.storage_received


def test_changed_quarantined_atom_is_rejected_before_any_cached_parse(
    journal, catalog, monkeypatch
):
    with JournalValidationSession((journal,), attempt_id="a" * 32, catalog=catalog) as session:
        _reach(session, "entry")
        current = session._replay._current
        raw = current.frame.json_bytes
        session._replay._current = None
        session._store.db.execute(
            "UPDATE journal_validation_frame SET value=? WHERE singleton=1",
            (b"X" + raw[1:],),
        ).close()
        session._store.db.commit()
        decoded, hashed = observe_frames(monkeypatch)
        with pytest.raises(RuntimeError, match="retained entry atom is missing or changed"):
            session.advance(unit_budget())
        assert decoded == [] and len(hashed) == 1
        assert session._replay._current is None
    assert session.storage_received


def test_warm_parsed_data_does_not_replace_fresh_source_currentness(
    tmp_path, journal, catalog, journal_constructions
):
    source = OwnedSource(journal, catalog)
    with open_session(
        directory(tmp_path),
        source,
        catalog,
        create=True,
        journal_constructions=journal_constructions,
    ) as session:
        _reach(session, "entry")
        assert session._replay._current.frame.document
        source.calls.clear()
        source.current = False
        with pytest.raises(IncompleteSourceError, match="no longer current"):
            session.advance(unit_budget())
        assert source.calls == [] and not session._store.db.in_transaction
    assert session.storage_received


def test_parsed_view_and_hash_never_cross_original_executor_custody(monkeypatch):
    decoded, hashed = observe_frames(monkeypatch)
    with closing(sqlite3.connect(":memory:", factory=SqliteProgressConnection)) as db:
        first, second = EpochContext(db), EpochContext(db)
        public = JournalFrame(b'{"sequence":"0"}')
        owned = own_frame(public, first)
        assert own_frame(owned, first) is owned
        replacement = own_frame(owned, second)
        assert replacement is not owned
        assert replacement.document == owned.document
        assert replacement.document is not owned.document
        assert replacement.sha256 == owned.sha256
        assert len(decoded) == len(hashed) == 2
        # Caller-owned pure frames still return independent mutable documents.
        detached = public.document
        detached["sequence"] = "changed"
        assert public.document == owned.document == {"sequence": "0"}
        with pytest.raises(FrozenInstanceError):
            owned.json_bytes = b"changed"
        with pytest.raises(TypeError, match="immutable exact bytes"):
            NativeFrame(bytearray(public.json_bytes), first)


def test_actual_hash_consumes_its_input_once_without_fresh_source_credit(monkeypatch):
    _, hashed = observe_frames(monkeypatch)
    with closing(sqlite3.connect(":memory:", factory=SqliteProgressConnection)) as db:
        epoch = EpochContext(db)
        raw = b'{"sequence":"0"}'
        frame = NativeFrame(raw, epoch)
        for ordinal in range(2):
            work = WorkQuantum(WorkAmount(rows=1, octets=len(raw), source_octets=1, transitions=1))
            with epoch.execution(None, work=work):
                assert frame.sha256 == hashlib.sha256(raw).hexdigest()
                assert frame.sha256 == hashlib.sha256(raw).hexdigest()
            assert work.consumed == (WorkAmount(octets=len(raw)) if ordinal == 0 else WorkAmount())
            work.require_received()
        assert hashed == [frame]


def test_hash_capacity_refuses_before_hashing_and_keeps_original_executor_usable(monkeypatch):
    _, hashed = observe_frames(monkeypatch)
    with closing(sqlite3.connect(":memory:", factory=SqliteProgressConnection)) as db:
        epoch = EpochContext(db)
        raw = b'{"sequence":"0"}'
        frame = NativeFrame(raw, epoch)
        small = WorkQuantum(WorkAmount(rows=1, octets=len(raw) - 1, source_octets=1, transitions=1))
        with pytest.raises(NativeWorkCapacity):
            with epoch.execution(None, work=small):
                _ = frame.sha256
        assert hashed == [] and frame._sha256 is None and epoch.quiescent
        assert small.consumed == WorkAmount()
        small.require_received()
        enough = WorkQuantum(WorkAmount(rows=1, octets=len(raw), source_octets=1, transitions=1))
        with epoch.execution(None, work=enough):
            assert frame.sha256 == hashlib.sha256(raw).hexdigest()
        assert hashed == [frame] and enough.consumed == WorkAmount(octets=len(raw))
        enough.require_received()


def test_failed_original_epoch_cannot_reuse_even_an_already_parsed_and_hashed_frame():
    with closing(sqlite3.connect(":memory:", factory=SqliteProgressConnection)) as db:
        epoch = EpochContext(db)
        frame = NativeFrame(b'{"sequence":"0"}', epoch)
        assert frame.document and frame.sha256

        def failed(_original):
            raise OSError("original private application failed")

        with pytest.raises(OSError, match="original private application failed"):
            epoch.apply(failed, receive=lambda _value: None)
        with pytest.raises(RuntimeError, match="requires reconstruction"):
            _ = frame.document
        with pytest.raises(RuntimeError, match="requires reconstruction"):
            _ = frame.sha256
        with pytest.raises(RuntimeError, match="requires reconstruction"):
            own_frame(frame, epoch)


def test_parse_consumes_its_input_once_without_fresh_source_credit(monkeypatch):
    decoded, hashed = observe_frames(monkeypatch)
    with closing(sqlite3.connect(":memory:", factory=SqliteProgressConnection)) as db:
        epoch = EpochContext(db)
        raw = b'{"sequence":"0"}'
        frame, received = NativeFrame(raw, epoch), None
        for ordinal in range(2):
            work = WorkQuantum(WorkAmount(rows=1, octets=len(raw), source_octets=1, transitions=1))
            with epoch.execution(None, work=work):
                current = frame.document
                assert current == {"sequence": "0"} and frame.document is current
                if received is not None:
                    assert current is received
                received = current
            assert work.consumed == (WorkAmount(octets=len(raw)) if ordinal == 0 else WorkAmount())
            work.require_received()
        assert decoded == [frame] and hashed == []


def test_parse_capacity_refuses_before_decoding_and_retries_the_same_original_frame(monkeypatch):
    decoded, hashed = observe_frames(monkeypatch)
    with closing(sqlite3.connect(":memory:", factory=SqliteProgressConnection)) as db:
        epoch = EpochContext(db)
        raw = b'{"sequence":"0"}'
        frame = NativeFrame(raw, epoch)
        small = WorkQuantum(WorkAmount(rows=1, octets=len(raw) - 1, source_octets=1, transitions=1))
        with pytest.raises(NativeWorkCapacity):
            with epoch.execution(None, work=small):
                _ = frame.document
        assert decoded == hashed == [] and frame._document is frame._sha256 is None
        assert small.consumed == small.held == WorkAmount() and epoch.quiescent
        assert own_frame(frame, epoch) is frame
        enough = WorkQuantum(WorkAmount(rows=1, octets=len(raw), source_octets=1, transitions=1))
        with epoch.execution(None, work=enough):
            assert frame.document == {"sequence": "0"}
        assert decoded == [frame] and hashed == []
        assert enough.consumed == WorkAmount(octets=len(raw)) and enough.held == WorkAmount()
        small.require_received()
        enough.require_received()


def test_failed_parse_keeps_actual_input_cost_without_a_successful_cached_view(monkeypatch):
    decoded, hashed = observe_frames(monkeypatch)
    with closing(sqlite3.connect(":memory:", factory=SqliteProgressConnection)) as db:
        epoch = EpochContext(db)
        raw = b'{"sequence":'
        frame = NativeFrame(raw, epoch)
        for _ in range(2):
            work = WorkQuantum(WorkAmount(rows=1, octets=len(raw), source_octets=1, transitions=1))
            with epoch.execution(None, work=work):
                with pytest.raises(ValueError, match="invalid provenance JSON"):
                    _ = frame.document
            assert frame._document is frame._sha256 is None and epoch.quiescent
            assert work.consumed == WorkAmount(octets=len(raw)) and work.held == WorkAmount()
            work.require_received()
        assert decoded == [frame, frame] and hashed == []


def test_parse_and_hash_consume_the_original_application_reservation(monkeypatch):
    decoded, hashed = observe_frames(monkeypatch)
    with closing(sqlite3.connect(":memory:", factory=SqliteProgressConnection)) as db:
        epoch, raw = EpochContext(db), b'{"sequence":"0"}'
        frame = NativeFrame(raw, epoch)
        work = WorkQuantum(WorkAmount(rows=1, octets=3 * len(raw), source_octets=1, transitions=1))
        received = []

        def mutation(original):
            assert original is epoch and work.held == WorkAmount(octets=2 * len(raw))
            view = frame.document
            assert work.consumed == work.held == WorkAmount(octets=len(raw))
            digest = frame.sha256
            assert work.consumed == WorkAmount(octets=2 * len(raw)) and work.held == WorkAmount()
            return view, digest

        def receive(value):
            epoch.require_receiving(db)
            assert frame.document is value[0] and frame.sha256 == value[1]
            received.append(value)

        with epoch.execution(None, work=work):
            result = epoch.apply(
                mutation, receive=receive, admission=WorkAmount(octets=2 * len(raw))
            )
        assert received == [result] and decoded == hashed == [frame]
        assert epoch.quiescent and work.consumed == WorkAmount(octets=2 * len(raw))
        assert work.held == WorkAmount()
        work.require_received()


def test_hash_cannot_bypass_original_application_admission_with_unreserved_capacity(monkeypatch):
    decoded, hashed = observe_frames(monkeypatch)
    with closing(sqlite3.connect(":memory:", factory=SqliteProgressConnection)) as db:
        epoch, raw = EpochContext(db), b'{"sequence":"0"}'
        frame = NativeFrame(raw, epoch)
        work = WorkQuantum(WorkAmount(rows=1, octets=3 * len(raw), source_octets=1, transitions=1))
        with pytest.raises(RuntimeError, match="exceeds its original admission"):
            with epoch.execution(None, work=work):
                epoch.apply(
                    lambda _original: (frame.document, frame.sha256),
                    receive=lambda _value: pytest.fail("unadmitted hash was received"),
                    admission=WorkAmount(octets=len(raw)),
                )
        assert decoded == [frame] and hashed == [] and frame._sha256 is None
        assert work.consumed == WorkAmount(octets=len(raw)) and work.held == WorkAmount()
        with pytest.raises(RuntimeError, match="requires reconstruction"):
            _ = frame.document
        work.require_received()
