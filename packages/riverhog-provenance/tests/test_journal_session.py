from __future__ import annotations

import hashlib

import pytest
from riverhog_provenance import ProvenanceValidationError, validate_journal
from riverhog_provenance.errors import JournalValidationCapacity
from riverhog_provenance.journal_session import (
    JournalValidationBudget,
    JournalValidationSession,
)
from riverhog_provenance.journal_store import JournalStore

ATTEMPT = "b" * 32


def finish(session, *, rows=64):
    progress = session.advance(JournalValidationBudget(rows=rows))
    while not progress.complete:
        progress = session.advance(JournalValidationBudget(rows=rows))
    return progress


@pytest.mark.parametrize("rows", [1, 16, 64])
def test_public_session_matches_native_summary_and_exact_source(journal, catalog, rows):
    with validate_journal(journal, catalog=catalog) as expected:
        with JournalValidationSession((journal,), attempt_id=ATTEMPT, catalog=catalog) as session:
            with pytest.raises(RuntimeError, match="terminal result"):
                session.result()
            progress = finish(session, rows=rows)
            assert progress.attempt_id == ATTEMPT
            assert progress.phase == "done"
            assert progress.source_octets == len(journal)
            assert progress.accepted_frames == len(expected.frames)
            assert session.advance() == progress
            result = session.result()
        with result:
            assert result.materialize() == expected.materialize()
            assert result.journal_sha256 == hashlib.sha256(journal).hexdigest()
            assert result.journal_bytes == len(journal)


def test_each_empty_chunk_is_one_bounded_source_pull(journal, catalog):
    pulls = []

    def chunks():
        for value in (b"", b"", b"", journal):
            pulls.append(value)
            yield value

    with JournalValidationSession(chunks(), attempt_id=ATTEMPT, catalog=catalog) as session:
        assert not pulls
        for count in range(1, 4):
            progress = session.advance()
            assert len(pulls) == count
            assert progress.source_octets == 0 and not progress.complete
        finish(session)


def test_single_octet_framing_preserves_one_pending_atom_on_capacity(journal, catalog):
    pulls = []

    def chunks():
        pulls.append(1)
        yield journal
        pulls.append(2)

    with JournalValidationSession(chunks(), attempt_id=ATTEMPT, catalog=catalog) as session:
        tiny = JournalValidationBudget(rows=1, source_octets=1)
        previous = 0
        for _ in range(len(journal)):
            progress = session.advance(tiny)
            assert 0 <= progress.source_octets - previous <= 1
            previous = progress.source_octets
            if session._input.pending:
                break
        before = session._ledger.load()
        with pytest.raises(JournalValidationCapacity):
            session.advance(JournalValidationBudget(rows=1, octets=1, source_octets=1))
        assert session._ledger.load() == before
        assert pulls == [1]
        finish(session)
        assert pulls == [1, 2]


@pytest.mark.parametrize("same_chunk", [False, True])
def test_native_failure_precedes_later_source_framing(journal, catalog, monkeypatch, same_chunk):
    pulls = []

    def chunks():
        pulls.append(1)
        yield journal + (b"invalid following bytes" if same_chunk else b"")
        pulls.append(2)
        yield b"invalid following bytes"

    def reject(_self, _document):
        raise ProvenanceValidationError("earlier native recorder failure")

    monkeypatch.setattr(JournalStore, "_check_entry_recording", reject)
    with JournalValidationSession(chunks(), attempt_id=ATTEMPT, catalog=catalog) as session:
        with pytest.raises(ProvenanceValidationError, match="earlier native recorder failure"):
            finish(session, rows=1)
        assert pulls == [1]
        with pytest.raises(RuntimeError, match="session failed"):
            session.advance()
        assert pulls == [1]


@pytest.mark.parametrize(
    "chunks,error,message",
    [
        ([], ProvenanceValidationError, "journal is empty"),
        ([b"\x1e{}"], ProvenanceValidationError, "incomplete final journal frame"),
        ([b"x"], ProvenanceValidationError, "bytes outside RFC 7464 frames"),
        ([b"\x1e{}\x1e\n"], ProvenanceValidationError, "unframed record separator"),
        ([b"\x1einvalid\n"], ProvenanceValidationError, None),
        ([bytearray(b"x")], TypeError, "journal chunks must be bytes"),
    ],
)
def test_native_framing_errors_are_explicit_terminal_failures(chunks, error, message, catalog):
    with JournalValidationSession(chunks, attempt_id=ATTEMPT, catalog=catalog) as session:
        with pytest.raises(error, match=message):
            finish(session)
        with pytest.raises(RuntimeError, match="terminal result"):
            session.result()


def test_result_borrows_have_independent_lifetimes(journal, catalog):
    with JournalValidationSession((journal,), attempt_id=ATTEMPT, catalog=catalog) as session:
        finish(session)
        one, two = session.result(), session.result()
        directory = session._store._storage.directory
        one.close()
        assert two.journal_bytes == len(journal)
        assert two.frames[0].json_bytes
    assert directory.exists()
    assert two.frames[0].json_bytes
    two.close()
    assert not directory.exists()
    with pytest.raises(RuntimeError, match="session is closed"):
        session.result()


def test_closing_session_preserves_caller_stream_ownership(journal, catalog):
    events = []

    def chunks():
        try:
            yield journal
        finally:
            events.append("caller closed")

    source = chunks()
    session = JournalValidationSession(source, attempt_id=ATTEMPT, catalog=catalog)
    session.advance()
    session.close()
    session.close()
    assert not events
    source.close()
    assert events == ["caller closed"]
