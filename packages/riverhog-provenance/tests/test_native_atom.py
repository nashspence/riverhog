from __future__ import annotations

import hashlib
import json

import pytest
from riverhog_provenance import _entry_replay, _generic_semantics
from riverhog_provenance._changed_assertions import read_changed_assertion
from riverhog_provenance._generic_semantics import (
    GenericAtom,
    GenericSemantics,
    StaleGenericSemantics,
)
from riverhog_provenance._native_epoch import EpochContext
from riverhog_provenance._native_work import WorkAmount, WorkQuantum
from riverhog_provenance._semantic_program import Nodes
from riverhog_provenance.journal_session import JournalValidationWorkBudget
from test_native_epoch_integration import _reach
from test_owner_row_work import context


@pytest.fixture(params=[False, True], ids=["ordinary", "retained"])
def session(tmp_path, journal, catalog, journal_constructions, request):
    with context(tmp_path, journal, catalog, journal_constructions, request.param) as current:
        _reach(current, "row-admit")
        yield current
    assert current.storage_received


def quantum(octets):
    # These cases isolate byte admission; use the owning default for row work.
    return WorkQuantum(
        WorkAmount(
            rows=JournalValidationWorkBudget().rows,
            octets=octets,
            source_octets=1,
            transitions=1,
        )
    )


@pytest.mark.parametrize("cold", [False, True])
def test_original_native_atom_uses_one_hash_one_decode_and_no_canonical_reencode(
    session, monkeypatch, cold
):
    replay, epoch = session._replay, session._ledger.epoch
    before, current = session._ledger.load(), replay._current
    if cold:
        before = replay._admit_row(before, current)
        current.row_key, current.row_sha256, current.row, current.atom = None, None, None, None
    expected = read_changed_assertion(
        replay.db, after=before.checkpoint.row_after, byte_budget=1024 * 1024
    )
    raw = expected.raw
    selected, hashes, decodes, encodes = [], [], [], []
    select = _entry_replay.read_changed_assertion
    digest, decode, encode = hashlib.sha256, json.loads, _generic_semantics.canonical_document

    def selection(*args, **kwargs):
        result = select(*args, **kwargs)
        selected.append(result)
        return result

    def hashing(value=b"", *args, **kwargs):
        if value == raw:
            hashes.append(value)
        return digest(value, *args, **kwargs)

    def decoding(value, *args, **kwargs):
        if value == raw:
            decodes.append(value)
        return decode(value, *args, **kwargs)

    def encoding(value):
        encodes.append(value)
        return encode(value)

    work = quantum(3 * len(raw))
    with monkeypatch.context() as patch:
        patch.setattr(_entry_replay, "read_changed_assertion", selection)
        patch.setattr(hashlib, "sha256", hashing)
        patch.setattr(json, "loads", decoding)
        patch.setattr(_generic_semantics, "canonical_document", encoding)
        with epoch.execution(None, work=work):
            if cold:
                assert replay._row(before.checkpoint, current) is current.row
            else:
                before = replay._admit_row(before, current)
        assert hashes == decodes == [raw] and encodes == []
        assert len(selected) == 1 and current.atom.encoded is selected[0].raw
        assert current.atom.nodes.values[0] is current.row
        assert current.atom.sha256 == current.row_sha256 == digest(raw).hexdigest()
        assert current.atom._epoch is epoch and encode(current.row) == raw
        warm = quantum(1)
        with epoch.execution(None, work=warm):
            assert replay._row(before.checkpoint, current) is current.row
        assert hashes == decodes == [raw] and encodes == []
    assert work.consumed.octets == 3 * len(raw) and work.consumed.source_octets == 0
    assert work.held == warm.consumed == warm.held == WorkAmount()
    work.require_received()
    warm.require_received()


def test_owned_atom_rejects_foreign_and_failed_original_executors(session):
    session.advance()
    current, epoch, db = session._replay._current, session._ledger.epoch, session._store.db
    atom, statements = current.atom, []
    assert atom is not None and atom._epoch is epoch

    def construct(selected_epoch):
        return GenericSemantics(
            db,
            attempt_id="native-owned-atom",
            source_binding=current.source,
            atom=atom,
            catalog=session._replay.catalog,
            epoch=selected_epoch,
        )

    db.set_trace_callback(statements.append)
    try:
        with pytest.raises(StaleGenericSemantics, match="another Native executor"):
            construct(EpochContext(db))
        assert statements == []
        assert construct(epoch).epoch is epoch
        assert statements == []
        failure = OSError("original Native semantic epoch failed")

        def fail(_epoch):
            raise failure

        with pytest.raises(OSError) as caught:
            epoch.apply(fail, receive=lambda _value: pytest.fail("failed epoch was received"))
        assert caught.value is failure
        with pytest.raises(RuntimeError, match="requires reconstruction"):
            construct(epoch)
        with pytest.raises(RuntimeError, match="requires reconstruction"):
            GenericAtom._borrow(atom.encoded, atom.sha256, current.row, epoch=epoch)
    finally:
        db.set_trace_callback(None)


def test_failed_index_does_not_publish_partial_owned_row_cache_and_can_retry(session, monkeypatch):
    replay, epoch = session._replay, session._ledger.epoch
    before, current = session._ledger.load(), replay._current
    selected = read_changed_assertion(
        replay.db, after=before.checkpoint.row_after, byte_budget=1024 * 1024
    )
    work = quantum(3 * len(selected.raw))
    changes, failure = replay.db.total_changes, OSError("original native node index failed")

    def fail(_nodes, _document, **kwargs):
        assert kwargs["epoch"] is epoch
        assert current.row is None and current.atom is None
        raise failure

    with monkeypatch.context() as patch:
        patch.setattr(Nodes, "__init__", fail)
        with pytest.raises(OSError) as caught:
            with epoch.execution(None, work=work):
                replay._admit_row(before, current)
    assert caught.value is failure and current.row is None and current.atom is None
    assert current.row_key is current.row_sha256 is None
    assert work.consumed.octets == 3 * len(selected.raw) and work.consumed.source_octets == 0
    assert work.held == WorkAmount() and epoch.quiescent
    assert session._ledger.load() == before and replay.db.total_changes == changes
    retry = quantum(3 * len(selected.raw))
    with epoch.execution(None, work=retry):
        after = replay._admit_row(before, current)
    assert after.checkpoint.phase == "generic-admission"
    assert current.atom._epoch is epoch and current.atom.nodes.values[0] is current.row
    assert retry.consumed.octets == 3 * len(selected.raw) and retry.held == WorkAmount()
    work.require_received()
    retry.require_received()
