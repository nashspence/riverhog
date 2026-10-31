from __future__ import annotations

import builtins
import hashlib
import json
import sqlite3
from collections import defaultdict
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
import sha256_continuation as sha_owner
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import (
    append_assertion_batches,
    append_correction,
    assertion,
    assertion_reference,
    parse_journal,
    reference,
    validate_journal,
)
from riverhog_provenance import journal as owner
from riverhog_provenance.journal_store import InvalidJournalWorkspace
from test_journal_result import completed, history

ROOT = owner.JournalSummary.anchor_at
PADDING = sha_owner.sha256_compress


def octets(value):
    return len(value.encode() if isinstance(value, str) else value)


class Cursor:
    def __init__(self, actual, recorder):
        self.actual, self.recorder = actual, recorder
        self.closed = False

    def fetchone(self):
        result = self.actual.fetchone()
        if result is not None:
            self.recorder.metrics["returned_rows"] += 1
            self.recorder.metrics["metadata_octets"] += sum(map(octets, result))
        return result

    def close(self):
        self.actual.close()
        self.closed = True
        self.recorder.metrics["closed_cursors"] += 1


class Recorder:
    def __init__(self, actual):
        self.actual, self.metrics, self.children, self.cursors = actual, defaultdict(int), [], []
        self.cursor_active = False

    def execute(self, query, parameters=()):
        self.metrics["sql_statements"] += 1
        self.metrics["bound_parameter_octets"] += sum(map(octets, parameters))
        cursor = Cursor(self.actual.execute(query, parameters), self)
        self.cursors.append(cursor)
        return cursor

    @contextmanager
    def observe(self, current, monkeypatch):
        load, dump = json.loads, json.dumps
        canonical = owner.canonical_document
        compress, rotate, update = PADDING, sha_owner._rotr, sha_owner.CheckpointSHA256.update_bytes

        def decoded(value, *args, **kwargs):
            self.metrics["json_decodes"] += 1
            self.metrics["decoded_octets"] += octets(value)
            return load(value, *args, **kwargs)

        def encoded(encoder, *args, **kwargs):
            self.metrics["json_encodes"] += 1
            result = encoder(*args, **kwargs)
            self.metrics["encoded_octets"] += octets(result)
            return result

        def rotated(value, distance):
            self.children[-1]["rotate_calls"] += 1
            return rotate(value, distance)

        def compressed(state, block):
            assert all(cursor.closed for cursor in self.cursors)
            self.metrics["compression_calls"] += 1
            child = {"block_input_octets": len(block), "rotate_calls": 0, "returned_words": 0}
            self.children.append(child)
            result = compress(state, block)
            child["returned_words"] = len(result)
            return result

        def updated(current, value):
            self.metrics["source_update_octets"] += len(value)
            return update(current, value)

        def stepped():
            self.metrics["sqlite_vm_instructions"] += 1
            return 0

        def authorized(action, table, column, _database, _trigger):
            if action == sqlite3.SQLITE_READ and (
                table != "frames" or column not in {"sequence", "reference", "prefix_state"}
            ):
                self.metrics["source_read_octets"] += 1
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        def forbidden(*args, **kwargs):
            pytest.fail("source-header selection opened unowned input or hashed source bytes")

        self.actual.set_authorizer(authorized)
        self.actual.set_progress_handler(stepped, 1)
        try:
            with monkeypatch.context() as patch:
                patch.setattr(current._store._storage, "connection", self)
                patch.setattr(owner, "json", SimpleNamespace(loads=decoded))
                patch.setattr(
                    owner, "canonical_document", lambda *a, **kw: encoded(canonical, *a, **kw)
                )
                patch.setattr(
                    sha_owner,
                    "json",
                    SimpleNamespace(
                        loads=decoded,
                        dumps=lambda *a, **kw: encoded(dump, *a, **kw),
                        JSONDecodeError=json.JSONDecodeError,
                    ),
                )
                patch.setattr(sha_owner, "sha256_compress", compressed)
                patch.setattr(sha_owner, "_rotr", rotated)
                patch.setattr(sha_owner.CheckpointSHA256, "update_bytes", updated)
                patch.setattr(builtins, "open", forbidden)
                patch.setattr(sqlite3, "connect", forbidden)
                patch.setattr(owner.hashlib, "sha256", forbidden)
                yield
        finally:
            self.actual.set_authorizer(None)
            self.actual.set_progress_handler(None, 0)
            assert all(cursor.closed for cursor in self.cursors)
            assert not self.actual.in_transaction


def record(binding, dimensions, metrics, delegates=None):
    projection = project_bindings((binding,))
    sample = {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(
            canonical_json_bytes(projection["declarations"][binding.declaration.id])
        ).hexdigest(),
        "dimensions": dimensions,
        "metrics": {bound.metric: metrics[bound.metric] for bound in binding.declaration.bounds},
        "delegates": delegates or {},
    }
    validate_execution_samples(projection, binding.declaration.id, [sample])
    return sample


def measure(current, sequence, monkeypatch):
    db = current._store.db
    encoded, state = db.execute(
        "SELECT reference,prefix_state FROM frames WHERE sequence=?", (str(sequence),)
    ).fetchone()
    restored = sha_owner.CheckpointSHA256.from_state(state)
    dimensions = {
        "retained_frames": len(current.frames),
        "sequence_octets": len(str(sequence)),
        "header_octets": octets(encoded) + octets(state),
        "padding_blocks": 1 if restored.total_bytes % 64 < 56 else 2,
    }
    observer = Recorder(db)
    with observer.observe(current, monkeypatch):
        result = ROOT(current, sequence)
    children = [
        record(growth_binding(PADDING), {"block_octets": 64}, child) for child in observer.children
    ]
    for metric in ("block_input_octets", "rotate_calls", "returned_words"):
        observer.metrics["padding_" + metric] = sum(child[metric] for child in observer.children)
    return result, record(growth_binding(ROOT), dimensions, observer.metrics, {"padding": children})


@pytest.mark.growth_witness(
    "native-frozen-source-header-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.journal.JournalSummary.anchor_at",
)
def test_actual_source_header_selection_preserves_every_original_prefix(
    journal, catalog, monkeypatch, record_property
):
    records = []
    for count in (1, 17, 129, 1025):
        raw = history(journal, count, catalog)
        expected, digest, size = [], hashlib.sha256(), 0
        for frame in parse_journal(raw):
            digest.update(frame.encoded)
            size += len(frame.encoded)
            expected.append(
                {
                    "through": frame.reference,
                    "prefix_sha256": digest.hexdigest(),
                    "prefix_bytes": str(size),
                }
            )
        with validate_journal(raw, catalog=catalog) as current:
            for sequence, header in enumerate(expected):
                result, sample = measure(current, sequence, monkeypatch)
                assert result == {"journal_id": current.journal_id, **header}
                records.append(sample)
            with current.borrow() as independent:
                for sequence in (count - 1, 0):
                    result, sample = measure(independent, sequence, monkeypatch)
                    assert result == {"journal_id": current.journal_id, **expected[sequence]}
                    records.append(sample)
                    result["through"]["entry_id"] = "changed caller view"
                    assert (
                        independent.anchor_at(sequence)["through"] == expected[sequence]["through"]
                    )
    record_property("growth_samples", records)


def test_source_headers_preserve_correction_boundaries_without_returning_tail_views(
    journal, who, catalog
):
    with validate_journal(journal, catalog=catalog) as initial:
        identity = initial.graph["artifacts"][0]["id"]
    extra = assertion(
        "extension",
        who,
        subject=reference(identity, "artifact"),
        property="urn:test:prefix-header",
        value={"type": "text", "value": "historical"},
    )
    before = append_assertion_batches(
        journal, ({"extensions": [extra]},), recorded_by_agent_id=who, catalog=catalog
    )
    with validate_journal(before, catalog=catalog) as current:
        target = assertion_reference(current, extra["assertion_id"])
    raw = append_correction(
        before, (target,), reason="exact withdrawal", recorded_by_agent_id=who, catalog=catalog
    )
    frames = parse_journal(raw)
    with validate_journal(raw, catalog=catalog) as complete:
        for sequence in range(len(frames)):
            with validate_journal(
                b"".join(frame.encoded for frame in frames[: sequence + 1]), catalog=catalog
            ) as selected:
                assert complete.anchor_at(sequence) == selected.anchor
        assert extra["assertion_id"] in complete.retracted_assertion_ids
        assert complete.anchor_at(0) != complete.anchor


def test_retained_source_header_survives_reopen_and_independent_reader_close(
    tmp_path, journal, catalog
):
    from riverhog_provenance import open_retained_journal_result

    path, source, confirmed, certificates, expected = completed(tmp_path, journal, catalog)
    with open_retained_journal_result(path, source, confirmed=confirmed, catalog=catalog) as first:
        independent = first.borrow()
        header = first.anchor_at(0)
        assert header == expected["journal"]
    with independent:
        assert independent.anchor_at(0) == header
    with pytest.raises(RuntimeError, match="closed"):
        independent.anchor_at(0)


@pytest.mark.parametrize("sequence", [-1, 1, 2**200])
def test_source_header_rejects_unaccepted_sequences(journal, catalog, sequence):
    with validate_journal(journal, catalog=catalog) as current:
        with pytest.raises(IndexError):
            current.anchor_at(sequence)


@pytest.mark.parametrize("sequence", [True, "0", None])
def test_source_header_requires_an_exact_integer_sequence(journal, catalog, sequence):
    with validate_journal(journal, catalog=catalog) as current:
        with pytest.raises(TypeError):
            current.anchor_at(sequence)


@pytest.mark.parametrize(
    "fault", ["missing", "reference", "sequence", "noncanonical", "state", "position"]
)
def test_source_header_corruption_fails_closed(journal, catalog, fault):
    with validate_journal(journal, catalog=catalog) as current:
        db = current._store.db
        db.execute("PRAGMA query_only=OFF").close()
        if fault == "missing":
            db.execute("DELETE FROM frames WHERE sequence=?", ("0",)).close()
        elif fault in {"reference", "sequence", "noncanonical"}:
            original = current.anchor["through"]
            if fault == "reference":
                original["entry_id"] = "invalid"
            if fault == "sequence":
                original["sequence"] = "1"
            value = canonical_json_bytes(original)
            if fault == "noncanonical":
                value += b" "
            db.execute("UPDATE frames SET reference=? WHERE sequence=?", (value, "0")).close()
        else:
            value = "invalid state"
            if fault == "position":
                state = sha_owner.CheckpointSHA256()
                state._total_bytes = ((len(journal) // 64) + 2) * 64
                value = state.export_state()
            db.execute("UPDATE frames SET prefix_state=? WHERE sequence=?", (value, "0")).close()
        db.commit()
        db.execute("PRAGMA query_only=ON").close()
        with pytest.raises(InvalidJournalWorkspace):
            current.anchor_at(0)
