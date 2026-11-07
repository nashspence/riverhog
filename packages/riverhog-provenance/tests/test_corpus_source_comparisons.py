from __future__ import annotations

from contextlib import contextmanager

import pytest
from riverhog_provenance import (
    CanonicalCorpusValidator,
    ProvenanceValidationError,
    append_assertions,
    append_correction,
    assertion,
    assertion_reference,
    validate_journal,
)


class ScalarCursor:
    def __init__(self, actual, rows, *, comparison=False):
        self.actual, self.rows, self.comparison = actual, rows, comparison

    @property
    def rowcount(self):
        return self.actual.rowcount

    def fetchone(self):
        row = self.actual.fetchone()
        if row is not None:
            assert not any(isinstance(value, bytes) for value in row), (
                "raw source equality returned a retained preimage instead of a scalar"
            )
            if self.comparison:
                assert len(row) == 1 and (
                    row[0] is None or type(row[0]) is int and row[0] in (0, 1)
                ), "raw source equality returned retained metadata instead of a match scalar"
            self.rows.append(row)
        return row

    def close(self):
        self.actual.close()


class ScalarConnection:
    def __init__(self, actual, rows):
        self.actual, self.rows = actual, rows

    def execute(self, query, parameters=()):
        return ScalarCursor(
            self.actual.execute(query, parameters),
            self.rows,
            comparison=query.startswith("SELECT") and "FROM source_extents" not in query,
        )


@contextmanager
def scalar_reads(corpus, monkeypatch):
    rows = []
    with monkeypatch.context() as patch:
        patch.setattr(corpus, "_db", ScalarConnection(corpus._db, rows))
        yield rows


@pytest.mark.parametrize(
    "mode", ["warm", "replayed", "reference", "prefix", "bytes", "missing", "null"]
)
def test_source_entry_agreement_returns_no_retained_preimage(journal, catalog, monkeypatch, mode):
    with validate_journal(journal, catalog=catalog) as source, CanonicalCorpusValidator() as corpus:
        assert corpus.advance_source(source).complete
        if mode == "replayed":
            corpus._db.execute("DELETE FROM source_extents")
        elif mode == "missing":
            corpus._db.execute("DELETE FROM entries")
        elif mode == "null":
            corpus._db.execute("UPDATE entries SET reference=NULL")
        elif mode != "warm":
            value = b"foreign" * 131072 if mode == "reference" else "9" * 1048576
            corpus._db.execute("UPDATE entries SET " + mode + "=?", (value,))
        before = corpus._db.execute("SELECT * FROM entries").fetchall()
        with scalar_reads(corpus, monkeypatch) as rows:
            if mode in {"warm", "replayed"}:
                assert corpus.advance_source(source).complete
            else:
                with pytest.raises(ProvenanceValidationError, match="divergent writers"):
                    corpus.advance_source(source)
        assert rows
        assert corpus._db.execute("SELECT * FROM entries").fetchall() == before


@pytest.mark.parametrize("fault", ["none", "reference", "retired", "missing", "null"])
def test_source_retirement_compares_exact_entry_and_live_status_without_preimage_transfer(
    journal, who, catalog, monkeypatch, fault
):
    added = assertion("agent", who, kind="software", name="independent correction target")
    raw = append_assertions(journal, {"agents": [added]}, recorded_by_agent_id=who, catalog=catalog)
    with validate_journal(raw, catalog=catalog) as before:
        target = assertion_reference(before, added["assertion_id"])
    raw = append_correction(
        raw, [target], reason="withdrawn", recorded_by_agent_id=who, catalog=catalog
    )
    with validate_journal(raw, catalog=catalog) as source, CanonicalCorpusValidator() as corpus:
        assert not corpus.advance_source(source, frame_budget=2).complete
        key = (source.journal_id, added["assertion_id"])
        if fault == "reference":
            corpus._db.execute(
                "UPDATE assertions SET entry=? WHERE journal=? AND assertion=?",
                (b"foreign" * 131072, *key),
            )
        elif fault == "null":
            corpus._db.execute(
                "UPDATE assertions SET entry=NULL WHERE journal=? AND assertion=?", key
            )
        elif fault == "retired":
            corpus._db.execute(
                "UPDATE assertions SET retired='1' WHERE journal=? AND assertion=?", key
            )
        elif fault == "missing":
            corpus._db.execute("DELETE FROM assertions WHERE journal=? AND assertion=?", key)
        original = corpus._db.execute(
            "SELECT entry,retired FROM assertions WHERE journal=? AND assertion=?", key
        ).fetchone()
        with scalar_reads(corpus, monkeypatch) as rows:
            if fault == "none":
                assert corpus.advance_source(source).complete
            else:
                with pytest.raises(ProvenanceValidationError, match="corpus correction"):
                    corpus.advance_source(source)
        assert rows
        after = corpus._db.execute(
            "SELECT entry,retired FROM assertions WHERE journal=? AND assertion=?", key
        ).fetchone()
        if fault == "none":
            assert after == (original[0], "2")
        else:
            assert after == original
            assert corpus._db.execute("SELECT sequence FROM source_extents").fetchone() == ("1",)
