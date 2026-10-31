from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import defaultdict
from types import SimpleNamespace

import pytest
import riverhog_provenance.corpus as owner
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import (
    CanonicalCorpusValidator,
    CorpusDependencyProgress,
    JournalValidationCapacity,
    ProvenanceValidationError,
    create_journal,
    validate_journal,
)
from test_corpus_source_growth import source_reads


def populated(corpus, count):
    references = []
    for index in range(count):
        entry = canonical_json_bytes({"sequence": str(index)})
        reference = {
            "journal_id": "journal",
            "assertion_id": str(index),
            "object_id": "object-" + str(index),
            "object_type": "state",
            "entry": {"sequence": str(index)},
        }
        corpus._db.execute(
            "INSERT INTO assertions VALUES (?, ?, ?, ?, ?, ?, NULL, ?)",
            (
                "journal",
                str(index),
                reference["object_id"],
                "state",
                str(index),
                len(str(index)),
                entry,
            ),
        )
        corpus._db.execute("INSERT INTO refs VALUES (?)", (canonical_json_bytes(reference),))
        anchor = {
            "journal_id": "journal",
            "through": reference["entry"],
            "prefix_sha256": "0" * 64,
            "prefix_bytes": str(index + 1),
        }
        corpus._db.execute(
            "INSERT INTO entries VALUES (?, ?, ?, ?, ?)",
            ("journal", str(index), entry, anchor["prefix_sha256"], anchor["prefix_bytes"]),
        )
        corpus._db.execute("INSERT INTO forks VALUES (?)", (canonical_json_bytes(anchor),))
        corpus._db.execute(
            "INSERT INTO comparisons VALUES (?, ?, ?, ?, ?)",
            ("journal", str(index), "left", "right", "matching_fixity"),
        )
        references.append(reference)
    corpus._db.executemany(
        "INSERT INTO fixities VALUES (?, ?, ?)",
        (("left", "42", "a" * 64), ("right", "42", "a" * 64)),
    )
    assert corpus.advance_semantics().complete
    return references


def finish(corpus, **budgets):
    states = []
    while True:
        progress = corpus.advance_dependencies(**budgets)
        assert isinstance(progress, CorpusDependencyProgress)
        states.append(progress)
        if progress.complete:
            return states


@pytest.mark.parametrize("rows", [1, 7, 64])
@pytest.mark.parametrize("octets", [256, 4096])
def test_dependency_quanta_preserve_complete_output_and_phase_order(rows, octets):
    with CanonicalCorpusValidator() as corpus:
        populated(corpus, 17)
        states = finish(corpus, row_budget=rows, byte_budget=octets)
        assert states[-1].checked_obligations == 51
        phases = {"references": 0, "forks": 1, "comparisons": 2, "complete": 3}
        assert [phases[state.phase] for state in states] == sorted(
            phases[state.phase] for state in states
        )
        previous = 0
        for state in states:
            assert 0 <= state.checked_obligations - previous <= rows
            assert 0 <= state.admitted_octets <= octets
            previous = state.checked_obligations
        assert corpus.advance_dependencies().admitted_octets == 0
        corpus.validate()
        assert corpus.advance_semantics().complete
        assert finish(corpus)[-1].checked_obligations == 51


@pytest.mark.parametrize("name", ["row_budget", "byte_budget"])
@pytest.mark.parametrize("invalid", [0, -1, True, 1.0, "1", None])
def test_invalid_dependency_admission_does_no_work(name, invalid, monkeypatch):
    with CanonicalCorpusValidator() as corpus:
        populated(corpus, 1)

        def forbidden(*_args, **_kwargs):
            pytest.fail("invalid admission attempted a query")

        monkeypatch.setattr(corpus, "_source_read", forbidden)
        monkeypatch.setattr(corpus, "_db", SimpleNamespace())
        with pytest.raises(ValueError):
            corpus.advance_dependencies(**{name: invalid})
        assert corpus._dependency_phase == "references"
        # Restore the owner connection before context cleanup.
        monkeypatch.undo()


def test_dependencies_require_completed_semantics_and_an_open_owner():
    with CanonicalCorpusValidator() as corpus:
        with pytest.raises(RuntimeError, match="semantic registration"):
            corpus.advance_dependencies()
        assert corpus.advance_semantics().complete
        finish(corpus)
        with pytest.raises(RuntimeError, match="frozen"):
            corpus.select_source(None)
    with pytest.raises(RuntimeError, match="closed"):
        corpus.advance_dependencies()


def test_capacity_rejects_before_payload_read_and_retries_pending_atom(monkeypatch):
    with CanonicalCorpusValidator() as corpus:
        populated(corpus, 2)
        read = corpus._source_read

        def forbidden(query, parameters):
            if query.startswith("SELECT rowid,length("):
                return read(query, parameters)
            pytest.fail("capacity rejection fetched a dependency payload")

        with monkeypatch.context() as patch:
            patch.setattr(corpus, "_source_read", forbidden)
            with pytest.raises(JournalValidationCapacity):
                corpus.advance_dependencies(byte_budget=1)
        pending = corpus._dependency_pending
        assert pending is not None and corpus._dependency_checked == 0
        seen = []

        def observed(query, parameters):
            seen.append(query)
            return read(query, parameters)

        with monkeypatch.context() as patch:
            patch.setattr(corpus, "_source_read", observed)
            result = corpus.advance_dependencies(row_budget=1, byte_budget=4096)
        assert result.checked_obligations == 1
        assert corpus._dependency_pending is None
        assert len(seen) == 2
        assert finish(corpus)[-1].checked_obligations == 6


def test_interruption_retains_prior_checked_rows_and_retries_only_pending_reference(monkeypatch):
    class Interrupted(BaseException):
        pass

    with CanonicalCorpusValidator() as corpus:
        populated(corpus, 17)
        resolve = corpus._resolve
        completed = []

        def interrupted(reference):
            if len(completed) == 7:
                raise Interrupted
            resolve(reference)
            completed.append(reference["assertion_id"])

        with monkeypatch.context() as patch:
            patch.setattr(corpus, "_resolve", interrupted)
            with pytest.raises(Interrupted):
                corpus.advance_dependencies()
        assert corpus._dependency_checked == 7

        def resumed(reference):
            assert reference["assertion_id"] not in completed
            resolve(reference)
            completed.append(reference["assertion_id"])

        with monkeypatch.context() as patch:
            patch.setattr(corpus, "_resolve", resumed)
            assert finish(corpus, row_budget=1)[-1].checked_obligations == 51
        assert len(completed) == len(set(completed)) == 17


@pytest.mark.parametrize(
    "field", ["entry", "object_id", "object_type", "assertion_id", "journal_id"]
)
def test_each_foreign_coordinate_remains_required(field):
    with CanonicalCorpusValidator() as corpus:
        reference = populated(corpus, 1)[0]
        corpus._db.execute("DELETE FROM refs")
        reference[field] = {} if field == "entry" else "other"
        corpus._db.execute("INSERT INTO refs VALUES (?)", (canonical_json_bytes(reference),))
        with pytest.raises(ProvenanceValidationError, match="foreign reference|unresolved"):
            finish(corpus)


def test_reference_failure_precedes_fork_comparison_and_cycle_failures():
    with CanonicalCorpusValidator() as corpus:
        populated(corpus, 1)
        corpus._db.execute("DELETE FROM assertions")
        corpus._db.execute("DELETE FROM entries")
        corpus._db.execute("UPDATE fixities SET sha256=? WHERE object='right'", ("b" * 64,))
        corpus._db.execute("INSERT INTO edges VALUES ('derivation','a','a')")
        with pytest.raises(ProvenanceValidationError, match="unresolved"):
            corpus.validate()
        assert corpus._dependency_after is None


@pytest.mark.parametrize("phase", ["references", "forks", "comparisons"])
def test_interruption_retries_the_exact_pending_atom_in_each_phase(phase, monkeypatch):
    with CanonicalCorpusValidator() as corpus:
        populated(corpus, 17)
        while corpus._dependency_phase != phase:
            corpus.advance_dependencies()
        before = corpus._dependency_checked
        checked = []
        check = corpus._check_dependency

        def interrupted(pending, size):
            if len(checked) == 7:
                raise InterruptedError("pending dependency")
            check(pending, size)
            checked.append(pending)

        with monkeypatch.context() as patch:
            patch.setattr(corpus, "_check_dependency", interrupted)
            with pytest.raises(InterruptedError):
                corpus.advance_dependencies()
        assert corpus._dependency_checked == before + 7
        pending = corpus._dependency_pending
        assert pending not in checked
        state = corpus.advance_dependencies(row_budget=1)
        assert state.checked_obligations == before + 8
        assert finish(corpus)[-1].checked_obligations == 51


@pytest.mark.parametrize("field", ["through", "prefix_sha256", "prefix_bytes", "journal_id"])
def test_every_fork_coordinate_is_required(field):
    with CanonicalCorpusValidator() as corpus:
        populated(corpus, 1)
        (encoded,) = corpus._db.execute("SELECT anchor FROM forks").fetchone()
        anchor = json.loads(encoded)
        if field == "through":
            anchor[field]["sequence"] = "other"
        else:
            anchor[field] = "other"
        corpus._db.execute("DELETE FROM forks")
        corpus._db.execute("INSERT INTO forks VALUES (?)", (canonical_json_bytes(anchor),))
        with pytest.raises(ProvenanceValidationError, match="fork prefix"):
            finish(corpus)


def test_closing_owner_releases_pending_metadata():
    with CanonicalCorpusValidator() as corpus:
        populated(corpus, 17)
        corpus.advance_dependencies(row_budget=1)
        assert corpus._dependency_after is not None
    assert corpus._dependency_after is None


def test_real_native_fork_anchor_is_checked_in_bounded_steps(journal, who, catalog):
    with validate_journal(journal, catalog=catalog) as parent:
        fork = create_journal(
            {"agents": parent.graph["agents"]},
            recorded_by_agent_id=who,
            forked_from=parent.anchor,
            catalog=catalog,
        )
        with validate_journal(fork, catalog=catalog) as child, CanonicalCorpusValidator() as corpus:
            corpus.add(parent)
            corpus.add(child)
            while not corpus.advance_semantics(row_budget=1).complete:
                pass
            assert finish(corpus, row_budget=1)[-1].checked_obligations == 1
            corpus.validate()


class Cursor:
    def __init__(self, actual, observer, query):
        self.actual, self.observer, self.query = actual, observer, query
        self.closed = False

    def fetchone(self):
        row = self.actual.fetchone()
        if row is not None and self.query.startswith("SELECT substr("):
            self.observer.metrics["projected_payload_octets"] += len(row[0])
        if row is not None and self.query.startswith("SELECT left_object,"):
            self.observer.metrics["projected_payload_octets"] += sum(
                len(item.encode()) for item in row
            )
        return row

    def close(self):
        self.actual.close()
        self.closed = True


class Observer:
    def __init__(self, actual):
        self.actual, self.metrics, self.cursors = actual, defaultdict(int), []

    def execute(self, query, parameters=()):
        self.metrics["sql_statements"] += 1
        assert "assertion_payloads" not in query
        assert not query.startswith(("INSERT", "UPDATE", "DELETE"))
        cursor = Cursor(self.actual.execute(query, parameters), self, query)
        self.cursors.append(cursor)
        return cursor


@pytest.mark.growth_witness(
    "native-corpus-dependency-advance-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus.CanonicalCorpusValidator.advance_dependencies",
)
def test_actual_dependency_calls_bound_visits_and_payloads_without_repeated_completed_rows(
    monkeypatch, record_property
):
    operation = CanonicalCorpusValidator.advance_dependencies
    binding = growth_binding(operation)
    projection = project_bindings((binding,))
    samples = []
    for count, rows, octets in ((0, 1, 256), (1, 1, 256), (17, 7, 256), (129, 17, 4096)):
        with CanonicalCorpusValidator() as corpus:
            populated(corpus, count)
            before_samples = len(samples)
            observer = Observer(corpus._db)

            def decoded(value, observer=observer):
                observer.metrics["json_decodes"] += 1
                return json.loads(value)

            with source_reads(monkeypatch) as frames, monkeypatch.context() as patch:
                patch.setattr(corpus, "_db", observer)
                patch.setattr(owner, "json", SimpleNamespace(loads=decoded))
                previous = 0
                for _ in range(3 * count + 5):
                    observer.metrics.clear()
                    state = corpus.advance_dependencies(row_budget=rows, byte_budget=octets)
                    observer.metrics["processed_rows"] = state.checked_obligations - previous
                    previous = state.checked_obligations
                    observer.metrics["source_frame_reads"] = len(frames)
                    samples.append(
                        {
                            "declaration": binding.declaration.id,
                            "declaration_sha256": hashlib.sha256(
                                canonical_json_bytes(
                                    projection["declarations"][binding.declaration.id]
                                )
                            ).hexdigest(),
                            "dimensions": {"row_budget": rows, "byte_budget": octets},
                            "metrics": {
                                bound.metric: observer.metrics[bound.metric]
                                for bound in binding.declaration.bounds
                            },
                            "delegates": {},
                        }
                    )
                    if state.complete:
                        observer.metrics.clear()
                        assert corpus.advance_dependencies().admitted_octets == 0
                        assert observer.metrics["sql_statements"] == 0
                        break
                else:
                    pytest.fail("bounded dependency walk did not terminate")
            assert state.checked_obligations == 3 * count
            assert (
                sum(sample["metrics"]["sql_statements"] for sample in samples[before_samples:])
                == 9 * count + 3
            )
            assert all(cursor.closed for cursor in observer.cursors)
    validate_execution_samples(projection, binding.declaration.id, samples)
    record_property("growth_samples", samples)


@pytest.mark.parametrize("phase", ["references", "forks", "comparisons"])
def test_actual_sqlite_cancellation_cannot_turn_unread_dependencies_into_completion(phase):
    with CanonicalCorpusValidator() as corpus:
        populated(corpus, 17)
        corpus._db.commit()
        while corpus._dependency_phase != phase:
            corpus.advance_dependencies()
        checked = corpus._dependency_checked
        first = corpus.advance_dependencies(row_budget=7)
        assert first.checked_obligations == checked + 7
        after = corpus._dependency_after
        corpus._db.set_progress_handler(lambda: 1, 1)
        try:
            with pytest.raises(sqlite3.OperationalError, match="interrupted"):
                corpus.advance_dependencies(row_budget=1)
        finally:
            corpus._db.set_progress_handler(None, 0)
        assert corpus._dependency_after == after
        assert finish(corpus, row_budget=1)[-1].checked_obligations == 51


def test_dependency_resume_keys_use_owning_indexes_without_offset_scans():
    with CanonicalCorpusValidator() as corpus:
        populated(corpus, 17)
        queries = (
            ("SELECT rowid,length(value) FROM refs WHERE rowid>? ORDER BY rowid LIMIT 1", (7,)),
            ("SELECT rowid,length(anchor) FROM forks WHERE rowid>? ORDER BY rowid LIMIT 1", (7,)),
            (
                "SELECT journal,assertion FROM comparisons WHERE (journal,assertion)>(?,?) "
                "ORDER BY journal,assertion LIMIT 1",
                ("journal", "7"),
            ),
        )
        for query, parameters in queries:
            plan = " ".join(
                str(row[-1])
                for row in corpus._db.execute("EXPLAIN QUERY PLAN " + query, parameters)
            )
            assert "SEARCH" in plan
            assert "SCAN" not in plan and "TEMP B-TREE" not in plan
