from __future__ import annotations

import sqlite3

import pytest
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import JournalValidationCapacity, JournalValidationSession, parse_journal
from test_journal_session_growth import (
    ATTEMPT,
    FiniteSource,
    JournalValidationBudget,
    SessionConnection,
    SourcePorts,
    observed_work,
    rejected_input_dimensions,
    session_observer,
    snapshot,
)

ADVANCE = JournalValidationSession.advance


def capacity_samples(root, catalog, raw_source, *, samples, large=True):
    specifications = [(64, "metadata", 0), (64, "atom", 65537)]
    if large:
        specifications.extend(((8192, "metadata", 0), (8192, "atom", 1024 * 1024)))
    connect = sqlite3.connect
    for width, kind, payload in specifications:
        raw = raw_source(payload)
        source = FiniteSource(raw, parse_journal(raw)[0], 1, catalog, directory_width=width)
        directory = root / f"{width}-{kind}"
        directory.mkdir(parents=True)

        def observed_connect(*args, **kwargs):
            assert "factory" not in kwargs
            kwargs["factory"] = SessionConnection
            return connect(*args, **kwargs)

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(sqlite3, "connect", observed_connect)
            session = JournalValidationSession.open_retained(
                directory, source, attempt_id=ATTEMPT, worker_epoch=1, create=True, catalog=catalog
            )
            db = session._store.db
            session._source = session._input.source = SourcePorts(source, db)
            try:
                pending = None
                if kind == "atom":
                    for _ in range(10000):
                        if session._input.pending:
                            break
                        assert not session.advance().complete
                    else:
                        pytest.fail("finite retained source did not expose its pending atom")
                    pending = session._input.state.pending
                    budget = JournalValidationBudget(rows=1, octets=2 * pending.json_bytes + 65535)
                else:
                    budget = JournalValidationBudget(rows=1, octets=16384, source_octets=16384)
                original = snapshot(db)
                input_state = session._input.state
                for _ in range(2):
                    source.calls.clear()
                    dimensions, _ = rejected_input_dimensions(session, budget, kind)
                    with db.observe():
                        prior, changes, start = (
                            db.metrics.copy(),
                            db.total_changes,
                            len(db.statements),
                        )
                        with session_observer(session, dimensions, ADVANCE) as (frame, complete):
                            with pytest.raises(JournalValidationCapacity):
                                session.advance(budget)
                        work = observed_work(db, prior, changes, start)
                        work.update(
                            indexed_containers=db.all_node_containers,
                            index_python_opcode_events=db.all_node_opcodes,
                        )
                        sample = complete(frame, work)
                        validate_execution_samples(
                            project_bindings((growth_binding(ADVANCE),)),
                            growth_binding(ADVANCE).declaration.id,
                            [sample],
                        )
                    samples.append(sample)
                    assert snapshot(db) == original
                    assert session._input.state is input_state
                    assert session._failure is None and not session._closed
                    assert source.calls == [("verify",)]
                    assert (
                        sample["metrics"]["source_port_octets"]
                        == sample["metrics"]["changed_rows"]
                        == 0
                    )
                    assert (
                        sample["metrics"]["source_atoms"]
                        == sample["metrics"]["source_windows"]
                        == 0
                    )
                    assert len(sample["delegates"]["load"]) == 1
                    assert all(
                        not records for tag, records in sample["delegates"].items() if tag != "load"
                    )
                if kind == "atom":
                    assert session._input.state.pending is pending
                    source.calls.clear()
                    progress = session.advance()
                    assert progress.version > 0 and session._failure is None
                    assert source.calls == [
                        ("verify",),
                        ("atom", pending.byte_offset + 1, pending.json_bytes),
                    ]
            finally:
                session.close()


@pytest.mark.growth_witness(
    "riverhog-native-retained-input-capacity-work/v1",
    gates=("unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.journal_session.JournalValidationSession.advance",
)
def test_actual_input_capacity_preserves_checkpoint_and_pending_atom(
    tmp_path, catalog, record_property
):
    from riverhog_provenance import (
        BoundedSourceObserver,
        BytesSource,
        assertion,
        create_journal,
        reference,
    )

    def source(payload):
        observed = BoundedSourceObserver().observe(BytesSource(b"native retained input capacity"))
        graph = observed.graph_fragment()
        if payload:
            graph["extensions"] = [
                assertion(
                    "extension",
                    observed.observer_agent_id,
                    subject=reference(graph["artifacts"][0]["id"], "artifact"),
                    property="urn:test:input-capacity",
                    value={"type": "text", "value": "x" * payload},
                )
            ]
        return create_journal(graph, recorded_by_agent_id=observed.observer_agent_id)

    samples = []
    record_property("growth_samples", samples)
    capacity_samples(tmp_path, catalog, source, samples=samples)
