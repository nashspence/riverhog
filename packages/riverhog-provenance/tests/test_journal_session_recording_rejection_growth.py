from __future__ import annotations

import sqlite3
from copy import deepcopy

import pytest
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    JournalValidationSession,
    ProvenanceValidationError,
    append_assertions,
    assertion,
    create_journal,
    encode_entry,
    new_id,
    parse_journal,
    reference,
)
from riverhog_provenance._entry_replay import EntryReplay
from riverhog_provenance.journal_store import JournalStore
from test_journal_session_growth import (
    ATTEMPT,
    FiniteSource,
    JournalValidationBudget,
    SessionConnection,
    SourcePorts,
    observed_work,
    rejected_recording_dimensions,
    session_observer,
    snapshot,
)

ADVANCE = JournalValidationSession.advance


def recording_source(kind, payload, catalog, *, journal_id=None):
    observed = BoundedSourceObserver(catalog=catalog).observe(BytesSource(b"recording rejection"))
    graph = observed.graph_fragment()
    first = create_journal(
        graph,
        recorded_by_agent_id=observed.observer_agent_id,
        catalog=catalog,
        journal_id=journal_id,
    )
    extension = assertion(
        "extension",
        observed.observer_agent_id,
        subject=reference(graph["artifacts"][0]["id"], "artifact"),
        property="urn:test:recording-rejection",
        value={"type": "text", "value": "x" * payload},
    )
    valid = append_assertions(
        first,
        {"extensions": [extension]},
        recorded_by_agent_id=observed.observer_agent_id,
        catalog=catalog,
    )
    last = deepcopy(parse_journal(valid)[-1].document)
    last["recorded_by_agent_id" if kind == "recorder" else "recording_context_id"] = new_id()
    return first + encode_entry(last, catalog=catalog)


def recording_samples(root, catalog, *, samples, large=True):
    specifications = [(64, 0)]
    if large:
        specifications.extend(((8192, 65537), (64, 1024 * 1024)))
    connect = sqlite3.connect
    for width, payload in specifications:
        for kind, message in (
            ("recorder", "entry recorder must resolve to an effective agent"),
            ("context", "recording context does not resolve"),
        ):
            raw = recording_source(kind, payload, catalog)
            source = FiniteSource(raw, parse_journal(raw)[0], 2, catalog, directory_width=width)
            directory = root / f"{width}-{payload}-{kind}"
            directory.mkdir(parents=True)

            def observed_connect(*args, **kwargs):
                assert "factory" not in kwargs
                kwargs["factory"] = SessionConnection
                return connect(*args, **kwargs)

            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(sqlite3, "connect", observed_connect)
                with JournalValidationSession.open_retained(
                    directory,
                    source,
                    attempt_id=ATTEMPT,
                    worker_epoch=0,
                    create=True,
                    catalog=catalog,
                ) as session:
                    db = session._store.db
                    session._source = session._input.source = SourcePorts(source, db)
                    for _ in range(10000):
                        before = session._ledger.load()
                        if (
                            before.checkpoint.phase == "recording"
                            and before.checkpoint.frame_ordinal == 1
                        ):
                            break
                        assert not session.advance(JournalValidationBudget(rows=1)).complete
                    else:
                        pytest.fail("finite source did not reach the selected recording predicate")
                    original = snapshot(db)
                    input_state = session._input.state
                    dimensions, _ = rejected_recording_dimensions(
                        session, JournalValidationBudget()
                    )
                    source.calls.clear()
                    with db.observe():
                        prior, changes, start = (
                            db.metrics.copy(),
                            db.total_changes,
                            len(db.statements),
                        )
                        with session_observer(
                            session,
                            dimensions,
                            ADVANCE,
                            failed_operations=(
                                EntryReplay.advance,
                                JournalStore._check_entry_recording,
                            ),
                        ) as (frame, complete):
                            with pytest.raises(
                                ProvenanceValidationError, match=message
                            ) as rejected:
                                session.advance()
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
                    assert session._failure is rejected.value and not session._closed
                    assert snapshot(db) == original and session._input.state is input_state
                    assert session._ledger.load() == before
                    assert session.progress().accepted_frames == 1
                    assert source.calls == [("verify",), ("verify",)]
                    assert (
                        sample["metrics"]["changed_rows"]
                        == sample["metrics"]["source_port_octets"]
                        == 0
                    )
                    assert not sample["delegates"]["progress"]
                    execution = sample["delegates"]["entry_advance"][0]
                    assert len(execution["delegates"]["recording"]) == 1
                    assert (
                        not execution["delegates"]["fence"]
                        and not execution["delegates"]["publication"]
                    )
                    source.calls.clear()
                    with pytest.raises(RuntimeError, match="session failed"):
                        session.advance()
                    assert not source.calls and snapshot(db) == original


@pytest.mark.growth_witness(
    "riverhog-native-recording-rejection-work/v1",
    gates=("unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.journal_session.JournalValidationSession.advance",
)
def test_actual_recording_rejection_keeps_last_accepted_prefix(tmp_path, catalog, record_property):
    samples = []
    record_property("growth_samples", samples)
    recording_samples(tmp_path, catalog, samples=samples)
