from __future__ import annotations

import pytest
from native_units import unit_budget
from riverhog_provenance._generic_semantics import GenericSemantics, StaleGenericSemantics
from riverhog_provenance.journal_session import JournalValidationSession
from test_entry_replay_growth import admission_histories as admission_histories
from test_journal_session_growth import (
    SESSION_CASES,
    session_cases,
    session_dimensions,
    session_fixture,
)


@pytest.fixture(scope="module")
def checkpoints(tmp_path_factory, catalog, admission_histories, module_journal_constructions):
    root = tmp_path_factory.mktemp("session-fixture-custody")

    def unexpected_graph(_specification):
        pytest.fail("the selected small source has no graph-correction variant")

    return session_cases(
        root,
        catalog,
        admission_histories,
        unexpected_graph,
        specifications=SESSION_CASES[:2],
        journal_constructions=module_journal_constructions,
    )


@pytest.mark.parametrize("mode", ["stream", "retained"])
@pytest.mark.parametrize("measured", [False, True])
def test_session_growth_setup_reconstructs_current_atom_before_dimensions_or_native_work(
    catalog, checkpoints, journal_constructions, mode, measured
):
    case = next(
        case
        for case in checkpoints
        if case["mode"] == mode and case["before"].checkpoint.phase == "generic"
    )
    assert case["cached"].atom is not None
    with session_fixture(
        case,
        catalog,
        cold=False,
        measured=measured,
        journal_constructions=journal_constructions,
    ) as session:
        replay, epoch, storage = session._replay, session._ledger.epoch, session._store._storage
        before, current = session._ledger.load(), replay._current
        assert current is not case["cached"] and current.frame is not case["cached"].frame
        assert current.frame._epoch is epoch and current.atom._epoch is epoch
        assert current.atom is not case["cached"].atom
        assert current.atom.encoded == case["cached"].atom.encoded
        assert current.atom.nodes.values[0] is current.row
        with pytest.raises(StaleGenericSemantics, match="another Native executor"):
            GenericSemantics(
                session._store.db,
                attempt_id="foreign-fixture-atom",
                source_binding=current.source,
                atom=case["cached"].atom,
                catalog=catalog,
                epoch=epoch,
            )
        child = replay._child(
            before.checkpoint, current, before.checkpoint.phase, before.checkpoint.child_id
        )
        assert child.epoch is epoch and child.atom is current.atom
        budget = unit_budget(rows=16)
        dimensions, _ = session_dimensions(
            session, budget, JournalValidationSession.advance, cold=False
        )
        assert dimensions
        progress = session.advance(budget)
        assert progress.version > before.checkpoint.version and session._failure is None
        assert session._ledger.epoch.quiescent
    assert storage._closed and storage._readers == 0 and session.storage_received
