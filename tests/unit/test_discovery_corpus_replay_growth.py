from __future__ import annotations

from contextlib import contextmanager

import pytest
from riverhog_core.services import discovery_validation_owner as owner
from riverhog_growth_contracts import growth_binding
from riverhog_provenance import JournalSummary, JournalValidationSession

from tests.unit.test_canonical_discovery_corpus_source import case as case
from tests.unit.test_canonical_discovery_corpus_source import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_corpus_source import original as original
from tests.unit.test_canonical_discovery_corpus_source import qualified as qualified
from tests.unit.test_canonical_discovery_corpus_source import seed
from tests.unit.test_discovery_corpus_replay import prepare_source, progress
from tests.unit.test_discovery_validation_owner import unit
from tests.unit.test_journal_validation_workspace_reference_growth import sample

RETAIN = owner.DiscoveryValidationWork.corpus_replay
NEXT = owner.DiscoveryValidationWork.next_corpus_source
ADVANCE = owner.DiscoveryValidationWork.advance_corpus


@contextmanager
def observe(operation, monkeypatch):
    metrics = {bound.metric: 0 for bound in growth_binding(operation).declaration.bounds}
    depth = 0

    def wrap(function, metric):
        def measured(*args, **kwargs):
            nonlocal depth
            if depth == 0 and metric in metrics:
                metrics[metric] += 1
            depth += 1
            try:
                return function(*args, **kwargs)
            finally:
                depth -= 1

        return measured

    targets = (
        (owner.DiscoveryValidationWork, "corpus", "corpus_dispatches"),
        (owner.DiscoveryValidationWork, "corpus_replay", "conductor_dispatches"),
        (owner.DiscoveryMetadata, "read", "metadata_reads"),
        (owner.DiscoveryMetadata, "transaction", "metadata_transactions"),
        (owner, "frozen_corpus_selection", "freeze_calls"),
        (owner.replay.CorpusReplayWork, "__init__", "conductor_constructions"),
        (owner.replay, "begin_corpus_replay", "conductor_begins"),
        (owner.replay, "next_corpus_replay_source", "next_source_calls"),
        (owner.replay, "prepare_corpus_replay", "conductor_prepares"),
        (owner.replay, "apply_corpus_replay", "conductor_applies"),
        (owner.DiscoverySealedJournalOwner, "borrow", "sealed_borrows"),
        (owner.DiscoveryJournalOwner, "close", "journal_closes"),
        (JournalValidationSession, "progress", "native_progress_calls"),
        (JournalValidationSession, "result", "native_results"),
        (JournalSummary, "close", "summary_closes"),
    )
    from riverhog_core.services.discovery_journal_owner import DiscoveryJournalCustodySource

    with monkeypatch.context() as patch:
        for module, name, metric in (
            *targets,
            (DiscoveryJournalCustodySource, "verify_current", "source_checks"),
        ):
            patch.setattr(module, name, wrap(getattr(module, name), metric))
        yield metrics


def measure(work, selection, operation, values, monkeypatch, samples):
    with observe(operation, monkeypatch) as metrics:
        result = (
            operation(work, selection, row_budget=1, byte_budget=65536)
            if operation is ADVANCE
            else operation(work, selection)
        )
    samples.append(sample(operation, values, metrics))
    return result


def exercise(case, qualified, retained, monkeypatch, record_property, operation):
    samples = []
    app = owner.DiscoveryValidationOwner(case.factory, case.root)
    try:
        with unit(app, case) as work:
            selection, _ = seed(work.metadata, qualified)
            with work.metadata.transaction() as (db, key):
                source = db.execute(
                    "SELECT journal_id,source_key,source_binding_json,anchor_json "
                    "FROM discovery_corpus_source_inputs WHERE build_id=? AND admission_sha256=?",
                    (key.build_id, key.admission_sha256),
                ).fetchone()
                for ordinal in range(retained):
                    db.execute(
                        "INSERT INTO discovery_corpus_source_inputs VALUES(?,?,?,?,?,?)",
                        ("independent-" + str(ordinal), key.admission_sha256, *source),
                    ).close()
                observed = db.execute(
                    "SELECT count(*) FROM discovery_corpus_source_inputs"
                ).fetchone()[0]
            assert observed == retained + 1
            values = {"retained_sources": observed}
            first = measure(work, selection, operation, values, monkeypatch, samples)
            if operation is ADVANCE:
                assert first is None
                assert work._slot.journal is None and work._slot.replay_summary is None
            prepare_source(work, selection)
            phases = set()
            for _ in range(1000):
                before = progress(work, selection)
                phases.add(before.phase)
                result = measure(work, selection, operation, values, monkeypatch, samples)
                if before.phase == "complete":
                    for _retry in range(2):
                        measure(work, selection, operation, values, monkeypatch, samples)
                    break
                after = (
                    result
                    if operation is ADVANCE
                    else work.advance_corpus(selection, row_budget=1, byte_budget=65536)
                )
                assert after is not None and after.version == before.version + 1
            else:
                pytest.fail("finite original corpus did not complete")
            assert phases == {
                "sources",
                "selections",
                "semantics",
                "dependencies",
                "derivation",
                "specialization",
                "complete",
            }
    finally:
        app.close()
    record_property("growth_samples", samples)


@pytest.mark.parametrize("retained", [1, 17, 1025])
@pytest.mark.growth_witness(
    "riverhog-discovery-corpus-replay-retain-dispatch/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.discovery_validation_owner.DiscoveryValidationWork.corpus_replay",
)
def test_actual_cold_and_retained_conductor_ports(
    case, qualified, retained, monkeypatch, record_property
):
    exercise(case, qualified, retained, monkeypatch, record_property, RETAIN)


@pytest.mark.parametrize("retained", [1, 17, 1025])
@pytest.mark.growth_witness(
    "riverhog-discovery-corpus-replay-next-dispatch/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation=(
        "riverhog_core.services.discovery_validation_owner.DiscoveryValidationWork.next_corpus_source"
    ),
)
def test_actual_original_full_source_request_ports(
    case, qualified, retained, monkeypatch, record_property
):
    exercise(case, qualified, retained, monkeypatch, record_property, NEXT)


@pytest.mark.parametrize("retained", [1, 17, 1025])
@pytest.mark.growth_witness(
    "riverhog-discovery-corpus-replay-advance-dispatch/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.discovery_validation_owner.DiscoveryValidationWork.advance_corpus",
)
def test_actual_pending_source_and_all_native_phase_dispatch_ports(
    case, qualified, retained, monkeypatch, record_property
):
    exercise(case, qualified, retained, monkeypatch, record_property, ADVANCE)
