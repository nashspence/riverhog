from __future__ import annotations

import json
from dataclasses import replace

import pytest
import riverhog_growth_contracts as growth
from riverhog_core.services import collection_uploads as source_owner
from riverhog_core.services import upload_journal_validation_owner as owner
from riverhog_provenance.journal_session import (
    JournalValidationBudget,
    JournalValidationSession,
    JournalValidationWorkBudget,
)

from tests.unit.test_journal_capture import retained
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_staged_journal_range import JOURNAL
from tests.unit.test_upload_journal_certificates import ready

LIMITS = (
    "current_calls_limit",
    "window_calls_limit",
    "exact_calls_limit",
    "certificate_calls_limit",
    "source_read_calls_limit",
)


@pytest.mark.parametrize("transitions", (1, 4, 16))
def test_actual_core_source_callbacks_share_the_original_transition_allowance(
    tmp_path, journal_constructions, monkeypatch, numeric_policy, record_property, transitions
):
    budget = JournalValidationBudget(
        source_octets=7, work=JournalValidationWorkBudget(transitions=transitions)
    )
    binding = growth.growth_binding(owner.UploadJournalValidationOwner.advance)
    dimensions = {item.name: 0 for item in binding.declaration.dimensions}
    dimensions.update(row_budget=budget.rows, transition_budget=budget.work.transitions)
    ceilings = {
        metric: growth.evaluate_bound(binding, metric, dimensions, numeric_policy=numeric_policy)
        for metric in LIMITS
    }
    observations = []
    with retained(tmp_path, source(journal_id=JOURNAL)) as (engine, factory):
        selected, _ = ready(factory)
        current = owner.UploadJournalValidationOwner(
            factory, tmp_path / "native", construction_factory=journal_constructions.retain
        )
        catalog = source_owner.admission_provenance_catalog()
        try:
            assert current.advance(
                selected,
                catalog=catalog,
                budget=JournalValidationBudget(
                    source_octets=7, work=JournalValidationWorkBudget(transitions=1)
                ),
            ).progressed
            calls = {name: 0 for name in ("current", "window", "exact", "certificate")}
            units = []

            def counted(name, operation):
                def invoke(*args, **kwargs):
                    calls[name] += 1
                    return operation(*args, **kwargs)

                return invoke

            for name, attribute in (
                ("current", "verify_current"),
                ("window", "read_window"),
                ("exact", "read_exact"),
            ):
                monkeypatch.setattr(
                    owner._OwnedSource,
                    attribute,
                    counted(name, getattr(owner._OwnedSource, attribute)),
                )
            monkeypatch.setattr(
                owner.UploadJournalGraphCertificates,
                "edge",
                counted("certificate", owner.UploadJournalGraphCertificates.edge),
            )
            advance_unit = JournalValidationSession._advance_unit

            def advance(session, selected_budget):
                units.append(session._ledger.epoch.work)
                return advance_unit(session, selected_budget)

            monkeypatch.setattr(JournalValidationSession, "_advance_unit", advance)
            for _ in range(3):
                for name in calls:
                    calls[name] = 0
                units.clear()
                result = current.advance(selected, catalog=catalog, budget=budget)
                assert result.progressed and result.outcome is None
                assert 1 <= len(units) <= transitions
                quantum = units[0]
                assert quantum is not None and all(item is quantum for item in units)
                assert quantum.consumed.transitions == len(units)
                assert quantum.consumed.fits(quantum.limits)
                quantum.require_received()
                for name, actual in calls.items():
                    assert actual <= ceilings[name + "_calls_limit"]
                assert calls["window"] + calls["exact"] <= ceilings["source_read_calls_limit"]
                assert engine.pool.checkedout() == 0
                observations.append({"calls": dict(calls), "transitions": len(units)})
            assert max(row["calls"]["window"] for row in observations) == transitions
        finally:
            current.close()
    record_property("native_quantum_owner_callbacks", json.dumps(observations, sort_keys=True))


@pytest.mark.parametrize("metric", LIMITS)
@pytest.mark.parametrize("transitions", (1, 4, 16))
def test_owner_call_limit_cannot_expand_with_observed_child_count(
    metric, transitions, numeric_policy
):
    original = growth.growth_binding(owner.UploadJournalValidationOwner.advance).declaration
    declaration = replace(
        original,
        id="fixture.native-quantum-call-limits.growth/v1",
        authority="fixture",
        subjects=("fixture.native_quantum_call_limits",),
        dimensions=tuple(
            item for item in original.dimensions if item.name in {"row_budget", "transition_budget"}
        ),
        bounds=tuple(bound for bound in original.bounds if bound.metric in LIMITS),
    )

    @growth.declare_growth(declaration)
    def operation():
        return None

    binding = growth.growth_binding(operation)
    dimensions = {"row_budget": 64, "transition_budget": transitions}
    metrics = {
        bound.metric: growth.evaluate_bound(
            binding, bound.metric, dimensions, numeric_policy=numeric_policy
        )
        for bound in declaration.bounds
    }
    projection = growth.project_bindings((binding,), numeric_policy=numeric_policy)
    growth.validate_sample(
        projection, declaration.id, dimensions, metrics, numeric_policy=numeric_policy
    )
    metrics[metric] += 1
    with pytest.raises(ValueError, match="primitive observations violate"):
        growth.validate_sample(
            projection, declaration.id, dimensions, metrics, numeric_policy=numeric_policy
        )
