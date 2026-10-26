from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from riverhog_core import canonical_discovery_state_source_work as owner
from riverhog_core.canonical_discovery_selector_sources import SelectorSourceReads
from riverhog_core.canonical_discovery_selector_work import SelectorCapacity

from tests.support.canonical_discovery_work import write
from tests.unit.test_canonical_discovery_activity_selector_growth import observed_activity
from tests.unit.test_canonical_discovery_history_selector_growth import dimensions
from tests.unit.test_canonical_discovery_selector_growth import retain
from tests.unit.test_canonical_discovery_state_source import (
    APPLY,
    RESOLVE,
    compiler,
    copied,
    graph,
    prepare,
    progress,
    reopen,
    seed,
)
from tests.unit.test_canonical_discovery_state_source import state_originals as state_originals
from tests.unit.test_journal_validation_workspace_reference_growth import sample


@contextmanager
def observed(case, monkeypatch):
    equal = owner._equal_binding
    with observed_activity(case, monkeypatch) as recorder:

        def compared(left, right):
            recorder.metrics["compared_octets"] += len(left.encode()) + len(right.encode())
            return equal(left, right)

        with monkeypatch.context() as patch:
            patch.setattr(owner, "_equal_binding", compared)
            yield recorder


def exercise(original, tmp_path, monkeypatch, record_property, operation):
    samples = []
    for backlog in (1, 17, 1025, 8193):
        case = copied(original, tmp_path / f"state-{backlog}.sqlite3")
        case.additional_sources = (case.foreign_source,)
        try:
            retain(SimpleNamespace(db=case.db, key=case.archive_key), backlog)
            before = seed(case)
            work = compiler(case)
            values = dimensions(
                case,
                before,
                work._binding_json,
                work._foreign_binding_json,
                case.reference.model_dump(mode="json"),
            )
            if operation is RESOLVE:
                for address in (case.local_reference, case.reference):
                    for cold in (False, True):
                        if cold:
                            reopen(case)
                            work = compiler(case)
                        with observed(case, monkeypatch) as recorder:
                            reads = SelectorSourceReads(recorder, octet_budget=work.octet_budget)
                            selected = RESOLVE(reads, case.archive_key, work, address)
                        assert recorder.metrics["sql_statements"] > 0
                        assert recorder.metrics["returned_rows"] > 0
                        values["metadata_octets"] = max(
                            values["metadata_octets"],
                            *(len(row.exact_json.encode()) for row in reads.receipts),
                        )
                        samples.append(sample(operation, values, recorder.metrics))
                        assert selected.object_id == address.object_id
                        assert selected.scope == "input-history"
                for failure in ("missing", "capacity", "closed"):
                    work = compiler(case, foreign=failure != "missing")
                    if failure == "closed":
                        case.foreign_source.close()
                    from riverhog_core.canonical_discovery_journal_work import StaleJournalTask

                    error = {
                        "missing": owner.StateSourceRequired,
                        "capacity": SelectorCapacity,
                        "closed": StaleJournalTask,
                    }[failure]
                    with observed(case, monkeypatch) as recorder:
                        reads = SelectorSourceReads(
                            recorder,
                            octet_budget=32 if failure == "capacity" else work.octet_budget,
                        )
                        with pytest.raises(error):
                            RESOLVE(reads, case.archive_key, work, case.reference)
                    samples.append(sample(operation, values, recorder.metrics))
            else:
                for cold in (False, True):
                    if cold:
                        reopen(case)
                        work = compiler(case)
                    prepared = prepare(case, work, before)
                    values.update(
                        source_controls=len(prepared.sources),
                        control_octets=max(
                            len(row.exact_json.encode()) for row in prepared.sources
                        ),
                    )
                    case.db.execute("BEGIN IMMEDIATE").close()
                    with observed(case, monkeypatch) as recorder:
                        APPLY(recorder, case.archive_key, work, prepared)
                    samples.append(sample(operation, values, recorder.metrics))
                    case.db.rollback()
                    assert (
                        progress(SimpleNamespace(db=case.db, key=case.archive_key), before.selector)
                        == before
                    )
                for _retry in range(2):
                    with write(case.db):
                        with observed(case, monkeypatch) as recorder:
                            APPLY(recorder, case.archive_key, work, prepared)
                    samples.append(sample(operation, values, recorder.metrics))
                assert graph(case)[1] == {(before.selector, prepared.page.output)}
        finally:
            case.source.close()
            case.foreign_source.close()
            case.db.close()
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-state-input-exact-source/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_state_source_work.state_input_selector",
)
def test_exact_input_source_lookup_bounds_retained_rows_failures_and_cold_prefixes(
    state_originals,
    tmp_path,
    monkeypatch,
    record_property,
):
    exercise(state_originals, tmp_path, monkeypatch, record_property, RESOLVE)


@pytest.mark.growth_witness(
    "riverhog-state-input-exact-replay/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_selector_work.apply_journal_selector",
)
def test_two_source_replay_rollback_retry_and_cold_cursor_keep_shared_apply_bounds(
    state_originals,
    tmp_path,
    monkeypatch,
    record_property,
):
    exercise(state_originals, tmp_path, monkeypatch, record_property, APPLY)
