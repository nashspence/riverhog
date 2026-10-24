from __future__ import annotations

import hashlib
from contextlib import ExitStack, contextmanager
from dataclasses import asdict, replace
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.catalog_journal_validation_models import JournalValidationJobRecord as Job
from riverhog_core.checkpoint_sha256 import CheckpointSHA256
from riverhog_core.services import upload_journal_validation_jobs as jobs
from riverhog_core.services import upload_journal_validation_retirement as owner
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from sqlalchemy import delete

from tests.unit.test_journal_capture import retained
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_staged_journal_range import JOURNAL
from tests.unit.test_upload_journal_source_growth import octets
from tests.unit.test_upload_journal_validation_jobs_growth import Recorder, snapshot

ONE, TWO, THREE = (letter * 32 for letter in "abc")
PHASES = ("directory", "retiring", "orphan")


def fields(identity, *, state="active", width=0, epoch=0, linked=None):
    return {
        "workspace_id": identity,
        "collection_id": 1 if linked is not None else None,
        "journal_id": linked,
        "attempt_id": "retired-attempt",
        "source_binding_json": "opaque source " + "s" * width,
        "certificate_binding_json": "opaque certificate " + "c" * width,
        "worker_epoch": epoch,
        "version": "0" * 64,
        "state": state,
        "progress_json": "opaque progress " + "p" * width,
        "outcome_json": "opaque outcome " + "o" * width,
        "failure": "previous blocker " + "f" * width,
    }


@contextmanager
def build_cases(root, *, large=True):
    specifications = [(0, 0, 0, 0), (1, 1, 0, 0), (17, 1, 0, 0)]
    if large:
        specifications.extend(
            ((1025, 1, 0, 0), (1, 17, 0, 0), (1, 1025, 0, 0), (1, 1, 65537, 0), (1, 1, 0, 2**60))
        )
    with ExitStack() as resources:
        cases = []
        for index, (linked, orphans, width, epoch) in enumerate(specifications):
            path = root / str(index)
            engine, factory = resources.enter_context(retained(path, source(journal_id=JOURNAL)))
            factory.configure(autoflush=False, expire_on_commit=False)
            identities = [JOURNAL] + [
                f"urn:uuid:00000000-0000-0000-0000-{number:012x}" for number in range(1, linked)
            ]
            with factory() as session, session.begin():
                session.add_all(
                    jobs.Journal(
                        collection_id=1,
                        journal_id=identity,
                        bytes=0,
                        sha256=hashlib.sha256(b"").hexdigest(),
                        content_hash_state=CheckpointSHA256().export_state(),
                    )
                    for identity in identities[1:]
                )
            base = [
                fields(f"{number + 1:032x}", linked=identity)
                for number, identity in enumerate(identities[:linked])
            ]
            base.extend(fields("8" + f"{number:031x}") for number in range(orphans))
            case = SimpleNamespace(
                engine=engine,
                factory=factory,
                root=path / "native",
                base=base,
                selected=fields(ONE, width=width, epoch=epoch),
            )
            reset(case)
            cases.append(case)
        yield cases


@pytest.fixture(scope="module")
def retirement_cases(tmp_path_factory):
    with build_cases(tmp_path_factory.mktemp("retirement-catalog-inputs")) as cases:
        yield cases


def reset(case, *selected):
    with case.factory() as session, session.begin():
        session.execute(delete(Job)).close()
        session.add_all(Job(**row) for row in (*case.base, *selected))


def read(case, identity=ONE):
    with case.factory() as session:
        row = session.get(Job, identity)
        return None if row is None else jobs.ValidationJob.read(row)


def measure(case, current, mode, argument, *, proposed=None, cold=False, changed=0):
    operation = getattr(type(current), mode)
    before = snapshot(case.engine)
    job = argument if isinstance(argument, jobs.ValidationJob) else None
    controls = (
        (tuple(current._after.items()), argument)
        if job is None
        else (tuple(asdict(job).values()), proposed)
    )
    proposed_row = () if job is None else tuple(asdict(job).values()) + (proposed,)
    inputs = {
        "retained_rows": sum(map(len, before.values())),
        "metadata_row_octets": max(
            [octets(proposed_row), *(octets(row) for rows in before.values() for row in rows)]
        ),
        "control_octets": octets(controls),
        "excluded_jobs": len(argument) if job is None else 0,
    }
    recorder = Recorder()
    with recorder.observe(case.engine, cold=cold):
        result = (
            operation(current, argument) if job is None else operation(current, argument, proposed)
        )
    after = snapshot(case.engine)
    assert before[jobs.Journal.__tablename__] == after[jobs.Journal.__tablename__]
    assert recorder.metrics["changed_rows"] == changed
    if not changed:
        assert after == before
    else:
        untouched_before = tuple(
            row for row in before[Job.__tablename__] if row[0] != job.workspace_id
        )
        untouched_after = tuple(
            row for row in after[Job.__tablename__] if row[0] != job.workspace_id
        )
        assert untouched_before == untouched_after
    if isinstance(result, jobs.ValidationJob):
        assert result == read(case, result.workspace_id)
    binding = growth_binding(operation)
    projection = project_bindings((binding,))
    sample = {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(
            canonical_json_bytes(projection["declarations"][binding.declaration.id])
        ).hexdigest(),
        "dimensions": inputs,
        "metrics": {
            bound.metric: recorder.metrics[bound.metric] for bound in binding.declaration.bounds
        },
        "delegates": {},
    }
    validate_execution_samples(projection, binding.declaration.id, [sample])
    return result, sample


def selection_samples(cases, samples):
    for case in cases:
        for cold in (False, True):
            scenarios = [
                (state, 0, False, ())
                for state in (
                    None,
                    "directory",
                    "retiring",
                    "active",
                    "complete",
                    "failed",
                    "lost",
                    "opening",
                )
            ]
            scenarios.extend(
                ("active", phase, wrap, excluded)
                for phase in range(len(PHASES))
                for wrap in (False, True)
                for excluded in ((), (ONE,), (ONE, TWO))
            )
            for state, phase, wrap, excluded in scenarios:
                selected = [] if state is None else [{**case.selected, "state": state}]
                reset(case, *selected)
                if excluded:
                    eligible = sorted(
                        row["workspace_id"]
                        for row in (*case.base, *selected)
                        if row["collection_id"] is None
                    )
                    excluded = tuple(dict.fromkeys((*eligible, ONE, TWO)))[: len(excluded)]
                current = owner.UploadJournalValidationRetirement(case.factory, case.root)
                current._phase = phase
                current._after = dict.fromkeys(PHASES, "f" * 32 if wrap else None)
                before_cursor = dict(current._after)
                # The independent fixture model selects by phase and exact identity.
                expected, selected_phase = None, None
                for distance in range(len(PHASES)):
                    index = (phase + distance) % len(PHASES)
                    candidates = sorted(
                        row["workspace_id"]
                        for row in (*case.base, *selected)
                        if row["collection_id"] is None
                        and row["workspace_id"] not in excluded
                        and (
                            row["state"] == PHASES[index]
                            if PHASES[index] != "orphan"
                            else row["state"] not in {"directory", "retiring"}
                        )
                    )
                    if candidates:
                        expected, selected_phase = candidates[0], index
                        break
                result, sample = measure(case, current, "_select", excluded, cold=cold)
                assert (None if result is None else result.workspace_id) == expected
                if expected is None:
                    assert current._phase == phase and current._after == before_cursor
                else:
                    before_cursor[PHASES[selected_phase]] = expected
                    assert current._after == before_cursor
                    assert current._phase == (selected_phase + 1) % len(PHASES)
                samples.append(sample)
            # Repeated calls exercise a real keyset successor and fair phase rotation.
            reset(
                case,
                *(
                    {**case.selected, "workspace_id": identity, "state": state}
                    for identity, state in (
                        (ONE, "directory"),
                        (TWO, "retiring"),
                        (THREE, "active"),
                    )
                ),
            )
            current = owner.UploadJournalValidationRetirement(case.factory, case.root)
            orphan_keys = sorted(
                [THREE, *(row["workspace_id"] for row in case.base if row["collection_id"] is None)]
            )
            for expected in (ONE, TWO, orphan_keys[0], ONE, TWO, orphan_keys[1 % len(orphan_keys)]):
                result, sample = measure(case, current, "_select", (), cold=cold)
                assert result.workspace_id == expected
                samples.append(sample)


def mutation_samples(mode, cases, samples):
    for case in cases:
        for cold in (False, True):
            current = owner.UploadJournalValidationRetirement(case.factory, case.root)
            for rejected in (
                "version",
                "epoch",
                "attempt",
                "source-binding",
                "certificate-binding",
                "state",
                "missing",
                "linked",
            ):
                selected = dict(case.selected)
                if rejected == "linked":
                    # A different journal avoids the fixture's unique source link.
                    selected.update(collection_id=1, journal_id=JOURNAL)
                    base, case.base = (
                        case.base,
                        [row for row in case.base if row["journal_id"] != JOURNAL],
                    )
                reset(case, *([] if rejected == "missing" else [selected]))
                if rejected == "linked":
                    case.base = base
                argument = jobs.ValidationJob(**selected)
                changes = {
                    "version": {"version": "1" * 64},
                    "epoch": {"worker_epoch": argument.worker_epoch + 1},
                    "attempt": {"attempt_id": argument.attempt_id + "-stale"},
                    "source-binding": {"source_binding_json": argument.source_binding_json + " "},
                    "certificate-binding": {
                        "certificate_binding_json": argument.certificate_binding_json + " "
                    },
                    "state": {"state": "opening"},
                }.get(rejected, {})
                argument = replace(argument, **changes)
                proposed = "retiring" if mode == "_transition" else "blocked failure"
                result, sample = measure(
                    case, current, mode, argument, proposed=proposed, cold=cold
                )
                assert result is (None if mode == "_transition" else False)
                samples.append(sample)
            if mode == "_transition":
                reset(case, case.selected)
                argument = read(case)
                for target in ("retiring", "directory"):
                    result, sample = measure(
                        case, current, mode, argument, proposed=target, cold=cold, changed=1
                    )
                    assert result.state == target and result.failure is None
                    assert result.version != argument.version
                    assert (
                        replace(
                            result,
                            state=argument.state,
                            failure=argument.failure,
                            version=argument.version,
                        )
                        == argument
                    )
                    samples.append(sample)
                    stale, sample = measure(
                        case, current, mode, argument, proposed=target, cold=cold
                    )
                    assert stale is None
                    samples.append(sample)
                    argument = result
            else:
                reset(case, case.selected)
                argument = read(case)
                for proposed in (
                    "blocking condition",
                    "changed condition δ" + "界" * (len(argument.failure) + 1),
                ):
                    result, sample = measure(
                        case, current, mode, argument, proposed=proposed, cold=cold, changed=1
                    )
                    assert result is True
                    updated = read(case)
                    assert updated.failure == proposed and updated.version != argument.version
                    assert (
                        replace(updated, failure=argument.failure, version=argument.version)
                        == argument
                    )
                    samples.append(sample)
                    for repeated in (argument, updated):
                        result, sample = measure(
                            case, current, mode, repeated, proposed=proposed, cold=cold
                        )
                        assert result is False
                        samples.append(sample)
                    argument = updated


@pytest.mark.growth_witness(
    "riverhog-validation-retirement-catalog-select-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_retirement.UploadJournalValidationRetirement._select",
)
def test_retirement_selection_uses_actual_indexed_phases_and_wraps(
    retirement_cases, record_property
):
    samples = []
    record_property("growth_samples", samples)
    selection_samples(retirement_cases, samples)


@pytest.mark.growth_witness(
    "riverhog-validation-retirement-catalog-transition-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_retirement.UploadJournalValidationRetirement._transition",
)
def test_retirement_transitions_preserve_exact_orphan_fences(retirement_cases, record_property):
    samples = []
    record_property("growth_samples", samples)
    mutation_samples("_transition", retirement_cases, samples)


@pytest.mark.growth_witness(
    "riverhog-validation-retirement-catalog-blocked-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_retirement.UploadJournalValidationRetirement._blocked",
)
def test_retirement_blockers_preserve_full_width_and_do_not_hot_rewrite(
    retirement_cases, record_property
):
    samples = []
    record_property("growth_samples", samples)
    mutation_samples("_blocked", retirement_cases, samples)
