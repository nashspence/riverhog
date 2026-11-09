from __future__ import annotations

import os
from dataclasses import asdict, replace

import pytest
from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core.canonical_discovery_parent_reclamation import DiscoveryParentRetirement
from riverhog_core.catalog_discovery_validation_models import DiscoveryValidationJobRecord as Row
from riverhog_core.journal_validation_workspace import lease_journal_validation_workspace
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from sqlalchemy import update

from tests.support.catalog_work import CatalogRecorder, octets
from tests.support.workspace_reclamation import execution_sample
from tests.unit.test_canonical_discovery_corpus_jobs_growth import cases as cases
from tests.unit.test_canonical_discovery_corpus_jobs_growth import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_corpus_jobs_growth import original as original
from tests.unit.test_canonical_discovery_corpus_jobs_growth import qualified as qualified
from tests.unit.test_canonical_discovery_corpus_jobs_growth import reset, snapshot

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX original physical preparation")


@pytest.mark.growth_witness(
    "canonical-discovery-job-delete-retired/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_corpus_jobs.delete_retired_discovery_job",
)
def test_measured_exact_catalog_deletion(cases, record_property):
    binding = growth_binding(jobs.delete_retired_discovery_job)
    projection = project_bindings((binding,))
    samples = []
    for case in cases:
        case.root.parent.chmod(0o700)
        for cold in (False, True):
            for variant in ("fresh", "missing", "stale", "wrong-phase"):
                job = reset(case, "confirm_discovery_storage", "fresh")
                failure = "x" * case.metadata_width
                with case.factory() as session, session.begin():
                    session.execute(
                        update(Row)
                        .where(Row.workspace_id == job.workspace_id)
                        .values(failure=failure)
                    )
                job = replace(job, failure=failure)
                with lease_journal_validation_workspace(
                    case.root,
                    job.workspace_id,
                    create=True,
                    creation_current=lambda value=job, factory=case.factory: (
                        jobs.discovery_worker_current(factory, value)
                    ),
                ) as path:
                    assert path is not None
                with case.factory() as session, session.begin():
                    job = jobs.retire_discovery_job(session, job)
                    assert job is not None
                retained = DiscoveryParentRetirement(case.factory, job, case.root)
                actual = retained.reclaim()
                assert actual.namespace_received and retained._closed
                job = actual.job
                if variant == "missing":
                    with case.factory() as session, session.begin():
                        assert jobs.delete_retired_discovery_job(session, job)
                selected = (
                    replace(job, version="f" * 64)
                    if variant == "stale"
                    else replace(job, state="retiring", reclamation_phase="metadata")
                    if variant == "wrong-phase"
                    else job
                )
                before = snapshot(case.engine)
                dimensions = {
                    "retained_rows": sum(map(len, before.values())),
                    "metadata_row_octets": max(
                        octets(tuple(asdict(selected).values())),
                        *(octets(row) for rows in before.values() for row in rows),
                    ),
                    "control_octets": octets(tuple(asdict(selected).values())),
                }
                recorder = CatalogRecorder(jobs)
                with recorder.observe(case.engine, cold=cold):
                    with case.factory() as session, session.begin():
                        received = jobs.delete_retired_discovery_job(session, selected)
                assert received is (variant in {"fresh", "missing"})
                assert recorder.metrics["changed_rows"] == int(variant == "fresh")
                after = snapshot(case.engine)
                for table in before:
                    if table != Row.__tablename__:
                        assert after[table] == before[table]
                if variant != "fresh":
                    assert after == before
                observed = execution_sample(
                    binding,
                    projection,
                    dimensions,
                    {
                        bound.metric: recorder.metrics[bound.metric]
                        for bound in binding.declaration.bounds
                    },
                    {},
                )
                validate_execution_samples(projection, binding.declaration.id, [observed])
                samples.append(observed)
    record_property("growth_samples", samples)
    record_property(
        "catalog_receiving_conditions",
        "Actual indexed exact DELETE and absent/stale point receiving after positive actual "
        "parent namespace and descriptor receiving. Complete SQL/driver/VM, returned and "
        "parameter widths, selected failure and independent retained controls1/17/1025, "
        "cold/warm, fresh/missing/stale/wrong-phase. No other catalog table changes.",
    )
