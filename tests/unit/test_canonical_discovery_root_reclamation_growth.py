from __future__ import annotations

import fcntl
import os
from contextlib import contextmanager
from dataclasses import asdict

import pytest
from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core import canonical_discovery_root_reclamation as owner
from riverhog_core.canonical_discovery_corpus_reclamation import reclaim_discovery_corpus
from riverhog_core.canonical_discovery_retired_metadata import RetiredDiscoveryMetadata
from riverhog_core.journal_validation_retirement import lease_retiring_journal_workspace
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples

from tests.support.catalog_work import octets
from tests.support.workspace_reclamation import execution_sample
from tests.unit import test_canonical_discovery_metadata_growth as basis
from tests.unit.test_canonical_discovery_child_reclamation_growth import direct_primitives
from tests.unit.test_canonical_discovery_metadata_growth import (
    catalog_templates as catalog_templates,
)
from tests.unit.test_canonical_discovery_metadata_growth import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_metadata_growth import original as original
from tests.unit.test_canonical_discovery_metadata_growth import qualified as qualified
from tests.unit.test_canonical_discovery_retired_metadata import child_binding, register_children
from tests.unit.test_canonical_discovery_retired_metadata_growth import observe_binding_codec
from tests.unit.test_canonical_discovery_root_reclamation import private_file
from tests.unit.test_discovery_validation_owner import UncertainClose

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX original journals-root receiving")


@contextmanager
def root_case(root, admission, template, retained, width, variant):
    with basis.case_for(root, admission, retained, width, template=template) as case:
        case.metadata.open()
        basis.seed_core(case)
        binding = child_binding(case.metadata.job, width=width)
        if variant == "remaining-child":
            with case.metadata.transaction() as (db, _):
                register_children(db, case.metadata.job, [binding])
        case.metadata.close()
        lost = jobs.lose_discovery_corpus(case.factory, case.metadata.job, failure="x" * width)
        assert lost is not None
        with case.factory() as session, session.begin():
            job = jobs.retire_discovery_job(session, lost)
            assert job is not None
            job = jobs.advance_discovery_reclamation(session, job, "corpus")
            assert job is not None
            if variant == "remaining-child":
                # Deliberately missing physical proof must still be refused by
                # the root owner, even when its catalog phase was advanced.
                job = jobs.advance_discovery_reclamation(session, job, "journals_root")
                assert job is not None
    with lease_retiring_journal_workspace(case.path.parent, job.workspace_id) as custody:
        assert custody is not None
        metadata = RetiredDiscoveryMetadata(case.factory, job, custody)
        metadata.open()
        try:
            if variant not in {"remaining-child", "wrong-phase"}:
                job = reclaim_discovery_corpus(metadata, octet_budget=3).job
                metadata.close()
                metadata = RetiredDiscoveryMetadata(case.factory, job, custody)
            if variant != "absent":
                namespace = case.path / "journals"
                namespace.mkdir(mode=0o700)
                if variant != "prefix":
                    private_file(namespace, ".coordinator", 0)
                if variant == "unknown":
                    private_file(namespace, "unclassified", 17)
            if variant == "remaining-corpus":
                private_file(case.path, "corpus.sqlite3", 17)
            recorder = basis.Recorder()
            if metadata._initialized:
                metadata.close()
                metadata = RetiredDiscoveryMetadata(case.factory, job, custody)
            with recorder.connect_only():
                metadata.open()
            if variant == "stale":
                metadata._custody._held = False
            if variant == "core-close-refusal":
                metadata._db = UncertainClose(metadata._db, lost_reply=False)
            descriptor = None
            try:
                if variant == "busy":
                    descriptor = os.open(case.path / "journals" / ".coordinator", os.O_RDWR)
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                yield case, metadata, binding, recorder
            finally:
                if descriptor is not None:
                    os.close(descriptor)
        finally:
            metadata._custody._held = True
            metadata.close()
            case.engine.dispose()


def sample(recorder, projection, binding, dimensions):
    metrics = dict(recorder.metrics)
    for source, target in (
        ("sql_statements", "catalog_sql_statements"),
        ("driver_statements", "catalog_driver_statements"),
        ("returned_rows", "catalog_returned_rows"),
        ("returned_metadata_octets", "catalog_returned_octets"),
        ("bound_parameter_octets", "catalog_parameter_octets"),
        ("sqlite_vm_instructions", "catalog_vm_instructions"),
        ("changed_rows", "catalog_changed_rows"),
        ("uuid_values", "uuid_values"),
    ):
        metrics[target] = recorder.catalog.metrics[source]
    metrics["hashed_octets"] = (
        metrics.get("hashed_octets", 0) + recorder.catalog.metrics["hashed_octets"]
    )
    return execution_sample(
        binding,
        projection,
        dimensions,
        {bound.metric: metrics.get(bound.metric, 0) for bound in binding.declaration.bounds},
        {},
    )


@pytest.mark.growth_witness(
    "canonical-discovery-journals-root-reclamation/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_root_reclamation.reclaim_discovery_journals_root",
)
def test_measured_journals_root_receiving(
    tmp_path, fixed_inputs, catalog_templates, record_property
):
    admission, _ = fixed_inputs
    binding = growth_binding(owner.reclaim_discovery_journals_root)
    projection = project_bindings((binding,))
    samples = []
    variants = (
        "absent",
        "prefix",
        "control",
        "busy",
        "unknown",
        "remaining-child",
        "remaining-corpus",
        "wrong-phase",
        "stale",
        "sync-refusal",
        "phase-rollback",
        "core-close-refusal",
    )
    for retained, width in basis.SPECIFICATIONS:
        for cold in (False, True):
            for variant in variants:
                root = tmp_path / f"root-{retained}-{width}-{cold}-{variant}"
                with root_case(
                    root, admission, catalog_templates[(retained, width)], retained, width, variant
                ) as (case, metadata, child, recorder):
                    dimensions = {
                        "retained_core_rows": retained * 2 + 3,
                        "retained_catalog_rows": retained * 3 + 3,
                        "control_octets": octets(
                            (
                                tuple(asdict(metadata.job).values()),
                                metadata._expected,
                                child.to_json(),
                            )
                        ),
                        "path_octets": len(os.fsencode(case.path / "discovery.sqlite3")),
                    }
                    with pytest.MonkeyPatch.context() as patch:
                        if variant == "sync-refusal":

                            def uncertain(_descriptor):
                                raise OSError(28, "actual root sync capacity uncertain")

                            patch.setattr(os, "fsync", uncertain)
                        if variant == "phase-rollback":
                            actual = jobs.advance_discovery_reclamation

                            def rolled_back(session, job, phase, _actual=actual, **kwargs):
                                assert _actual(session, job, phase, **kwargs) is not None
                                raise OSError("actual root phase rollback")

                            patch.setattr(jobs, "advance_discovery_reclamation", rolled_back)
                        with (
                            observe_binding_codec(recorder),
                            recorder.observe(case, cold=cold),
                            direct_primitives(recorder),
                        ):
                            if variant in {"wrong-phase", "stale"}:
                                with pytest.raises(owner.StaleArchiveTask):
                                    owner.reclaim_discovery_journals_root(metadata)
                            elif variant in {"unknown", "remaining-child", "remaining-corpus"}:
                                with pytest.raises(owner.LostJournalValidationWorkspace):
                                    owner.reclaim_discovery_journals_root(metadata)
                            elif variant == "sync-refusal":
                                with pytest.raises(owner.JournalValidationCapacity):
                                    owner.reclaim_discovery_journals_root(metadata)
                            elif variant in {"phase-rollback", "core-close-refusal"}:
                                with pytest.raises(OSError):
                                    owner.reclaim_discovery_journals_root(metadata)
                            else:
                                result = owner.reclaim_discovery_journals_root(metadata)
                                assert result.advanced is (variant != "busy")
                                assert result.metadata_closed is (variant != "busy")
                                if result.advanced:
                                    assert result.job.reclamation_phase == "metadata"
                    assert not recorder.live
                    for statement, parameters in recorder.statements:
                        if statement.lstrip().upper().startswith("SELECT"):
                            with basis._REAL_CONNECT(case.path / "discovery.sqlite3") as db:
                                plan = " ".join(
                                    str(row[3]).upper()
                                    for row in db.execute(
                                        "EXPLAIN QUERY PLAN " + statement, parameters
                                    )
                                )
                            assert "SCAN " not in plan and "TEMP B-TREE" not in plan, plan
                    observed = sample(recorder, projection, binding, dimensions)
                    try:
                        validate_execution_samples(projection, binding.declaration.id, [observed])
                    except ValueError:
                        record_property("failed_root_primitives", observed)
                        raise
                    samples.append(observed)
    record_property("growth_samples", samples)
    record_property(
        "root_receiving_conditions",
        "Actual Core/catalog SQL, driver/VM and complete controls, nonblocking locks, no-follow "
        "names, descriptor close, fsync/unlink/rmdir and zero payload/enumeration. Independent "
        "retained rows and real lost-disposition widths1/17/1025, cold/warm, busy/stale/unknown/"
        "remaining-child/corpus/capacity/rollback/actual-Core-close refusal.",
    )
