from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import asdict

import pytest
from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core import canonical_discovery_corpus_reclamation as owner
from riverhog_core.canonical_discovery_retired_metadata import RetiredDiscoveryMetadata
from riverhog_core.catalog_discovery_validation_models import DiscoveryValidationJobRecord as Job
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import JournalValidationCapacity
from riverhog_provenance import corpus_reclamation as native
from sqlalchemy import update

from tests.support.catalog_work import octets
from tests.support.retirement_namespace import lease_retiring_journal_workspace
from tests.support.workspace_reclamation import (
    execution_sample,
    observe_file_delegate,
    observe_reclamation,
)
from tests.unit import test_canonical_discovery_metadata_growth as basis
from tests.unit.test_canonical_discovery_child_reclamation_growth import (
    combined_sample,
    direct_primitives,
)
from tests.unit.test_canonical_discovery_metadata_growth import (
    catalog_templates as catalog_templates,
)
from tests.unit.test_canonical_discovery_metadata_growth import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_metadata_growth import original as original
from tests.unit.test_canonical_discovery_metadata_growth import qualified as qualified
from tests.unit.test_canonical_discovery_retired_metadata import child_binding, register_children
from tests.unit.test_canonical_discovery_retired_metadata_growth import observe_binding_codec

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX original corpus reclamation")


@contextmanager
def corpus_case(root, inputs, template, retained, width, variant):
    admission, frozen = inputs
    with basis.case_for(root, admission, retained, width, template=template) as case:
        case.metadata.open()
        basis.seed_core(case)
        pinned = variant not in {"pre-pin", "unowned-file"}
        if pinned:
            job = jobs.pin_discovery_corpus(case.factory, case.metadata.job, frozen)
            assert job is not None
            case.metadata.job = job
        binding = child_binding(case.metadata.job, width=width)
        if variant == "remaining-child":
            with case.metadata.transaction() as (db, _):
                register_children(db, case.metadata.job, [binding])
        extent = 17 if variant == "truncate" else 2
        file_present = variant in {
            "truncate",
            "unlink",
            "unowned-file",
            "malformed-count",
            "sync-refusal",
            "phase-rollback",
            "wrong-phase",
        }
        if file_present:
            file = case.path / "corpus.sqlite3"
            file.write_bytes(b"x" * extent)
            file.chmod(0o600)
        case.metadata.close()
        with case.factory() as session, session.begin():
            job = jobs.retire_discovery_job(session, case.metadata.job)
            assert job is not None
            job = jobs.advance_discovery_reclamation(session, job, "corpus")
            assert job is not None
            if pinned:
                # Independent opaque original-binding widths are not an
                # assertion that this disposable projection accepted an archive.
                session.execute(
                    update(Job)
                    .where(Job.workspace_id == job.workspace_id)
                    .values(
                        journal_count="9" * width,
                        source_count="9" * width,
                        selection_count="9" * width,
                    )
                )
                session.expire_all()
                job = jobs.DiscoveryJob.read(session.get(Job, job.workspace_id))
            if variant == "malformed-count":
                session.execute(
                    update(Job)
                    .where(Job.workspace_id == job.workspace_id)
                    .values(
                        source_count="00",
                    )
                )
                session.expire_all()
                job = jobs.DiscoveryJob.read(session.get(Job, job.workspace_id))
            if variant == "wrong-phase":
                job = jobs.advance_discovery_reclamation(session, job, "journals_root")
                assert job is not None
    with lease_retiring_journal_workspace(case.path.parent, job.workspace_id) as custody:
        assert custody is not None
        metadata = RetiredDiscoveryMetadata(case.factory, job, custody)
        recorder = basis.Recorder()
        try:
            with recorder.connect_only():
                metadata.open()
            if variant == "stale":
                metadata._custody._held = False
            yield (
                case,
                metadata,
                binding,
                recorder,
                extent if file_present else 0,
                int(file_present),
            )
        finally:
            metadata._custody._held = True
            metadata.close()
            case.engine.dispose()


@contextmanager
def count_original_scalars(recorder):
    actual = jobs.parse_scalar

    def parsed(domain, value):
        if recorder.active:
            recorder.metrics["scalar_parse_calls"] += 1
            recorder.metrics["scalar_parsed_octets"] += octets(value)
        return actual(domain, value)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(jobs, "parse_scalar", parsed)
        yield


@contextmanager
def native_delegate(recorder, projection, dimensions, samples, fault):
    operation = owner.reclaim_retired_corpus_workspace
    binding = growth_binding(operation)

    def observed(directory, *, receiver, octet_budget):
        directory_width = len(os.fsencode(directory))
        active, recorder.active = recorder.active, False
        files = []
        try:
            with pytest.MonkeyPatch.context() as patch:

                def uncertain(descriptor):
                    raise OSError(28, "actual delegated corpus sync capacity")

                if fault:
                    patch.setattr(os, "fsync", uncertain)
                with observe_reclamation(patch, operation) as metrics:
                    try:
                        with observe_file_delegate(
                            patch, native, projection, dimensions, metrics
                        ) as files:
                            return operation(
                                directory, receiver=receiver, octet_budget=octet_budget
                            )
                    finally:
                        samples.append(
                            execution_sample(
                                binding,
                                projection,
                                {
                                    **dimensions,
                                    "directory_octets": directory_width,
                                    "octet_budget": octet_budget,
                                },
                                metrics,
                                {"files": files},
                            )
                        )
        finally:
            recorder.active = active

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(owner, "reclaim_retired_corpus_workspace", observed)
        yield


@pytest.mark.growth_witness(
    "canonical-discovery-corpus-reclamation/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_corpus_reclamation.reclaim_discovery_corpus",
)
def test_measured_original_corpus_reclamation(
    tmp_path, fixed_inputs, catalog_templates, record_property
):
    binding = growth_binding(owner.reclaim_discovery_corpus)
    projection = project_bindings((binding,))
    samples = []
    variants = (
        "truncate",
        "unlink",
        "empty-files",
        "pre-pin",
        "unowned-file",
        "malformed-count",
        "remaining-child",
        "wrong-phase",
        "stale",
        "sync-refusal",
        "phase-rollback",
    )
    for retained, width in basis.SPECIFICATIONS:
        for cold in (False, True):
            for variant in variants:
                root = tmp_path / f"corpus-{retained}-{width}-{cold}-{variant}"
                with corpus_case(
                    root,
                    fixed_inputs,
                    catalog_templates[(retained, width)],
                    retained,
                    width,
                    variant,
                ) as (case, metadata, original_child, recorder, extent, files):
                    dimensions = {
                        "retained_core_rows": retained * 2 + 3,
                        "retained_catalog_rows": retained * 3 + 3,
                        "control_octets": octets(
                            (
                                tuple(asdict(metadata.job).values()),
                                metadata._expected,
                                original_child.to_json(),
                            )
                        ),
                        "path_octets": len(os.fsencode(case.path / "discovery.sqlite3")),
                        "native_calls": 0,
                        "native_directory_octets": len(os.fsencode(case.path)),
                        "native_retained_octets": extent,
                        "native_retained_files": files,
                        "native_octet_budget": 3,
                    }
                    children = []
                    native_dimensions = {
                        "retained_octets": extent,
                        "retained_files": files,
                        "octet_budget": 3,
                    }
                    with (
                        observe_binding_codec(recorder),
                        recorder.observe(case, cold=cold, file_primitives=False),
                        direct_primitives(recorder),
                        count_original_scalars(recorder),
                        native_delegate(
                            recorder,
                            projection,
                            native_dimensions,
                            children,
                            variant == "sync-refusal",
                        ),
                        pytest.MonkeyPatch.context() as patch,
                    ):
                        if variant in {"malformed-count", "wrong-phase", "stale"}:
                            with pytest.raises(owner.StaleArchiveTask):
                                owner.reclaim_discovery_corpus(metadata, octet_budget=3)
                        elif variant in {"unowned-file", "remaining-child"}:
                            with pytest.raises(owner.LostJournalValidationWorkspace):
                                owner.reclaim_discovery_corpus(metadata, octet_budget=3)
                        elif variant == "sync-refusal":
                            with pytest.raises(JournalValidationCapacity):
                                owner.reclaim_discovery_corpus(metadata, octet_budget=3)
                        elif variant == "phase-rollback":
                            actual = jobs.advance_discovery_reclamation

                            def rolled_back(session, job, phase, _actual=actual, **kwargs):
                                assert _actual(session, job, phase, **kwargs) is not None
                                raise OSError("actual corpus phase rollback")

                            patch.setattr(jobs, "advance_discovery_reclamation", rolled_back)
                            with pytest.raises(OSError, match="phase rollback"):
                                owner.reclaim_discovery_corpus(metadata, octet_budget=3)
                        else:
                            result = owner.reclaim_discovery_corpus(metadata, octet_budget=3)
                            assert result.advanced
                            if variant == "truncate":
                                assert (
                                    result.job == metadata.job
                                    and result.files.remaining_octets == 14
                                )
                            else:
                                assert result.job.reclamation_phase == "journals_root"
                    assert not recorder.live
                    sample = combined_sample(recorder, projection, binding, dimensions, children)
                    try:
                        validate_execution_samples(projection, binding.declaration.id, [sample])
                    except ValueError:
                        record_property(
                            "failed_corpus_reclamation_metrics",
                            {
                                "variant": variant,
                                "sample": sample,
                            },
                        )
                        raise
                    for statement, parameters in recorder.statements:
                        if statement.lstrip().upper().startswith("SELECT"):
                            with basis._REAL_CONNECT(case.path / "discovery.sqlite3") as db:
                                plan = " ".join(
                                    str(row[3]).upper()
                                    for row in db.execute(
                                        "EXPLAIN QUERY PLAN " + statement,
                                        parameters,
                                    )
                                )
                            assert "SCAN " not in plan and "TEMP B-TREE" not in plan, plan
                    samples.append(sample)
    record_property("growth_samples", samples)
    record_property(
        "corpus_reclamation_conditions",
        "Actual Core/catalog SQL/driver/VM, full original controls/binding/count parsing, "
        "exclusive descriptor checks and explicit final absence sync, plus actual nested "
        "Native/neutral corpus extents. Independent retained rows/scalar widths 1/17/1025, "
        "cold/warm, truncate/unlink/empty/pre-pin, malformed/unowned/remaining-child/stale "
        "refusal, actual delegated sync failure and actual catalog rollback. Synthetic "
        "opaque widths grant no archive acceptance or parent completion.",
    )
