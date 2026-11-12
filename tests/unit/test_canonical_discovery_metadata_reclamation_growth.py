from __future__ import annotations

import os
from contextlib import closing, contextmanager
from dataclasses import asdict

import pytest
from riverhog_core import canonical_discovery_metadata_reclamation as owner
from riverhog_core import canonical_discovery_root_reclamation as root_owner
from riverhog_core.catalog_discovery_validation_models import DiscoveryValidationJobRecord as Job
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from sqlalchemy import update
from workspace_reclamation import FileReclamationOwner

from tests.support.catalog_work import octets
from tests.support.workspace_reclamation import execution_sample, observe_reclamation
from tests.unit import test_canonical_discovery_metadata_growth as basis
from tests.unit.test_canonical_discovery_child_reclamation_growth import direct_primitives
from tests.unit.test_canonical_discovery_metadata_growth import (
    catalog_templates as catalog_templates,
)
from tests.unit.test_canonical_discovery_metadata_growth import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_metadata_growth import original as original
from tests.unit.test_canonical_discovery_metadata_growth import qualified as qualified
from tests.unit.test_canonical_discovery_root_reclamation import private_file
from tests.unit.test_canonical_discovery_root_reclamation_growth import root_case, sample

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX original metadata extents")


@contextmanager
def metadata_case(root, admission, template, retained, width, variant):
    with root_case(root, admission, template, retained, width, "control") as (
        case,
        metadata,
        _child,
        _recorder,
    ):
        if variant == "wrong-phase":
            job = metadata.job
            metadata.close()
        else:
            result = root_owner.reclaim_discovery_journals_root(metadata)
            assert result.metadata_closed
            job = result.job
        file = case.path / "discovery.sqlite3"
        extent = int(variant.partition("-")[2]) if variant.startswith("extent-") else 2
        os.truncate(file, extent)
        files = 1
        if variant == "empty":
            file.unlink()
            extent = files = 0
        elif variant == "sidecars":
            for suffix in ("-journal", "-wal", "-shm"):
                private_file(case.path, "discovery.sqlite3" + suffix, 2)
            extent, files = 8, 4
        elif variant == "prior-corpus":
            private_file(case.path, "corpus.sqlite3", 17)
        elif variant == "stale":
            metadata._custody._held = False
        elif variant == "catalog-stale":
            with case.factory() as session, session.begin():
                session.execute(
                    update(Job).where(Job.workspace_id == job.workspace_id).values(version="f" * 64)
                )
        yield case, metadata.retirement, job, basis.Recorder(), extent, files


@contextmanager
def file_delegate(recorder, projection, dimensions, samples, *, fault=False):
    operation = owner.reclaim_retired_files
    binding = growth_binding(operation)

    def observed(directory, *, receiver, names, octet_budget):
        active, recorder.active = recorder.active, False
        sizes = tuple(len(name.encode()) for name in names)
        try:
            with pytest.MonkeyPatch.context() as patch:
                if fault:

                    def uncertain(_descriptor):
                        raise OSError(28, "actual metadata extent sync capacity uncertain")

                    patch.setattr(os, "fsync", uncertain)
                with observe_reclamation(patch, operation) as metrics:
                    try:
                        return operation(
                            directory, receiver=receiver, names=names, octet_budget=octet_budget
                        )
                    finally:
                        samples.append(
                            execution_sample(
                                binding,
                                projection,
                                {
                                    "candidate_files": len(names),
                                    "name_octets": sum(sizes),
                                    "maximum_name_octets": max(sizes, default=0),
                                    **dimensions,
                                    "octet_budget": octet_budget,
                                },
                                metrics,
                                {},
                            )
                        )
        finally:
            recorder.active = active

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(owner, "reclaim_retired_files", observed)
        yield


@pytest.mark.growth_witness(
    "canonical-discovery-metadata-reclamation/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_metadata_reclamation.reclaim_discovery_metadata",
)
def test_measured_metadata_extents(tmp_path, fixed_inputs, catalog_templates, record_property):
    admission, _ = fixed_inputs
    binding = growth_binding(owner.reclaim_discovery_metadata)
    projection = project_bindings((binding,))
    samples = []
    variants = (
        "extent-1",
        "extent-17",
        "extent-1025",
        "sidecars",
        "empty",
        "stale",
        "wrong-phase",
        "prior-corpus",
        "catalog-stale",
        "sync-refusal",
        "phase-rollback",
    )
    sizes = tuple(len(name.encode()) for name in owner._FILES)
    for retained, width in basis.SPECIFICATIONS:
        for cold in (False, True):
            for variant in variants:
                root = tmp_path / f"metadata-{retained}-{width}-{cold}-{variant}"
                with (
                    metadata_case(
                        root,
                        admission,
                        catalog_templates[(retained, width)],
                        retained,
                        width,
                        variant,
                    ) as (case, retirement, job, recorder, extent, files),
                    closing(FileReclamationOwner()) as receiver,
                ):
                    delegated = {
                        "candidate_files": len(owner._FILES),
                        "directory_octets": len(os.fsencode(case.path)),
                        "name_octets": sum(sizes),
                        "maximum_name_octets": max(sizes),
                        "retained_octets": extent,
                        "retained_files": files,
                        "octet_budget": 3,
                    }
                    dimensions = {
                        "retained_catalog_rows": retained * 3 + 3,
                        "control_octets": octets(tuple(asdict(job).values())),
                        "path_octets": len(os.fsencode(case.path / "discovery.sqlite3")),
                        "files_calls": 0,
                        **{"files_" + name: value for name, value in delegated.items()},
                    }
                    children = []
                    with pytest.MonkeyPatch.context() as patch:
                        if variant == "phase-rollback":
                            actual = owner.jobs.advance_discovery_reclamation

                            def rolled_back(session, before, phase, _actual=actual, **kwargs):
                                assert _actual(session, before, phase, **kwargs) is not None
                                raise OSError("actual metadata phase rollback")

                            patch.setattr(owner.jobs, "advance_discovery_reclamation", rolled_back)
                        with (
                            recorder.observe(case, cold=cold, file_primitives=False),
                            direct_primitives(recorder),
                            file_delegate(
                                recorder,
                                projection,
                                delegated,
                                children,
                                fault=variant == "sync-refusal",
                            ),
                        ):
                            if variant in {"stale", "wrong-phase", "catalog-stale"}:
                                with pytest.raises(owner.StaleArchiveTask):
                                    owner.reclaim_discovery_metadata(
                                        case.factory,
                                        job,
                                        retirement,
                                        receiver=receiver,
                                        octet_budget=3,
                                    )
                            elif variant == "prior-corpus":
                                with pytest.raises(owner.LostJournalValidationWorkspace):
                                    owner.reclaim_discovery_metadata(
                                        case.factory,
                                        job,
                                        retirement,
                                        receiver=receiver,
                                        octet_budget=3,
                                    )
                            elif variant == "sync-refusal":
                                with pytest.raises(owner.JournalValidationCapacity):
                                    owner.reclaim_discovery_metadata(
                                        case.factory,
                                        job,
                                        retirement,
                                        receiver=receiver,
                                        octet_budget=3,
                                    )
                            elif variant == "phase-rollback":
                                with pytest.raises(OSError, match="phase rollback"):
                                    owner.reclaim_discovery_metadata(
                                        case.factory,
                                        job,
                                        retirement,
                                        receiver=receiver,
                                        octet_budget=3,
                                    )
                            else:
                                result = owner.reclaim_discovery_metadata(
                                    case.factory, job, retirement, receiver=receiver, octet_budget=3
                                )
                                assert result.advanced
                                if variant in {"extent-1", "empty"}:
                                    assert result.job.reclamation_phase == "directory"
                                else:
                                    assert result.job == job and result.files.remaining_files > 0
                    assert not recorder.live and not recorder.statements
                    observed = sample(recorder, projection, binding, dimensions)
                    observed["dimensions"]["files_calls"] = len(children)
                    observed["metrics"]["files_calls"] = len(children)
                    observed["metrics"]["files_calls_limit"] = len(children)
                    observed["delegates"] = {"files": children}
                    for child in children:
                        for name, value in child["metrics"].items():
                            key = "files_" + name
                            observed["metrics"][key] = observed["metrics"].get(key, 0) + value
                    try:
                        validate_execution_samples(projection, binding.declaration.id, [observed])
                    except ValueError:
                        record_property("failed_metadata_extent_primitives", observed)
                        raise
                    samples.append(observed)
    record_property("growth_samples", samples)
    record_property(
        "metadata_reclamation_conditions",
        "Actual complete catalog SQL/driver/VM/control widths and original parent custody; "
        "exact prior-name probes and final absence sync. One explicitly sampled neutral "
        "delegate including real name encoding/comparison/syscalls; zero Core SQLite, "
        "payload, enumeration or repair. Independent retained rows and opaque lost-disposition "
        "widths1/17/1025, cold/warm, independent extents1/17/1025, sidecars, absent names, "
        "stale identity/custody, prior files, capacity and phase rollback.",
    )
