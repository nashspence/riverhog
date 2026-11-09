from __future__ import annotations

import fcntl
import os
from dataclasses import asdict, replace

import pytest
from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core import canonical_discovery_parent_reclamation as owner
from riverhog_core.catalog_discovery_validation_models import DiscoveryValidationJobRecord as Row
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from sqlalchemy import update

from tests.support.catalog_work import octets
from tests.unit import test_canonical_discovery_metadata_growth as basis
from tests.unit.test_canonical_discovery_child_reclamation_growth import direct_primitives
from tests.unit.test_canonical_discovery_metadata_growth import (
    catalog_templates as catalog_templates,
)
from tests.unit.test_canonical_discovery_metadata_growth import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_metadata_growth import original as original
from tests.unit.test_canonical_discovery_metadata_growth import qualified as qualified
from tests.unit.test_canonical_discovery_root_reclamation_growth import sample

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX original parent receiving")


@pytest.mark.growth_witness(
    "canonical-discovery-parent-reclamation/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_parent_reclamation.DiscoveryParentRetirement.reclaim",
)
def test_measured_original_parent_receiving(
    tmp_path, fixed_inputs, catalog_templates, record_property
):
    admission, _ = fixed_inputs
    binding = growth_binding(owner.DiscoveryParentRetirement.reclaim)
    projection = project_bindings((binding,))
    samples = []
    variants = (
        "prefix",
        "directory",
        "absent-selected",
        "absent-root",
        "absent-control",
        "missing-lease",
        "busy-root",
        "busy-control",
        "busy-lease",
        "unknown",
        "known-data",
        "catalog-stale",
        "sync-refusal",
        "phase-rollback",
    )
    for retained, width in basis.SPECIFICATIONS:
        for cold in (False, True):
            for variant in variants:
                root = tmp_path / f"parent-{retained}-{width}-{cold}-{variant}"
                with basis.case_for(
                    root,
                    admission,
                    retained,
                    width,
                    template=catalog_templates[(retained, width)],
                ) as case:
                    before = case.metadata.job
                    # Actual selected failure controls vary independently from retained rows.
                    with case.factory() as session, session.begin():
                        session.execute(
                            update(Row)
                            .where(Row.workspace_id == before.workspace_id)
                            .values(state="lost", failure="x" * width)
                        )
                        job = jobs.retire_discovery_job(
                            session, replace(before, state="lost", failure="x" * width)
                        )
                        assert job is not None
                        if variant == "directory":
                            job = jobs.advance_discovery_reclamation(session, job, "directory")
                            assert job is not None
                # Actual generic lease/context is fully received before measuring a new owner.
                root.chmod(0o700)
                namespace = case.path.parent
                path = case.path
                if variant in {"absent-selected", "absent-root", "absent-control"}:
                    (path / ".lease").unlink()
                    path.rmdir()
                if variant in {"absent-root", "absent-control"}:
                    (namespace / ".coordinator").unlink()
                if variant == "absent-root":
                    namespace.rmdir()
                if variant == "missing-lease":
                    (path / ".lease").unlink()
                if variant in {"unknown", "known-data"}:
                    (
                        path / ("unclassified" if variant == "unknown" else "discovery.sqlite3")
                    ).write_bytes(b"preserved")
                if variant == "catalog-stale":
                    with case.factory() as session, session.begin():
                        session.execute(
                            update(Row)
                            .where(Row.workspace_id == job.workspace_id)
                            .values(version="f" * 64)
                        )
                descriptor = None
                if variant.startswith("busy-"):
                    target = {
                        "busy-root": namespace,
                        "busy-control": namespace / ".coordinator",
                        "busy-lease": path / ".lease",
                    }[variant]
                    descriptor = os.open(target, os.O_RDONLY)
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                recorder = basis.Recorder()
                receiver = owner.DiscoveryParentRetirement(case.factory, job, namespace)
                dimensions = {
                    "retained_catalog_rows": retained * 3 + 3,
                    "control_octets": octets(tuple(asdict(job).values())),
                    "path_octets": len(os.fsencode(path)),
                }
                try:
                    with pytest.MonkeyPatch.context() as patch:
                        if variant == "sync-refusal":

                            def uncertain(_descriptor):
                                raise OSError(28, "actual parent sync capacity")

                            patch.setattr(os, "fsync", uncertain)
                        if variant == "phase-rollback":
                            actual = jobs.advance_discovery_reclamation

                            def rollback(session, value, phase, _actual=actual):
                                assert _actual(session, value, phase) is not None
                                raise OSError("actual parent phase rollback")

                            patch.setattr(jobs, "advance_discovery_reclamation", rollback)
                        # Include mutation path encoding as well as acquisition/stat arguments.
                        for name in ("unlink", "rmdir"):
                            actual_path = getattr(os, name)

                            def mutation(*args, _actual=actual_path, _recorder=recorder, **kwargs):
                                if _recorder.active:
                                    _recorder.metrics["encoded_path_octets"] += len(
                                        os.fsencode(args[0])
                                    )
                                return _actual(*args, **kwargs)

                            patch.setattr(os, name, mutation)
                        with recorder.observe(case, cold=cold), direct_primitives(recorder):
                            if variant == "catalog-stale":
                                with pytest.raises(owner.StaleArchiveTask):
                                    receiver.reclaim()
                            elif variant in {"unknown", "known-data"}:
                                with pytest.raises(owner.LostJournalValidationWorkspace):
                                    receiver.reclaim()
                            elif variant == "sync-refusal":
                                with pytest.raises(owner.JournalValidationCapacity):
                                    receiver.reclaim()
                            elif variant == "phase-rollback":
                                with pytest.raises(OSError, match="phase rollback"):
                                    receiver.reclaim()
                            else:
                                result = receiver.reclaim()
                                assert result.namespace_received is (
                                    not variant.startswith("busy-")
                                )
                    assert receiver._closed and not recorder.live
                    assert not recorder.statements
                    observed = sample(recorder, projection, binding, dimensions)
                    try:
                        validate_execution_samples(projection, binding.declaration.id, [observed])
                    except ValueError:
                        record_property("failed_parent_primitives", observed)
                        raise
                    samples.append(observed)
                finally:
                    if descriptor is not None:
                        os.close(descriptor)
                    receiver.close()
                    case.engine.dispose()
    record_property("growth_samples", samples)
    record_property(
        "parent_receiving_conditions",
        "Actual catalog SQL/driver/VM and complete original controls; no Core SQLite, payload, "
        "enumeration or repair. Five-descriptor closure, no-follow identity/mode checks, "
        "three nonblocking kernel locks, exact thirteen-name probes and synchronized "
        "unlink/rmdir. Independent retained rows and actual selected failure widths1/17/1025, "
        "cold/warm, missing prefixes, busy custody, unknown/data, stale, capacity and rollback.",
    )
