from __future__ import annotations

import fcntl
import os
from contextlib import ExitStack, contextmanager
from dataclasses import asdict

import pytest
from riverhog_core import canonical_discovery_child_reclamation as owner
from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core.canonical_discovery_retired_metadata import RetiredDiscoveryMetadata
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import JournalValidationCapacity
from riverhog_provenance import journal_reclamation as native

from tests.support.catalog_work import octets
from tests.support.retirement_namespace import lease_retiring_journal_workspace
from tests.support.validation_workspace import leased_workspace
from tests.support.workspace_reclamation import (
    execution_sample,
    observe_file_delegate,
    observe_reclamation,
)
from tests.unit import test_canonical_discovery_metadata_growth as basis
from tests.unit.test_canonical_discovery_metadata_growth import (
    catalog_templates as catalog_templates,
)
from tests.unit.test_canonical_discovery_metadata_growth import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_metadata_growth import original as original
from tests.unit.test_canonical_discovery_metadata_growth import qualified as qualified
from tests.unit.test_canonical_discovery_retired_metadata import child_binding, register_children
from tests.unit.test_canonical_discovery_retired_metadata_growth import observe_binding_codec

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX composed child reclamation")


@contextmanager
def physical_case(root, admission, template, retained, width, variant):
    with ExitStack() as held:
        with basis.case_for(root, admission, retained, width, template=template) as case:
            case.metadata.open()
            basis.seed_core(case)
            bindings = [
                child_binding(case.metadata.job, index + 1, width if index == 0 else 1)
                for index in range(retained)
            ]
            with case.metadata.transaction() as (db, _):
                register_children(db, case.metadata.job, bindings, confirmed=variant != "prefix")
            directory = case.path / "journals" / bindings[0].directory_id
            extent = 17 if variant in {"truncate", "busy"} else 2
            file_present = variant in {"truncate", "unlink", "busy", "native-sync-refusal"}
            if variant not in {"prefix", "removed-namespace", "deleted-routing", "stale"}:
                with leased_workspace(
                    directory.parent,
                    directory.name,
                    create=True,
                    creation_current=case.metadata.verify_current,
                ) as child:
                    assert child == directory
                    if file_present:
                        (child / "snapshot.sqlite3").write_bytes(b"x" * extent)
                        (child / "snapshot.sqlite3").chmod(0o600)
                if variant == "busy":
                    reader = held.enter_context(
                        leased_workspace(directory.parent, directory.name, shared=True)
                    )
                    assert reader == directory
                if variant == "unknown":
                    (directory / "unclassified").write_bytes(b"retained")
            case.metadata.close()
            with case.factory() as session, session.begin():
                job = jobs.retire_discovery_job(session, case.metadata.job)
                assert job is not None
                if variant in {"directory", "removed-namespace", "deleted-routing", "unknown"}:
                    job = jobs.advance_discovery_reclamation(
                        session,
                        job,
                        "child_directory",
                        journal_id=bindings[0].journal_id,
                        directory_id=bindings[0].directory_id,
                    )
                    assert job is not None
        with lease_retiring_journal_workspace(case.path.parent, job.workspace_id) as custody:
            assert custody is not None
            metadata = RetiredDiscoveryMetadata(case.factory, job, custody)
            recorder = basis.Recorder()
            try:
                with recorder.connect_only():
                    metadata.open()
                if variant == "deleted-routing":
                    metadata._db.execute(
                        "DELETE FROM discovery_journal_children WHERE journal_id=?",
                        (bindings[0].journal_id,),
                    ).close()
                if variant == "stale":
                    metadata._custody._held = False
                yield (
                    case,
                    metadata,
                    bindings,
                    recorder,
                    extent if file_present else 0,
                    int(file_present),
                )
            finally:
                metadata._custody._held = True
                metadata.close()
                case.engine.dispose()


@contextmanager
def direct_primitives(recorder):
    with pytest.MonkeyPatch.context() as patch:
        for name, metric in (
            ("flock", "flock_calls"),
            ("unlink", "unlink_calls"),
            ("rmdir", "rmdir_calls"),
            ("mkdir", "mkdir_calls"),
            ("ftruncate", "truncate_calls"),
        ):
            module = fcntl if name == "flock" else os
            actual = getattr(module, name)

            def observed(*args, _actual=actual, _metric=metric, **kwargs):
                if recorder.active:
                    recorder.metrics[_metric] += 1
                    if _metric == "flock_calls":
                        assert args[1] == fcntl.LOCK_UN or args[1] & fcntl.LOCK_NB
                return _actual(*args, **kwargs)

            patch.setattr(module, name, observed)
        yield


@contextmanager
def native_delegate(recorder, projection, dimensions, samples, fault):
    operation = owner.reclaim_retired_journal_workspace
    binding = growth_binding(operation)

    def observed(directory, *, receiver, octet_budget):
        directory_width = len(os.fsencode(directory))
        active, recorder.active = recorder.active, False
        files = []
        try:
            with pytest.MonkeyPatch.context() as patch:

                def uncertain(descriptor):
                    raise OSError(28, "actual delegated child sync capacity")

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
        patch.setattr(owner, "reclaim_retired_journal_workspace", observed)
        yield


def combined_sample(recorder, projection, binding, dimensions, children):
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
    metrics["native_calls"] = metrics["native_calls_limit"] = len(children)
    for child in children:
        for key, value in child["metrics"].items():
            metrics["native_" + key] = metrics.get("native_" + key, 0) + value
    dimensions["native_calls"] = len(children)
    return execution_sample(
        binding,
        projection,
        dimensions,
        {bound.metric: metrics.get(bound.metric, 0) for bound in binding.declaration.bounds},
        {"native": children},
    )


@pytest.mark.growth_witness(
    "canonical-discovery-child-reclamation/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_child_reclamation.reclaim_discovery_child",
)
def test_measured_child_reclamation(tmp_path, fixed_inputs, catalog_templates, record_property):
    admission, _ = fixed_inputs
    binding = growth_binding(owner.reclaim_discovery_child)
    projection = project_bindings((binding,))
    samples = []
    variants = (
        "truncate",
        "unlink",
        "empty-files",
        "directory",
        "removed-namespace",
        "deleted-routing",
        "busy",
        "prefix",
        "stale",
        "unknown",
        "native-sync-refusal",
    )
    for retained, width in basis.SPECIFICATIONS:
        for cold in (False, True):
            for variant in variants:
                root = tmp_path / f"physical-{retained}-{width}-{cold}-{variant}"
                with physical_case(
                    root, admission, catalog_templates[(retained, width)], retained, width, variant
                ) as (case, metadata, bindings, recorder, extent, files):
                    dimensions = {
                        "retained_core_rows": retained * 2 + 3,
                        "retained_catalog_rows": retained * 3 + 3,
                        "control_octets": octets(
                            (
                                tuple(asdict(metadata.job).values()),
                                metadata._expected,
                                bindings[0].to_json(),
                            )
                        ),
                        "path_octets": len(
                            os.fsencode(case.path / "journals" / bindings[0].directory_id)
                        ),
                        "native_calls": 0,
                        "native_directory_octets": len(
                            os.fsencode(case.path / "journals" / bindings[0].directory_id)
                        ),
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
                        native_delegate(
                            recorder,
                            projection,
                            native_dimensions,
                            children,
                            variant == "native-sync-refusal",
                        ),
                    ):
                        if variant == "stale":
                            with pytest.raises(owner.StaleArchiveTask):
                                owner.reclaim_discovery_child(metadata, octet_budget=3)
                        elif variant == "unknown":
                            with pytest.raises(owner.LostJournalValidationWorkspace):
                                owner.reclaim_discovery_child(metadata, octet_budget=3)
                        elif variant == "native-sync-refusal":
                            with pytest.raises(JournalValidationCapacity):
                                owner.reclaim_discovery_child(metadata, octet_budget=3)
                        else:
                            result = owner.reclaim_discovery_child(metadata, octet_budget=3)
                            assert result.advanced is (variant != "busy")
                            if variant == "truncate":
                                assert (
                                    result.job == metadata.job
                                    and result.files.remaining_octets == 14
                                )
                            elif variant in {"unlink", "empty-files", "prefix"}:
                                assert result.job.reclamation_phase == "child_directory"
                            elif variant in {"directory", "removed-namespace", "deleted-routing"}:
                                assert result.job.reclamation_phase == "children"
                    assert not recorder.live
                    sample = combined_sample(recorder, projection, binding, dimensions, children)
                    try:
                        validate_execution_samples(projection, binding.declaration.id, [sample])
                    except ValueError:
                        record_property(
                            "failed_child_reclamation_metrics",
                            {
                                "variant": variant,
                                "sample": sample,
                            },
                        )
                        raise
                    for statement, parameters in recorder.statements:
                        if statement.lstrip().upper().startswith(("SELECT", "DELETE")):
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
        "child_reclamation_conditions",
        "Actual complete Core/catalog SQL/driver/VM, full controls/binding codecs, descriptor "
        "and nonblocking lock work, exact namespace sync/unlink/rmdir, and one nested Native/"
        "neutral extent delegate. Independent retained rows/scalar widths 1/17/1025, cold/warm, "
        "truncate/unlink/empty, selected directory/absent/deleted routing, busy/prefix/stale, "
        "unknown-file refusal and real delegated sync failure. No parent completion or "
        "archive acceptance is inferred.",
    )
