from __future__ import annotations

import fcntl
import os
from contextlib import contextmanager
from dataclasses import asdict, replace

import pytest
from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core import canonical_discovery_retirement as owner
from riverhog_core.catalog_discovery_validation_models import DiscoveryValidationJobRecord as Row
from riverhog_core.journal_validation_workspace import lease_journal_validation_workspace
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from sqlalchemy import update

from tests.support.catalog_work import octets
from tests.unit import test_canonical_discovery_metadata_growth as basis
from tests.unit.test_canonical_discovery_child_reclamation import prepare_child
from tests.unit.test_canonical_discovery_child_reclamation_growth import direct_primitives
from tests.unit.test_canonical_discovery_metadata_growth import (
    catalog_templates as catalog_templates,
)
from tests.unit.test_canonical_discovery_metadata_growth import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_metadata_growth import original as original
from tests.unit.test_canonical_discovery_metadata_growth import qualified as qualified
from tests.unit.test_canonical_discovery_retired_metadata import case as case
from tests.unit.test_canonical_discovery_retired_metadata import child_binding, register_children
from tests.unit.test_canonical_discovery_retired_metadata_growth import observe_binding_codec
from tests.unit.test_canonical_discovery_root_reclamation_growth import sample

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX complete discovery turn")


@contextmanager
def prepared(root, inputs, template, retained, width, variant):
    admission, frozen = inputs
    with basis.case_for(root, admission, retained, width, template=template) as case:
        binding = child_binding(case.metadata.job, width=width)
        if variant != "prefix":
            case.metadata.open()
            basis.seed_core(case)
            with case.metadata.transaction() as (db, _):
                for index in range(retained):
                    # Retained original child rows share the queried table and
                    # indexes, but have independent archive/worker authority.
                    register_children(
                        db,
                        replace(case.metadata.job, original_build_id="retained-0"),
                        [child_binding(case.metadata.job, index + 2000)],
                    )
                if variant in {"children", "child_directory", "child-truncate", "busy", "stale"}:
                    register_children(db, case.metadata.job, [binding])
            if variant.startswith("corpus"):
                pinned = jobs.pin_discovery_corpus(case.factory, case.metadata.job, frozen)
                assert pinned is not None
                case.metadata.job = pinned
        case.metadata.close()
        lost = jobs.lose_discovery_corpus(case.factory, case.metadata.job, failure="x" * width)
        assert lost is not None
        with case.factory() as session, session.begin():
            job = jobs.retire_discovery_job(session, lost)
            assert job is not None
    root.chmod(0o700)
    namespace = case.path.parent
    if variant in {"children", "child_directory", "child-truncate", "busy", "stale"}:
        child_root = case.path / "journals"
        with lease_journal_validation_workspace(
            child_root, binding.directory_id, create=True, creation_current=lambda: True
        ) as child:
            assert child is not None
            file = child / "snapshot.sqlite3"
            file.write_bytes(b"x" * (17 if variant == "child-truncate" else 2))
            file.chmod(0o600)
    else:
        child_root = case.path / "journals"
        child_root.mkdir(mode=0o700)
        (child_root / ".coordinator").touch(mode=0o600)
    if variant in {"corpus", "corpus-truncate"}:
        file = case.path / "corpus.sqlite3"
        file.write_bytes(b"x" * (17 if variant == "corpus-truncate" else 2))
        file.chmod(0o600)
    target = {
        "child_directory": "child_directory",
        "corpus": "corpus",
        "corpus-truncate": "corpus",
        "journals_root": "journals_root",
        "sync-refusal": "journals_root",
        "metadata": "metadata",
        "directory": "directory",
    }.get(variant)
    if variant == "prefix":
        (child_root / ".coordinator").unlink()
        child_root.rmdir()
        (case.path / ".lease").unlink()
    if target is not None:
        for _ in range(12):
            if job.reclamation_phase == target:
                break
            turn = owner.DiscoveryRetirementTurn(case.factory, job, namespace)
            try:
                result = turn.advance(octet_budget=1024 * 1024)
                assert result.advanced
                job = result.job
            finally:
                turn.close()
        assert job.reclamation_phase == target
    if variant == "stale":
        with case.factory() as session, session.begin():
            session.execute(
                update(Row).where(Row.workspace_id == job.workspace_id).values(version="f" * 64)
            )
    descriptor = None
    if variant == "busy":
        descriptor = os.open(case.path / ".lease", os.O_RDWR)
        fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
    try:
        yield case, job, binding
    finally:
        if descriptor is not None:
            os.close(descriptor)
        case.engine.dispose()


@pytest.mark.growth_witness(
    "canonical-discovery-retirement-turn/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_retirement.DiscoveryRetirementTurn.advance",
)
def test_measured_complete_original_retirement_turn(
    tmp_path, fixed_inputs, catalog_templates, record_property
):
    binding = growth_binding(owner.DiscoveryRetirementTurn.advance)
    projection = project_bindings((binding,))
    samples = []
    variants = (
        "children",
        "child-truncate",
        "child_directory",
        "empty",
        "corpus",
        "corpus-truncate",
        "journals_root",
        "metadata",
        "directory",
        "prefix",
        "busy",
        "stale",
        "sync-refusal",
    )
    for retained, width in basis.SPECIFICATIONS:
        for cold in (False, True):
            for variant in variants:
                root = tmp_path / f"turn-{retained}-{width}-{cold}-{variant}"
                with prepared(
                    root,
                    fixed_inputs,
                    catalog_templates[(retained, width)],
                    retained,
                    width,
                    variant,
                ) as (case, job, selected):
                    recorder = basis.Recorder()
                    turn = owner.DiscoveryRetirementTurn(case.factory, job, case.path.parent)
                    dimensions = {
                        "retained_core_rows": retained * 2 + 4,
                        "retained_catalog_rows": retained * 3 + 3,
                        "control_octets": octets(
                            (
                                tuple(asdict(job).values()),
                                selected.to_json(),
                                "delete",
                                "FULL",
                                "schema_version",
                            )
                        ),
                        "path_octets": len(
                            os.fsencode(
                                case.path / "journals" / selected.directory_id / "snapshot.sqlite3"
                            )
                        ),
                        "octet_budget": 3,
                    }
                    try:
                        with pytest.MonkeyPatch.context() as patch:
                            if variant == "sync-refusal":

                                def uncertain(_descriptor):
                                    raise OSError(28, "actual composed sync capacity")

                                patch.setattr(os, "fsync", uncertain)
                            for name in ("unlink", "rmdir"):
                                actual = getattr(os, name)

                                def mutation(*args, _actual=actual, _recorder=recorder, **kwargs):
                                    if _recorder.active:
                                        _recorder.metrics["encoded_path_octets"] += len(
                                            os.fsencode(args[0])
                                        )
                                    return _actual(*args, **kwargs)

                                patch.setattr(os, name, mutation)
                            with (
                                recorder.observe(case, cold=cold),
                                direct_primitives(recorder),
                                observe_binding_codec(recorder),
                            ):
                                if variant == "stale":
                                    with pytest.raises(owner.StaleArchiveTask):
                                        turn.advance(octet_budget=3)
                                elif variant == "sync-refusal":
                                    with pytest.raises(owner.JournalValidationCapacity):
                                        turn.advance(octet_budget=3)
                                else:
                                    result = turn.advance(octet_budget=3)
                                    assert result.advanced is (variant != "busy")
                                    assert result.removed_octets <= 3
                                    recorder.metrics["removed_octets"] = result.removed_octets
                                # A rejected/failed execution keeps the retained
                                # owner; account its actual received close here.
                                turn.close()
                        assert turn._closed and not recorder.live
                        observed = sample(recorder, projection, binding, dimensions)
                        try:
                            validate_execution_samples(
                                projection, binding.declaration.id, [observed]
                            )
                        except ValueError:
                            record_property("failed_turn_primitives", observed)
                            raise
                        samples.append(observed)
                    finally:
                        turn.close()
    record_property("growth_samples", samples)
    record_property(
        "retirement_turn_conditions",
        "All Core/catalog SQL, driver, VM, complete row/parameter/binding widths, JSON, "
        "hashing, actual namespace and Native/neutral file primitives and dependent closes "
        "are observed inclusively. Independent retained rows and full selected failure/"
        "binding widths1/17/1025, cold/warm, every phase, empty prefix, busy, stale and "
        "actual capacity refusal. No payload I/O, enumeration, repair or worker reconstruction.",
    )


@pytest.mark.growth_witness(
    "canonical-discovery-retained-child-close/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_retired_metadata.RetiredDiscoveryMetadata.close",
)
@pytest.mark.parametrize("mode", ("prefix-probe", "child-lease", "child-directory"))
def test_measured_dependent_child_close_receiving(case, monkeypatch, record_property, mode):
    job, path, bindings = prepare_child(case, count=1)
    if mode == "child-directory":
        first = owner.DiscoveryRetirementTurn(case.factory, job, case.root)
        try:
            job = first.advance(octet_budget=1024 * 1024).job
        finally:
            first.close()
        assert job.reclamation_phase == "child_directory"
    turn = owner.DiscoveryRetirementTurn(case.factory, job, case.root)
    recorder = basis.Recorder()
    actual_open, actual_close = os.open, os.close
    targets = set()
    child_inode = (path / "journals" / bindings[0].directory_id).stat().st_ino

    def opened(name, flags, *args, **kwargs):
        descriptor = actual_open(name, flags, *args, **kwargs)
        if (mode == "prefix-probe" and name == bindings[0].directory_id) or (
            mode != "prefix-probe"
            and name == ".lease"
            and turn._metadata is not None
            and os.fstat(kwargs["dir_fd"]).st_ino == child_inode
        ):
            targets.add(descriptor)
        return descriptor

    def refused(descriptor):
        if descriptor in targets:
            raise OSError("actual original child close refused")
        return actual_close(descriptor)

    try:
        with monkeypatch.context() as patch, recorder.connect_only():
            patch.setattr(os, "open", opened)
            patch.setattr(os, "close", refused)
            with pytest.raises(OSError, match="child close refused"):
                turn.advance(octet_budget=1024 * 1024)
        metadata = turn._metadata
        assert metadata is not None and metadata._child_handles.uncertain
        assert turn._custody.verify_current()
        binding = growth_binding(owner.RetiredDiscoveryMetadata.close)
        projection = project_bindings((binding,))
        with recorder.observe(case, cold=True), direct_primitives(recorder):
            metadata.close()
        assert metadata._closed and not metadata._child_handles._callbacks
        assert turn._custody.verify_current()
        observed = sample(
            recorder,
            projection,
            binding,
            {
                "retained_core_rows": 4,
                "retained_catalog_rows": 4,
                "control_octets": octets(tuple(asdict(job).values())),
                "path_octets": len(os.fsencode(path / "discovery.sqlite3")),
            },
        )
        assert observed["metrics"]["close_calls"] >= 3
        assert observed["metrics"]["core_closed_connections"] == 1
        validate_execution_samples(projection, binding.declaration.id, [observed])
        record_property("growth_samples", [observed])
    finally:
        turn.close()
