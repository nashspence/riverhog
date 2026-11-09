from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict, replace
from types import SimpleNamespace

import pytest
from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core import canonical_discovery_retired_metadata as owner
from riverhog_core.journal_validation_retirement import lease_retiring_journal_workspace
from riverhog_growth_contracts import growth_binding
from riverhog_provenance import replay_binding

from tests.support.catalog_work import octets
from tests.unit import test_canonical_discovery_metadata_growth as basis
from tests.unit.test_canonical_discovery_metadata_growth import (
    catalog_templates as catalog_templates,
)
from tests.unit.test_canonical_discovery_metadata_growth import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_metadata_growth import original as original
from tests.unit.test_canonical_discovery_metadata_growth import qualified as qualified
from tests.unit.test_canonical_discovery_retired_metadata import child_binding, register_children

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX retired Core custody")


@contextmanager
def selected_case(root, admission, template, retained, width, variant):
    with basis.case_for(root, admission, retained, width, template=template) as case:
        case.metadata.open()
        basis.seed_core(case)
        bindings = [
            child_binding(case.metadata.job, index + 1, width if index == 0 else 1)
            for index in range(retained)
        ]
        with case.metadata.transaction() as (db, _):
            register_children(db, case.metadata.job, bindings)
        case.metadata.close()
        with case.factory() as session, session.begin():
            job = jobs.retire_discovery_job(session, case.metadata.job)
            assert job is not None
            if variant == "selected":
                job = jobs.advance_discovery_reclamation(
                    session,
                    job,
                    "child_directory",
                    journal_id=bindings[0].journal_id,
                    directory_id=bindings[0].directory_id,
                )
                assert job is not None
    # The actual normal lease has ended; the factory's disposed engine can
    # reconnect for the independently held retired owner.
    with lease_retiring_journal_workspace(case.path.parent, job.workspace_id) as retired:
        assert retired is not None and retired.custody.verify_current()
        metadata = owner.RetiredDiscoveryMetadata(case.factory, job, retired)
        try:
            if variant in {"empty", "binding-refusal"}:
                with sqlite3.connect(case.path / "discovery.sqlite3") as db:
                    if variant == "empty":
                        db.execute(
                            "DELETE FROM discovery_journal_children WHERE build_id=? AND "
                            "admission_sha256=?",
                            (job.original_build_id, job.admission_sha256),
                        ).close()
                    else:
                        db.execute(
                            "UPDATE discovery_journal_children SET binding_json=? WHERE "
                            "journal_id=?",
                            (
                                replace(bindings[0], require_profiles=True).to_json(),
                                bindings[0].journal_id,
                            ),
                        ).close()
            yield case, metadata, bindings
        finally:
            metadata.close()
            case.engine.dispose()


@contextmanager
def observe_binding_codec(recorder):
    loads, encode = replay_binding.json.loads, replay_binding.canonical_json_bytes

    def decoded(raw, *args, **kwargs):
        if recorder.active:
            recorder.metrics["json_decodes"] += 1
            recorder.metrics["json_decoded_octets"] += octets(raw)
        return loads(raw, *args, **kwargs)

    def encoded(*args, **kwargs):
        result = encode(*args, **kwargs)
        if recorder.active:
            recorder.metrics["json_encodes"] += 1
            recorder.metrics["json_encoded_octets"] += len(result)
        return result

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(replay_binding, "json", SimpleNamespace(loads=decoded))
        patch.setattr(replay_binding, "canonical_json_bytes", encoded)
        yield


def exercise(tmp_path, fixed_inputs, catalog_templates, operation, record_property):
    admission, _ = fixed_inputs
    variants = {
        "open": ("current", "stale"),
        "verify_current": ("current", "stale", "closed"),
        "child": ("current", "selected", "empty", "binding-refusal", "stale"),
        "close": ("current", "closed"),
    }[operation]
    samples = []
    for retained, width in basis.SPECIFICATIONS:
        for cold in (False, True):
            for variant in variants:
                root = tmp_path / f"{operation}-{retained}-{width}-{cold}-{variant}"
                with selected_case(
                    root, admission, catalog_templates[(retained, width)], retained, width, variant
                ) as (case, metadata, bindings):
                    recorder = basis.Recorder()
                    if operation != "open":
                        with recorder.connect_only():
                            metadata.open()
                    if variant == "closed":
                        metadata.close()
                    if variant == "stale":
                        metadata._custody._held = False
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
                        "path_octets": len(os.fsencode(case.path / "discovery.sqlite3")),
                    }
                    try:
                        with observe_binding_codec(recorder), recorder.observe(case, cold=cold):
                            if operation == "verify_current":
                                assert metadata.verify_current() is (variant == "current")
                            elif operation == "child":
                                if variant == "stale":
                                    with pytest.raises(owner.StaleArchiveTask):
                                        metadata.child()
                                elif variant == "binding-refusal":
                                    with pytest.raises(owner.LostJournalValidationWorkspace):
                                        metadata.child()
                                else:
                                    result = metadata.child()
                                    assert (result is None) is (variant == "empty")
                                    if result is not None:
                                        assert result.binding == bindings[0]
                            elif operation == "open" and variant == "stale":
                                with pytest.raises(owner.StaleArchiveTask):
                                    metadata.open()
                            else:
                                getattr(metadata, operation)()
                    finally:
                        if variant == "stale":
                            metadata._custody._held = True
                    assert not recorder.live or operation == "open"
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
                    try:
                        sample = recorder.sample(
                            operation, dimensions, implementation=owner.RetiredDiscoveryMetadata
                        )
                    except ValueError:
                        record_property(
                            "failed_retired_primitive_metrics",
                            {
                                "operation": operation,
                                "dimensions": dimensions,
                                "core": dict(recorder.metrics),
                                "catalog": dict(recorder.catalog.metrics),
                            },
                        )
                        raise
                    binding = growth_binding(getattr(owner.RetiredDiscoveryMetadata, operation))
                    assert sample["declaration"] == binding.declaration.id
                    samples.append(sample)
    record_property("growth_samples", samples)
    record_property(
        "retired_metadata_conditions",
        "Actual catalog/Core SQL, driver/VM, complete row/parameter widths, exclusive descriptor "
        "checks and Native binding decode/canonical encode work. Independent retained rows and "
        "binding scalar widths 1/17/1025; cold/warm catalog; stale/closed, empty/exact selected "
        "child "
        "and binding refusal. Routing observations grant no Native acceptance or physical "
        "disposal.",
    )


@pytest.mark.growth_witness(
    "canonical-discovery-retired-metadata-open/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_retired_metadata.RetiredDiscoveryMetadata.open",
)
def test_measured_retired_metadata_open(tmp_path, fixed_inputs, catalog_templates, record_property):
    exercise(tmp_path, fixed_inputs, catalog_templates, "open", record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-retired-metadata-current/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation=(
        "riverhog_core.canonical_discovery_retired_metadata.RetiredDiscoveryMetadata.verify_current"
    ),
)
def test_measured_retired_metadata_current(
    tmp_path, fixed_inputs, catalog_templates, record_property
):
    exercise(tmp_path, fixed_inputs, catalog_templates, "verify_current", record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-retired-metadata-child/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_retired_metadata.RetiredDiscoveryMetadata.child",
)
def test_measured_retired_metadata_child(
    tmp_path, fixed_inputs, catalog_templates, record_property
):
    exercise(tmp_path, fixed_inputs, catalog_templates, "child", record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-retired-metadata-close/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_retired_metadata.RetiredDiscoveryMetadata.close",
)
def test_measured_retired_metadata_close(
    tmp_path, fixed_inputs, catalog_templates, record_property
):
    exercise(tmp_path, fixed_inputs, catalog_templates, "close", record_property)
