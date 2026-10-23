from __future__ import annotations

import hashlib
from types import SimpleNamespace

import pytest
import riverhog_canonical_json as canonical
from riverhog_core import incremental_plan as planner_owner
from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord as Volume
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.collection_plan import CollectionVolumePolicy
from riverhog_core.domain.archive import ArchiveArtifact
from riverhog_core.incremental_plan import (
    IncrementalVolumePlannerCheckpoint,
    incremental_volume_planner_checkpoint_bytes,
)
from riverhog_core.services import collection_uploads as owner
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from sqlalchemy import delete

from tests.unit.test_journal_capture import retained
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_staged_journal_range import JOURNAL
from tests.unit.test_upload_journal_validation_jobs_growth import Recorder
from tests.unit.test_upload_journal_validation_owner import scheduler_for

NOW = "2026-01-01T00:00:00Z"


def volume(identity, ordinal, state):
    return Volume(
        collection_id=identity,
        object_id=f"volume-{ordinal}",
        sequence=ordinal,
        kind="pack",
        relative_path=f"volume-{ordinal}",
        object_path=f"volume-{ordinal}",
        plaintext_bytes=0,
        source_bytes=0,
        unit_plaintext_bytes=1,
        plan_json="{}",
        plan_sha256="0" * 64,
        state=state,
        updated_at=NOW,
    )


def clone_upload(factory, identity):
    with factory() as session, session.begin():
        old = session.get(Upload, 1)
        values = {column.key: getattr(old, column.key) for column in Upload.__table__.columns}
        values.update(collection_id=identity, idempotency_key=f"readiness-{identity}")
        session.add(Upload(**values))


def cases(root, *, large=True):
    specifications = [(1, 0, 0, 1), (0, 0, 0, 1), (17, 0, 0, 1)]
    if large:
        specifications.extend(
            ((1025, 0, 0, 1), (1, 1025, 0, 1), (1, 0, 17, 1), (1, 0, 1025, 1), (1, 0, 0, 2**60))
        )
    for index, (rows, unrelated, pending, identity) in enumerate(specifications):
        with retained(root / str(index), source(journal_id=JOURNAL)) as (engine, factory):
            factory.configure(autoflush=False, expire_on_commit=False)
            if identity != 1:
                clone_upload(factory, identity)
            if unrelated:
                clone_upload(factory, 2)
                with factory() as session, session.begin():
                    session.add_all(volume(2, number, "sealed") for number in range(unrelated))
            yield SimpleNamespace(
                engine=engine,
                factory=factory,
                identity=identity,
                rows=rows,
                unrelated=unrelated,
                pending=pending,
            )


def prepare(case, kind):
    states = ["sealed"] * case.rows
    if kind == "unsealed-extra":
        states.append("planned")
    expected_sequence = max(1, case.rows)
    if kind == "missing-volume":
        expected_sequence = case.rows + 1
    if kind == "excess-volume":
        expected_sequence = max(1, case.rows - 1)
    if kind == "closed-empty":
        expected_sequence = 0
    closed = kind != "open"
    artifacts = max(1, case.pending)
    payload_count = artifacts if kind != "payload-count" else artifacts - 1
    payload_bytes = artifacts - 1 if kind in {"payload-count", "payload-bytes"} else artifacts
    if kind == "empty-artifacts":
        artifacts = payload_count = payload_bytes = 0
    pending = (
        tuple(
            ArchiveArtifact(artifact_id=f"{number + 1:064x}", bytes=1, sha256="0" * 64)
            for number in range(case.pending)
        )
        if not closed
        else ()
    )
    checkpoint = IncrementalVolumePlannerCheckpoint(
        policy=CollectionVolumePolicy(),
        next_artifact_order=artifacts,
        artifacts_seen=artifacts,
        bytes_seen=artifacts,
        pending_pack_artifacts=pending,
        closed=closed,
        next_sequence=expected_sequence,
    )
    text = incremental_volume_planner_checkpoint_bytes(checkpoint).decode()
    if kind == "malformed":
        text = "{}"
    if kind == "missing-planner":
        text = ""
    if kind == "noncanonical-number":
        text = "1e20"
    admitted = int(
        kind
        not in {
            "open",
            "closed-empty",
            "payload-count",
            "payload-bytes",
            "empty-artifacts",
            "malformed",
            "missing-planner",
            "noncanonical-number",
        }
    )
    expected = admitted and states.count("sealed") == expected_sequence
    with case.factory() as session, session.begin():
        session.execute(delete(Volume).where(Volume.collection_id == case.identity)).close()
        session.add_all(volume(case.identity, number, state) for number, state in enumerate(states))
        upload = session.get(Upload, case.identity)
        upload.planner_checkpoint_json = text
        upload.artifact_count = upload.artifact_bytes = artifacts
        upload.payload_sealed_artifact_count, upload.payload_sealed_artifact_bytes = (
            payload_count,
            payload_bytes,
        )
    return {
        "checkpoint_octets": len(text.encode()),
        "volume_rows": len(states),
        "catalog_volume_rows": len(states) + case.unrelated,
        "count_admitted": admitted,
        "lookup_octets": len(str(case.identity).encode()),
        "counter_octets": len(str(len(states)).encode()),
    }, bool(expected)


def measure(case, dimensions, *, error=None):
    recorder = Recorder()
    loads, dumps, encode = (
        canonical.json.loads,
        canonical.json.dumps,
        canonical.canonical_json_bytes,
    )
    assert planner_owner.canonical_json_bytes is encode
    codecs = {
        name: 0
        for name in ("json_decodes", "json_decoded_octets", "json_encodes", "json_encoded_octets")
    }

    def decoded(value, *args, **kwargs):
        if recorder.active:
            codecs["json_decodes"] += 1
            codecs["json_decoded_octets"] += (
                len(value.encode()) if isinstance(value, str) else len(value)
            )
        return loads(value, *args, **kwargs)

    def encoded(*args, **kwargs):
        value = encode(*args, **kwargs)
        if recorder.active:
            codecs["json_encodes"] += 1
            codecs["json_encoded_octets"] += len(value)
        return value

    returned = None
    with recorder.observe(case.engine, cold=False), pytest.MonkeyPatch.context() as patch:
        patch.setattr(
            canonical,
            "json",
            SimpleNamespace(
                loads=decoded, dumps=dumps, JSONDecodeError=canonical.json.JSONDecodeError
            ),
        )
        patch.setattr(canonical, "canonical_json_bytes", encoded)
        patch.setattr(planner_owner, "canonical_json_bytes", encoded)
        recorder.active = False
        with case.factory() as session:
            upload = session.get(Upload, case.identity)
            recorder.active = True
            try:
                if error is None:
                    returned = owner._ready_for_finalization(session, upload)
                else:
                    with pytest.raises(error):
                        owner._ready_for_finalization(session, upload)
            finally:
                recorder.active = False
    metrics = dict(recorder.metrics)
    for name, value in codecs.items():
        metrics[name] += value
    metrics["sealed_count_calls"] = sum(
        "count(" in statement.lower() and Volume.__tablename__ in statement
        for statement, _ in recorder.statements
    )
    metrics["sealed_count_calls_limit"] = metrics["sealed_count_calls"]
    binding = growth_binding(owner._ready_for_finalization)
    projection = project_bindings((binding,))
    sample = {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(
            canonical.canonical_json_bytes(projection["declarations"][binding.declaration.id])
        ).hexdigest(),
        "dimensions": dimensions,
        "metrics": {bound.metric: metrics[bound.metric] for bound in binding.declaration.bounds},
        "delegates": {},
    }
    validate_execution_samples(projection, binding.declaration.id, [sample])
    return returned, sample


def readiness_samples(root, samples, *, large=True):
    for case in cases(root, large=large):
        for kind in (
            "open",
            "closed-empty",
            "payload-count",
            "payload-bytes",
            "empty-artifacts",
            "match",
            "missing-volume",
            "excess-volume",
            "unsealed-extra",
            "malformed",
            "missing-planner",
            "noncanonical-number",
        ):
            dimensions, expected = prepare(case, kind)
            error = (
                ValueError
                if kind in {"malformed", "noncanonical-number"}
                else RuntimeError
                if kind == "missing-planner"
                else None
            )
            returned, sample = measure(case, dimensions, error=error)
            if error is None:
                assert returned is expected
            assert sample["metrics"]["changed_rows"] == 0
            assert case.engine.pool.checkedout() == 0
            samples.append(sample)


@pytest.mark.growth_witness(
    "riverhog-finalization-readiness-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.collection_uploads._ready_for_finalization",
)
def test_actual_readiness_preserves_exact_count_after_cheap_prerequisites(
    tmp_path, record_property
):
    samples = []
    record_property("growth_samples", samples)
    readiness_samples(tmp_path, samples)


def test_validation_progress_does_not_scan_volumes_while_planning_is_open(tmp_path):
    observations = []
    for count in (0, 17, 1025):
        with retained(tmp_path / str(count), source(journal_id=JOURNAL)) as (engine, factory):
            factory.configure(autoflush=False, expire_on_commit=False)
            with factory() as session, session.begin():
                session.add_all(volume(1, number, "sealed") for number in range(count))
            service = scheduler_for(factory)
            schedule = service._schedule_finalization_if_ready

            def measured(collection_id, schedule=schedule, engine=engine):
                recorder = Recorder()
                with recorder.observe(engine, cold=False):
                    schedule(collection_id)
                assert not any(
                    Volume.__tablename__ in statement for statement, _ in recorder.statements
                )
                observations.append(recorder.metrics["sqlite_vm_instructions"])

            service._schedule_finalization_if_ready = measured
            try:
                for _ in range(2):
                    assert service.process_due_provenance_journal_validations(limit=1) == 1
                assert engine.pool.checkedout() == 0
            finally:
                service.close()
    assert len(observations) == 6 and len(set(observations)) == 1
