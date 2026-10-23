from __future__ import annotations

import hashlib
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
import riverhog_canonical_json as canonical
from riverhog_core import incremental_plan as planner_owner
from riverhog_core.catalog_journal_validation_models import JournalValidationJobRecord as Job
from riverhog_core.catalog_models import CollectionUploadProvenanceEpochScanRecord as Scan
from riverhog_core.catalog_models import CollectionUploadProvenanceJournalRecord as Journal
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.checkpoint_sha256 import CheckpointSHA256
from riverhog_core.services import collection_uploads as owner
from riverhog_growth_contracts import (
    growth_binding,
    project_bindings,
    validate_execution_samples,
)
from sqlalchemy import delete, event, select

from tests.unit import test_finalization_readiness_growth as readiness
from tests.unit.test_journal_capture import retained
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_staged_journal_range import JOURNAL
from tests.unit.test_upload_journal_source_growth import octets
from tests.unit.test_upload_journal_validation_jobs_growth import Recorder
from tests.unit.test_upload_journal_validation_retirement_catalog_growth import fields


def sample(operation, dimensions, metrics, children=None):
    binding = growth_binding(operation)
    projection = project_bindings((binding,))
    observed = {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(
            canonical.canonical_json_bytes(projection["declarations"][binding.declaration.id])
        ).hexdigest(),
        "dimensions": dimensions,
        "metrics": {bound.metric: metrics[bound.metric] for bound in binding.declaration.bounds},
        "delegates": children or {},
    }
    validate_execution_samples(projection, binding.declaration.id, [observed])
    return observed


@contextmanager
def codecs(recorder):
    loads, dumps, encode = (
        canonical.json.loads,
        canonical.json.dumps,
        canonical.canonical_json_bytes,
    )
    assert planner_owner.canonical_json_bytes is encode

    def decoded(value, *args, **kwargs):
        if recorder.active:
            recorder.metrics["json_decodes"] += 1
            recorder.metrics["json_decoded_octets"] += octets(value)
        return loads(value, *args, **kwargs)

    def encoded(*args, **kwargs):
        value = encode(*args, **kwargs)
        if recorder.active:
            recorder.metrics["json_encodes"] += 1
            recorder.metrics["json_encoded_octets"] += len(value)
        return value

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(
            canonical,
            "json",
            SimpleNamespace(
                loads=decoded, dumps=dumps, JSONDecodeError=canonical.json.JSONDecodeError
            ),
        )
        patch.setattr(canonical, "canonical_json_bytes", encoded)
        patch.setattr(planner_owner, "canonical_json_bytes", encoded)
        yield


def rows(engine, *models):
    with engine.connect() as connection:
        return {
            model.__tablename__: tuple(
                connection.exec_driver_sql(f'SELECT * FROM "{model.__tablename__}"').fetchall()
            )
            for model in models
        }


def catalog_cases(root, *, large=True):
    specifications = [(1, 1, 0, 0, 1), (17, 1, 0, 0, 1)]
    if large:
        specifications.extend(
            (
                (1025, 1, 0, 0, 1),
                (1, 1025, 0, 0, 1),
                (1, 1, 65537, 0, 1),
                (1, 1, 0, 2**200, 1),
                (1, 1, 0, 0, 2**60),
            )
        )
    for index, (count, unrelated, width, version, identity) in enumerate(specifications):
        with retained(root / str(index), source(journal_id=JOURNAL)) as (engine, factory):
            factory.configure(autoflush=False, expire_on_commit=False)
            if identity != 1:
                readiness.clone_upload(factory, identity)
            readiness.clone_upload(factory, 2)
            selected = "selected-" + "é" * width
            ids = [selected, *(f"candidate-{number:08x}" for number in range(count - 1))]
            with factory() as session, session.begin():
                session.add_all(
                    Journal(
                        collection_id=identity,
                        journal_id=key,
                        bytes=0,
                        sha256="0" * 64,
                        content_hash_state=CheckpointSHA256().export_state(),
                        state="validating",
                        capture_attempt_id="current",
                        capture_version=version,
                        failure="opaque " + "f" * width,
                    )
                    for key in ids
                )
                session.add_all(
                    Journal(
                        collection_id=2,
                        journal_id=f"unrelated-{number:08x}",
                        bytes=0,
                        sha256="0" * 64,
                        content_hash_state=CheckpointSHA256().export_state(),
                        state="sealed",
                    )
                    for number in range(unrelated)
                )
            yield SimpleNamespace(
                engine=engine,
                factory=factory,
                identity=identity,
                selected=selected,
                ids=ids,
                width=width,
                version=version,
            )


def catalog_measure(case, operation, action, controls, excluded, cold):
    before = rows(case.engine, Journal, Scan, Job)
    dimensions = {
        "retained_rows": sum(map(len, before.values())),
        "metadata_row_octets": max(
            256, *(octets(row) for values in before.values() for row in values)
        ),
        "control_octets": octets(controls),
        "excluded_keys": excluded,
    }
    recorder = Recorder()
    with recorder.observe(case.engine, cold=cold), codecs(recorder):
        value = action()
    assert rows(case.engine, Journal, Scan, Job) == before
    return value, sample(operation, dimensions, recorder.metrics)


def stamp_samples(case, samples):
    for kind in ("absent", "source", "scan", "job", "stale-scan", "stale-job", "stale-both"):
        for cold in (False, True):
            scan_attempt = "stale" if kind in {"stale-scan", "stale-both"} else "current"
            job_attempt = "stale" if kind in {"stale-job", "stale-both"} else "current"
            with case.factory() as session, session.begin():
                session.execute(delete(Scan)).close()
                session.execute(delete(Job)).close()
                if kind in {"scan", "job", "stale-scan", "stale-job", "stale-both"}:
                    session.add(
                        Scan(
                            collection_id=case.identity,
                            journal_id=case.selected,
                            attempt_id=scan_attempt,
                            directory_id="directory",
                            algorithm="opaque",
                            version=case.version + 7,
                            next_frame=0,
                            byte_offset=0,
                            epoch=0,
                            state="scanning",
                            replay_binding_json="opaque " + "r" * case.width,
                        )
                    )
                if kind in {"job", "stale-scan", "stale-job", "stale-both"}:
                    session.add(
                        Job(
                            **{
                                **fields("a" * 32, linked=case.selected, width=case.width),
                                "collection_id": case.identity,
                                "attempt_id": job_attempt,
                            }
                        )
                    )
            key = "absent" if kind == "absent" else case.selected
            value, observed = catalog_measure(
                case,
                owner._upload_journal_validation_stamp,
                lambda key=key: owner._upload_journal_validation_stamp(
                    case.factory, case.identity, key
                ),
                (case.identity, key),
                0,
                cold,
            )
            expected = (
                None
                if kind == "absent"
                else (
                    "validating",
                    case.version,
                    case.version + 7 if kind in {"scan", "job", "stale-job"} else None,
                    "0" * 64 if kind in {"job", "stale-scan"} else None,
                )
            )
            assert value == expected
            samples.append(observed)


def selection_samples(case, samples):
    with case.factory() as session, session.begin():
        session.execute(delete(Scan)).close()
        session.execute(delete(Job)).close()
    with case.factory() as session:
        keys = tuple(
            sorted(
                session.execute(
                    select(Journal.collection_id, Journal.journal_id).where(
                        Journal.state == "validating"
                    )
                ).all()
            )
        )
    key_tuples = tuple(tuple(key) for key in keys)
    for cold in (False, True):
        for after in (None, key_tuples[0], key_tuples[-1], (2**61, "past-end")):
            for exclusions in ((), key_tuples[:1], key_tuples[:17], key_tuples):
                # Large independent populations need only bounded scheduler-sized exclusions.
                if len(exclusions) > 17:
                    continue
                controls = (after, exclusions)
                value, observed = catalog_measure(
                    case,
                    owner._next_upload_journal_validation,
                    lambda after=after, exclusions=exclusions: (
                        owner._next_upload_journal_validation(
                            case.factory, after=after, excluding=exclusions
                        )
                    ),
                    controls,
                    len(exclusions),
                    cold,
                )
                eligible = [key for key in key_tuples if key not in exclusions]
                following = [key for key in eligible if after is None or key > after]
                expected = next(iter(following or eligible), None)
                assert value == expected
                samples.append(observed)
    # Every eligible source disappears from scheduling when its state changes.
    with case.factory() as session, session.begin():
        for row in session.scalars(select(Journal).where(Journal.state == "validating")):
            row.state = "sealed"
    value, observed = catalog_measure(
        case,
        owner._next_upload_journal_validation,
        lambda: owner._next_upload_journal_validation(case.factory, after=None, excluding=()),
        (None, ()),
        0,
        False,
    )
    assert value is None
    samples.append(observed)


def catalog_samples(root, samples, *, large=True):
    for case in catalog_cases(root, large=large):
        stamp_samples(case, samples)
        selection_samples(case, samples)


@contextmanager
def interrupted_update(engine, enabled):
    def interrupt(_connection, _cursor, statement, *_args):
        if statement.lstrip().upper().startswith("UPDATE " + Upload.__tablename__.upper()):
            raise RuntimeError("scheduling transaction interrupted after its update")

    if enabled:
        event.listen(engine, "after_cursor_execute", interrupt)
    try:
        yield
    finally:
        if enabled:
            event.remove(engine, "after_cursor_execute", interrupt)


def scheduling_samples(root, samples, *, large=True):
    operation = owner.SqlAlchemyCollectionUploadService._schedule_finalization_if_ready
    child = owner._ready_for_finalization
    for case in readiness.cases(root, large=large):
        for kind in (
            "absent",
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
            "interrupted",
        ):
            dimensions, expected = readiness.prepare(
                case, "match" if kind in {"absent", "interrupted"} else kind
            )
            if kind == "interrupted" and not expected:
                continue
            with case.factory() as session, session.begin():
                upload = session.get(Upload, case.identity)
                upload.state, upload.archive_phase = "uploading", "uploading"
                upload.archive_next_attempt_at = None
            for cold in (False, True):
                identity = 2**61 if kind == "absent" else case.identity
                before = rows(case.engine, Upload)
                own_dimensions = {
                    "retained_rows": sum(map(len, before.values())),
                    "metadata_row_octets": max(
                        256, *(octets(row) for values in before.values() for row in values)
                    ),
                    "control_octets": octets((identity, readiness.NOW)),
                }
                recorder, children = Recorder(), []

                def observed_child(
                    session, upload, recorder=recorder, children=children, dimensions=dimensions
                ):
                    prior = dict(recorder.metrics)
                    try:
                        return child(session, upload)
                    finally:
                        metrics = {
                            name: value - prior.get(name, 0)
                            for name, value in recorder.metrics.items()
                        }
                        metrics.update(
                            connection_statements=0, changed_rows=0, source_payload_octets=0
                        )
                        metrics["sealed_count_calls"] = metrics["sealed_count_calls_limit"] = (
                            metrics["sql_statements"]
                        )
                        recorder.active = False
                        try:
                            children.append(sample(child, dimensions, metrics))
                        finally:
                            recorder.active = True

                error = (
                    ValueError
                    if kind in {"malformed", "noncanonical-number"}
                    else RuntimeError
                    if kind == "missing-planner" or (kind == "interrupted" and not cold)
                    else None
                )
                current = object.__new__(owner.SqlAlchemyCollectionUploadService)
                current._session_factory = case.factory
                with (
                    recorder.observe(case.engine, cold=cold),
                    codecs(recorder),
                    interrupted_update(case.engine, kind == "interrupted" and not cold),
                    pytest.MonkeyPatch.context() as patch,
                ):
                    patch.setattr(owner, "_ready_for_finalization", observed_child)
                    patch.setattr(owner, "utc_timestamp_now", lambda: readiness.NOW)
                    if error is None:
                        assert operation(current, identity) is None
                    else:
                        with pytest.raises(error):
                            operation(current, identity)
                metrics = dict(recorder.metrics)
                for name in ("sealed_count_calls", "sealed_count_calls_limit"):
                    metrics[name] = sum(row["metrics"][name] for row in children)
                metrics["readiness_calls"] = metrics["readiness_calls_limit"] = len(children)
                own_dimensions["readiness_calls"] = len(children)
                for item in growth_binding(child).declaration.dimensions:
                    own_dimensions["readiness_" + item.name] = (
                        dimensions[item.name] if children else 0
                    )
                assert len(children) == int(kind != "absent")
                if error or not expected or kind == "absent":
                    assert rows(case.engine, Upload) == before
                    assert metrics["changed_rows"] == int(kind == "interrupted" and not cold)
                else:
                    with case.factory() as session:
                        upload = session.get(Upload, identity)
                        assert (
                            upload.state,
                            upload.archive_phase,
                            upload.archive_next_attempt_at,
                        ) == ("finalizing", "finalization_queued", readiness.NOW)
                    assert metrics["changed_rows"] == int(not cold or kind == "interrupted")
                samples.append(sample(operation, own_dimensions, metrics, {"readiness": children}))


@pytest.mark.growth_witness
def test_actual_service_catalog_selection_and_attempt_stamps(tmp_path, record_property):
    samples = []
    catalog_samples(tmp_path, samples)
    record_property("growth_samples", samples)


@pytest.mark.growth_witness
def test_actual_service_readiness_scheduling_owns_complete_transaction(tmp_path, record_property):
    samples = []
    scheduling_samples(tmp_path, samples)
    record_property("growth_samples", samples)
