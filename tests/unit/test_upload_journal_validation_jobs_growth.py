from __future__ import annotations

import hashlib
import re
import sqlite3
from contextlib import ExitStack, contextmanager
from dataclasses import asdict, replace
from functools import partial
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.catalog_journal_validation_models import JournalValidationJobRecord as Job
from riverhog_core.checkpoint_sha256 import CheckpointSHA256
from riverhog_core.services import collection_uploads as source_owner
from riverhog_core.services import upload_journal_validation_jobs as owner
from riverhog_core.services.upload_journal_validation_owner import UploadJournalValidationOwner
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from sqlalchemy import delete, event, select, update

from tests.unit.test_journal_capture import retained
from tests.unit.test_journal_epoch_union import uid
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_staged_journal_range import JOURNAL
from tests.unit.test_upload_journal_certificate_edge_growth import ready
from tests.unit.test_upload_journal_source_growth import octets

TABLES = (owner.Journal, Job)


def snapshot(engine):
    with engine.connect() as connection:
        return {
            model.__tablename__: tuple(
                sorted(
                    connection.exec_driver_sql(f'SELECT * FROM "{model.__tablename__}"').fetchall(),
                    key=lambda row: str(row),
                )
            )
            for model in TABLES
        }


def selected(factory):
    with factory() as session:
        row = session.scalar(select(Job).where(Job.collection_id.is_not(None)))
        return (
            None
            if row is None
            else {column.key: getattr(row, column.key) for column in Job.__table__.columns}
        )


@contextmanager
def build_cases(root, *, large=True):
    specifications = [(1, 1, 0, False, False), (17, 1, 0, False, False), (1, 1, 0, True, False)]
    if large:
        specifications.extend(
            (
                (1, 17, 0, False, False),
                (1025, 1, 0, False, False),
                (1, 1025, 0, False, False),
                (1, 1, 65537, False, False),
                (1, 1, 0, False, True),
            )
        )
    with ExitStack() as resources:
        cases = []
        catalog = source_owner.admission_provenance_catalog()
        for index, (journals, jobs, payload, wide, malformed) in enumerate(specifications):
            path = root / str(index)
            raw = source(payload, journal_id=JOURNAL) + (b"malformed tail" if malformed else b"")
            engine, factory = resources.enter_context(retained(path, raw))
            factory.configure(autoflush=False, expire_on_commit=False)
            work, certificates = ready(factory)
            source_binding = certificates.binding.source.to_json()
            certificate_binding = certificates.binding.to_json()
            first = owner.select_job(
                factory,
                work,
                source_binding=source_binding,
                certificate_binding=certificate_binding,
            )
            if wide:
                with factory() as session, session.begin():
                    session.execute(update(Job).values(worker_epoch=2**62)).close()
            opening = selected(factory)
            current = UploadJournalValidationOwner(factory, path / "native")
            progresses, active = [], None
            try:
                for _ in range(10000):
                    step = current.advance(work, catalog=catalog)
                    record = selected(factory)
                    if record["progress_json"] is not None:
                        progress = owner.decode_progress(record["progress_json"])
                        if not progresses or progress != progresses[-1]:
                            progresses.append(progress)
                        if active is None and record["state"] == "active":
                            active = record
                    if step.outcome is not None:
                        complete, outcome = record, step.outcome
                        break
                else:
                    pytest.fail("native control fixture failed to complete")
            finally:
                current.close()
            assert first is not None and active is not None and len(progresses) > 1
            assert complete["state"] == ("failed" if malformed else "complete")
            with factory() as session, session.begin():
                session.add_all(
                    owner.Journal(
                        collection_id=work.collection_id,
                        journal_id=uid(20000 + number),
                        bytes=0,
                        sha256=hashlib.sha256(b"").hexdigest(),
                        content_hash_state=CheckpointSHA256().export_state(),
                    )
                    for number in range(journals - 1)
                )
                session.add_all(
                    Job(
                        workspace_id="e" * 24 + f"{number:08x}",
                        collection_id=None,
                        journal_id=None,
                        attempt_id=work.attempt_id,
                        source_binding_json=source_binding,
                        certificate_binding_json=certificate_binding,
                        worker_epoch=0,
                        version="0" * 64,
                        state="lost",
                    )
                    for number in range(jobs - 1)
                )
            cases.append(
                SimpleNamespace(
                    engine=engine,
                    factory=factory,
                    work=work,
                    source_binding=source_binding,
                    certificate_binding=certificate_binding,
                    opening=opening,
                    active=active,
                    complete=complete,
                    progresses=progresses,
                    outcome=outcome,
                    cleanup_ids={opening["workspace_id"]},
                    malformed=malformed,
                )
            )
        yield cases


@pytest.fixture(scope="module")
def job_cases(tmp_path_factory):
    with build_cases(tmp_path_factory.mktemp("validation-job-inputs")) as cases:
        yield cases


def reset(case, fields=None):
    with case.factory() as session, session.begin():
        session.execute(delete(Job).where(Job.workspace_id.in_(case.cleanup_ids))).close()
        if fields is not None:
            case.cleanup_ids.add(fields["workspace_id"])
            session.add(Job(**fields))


def canonical(value):
    return canonical_json_bytes(
        {key: str(item) if type(item) is int else item for key, item in asdict(value).items()}
    ).decode()


class Recorder:
    def __init__(self):
        self.active = False
        self.metrics = {
            name: 0
            for name in (
                "driver_statements",
                "sql_statements",
                "returned_rows",
                "returned_metadata_octets",
                "bound_parameter_octets",
                "sqlite_vm_instructions",
                "json_decodes",
                "json_encodes",
                "json_decoded_octets",
                "json_encoded_octets",
                "hashed_octets",
                "uuid_values",
            )
        }
        self.native, self.statements = [], []

    def row(self, value):
        if self.active and value is not None:
            self.metrics["returned_rows"] += 1
            assert not any(isinstance(item, bytes) for item in value)
            self.metrics["returned_metadata_octets"] += octets(value)
        return value

    def trace(self, _statement):
        if self.active:
            self.metrics["driver_statements"] += 1

    def tick(self):
        if self.active:
            self.metrics["sqlite_vm_instructions"] += 1
        return 0

    @contextmanager
    def observe(self, engine, *, cold):
        observer = self
        real_uuid = owner.uuid4
        engine.dispose()
        dbapi, connect = engine.dialect.dbapi, engine.dialect.dbapi.connect
        real_loads, real_encode, real_hash = (
            owner.json.loads,
            owner.canonical_json_bytes,
            owner.hashlib.sha256,
        )

        class Cursor(sqlite3.Cursor):
            def execute(self, sql, parameters=()):
                if observer.active:
                    observer.metrics["bound_parameter_octets"] += octets(parameters)
                return super().execute(sql, parameters)

            def fetchone(self):
                return observer.row(super().fetchone())

            def fetchall(self):
                return [observer.row(row) for row in super().fetchall()]

            def fetchmany(self, *args):
                return [observer.row(row) for row in super().fetchmany(*args)]

            def __next__(self):
                return observer.row(super().__next__())

        class Connection(sqlite3.Connection):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                observer.native.append(self)
                self.set_trace_callback(observer.trace)
                self.set_progress_handler(observer.tick, 1)
                self.set_authorizer(self.authorize)

            def cursor(self, *args, **kwargs):
                kwargs["factory"] = Cursor
                return super().cursor(*args, **kwargs)

            def authorize(self, action, _table, column, _database, _trigger):
                if (
                    observer.active
                    and action == sqlite3.SQLITE_READ
                    and column
                    in {
                        "content",
                        "canonical_json",
                        "exact_json",
                        "schema_json",
                    }
                ):
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

        def observed_connect(*args, **kwargs):
            kwargs["factory"] = Connection
            return connect(*args, **kwargs)

        def sql(_connection, _cursor, statement, parameters, _context, many):
            assert not many
            if observer.active:
                observer.metrics["sql_statements"] += 1
                observer.statements.append((statement, parameters))

        def loads(value, *args, **kwargs):
            observer.metrics["json_decodes"] += 1
            observer.metrics["json_decoded_octets"] += octets(value)
            return real_loads(value, *args, **kwargs)

        def encode(*args, **kwargs):
            observer.metrics["json_encodes"] += 1
            value = real_encode(*args, **kwargs)
            observer.metrics["json_encoded_octets"] += len(value)
            return value

        class Hash:
            def __init__(self, actual):
                self.actual = actual

            def update(self, value):
                observer.metrics["hashed_octets"] += len(value)
                self.actual.update(value)

            def copy(self):
                return Hash(self.actual.copy())

            def __getattr__(self, name):
                return getattr(self.actual, name)

        def sha256(value=b"", *args, **kwargs):
            observer.metrics["hashed_octets"] += len(value)
            return Hash(real_hash(value, *args, **kwargs))

        def uuid4():
            observer.metrics["uuid_values"] += 1
            return real_uuid()

        event.listen(engine, "before_cursor_execute", sql)
        try:
            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(dbapi, "connect", observed_connect)
                if not cold:
                    with engine.connect():
                        pass
                patch.setattr(owner, "json", SimpleNamespace(loads=loads))
                patch.setattr(owner, "canonical_json_bytes", encode)
                patch.setattr(owner, "uuid4", uuid4)
                patch.setattr(owner, "hashlib", SimpleNamespace(sha256=sha256))
                self.active = True
                try:
                    yield
                finally:
                    self.active = False
        finally:
            event.remove(engine, "before_cursor_execute", sql)
        self.metrics["connection_statements"] = (
            self.metrics["driver_statements"] - self.metrics["sql_statements"]
        )
        self.metrics["changed_rows"] = sum(db.total_changes for db in self.native)
        self.metrics["source_payload_octets"] = 0
        assert engine.pool.checkedout() == 0
        for statement, parameters in self.statements:
            with engine.connect() as connection:
                with connection.exec_driver_sql(
                    "EXPLAIN QUERY PLAN " + statement, parameters
                ) as result:
                    plan = " ".join(row[3] for row in result)
            assert "SCAN" not in plan and "USE TEMP" not in plan
            if statement.lstrip().upper().startswith(("SELECT", "UPDATE", "DELETE")):
                assert "SEARCH" in plan


def measure(
    case, mode, action, *, controls=(), job=None, cold=False, error=None, changed=0, inspect=None
):
    operation = getattr(owner, mode)
    before = snapshot(case.engine)
    proposed = tuple(asdict(job).values()) if job is not None else tuple(case.opening.values())
    control_octets = octets(controls)
    inputs = {
        "retained_rows": sum(map(len, before.values())),
        "metadata_row_octets": max(
            [
                octets(proposed),
                control_octets,
                *(octets(row) for values in before.values() for row in values),
            ]
        ),
        "control_octets": control_octets,
    }
    recorder = Recorder()
    if mode in {"encode_progress", "decode_progress", "outcome"}:
        # These actual codec functions have no engine or transaction at all.
        # The observer's warmed connection is created before measurement.
        cold = False
    result = None
    with recorder.observe(case.engine, cold=cold):
        if error is None:
            result = action()
        else:
            with pytest.raises(error):
                action()
    after = snapshot(case.engine)
    assert before[owner.Journal.__tablename__] == after[owner.Journal.__tablename__]
    assert recorder.metrics["changed_rows"] == changed
    if not changed:
        assert after == before
    if isinstance(result, owner.ValidationJob):
        case.cleanup_ids.add(result.workspace_id)
        with case.factory() as session:
            persisted = session.get(Job, result.workspace_id)
            assert asdict(result) == {
                column.key: getattr(persisted, column.key) for column in Job.__table__.columns
            }
    if inspect is not None:
        inspect(result)
    binding = growth_binding(operation)
    projection = project_bindings((binding,))
    sample = {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(
            canonical_json_bytes(projection["declarations"][binding.declaration.id])
        ).hexdigest(),
        "dimensions": inputs,
        "metrics": {
            bound.metric: recorder.metrics[bound.metric] for bound in binding.declaration.bounds
        },
        "delegates": {},
    }
    validate_execution_samples(projection, binding.declaration.id, [sample])
    return result, sample


def read_job(fields):
    return owner.ValidationJob(**fields)


def sql_samples(mode, cases, samples):
    for case in cases:
        for cold in (False, True):
            opening, active, complete = (
                read_job(getattr(case, field)) for field in ("opening", "active", "complete")
            )
            stale_source = replace(case.work, version=case.work.version + 1)
            stale_job = replace(
                active, version=("0" if active.version[0] != "0" else "1") + active.version[1:]
            )
            if mode == "select_job":
                arguments = {
                    "source_binding": case.source_binding,
                    "certificate_binding": case.certificate_binding,
                }
                for fields, source_work, changes in (
                    (None, case.work, 1),
                    (case.opening, case.work, 0),
                    (case.opening, stale_source, 0),
                ):
                    reset(case, fields)
                    result, sample = measure(
                        case,
                        mode,
                        partial(owner.select_job, case.factory, source_work, **arguments),
                        controls=tuple(arguments.values()),
                        cold=cold,
                        changed=changes,
                    )
                    if source_work is stale_source:
                        assert result is None
                    else:
                        assert (
                            result.attempt_id == case.work.attempt_id
                            and result.source_binding_json == case.source_binding
                            and result.certificate_binding_json == case.certificate_binding
                        )
                        assert (
                            result.worker_epoch == opening.worker_epoch
                            if fields
                            else result.worker_epoch == 0
                        )
                        assert result.state == "opening" and re.fullmatch(
                            "[0-9a-f]{32}", result.workspace_id
                        )
                    samples.append(sample)
                other = cases[1] if case is cases[0] else cases[0]
                reset(case, other.opening)
                old = read_job(other.opening)
                result, sample = measure(
                    case,
                    mode,
                    partial(owner.select_job, case.factory, case.work, **arguments),
                    controls=tuple(arguments.values()),
                    cold=cold,
                    changed=2,
                )
                assert (
                    result.attempt_id == case.work.attempt_id
                    and result.workspace_id != old.workspace_id
                )
                with case.factory() as session:
                    retired = session.get(Job, old.workspace_id)
                    assert (
                        retired.collection_id is None
                        and retired.journal_id is None
                        and retired.state == "retiring"
                    )
                    assert (
                        retired.source_binding_json == old.source_binding_json
                        and retired.progress_json == old.progress_json
                    )
                samples.append(sample)
                reset(case, case.opening)
                _, sample = measure(
                    case,
                    mode,
                    partial(
                        owner.select_job,
                        case.factory,
                        case.work,
                        **{**arguments, "source_binding": case.source_binding + " "},
                    ),
                    controls=(*arguments.values(), case.source_binding + " "),
                    cold=cold,
                    error=RuntimeError,
                )
                samples.append(sample)
            elif mode == "claim_worker":
                for fields, source_work, argument, changes in (
                    (case.opening, case.work, opening, 1),
                    (case.active, case.work, stale_job, 0),
                    (case.active, stale_source, active, 0),
                    (case.complete, case.work, complete, 0),
                ):
                    reset(case, fields)
                    result, sample = measure(
                        case,
                        mode,
                        partial(owner.claim_worker, case.factory, source_work, argument),
                        job=argument,
                        cold=cold,
                        changed=changes,
                    )
                    if changes:
                        assert (
                            result.worker_epoch == argument.worker_epoch + 1
                            and result.version != argument.version
                        )
                        assert (
                            replace(
                                result, worker_epoch=argument.worker_epoch, version=argument.version
                            )
                            == argument
                        )
                    else:
                        assert result is None
                    samples.append(sample)
            elif mode == "worker_current":
                for fields, argument, expected in (
                    (case.opening, opening, True),
                    (case.active, active, True),
                    (case.complete, complete, not case.malformed),
                    (case.active, replace(active, worker_epoch=active.worker_epoch + 1), False),
                    (
                        case.active,
                        replace(
                            active, certificate_binding_json=active.certificate_binding_json + " "
                        ),
                        False,
                    ),
                    (None, active, False),
                ):
                    reset(case, fields)
                    result, sample = measure(
                        case,
                        mode,
                        partial(owner.worker_current, case.factory, argument),
                        job=argument,
                        cold=cold,
                    )
                    assert result is expected
                    samples.append(sample)
            elif mode == "creation_current":
                for fields, source_work, argument, expected in (
                    (case.opening, case.work, opening, True),
                    (case.active, case.work, active, False),
                    (case.opening, stale_source, opening, False),
                    (case.opening, case.work, replace(opening, version=stale_job.version), False),
                    (None, case.work, opening, False),
                ):
                    reset(case, fields)
                    result, sample = measure(
                        case,
                        mode,
                        partial(owner.creation_current, case.factory, source_work, argument),
                        job=argument,
                        cold=cold,
                    )
                    assert result is expected
                    samples.append(sample)
            elif mode == "retire_source":
                for fields, attempt, changes in (
                    (case.opening, case.work.attempt_id, 1),
                    (case.complete, case.work.attempt_id, 1),
                    (case.opening, case.work.attempt_id + "-stale", 0),
                ):
                    reset(case, fields)

                    def retire(case=case, attempt=attempt):
                        with case.factory() as session, session.begin():
                            owner.retire_source(
                                session, case.work.collection_id, case.work.journal_id, attempt
                            )

                    _, sample = measure(case, mode, retire, cold=cold, changed=changes)
                    if changes:
                        with case.factory() as session:
                            row = session.get(Job, fields["workspace_id"])
                            assert (
                                row.collection_id is None
                                and row.journal_id is None
                                and row.state == "retiring"
                                and row.version != fields["version"]
                            )
                            assert (
                                row.progress_json == fields["progress_json"]
                                and row.outcome_json == fields["outcome_json"]
                            )
                    samples.append(sample)
            elif mode == "publish_progress":
                first, last = case.progresses[0], case.progresses[-1]
                arguments = [
                    (case.active, active, case.work, first, {}, 1, None),
                    (case.active, active, case.work, last, {"outcome": case.outcome}, 1, None),
                    (case.active, stale_job, case.work, first, {}, 0, None),
                    (case.active, active, stale_source, first, {}, 0, None),
                    (
                        case.active,
                        active,
                        case.work,
                        None,
                        {"lost": "retained fixture lost"},
                        1,
                        None,
                    ),
                    (
                        case.active,
                        active,
                        case.work,
                        replace(first, worker_epoch=first.worker_epoch + 1),
                        {},
                        0,
                        RuntimeError,
                    ),
                    (
                        {**case.active, "progress_json": owner.encode_progress(last)},
                        replace(active, progress_json=owner.encode_progress(last)),
                        case.work,
                        first,
                        {},
                        0,
                        RuntimeError,
                    ),
                ]
                for fields, argument, source_work, progress, extra, changes, error in arguments:
                    reset(case, fields)
                    encoded_progress = "" if progress is None else canonical(progress)
                    encoded_outcome = "" if "outcome" not in extra else canonical(extra["outcome"])
                    result, sample = measure(
                        case,
                        mode,
                        partial(
                            owner.publish_progress,
                            case.factory,
                            source_work,
                            argument,
                            progress,
                            **extra,
                        ),
                        controls=(
                            argument.progress_json,
                            encoded_progress,
                            encoded_outcome,
                            extra.get("lost"),
                        ),
                        job=argument,
                        cold=cold,
                        changed=changes,
                        error=error,
                    )
                    if changes:
                        assert (
                            result.version != argument.version
                            and result.worker_epoch == argument.worker_epoch
                        )
                        assert (
                            result.source_binding_json == argument.source_binding_json
                            and result.certificate_binding_json == argument.certificate_binding_json
                        )
                        assert result.progress_json == (
                            argument.progress_json if progress is None else encoded_progress
                        )
                        assert result.outcome_json == (encoded_outcome or None)
                        assert result.state == (
                            "lost"
                            if "lost" in extra
                            else ("failed" if case.outcome.failure is not None else "complete")
                            if "outcome" in extra
                            else "active"
                        )
                    else:
                        assert result is None
                    samples.append(sample)
            else:
                raise AssertionError(mode)
            reset(case)


def codec_samples(mode, cases, samples):
    for case in cases:
        reset(case)
        for progress in (
            case.progresses[0],
            case.progresses[len(case.progresses) // 2],
            case.progresses[-1],
        ):
            raw = canonical(progress)
            if mode == "encode_progress":
                result, sample = measure(
                    case, mode, partial(owner.encode_progress, progress), controls=(raw,)
                )
                assert result == raw
                samples.append(sample)
            elif mode == "decode_progress":
                result, sample = measure(
                    case, mode, partial(owner.decode_progress, raw), controls=(raw,)
                )
                assert result == progress
                samples.append(sample)
        if mode == "decode_progress":
            for raw in ("{", canonical(case.progresses[0]) + " ", "{}"):
                _, sample = measure(
                    case,
                    mode,
                    partial(owner.decode_progress, raw),
                    controls=(raw,),
                    error=RuntimeError,
                )
                samples.append(sample)
        if mode == "outcome":
            terminal_job = read_job(case.complete)
            result, sample = measure(
                case,
                mode,
                partial(owner.outcome, terminal_job),
                controls=(terminal_job.outcome_json,),
                job=terminal_job,
            )
            assert result == case.outcome
            samples.append(sample)
            for argument in (
                read_job(case.active),
                replace(terminal_job, outcome_json="{"),
                replace(terminal_job, outcome_json=terminal_job.outcome_json + " "),
            ):
                _, sample = measure(
                    case,
                    mode,
                    partial(owner.outcome, argument),
                    controls=(argument.outcome_json,),
                    job=argument,
                    error=RuntimeError,
                )
                samples.append(sample)


@pytest.mark.growth_witness(
    "riverhog-validation-job-select-job-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_jobs.select_job",
)
def test_actual_job_select_job_includes_owned_metadata_work(job_cases, record_property):
    samples = []
    record_property("growth_samples", samples)
    sql_samples("select_job", job_cases, samples)


@pytest.mark.growth_witness(
    "riverhog-validation-job-claim-worker-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_jobs.claim_worker",
)
def test_actual_job_claim_worker_includes_owned_metadata_work(job_cases, record_property):
    samples = []
    record_property("growth_samples", samples)
    sql_samples("claim_worker", job_cases, samples)


@pytest.mark.growth_witness(
    "riverhog-validation-job-worker-current-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_jobs.worker_current",
)
def test_actual_job_worker_current_includes_owned_metadata_work(job_cases, record_property):
    samples = []
    record_property("growth_samples", samples)
    sql_samples("worker_current", job_cases, samples)


@pytest.mark.growth_witness(
    "riverhog-validation-job-publish-progress-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_jobs.publish_progress",
)
def test_actual_job_publish_progress_includes_owned_metadata_work(job_cases, record_property):
    samples = []
    record_property("growth_samples", samples)
    sql_samples("publish_progress", job_cases, samples)


@pytest.mark.growth_witness(
    "riverhog-validation-job-creation-current-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_jobs.creation_current",
)
def test_actual_job_creation_current_includes_owned_metadata_work(job_cases, record_property):
    samples = []
    record_property("growth_samples", samples)
    sql_samples("creation_current", job_cases, samples)


@pytest.mark.growth_witness(
    "riverhog-validation-job-retire-source-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_jobs.retire_source",
)
def test_actual_job_retire_source_includes_owned_metadata_work(job_cases, record_property):
    samples = []
    record_property("growth_samples", samples)
    sql_samples("retire_source", job_cases, samples)


@pytest.mark.growth_witness(
    "riverhog-validation-job-encode-progress-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_jobs.encode_progress",
)
def test_actual_job_encode_progress_includes_owned_metadata_work(job_cases, record_property):
    samples = []
    record_property("growth_samples", samples)
    codec_samples("encode_progress", job_cases, samples)


@pytest.mark.growth_witness(
    "riverhog-validation-job-decode-progress-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_jobs.decode_progress",
)
def test_actual_job_decode_progress_includes_owned_metadata_work(job_cases, record_property):
    samples = []
    record_property("growth_samples", samples)
    codec_samples("decode_progress", job_cases, samples)


@pytest.mark.growth_witness(
    "riverhog-validation-job-outcome-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_jobs.outcome",
)
def test_actual_job_outcome_includes_owned_metadata_work(job_cases, record_property):
    samples = []
    record_property("growth_samples", samples)
    codec_samples("outcome", job_cases, samples)
