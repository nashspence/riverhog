from __future__ import annotations

import hashlib
from contextlib import contextmanager
from dataclasses import asdict
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.services import upload_journal_result as owner
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import graph_certificates, replay_binding
from sqlalchemy import event, insert, select, update

from tests.support.catalog_work import octets
from tests.unit.test_upload_journal_result import JOURNAL, Job, Journal, sealed, source_owner

PREPARE = owner.prepare_upload_journal_result
CURRENT = owner.SealedJournalResultAdmission.verify_current


class Rows:
    def __init__(self, actual, metrics, *, base=None):
        self.actual, self.metrics, self.base = actual, metrics, base or actual

    def __enter__(self):
        self.actual.__enter__()
        return self

    def __exit__(self, *exc):
        return self.actual.__exit__(*exc)

    def mappings(self):
        return Rows(self.actual.mappings(), self.metrics, base=self.base)

    def one_or_none(self):
        row = self.actual.one_or_none()
        if row is not None:
            self.metrics["returned_rows"] += 1
            self.metrics["returned_octets"] += octets(
                tuple(row.values()) if hasattr(row, "values") else tuple(row)
            )
        return row


@contextmanager
def observe(session, operation, monkeypatch):
    metrics = {bound.metric: 0 for bound in growth_binding(operation).declaration.bounds}
    executions, results = [], []
    driver = session.connection().connection.driver_connection

    def tick():
        metrics["sqlite_vm_instructions"] += 1
        return 0

    execute = session.execute

    def observed(statement, *args, **kwargs):
        result = execute(statement, *args, **kwargs)
        executions.append((statement, ()))
        wrapped = Rows(result, metrics)
        results.append(wrapped)
        return wrapped

    def forbidden(*args, **kwargs):
        metrics["native_storage_calls"] += 1
        pytest.fail("metadata admission invoked Native storage or kernel custody")

    def before(_connection, _cursor, _statement, parameters, _context, _many):
        metrics["sql_statements"] += 1
        metrics["parameter_octets"] += octets(parameters)

    with monkeypatch.context() as patch:
        patch.setattr(session, "execute", observed)
        patch.setattr(owner, "open_retained_journal_result", forbidden)
        patch.setattr(owner, "lease_journal_validation_workspace", forbidden)
        for module in (owner.jobs, replay_binding, graph_certificates):
            loads = module.json.loads

            def decoded(raw, *args, _loads=loads, **kwargs):
                metrics["json_decodes"] += 1
                metrics["json_decoded_octets"] += len(raw.encode() if isinstance(raw, str) else raw)
                return _loads(raw, *args, **kwargs)

            patch.setattr(module, "json", SimpleNamespace(loads=decoded))
            encode = module.canonical_json_bytes

            def encoded(value, _encode=encode):
                raw = _encode(value)
                metrics["json_encodes"] += 1
                metrics["json_encoded_octets"] += len(raw)
                return raw

            patch.setattr(module, "canonical_json_bytes", encoded)
            if hasattr(module, "hashlib"):
                original = module.hashlib.sha256

                class Hash:
                    def __init__(self, raw=b"", _original=original, **kwargs):
                        metrics["hashed_octets"] += len(raw)
                        self.actual = _original(raw, **kwargs)

                    def update(self, raw):
                        metrics["hashed_octets"] += len(raw)
                        return self.actual.update(raw)

                    def __getattr__(self, name):
                        return getattr(self.actual, name)

                patch.setattr(module, "hashlib", SimpleNamespace(sha256=Hash))
        event.listen(session.bind, "before_cursor_execute", before)
        driver.set_progress_handler(tick, 1)
        try:
            yield metrics, executions
        finally:
            driver.set_progress_handler(None, 0)
            event.remove(session.bind, "before_cursor_execute", before)
        metrics["open_cursors"] = sum(not result.base.closed for result in results)


def sample(operation, dimensions, metrics):
    binding = growth_binding(operation)
    projection = project_bindings((binding,))
    result = {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(
            canonical_json_bytes(projection["declarations"][binding.declaration.id])
        ).hexdigest(),
        "dimensions": dimensions,
        "metrics": dict(metrics),
        "delegates": {},
    }
    validate_execution_samples(projection, binding.declaration.id, [result])
    return result


def seed(factory, retained):
    with factory() as session, session.begin():
        journal = session.execute(select(Journal).where(Journal.journal_id == JOURNAL)).scalar_one()
        job = session.execute(select(Job).where(Job.collection_id == 1)).scalar_one()
        journal_row = {
            column.key: getattr(journal, column.key) for column in Journal.__table__.columns
        }
        job_row = {column.key: getattr(job, column.key) for column in Job.__table__.columns}
        session.execute(
            insert(Journal),
            [
                dict(
                    journal_row,
                    journal_id=f"urn:uuid:{index:08x}-7777-4777-8777-777777777777",
                    state="failed",
                )
                for index in range(retained)
            ],
        )
        session.execute(
            insert(Job),
            [
                dict(
                    job_row,
                    workspace_id=f"{index + 2000000:032x}",
                    collection_id=None,
                    journal_id=None,
                    state="lost",
                    failure="independent retained job",
                )
                for index in range(retained)
            ],
        )


def exercise(tmp_path, monkeypatch, record_property, operation):
    samples = []
    catalog = source_owner.admission_provenance_catalog()
    for retained in (1, 17, 1025):
        with sealed(tmp_path / str(retained)) as (_, factory, _root, job, _raw):
            seed(factory, retained)
            with factory() as session, session.begin():
                admitted = PREPARE(session, 1, JOURNAL, catalog=catalog)
            width = (
                octets(tuple(value for _key, value in admitted._source))
                + octets(tuple(asdict(admitted.job).values()))
                + octets((1, JOURNAL, catalog.checkpoint_sha256()))
            )
            for kind in ("first", "retry", "rejected"):
                selected_id = JOURNAL if kind != "rejected" else "x" * 4096
                dimensions = {
                    "catalog_journals": retained + 1,
                    "catalog_jobs": retained + 1,
                    "control_octets": width + octets(selected_id),
                }
                with factory() as session, session.begin():
                    if operation is CURRENT and kind == "rejected":
                        session.execute(
                            update(Job)
                            .where(Job.workspace_id == job.workspace_id)
                            .values(version="e" * 64)
                        )
                    with observe(session, operation, monkeypatch) as (metrics, executions):
                        if operation is PREPARE:
                            if kind == "rejected":
                                with pytest.raises(KeyError):
                                    PREPARE(session, 1, selected_id, catalog=catalog)
                            else:
                                result = PREPARE(session, 1, JOURNAL, catalog=catalog)
                                metrics["detached_controls"] += int(
                                    isinstance(result, owner.SealedJournalResultAdmission)
                                )
                        else:
                            assert CURRENT(admitted, session) is (kind != "rejected")
                    samples.append(sample(operation, dimensions, metrics))
                    for statement, _parameters in executions:
                        compiled = statement.compile(
                            session.bind, compile_kwargs={"literal_binds": True}
                        )
                        plan = " ".join(
                            row[3]
                            for row in session.connection().exec_driver_sql(
                                "EXPLAIN QUERY PLAN " + str(compiled)
                            )
                        )
                        assert "SCAN " not in plan and "TEMP B-TREE" not in plan, plan
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-sealed-journal-metadata-admission-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_result.prepare_upload_journal_result",
)
def test_actual_detached_admission_work_is_independent_of_other_catalog_rows(
    tmp_path, monkeypatch, record_property
):
    exercise(tmp_path, monkeypatch, record_property, PREPARE)


@pytest.mark.growth_witness(
    "riverhog-sealed-journal-admission-current-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_result.SealedJournalResultAdmission.verify_current",
)
def test_actual_detached_currentness_is_one_indexed_join(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, CURRENT)
