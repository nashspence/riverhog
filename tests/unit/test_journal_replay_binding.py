from __future__ import annotations

import hashlib
import json
import sqlite3
import subprocess
import sys
from contextlib import contextmanager
from dataclasses import replace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.services import collection_uploads as owner
from riverhog_core.services import upload_journal_rank as rank
from riverhog_core.services import upload_journal_replay as replay
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import validate_journal
from riverhog_provenance.replay_binding import JournalReplayBinding
from riverhog_provenance_contracts import ContractCatalog, ProvenanceContractBinding
from riverhog_provenance_contracts import schemas as schemas_owner
from sqlalchemy import event, update

from tests.unit.test_journal_capture import retained
from tests.unit.test_journal_epoch_rank import finish as finish_rank
from tests.unit.test_journal_epoch_rank import prepared
from tests.unit.test_journal_epoch_scan import ObservedWindow
from tests.unit.test_journal_epoch_union import uid
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_journal_validation_handoff import (
    complete_capture,
    native_outcome,
    service_for,
    terminal,
)
from tests.unit.test_staged_journal_range import JOURNAL, Journal

Scan = replay.Scan


def pack(name, title="one"):
    return ProvenanceContractBinding(
        contract_id=f"urn:test:pack:{name}",
        schemas=[
            {
                "$id": f"urn:test:schema:{name}",
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "title": title,
            }
        ],
    )


def ready(factory):
    work = prepared(factory)
    finish_rank(factory, work)
    return work


def progress(factory):
    with factory() as session:
        row = session.get(Scan, (1, JOURNAL))
        return row.version, row.replay_binding_json


def test_catalog_pin_names_exact_constructed_core_and_external_bindings(monkeypatch):
    one, two = pack("one"), pack("two")
    base = ContractCatalog()
    original = ContractCatalog((one, two))
    assert original.checkpoint_sha256() == ContractCatalog((one, two, one)).checkpoint_sha256()
    assert original.checkpoint_sha256() != base.checkpoint_sha256()
    assert original.checkpoint_sha256() != ContractCatalog((two, one)).checkpoint_sha256()
    assert (
        original.checkpoint_sha256()
        != ContractCatalog((pack("one", "changed"), two)).checkpoint_sha256()
    )
    assert (
        original.with_contracts((pack("three"),)).checkpoint_sha256()
        != original.checkpoint_sha256()
    )
    encoded = one.schemas
    encoded["urn:test:schema:one"]["title"] = "mutable caller copy"
    expected = original.checkpoint_sha256()

    def no_repeat(*_args, **_kwargs):
        raise AssertionError("catalog replay pin repeated whole-catalog work")

    monkeypatch.setattr(schemas_owner.hashlib, "sha256", no_repeat)
    assert original.checkpoint_sha256() == expected
    assert len(expected) == 64


@pytest.mark.parametrize(
    "fault", ["algorithm", "mode", "extra", "scalar", "policy", "identity", "canonical"]
)
def test_binding_codec_rejects_unknown_or_changed_protocol(tmp_path, fault):
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        work = complete_capture(factory)
        binding = replay._binding(work, owner.admission_provenance_catalog())
        raw = binding.to_json()
        assert JournalReplayBinding.from_json(raw) == binding
        opaque = replace(binding, attempt_id="replacement", directory_id="owned-workspace")
        assert JournalReplayBinding.from_json(opaque.to_json()) == opaque
        value = json.loads(raw)
        if fault == "algorithm":
            value["algorithm"] = "different"
        elif fault == "mode":
            value["mode"] = "online"
        elif fault == "extra":
            value["unknown"] = "ignored"
        elif fault == "scalar":
            value["source_bytes"] = "00"
        elif fault == "policy":
            value["require_profiles"] = 0
        elif fault == "identity":
            value["attempt_id"] = ""
        changed = (
            json.dumps(value) if fault == "canonical" else canonical_json_bytes(value).decode()
        )
        with pytest.raises(RuntimeError, match="retained"):
            JournalReplayBinding.from_json(changed)


def test_prepared_binding_is_idempotent_and_accepts_no_semantics(tmp_path):
    with retained(tmp_path, source(journal_id=JOURNAL)) as (engine, factory):
        work = ready(factory)
        before = progress(factory)
        assert before[1] is None
        assert replay.prepare_upload_journal_replay(
            factory, work, catalog=owner.admission_provenance_catalog()
        )
        version, raw = progress(factory)
        assert version == before[0] + 1 and raw is not None
        assert JournalReplayBinding.from_json(raw) == replay._binding(
            work, owner.admission_provenance_catalog()
        )
        assert replay.prepare_upload_journal_replay(
            factory, work, catalog=owner.admission_provenance_catalog()
        )
        assert progress(factory) == (version, raw)
        assert replay.verify_upload_journal_replay_binding(
            factory, work, owner.admission_provenance_catalog()
        )
        assert terminal(factory)["anchor"] is None and terminal(factory)["state"] == "validating"
        assert engine.pool.checkedout() == 0


@pytest.mark.parametrize("phase", ["reopen", "compute", "publish"])
def test_catalog_replacement_cannot_reuse_replay_binding_or_publish_anchor(
    tmp_path, monkeypatch, phase
):
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        work = ready(factory)
        assert replay.prepare_upload_journal_replay(
            factory, work, catalog=owner.admission_provenance_catalog()
        )
        outcome = native_outcome(factory, work) if phase == "publish" else None
        monkeypatch.setattr(
            owner, "admission_provenance_catalog", lambda: ContractCatalog((pack("replacement"),))
        )
        with pytest.raises(RuntimeError, match="retained replay inputs"):
            if phase == "reopen":
                replay.prepare_upload_journal_replay(
                    factory, work, catalog=owner.admission_provenance_catalog()
                )
            elif phase == "compute":
                native_outcome(factory, work)
            else:
                owner._commit_upload_journal_validation(factory, work, outcome)
        assert terminal(factory)["anchor"] is None and terminal(factory)["state"] == "validating"


@pytest.mark.parametrize(
    "field",
    [
        "source_sha256",
        "source_bytes",
        "source_version",
        "frame_count",
        "catalog_sha256",
        "require_profiles",
    ],
)
def test_changed_binding_fails_closed_before_native_replay(tmp_path, field):
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        work = ready(factory)
        binding = replay._binding(work, owner.admission_provenance_catalog())
        value = (
            False
            if field == "require_profiles"
            else "f" * 64
            if field.endswith("sha256")
            else "999"
        )
        if field == "require_profiles":
            value = True
        with factory() as session, session.begin():
            session.execute(
                update(Scan).values(
                    replay_binding_json=replace(binding, **{field: value}).to_json()
                )
            )
        with pytest.raises(RuntimeError, match="retained replay inputs"):
            native_outcome(factory, work)
        assert terminal(factory)["anchor"] is None


@pytest.mark.parametrize(
    "changed", ["capture_attempt_id", "capture_directory_id", "capture_version", "sha256"]
)
def test_stale_worker_cannot_bind_replay_to_replacement_source(tmp_path, changed):
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        source_work = ready(factory)
        work = rank._begin(factory, source_work)
        binding = replay._binding(source_work, owner.admission_provenance_catalog())
        before = progress(factory)
        with factory() as session, session.begin():
            session.execute(
                update(Journal).values({changed: 99 if changed == "capture_version" else "f" * 64})
            )
        assert not replay._commit_binding(factory, work, binding)
        assert progress(factory) == before


def test_duplicate_binding_worker_cannot_republish_the_same_version(tmp_path):
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        source_work = ready(factory)
        work = rank._begin(factory, source_work)
        binding = replay._binding(source_work, owner.admission_provenance_catalog())
        assert replay._commit_binding(factory, work, binding)
        before = progress(factory)
        assert not replay._commit_binding(factory, work, binding)
        assert progress(factory) == before


@pytest.mark.parametrize("committed", [False, True])
def test_process_death_retains_only_committed_replay_binding(tmp_path, committed):
    with retained(tmp_path, source(journal_id=JOURNAL)) as (engine, factory):
        source_work = ready(factory)
        before = progress(factory)
        code = """import os,sys
from sqlalchemy import create_engine,event
from sqlalchemy.orm import sessionmaker
from riverhog_core.services import collection_uploads as owner
from riverhog_core.services import upload_journal_rank as rank
from riverhog_core.services import upload_journal_replay as replay
engine=create_engine(sys.argv[1])
factory=sessionmaker(engine)
source=owner._read_upload_journal_validation(factory,1,sys.argv[2])
work=rank._begin(factory,source)
binding=replay._binding(source,owner.admission_provenance_catalog())
if sys.argv[3]=='before':
    event.listen(engine,'commit',lambda _connection: os._exit(73))
assert replay._commit_binding(factory,work,binding)
os._exit(73)
"""
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                code,
                str(engine.url),
                JOURNAL,
                "after" if committed else "before",
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 73, result.stderr
        engine.dispose()
        if committed:
            assert progress(factory)[0] == before[0] + 1 and progress(factory)[1] is not None
        else:
            assert progress(factory) == before
        assert replay.prepare_upload_journal_replay(
            factory, source_work, catalog=owner.admission_provenance_catalog()
        )
        assert terminal(factory)["anchor"] is None


def test_native_handoff_verifies_binding_after_reader_release(tmp_path, monkeypatch):
    raw = source(journal_id=JOURNAL)
    with retained(tmp_path, raw) as (engine, factory):
        ready(factory)
        service = service_for(factory)
        native = service._journal_validation_owner.advance
        observed = []

        def detached(*args, **kwargs):
            assert progress(factory)[1] is not None
            assert engine.pool.checkedout() == 0 and terminal(factory)["anchor"] is None
            observed.append(True)
            return native(*args, **kwargs)

        monkeypatch.setattr(service._journal_validation_owner, "advance", detached)
        try:
            for _ in range(10000):
                service._advance_provenance_journal_validation(1, JOURNAL)
                if terminal(factory)["state"] == "sealed":
                    break
            else:
                pytest.fail("small native handoff did not complete")
            assert len(observed) > 1
            with validate_journal(raw) as expected:
                assert terminal(factory)["anchor"] == expected.anchor
        finally:
            service.close()


def test_unprepared_current_source_differs_from_a_stale_source(tmp_path):
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        work = complete_capture(factory)
        assert replay._probe(factory, work) == (True, None)
        with factory() as session, session.begin():
            session.execute(update(Journal).values(capture_version=work.version + 1))
        assert replay._probe(factory, work) == (False, None)


@pytest.mark.growth_witness(
    "riverhog-journal-replay-binding-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_replay._probe",
)
def test_actual_replay_binding_metadata_probe_work(tmp_path, monkeypatch, record_property):
    binding = growth_binding(replay._probe)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    samples = []
    for count, position, bound in [
        (1, 0, False),
        (1, 0, True),
        (17, 0, True),
        (129, 128, True),
        (1025, 0, True),
        (1025, 512, True),
        (1025, 1024, True),
        (1025, 1024, False),
    ]:
        with retained(tmp_path / f"{count}-{position}-{bound}", source(journal_id=JOURNAL)) as (
            engine,
            factory,
        ):
            source_work = ready(factory)
            identities = sorted([JOURNAL, *(uid(200000 + n) for n in range(count - 1))])
            selected = identities[position]
            source_work = replace(source_work, journal_id=selected)
            raw = (
                replay._binding(source_work, owner.admission_provenance_catalog()).to_json()
                if bound
                else None
            )
            with factory() as session, session.begin():
                journal = session.get(Journal, (1, JOURNAL))
                scan = session.get(Scan, (1, JOURNAL))
                journal_values = {
                    column.key: getattr(journal, column.key) for column in Journal.__table__.columns
                }
                scan_values = {
                    column.key: getattr(scan, column.key) for column in Scan.__table__.columns
                }
                pending_scans = []
                for identity in identities:
                    if identity != JOURNAL:
                        session.add(Journal(**{**journal_values, "journal_id": identity}))
                        pending_scans.append(
                            Scan(
                                **{
                                    **scan_values,
                                    "journal_id": identity,
                                    "replay_binding_json": raw if identity == selected else None,
                                }
                            )
                        )
                    elif identity == selected:
                        scan.replay_binding_json = raw
                session.flush()
                session.add_all(pending_scans)
            observed = {"returned_positions": 0, "closed": False}
            snapshot_reader = replay.read_snapshot

            class ObservedProbe(ObservedWindow):
                def one_or_none(self):
                    row = self.rows.one_or_none()
                    self.counts["returned_positions"] += int(row is not None)
                    return row

            @contextmanager
            def observed_snapshot(factory, base=snapshot_reader, counts=observed):
                with base(factory) as session:
                    execute = session.execute

                    def wrapped(*args, **kwargs):
                        return ObservedProbe(execute(*args, **kwargs), counts)

                    monkeypatch.setattr(session, "execute", wrapped)
                    yield session

            with engine.connect() as connection:
                native = connection.connection.driver_connection
                assert isinstance(native, sqlite3.Connection)
            statements, instructions, payload_reads = [], [0], []

            def sql(_conn, _cursor, statement, parameters, _context, _many, output=statements):
                output.append((statement, parameters))

            def tick(counter=instructions):
                counter[0] += 1
                return 0

            def authorize(action, table, column, _database, _trigger, reads=payload_reads):
                if action == sqlite3.SQLITE_READ and column in {
                    "content",
                    "cursor_json",
                    "union_cursor_json",
                    "rank_cursor_json",
                }:
                    reads.append((table, column))
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            event.listen(engine, "before_cursor_execute", sql)
            native.set_authorizer(authorize)
            native.set_progress_handler(tick, 1)
            try:
                with monkeypatch.context() as patch:
                    patch.setattr(replay, "read_snapshot", observed_snapshot)
                    assert replay._probe(factory, source_work) == (True, raw)
            finally:
                native.set_progress_handler(None, 0)
                native.set_authorizer(None)
                event.remove(engine, "before_cursor_execute", sql)
            assert observed["returned_positions"] == 1 and observed["closed"]
            assert not payload_reads and engine.pool.checkedout() == 0
            for statement, parameters in statements:
                with engine.connect() as connection:
                    result = connection.exec_driver_sql(
                        "EXPLAIN QUERY PLAN " + statement, parameters
                    )
                    try:
                        plan = " ".join(row[3] for row in result)
                    finally:
                        result.close()
                assert "SEARCH" in plan and "SCAN" not in plan and "USE TEMP" not in plan
            sample = {
                "declaration": binding.declaration.id,
                "declaration_sha256": digest,
                "dimensions": {"retained_journals": count},
                "metrics": {
                    "sql_statements": len(statements),
                    "returned_rows": observed["returned_positions"],
                    "source_payload_octets": 0,
                    "binding_octets": len(raw.encode()) if raw is not None else 0,
                    "sqlite_vm_instructions": instructions[0],
                },
                "delegates": {},
            }
            validate_execution_samples(projection, binding.declaration.id, [sample])
            samples.append(sample)
    record_property("growth_samples", samples)
