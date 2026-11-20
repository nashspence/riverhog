from __future__ import annotations

import gc
import hashlib
import shutil
from pathlib import Path

import pytest
from riverhog_client import CompletionRecords
from riverhog_client import completion_records as owner

from tests.support.sqlite_admission import Capacity, Population


@pytest.fixture
def roots(tmp_path, monkeypatch):
    make, paths = owner.mkdtemp, []

    def created(**options):
        path = Path(make(dir=tmp_path, **options))
        paths.append(path)
        return str(path)

    monkeypatch.setattr(owner, "mkdtemp", created)
    return paths


def test_completion_spool_admits_before_schema_and_preserves_exact_preimages(roots):
    population, handles = Population(limit=1), []

    def retained(records):
        handles.append(records)
        assert population.active == 1
        assert (
            not population.connections[0]
            .execute("SELECT name FROM sqlite_master WHERE type = 'table'")
            .fetchall()
        )

    raw = b'{"optional":null,"quality":1.2300}' + b"\n" * 150000
    with CompletionRecords(connection_factory=population, on_storage_open=retained) as records:
        assert population.attempts == [(roots[0] / "records.sqlite3", {})]
        assert population.connections[0].execute("PRAGMA cache_size").fetchone() == (-512,)
        record = records.add("target-execution", (raw[:300], raw[300:]))
        assert (record.bytes, record.sha256) == (len(raw), hashlib.sha256(raw).hexdigest())
        chunks = list(record.read())
        assert b"".join(chunks) == raw and max(map(len, chunks)) <= 128 * 1024
        with pytest.raises(ValueError, match="duplicate"):
            records.add("target-execution", (b"replacement",))
        assert b"".join(records.record("target-execution").read()) == raw
        for key in ("b", "a"):
            records.add_output(
                {"artifact_id": key, "output_id": key, "state": "same"}, {"key": key}
            )
        assert records.output_count == 2
        assert [r["artifact_id"] for r in records.output_rows()] == ["a", "b"]
        assert list(records.output_rows(imports=True)) == [{"key": "a"}, {"key": "b"}]
        assert records.output_for_key("a")["state"] == records.output_for_key("b")["state"]
        with pytest.raises(ValueError, match="duplicate"):
            records.add_output({"artifact_id": "c", "output_id": "a"}, {})
    assert handles == [records] and population.active == 0 and population.peak == 1
    assert population.connections[0].close_calls == 1 and not roots[0].exists()
    records.close()
    assert population.connections[0].close_calls == 1


def test_completion_spool_refusal_preserves_factory_custody_before_schema(roots):
    population, handles = Population(limit=1), []
    population.refuse = True
    with pytest.raises(Capacity):
        CompletionRecords(connection_factory=population, on_storage_open=handles.append)
    gc.collect()
    assert not population.connections and not handles and population.active == 0
    assert roots[0].is_dir() and not list(roots[0].iterdir())
    shutil.rmtree(roots[0])


def test_completion_spool_unreturned_connection_and_files_stay_factory_owned(roots):
    population, handles = Population(limit=1), []
    population.unreturned = True
    with pytest.raises(OSError, match="construction unconfirmed"):
        CompletionRecords(connection_factory=population, on_storage_open=handles.append)
    gc.collect()
    assert population.active == 1 and population.connections[0].close_calls == 0
    assert not handles and (roots[0] / "records.sqlite3").is_file()
    population.connections[0].close()
    shutil.rmtree(roots[0])
    assert population.active == 0


def test_completion_spool_default_connection_failure_cleans_its_directory(roots, monkeypatch):
    def fail(_path):
        raise OSError("default connect interrupted")

    monkeypatch.setattr(owner.sqlite3, "connect", fail)
    with pytest.raises(OSError, match="default connect interrupted"):
        CompletionRecords()
    assert len(roots) == 1 and not roots[0].exists()


@pytest.mark.parametrize("stage", ["schema", "retention"])
def test_completion_spool_initialization_failure_releases_confirmed_storage(roots, stage):
    population, handles = Population(limit=1), []
    population.fail_schema = stage == "schema"

    def retained(records):
        handles.append(records)
        if stage == "retention":
            raise InterruptedError("early retention interrupted")

    with pytest.raises(InterruptedError):
        CompletionRecords(connection_factory=population, on_storage_open=retained)
    assert len(handles) == 1 and population.active == 0 and not roots[0].exists()
    handles[0].close()
    assert population.connections[0].close_calls == 1


@pytest.mark.parametrize("stage", ["schema", "retention"])
@pytest.mark.parametrize("after", [False, True])
def test_completion_spool_failed_initialization_retains_uncertain_close_for_retry(
    roots, stage, after
):
    population, handles = Population(limit=1), []
    population.fail_schema = stage == "schema"

    def retained(records):
        handles.append(records)
        population.connections[0].close_failures = 1
        population.connections[0].close_after = after
        if stage == "retention":
            raise InterruptedError("early retention interrupted")

    with pytest.raises(OSError, match="close unconfirmed") as failed:
        CompletionRecords(connection_factory=population, on_storage_open=retained)
    assert isinstance(failed.value.__context__, InterruptedError)
    gc.collect()
    assert population.active == 1 and population.connections[0].close_calls == 1
    assert roots[0].is_dir() and len(handles) == 1
    handles[0].close()
    assert population.active == 0 and population.connections[0].close_calls == 2
    assert not roots[0].exists()


@pytest.mark.parametrize("after", [False, True])
def test_completion_spool_context_exit_does_not_retry_an_uncertain_explicit_close(roots, after):
    population, handles = Population(limit=1), []
    with pytest.raises(OSError, match="close unconfirmed"):
        with CompletionRecords(
            connection_factory=population, on_storage_open=handles.append
        ) as records:
            record = records.add("target-execution", (b"exact",))
            population.connections[0].close_failures = 1
            population.connections[0].close_after = after
            records.close()
    gc.collect()
    assert population.active == 1 and population.connections[0].close_calls == 1
    assert roots[0].is_dir()
    for call in (
        lambda: records.add("later", (b"later",)),
        lambda: records.add_output({"artifact_id": "a", "output_id": "a"}, {}),
        lambda: records.output_count,
        lambda: records.output_for_key("a"),
        lambda: next(records.output_rows()),
        lambda: records.record("target-execution"),
        lambda: next(records.records()),
        lambda: next(record.read()),
        records.__enter__,
    ):
        with pytest.raises(RuntimeError, match="closed"):
            call()
    assert population.connections[0].close_calls == 1
    handles[0].close()
    assert population.active == 0 and not roots[0].exists()


@pytest.mark.parametrize("after", [False, True])
def test_completion_spool_cleanup_retry_does_not_release_connection_twice(
    roots, monkeypatch, after
):
    population = Population(limit=1)
    records = CompletionRecords(connection_factory=population)
    original, attempts = owner.shutil.rmtree, []

    def interrupted(path):
        attempts.append(path)
        if len(attempts) == 1:
            if after:
                original(path)
            raise OSError("directory cleanup unconfirmed")
        original(path)

    monkeypatch.setattr(owner.shutil, "rmtree", interrupted)
    with pytest.raises(OSError, match="directory cleanup unconfirmed"):
        records.close()
    assert population.active == 0 and population.connections[0].close_calls == 1
    assert roots[0].exists() is not after
    records.close()
    assert population.connections[0].close_calls == 1 and not roots[0].exists()
    records.close()
    assert len(attempts) == 2


@pytest.mark.parametrize("iterator_kind", ["records", "outputs", "bytes"])
@pytest.mark.parametrize("after", [False, True])
def test_completion_spool_suspended_read_stops_before_another_sql_step(roots, iterator_kind, after):
    population = Population(limit=1)
    records = CompletionRecords(connection_factory=population)
    record = records.add("a", (b"x" * (256 * 1024),))
    records.add("b", (b"second",))
    for key in ("a", "b"):
        records.add_output({"artifact_id": key, "output_id": key}, {})
    ticks = []
    population.connections[0].set_progress_handler(lambda: ticks.append(1) or 0, 1)
    iterator = {
        "records": records.records,
        "outputs": records.output_rows,
        "bytes": record.read,
    }[iterator_kind]()
    next(iterator)
    population.connections[0].close_failures = 1
    population.connections[0].close_after = after
    with pytest.raises(OSError, match="close unconfirmed"):
        records.close()
    previous = len(ticks)
    with pytest.raises(RuntimeError, match="closed"):
        next(iterator)
    assert len(ticks) == previous and population.connections[0].close_calls == 1
    records.close()
    assert population.active == 0 and not roots[0].exists()


@pytest.mark.parametrize("error_type", [OSError, KeyboardInterrupt])
def test_completion_spool_interrupted_record_retries_without_replacing_preimage(roots, error_type):
    population = Population(limit=1)

    def interrupted():
        yield b"partial"
        raise error_type("input interrupted")

    with CompletionRecords(connection_factory=population) as records:
        with pytest.raises(error_type, match="input interrupted"):
            records.add("target-execution", interrupted())
        retained = records.add("target-execution", (b"complete",))
        assert b"".join(retained.read()) == b"complete"
    assert population.active == 0 and not roots[0].exists()


@pytest.mark.parametrize("after", [False, True])
def test_completion_spool_input_callback_cannot_insert_after_close_begins(roots, after):
    population = Population(limit=1)
    records = CompletionRecords(connection_factory=population)
    population.connections[0].close_failures = 1
    population.connections[0].close_after = after

    def interrupted():
        yield b"uncommitted"
        with pytest.raises(OSError, match="close unconfirmed"):
            records.close()

    with pytest.raises(RuntimeError, match="closed"):
        records.add("target-execution", interrupted())
    assert population.active == 1 and population.connections[0].close_calls == 1
    if not after:
        assert population.connections[0].execute("SELECT count(*) FROM records").fetchone() == (0,)
    records.close()
    assert population.active == 0 and not roots[0].exists()
