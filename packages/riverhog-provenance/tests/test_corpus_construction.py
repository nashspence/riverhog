"""Actual POSIX corpus guards retain original acquisition and close obligations."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from contextlib import closing
from pathlib import Path
from threading import Event, Thread

import pytest
import retained_resource
import sqlite_admission
from riverhog_provenance import CorpusConstruction, RetainedCorpusValidator
from riverhog_provenance import corpus_workspace as owner
from sqlite_admission import SqliteCapacity, SqliteConnectionAdmission
from test_retained_corpus_faults import EmptySource

from tests.support.retained_resource import fault_native as fault_native

pytestmark = pytest.mark.skipif(os.name != "posix", reason="retained corpus has POSIX custody")


def pool(*, connections=4, verify=lambda: None):
    return SqliteConnectionAdmission(
        connection_limit=connections, cursor_limit=128, operation_limit=4, verify_process=verify
    )


def frame(admission):
    return CorpusConstruction(admission.connection_scope(), resource_limit=3)


@pytest.fixture(autouse=True)
def native(monkeypatch, fault_native):
    module, _path = fault_native
    monkeypatch.setattr(retained_resource, "Resource", module.Resource)
    monkeypatch.setattr(retained_resource, "Borrow", module.Borrow)
    return module


@pytest.fixture
def directory(tmp_path):
    path = tmp_path / "corpus"
    path.mkdir(mode=0o700)
    return path


def open_corpus(directory, catalog, original, **options):
    return RetainedCorpusValidator.open_retained(
        directory,
        EmptySource(catalog),
        construction=original,
        worker_epoch=0,
        create=True,
        **options,
    )


def test_passive_records_and_missing_frame_refuse_before_effects(directory, catalog):
    calls = []
    admission = pool(verify=lambda: calls.append("verify"))
    original = frame(admission)
    assert not calls and admission.status.connections == 0
    assert original.files.retained == 3 and not original.borrow.active
    assert original.guard.metrics["open_calls"] == original.directory.metrics["open_calls"] == 0
    with pytest.raises(TypeError):
        RetainedCorpusValidator.open_retained(
            directory,
            EmptySource(catalog),
            worker_epoch=0,
        )
    with pytest.raises(TypeError, match="original construction"):
        open_corpus(directory, catalog, object())
    assert not calls and not list(directory.iterdir()) and original.close_step()


@pytest.mark.parametrize("limit", [0, 1, 2, True, 3.0])
def test_explicit_file_grants_precede_process_and_files(directory, limit):
    calls = []
    admission = pool(verify=lambda: calls.append(True))
    with pytest.raises(ValueError):
        CorpusConstruction(admission.connection_scope(), resource_limit=limit)
    assert not calls and not list(directory.iterdir())


def test_shared_reservation_refuses_before_guard_and_same_frame_receives(directory, catalog):
    admission = pool(connections=1)
    occupied = admission.connect(":memory:")
    original = frame(admission)
    try:
        with pytest.raises(SqliteCapacity):
            open_corpus(directory, catalog, original)
        assert not list(directory.iterdir()) and original.guard.state == "new"
        assert original.close_step() and admission.status.connections == 1
    finally:
        occupied.close()


@pytest.mark.parametrize("fault", ["after_open", "signal_after_open", "stat_refusal"])
def test_lost_guard_reply_keeps_exact_resource_and_prevents_callback(directory, catalog, fault):
    admission = pool()
    original = frame(admission)
    original.guard._test_fault(fault)
    opened, released = [], []
    with pytest.raises((RuntimeError, KeyboardInterrupt, OSError)):
        open_corpus(
            directory,
            catalog,
            original,
            on_storage_open=opened.append,
            on_storage_close=lambda: released.append(True),
        )
    assert original.storage is not None and not opened and not released
    assert original.guard.state in {"live", "rejected"}
    assert original.guard.metrics["open_calls"] == 1
    assert not original.received and not original.borrow.active
    assert original.close_step() and original.received
    assert original.guard.metrics["close_calls"] == 1 and not released


@pytest.mark.parametrize("fault", ["after_open", "before_sync", "after_sync", "before_close"])
def test_sync_failure_does_not_close_long_lived_guard_or_transfer_parent(directory, catalog, fault):
    admission = pool()
    original = frame(admission)
    original.directory._test_fault(fault)
    released = []
    with pytest.raises((RuntimeError, OSError)):
        open_corpus(directory, catalog, original, on_storage_close=lambda: released.append(True))
    assert original.guard.state == "live" and original.borrow.active
    assert not released and not original.received
    assert original.close_step() and not released and admission.status.connections == 0
    assert original.guard.metrics["close_calls"] == original.directory.metrics["close_calls"] == 1


@pytest.mark.parametrize("phase", ["connection_reply", "wrapper", "callback", "schema"])
def test_partial_construction_has_exact_sqlite_receiver_even_without_returned_corpus(
    directory, catalog, monkeypatch, phase
):
    admission = pool()
    original = frame(admission)
    independent = admission.connect(":memory:")
    options = {}
    driver = sqlite_admission._connect

    def lose(*args, **kwargs):
        driver(*args, **kwargs)
        raise InterruptedError("original connection return lost")

    def fail(*args, **kwargs):
        assert original.storage is not None
        raise MemoryError("original wrapper or initialization failed")

    try:
        with monkeypatch.context() as patch:
            if phase == "connection_reply":
                patch.setattr(sqlite_admission, "_connect", lose)
            elif phase == "wrapper":
                patch.setattr(owner.CanonicalCorpusValidator, "_from_workspace", fail)
            elif phase == "callback":
                options["on_storage_open"] = fail
            else:
                patch.setattr(RetainedCorpusValidator, "_initialize", fail)
            with pytest.raises((InterruptedError, MemoryError)):
                open_corpus(directory, catalog, original, **options)
        monkeypatch.setattr(
            type(admission),
            "pending_connections",
            property(lambda self: pytest.fail("shared admission scan is not an exact receipt")),
        )
        assert original.storage is not None and original.guard.state == "live"
        assert original.borrow.active and not original.received
        assert original.close_step() and admission.status.connections == 1
        with closing(independent.execute("SELECT 17")) as rows:
            assert rows.fetchone() == (17,)
    finally:
        independent.close()


def test_database_then_borrow_guard_then_callback_and_retry_same_callback(directory, catalog):
    admission = pool()
    original = frame(admission)
    calls = []

    def release():
        assert original.scope.received and original.borrow.received and original.guard.received
        assert original.dependents_received and not native.storage_received
        calls.append("release")
        if len(calls) == 1:
            raise InterruptedError("parent release reply lost")

    native = open_corpus(directory, catalog, original, on_storage_close=release)
    assert original.directory.received and original.guard.state == "live"
    assert original.borrow.active and admission.status.connections == 1
    with pytest.raises(InterruptedError, match="parent release"):
        native.close()
    assert not original.received and not native.storage_received
    with pytest.raises(RuntimeError, match="closed"):
        native.advance_semantics()
    native.close()
    native.close()
    assert calls == ["release", "release"] and native.storage_received and original.received
    assert original.guard.metrics["close_calls"] == original.directory.metrics["close_calls"] == 1


@pytest.mark.parametrize("phase", ["constructor", "transaction"])
def test_original_execution_gate_covers_source_check_and_actual_join(directory, catalog, phase):
    admission = pool()
    original = frame(admission)
    entered, finish = Event(), Event()
    source = EmptySource(catalog)
    current = source.verify_current
    calls, errors = [], []

    def held():
        entered.set()
        assert finish.wait(10)
        return current()

    if phase == "constructor":
        source.verify_current = held

        def execute():
            calls.append(
                RetainedCorpusValidator.open_retained(
                    directory, source, construction=original, worker_epoch=0, create=True
                )
            )
    else:
        native = RetainedCorpusValidator.open_retained(
            directory, source, construction=original, worker_epoch=0, create=True
        )
        source.verify_current = held

        def execute():
            calls.append(native.advance_semantics())

    def run():
        try:
            execute()
        except BaseException as error:
            errors.append(error)

    worker = Thread(target=run)
    worker.start()
    other = frame(admission)
    other_directory = directory.parent / "independent"
    other_directory.mkdir(mode=0o700)
    try:
        assert entered.wait(10)
        with pytest.raises(BlockingIOError):
            original.close_step()
        assert not original.received and original.guard.metrics["close_calls"] == 0
        independent = open_corpus(other_directory, catalog, other)
        assert independent.advance_semantics().complete
        independent.close()
        assert worker.is_alive()
    finally:
        finish.set()
        worker.join(10)
    assert not worker.is_alive() and not errors and len(calls) == 1
    assert original.close_step() and admission.status.connections == 0


@pytest.mark.parametrize("fault", ["unknown_close_open", "unknown_close_closed"])
def test_unknown_guard_close_never_retries_integer_or_releases_parent(
    tmp_path, catalog, fault_native, fault
):
    _native, extension = fault_native
    code = r"""
import importlib.util,json,sys
from pathlib import Path
import retained_resource
from riverhog_provenance import CorpusConstruction,RetainedCorpusValidator
from riverhog_provenance_contracts import ContractCatalog
from sqlite_admission import SqliteConnectionAdmission
sys.path.insert(0,sys.argv[3])
from test_retained_corpus_faults import EmptySource
spec=importlib.util.spec_from_file_location("_retained_resource_test._native",sys.argv[2])
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
retained_resource.Resource,retained_resource.Borrow=module.Resource,module.Borrow
root=Path(sys.argv[1]);catalog=ContractCatalog();released=[]
admission=SqliteConnectionAdmission(connection_limit=2,cursor_limit=128,operation_limit=4,verify_process=lambda:None)
original=CorpusConstruction(admission.connection_scope(),resource_limit=3)
root.mkdir(mode=0o700)
native=RetainedCorpusValidator.open_retained(root,EmptySource(catalog),construction=original,worker_epoch=0,create=True,on_storage_close=lambda:released.append(True))
original.guard._test_fault(sys.argv[4])
try:native.close()
except OSError:pass
else:raise AssertionError("unknown close accepted")
assert original.guard.state=="unknown" and original.scope.received and original.borrow.received
count=original.guard.metrics["close_calls"]
for _ in range(17):
 try:original.close_step()
 except RuntimeError:pass
 else:raise AssertionError("unknown original received")
assert original.guard.metrics["close_calls"]==count and not released and not original.received
other=CorpusConstruction(admission.connection_scope(),resource_limit=3)
path=root.parent/"independent";path.mkdir(mode=0o700)
independent=RetainedCorpusValidator.open_retained(path,EmptySource(catalog),construction=other,worker_epoch=0,create=True)
assert independent.advance_semantics().complete
independent.close()
assert other.received and not released and not original.received
print(json.dumps({"unknown":True,"close_calls":count,"retries":17,"independent":True}))
"""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            code,
            str(tmp_path / "unknown"),
            str(extension),
            str(Path(__file__).parent),
            fault,
        ],
        check=True,
        text=True,
        capture_output=True,
        timeout=60,
    )
    receipt = json.loads(result.stdout)
    assert receipt == {
        "unknown": True,
        "close_calls": 0 if fault == "unknown_close_open" else 1,
        "retries": 17,
        "independent": True,
    }
