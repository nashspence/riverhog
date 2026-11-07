from __future__ import annotations

import errno
import hashlib
import os
import sqlite3
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path

import pytest
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import JournalValidationSession, parse_journal
from riverhog_provenance.errors import IncompleteSourceError
from riverhog_provenance.journal_store import InvalidJournalWorkspace
from test_child_admission_growth import octets
from test_journal_session_growth import ATTEMPT, FiniteSource
from test_journal_session_lifecycle_growth import (
    LifecycleRecorder,
    lifecycle_dimensions,
    observe_replay,
    observed_work,
)

OPEN = JournalValidationSession.open_retained.__func__


@contextmanager
def storage_fault(kind):
    with pytest.MonkeyPatch.context() as patch:
        if kind in {"space", "quota"}:
            actual = os.open

            def open_file(path, *args, **kwargs):
                if Path(path).name == "snapshot.sqlite3":
                    raise OSError(
                        errno.ENOSPC if kind == "space" else errno.EDQUOT,
                        ("selected native admission capacity"),
                    )
                return actual(path, *args, **kwargs)

            patch.setattr(os, "open", open_file)
        elif kind == "connect":

            def connect(*_args, **_kwargs):
                raise sqlite3.OperationalError("selected native connection unavailable")

            patch.setattr(sqlite3, "connect", connect)
        elif kind == "sql":
            import test_journal_session_lifecycle_growth as lifecycle

            actual = lifecycle.LifecycleCursor

            class Cursor(actual):
                def execute(self, sql, parameters=()):
                    if sql == "PRAGMA auto_vacuum=INCREMENTAL":
                        raise sqlite3.OperationalError("selected native statement capacity")
                    return super().execute(sql, parameters)

            patch.setattr(lifecycle, "LifecycleCursor", Cursor)
        yield


def measure(directory, source, catalog, kind):
    callbacks = []
    recorder = LifecycleRecorder()
    admissions = int(kind in {"corrupt", "format", "epoch", "epoch-text", "sql"})
    checks = int(kind not in {"catalog", "negative-epoch"})
    dimensions = lifecycle_dimensions(
        OPEN, metadata=32768 + 2 * octets(source.binding.to_json()), initializing=False
    )
    dimensions.update(
        connection_admissions=admissions, rejected_connections=admissions, source_checks=checks
    )
    for tag, _child in growth_binding(OPEN).delegates:
        dimensions[tag + "_calls"] = 0
    kwargs = {
        "attempt_id": ATTEMPT,
        "worker_epoch": -1 if kind == "negative-epoch" else 2,
        "create": kind in {"space", "quota", "sql"},
        "catalog": catalog,
        "on_storage_close": recorder.callback(lambda: callbacks.append(1)),
    }
    failure = None
    with storage_fault(kind), recorder.kernel():
        with observe_replay(recorder, catalog, dimensions, root_operation=OPEN) as (
            frame,
            complete,
        ):
            recorder.recording = True
            try:
                JournalValidationSession.open_retained(directory, recorder.source(source), **kwargs)
            except BaseException as error:
                failure = error
            finally:
                recorder.recording = False
        assert failure is not None
        primitive = observed_work(recorder, {name: 0 for name in recorder.metrics}, 0, 0)
        primitive.update(recorder.attempts)
        primitive.update(
            opened_database_limit=primitive["opened_databases"],
            closed_database_limit=primitive["closed_databases"],
            source_check_limit=primitive["source_verifications"],
            transition_call_limit=sum(
                len(frame["delegates"][tag]) for tag in ("initialize", "replacement")
            ),
            lifecycle_call_limit=sum(map(len, frame["delegates"].values())),
        )
        sample = complete(frame, primitive)
        validate_execution_samples(
            project_bindings((growth_binding(OPEN),)), growth_binding(OPEN).declaration.id, [sample]
        )
    expected = (
        IncompleteSourceError
        if kind == "source"
        else ValueError
        if kind in {"catalog", "negative-epoch", "epoch"}
        else RuntimeError
        if kind == "epoch-text"
        else sqlite3.DatabaseError
        if kind in {"connect", "sql"}
        else OSError
        if kind in {"space", "quota"}
        else InvalidJournalWorkspace
    )
    assert isinstance(failure, expected)
    assert callbacks == []
    assert source.calls == ([("verify",)] if checks else [])
    assert (
        sample["metrics"]["opened_databases"] == sample["metrics"]["closed_databases"] == admissions
    )
    assert all(connection.closed for connection in recorder.connections)
    assert sample["metrics"]["source_payload_octets"] == 0
    assert all(not children for children in sample["delegates"].values())
    if kind in {"space", "quota"}:
        assert failure.errno == (errno.ENOSPC if kind == "space" else errno.EDQUOT)
        assert sample["metrics"]["file_open_attempts"] == 1
        assert sample["metrics"]["database_open_attempts"] == 0
    if kind == "connect":
        assert sample["metrics"]["database_open_attempts"] == 1
    if kind == "sql":
        assert sample["metrics"]["sql_attempts"] == 1
    return sample


def admission_samples(root, catalog, journal, *, samples, large=True):
    frame = parse_journal(journal)[0]
    widths = (64, 8192) if large else (64,)
    kinds = (
        "source",
        "catalog",
        "negative-epoch",
        "directory",
        "missing",
        "symlink",
        "corrupt",
        "format",
        "epoch",
        "epoch-text",
        "space",
        "quota",
        "connect",
        "sql",
    )
    for width in widths:
        for kind in kinds:
            directory = root / f"{width}-{kind}"
            if kind != "directory":
                directory.mkdir()
            source = FiniteSource(journal, frame, 1, catalog, directory_width=width)
            path = directory / "snapshot.sqlite3"
            if kind in {"format", "epoch", "epoch-text", "connect"}:
                current = JournalValidationSession.open_retained(
                    directory,
                    source,
                    attempt_id=ATTEMPT,
                    worker_epoch=2,
                    create=True,
                    catalog=catalog,
                )
                current.close()
                if kind in {"format", "epoch-text"}:
                    with sqlite3.connect(path) as db:
                        db.execute(
                            "UPDATE journal_workspace SET format='incompatible'"
                            if kind == "format"
                            else ("UPDATE journal_validation SET worker_epoch='invalid'")
                        ).close()
            elif kind == "corrupt":
                path.write_bytes(b"not an owner native SQLite database")
            elif kind == "symlink":
                target = root / f"{width}-external.sqlite3"
                target.write_bytes(b"exact external bytes")
                path.symlink_to(target)
            if kind == "source":
                source.current = False
            elif kind == "catalog":
                source.binding = replace(source.binding, catalog_sha256="0" * 64)
            source.calls.clear()
            before = None if not path.is_file() else hashlib.sha256(path.read_bytes()).hexdigest()
            sample = measure(directory, source, catalog, kind)
            samples.append(sample)
            if before is not None:
                assert hashlib.sha256(path.read_bytes()).hexdigest() == before
            elif kind not in {"sql"}:
                assert not path.exists()


@pytest.mark.growth_witness(
    "riverhog-native-session-storage-admission-rejection/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.journal_session.JournalValidationSession.open_retained",
)
def test_actual_native_input_and_storage_admission_rejections(
    tmp_path, catalog, journal, record_property
):
    samples = []
    record_property("growth_samples", samples)
    admission_samples(tmp_path, catalog, journal, samples=samples)
