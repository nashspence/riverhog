"""Physical fact-work fixtures and detached read accounting for compiler tests."""

from __future__ import annotations

import hashlib
import sqlite3
from contextlib import contextmanager

from riverhog_core.canonical_discovery_fact_work import (
    FactTaskKey,
    begin_assertion,
    fact_extraction_cursor,
    initialize_fact_work,
)
from riverhog_core.canonical_discovery_source_work import (
    AdmittedAssertionSource,
    apply_source_page,
    prepare_lexical_source_page,
)

SOURCE = hashlib.sha256(b"qualified admitted exact source").hexdigest()


ORIGIN = hashlib.sha256(b"verified entry preimage").hexdigest()


KEY = FactTaskKey("build", "assertion", SOURCE, "worker")


@contextmanager
def write(db):
    db.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        db.rollback()
        raise
    else:
        db.commit()


def source_for(row, origin=ORIGIN):
    return AdmittedAssertionSource(SOURCE, origin, row)


def open_work(path, source):
    db = sqlite3.connect(path, isolation_level=None)
    initialize_fact_work(db)
    with write(db):
        begin_assertion(
            db,
            KEY,
            assertion_id=source.assertion_id,
            origin_sha256=source.origin_sha256,
            canonical_stream_ref=source.canonical_sha256,
        )
    return db


def streams(db, kind):
    result = {}
    for key, fragments, octets, complete in db.execute(
        "SELECT record_key,fragments,octets,complete FROM discovery_fact_streams "
        "WHERE build_id=? AND task_id=? AND kind=?",
        (KEY.build_id, KEY.task_id, kind),
    ).fetchall():
        parts = db.execute(
            "SELECT ordinal,exact_bytes FROM discovery_fact_segments "
            "WHERE build_id=? AND task_id=? AND kind=? AND record_key=? ORDER BY ordinal",
            (KEY.build_id, KEY.task_id, kind, key),
        ).fetchall()
        assert [i for i, _ in parts] == list(range(fragments))
        value = b"".join(part for _, part in parts)
        assert len(value) == octets
        assert complete
        result[int(key)] = value
    return result


def drive(db, source, budget, on_page=None):
    pages = 0
    while fact_extraction_cursor(db, KEY)[1].category in {"canonical", "scalars"}:
        page = prepare_lexical_source_page(db, KEY, source, action_budget=budget)
        assert 1 <= len(page.actions) <= budget
        assert sum(len(a.segment.exact_bytes) for a in page.actions if a.segment) <= budget * 4096
        with write(db):
            progress = apply_source_page(db, KEY, page)
        assert progress.version == page.expected_version + len(page.actions)
        pages += 1
        if on_page is not None:
            on_page(pages, page)
    return pages


def validated_source(fixture):
    from riverhog_provenance import validate_journal

    with validate_journal(fixture.raw, catalog=fixture.catalog, require_profiles=True) as summary:
        for sequence, frame in enumerate(summary.frames):
            for rows in frame.document["body"].get("assertions", {}).values():
                for row in rows:
                    if row["assertion_id"] == fixture.assertion_id:
                        origin = summary.anchor_at(sequence)["through"]["json_sha256"]
                        return source_for(row, origin)
    raise AssertionError("native fixture omitted its exact assertion")


class RecordedCursor:
    def __init__(self, actual, recorder):
        self.actual, self.recorder = actual, recorder

    def fetchone(self):
        row = self.actual.fetchone()
        self.recorder.metrics["returned_rows"] += int(row is not None)
        return row

    @property
    def rowcount(self):
        return self.actual.rowcount

    def close(self):
        self.actual.close()


class RecordedConnection:
    def __init__(self, actual):
        self.actual = actual
        self.metrics = {
            "sql_statements": 0,
            "returned_rows": 0,
            "prepared_actions": 0,
            "prepared_fragment_octets": 0,
            "hashed_octets": 0,
        }

    @property
    def in_transaction(self):
        return self.actual.in_transaction

    def execute(self, sql, parameters=()):
        self.metrics["sql_statements"] += 1
        return RecordedCursor(self.actual.execute(sql, parameters), self)

    def commit(self):
        self.actual.commit()

    def rollback(self):
        self.actual.rollback()


ADMISSION = hashlib.sha256(b"verified archive admission").hexdigest()


def open_journal_work(path):
    from riverhog_core.canonical_discovery_journal_work import initialize_journal_work

    db = sqlite3.connect(path, isolation_level=None)
    initialize_journal_work(db)
    return db


def corrected_journal_fixture():
    from riverhog_provenance import append_checkpoint, append_correction, validate_journal

    from tests.support.canonical_discovery import native_scalar_fixture

    fixture = native_scalar_fixture(3)
    with validate_journal(fixture.raw, catalog=fixture.catalog, require_profiles=True) as summary:
        target = {"entry": summary.anchor_at(0)["through"], "assertion_id": fixture.assertion_id}
        who = summary.frames[0].document["recorded_by_agent_id"]
    raw = append_correction(
        fixture.raw,
        [target],
        reason="fixture retirement",
        recorded_by_agent_id=who,
        catalog=fixture.catalog,
    )
    raw = append_checkpoint(
        raw,
        recorded_by_agent_id=who,
        purpose="urn:test:fixture:checkpoint",
        catalog=fixture.catalog,
    )
    return fixture, raw, target


def metadata_octets(value):
    if value is None:
        return 0
    return len(value) if isinstance(value, bytes) else len(str(value).encode())


class _MeasuredCursor:
    def __init__(self, actual, metrics):
        self.actual, self.metrics = actual, metrics

    def fetchone(self):
        row = self.actual.fetchone()
        if row is not None:
            self.metrics["returned_rows"] += 1
            self.metrics["returned_octets"] += sum(metadata_octets(value) for value in row)
        return row

    def fetchall(self):
        rows = []
        while (row := self.fetchone()) is not None:
            rows.append(row)
        return rows

    @property
    def rowcount(self):
        return self.actual.rowcount

    def close(self):
        self.actual.close()


class MeasuredConnection:
    def __init__(self, actual):
        self.actual = actual
        self.metrics = dict.fromkeys(
            (
                "sql_statements",
                "returned_rows",
                "parameter_octets",
                "returned_octets",
                "written_octets",
                "encoded_octets",
                "hashed_octets",
                "sqlite_vm_instructions",
                "native_anchor_calls",
            ),
            0,
        )
        self.statements = []

    @property
    def in_transaction(self):
        return self.actual.in_transaction

    def execute(self, sql, parameters=()):
        self.metrics["sql_statements"] += 1
        self.metrics["parameter_octets"] += sum(metadata_octets(value) for value in parameters)
        self.statements.append((sql, parameters))
        return _MeasuredCursor(self.actual.execute(sql, parameters), self.metrics)


@contextmanager
def observe_metadata(db, monkeypatch, *, reader=None):
    """Observe complete Core/Archive metadata execution around one operation."""
    import sys

    from riverhog_canonical_json import canonical_json_bytes

    recorder = MeasuredConnection(db)
    recorder.metrics.update(source_reads=0, source_read_octets=0)
    recorder.source_paths = []
    actual_execute = recorder.execute
    actual_hash = hashlib.sha256

    def execute(sql, parameters=()):
        if sql.lstrip().startswith(("INSERT", "UPDATE")):
            recorder.metrics["written_octets"] += sum(map(metadata_octets, parameters))
        return actual_execute(sql, parameters)

    recorder.execute = execute

    class ObservedHash:
        def __init__(self, value=b"", **kwargs):
            recorder.metrics["hashed_octets"] += len(value)
            self.actual = actual_hash(value, **kwargs)

        def update(self, value):
            recorder.metrics["hashed_octets"] += len(value)
            self.actual.update(value)

        def digest(self):
            return self.actual.digest()

        def hexdigest(self):
            return self.actual.hexdigest()

        def copy(self):
            result = object.__new__(ObservedHash)
            result.actual = self.actual.copy()
            return result

    def encode(value):
        result = canonical_json_bytes(value)
        recorder.metrics["encoded_octets"] += len(result)
        return result

    def tick():
        recorder.metrics["sqlite_vm_instructions"] += 1
        return 0

    actual_read = None if reader is None else reader._read_object

    def read(path):
        assert not db.in_transaction
        recorder.metrics["source_reads"] += 1
        recorder.source_paths.append(path)
        for block in actual_read(path):
            recorder.metrics["source_read_octets"] += len(block)
            yield block

    db.set_progress_handler(tick, 1)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(hashlib, "sha256", ObservedHash)
            for module in tuple(sys.modules.values()):
                if module is not None and getattr(module, "__name__", "").startswith(
                    (
                        "riverhog_archive_contracts",
                        "riverhog_canonical_json",
                        "riverhog_core.canonical_discovery",
                        "riverhog_core.provenance_archive_read",
                    )
                ):
                    if getattr(module, "canonical_json_bytes", None) is canonical_json_bytes:
                        patch.setattr(module, "canonical_json_bytes", encode)
                    if getattr(module, "sha256", None) is actual_hash:
                        patch.setattr(module, "sha256", ObservedHash)
            if reader is not None:
                patch.setattr(reader, "_read_object", read)
            yield recorder
    finally:
        db.set_progress_handler(None, 0)
