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
