from __future__ import annotations

import sqlite3
from collections import defaultdict
from contextlib import contextmanager

import pytest
from riverhog_growth_contracts import growth_binding
from riverhog_provenance import CanonicalCorpusValidator, ProvenanceValidationError
from test_source_anchor_growth import record

ROOT = CanonicalCorpusValidator._identity


def octets(value):
    if isinstance(value, (tuple, list)):
        return sum(octets(item) for item in value)
    if isinstance(value, bytes):
        return len(value)
    return 0 if value is None else len(str(value).encode())


class IdentityCursor:
    def __init__(self, actual, observer):
        self.actual, self.observer = actual, observer
        self.closed = False

    def fetchone(self):
        row = self.actual.fetchone()
        if row is not None:
            self.observer.metrics["returned_rows"] += 1
            self.observer.metrics["returned_octets"] += octets(row)
            assert len(row) == 1 and type(row[0]) is int and row[0] in (0, 1), (
                "identity comparison transferred retained preimage data instead of an exact match"
            )
        return row

    def close(self):
        self.actual.close()
        self.closed = True


class IdentityObserver:
    def __init__(self, actual):
        self.actual = actual
        self.metrics = defaultdict(int)
        self.cursors = []

    def execute(self, query, parameters=()):
        assert all(cursor.closed for cursor in self.cursors)
        self.metrics["sql_statements"] += 1
        self.metrics["parameter_octets"] += octets(parameters)
        actual = self.actual.execute(query, parameters)
        if query.startswith("INSERT") and actual.rowcount > 0:
            self.metrics["written_rows"] += actual.rowcount
            self.metrics["written_parameter_octets"] += octets(parameters)
        if query.startswith("SELECT"):
            self.metrics["comparison_input_octets"] += sum(
                len(value) for value in parameters if isinstance(value, bytes)
            )
        cursor = IdentityCursor(actual, self)
        self.cursors.append(cursor)
        return cursor


@contextmanager
def observe(corpus, observer, monkeypatch):
    from test_corpus_source_growth import source_reads

    actual = observer.actual
    while not isinstance(actual, sqlite3.Connection):
        actual = actual.actual if hasattr(actual, "actual") else actual.db

    def stepped():
        observer.metrics["sqlite_vm_instructions"] += 1
        return 0

    def authorized(action, table, _column, *_rest):
        if action == sqlite3.SQLITE_READ and table != "identities":
            raise AssertionError("identity comparison traversed unrelated corpus data")
        return sqlite3.SQLITE_OK

    actual.set_progress_handler(stepped, 1)
    actual.set_authorizer(authorized)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(corpus, "_db", observer)
            with source_reads(monkeypatch) as frames:
                yield
            assert frames == []
    finally:
        actual.set_progress_handler(None, 0)
        actual.set_authorizer(None)
    assert all(cursor.closed for cursor in observer.cursors)


def measure_identity(
    corpus, identity, domain, value, monkeypatch, *, retained_identities, reject=False
):
    dimensions = {
        "retained_identities": retained_identities,
        "identity_octets": len(identity.encode()),
        "domain_octets": len(domain.encode()),
        "preimage_octets": len(value),
    }
    observer = IdentityObserver(corpus._db)
    with observe(corpus, observer, monkeypatch):
        if reject:
            with pytest.raises(ProvenanceValidationError, match="identity or domain"):
                ROOT(corpus, identity, domain, value)
        else:
            ROOT(corpus, identity, domain, value)
    return record(growth_binding(ROOT), dimensions, observer.metrics)


def test_identity_equality_returns_no_retained_preimage(monkeypatch):
    with CanonicalCorpusValidator() as corpus:
        identity = "urn:test:identity"
        value = b"large exact retained preimage" * 65536
        corpus._identity(identity, "assertion", value)
        observer = IdentityObserver(corpus._db)
        with observe(corpus, observer, monkeypatch):
            ROOT(corpus, identity, "assertion", value)
        assert observer.metrics["returned_octets"] == 1
        assert observer.metrics["comparison_input_octets"] == len(value)


@pytest.mark.growth_witness(
    "native-corpus-identity-comparison/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus.CanonicalCorpusValidator._identity",
)
def test_actual_identity_comparison_charges_candidate_bytes_without_old_preimage_transfer(
    monkeypatch, record_property
):
    samples = []
    for unrelated, width, size in ((0, 1, 0), (1, 17, 1), (17, 129, 2048), (129, 129, 65536)):
        with CanonicalCorpusValidator() as corpus:
            corpus._db.executemany(
                "INSERT INTO identities VALUES (?, ?, ?)",
                (("unrelated-" + str(index), "entry", b"x" * 4096) for index in range(unrelated)),
            )
            identity, domain = "ü" * width, "assertion"
            value = (b"\x00\xffabc" * (size // 5 + 1))[:size]
            for repeat in range(3):
                sample = measure_identity(
                    corpus,
                    identity,
                    domain,
                    value,
                    monkeypatch,
                    retained_identities=unrelated + 1,
                )
                assert sample["metrics"]["sql_statements"] == 2
                assert sample["metrics"]["returned_rows"] == (0 if repeat == 0 else 1)
                assert sample["metrics"]["written_rows"] == (1 if repeat == 0 else 0)
                samples.append(sample)
            for changed_domain, candidate in (
                ("referent", value),
                (domain, value + b"x"),
                (domain, value[:-1] if value else b"x"),
                (domain, (b"x" + value[1:]) if value else b"x"),
                (domain, (value[:-1] + b"x") if value else b"x"),
            ):
                before = corpus._db.total_changes
                sample = measure_identity(
                    corpus,
                    identity,
                    changed_domain,
                    candidate,
                    monkeypatch,
                    retained_identities=unrelated + 1,
                    reject=True,
                )
                assert sample["metrics"]["sql_statements"] == 1
                assert sample["metrics"]["returned_octets"] == 1
                assert sample["metrics"]["written_rows"] == 0
                assert corpus._db.total_changes == before
                samples.append(sample)
            corpus._identity("large-preimage", "entry", b"x" * 1048576)
            samples.append(
                measure_identity(
                    corpus,
                    "large-preimage",
                    "entry",
                    b"x",
                    monkeypatch,
                    retained_identities=unrelated + 2,
                    reject=True,
                )
            )
    record_property("growth_samples", samples)
