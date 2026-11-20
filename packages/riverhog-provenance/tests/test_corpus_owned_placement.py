"""Caller placement preserves original resources and never owns namespace deletion."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from riverhog_provenance import CanonicalCorpusValidator
from riverhog_provenance import corpus as owner
from sqlite_admission import SqliteConnectionAdmission


@pytest.mark.parametrize("failure", [None, "connection_reply", "callback", "schema"])
def test_explicit_corpus_placement_keeps_the_callers_namespace_and_original_connection(
    tmp_path, monkeypatch, failure
):
    directory = tmp_path / "registered"
    directory.mkdir(mode=0o700)
    database = directory / "corpus.sqlite3"
    database.touch(mode=0o600)
    sibling = directory / "checks.sqlite3"
    sibling.write_bytes(b"independently owned memo")
    admission = SqliteConnectionAdmission(
        connection_limit=1, cursor_limit=4, operation_limit=1, verify_process=lambda: None
    )
    original = admission.connection_scope()
    retained, supplied = [], []

    def forbidden(*args, **kwargs):
        pytest.fail("caller placement cannot allocate or recursively delete a namespace")

    monkeypatch.setattr(owner, "mkdtemp", forbidden)
    monkeypatch.setattr(owner, "shutil", SimpleNamespace(rmtree=forbidden))

    def connect(selected):
        assert selected == database
        supplied.append(selected)
        connection = original.connect(selected.as_uri() + "?mode=rw", uri=True)
        if failure == "connection_reply":
            raise InterruptedError("original connection acknowledgement lost")
        return connection

    def retain(corpus):
        retained.append(corpus)
        if failure == "callback":
            raise InterruptedError("original callback acknowledgement lost")

    def schema(corpus):
        assert retained == [corpus]
        raise InterruptedError("original schema acknowledgement lost")

    if failure == "schema":
        monkeypatch.setattr(CanonicalCorpusValidator, "_initialize_schema", schema)
    try:
        if failure is None:
            corpus = CanonicalCorpusValidator(
                directory=directory, connection_factory=connect, on_storage_open=retain
            )
            assert retained == [corpus] and admission.status.connections == 1
            corpus.close()
            corpus.close()
        else:
            with pytest.raises(InterruptedError, match="acknowledgement lost"):
                CanonicalCorpusValidator(
                    directory=directory, connection_factory=connect, on_storage_open=retain
                )
        assert supplied == [database]
        assert bool(retained) is (failure != "connection_reply")
        assert directory.is_dir() and database.is_file()
        assert sibling.read_bytes() == b"independently owned memo"
        if failure == "connection_reply":
            assert admission.status.connections == 1 and not original.received
    finally:
        original.close()
    assert original.received and admission.status.connections == 0


def test_explicit_corpus_placement_requires_the_callers_absolute_original_factory(tmp_path):
    with pytest.raises(ValueError, match="absolute directory and factory"):
        CanonicalCorpusValidator(directory=tmp_path)
    with pytest.raises(ValueError, match="absolute directory and factory"):
        CanonicalCorpusValidator(
            directory=tmp_path.relative_to(tmp_path.anchor), connection_factory=lambda path: None
        )
