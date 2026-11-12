"""Explicit test-owned original Native journal construction records."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from riverhog_provenance import CorpusConstruction, JournalConstruction
from sqlite_admission import SqliteConnectionAdmission


class JournalConstructions:
    """Keep every exact frame before a constructor call, including failed calls."""

    def __init__(self) -> None:
        self.admission = SqliteConnectionAdmission(
            connection_limit=64,
            cursor_limit=128,
            operation_limit=8,
            verify_process=lambda: None,
        )
        self.frames: list[JournalConstruction | CorpusConstruction] = []

    def retain(self, admission: SqliteConnectionAdmission | None = None) -> JournalConstruction:
        selected = self.admission if admission is None else admission
        original = JournalConstruction(selected.connection_scope(), resource_limit=2)
        self.frames.append(original)
        return original

    def retain_corpus(self) -> CorpusConstruction:
        original = CorpusConstruction(self.admission.connection_scope(), resource_limit=3)
        self.frames.append(original)
        return original

    def close(self) -> None:
        while self.frames:
            original = self.frames[-1]
            if not original.close_step():
                raise BlockingIOError("test owner still retains independent Native readers")
            self.frames.pop()
        status = self.admission.status
        assert not (status.connections or status.cursors or status.blobs or status.operations)


@pytest.fixture
def journal_constructions() -> Iterator[JournalConstructions]:
    original = JournalConstructions()
    try:
        yield original
    finally:
        original.close()


@pytest.fixture(scope="module")
def module_journal_constructions() -> Iterator[JournalConstructions]:
    original = JournalConstructions()
    try:
        yield original
    finally:
        original.close()
