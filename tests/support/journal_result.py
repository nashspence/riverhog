"""Explicit test-owned application result receiving before Native effects."""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING

import pytest

from tests.support.native_construction import JournalConstructions

if TYPE_CHECKING:
    from riverhog_core.services.upload_journal_result_receiving import UploadJournalResultOpening


class ResultOpenings:
    def __init__(self, constructions: JournalConstructions) -> None:
        self.constructions = constructions
        self.originals: list[UploadJournalResultOpening] = []

    def retain(self) -> UploadJournalResultOpening:
        from riverhog_core.services.upload_journal_result_receiving import (
            UploadJournalResultOpening,
        )

        original = UploadJournalResultOpening(self.constructions.retain())
        self.originals.append(original)
        return original

    def close(self) -> None:
        while self.originals:
            self.originals[-1].close()
            self.originals.pop()


@pytest.fixture
def upload_result_openings(journal_constructions: JournalConstructions) -> Iterator[ResultOpenings]:
    original = ResultOpenings(journal_constructions)
    try:
        yield original
    finally:
        original.close()
