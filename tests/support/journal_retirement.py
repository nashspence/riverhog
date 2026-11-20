from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace

import pytest
from riverhog_core.services.archive_construction_retention import ArchiveConstructionRetention
from riverhog_core.services.upload_journal_validation_retirement import (
    UploadJournalValidationRetirement,
)

RETRY = timedelta(microseconds=1)


@pytest.fixture
def cleanups(owned):
    originals = []

    def make(case, *, metadata_budget=1024 * 1024, terminal_workers=2):
        pool = owned.make(terminal_workers=terminal_workers)
        ledger = ArchiveConstructionRetention(pool._tuning)
        cleanup = UploadJournalValidationRetirement(
            case.factory,
            case.root,
            execution=pool,
            retention=ledger,
            metadata_budget=metadata_budget,
            receive_limit=16,
            retry_after=RETRY,
        )
        original = SimpleNamespace(
            cleanup=cleanup, registry=cleanup._registry, pool=pool, ledger=ledger
        )
        originals.append(original)
        return original

    yield make
    for original in originals:
        original.cleanup.close()
        original.pool.close()
        assert original.cleanup.receive_stopped()
