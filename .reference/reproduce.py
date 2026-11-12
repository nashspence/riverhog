"""Reproduce the original bookkeeping mismatch without editing product source."""
from __future__ import annotations
import hashlib
import json
import pathlib
import sys
import tempfile
import time
from datetime import timedelta

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from riverhog_core.archive_execution_tuning import ArchiveExecutionTuning
from riverhog_core.services.physical_execution import PhysicalExecutionOwner
from riverhog_core.services.archive_construction_retention import ArchiveConstructionRetention
from riverhog_core.services.upload_journal_validation_retirement import UploadJournalValidationRetirement
from tests.unit.test_journal_retirement_registry import native_case
from tests.unit.test_journal_validation_retirement import ONE

NAMES = (
    "riverhog/src/riverhog_core/services/upload_journal_validation_retirement.py",
    "riverhog/src/riverhog_core/services/workspace_retirement_registry.py",
    "riverhog/src/riverhog_core/services/journal_retirement_registry.py",
)

def hashes():
    return {name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in NAMES}

def main():
    before=hashes();started=time.monotonic()
    with tempfile.TemporaryDirectory(prefix="retirement-scope-") as tmp:
        with native_case(pathlib.Path(tmp)) as case:
            pool=PhysicalExecutionOwner(ArchiveExecutionTuning(terminal_workers=2))
            ledger=ArchiveConstructionRetention(pool._tuning)
            cleanup=UploadJournalValidationRetirement(case.factory,case.root,execution=pool,
                retention=ledger,metadata_budget=1024*1024,receive_limit=16,
                retry_after=timedelta(microseconds=1))
            try:
                first=cleanup.advance(limit=1,octet_budget=3)
                original=cleanup._registry._contexts[ONE]
                worker=pool._slots[original.reservation.key].worker
                worker.join(timeout=10)
                assert not worker.is_alive()
                assert original.actor.received and ledger._counts[False]==1
                second=cleanup.advance(limit=1,octet_budget=3)
                assert (first,second)==(1,3)
            finally:
                cleanup.close();pool.close()
                while cleanup._registry.collect(limit=16):
                    pass
                assert cleanup.receive_stopped()
                assert ledger._counts=={False:0,True:0}
    assert hashes()==before
    return dict(phase="reproduced-not-qualified",qualification=False,
                source="dd09f4ab5e824935e680f26fba3d73d2484cb426",source_hashes=before,
                first_return=first,second_return=second,limit=1,original_worker_joined=True,
                original_receiving=True,ordinary_charged_before_collection=True,
                all_originals_received=True,source_unchanged=True,elapsed_seconds=time.monotonic()-started)

if __name__=="__main__":
    print(json.dumps(main(),sort_keys=True))
