"""Issue-provided public-source bug witness, not qualification of the repair.

Run from the pinned checkout in its locked all-packages environment. All files
are temporary and the injection refuses only the initial creator close.
"""
from __future__ import annotations

import hashlib
import json
import os
import pathlib
import tempfile
import time
from unittest.mock import patch

from tests.unit.test_journal_capture import retained
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_staged_journal_range import JOURNAL
from tests.unit.test_upload_journal_certificates import ready
from riverhog_core.services import collection_uploads as source_owner
from riverhog_core.services.upload_journal_validation_owner import UploadJournalValidationOwner
from riverhog_core.journal_validation_workspace import lease_journal_validation_workspace


def main():
    root = pathlib.Path.cwd().resolve()
    names = [
        "packages/riverhog-provenance/src/riverhog_provenance/journal_store.py",
        "riverhog/src/riverhog_core/services/upload_journal_validation_owner.py",
        "riverhog/src/riverhog_core/journal_validation_workspace.py",
    ]
    hashes = {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in names}
    started = time.monotonic()
    opened, refused, slots = [], [], []
    original_open, original_close = os.open, os.close
    reserve = UploadJournalValidationOwner._reserve
    active = True

    def opening(path, flags, *args, **options):
        fd = original_open(path, flags, *args, **options)
        if active and pathlib.Path(os.fsdecode(path)).name == "snapshot.sqlite3" and flags & os.O_CREAT:
            opened.append(fd)
        return fd

    def closing(fd):
        if active and opened and fd == opened[0] and not refused:
            refused.append(fd)
            raise InterruptedError("refused before dispatching initial Native close")
        return original_close(fd)

    def reserving(owner, key):
        slot = reserve(owner, key)
        if slot is not None:
            slots.append(slot)
        return slot

    with tempfile.TemporaryDirectory(prefix="native-creation-reproduction-") as tmp:
        path = pathlib.Path(tmp)
        with retained(path, source(journal_id=JOURNAL)) as (_, factory):
            work, _ = ready(factory)
            owner = UploadJournalValidationOwner(factory, path / "native")
            try:
                with patch.object(os, "open", opening), patch.object(os, "close", closing), patch.object(
                    UploadJournalValidationOwner, "_reserve", reserving
                ):
                    try:
                        owner.advance(work, catalog=source_owner.admission_provenance_catalog())
                    except InterruptedError:
                        pass
                    else:
                        raise AssertionError("injected refusal did not escape")
                active = False
                assert len(opened) == len(refused) == len(slots) == 1
                stat = os.fstat(opened[0])
                assert slots[0].session is None and not slots[0].resources._exit_callbacks
                assert owner.active_keys() == ()
                files = list((path / "native").glob("*/snapshot.sqlite3"))
                assert len(files) == 1
                with lease_journal_validation_workspace(path / "native", files[0].parent.name) as lease:
                    assert lease is not None and os.fstat(opened[0]) == stat
            finally:
                active = False
                for fd in opened:
                    original_close(fd)
                owner.close()
    assert all(hashlib.sha256((root / name).read_bytes()).hexdigest() == value for name, value in hashes.items())
    return {"phase": "reproduced-not-qualified", "qualification": False,
            "source": "c5157d69712185aa8d4ad185e2e6d465dfbd7179", "source_hashes": hashes,
            "descriptor_open_after_slot_removed": True, "competing_lease_acquired": True,
            "all_probe_descriptors_closed": True, "source_unchanged": True,
            "elapsed_seconds": time.monotonic() - started}

if __name__ == "__main__":
    print(json.dumps(main(), sort_keys=True))
