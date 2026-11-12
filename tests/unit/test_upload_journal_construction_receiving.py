"""Actual Core.advance retains constructor origins before any Native return."""

from __future__ import annotations

import os
import subprocess
import sys

import pytest
import retained_resource
from riverhog_core.journal_storage_grants import JournalStorageGrants
from riverhog_core.services import collection_uploads as owner_source
from riverhog_core.services.journal_storage_resources import JournalStorageResources
from riverhog_core.services.upload_journal_validation_owner import UploadJournalValidationOwner

from tests.support.retained_resource import fault_native as fault_native
from tests.unit.test_journal_capture import retained
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_staged_journal_range import JOURNAL
from tests.unit.test_upload_journal_certificates import ready

pytestmark = pytest.mark.skipif(os.name != "posix", reason="Core workspace custody uses POSIX")


def test_actual_advance_receives_original_creator_before_parent_release(
    tmp_path, monkeypatch, fault_native
):
    native, _path = fault_native
    monkeypatch.setattr(retained_resource, "Resource", native.Resource)
    resources = JournalStorageResources(JournalStorageGrants(connections=2))
    frames = []

    def construction():
        original = resources.journal()
        original.creator._test_fault("before_close")
        frames.append(original)
        return original

    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        work, _certificates = ready(factory)
        owner = UploadJournalValidationOwner(
            factory, tmp_path / "native", construction_factory=construction
        )
        try:
            with pytest.raises(RuntimeError):
                owner.advance(work, catalog=owner_source.admission_provenance_catalog())
            assert len(frames) == 1 and frames[0].received
            assert frames[0].creator.metrics["close_calls"] == 1
            assert owner.active_keys() == () and resources.status.connections == 0
        finally:
            owner.close()
            resources.close()


def test_actual_unknown_retains_original_slot_capacity_and_lease(tmp_path, fault_native):
    _native, extension = fault_native
    code = r"""
import importlib.util,os,sys
from pathlib import Path
import retained_resource
from riverhog_provenance import JournalConstruction,JournalStore
from sqlite_admission import SqliteConnectionAdmission
from riverhog_core.services import collection_uploads as source_owner
from riverhog_core.services.upload_journal_validation_owner import UploadJournalValidationOwner
from riverhog_core.journal_validation_workspace import lease_journal_validation_workspace
from riverhog_core.journal_validation_workspace_lease import ValidationWorkspaceLease
from tests.unit.test_journal_capture import retained
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_staged_journal_range import JOURNAL
from tests.unit.test_upload_journal_certificates import ready
spec=importlib.util.spec_from_file_location("_retained_resource_test._native",sys.argv[2])
native=importlib.util.module_from_spec(spec);spec.loader.exec_module(native)
retained_resource.Resource=native.Resource
root=Path(sys.argv[1]);frames=[]
admission=SqliteConnectionAdmission(connection_limit=2,cursor_limit=128,operation_limit=2,verify_process=lambda:None)
def make():
 original=JournalConstruction(admission.connection_scope(),resource_limit=2)
 original.creator._test_fault("unknown_close_open")
 frames.append(original)
 return original
with retained(root,source(journal_id=JOURNAL)) as (_,factory):
 work,_certificates=ready(factory)
 owner=UploadJournalValidationOwner(factory,root/"native",construction_factory=make,concurrency=1)
 try:owner.advance(work,catalog=source_owner.admission_provenance_catalog())
 except (RuntimeError,OSError):pass
 else:raise AssertionError("unknown creator returned success")
 assert len(frames)==1 and not frames[0].received and frames[0].creator.state=="unknown"
 assert owner.active_keys() and len(owner._slots)==1
 slot=next(iter(owner._slots.values()))
 assert slot.session is None and slot.construction is frames[0] and slot.closing
 files=list((root/"native").glob("*/snapshot.sqlite3"));assert len(files)==1
 competitor=ValidationWorkspaceLease()
 with lease_journal_validation_workspace(
  root/"native",files[0].parent.name,receiver=competitor
 ) as claimed:
  assert claimed is None,"unknown creator released original parent custody"
 competitor.close()
 calls=frames[0].creator.metrics["close_calls"]
 for _ in range(17):
  try:owner.close()
  except RuntimeError:pass
  else:raise AssertionError("unknown construction was force-received")
 assert frames[0].creator.metrics["close_calls"]==calls
 assert owner.active_keys() and not frames[0].received and not slot.lease.received
 other=root/"independent";other.mkdir(mode=0o700)
 independent=JournalConstruction(admission.connection_scope(),resource_limit=2)
 store=JournalStore.open_retained(other,construction=independent,create=True)
 store.close();assert independent.close_step()
 assert not frames[0].received and owner.active_keys()
 print("ORIGINAL_CORE_UNKNOWN_RETAINED;INDEPENDENT_NATIVE_RECEIVED",flush=True)
 # Process exit is fixture cleanup, never a product receiving receipt.
 os._exit(0)
"""
    run = subprocess.run(
        [sys.executable, "-c", code, str(tmp_path), str(extension)],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert run.returncode == 0, (run.stdout + run.stderr)[-5000:]
    assert "ORIGINAL_CORE_UNKNOWN_RETAINED;INDEPENDENT_NATIVE_RECEIVED" in run.stdout
