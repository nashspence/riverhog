from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
from riverhog_core.services import collection_uploads as owner

MODULES = (
    "collection_uploads",
    *(path.stem for path in sorted(Path(owner.__file__).parent.glob("upload_journal_*.py"))),
)


@pytest.mark.parametrize("first", MODULES)
def test_journal_service_declarations_are_complete_for_every_first_import(first):
    code = f"""
import importlib
from riverhog_growth_contracts import growth_binding
importlib.import_module("riverhog_core.services.{first}")
from riverhog_core.services import collection_uploads as owner
from riverhog_core.services.upload_journal_validation_owner import UploadJournalValidationOwner
unit=owner.SqlAlchemyCollectionUploadService._advance_provenance_journal_validation_unit
caller=owner.SqlAlchemyCollectionUploadService._advance_provenance_journal_validation
assert growth_binding(unit).operation is unit
assert growth_binding(caller).operation is caller
assert growth_binding(unit).declaration.id == "riverhog-server.validation-service-unit.growth/v1"
assert growth_binding(caller).declaration.id == (
    "riverhog-server.validation-service-advance.growth/v1"
)
assert dict(growth_binding(unit).delegates)["owner"] is UploadJournalValidationOwner.advance
assert dict(growth_binding(caller).delegates)["unit"] is unit
assert dict(growth_binding(caller).delegates)["stamp"] is owner._upload_journal_validation_stamp
scheduler=owner.SqlAlchemyCollectionUploadService.process_due_provenance_journal_validations
binding=growth_binding(scheduler)
assert binding.operation is scheduler
assert binding.declaration.id == "riverhog-server.validation-service-scheduler.growth/v1"
assert dict(binding.delegates)["validation"] is caller
assert dict(binding.delegates)["sweep"] is UploadJournalValidationOwner.release_stale
from riverhog_core.services.upload_journal_validation_retirement import (
    UploadJournalValidationRetirement,
)
assert dict(binding.delegates)["retirement"] is UploadJournalValidationRetirement.advance
"""
    subprocess.run([sys.executable, "-B", "-c", code], check=True, capture_output=True)
