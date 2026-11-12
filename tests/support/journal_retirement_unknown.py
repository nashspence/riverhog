"""Isolated real-backend uncertainty witness; process exit is test cleanup only."""

from __future__ import annotations

import importlib.util
import json
import os
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import workspace_reclamation as files
from riverhog_core.archive_execution_tuning import ArchiveExecutionTuning
from riverhog_core.services.archive_construction_retention import ArchiveConstructionRetention
from riverhog_core.services.physical_execution import PhysicalExecutionOwner
from riverhog_core.services.upload_journal_validation_retirement import (
    UploadJournalValidationRetirement,
)

from tests.support.validation_workspace import leased_workspace
from tests.unit.test_archive_upload_execution import _joined
from tests.unit.test_journal_retirement_registry import native_case
from tests.unit.test_journal_validation_retirement import ONE, TWO, selected


def run(extension: str, root: str, fault: str) -> None:
    spec = importlib.util.spec_from_file_location("_retained_resource_test._native", extension)
    assert spec is not None and spec.loader is not None
    native = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(native)
    with (
        patch.object(files, "Resource", native.Resource),
        patch.object(files, "Borrow", native.Borrow),
    ):
        with native_case(Path(root), identities=(ONE, TWO)) as case:
            tuning = ArchiveExecutionTuning(terminal_workers=2)
            pool = PhysicalExecutionOwner(tuning)
            ledger = ArchiveConstructionRetention(tuning)
            cleanup = UploadJournalValidationRetirement(
                case.factory,
                case.root,
                execution=pool,
                retention=ledger,
                metadata_budget=1024 * 1024,
                receive_limit=2,
                retry_after=timedelta(microseconds=1),
            )
            registry = cleanup._registry
            create = registry._create

            def original(job):
                actor = create(job)
                if job.workspace_id == ONE:
                    actor._files._file._test_fault(fault)
                return actor

            registry._create = original
            assert cleanup.advance(limit=1, octet_budget=3) == 1
            first = registry._contexts[ONE]
            _joined(pool)
            assert registry.collect(limit=1) == 1
            actor = first.actor
            assert actor is not None and not actor.received
            assert actor._files._file.state == "unknown"
            assert first.closing and first.received and not first.closed
            assert ledger._counts[False] == 1
            with leased_workspace(case.root, ONE, shared=True) as reader:
                assert reader is None
            before = dict(actor._files._file.metrics)
            assert (
                registry.submit(
                    selected(case.factory, TWO),
                    octet_budget=3,
                    metadata_budget=1024 * 1024,
                    retry_after=timedelta(microseconds=1),
                )
                == 1
            )
            second = registry._contexts[TWO]
            _joined(pool)
            assert registry.collect(limit=2) == 2
            assert second.actor is not None and second.actor.received
            assert not registry.retained(TWO) and ledger._counts[False] == 1
            assert registry.resume_ready(limit=2, retry_after=timedelta(microseconds=1)) == 1
            _joined(pool)
            assert registry.collect(limit=2) == 1
            assert registry._contexts[ONE] is first and first.actor is actor
            assert actor._files._file.metrics == before
            assert not first.closed and not actor.received and ledger._counts[False] == 1
            assert case.files[ONE].read_bytes() == case.files[TWO].read_bytes() == b"x" * 14
            cleanup.close()
            pool.close()
            try:
                cleanup.receive_stopped()
            except RuntimeError as error:
                assert "unknown" in str(error)
            else:
                raise AssertionError("shutdown silently received an unknown original")
            assert ledger._counts[False] == 1 and not actor.received
            assert actor._files._file.metrics == before
            print(
                json.dumps(
                    {
                        "fault": fault,
                        "original_retained": True,
                        "ordinary_charge": 1,
                        "physical_workers": len(pool._slots),
                        "independent_received": True,
                        "repeat_close_dispatches": 0,
                        "source_octets_each": 14,
                        "forced_receiving": False,
                        "test_process_exit_is_not_receiving": True,
                    }
                ),
                flush=True,
            )
            # Unknown originals deliberately survive until this isolated process exits.
            # Do not report process cleanup as positive in-process receiving.
            os._exit(0)
