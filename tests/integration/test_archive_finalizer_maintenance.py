from __future__ import annotations

from types import SimpleNamespace

import pytest
from riverhog_core.catalog_archive_write_models import ArchiveUploadTerminalRecord as Terminal
from riverhog_core.catalog_db import session_scope
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from sqlalchemy import select

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_finalizer_maintenance import _actual_maintenance, _clock, _record
from tests.unit.test_archive_write_admission import _case, _selected
from tests.unit.test_collection_uploads import _CREATOR

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_postgres_actual_service_receives_joined_original_before_terminal_drain(
    database_url, tmp_path, monkeypatch, mode
):
    _actual_maintenance(tmp_path, monkeypatch, mode, database_url=database_url)


def test_postgres_terminal_due_selector_skips_an_actual_held_request_lock(
    database_url, tmp_path, monkeypatch
):
    with _case(tmp_path, database_url=database_url) as case:
        operation = _selected(case)
        other = case.service.create_or_resume(
            idempotency_key="independent-terminal",
            ingest_source="fixture",
            archive_store=None,
            initiator=_CREATOR,
            event_context=None,
        )
        second = SimpleNamespace(
            collection_id=int(other["collection_id"]),
            factory=case.factory,
            owner=ArchiveWriteAdmission(case.factory),
        )
        other_operation = _selected(second)
        _clock(monkeypatch)
        first_before, second_before = _record(case, operation), _record(case, other_operation)
        with session_scope(case.factory) as session:
            first = session.scalar(
                select(Terminal).where(Terminal.id == operation).with_for_update()
            )
            assert first.drain_next_attempt_at == first_before[2]
            assert case.service.process_due_archive_finalizers(limit=1) == 0
            assert first.drain_next_attempt_at == first_before[2]
            assert _record(case, other_operation)[2] > second_before[2]
        assert _record(case, operation) == first_before
        assert _record(case, other_operation)[:2] == ("draining", None)


@pytest.mark.parametrize("lock", ("upload", "control"))
def test_postgres_initial_terminal_fencing_defers_held_parent_locks(
    database_url, tmp_path, monkeypatch, lock
):
    from concurrent.futures import ThreadPoolExecutor

    from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord
    from riverhog_core.catalog_models import CollectionUploadRecord

    from tests.unit.test_archive_finalizer_maintenance import _join
    from tests.unit.test_archive_finalizer_registry import Finalizer, _scope

    with _case(tmp_path, database_url=database_url) as case:
        service = case.service
        key, target = _scope(case)
        original = Finalizer()
        assert (
            service._archive_finalizers.submit(
                key,
                target_incarnation=target,
                selection="original",
                create=lambda: original,
                advance=lambda owner: owner.advance(),
            )
            == "submitted"
        )
        _join(service)
        service._archive_finalizers.collect(limit=1)[0].result()
        operation = _selected(case)
        now = _clock(monkeypatch)
        model = CollectionUploadRecord if lock == "upload" else ArchiveWriteControlRecord
        with ThreadPoolExecutor(max_workers=1) as pool:
            with session_scope(case.factory) as session:
                session.scalar(
                    select(model).where(model.collection_id == case.collection_id).with_for_update()
                )
                future = pool.submit(service.process_due_archive_finalizers)
                assert future.result(timeout=3) == 0
                context = service._archive_finalizers._contexts[case.collection_id]
                assert context.terminal_id is None and context.owner is original
                assert original.drains == [] and not service._archive_execution._slots
        now[0] += service._config.archive_upload_sweep_interval
        assert service.process_due_archive_finalizers() == 1
        _join(service)
        assert service.process_due_archive_finalizers() == 1
        assert context.drained and original.drains == [operation]
        assert _record(case, operation)[:2] == ("draining", None)
