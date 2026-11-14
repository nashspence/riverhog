from __future__ import annotations

from dataclasses import replace

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CatalogEventRecord, CollectionRecord
from riverhog_core.catalog_registration_models import (
    CollectionRegistrationRecord,
    CollectionRegistrationVolumeRecord,
)
from riverhog_core.services import collection_registration_cleanup as cleanup
from riverhog_core.services.collection_deletions import SqlAlchemyCollectionDeletionService
from riverhog_protocol.errors import Conflict
from sqlalchemy import func, select

from tests.unit.test_collection_deletions import COLLECTION_ID, DELETER, _service
from tests.unit.test_collection_registration_controls import _published
from tests.unit.test_collection_registration_operations import _close, _open


def _retained_fixture(tmp_path):
    (tmp_path / "registration").mkdir()
    source, keys, principal, created, source_id = _open(tmp_path / "registration")
    try:
        acknowledgement = _published(source, source_id, principal)
        with session_scope(source._session_factory) as session:
            operation = session.get(CollectionRegistrationRecord, acknowledgement["operation_id"])
            retained = {
                column.name: getattr(operation, column.name)
                for column in CollectionRegistrationRecord.__table__.columns
            }
            summaries = [
                {"sequence": row.sequence, "object_id": row.object_id, "kind": row.kind}
                for row in session.scalars(
                    select(CollectionRegistrationVolumeRecord).where(
                        CollectionRegistrationVolumeRecord.registration_id == operation.id
                    )
                )
            ]
    finally:
        _close(source, keys)
    config, archive_store, service = _service(tmp_path / "retirement.sqlite3")
    service._registration_tuning = replace(config.registration, plans_per_step=2)
    # Retained operational metadata is opaque to teardown. The independent
    # finalized archive fixture owns this collection's archive authority.
    with session_scope(service._session_factory) as session:
        for index, collection_id in enumerate((COLLECTION_ID, COLLECTION_ID, COLLECTION_ID + 1)):
            operation_id = f"{index + 1:064x}"
            session.add(
                CollectionRegistrationRecord(
                    **{**retained, "id": operation_id, "collection_id": collection_id}
                )
            )
            session.flush()
            for summary in summaries:
                session.add(
                    CollectionRegistrationVolumeRecord(registration_id=operation_id, **summary)
                )
    return config, archive_store, service, len(summaries)


def _counts(service):
    with session_scope(service._session_factory) as session:
        return (
            session.scalar(select(func.count()).select_from(CollectionRegistrationRecord)),
            session.scalar(select(func.count()).select_from(CollectionRegistrationVolumeRecord)),
        )


@pytest.mark.parametrize("interrupt", [False, True])
def test_confirmed_collection_deletion_reclaims_retained_state_in_bounded_cold_resumable_windows(
    tmp_path, monkeypatch, interrupt
):
    config, archive_store, service, summaries = _retained_fixture(tmp_path)
    original_counts = _counts(service)
    with session_scope(service._session_factory) as session:
        with pytest.raises(Conflict, match="selected collection deletion"):
            cleanup.reclaim_collection_registrations(
                session, COLLECTION_ID, limits=config.registration.emission_limits()
            )
    assert _counts(service) == original_counts
    challenge = str(service.plan(COLLECTION_ID)["challenge"])
    service.delete(COLLECTION_ID, challenge=challenge, initiator=DELETER)
    original = cleanup._delete
    interrupted = False

    def delete(*args, **kwargs):
        nonlocal interrupted
        original(*args, **kwargs)
        if interrupt and not interrupted:
            interrupted = True
            raise RuntimeError("fixture interruption after metadata deletion")

    monkeypatch.setattr(cleanup, "_delete", delete)
    previous = original_counts
    reopened = False
    for _ in range(100):
        try:
            progressed = service.process_due(limit=1)
        except RuntimeError as exc:
            assert str(exc) == "fixture interruption after metadata deletion"
            assert _counts(service) == previous
            monkeypatch.setattr(cleanup, "_delete", original)
            continue
        current = _counts(service)
        assert 0 <= previous[0] - current[0] <= 1
        assert 0 <= previous[1] - current[1] <= 2
        if current != previous and not reopened:
            service = SqlAlchemyCollectionDeletionService(
                replace(config, registration=service._registration_tuning),
                service._archive_stores,
                None,
            )
            reopened = True
        with session_scope(service._session_factory) as session:
            event = session.scalar(select(CatalogEventRecord))
            if event is not None and event.published:
                assert current == (1, summaries)
                assert session.get(CollectionRecord, COLLECTION_ID) is None
        previous = current
        if not progressed:
            break
    else:
        pytest.fail("bounded registration retirement did not finish")
    assert reopened and interrupted is interrupt
    assert current == (1, summaries)
    with session_scope(service._session_factory) as session:
        unrelated = session.get(CollectionRegistrationRecord, f"{3:064x}")
        assert unrelated is not None and unrelated.collection_id == COLLECTION_ID + 1
        event = session.scalar(select(CatalogEventRecord))
        assert event is not None and event.published
