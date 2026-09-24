"""Concrete transaction-scoped bridge against the audited Riverhog base.

Not imported by the running server on this reference branch. This file was
source-audited and syntax-checked, NOT executed against the full application.
Its private helper dependencies deliberately make reconciliation explicit.
"""
from __future__ import annotations

from collections.abc import Callable
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from handoff import AccessDenied, Actor, JobReceipt, TerminalHandoffError


class RiverhogAuthority:
    def __init__(self, *, clock: Callable[[], str]) -> None:
        self.clock = clock

    def _fresh_principal(self, session: Session, actor: Actor):
        from riverhog_core.app_permissions import ApplicationAccess, Principal
        from riverhog_core.catalog_models import AppKeyAccessGrantRecord, AppKeyRecord

        actor.validate()
        # Never persist tokens or resurrect a Principal from saved grant claims.
        key = session.scalar(select(AppKeyRecord).where(AppKeyRecord.id == actor.key_id)
            .with_for_update().execution_options(populate_existing=True))
        if key is None or key.app != actor.app or key.revoked_at is not None or (
            key.expires_at is not None and key.expires_at <= self.clock()
        ):
            raise AccessDenied("original application key is not active")
        rows = session.scalars(select(AppKeyAccessGrantRecord)
            .where(AppKeyAccessGrantRecord.key_id == key.id).with_for_update()
            .execution_options(populate_existing=True)).all()
        # No artifact capability, bootstrap bypass, or delegated system identity.
        return Principal(id=key.app, key_id=key.id,
            access=frozenset(ApplicationAccess(permission=row.permission, resource=row.resource) for row in rows))

    def require_accept(self, session: Session, actor: Actor, tags: tuple[str, ...], *, copies: bool) -> None:
        from riverhog_core.app_permissions import ARCHIVES_MANAGE, COLLECTIONS_CREATE
        from riverhog_core.collection_access import require_collection_create_access
        from riverhog_protocol.errors import Forbidden, NotFound

        principal = self._fresh_principal(session, actor)
        try:
            require_collection_create_access(principal, COLLECTIONS_CREATE, tags=tags)
            if copies:
                # Conservative pre-publication scope: all initial tags must be
                # covered, as on collection creation. Not a new accepted policy.
                require_collection_create_access(principal, ARCHIVES_MANAGE, tags=tags)
        except (Forbidden, NotFound) as exc:
            raise AccessDenied("copy-intent acceptance authority denied") from exc

    def require_handoff(self, session: Session, actor: Actor, collection_id: int) -> None:
        from riverhog_core.app_permissions import ARCHIVES_MANAGE
        from riverhog_core.collection_access import require_collection_access
        from riverhog_core.catalog_models import CollectionRecord, CollectionTagMembershipRecord
        from riverhog_protocol.errors import Forbidden, NotFound

        principal = self._fresh_principal(session, actor)
        # Hold current collection/tag-membership authority through job insertion.
        # Reconcile global lock ordering with ordinary mutation paths before use.
        session.execute(select(CollectionRecord.id).where(CollectionRecord.id == collection_id).with_for_update()).all()
        session.execute(select(CollectionTagMembershipRecord.collection_id).where(
            CollectionTagMembershipRecord.collection_id == collection_id,
        ).with_for_update()).all()
        if not principal.allows(ARCHIVES_MANAGE):
            raise AccessDenied("archive-management authority denied")
        try:
            require_collection_access(session, principal, ARCHIVES_MANAGE, collection_id)
        except (Forbidden, NotFound) as exc:
            raise AccessDenied("archive-management collection scope denied") from exc


class RiverhogCatalog:
    def require_publication(self, session: Session, collection_id: int, actor: Actor, archive_store: str) -> None:
        from riverhog_core.catalog_models import CollectionArchiveCopyRecord, CollectionRecord
        from riverhog_core.services.archive_records import archive_copy_is_complete

        session.flush()
        collection = session.scalar(select(CollectionRecord).where(CollectionRecord.id == collection_id).with_for_update())
        copy = session.get(CollectionArchiveCopyRecord, (collection_id, archive_store))
        if collection is None or not collection.is_published or collection.created_by_principal_id != actor.app or copy is None or not archive_copy_is_complete(copy):
            raise TerminalHandoffError("publication_missing")


class RiverhogJobs:
    """Small extracted creation primitive; normal execution stays unchanged.

    Do NOT implement this port by calling create_or_resume: that method owns a
    separate committing Session and deliberately restarts terminal jobs.
    """
    def __init__(self, ordinary_copy_service: Any, authority: RiverhogAuthority) -> None:
        self.service = ordinary_copy_service
        self.authority = authority

    def ensure(self, session: Session, collection_id: int, destination_store: str, actor: Actor,
               event_context: dict[str, Any], now: str) -> JobReceipt:
        from riverhog_core.catalog_models import ArchiveCopyJobRecord, CollectionArchiveCopyRecord, CollectionRecord
        from riverhog_core.services.archive_copy_jobs import _select_source_copy
        from riverhog_core.services.archive_records import archive_copy_is_complete
        from riverhog_core.services.collection_mutations import require_collection_archive_idle
        from riverhog_core.services.lifecycle_events import event_context_json
        from riverhog_protocol.errors import BadRequest, Conflict, NotFound

        # Defend this creation primitive even if invoked outside Handoff.
        self.authority.require_handoff(session, actor, collection_id)
        require_collection_archive_idle(session, collection_id)
        collection = session.get(CollectionRecord, collection_id)
        if collection is None or not collection.is_published:
            raise TerminalHandoffError("publication_missing")
        existing = session.scalar(select(ArchiveCopyJobRecord).where(
            ArchiveCopyJobRecord.collection_id == collection_id,
            ArchiveCopyJobRecord.destination_store == destination_store,
        ).with_for_update())
        if existing is not None:
            # Natural job identity already exists, including canceled/failed.
            # Retain the actual job's initiator, state, and event history.
            return JobReceipt(collection_id, destination_store, False)
        copy = session.get(CollectionArchiveCopyRecord, (collection_id, destination_store))
        if copy is not None and archive_copy_is_complete(copy):
            # No synthetic success receipt for an ordinary API conflict.
            raise TerminalHandoffError("destination_already_present")
        try:
            destination = self.service._configured_store(destination_store)
            source_copy = _select_source_copy(collection, config=self.service._config,
                destination_store=destination, source_store=None)
        except (BadRequest, Conflict, NotFound) as exc:
            raise TerminalHandoffError("source_unavailable") from exc
        destination_adapter = self.service._archive_stores.require(destination).store
        job = ArchiveCopyJobRecord(
            collection_id=collection_id, source_store=source_copy.store,
            destination_store=destination,
            destination_storage_prefix=destination_adapter.new_collection_archive_storage_prefix(),
            initiated_by_app=actor.app, initiated_by_key_id=actor.key_id,
            event_context_json=event_context_json(event_context), state="requested",
            requested_at=now, next_attempt_at=now,
        )
        session.add(job)
        session.flush()
        self.service._emit(job, type="archive_copy_job.requested", session=session)
        session.flush()
        return JobReceipt(collection_id, destination, True)

    def observe(self, session: Session, receipt: JobReceipt) -> dict[str, Any] | None:
        from riverhog_core.catalog_models import ArchiveCopyJobRecord
        from riverhog_core.services.archive_copy_jobs import _job_payload

        job = session.get(ArchiveCopyJobRecord, (receipt.collection_id, receipt.destination_store))
        return _job_payload(job) if job is not None else None
