from __future__ import annotations

import hashlib
import json
import logging
import re
import threading
import time
from collections.abc import Iterable, Iterator, Mapping, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Any

from http_api_contracts import closed_literal_values
from riverhog_age import UploadState
from riverhog_canonical_json import format_scalar
from riverhog_protocol import ArchiveCopyJobSort, SortOrder
from riverhog_protocol.errors import BadRequest, Conflict, InvalidState, NotFound
from riverhog_protocol.paths import PathNormalizationError, normalize_collection_id
from sqlalchemy import asc, delete, desc, exists, func, or_, select
from sqlalchemy.orm import Session, aliased, selectinload
from state_schema import read_snapshot
from time_formats import format_utc_timestamp, utc_now

from riverhog_core.app_permissions import ARCHIVES_MANAGE, Principal, normalize_access
from riverhog_core.archive_formats import (
    PACK_VOLUME_STORAGE_FORMAT,
    RAW_VOLUME_STORAGE_FORMAT,
    archive_object_storage_format,
)
from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.browse import bounded_page, keyset_statement, validate_page_size
from riverhog_core.catalog_db import SessionFactory, make_session_factory, session_scope
from riverhog_core.catalog_models import (
    AppKeyAccessGrantRecord,
    AppKeyRecord,
    ArchiveCopyJobRecord,
    ArchiveCopyObjectUploadRecord,
    CollectionArchiveCopyRecord,
    CollectionArchiveFileObjectRecord,
    CollectionArchiveObjectRecord,
    CollectionRecord,
    CollectionUploadCopyIntentRecord,
    RetrievalCacheLeaseRecord,
)
from riverhog_core.collection_access import collection_access_filter, require_collection_access
from riverhog_core.pack_upload import PACK_VOLUME_CONTENT_TYPE
from riverhog_core.placement_choices import resolve_use_cache
from riverhog_core.ports.archive_objects import (
    ArchiveResumableObjectStore,
    ImmutableArchiveObjectStore,
    WriteCompletionPrecondition,
    WriteSegmentCursor,
    WriteSegmentReceipt,
    WriteSession,
)
from riverhog_core.ports.archive_store import (
    ArchiveObjectIdentity,
    ArchiveStore,
)
from riverhog_core.ports.retrieval_cache import RetrievalCache, RetrievalCacheReceipt
from riverhog_core.raw_upload import RAW_VOLUME_CONTENT_TYPE
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services.archive_copy_job_states import (
    ARCHIVE_COPY_JOB_STATES,
    ARCHIVE_COPY_JOB_TRANSFER_STATES,
)
from riverhog_core.services.archive_records import archive_copy_is_complete
from riverhog_core.services.collection_descriptions import (
    ensure_description_publication_for_copy,
)
from riverhog_core.services.collection_mutations import require_collection_archive_idle
from riverhog_core.services.collection_tags import ensure_tag_publication_for_copy
from riverhog_core.services.lifecycle_events import (
    SqlAlchemyLifecycleEventService,
    event_context_json,
)
from riverhog_core.services.retrieval_cache import register_cache_ready
from riverhog_core.stores.mirrored_archive_resumable_object_store import (
    MirroredArchiveResumableObjectStore,
)
from riverhog_core.throughput import (
    ArchiveThroughputTuning,
    ArchiveTransferResources,
    TransferTiming,
    log_transfer_timing,
)
from riverhog_core.write_segments import WriteSegmentPlan, iter_write_segments

_LOG = logging.getLogger(__name__)
_SORT_FIELDS = closed_literal_values(ArchiveCopyJobSort)
_SORT_ORDERS = closed_literal_values(SortOrder)
_COPY_OBJECT_KINDS = frozenset(
    {
        "pack",
        "segment",
        "volume-metadata",
        "volume-terminal",
        "provenance-root",
        "provenance-volume-metadata",
        "provenance-terminal",
        "provenance-bindings",
        "provenance-journal-segment",
        "manifest",
        "recovery-descriptor",
    }
)
_COPY_OBJECT_BATCH_MAX = 32


@dataclass(frozen=True, slots=True)
class _CopiedObject:
    object_id: str
    object_path: str
    revision: str | None
    completed_at: str
    archive_parts_json: str | None = None
    retrieval_cache: RetrievalCacheReceipt | None = None


@dataclass(frozen=True, slots=True)
class _CopiedPart:
    receipt: WriteSegmentReceipt
    timing: TransferTiming


class _ArchivePartReservation:
    """Hold one buffered archive part against the shared byte budget."""

    def __init__(
        self,
        resources: ArchiveTransferResources,
        *,
        stored_bytes: int,
        consumers: int,
    ) -> None:
        self._resources = resources
        self._stored_bytes = stored_bytes
        self._remaining = consumers
        self._lock = threading.Lock()

    def release(self, count: int = 1) -> None:
        if count < 1:
            return
        release = False
        with self._lock:
            if count > self._remaining:
                raise RuntimeError("archive-part buffer reservation released too many times")
            self._remaining -= count
            release = self._remaining == 0
        if release:
            self._resources.upload_bytes.release(self._stored_bytes)


class SqlAlchemyArchiveCopyJobService:
    def __init__(
        self,
        config: RuntimeConfig,
        archive_stores: ArchiveStoreRegistry,
        *,
        retrieval_cache: RetrievalCache | None = None,
        session_factory: SessionFactory | None = None,
        throughput_tuning: ArchiveThroughputTuning | None = None,
        transfer_resources: ArchiveTransferResources | None = None,
    ) -> None:
        self._config = config
        self._archive_stores = archive_stores
        self._retrieval_cache = retrieval_cache
        self._session_factory = session_factory or make_session_factory(config.database_url)
        self._throughput = throughput_tuning or config.throughput_tuning
        self._resources = transfer_resources or ArchiveTransferResources.from_tuning(
            self._throughput
        )
        self._lifecycle_events = SqlAlchemyLifecycleEventService(
            config,
            session_factory=self._session_factory,
        )

    def requeue_interrupted_jobs_for_startup(self, *, limit: int = 100) -> int:
        if limit < 1:
            return 0
        current_text = format_utc_timestamp(utc_now())
        with session_scope(self._session_factory) as session:
            jobs = session.scalars(
                select(ArchiveCopyJobRecord)
                .where(ArchiveCopyJobRecord.state.in_(("checking", "copying")))
                .order_by(ArchiveCopyJobRecord.requested_at)
                .limit(limit)
                .with_for_update()
            ).all()
            for job in jobs:
                job.state = "requested"
                job.next_attempt_at = current_text
            pending_cancellations = session.execute(
                select(
                    ArchiveCopyJobRecord.collection_id,
                    ArchiveCopyJobRecord.destination_store,
                )
                .where(
                    ArchiveCopyJobRecord.state == "canceling",
                )
                .limit(limit)
            ).all()
        for collection_id, destination_store in pending_cancellations:
            self._cleanup_canceled_destination(
                collection_id=collection_id,
                destination_store=str(destination_store),
            )
        return len(jobs) + len(pending_cancellations)

    def create_or_resume(
        self,
        collection_id: int,
        *,
        destination_store: str,
        source_store: str | None = None,
        use_cache: bool | None = None,
        initiator: Principal,
        event_context: dict[str, object] | None = None,
    ) -> dict[str, object]:
        normalized_collection_id = _normalize_collection_id(collection_id)
        destination = self._configured_store(destination_store)
        source = self._configured_store(source_store) if source_store is not None else None
        if source == destination:
            raise BadRequest("archive copy source and destination stores must differ")
        current_text = format_utc_timestamp(utc_now())
        normalized_context_json = event_context_json(event_context)
        with session_scope(self._session_factory) as session:
            require_collection_archive_idle(session, normalized_collection_id)
            collection = session.get(CollectionRecord, normalized_collection_id)
            if collection is None or not collection.is_published:
                raise NotFound(f"collection not found: {normalized_collection_id}")
            existing = session.get(
                CollectionArchiveCopyRecord,
                (normalized_collection_id, destination),
            )
            job = session.get(ArchiveCopyJobRecord, (normalized_collection_id, destination))
            if existing is not None and archive_copy_is_complete(existing):
                if job is None or job.state != "completed":
                    raise Conflict("destination already has an uploaded archive copy")
                if use_cache is not None and job.use_cache != use_cache:
                    raise Conflict("archive copy job cache choice changed")
                if source is not None and job.source_store != source:
                    raise Conflict("archive copy job source choice changed")
                return _job_payload(job)
            resolved_cache = resolve_use_cache(
                requested=(job.use_cache if use_cache is None and job is not None else use_cache),
                store_name=destination,
                config=self._config,
                archive_stores=self._archive_stores,
                retrieval_cache=self._retrieval_cache,
            )
            if (
                job is not None
                and job.state in ARCHIVE_COPY_JOB_TRANSFER_STATES
                and job.use_cache != resolved_cache
            ):
                raise Conflict("archive copy job cache choice changed")
            if (
                job is not None
                and job.state in ARCHIVE_COPY_JOB_TRANSFER_STATES
                and source is not None
                and job.source_store != source
            ):
                raise Conflict("archive copy job source choice changed")
            if job is not None and job.state == "failed" and job.use_cache != resolved_cache:
                continuation = session.scalar(
                    select(ArchiveCopyObjectUploadRecord.object_id)
                    .where(
                        ArchiveCopyObjectUploadRecord.collection_id == normalized_collection_id,
                        ArchiveCopyObjectUploadRecord.destination_store == destination,
                        ArchiveCopyObjectUploadRecord.write_token.is_not(None),
                    )
                    .limit(1)
                )
                if continuation is not None:
                    raise Conflict(
                        "archive copy job cache choice changed with retained continuation"
                    )
            source_copy = _select_source_copy(
                collection,
                config=self._config,
                destination_store=destination,
                source_store=source,
                archive_stores=self._archive_stores,
            )
            self._archive_stores.require_incarnation(source_copy.store, source_copy.incarnation_id)
            self._preflight_source_parts(
                session,
                collection_id=normalized_collection_id,
                source_store=source_copy.store,
                destination_store=destination,
                use_cache=resolved_cache,
            )
            if job is None:
                job = self._create_job_in_session(
                    session,
                    collection_id=normalized_collection_id,
                    source_store=source_copy.store,
                    destination_store=destination,
                    use_cache=resolved_cache,
                    initiator=initiator,
                    event_context_json=normalized_context_json,
                    requested_at=current_text,
                )
            elif job.state not in ARCHIVE_COPY_JOB_TRANSFER_STATES:
                if job.state == "canceling":
                    raise Conflict("archive copy cancellation cleanup is still in progress")
                job.source_store = source_copy.store
                job.source_incarnation_id = source_copy.incarnation_id
                job.destination_incarnation_id = self._archive_stores.incarnation_id(destination)
                job.use_cache = resolved_cache
                job.initiated_by_app = initiator.id
                job.initiated_by_key_id = initiator.key_id
                job.event_context_json = normalized_context_json
                job.state = "requested"
                job.requested_at = current_text
                job.next_attempt_at = current_text
                job.finished_at = None
                job.failure = None
                job.read_requested_at = None
                job.ready_at = None
                job.expires_at = None
                job.batch_start_order = None
                job.batch_end_order = None
                job.destination_discarded_at = None
                self._emit(job, type="archive_copy_job.requested", session=session)
            return _job_payload(job)

    def _create_job_in_session(
        self,
        session: Session,
        *,
        collection_id: int,
        source_store: str,
        destination_store: str,
        use_cache: bool,
        initiator: Principal,
        event_context_json: str | None,
        requested_at: str,
    ) -> ArchiveCopyJobRecord:
        destination = self._archive_stores.require(destination_store).store
        job = ArchiveCopyJobRecord(
            collection_id=collection_id,
            source_store=source_store,
            source_incarnation_id=_required_copy(
                session, collection_id, source_store
            ).incarnation_id,
            destination_store=destination_store,
            destination_incarnation_id=self._archive_stores.incarnation_id(destination_store),
            destination_storage_prefix=destination.new_collection_archive_storage_prefix(),
            use_cache=use_cache,
            initiated_by_app=initiator.id,
            initiated_by_key_id=initiator.key_id,
            event_context_json=event_context_json,
            state="requested",
            requested_at=requested_at,
            next_attempt_at=requested_at,
        )
        session.add(job)
        session.flush()
        self._emit(job, type="archive_copy_job.requested", session=session)
        return job

    def cancel(
        self,
        collection_id: int,
        *,
        destination_store: str,
        principal: Principal | None = None,
    ) -> dict[str, object]:
        normalized_collection_id = _normalize_collection_id(collection_id)
        destination = self._configured_store(destination_store)
        copying = False
        with session_scope(self._session_factory) as session:
            job = session.scalar(
                select(ArchiveCopyJobRecord)
                .where(
                    ArchiveCopyJobRecord.collection_id == normalized_collection_id,
                    ArchiveCopyJobRecord.destination_store == destination,
                    collection_access_filter(
                        ArchiveCopyJobRecord.collection_id,
                        principal,
                        ARCHIVES_MANAGE,
                    ),
                )
                .with_for_update()
            )
            if job is None:
                raise NotFound(
                    f"archive copy job not found: {normalized_collection_id} to {destination}"
                )
            if job.state == "canceled":
                return _job_payload(job)
            if job.state == "canceling":
                return _job_payload(job)
            if job.state not in ARCHIVE_COPY_JOB_TRANSFER_STATES:
                raise InvalidState(f"archive copy cannot be canceled in state {job.state}")
            copying = job.state in {"checking", "copying"}
            job.state = "canceling"
            job.next_attempt_at = None
            job.failure = None
            job.finished_at = None
        if not copying:
            self._cleanup_source_read(
                collection_id=normalized_collection_id,
                destination_store=destination,
            )
            self._cleanup_canceled_destination(
                collection_id=normalized_collection_id,
                destination_store=destination,
            )
        return self.get(
            normalized_collection_id,
            destination_store=destination,
            principal=principal,
        )

    def get(
        self,
        collection_id: int,
        *,
        destination_store: str,
        principal: Principal | None = None,
    ) -> dict[str, object]:
        normalized_collection_id = _normalize_collection_id(collection_id)
        destination = self._configured_store(destination_store)
        with session_scope(self._session_factory) as session:
            job = session.scalar(
                select(ArchiveCopyJobRecord).where(
                    ArchiveCopyJobRecord.collection_id == normalized_collection_id,
                    ArchiveCopyJobRecord.destination_store == destination,
                    collection_access_filter(
                        ArchiveCopyJobRecord.collection_id,
                        principal,
                        ARCHIVES_MANAGE,
                    ),
                )
            )
            if job is None:
                raise NotFound(
                    f"archive copy job not found: {normalized_collection_id} to {destination}"
                )
            return _job_payload(job)

    def get_upload_copy_intents(
        self, collection_id: int, *, principal: Principal
    ) -> dict[str, object]:
        normalized_id = _normalize_collection_id(collection_id)
        with session_scope(self._session_factory) as session:
            require_collection_access(session, principal, ARCHIVES_MANAGE, normalized_id)
            collection = session.get(CollectionRecord, normalized_id)
            assert collection is not None
            intents = session.scalars(
                select(CollectionUploadCopyIntentRecord)
                .where(CollectionUploadCopyIntentRecord.collection_id == normalized_id)
                .order_by(CollectionUploadCopyIntentRecord.destination_store)
            ).all()
            return {
                "collection_id": format_scalar("sequence63", normalized_id),
                "archive_store": collection.creation_archive_store,
                "use_cache": collection.creation_use_cache,
                "copy_to": json.loads(collection.creation_copy_to_json),
                "intents": [
                    {
                        "destination_store": intent.destination_store,
                        "state": intent.state,
                        "failure_code": intent.failure_code,
                        "job_created": intent.job_created,
                        "job_state": (
                            job.state
                            if (
                                job := session.get(
                                    ArchiveCopyJobRecord,
                                    (normalized_id, intent.destination_store),
                                )
                            )
                            is not None
                            and intent.state == "handed_off"
                            else None
                        ),
                    }
                    for intent in intents
                ],
            }

    def list(
        self,
        *,
        page_size: int,
        position: tuple[str | int | bool | bytes | None, ...] | None,
        q: str | None,
        sort: str,
        order: str,
        state: str | None = None,
        principal: Principal | None = None,
    ) -> dict[str, object]:
        validate_page_size(page_size)
        if sort not in _SORT_FIELDS:
            raise BadRequest(f"sort must be one of {', '.join(sorted(_SORT_FIELDS))}")
        if order not in _SORT_ORDERS:
            raise BadRequest("order must be asc or desc")
        query, normalized_state, _, statement, key_columns = _archive_copy_job_list_statement(
            q=q,
            state=state,
            sort=sort,
            order=order,
            principal=principal,
        )
        with read_snapshot(self._session_factory) as session:
            records, next_position = bounded_page(
                list(
                    session.scalars(
                        keyset_statement(
                            statement,
                            columns=key_columns,
                            position=position,
                            order=order,
                            page_size=page_size,
                        )
                    )
                ),
                page_size=page_size,
                position_of=lambda job: _archive_copy_job_list_position(job, sort=sort),
            )
        return {
            "page_size": page_size,
            "_next_position": next_position,
            "sort": sort,
            "order": order,
            "query": query,
            "filters": ({"state": normalized_state} if normalized_state is not None else {}),
            "jobs": [_job_payload(job) for job in records],
        }

    def iter_jobs(
        self,
        *,
        q: str | None,
        sort: str,
        order: str,
        state: str | None = None,
        principal: Principal | None = None,
    ) -> Iterator[dict[str, object]]:
        _, _, _, statement, key_columns = _archive_copy_job_list_statement(
            q=q,
            state=state,
            sort=sort,
            order=order,
            principal=principal,
        )
        direction = desc if order == "desc" else asc
        statement = statement.order_by(*(direction(column) for column in key_columns))
        with read_snapshot(self._session_factory) as session:
            for job in session.scalars(statement.execution_options(yield_per=100)):
                yield _job_payload(job)

    def process_due(self, *, limit: int = 1) -> int:
        if limit < 1:
            return 0
        handoffs = self.process_due_upload_copy_intents(limit=limit)
        remaining = limit - handoffs
        if remaining < 1:
            return handoffs
        current_text = format_utc_timestamp(utc_now())
        with session_scope(self._session_factory) as session:
            jobs = session.execute(
                select(
                    ArchiveCopyJobRecord.collection_id,
                    ArchiveCopyJobRecord.destination_store,
                )
                .where(
                    ArchiveCopyJobRecord.state.in_(("requested", "waiting", "canceling")),
                    or_(
                        ArchiveCopyJobRecord.next_attempt_at.is_(None),
                        ArchiveCopyJobRecord.next_attempt_at <= current_text,
                    ),
                )
                .order_by(
                    ArchiveCopyJobRecord.next_attempt_at,
                    ArchiveCopyJobRecord.requested_at,
                    ArchiveCopyJobRecord.collection_id,
                )
                .limit(remaining)
            ).all()
        for collection_id, destination_store in jobs:
            try:
                self._process_one(
                    collection_id=collection_id,
                    destination_store=str(destination_store),
                )
            except Exception as exc:
                _LOG.exception(
                    "archive copy failed: collection=%s destination=%s",
                    collection_id,
                    destination_store,
                )
                self._record_failure(
                    collection_id=collection_id,
                    destination_store=str(destination_store),
                    exc=exc,
                )
                self._cleanup_source_read(
                    collection_id=collection_id,
                    destination_store=str(destination_store),
                )
                self._cleanup_canceled_destination(
                    collection_id=collection_id,
                    destination_store=str(destination_store),
                )
        return handoffs + len(jobs)

    def process_due_upload_copy_intents(self, *, limit: int = 1) -> int:
        if limit < 1:
            return 0
        now = format_utc_timestamp(utc_now())
        with session_scope(self._session_factory) as session:
            due = session.execute(
                select(
                    CollectionUploadCopyIntentRecord.collection_id,
                    CollectionUploadCopyIntentRecord.destination_store,
                )
                .where(
                    CollectionUploadCopyIntentRecord.state == "pending",
                    CollectionUploadCopyIntentRecord.next_attempt_at <= now,
                )
                .order_by(
                    CollectionUploadCopyIntentRecord.next_attempt_at,
                    CollectionUploadCopyIntentRecord.collection_id,
                    CollectionUploadCopyIntentRecord.destination_store,
                )
                .limit(limit)
            ).all()
        for collection_id, destination_store in due:
            try:
                self._handoff_upload_copy_intent(collection_id, destination_store)
            except Exception:
                _LOG.exception(
                    "upload copy handoff deferred: collection=%s destination=%s",
                    collection_id,
                    destination_store,
                )
                self._defer_upload_copy_intent(collection_id, destination_store)
        return len(due)

    def _handoff_upload_copy_intent(self, collection_id: int, destination_store: str) -> None:
        now = format_utc_timestamp(utc_now())
        with session_scope(self._session_factory) as session:
            intent = session.scalar(
                select(CollectionUploadCopyIntentRecord)
                .where(
                    CollectionUploadCopyIntentRecord.collection_id == collection_id,
                    CollectionUploadCopyIntentRecord.destination_store == destination_store,
                )
                .with_for_update(skip_locked=True)
            )
            if intent is None or intent.state != "pending":
                return
            collection = session.get(CollectionRecord, collection_id, with_for_update=True)
            if collection is None or not collection.is_published:
                _fail_upload_copy_intent(intent, "source_unavailable")
                return
            try:
                self._archive_stores.require_incarnation(
                    intent.source_store, intent.source_incarnation_id
                )
                self._archive_stores.require_incarnation(
                    intent.destination_store, intent.destination_incarnation_id
                )
            except ValueError:
                _fail_upload_copy_intent(intent, "configuration_changed")
                return
            key = session.get(AppKeyRecord, intent.initiated_by_key_id, with_for_update=True)
            if (
                key is None
                or key.app != intent.initiated_by_app
                or key.revoked_at is not None
                or (key.expires_at is not None and key.expires_at <= now)
            ):
                _fail_upload_copy_intent(intent, "authorization_denied")
                return
            grants = session.scalars(
                select(AppKeyAccessGrantRecord)
                .where(AppKeyAccessGrantRecord.key_id == key.id)
                .with_for_update()
            ).all()
            principal = Principal(
                id=key.app,
                key_id=key.id,
                access=frozenset(
                    normalize_access((grant.permission, grant.resource) for grant in grants)
                ),
            )
            try:
                require_collection_access(session, principal, ARCHIVES_MANAGE, collection_id)
            except NotFound:
                _fail_upload_copy_intent(intent, "authorization_denied")
                return
            source_copy = session.get(
                CollectionArchiveCopyRecord, (collection_id, intent.source_store)
            )
            if source_copy is None or not archive_copy_is_complete(source_copy):
                _fail_upload_copy_intent(intent, "source_unavailable")
                return
            try:
                resolved_cache = resolve_use_cache(
                    requested=intent.use_cache,
                    store_name=destination_store,
                    config=self._config,
                    archive_stores=self._archive_stores,
                    retrieval_cache=self._retrieval_cache,
                )
                self._preflight_source_parts(
                    session,
                    collection_id=collection_id,
                    source_store=intent.source_store,
                    destination_store=destination_store,
                    use_cache=resolved_cache,
                )
            except BadRequest:
                _fail_upload_copy_intent(intent, "configuration_changed")
                return
            job = session.get(ArchiveCopyJobRecord, (collection_id, destination_store))
            if job is None:
                destination_copy = session.get(
                    CollectionArchiveCopyRecord, (collection_id, destination_store)
                )
                if destination_copy is not None and archive_copy_is_complete(destination_copy):
                    _fail_upload_copy_intent(intent, "destination_already_present")
                    return
                require_collection_archive_idle(session, collection_id)
                self._create_job_in_session(
                    session,
                    collection_id=collection_id,
                    source_store=intent.source_store,
                    destination_store=destination_store,
                    use_cache=resolved_cache,
                    initiator=principal,
                    event_context_json=intent.event_context_json,
                    requested_at=now,
                )
            else:
                _fail_upload_copy_intent(intent, "destination_job_conflict")
                return
            intent.state = "handed_off"
            intent.next_attempt_at = None
            intent.handed_off_at = now
            intent.job_created = job is None

    def _defer_upload_copy_intent(self, collection_id: int, destination_store: str) -> None:
        with session_scope(self._session_factory) as session:
            intent = session.get(
                CollectionUploadCopyIntentRecord, (collection_id, destination_store)
            )
            if intent is None or intent.state != "pending":
                return
            intent.attempts += 1
            delay_seconds = min(3600, 2 ** min(intent.attempts, 12))
            intent.next_attempt_at = format_utc_timestamp(
                utc_now() + timedelta(seconds=delay_seconds)
            )

    def _preflight_source_parts(
        self,
        session: Session,
        *,
        collection_id: int,
        source_store: str,
        destination_store: str,
        use_cache: bool,
    ) -> None:
        destination = self._archive_stores.require(destination_store)
        constraints = destination.resumable_objects.write_constraints()
        if use_cache:
            if self._retrieval_cache is None:
                raise BadRequest("use_cache requires a configured retrieval cache")
            mirror_constraints = getattr(self._retrieval_cache, "mirror_write_constraints", None)
            if callable(mirror_constraints):
                common = mirror_constraints(constraints)
                if common is None:
                    raise BadRequest(
                        "archive and retrieval cache write constraints are incompatible"
                    )
                constraints = common
        parts = session.scalars(
            select(CollectionArchiveObjectRecord.archive_parts_json).where(
                CollectionArchiveObjectRecord.collection_id == collection_id,
                CollectionArchiveObjectRecord.store == source_store,
                CollectionArchiveObjectRecord.kind.in_(("pack", "segment")),
            )
        )
        for archive_parts_json in parts:
            rows = _part_rows(archive_parts_json)
            try:
                for _ in iter_write_segments(
                    (_part_int(row, "stored_bytes") for row in rows), constraints
                ):
                    pass
            except ValueError as exc:
                raise BadRequest(
                    "authoritative archive parts cannot satisfy destination write constraints"
                ) from exc

    def _process_one(self, *, collection_id: int, destination_store: str) -> None:
        current = utc_now()
        current_text = format_utc_timestamp(current)
        finalize = False
        canceling = False
        source_records: list[CollectionArchiveObjectRecord]
        data_objects: tuple[ArchiveObjectIdentity, ...]
        with session_scope(self._session_factory) as session:
            job = session.scalar(
                select(ArchiveCopyJobRecord)
                .where(
                    ArchiveCopyJobRecord.collection_id == collection_id,
                    ArchiveCopyJobRecord.destination_store == destination_store,
                )
                .with_for_update()
            )
            if job is None:
                return
            if job.state == "canceling":
                canceling = True
            elif job.state not in {"requested", "waiting"}:
                return
            if canceling:
                source_records = []
                data_objects = ()
                source_store_name = job.source_store
                destination_store_name = job.destination_store
                read_requested_at = job.read_requested_at
                ready_at = job.ready_at
                destination_storage_prefix = job.destination_storage_prefix
            else:
                _required_copy(session, collection_id, job.source_store)
                source_store_name = job.source_store
                destination_store_name = job.destination_store
                destination_copy = session.get(
                    CollectionArchiveCopyRecord,
                    (collection_id, destination_store_name),
                )
                if destination_copy is None:
                    destination_copy = CollectionArchiveCopyRecord(
                        collection_id=collection_id,
                        store=destination_store_name,
                        incarnation_id=job.destination_incarnation_id,
                        state="uploading",
                        archive_storage_prefix=job.destination_storage_prefix,
                    )
                    session.add(destination_copy)
                    session.flush()
                elif destination_copy.state not in {"pending", "uploading"}:
                    raise Conflict("archive copy destination changed while transfer was active")
                if job.batch_start_order is None:
                    source_object = aliased(CollectionArchiveObjectRecord)
                    destination_object = aliased(CollectionArchiveObjectRecord)
                    orders = list(
                        session.scalars(
                            select(source_object.object_order)
                            .where(
                                source_object.collection_id == collection_id,
                                source_object.store == source_store_name,
                                source_object.kind.in_(_COPY_OBJECT_KINDS),
                                ~exists(
                                    select(1).where(
                                        destination_object.collection_id == collection_id,
                                        destination_object.store == destination_store_name,
                                        destination_object.object_id == source_object.object_id,
                                    )
                                ),
                            )
                            .order_by(source_object.object_order)
                            .limit(_COPY_OBJECT_BATCH_MAX)
                        )
                    )
                    if not orders:
                        finalize = True
                    else:
                        job.batch_start_order = orders[0]
                        job.batch_end_order = orders[-1]
                if finalize:
                    source_records = []
                else:
                    assert job.batch_start_order is not None
                    assert job.batch_end_order is not None
                    source_records = list(
                        session.scalars(
                            select(CollectionArchiveObjectRecord)
                            .options(selectinload(CollectionArchiveObjectRecord.placements))
                            .where(
                                CollectionArchiveObjectRecord.collection_id == collection_id,
                                CollectionArchiveObjectRecord.store == source_store_name,
                                CollectionArchiveObjectRecord.kind.in_(_COPY_OBJECT_KINDS),
                                CollectionArchiveObjectRecord.object_order >= job.batch_start_order,
                                CollectionArchiveObjectRecord.object_order <= job.batch_end_order,
                            )
                            .order_by(CollectionArchiveObjectRecord.object_order)
                        )
                    )
                    if not source_records or len(source_records) > _COPY_OBJECT_BATCH_MAX:
                        raise Conflict("archive copy object window is invalid")
                data_objects = tuple(_archive_object_identity(record) for record in source_records)
                read_requested_at = job.read_requested_at
                ready_at = job.ready_at
                destination_storage_prefix = job.destination_storage_prefix
                use_cache = job.use_cache
                if not finalize:
                    job.state = "checking"
                    job.next_attempt_at = None

        if canceling:
            self._cleanup_source_read(
                collection_id=collection_id,
                destination_store=destination_store,
            )
            self._cleanup_canceled_destination(
                collection_id=collection_id,
                destination_store=destination_store,
            )
            return

        if finalize:
            self._finalize_completed_copy(
                collection_id=collection_id,
                destination_store=destination_store,
            )
            return

        with session_scope(self._session_factory) as session:
            self._preflight_source_parts(
                session,
                collection_id=collection_id,
                source_store=source_store_name,
                destination_store=destination_store_name,
                use_cache=use_cache,
            )

        source_store = self._archive_stores.require(source_store_name).store
        estimated_ready_at: str | None
        if read_requested_at is None:
            estimated_ready_at = format_utc_timestamp(
                current + self._config.retrieval_estimated_latency
            )
            status = source_store.prepare_archive_objects_read(
                collection_id=collection_id,
                objects=data_objects,
            )
            read_requested_at = current_text
        else:
            estimated_ready_at = ready_at
            status = source_store.get_archive_objects_read_status(
                collection_id=collection_id,
                objects=data_objects,
            )

        canceled = False
        with session_scope(self._session_factory) as session:
            job = session.scalar(
                select(ArchiveCopyJobRecord)
                .where(
                    ArchiveCopyJobRecord.collection_id == collection_id,
                    ArchiveCopyJobRecord.destination_store == destination_store,
                )
                .with_for_update()
            )
            if job is None:
                return
            if job.state not in {"checking", "canceling"}:
                raise Conflict("archive copy changed while checking source availability")
            job.read_requested_at = read_requested_at
            job.ready_at = status.ready_at or job.ready_at or estimated_ready_at
            job.expires_at = status.expires_at or job.expires_at
            if job.state == "canceling":
                canceled = True
            elif status.state == "expired":
                job.state = "requested"
                job.read_requested_at = None
                job.ready_at = None
                job.expires_at = None
                job.next_attempt_at = current_text
                return
            elif status.state != "ready":
                job.state = "waiting"
                job.next_attempt_at = format_utc_timestamp(
                    current + self._config.retrieval_restore_poll_interval
                )
                return
            else:
                job.state = "copying"
                job.next_attempt_at = None

        if canceled:
            self._cleanup_source_read(
                collection_id=collection_id,
                destination_store=destination_store,
            )
            self._cleanup_canceled_destination(
                collection_id=collection_id,
                destination_store=destination_store,
            )
            return

        self._copy_immutable_objects(
            collection_id=collection_id,
            source_store_name=source_store_name,
            destination_store_name=destination_store_name,
            source_records=source_records,
            destination_storage_prefix=destination_storage_prefix,
        )
        source_store.cleanup_archive_objects_read(
            collection_id=collection_id,
            objects=data_objects,
        )
        finalize = False
        with session_scope(self._session_factory) as session:
            job = session.scalar(
                select(ArchiveCopyJobRecord)
                .where(
                    ArchiveCopyJobRecord.collection_id == collection_id,
                    ArchiveCopyJobRecord.destination_store == destination_store,
                )
                .with_for_update()
            )
            if job is None:
                raise Conflict("archive copy job disappeared during transfer")
            if job.state != "copying":
                raise Conflict("archive copy was canceled during transfer")
            source_object = aliased(CollectionArchiveObjectRecord)
            destination_object = aliased(CollectionArchiveObjectRecord)
            remaining = session.scalar(
                select(source_object.object_id)
                .where(
                    source_object.collection_id == collection_id,
                    source_object.store == job.source_store,
                    source_object.kind.in_(_COPY_OBJECT_KINDS),
                    ~exists(
                        select(1).where(
                            destination_object.collection_id == collection_id,
                            destination_object.store == destination_store,
                            destination_object.object_id == source_object.object_id,
                        )
                    ),
                )
                .limit(1)
            )
            job.batch_start_order = None
            job.batch_end_order = None
            job.read_requested_at = None
            job.ready_at = None
            job.expires_at = None
            job.state = "requested"
            job.next_attempt_at = current_text
            finalize = remaining is None
        if finalize:
            self._finalize_completed_copy(
                collection_id=collection_id,
                destination_store=destination_store,
            )

    def _plan_destination_upload(
        self,
        *,
        collection_id: int,
        destination_store: str,
        destination_storage_prefix: str,
        objects: Sequence[CollectionArchiveObjectRecord],
    ) -> None:
        expected = {
            current.object_id: current for current in objects if current.kind in {"pack", "segment"}
        }
        with session_scope(self._session_factory) as session:
            existing = {
                current.object_id: current
                for current in session.scalars(
                    select(ArchiveCopyObjectUploadRecord).where(
                        ArchiveCopyObjectUploadRecord.collection_id == collection_id,
                        ArchiveCopyObjectUploadRecord.destination_store == destination_store,
                    )
                )
            }
            if set(existing) - set(expected):
                raise Conflict("archive copy upload checkpoint does not match its manifest")
            for object_id, current in expected.items():
                record = existing.get(object_id)
                if record is None:
                    session.add(
                        ArchiveCopyObjectUploadRecord(
                            collection_id=collection_id,
                            destination_store=destination_store,
                            object_id=object_id,
                            kind=current.kind,
                            object_path=_destination_object_path(
                                source=current,
                                destination_storage_prefix=destination_storage_prefix,
                            ),
                            plaintext_bytes=current.plaintext_bytes,
                            sha256=current.sha256,
                            expected_stored_bytes=current.stored_bytes,
                        )
                    )
                    continue
                if (
                    record.kind != current.kind
                    or record.plaintext_bytes != current.plaintext_bytes
                    or record.sha256 != current.sha256
                    or record.expected_stored_bytes != current.stored_bytes
                ):
                    raise Conflict("archive copy upload checkpoint does not match its manifest")

    def _copy_immutable_objects(
        self,
        *,
        collection_id: int,
        source_store_name: str,
        destination_store_name: str,
        source_records: Sequence[CollectionArchiveObjectRecord],
        destination_storage_prefix: str,
    ) -> None:
        if len(source_records) > _COPY_OBJECT_BATCH_MAX:
            raise Conflict("archive copy object window exceeds its bounded contract")
        self._plan_destination_upload(
            collection_id=collection_id,
            destination_store=destination_store_name,
            destination_storage_prefix=destination_storage_prefix,
            objects=source_records,
        )
        source_store = self._archive_stores.require(source_store_name).store
        destination = self._archive_stores.require(destination_store_name)
        for record in source_records:
            with session_scope(self._session_factory) as session:
                if (
                    session.get(
                        CollectionArchiveObjectRecord,
                        (collection_id, destination_store_name, record.object_id),
                    )
                    is not None
                ):
                    continue
            self._require_copy_active(collection_id, destination_store_name)
            identity = _archive_object_identity(record)
            if record.kind in {"pack", "segment"}:
                result = self._copy_volume(
                    collection_id=collection_id,
                    destination_store_name=destination_store_name,
                    destination_storage_prefix=destination_storage_prefix,
                    source_store=source_store,
                    destination_object_store=self._volume_object_store(
                        store_name=destination_store_name,
                        collection_id=collection_id,
                        object_id=record.object_id,
                    ),
                    source=record,
                    identity=identity,
                )
            else:
                result = self._copy_small_immutable_object(
                    collection_id=collection_id,
                    destination_storage_prefix=destination_storage_prefix,
                    source_store=source_store,
                    destination_store=destination.immutable_objects,
                    source=record,
                    identity=identity,
                )
            self._record_copied_object(
                collection_id=collection_id,
                destination_store=destination_store_name,
                source=record,
                receipt=result,
            )

    def _copy_volume(
        self,
        *,
        collection_id: int,
        destination_store_name: str,
        destination_storage_prefix: str,
        source_store: ArchiveStore,
        destination_object_store: ArchiveResumableObjectStore,
        source: CollectionArchiveObjectRecord,
        identity: ArchiveObjectIdentity,
    ) -> _CopiedObject:
        destination_path = _destination_object_path(
            source=source,
            destination_storage_prefix=destination_storage_prefix,
        )
        part_rows = _part_rows(source.archive_parts_json)
        metadata = _volume_metadata(source)
        content_type = (
            PACK_VOLUME_CONTENT_TYPE if source.kind == "pack" else RAW_VOLUME_CONTENT_TYPE
        )
        completed = destination_object_store.find_completed_write(
            object_path=destination_path,
            expected_bytes=source.stored_bytes,
            expected_content_type=content_type,
            expected_metadata=metadata,
        )
        if completed is not None:
            if completed.bytes != source.stored_bytes:
                raise Conflict("completed archive-copy volume has a different byte count")
            return _CopiedObject(
                source.object_id,
                completed.object_path,
                completed.revision,
                completed.completed_at,
                source.archive_parts_json,
                completed.retrieval_cache,
            )

        with session_scope(self._session_factory) as session:
            checkpoint = session.get(
                ArchiveCopyObjectUploadRecord,
                (collection_id, destination_store_name, source.object_id),
            )
            if checkpoint is None:
                raise Conflict("archive copy upload checkpoint disappeared")
            write_session = (
                WriteSession(destination_path, checkpoint.write_token, source.stored_bytes)
                if checkpoint.write_token
                else None
            )
        if write_session is None:
            write_session = destination_object_store.begin_write(
                object_path=destination_path,
                expected_bytes=source.stored_bytes,
                content_type=content_type,
                metadata=metadata,
            )
            with session_scope(self._session_factory) as session:
                checkpoint = session.get(
                    ArchiveCopyObjectUploadRecord,
                    (collection_id, destination_store_name, source.object_id),
                )
                if checkpoint is None:
                    raise Conflict("archive copy upload checkpoint disappeared")
                checkpoint.write_token = write_session.write_token
                checkpoint.object_path = destination_path

        constraints = destination_object_store.write_constraints()
        total_segments = sum(
            1
            for _ in iter_write_segments(
                (_part_int(current, "stored_bytes") for current in part_rows),
                constraints,
            )
        )
        self._copy_volume_parts(
            collection_id=collection_id,
            destination_store_name=destination_store_name,
            source_store=source_store,
            destination_object_store=destination_object_store,
            source=source,
            identity=identity,
            write_session=write_session,
            part_rows=part_rows,
            segment_plans=iter_write_segments(
                (_part_int(current, "stored_bytes") for current in part_rows),
                constraints,
            ),
            total_segments=total_segments,
        )
        completed = destination_object_store.complete_write(
            session=write_session,
            completion=self._copy_completion_precondition(
                destination_object_store,
                session=write_session,
                expected=iter_write_segments(
                    (_part_int(current, "stored_bytes") for current in part_rows),
                    constraints,
                ),
            ),
            expected_bytes=source.stored_bytes,
            expected_content_type=content_type,
            expected_metadata=metadata,
        )
        return _CopiedObject(
            source.object_id,
            completed.object_path,
            completed.revision,
            completed.completed_at,
            source.archive_parts_json,
            completed.retrieval_cache,
        )

    def _volume_object_store(
        self,
        *,
        store_name: str,
        collection_id: int,
        object_id: str,
    ) -> ArchiveResumableObjectStore:
        binding = self._archive_stores.require(store_name)
        archive = binding.resumable_objects
        with session_scope(self._session_factory) as session:
            use_cache = session.scalar(
                select(ArchiveCopyJobRecord.use_cache).where(
                    ArchiveCopyJobRecord.collection_id == collection_id,
                    ArchiveCopyJobRecord.destination_store == store_name,
                )
            )
        if use_cache and self._retrieval_cache is None:
            raise Conflict("accepted retrieval cache is no longer configured")
        if not use_cache:
            return archive
        assert self._retrieval_cache is not None
        return MirroredArchiveResumableObjectStore(
            archive=archive,
            cache=self._retrieval_cache,
            source_store=store_name,
            collection_id=collection_id,
            object_id=object_id,
            owner=f"archive-copy:{collection_id}:{store_name}:{object_id}",
        )

    def _copy_volume_parts(
        self,
        *,
        collection_id: int,
        destination_store_name: str,
        source_store: ArchiveStore,
        destination_object_store: ArchiveResumableObjectStore,
        source: CollectionArchiveObjectRecord,
        identity: ArchiveObjectIdentity,
        write_session: WriteSession,
        part_rows: Sequence[dict[str, object]],
        segment_plans: Iterable[WriteSegmentPlan],
        total_segments: int,
    ) -> None:
        worker_count = self._throughput.write_concurrency
        window = worker_count * 2
        source_chunks = iter(
            source_store.iter_stored_archive_object(
                collection_id=collection_id,
                object=identity,
            )
        )
        source_buffer = bytearray()
        pending: dict[Future[_CopiedPart], WriteSegmentPlan] = {}
        uploaded_segments = 0
        uploaded_bytes = 0
        exhausted = False
        retrieval_wait_seconds = 0.0
        retrieval_wait_recorded = False

        plan_iterator = iter(segment_plans)
        next_plan = next(plan_iterator, None)

        def segment_inputs() -> Iterator[
            tuple[WriteSegmentPlan, bytes, float, float, float, _ArchivePartReservation]
        ]:
            nonlocal next_plan, retrieval_wait_recorded
            for row in part_rows:
                archive_part_number = _part_int(row, "number")
                archive_part_bytes = _part_int(row, "stored_bytes")
                plans: list[WriteSegmentPlan] = []
                while (
                    next_plan is not None and next_plan.archive_part_number == archive_part_number
                ):
                    plans.append(next_plan)
                    next_plan = next(plan_iterator, None)
                if not plans:
                    raise Conflict("archive part has no destination write segments")
                byte_wait_seconds = self._resources.upload_bytes.acquire(archive_part_bytes)
                reservation = _ArchivePartReservation(
                    self._resources,
                    stored_bytes=archive_part_bytes,
                    consumers=len(plans),
                )
                transferred = 0
                try:
                    source_started = time.perf_counter()
                    while len(source_buffer) < archive_part_bytes:
                        try:
                            chunk = bytes(next(source_chunks))
                        except StopIteration as exc:
                            raise Conflict(
                                "source archive volume ended before its part receipts"
                            ) from exc
                        if chunk:
                            source_buffer.extend(chunk)
                    source_seconds = time.perf_counter() - source_started
                    integrity_started = time.perf_counter()
                    content_view = memoryview(source_buffer)[:archive_part_bytes]
                    try:
                        stored_sha256 = hashlib.sha256(content_view).hexdigest()
                    finally:
                        content_view.release()
                    if stored_sha256 != str(row["stored_sha256"]):
                        raise Conflict(
                            f"source archive volume part {archive_part_number} failed verification"
                        )
                    integrity_seconds = time.perf_counter() - integrity_started
                    offset = 0
                    for plan in plans:
                        if plan.archive_part_offset != offset:
                            raise Conflict(
                                "destination write segments do not cover an archive part"
                            )
                        content = bytes(source_buffer[: plan.stored_bytes])
                        del source_buffer[: plan.stored_bytes]
                        offset += plan.stored_bytes
                        transferred += 1
                        queue_wait_seconds = byte_wait_seconds if transferred == 1 else 0.0
                        if not retrieval_wait_recorded:
                            queue_wait_seconds += retrieval_wait_seconds
                            retrieval_wait_recorded = True
                        yield (
                            plan,
                            content,
                            queue_wait_seconds,
                            source_seconds if transferred == 1 else 0.0,
                            integrity_seconds if transferred == 1 else 0.0,
                            reservation,
                        )
                    if offset != archive_part_bytes:
                        raise Conflict("destination write segments do not cover an archive part")
                finally:
                    reservation.release(len(plans) - transferred)
            if source_buffer or any(bytes(chunk) for chunk in source_chunks):
                raise Conflict("source archive volume has bytes beyond its part receipts")
            if next_plan is not None:
                raise Conflict("destination write segments exceed the archive parts")

        inputs = iter(segment_inputs())

        def write_segment(
            plan: WriteSegmentPlan,
            content: bytes,
            queue_wait_seconds: float,
            source_seconds: float,
            integrity_seconds: float,
        ) -> _CopiedPart:
            with self._resources.upload_requests.reserve() as request_wait_seconds:
                queue_wait_seconds += request_wait_seconds
                remote_started = time.perf_counter()
                receipt = destination_object_store.write_segment(
                    session=write_session,
                    number=plan.number,
                    content=content,
                )
                remote_seconds = time.perf_counter() - remote_started
            if receipt.bytes != len(content):
                raise Conflict("archive copy write-segment byte count changed")
            receipt = replace(receipt, sha256=hashlib.sha256(content).hexdigest())
            return _CopiedPart(
                receipt=receipt,
                timing=TransferTiming(
                    operation="archive_copy_segment",
                    identity=f"{source.object_id}:{plan.number}",
                    plaintext_bytes=len(content),
                    stored_bytes=len(content),
                    queue_wait_seconds=queue_wait_seconds,
                    source_seconds=source_seconds,
                    integrity_seconds=integrity_seconds,
                    crypto_seconds=0.0,
                    processing_seconds=0.0,
                    remote_seconds=remote_seconds,
                    checkpoint_seconds=0.0,
                    elapsed_seconds=(
                        queue_wait_seconds + source_seconds + integrity_seconds + remote_seconds
                    ),
                ),
            )

        with self._resources.retrieval_requests.reserve() as current_retrieval_wait_seconds:
            retrieval_wait_seconds = current_retrieval_wait_seconds
            with ThreadPoolExecutor(
                max_workers=worker_count,
                thread_name_prefix="riverhog-archive-copy-segment",
            ) as executor:

                def fill() -> None:
                    nonlocal exhausted, retrieval_wait_recorded
                    while not exhausted and len(pending) < window:
                        try:
                            (
                                plan,
                                content,
                                queue_wait_seconds,
                                source_seconds,
                                integrity_seconds,
                                reservation,
                            ) = next(inputs)
                        except StopIteration:
                            exhausted = True
                            return
                        try:
                            future = executor.submit(
                                write_segment,
                                plan,
                                content,
                                queue_wait_seconds,
                                source_seconds,
                                integrity_seconds,
                            )
                        except BaseException:
                            reservation.release()
                            raise

                        def release_buffer(
                            _future: Future[_CopiedPart],
                            current: _ArchivePartReservation = reservation,
                        ) -> None:
                            current.release()

                        future.add_done_callback(release_buffer)
                        pending[future] = plan

                fill()
                try:
                    while pending:
                        done, _ = wait(tuple(pending), return_when=FIRST_COMPLETED)
                        completed_parts: list[_CopiedPart] = []
                        for future in done:
                            pending.pop(future)
                            result = future.result()
                            uploaded_segments += 1
                            uploaded_bytes += result.receipt.bytes
                            completed_parts.append(result)
                        checkpoint_started = time.perf_counter()
                        self._record_copy_progress(
                            collection_id=collection_id,
                            destination_store=destination_store_name,
                            object_id=source.object_id,
                            uploaded_bytes=uploaded_bytes,
                            uploaded_segments=uploaded_segments,
                            total_segments=total_segments,
                        )
                        checkpoint_seconds = time.perf_counter() - checkpoint_started
                        checkpoint_share = checkpoint_seconds / len(completed_parts)
                        for result in completed_parts:
                            log_transfer_timing(
                                replace(
                                    result.timing,
                                    checkpoint_seconds=checkpoint_share,
                                    elapsed_seconds=(
                                        result.timing.elapsed_seconds + checkpoint_share
                                    ),
                                )
                            )
                        fill()
                except BaseException:
                    for future in pending:
                        future.cancel()
                    raise
        if uploaded_segments != total_segments or uploaded_bytes != source.stored_bytes:
            raise Conflict("archive copy write-segment receipts do not cover the volume")

    @staticmethod
    def _copy_completion_precondition(
        store: ArchiveResumableObjectStore,
        *,
        session: WriteSession,
        expected: Iterable[WriteSegmentPlan],
    ) -> WriteCompletionPrecondition:
        expected_iterator = iter(expected)
        cursor = WriteSegmentCursor()
        completion: WriteCompletionPrecondition | None = None
        while True:
            page = store.list_segments(session=session, cursor=cursor)
            for receipt in page.segments:
                plan = next(expected_iterator, None)
                if plan is None or (receipt.number, receipt.bytes) != (
                    plan.number,
                    plan.stored_bytes,
                ):
                    raise Conflict("archive copy write segments do not match the byte plan")
            if page.next_cursor is None:
                completion = page.completion
                break
            cursor = page.next_cursor
        if next(expected_iterator, None) is not None or completion is None:
            raise Conflict("archive copy write segments are incomplete")
        return completion

    def _copy_small_immutable_object(
        self,
        *,
        collection_id: int,
        destination_storage_prefix: str,
        source_store: ArchiveStore,
        destination_store: ImmutableArchiveObjectStore,
        source: CollectionArchiveObjectRecord,
        identity: ArchiveObjectIdentity,
    ) -> _CopiedObject:
        started = time.perf_counter()
        reserve_bytes = min(max(1, source.stored_bytes), self._resources.upload_bytes.capacity)
        with self._resources.upload_bytes.reserve(reserve_bytes) as byte_wait:
            with self._resources.retrieval_requests.reserve() as retrieval_wait:
                source_started = time.perf_counter()
                content = b"".join(
                    source_store.iter_stored_archive_object(
                        collection_id=collection_id,
                        object=identity,
                    )
                )
                source_seconds = time.perf_counter() - source_started
            if len(content) != source.stored_bytes:
                raise Conflict("source archive artifact byte count changed")
            integrity_started = time.perf_counter()
            stored_sha256 = hashlib.sha256(content).hexdigest()
            integrity_seconds = time.perf_counter() - integrity_started
            if source.stored_sha256 and stored_sha256 != source.stored_sha256:
                raise Conflict("source archive artifact sha256 changed")
            destination_path = _destination_object_path(
                source=source,
                destination_storage_prefix=destination_storage_prefix,
            )
            content_type = {
                "manifest": "application/vnd.riverhog.collection-manifest+age",
                "recovery-descriptor": "application/vnd.riverhog.recovery-descriptor+json",
                "volume-metadata": "application/vnd.riverhog.collection-volume+age",
                "volume-terminal": "application/vnd.riverhog.collection-volume+age",
                "provenance-root": "application/vnd.riverhog-provenance-root.v1.age",
                "provenance-volume-metadata": ("application/vnd.riverhog-provenance-volume.v1.age"),
                "provenance-terminal": ("application/vnd.riverhog-provenance-terminal.v1.age"),
                "provenance-bindings": ("application/vnd.riverhog-provenance-bindings.v1.age"),
                "provenance-journal-segment": (
                    "application/vnd.riverhog-provenance-journal-segment.v1.age"
                ),
            }[source.kind]
            with self._resources.upload_requests.reserve() as upload_wait:
                remote_started = time.perf_counter()
                receipt = destination_store.put_immutable_object(
                    object_path=destination_path,
                    content=content,
                    content_type=content_type,
                    required_identity_assertions={
                        "riverhog-format": archive_object_storage_format(source.kind),
                        "riverhog-plaintext-bytes": str(source.plaintext_bytes),
                        "riverhog-plaintext-sha256": source.sha256 or "",
                    },
                    placement_policy="immediate_default",
                )
                remote_seconds = time.perf_counter() - remote_started
        if receipt.stored_sha256 != stored_sha256:
            raise Conflict("destination archive artifact sha256 changed")
        log_transfer_timing(
            TransferTiming(
                operation="archive_copy_object",
                identity=source.object_id,
                plaintext_bytes=source.plaintext_bytes,
                stored_bytes=source.stored_bytes,
                queue_wait_seconds=byte_wait + retrieval_wait + upload_wait,
                source_seconds=source_seconds,
                integrity_seconds=integrity_seconds,
                crypto_seconds=0.0,
                processing_seconds=0.0,
                remote_seconds=remote_seconds,
                checkpoint_seconds=0.0,
                elapsed_seconds=time.perf_counter() - started,
            )
        )
        return _CopiedObject(
            source.object_id,
            receipt.object_path,
            receipt.revision,
            receipt.completed_at,
        )

    def _record_copy_progress(
        self,
        *,
        collection_id: int,
        destination_store: str,
        object_id: str,
        uploaded_bytes: int,
        uploaded_segments: int,
        total_segments: int,
    ) -> None:
        with session_scope(self._session_factory) as session:
            record = session.get(
                ArchiveCopyObjectUploadRecord,
                (collection_id, destination_store, object_id),
            )
            if record is None:
                raise Conflict("archive copy upload checkpoint disappeared")
            record.uploaded_bytes = uploaded_bytes
            record.uploaded_segments = uploaded_segments
            record.total_segments = total_segments

    def _require_copy_active(self, collection_id: int, destination_store: str) -> None:
        with session_scope(self._session_factory) as session:
            job = session.get(ArchiveCopyJobRecord, (collection_id, destination_store))
            if job is None or job.state != "copying":
                raise Conflict("archive copy was canceled during transfer")

    def _record_copied_object(
        self,
        *,
        collection_id: int,
        destination_store: str,
        source: CollectionArchiveObjectRecord,
        receipt: _CopiedObject,
    ) -> None:
        with session_scope(self._session_factory) as session:
            existing = session.get(
                CollectionArchiveObjectRecord,
                (collection_id, destination_store, source.object_id),
            )
            if existing is not None:
                if _archive_object_identity(existing) != ArchiveObjectIdentity(
                    object_id=receipt.object_id,
                    kind=source.kind,
                    object_path=receipt.object_path,
                    plaintext_bytes=source.plaintext_bytes,
                    stored_bytes=source.stored_bytes,
                    sha256=source.sha256,
                    stored_sha256=source.stored_sha256,
                    revision=receipt.revision,
                ):
                    raise Conflict("archive copy destination object identity changed")
                return
            copied_record = CollectionArchiveObjectRecord(
                collection_id=collection_id,
                store=destination_store,
                object_id=source.object_id,
                object_order=source.object_order,
                kind=source.kind,
                object_path=receipt.object_path,
                plaintext_bytes=source.plaintext_bytes,
                stored_bytes=source.stored_bytes,
                sha256=source.sha256,
                stored_sha256=source.stored_sha256,
                revision=receipt.revision,
                age_state_json=source.age_state_json,
                archive_parts_json=receipt.archive_parts_json,
                plan_sha256=source.plan_sha256,
                index_sha256=source.index_sha256,
                uploaded_at=receipt.completed_at,
                verified_at=receipt.completed_at,
            )
            for placement in source.placements:
                copied_record.placements.append(
                    CollectionArchiveFileObjectRecord(
                        collection_id=collection_id,
                        store=destination_store,
                        path=placement.path,
                        sequence=placement.sequence,
                        object_id=source.object_id,
                        file_offset=placement.file_offset,
                        object_offset=placement.object_offset,
                        bytes=placement.bytes,
                        member=placement.member,
                    )
                )
            session.add(copied_record)
            session.flush()
            if receipt.retrieval_cache is not None:
                cache_receipt = receipt.retrieval_cache
                if cache_receipt.stored_bytes != copied_record.stored_bytes or (
                    cache_receipt.stored_sha256 is not None
                    and len(cache_receipt.stored_sha256) != 64
                ):
                    raise RuntimeError(
                        "retrieval cache receipt does not match its copied archive volume"
                    )
                register_cache_ready(
                    session,
                    source_store=destination_store,
                    collection_id=collection_id,
                    object_id=copied_record.object_id,
                    receipt=cache_receipt,
                )
                session.flush()
                session.merge(
                    RetrievalCacheLeaseRecord(
                        owner="new-archive",
                        source_store=destination_store,
                        collection_id=collection_id,
                        object_id=copied_record.object_id,
                        expires_at=format_utc_timestamp(
                            utc_now() + self._config.retrieval_cache_new_archive_lease
                        ),
                    )
                )
            checkpoint = session.get(
                ArchiveCopyObjectUploadRecord,
                (collection_id, destination_store, source.object_id),
            )
            if checkpoint is not None:
                session.delete(checkpoint)

    def _finalize_completed_copy(
        self,
        *,
        collection_id: int,
        destination_store: str,
    ) -> None:
        with session_scope(self._session_factory) as session:
            job = session.scalar(
                select(ArchiveCopyJobRecord)
                .where(
                    ArchiveCopyJobRecord.collection_id == collection_id,
                    ArchiveCopyJobRecord.destination_store == destination_store,
                )
                .with_for_update()
            )
            if job is None or job.state != "requested":
                return
            _required_copy(session, collection_id, job.source_store)
            destination = session.get(
                CollectionArchiveCopyRecord,
                (collection_id, destination_store),
            )
            if destination is None or destination.state not in {"pending", "uploading"}:
                raise Conflict("archive copy destination is not ready for finalization")
            source_count = int(
                session.scalar(
                    select(func.count())
                    .select_from(CollectionArchiveObjectRecord)
                    .where(
                        CollectionArchiveObjectRecord.collection_id == collection_id,
                        CollectionArchiveObjectRecord.store == job.source_store,
                        CollectionArchiveObjectRecord.kind.in_(_COPY_OBJECT_KINDS),
                    )
                )
                or 0
            )
            destination_count = int(
                session.scalar(
                    select(func.count())
                    .select_from(CollectionArchiveObjectRecord)
                    .where(
                        CollectionArchiveObjectRecord.collection_id == collection_id,
                        CollectionArchiveObjectRecord.store == destination_store,
                        CollectionArchiveObjectRecord.kind.in_(_COPY_OBJECT_KINDS),
                        CollectionArchiveObjectRecord.verified_at.is_not(None),
                    )
                )
                or 0
            )
            if source_count == 0 or destination_count != source_count:
                raise Conflict("archive copy destination is not exactly complete")
            required_kinds = {
                "manifest",
                "recovery-descriptor",
                "volume-metadata",
                "volume-terminal",
            }
            present_kinds = set(
                session.scalars(
                    select(CollectionArchiveObjectRecord.kind)
                    .where(
                        CollectionArchiveObjectRecord.collection_id == collection_id,
                        CollectionArchiveObjectRecord.store == destination_store,
                        CollectionArchiveObjectRecord.kind.in_(required_kinds),
                    )
                    .distinct()
                )
            )
            if present_kinds != required_kinds:
                raise Conflict("archive copy result has no complete recoverable root")
            if not session.scalar(
                select(
                    exists().where(
                        CollectionArchiveObjectRecord.collection_id == collection_id,
                        CollectionArchiveObjectRecord.store == destination_store,
                        CollectionArchiveObjectRecord.kind.in_(("pack", "segment")),
                    )
                )
            ):
                raise Conflict("archive copy result has no archive volume")
            collection = session.get(CollectionRecord, collection_id)
            assert collection is not None
            if collection.provenance_mode != "omitted":
                provenance_kinds = set(
                    session.scalars(
                        select(CollectionArchiveObjectRecord.kind)
                        .where(
                            CollectionArchiveObjectRecord.collection_id == collection_id,
                            CollectionArchiveObjectRecord.store == destination_store,
                            CollectionArchiveObjectRecord.kind.in_(
                                (
                                    "provenance-root",
                                    "provenance-volume-metadata",
                                    "provenance-terminal",
                                )
                            ),
                        )
                        .distinct()
                    )
                )
                if provenance_kinds != {
                    "provenance-root",
                    "provenance-volume-metadata",
                    "provenance-terminal",
                }:
                    raise Conflict("archive copy provenance authority is incomplete")
            uploaded_at = session.scalar(
                select(func.max(CollectionArchiveObjectRecord.uploaded_at)).where(
                    CollectionArchiveObjectRecord.collection_id == collection_id,
                    CollectionArchiveObjectRecord.store == destination_store,
                )
            )
            if uploaded_at is None:
                raise Conflict("archive copy has no completed object timestamp")
            destination.state = "uploaded"
            destination.archive_storage_prefix = job.destination_storage_prefix
            destination.last_uploaded_at = str(uploaded_at)
            destination.last_verified_at = str(uploaded_at)
            destination.failure = None
            ensure_description_publication_for_copy(
                session,
                collection=collection,
                copy=destination,
                now=format_utc_timestamp(utc_now()),
            )
            ensure_tag_publication_for_copy(
                session,
                collection=collection,
                store_name=destination.store,
            )
            job.state = "completed"
            job.finished_at = format_utc_timestamp(utc_now())
            job.next_attempt_at = None
            job.failure = None
            self._emit(job, type="archive_copy_job.completed", terminal=True, session=session)

    def _record_failure(
        self,
        *,
        collection_id: int,
        destination_store: str,
        exc: Exception,
    ) -> None:
        with session_scope(self._session_factory) as session:
            job = session.get(ArchiveCopyJobRecord, (collection_id, destination_store))
            if job is None or job.state in {"canceling", "canceled"}:
                return
            job.state = "failed"
            job.next_attempt_at = None
            job.failure = f"{type(exc).__name__}: {exc}"
            job.finished_at = format_utc_timestamp(utc_now())
            self._emit(
                job,
                type="archive_copy_job.failed",
                terminal=True,
                details={"error": job.failure},
                session=session,
            )

    def _cleanup_source_read(self, *, collection_id: int, destination_store: str) -> None:
        with session_scope(self._session_factory) as session:
            job = session.get(ArchiveCopyJobRecord, (collection_id, destination_store))
            if job is None or job.read_requested_at is None:
                return
            if job.batch_start_order is None or job.batch_end_order is None:
                return
            source_store_name = job.source_store
            data_objects = tuple(
                _archive_object_identity(current)
                for current in session.scalars(
                    select(CollectionArchiveObjectRecord)
                    .where(
                        CollectionArchiveObjectRecord.collection_id == collection_id,
                        CollectionArchiveObjectRecord.store == source_store_name,
                        CollectionArchiveObjectRecord.kind.in_(_COPY_OBJECT_KINDS),
                        CollectionArchiveObjectRecord.object_order >= job.batch_start_order,
                        CollectionArchiveObjectRecord.object_order <= job.batch_end_order,
                    )
                    .order_by(CollectionArchiveObjectRecord.object_order)
                )
            )
            if len(data_objects) > _COPY_OBJECT_BATCH_MAX:
                raise Conflict("archive copy cleanup window exceeds its bounded contract")
        source_store = self._archive_stores.require(source_store_name).store
        try:
            source_store.cleanup_archive_objects_read(
                collection_id=collection_id,
                objects=data_objects,
            )
        except Exception:
            _LOG.exception(
                "failed to clean up archive-copy source read: collection=%s source=%s",
                collection_id,
                source_store_name,
            )
            return
        with session_scope(self._session_factory) as session:
            job = session.get(ArchiveCopyJobRecord, (collection_id, destination_store))
            if job is not None:
                job.read_requested_at = None
                job.ready_at = None
                job.expires_at = None

    def _cleanup_canceled_destination(
        self,
        *,
        collection_id: int,
        destination_store: str,
    ) -> None:
        with session_scope(self._session_factory) as session:
            job = session.get(ArchiveCopyJobRecord, (collection_id, destination_store))
            if job is None or job.state != "canceling":
                return
            destination_storage_prefix = job.destination_storage_prefix
            destination_discarded = job.destination_discarded_at is not None
            active_writes = list(
                session.execute(
                    select(
                        ArchiveCopyObjectUploadRecord.object_id,
                        ArchiveCopyObjectUploadRecord.object_path,
                        ArchiveCopyObjectUploadRecord.write_token,
                        ArchiveCopyObjectUploadRecord.expected_stored_bytes,
                    )
                    .where(
                        ArchiveCopyObjectUploadRecord.collection_id == collection_id,
                        ArchiveCopyObjectUploadRecord.destination_store == destination_store,
                        ArchiveCopyObjectUploadRecord.write_token.is_not(None),
                    )
                    .order_by(ArchiveCopyObjectUploadRecord.object_id)
                    .limit(_COPY_OBJECT_BATCH_MAX + 1)
                )
            )
            if len(active_writes) > _COPY_OBJECT_BATCH_MAX:
                raise RuntimeError("archive copy cancellation exceeded its bounded write window")
        if not destination_discarded:
            try:
                for checkpoint in active_writes:
                    assert checkpoint.write_token is not None
                    self._volume_object_store(
                        store_name=destination_store,
                        collection_id=collection_id,
                        object_id=checkpoint.object_id,
                    ).abort_write(
                        session=WriteSession(
                            checkpoint.object_path,
                            checkpoint.write_token,
                            checkpoint.expected_stored_bytes,
                        )
                    )
                self._archive_stores.require(
                    destination_store
                ).store.discard_collection_archive_upload(
                    archive_storage_prefix=destination_storage_prefix,
                )
            except Exception:
                _LOG.exception(
                    "failed to discard canceled archive copy: collection=%s destination=%s",
                    collection_id,
                    destination_store,
                )
                return
            with session_scope(self._session_factory) as session:
                job = session.get(ArchiveCopyJobRecord, (collection_id, destination_store))
                if job is None or job.state != "canceling":
                    return
                job.destination_discarded_at = format_utc_timestamp(utc_now())
        with session_scope(self._session_factory) as session:
            job = session.get(ArchiveCopyJobRecord, (collection_id, destination_store))
            if job is None or job.state != "canceling":
                return
            destination_object_ids = list(
                session.scalars(
                    select(CollectionArchiveObjectRecord.object_id)
                    .where(
                        CollectionArchiveObjectRecord.collection_id == collection_id,
                        CollectionArchiveObjectRecord.store == destination_store,
                    )
                    .order_by(CollectionArchiveObjectRecord.object_order)
                    .limit(_COPY_OBJECT_BATCH_MAX)
                )
            )
            if destination_object_ids:
                session.execute(
                    delete(CollectionArchiveObjectRecord).where(
                        CollectionArchiveObjectRecord.collection_id == collection_id,
                        CollectionArchiveObjectRecord.store == destination_store,
                        CollectionArchiveObjectRecord.object_id.in_(destination_object_ids),
                    )
                )
                job.next_attempt_at = format_utc_timestamp(utc_now())
                return
            session.execute(
                delete(ArchiveCopyObjectUploadRecord).where(
                    ArchiveCopyObjectUploadRecord.collection_id == collection_id,
                    ArchiveCopyObjectUploadRecord.destination_store == destination_store,
                )
            )
            destination_copy = session.get(
                CollectionArchiveCopyRecord,
                (collection_id, destination_store),
            )
            if destination_copy is not None:
                session.delete(destination_copy)
            job.state = "canceled"
            job.finished_at = format_utc_timestamp(utc_now())
            job.batch_start_order = None
            job.batch_end_order = None
            job.read_requested_at = None
            job.ready_at = None
            job.expires_at = None
            self._emit(job, type="archive_copy_job.canceled", terminal=True, session=session)

    def _configured_store(self, value: str) -> str:
        try:
            return self._config.archive_store(value).name
        except ValueError as exc:
            raise BadRequest(str(exc)) from exc

    def _emit(
        self,
        job: ArchiveCopyJobRecord,
        *,
        type: str,
        terminal: bool = False,
        details: dict[str, object] | None = None,
        session: Session,
    ) -> None:
        self._lifecycle_events.emit_collection(
            type=type,
            collection_id=job.collection_id,
            details={
                "source_store": job.source_store,
                "destination_store": job.destination_store,
                "state": job.state,
                **(details or {}),
            },
            terminal=terminal,
            initiator=Principal(
                id=job.initiated_by_app,
                key_id=job.initiated_by_key_id,
                access=frozenset(),
            ),
            event_context_json=job.event_context_json,
            session=session,
        )


def _select_source_copy(
    collection: CollectionRecord,
    *,
    config: RuntimeConfig,
    destination_store: str,
    source_store: str | None,
    archive_stores: ArchiveStoreRegistry,
) -> CollectionArchiveCopyRecord:
    usable = dict(archive_stores.items())
    candidates = [
        copy
        for copy in collection.archive_copies
        if copy.store != destination_store
        and archive_copy_is_complete(copy)
        and copy.store in usable
        and usable[copy.store].incarnation_id == copy.incarnation_id
    ]
    if source_store is not None:
        candidates = [copy for copy in candidates if copy.store == source_store]
    if not candidates:
        raise InvalidState(f"collection has no uploaded archive copy available: {collection.id}")
    read_rank = {store: index for index, store in enumerate(config.archive_read_order)}
    return min(
        candidates,
        key=lambda copy: (read_rank.get(copy.store, len(read_rank)), copy.store),
    )


def _required_copy(
    session: Session,
    collection_id: int,
    store: str,
) -> CollectionArchiveCopyRecord:
    copy = session.get(CollectionArchiveCopyRecord, (collection_id, store))
    if copy is None or not archive_copy_is_complete(copy):
        raise InvalidState(f"source archive copy is incomplete: {collection_id} in {store}")
    return copy


def _destination_object_path(
    *,
    source: CollectionArchiveObjectRecord,
    destination_storage_prefix: str,
) -> str:
    prefix = destination_storage_prefix.strip("/")
    if not prefix:
        raise Conflict("archive copy destination prefix is empty")
    if source.kind == "pack":
        relative = f"volumes/{source.object_id}.tar.age"
    elif source.kind == "segment":
        relative = f"volumes/{source.object_id}.bin.age"
    elif source.kind == "manifest":
        relative = "manifest.json.age"
    elif source.kind == "recovery-descriptor":
        relative = "recovery.json"
    elif source.kind == "volume-metadata":
        sequence = _object_sequence(source.object_id, "volume-metadata-")
        relative = f"metadata/volume-{sequence}.json.age"
    elif source.kind == "volume-terminal":
        sequence = _object_sequence(source.object_id, "volume-terminal-")
        relative = f"metadata/volume-{sequence}.json.age"
    elif source.kind == "provenance-root":
        relative = "provenance/root.json.age"
    elif source.kind == "provenance-volume-metadata":
        sequence = _object_sequence(source.object_id, "provenance-volume-")
        relative = f"provenance/metadata/volume-{sequence}.json.age"
    elif source.kind == "provenance-terminal":
        sequence = _object_sequence(source.object_id, "provenance-terminal-")
        relative = f"provenance/metadata/volume-{sequence}.json.age"
    elif source.kind in {"provenance-bindings", "provenance-journal-segment"}:
        sequence = _object_sequence(source.object_id, "provenance-payload-")
        relative = f"provenance/payloads/volume-{sequence}.bin.age"
    else:
        raise Conflict(f"archive copy object kind is not immutable: {source.kind}")
    return f"{prefix}/{relative}"


def _archive_object_identity(record: CollectionArchiveObjectRecord) -> ArchiveObjectIdentity:
    return ArchiveObjectIdentity(
        object_id=record.object_id,
        kind=record.kind,
        object_path=record.object_path,
        plaintext_bytes=record.plaintext_bytes,
        stored_bytes=record.stored_bytes,
        sha256=record.sha256,
        stored_sha256=record.stored_sha256,
        revision=record.revision,
    )


def _object_sequence(object_id: str, prefix: str) -> str:
    raw = object_id.removeprefix(prefix)
    if raw == object_id or re.fullmatch(r"[0-9a-f]{64}", raw) is None:
        raise Conflict(f"archive object identity is invalid: {object_id}")
    return raw


def _volume_metadata(source: CollectionArchiveObjectRecord) -> dict[str, str]:
    if source.age_state_json is None:
        raise Conflict("archive volume has no age state")
    try:
        state = UploadState.from_json_bytes(source.age_state_json)
    except (TypeError, ValueError) as exc:
        raise Conflict("archive volume age state is invalid") from exc
    if state.plaintext_size != source.plaintext_bytes:
        raise Conflict("archive volume age state size changed")
    metadata = {
        "riverhog-format": (
            PACK_VOLUME_STORAGE_FORMAT if source.kind == "pack" else RAW_VOLUME_STORAGE_FORMAT
        ),
        "riverhog-object-id": source.object_id,
        "riverhog-plaintext-bytes": str(source.plaintext_bytes),
        "riverhog-age-state-sha256": hashlib.sha256(state.to_json_bytes()).hexdigest(),
    }
    if source.kind == "pack":
        if not source.plan_sha256 or not source.index_sha256:
            raise Conflict("archive pack has no plan or index identity")
        metadata.update(
            {
                "riverhog-plan-sha256": source.plan_sha256,
                "riverhog-index-sha256": source.index_sha256,
            }
        )
    else:
        if len(source.placements) != 1:
            raise Conflict("archive segment has no unique file placement")
        placement = source.placements[0]
        metadata.update(
            {
                "riverhog-source-path-sha256": hashlib.sha256(
                    placement.path.encode("utf-8")
                ).hexdigest(),
                "riverhog-file-offset": str(placement.file_offset),
            }
        )
    return metadata


def _part_rows(value: str | None) -> list[dict[str, object]]:
    if value is None:
        raise Conflict("archive volume has no stored-part receipts")
    try:
        raw = json.loads(value)
    except json.JSONDecodeError as exc:
        raise Conflict("archive volume stored-part receipts are invalid") from exc
    if not isinstance(raw, list) or not raw:
        raise Conflict("archive volume stored-part receipts are empty")
    rows: list[dict[str, object]] = []
    expected_start = 0
    expected_keys = {
        "number",
        "plaintext_start",
        "plaintext_bytes",
        "plaintext_sha256",
        "stored_bytes",
        "stored_sha256",
    }
    for number, current in enumerate(raw, start=1):
        if not isinstance(current, dict) or set(current) != expected_keys:
            raise Conflict("archive volume stored-part receipt is not canonical")
        if current.get("number") != number or current.get("plaintext_start") != expected_start:
            raise Conflict("archive volume stored-part order changed")
        plaintext_bytes = current.get("plaintext_bytes")
        stored_bytes = current.get("stored_bytes")
        if (
            isinstance(plaintext_bytes, bool)
            or not isinstance(plaintext_bytes, int)
            or plaintext_bytes < 0
            or isinstance(stored_bytes, bool)
            or not isinstance(stored_bytes, int)
            or stored_bytes < 1
        ):
            raise Conflict("archive volume stored-part byte count is invalid")
        for key in ("plaintext_sha256", "stored_sha256"):
            digest = current.get(key)
            if not isinstance(digest, str) or len(digest) != 64:
                raise Conflict("archive volume stored-part digest is invalid")
        expected_start += plaintext_bytes
        rows.append(dict(current))
    return rows


def _iter_exact_parts(
    chunks: Iterable[bytes],
    parts: Sequence[Mapping[str, object]],
) -> Iterator[bytes]:
    source = iter(chunks)
    buffer = bytearray()
    for row in parts:
        needed = _part_int(row, "stored_bytes")
        while len(buffer) < needed:
            try:
                chunk = bytes(next(source))
            except StopIteration as exc:
                raise Conflict("source archive volume ended before its part receipts") from exc
            if chunk:
                buffer.extend(chunk)
        yield bytes(buffer[:needed])
        del buffer[:needed]
    if buffer or any(bytes(chunk) for chunk in source):
        raise Conflict("source archive volume has bytes beyond its part receipts")


def _part_int(row: Mapping[str, object], key: str) -> int:
    value = row[key]
    if isinstance(value, bool) or not isinstance(value, int):
        raise Conflict("archive volume stored-part integer is invalid")
    return value


def _normalize_collection_id(value: str | int) -> int:
    try:
        return normalize_collection_id(value)
    except PathNormalizationError as exc:
        raise BadRequest(str(exc)) from exc


def _archive_copy_job_list_statement(
    *,
    q: str | None,
    state: str | None,
    sort: str,
    order: str,
    principal: Principal | None,
) -> tuple[str | None, str | None, list[Any], Any, tuple[Any, ...]]:
    if sort not in _SORT_FIELDS:
        raise BadRequest(f"sort must be one of {', '.join(sorted(_SORT_FIELDS))}")
    if order not in _SORT_ORDERS:
        raise BadRequest("order must be asc or desc")
    query = q.strip().casefold() if q and q.strip() else None
    normalized_state = state.strip().casefold() if state and state.strip() else None
    if normalized_state is not None and normalized_state not in ARCHIVE_COPY_JOB_STATES:
        raise BadRequest(f"state must be one of {', '.join(sorted(ARCHIVE_COPY_JOB_STATES))}")
    filters = [
        collection_access_filter(
            ArchiveCopyJobRecord.collection_id,
            principal,
            ARCHIVES_MANAGE,
        )
    ]
    if normalized_state is not None:
        filters.append(ArchiveCopyJobRecord.state == normalized_state)
    if query is not None:
        escaped = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        pattern = f"%{escaped}%"
        filters.append(ArchiveCopyJobRecord.search_text.like(pattern, escape="\\"))
    sort_columns = {
        "collection_id": ArchiveCopyJobRecord.collection_id,
        "source_store": ArchiveCopyJobRecord.source_store,
        "destination_store": ArchiveCopyJobRecord.destination_store,
        "state": ArchiveCopyJobRecord.state,
        "requested_at": ArchiveCopyJobRecord.requested_at,
    }
    key_columns = tuple(
        dict.fromkeys(
            (
                sort_columns[sort],
                ArchiveCopyJobRecord.collection_id,
                ArchiveCopyJobRecord.destination_store,
            )
        )
    )
    return (
        query,
        normalized_state,
        filters,
        select(ArchiveCopyJobRecord).where(*filters),
        key_columns,
    )


def _archive_copy_job_list_position(
    job: ArchiveCopyJobRecord,
    *,
    sort: str,
) -> tuple[str | int, ...]:
    values: dict[str, str | int] = {
        "collection_id": job.collection_id,
        "source_store": job.source_store,
        "destination_store": job.destination_store,
        "state": job.state,
        "requested_at": job.requested_at,
    }
    keys = [sort]
    if sort != "collection_id":
        keys.append("collection_id")
    if sort != "destination_store":
        keys.append("destination_store")
    return tuple(values[key] for key in keys)


def _job_payload(job: ArchiveCopyJobRecord) -> dict[str, object]:
    return {
        "collection_id": format_scalar("sequence63", job.collection_id),
        "source_store": job.source_store,
        "destination_store": job.destination_store,
        "use_cache": job.use_cache,
        "initiated_by_app": job.initiated_by_app,
        "initiated_by_key_id": job.initiated_by_key_id,
        "state": job.state,
        "requested_at": job.requested_at,
        "ready_at": job.ready_at,
        "expires_at": job.expires_at,
        "finished_at": job.finished_at,
        "failure": job.failure,
    }


def _fail_upload_copy_intent(intent: CollectionUploadCopyIntentRecord, code: str) -> None:
    intent.state = "failed"
    intent.failure_code = code
    intent.next_attempt_at = None
