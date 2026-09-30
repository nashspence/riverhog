from __future__ import annotations

import hashlib
import json
import logging
import re
import secrets
import uuid
from collections import OrderedDict
from collections.abc import Iterator, Mapping, Sequence
from datetime import timedelta
from itertools import islice
from typing import Any, Literal, TypedDict, cast

from http_api_contracts import BrowseScalar, closed_literal_values
from riverhog_archive_contracts import (
    BOUND_HISTORY_EXTENT,
    MEMBER_HISTORY_IMPORTS_SCHEMA,
    MEMBER_HISTORY_ROOTS_SCHEMA,
    PROVENANCE_BINDING_PAGE_BYTES_MAX,
    PROVENANCE_BINDING_PAGE_MEMBERS_MAX,
    PROVENANCE_BINDINGS_FORMAT,
    PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX,
    PROVENANCE_SEQUENCE_DOMAIN,
    RETAINED_HISTORY_EXTENT,
    CollectionArchiveTerminalDocument,
    CollectionArchiveVolumeDocument,
    CollectionEncryptionBinding,
    HistoryJournalAnchor,
    MemberHistoryBinding,
    MemberHistoryDocument,
    MemberHistoryImport,
    MemberHistoryPrimary,
    MemberHistoryRoot,
    MemberHistoryStore,
    ProvenancePayload,
    ProvenanceRootDocument,
    ProvenanceTerminalDocument,
    ProvenanceVolumeDocument,
    RecordPage,
    RecordSetCommitment,
    RecordSetRef,
    SourceMemberHistoryBindingProof,
    binding_tree_commitment,
    format_archive_sequence,
    history_record_page_object_path,
    member_history_object_path,
    provenance_structure_identity,
    provenance_structure_object_id,
    update_archive_sequence_commitment,
    update_provenance_commitment,
    validate_member_history_binding_page,
    verify_member_history_sets,
    verify_record_pages,
)
from riverhog_canonical_json import format_scalar
from riverhog_protocol import (
    COLLECTION_TAG_REQUEST_MEMBERS_MAX,
    ArtifactId,
    ArtifactMaterializationDecisionBatchDocument,
    ArtifactMemberIdentityDocument,
    CollectionArtifactProvenanceBindingBatchDocument,
    CollectionArtifactProvenanceBindingDocument,
    CollectionDescription,
    CollectionDescriptionDocument,
    CollectionTag,
    CollectionTagHeadDocument,
    CollectionTagSet,
    CollectionTagSetRoot,
    CollectionUploadArtifactBatchDocument,
    CollectionUploadArtifactCustodyReceiptDocument,
    CollectionUploadArtifactIn,
    CollectionUploadCustodyMode,
    CollectionUploadCustodyObjectDocument,
    CollectionUploadProvenanceCustodyObjectDocument,
    CollectionUploadProvenanceJournalCreateDocument,
    CollectionUploadProvenanceJournalStatusDocument,
    CollectionUploadRawDigestBatchDocument,
    CollectionUploadRegistrationConstraintsDocument,
    CollectionUploadSort,
    CollectionUploadState,
    MemberHistoryBindingBatchDocument,
    MemoryCollectionTagNodeStore,
    PortableCollectionArtifact,
    PortableCollectionHeader,
    SortOrder,
    collection_description_identity,
    collection_upload_raw_digest_summary,
    decode_collection_tag_node,
    validate_collection_tag,
    validate_collection_upload_batch_against_registration_constraints,
)
from riverhog_protocol.collection_completion import (
    CollectionCompletionRecordingDocument,
    CollectionCompletionRecordingRequestDocument,
    CollectionCompletionRequirementDocument,
)
from riverhog_protocol.collection_completion_validation import validate_completion_preimages
from riverhog_protocol.collection_production_provenance import (
    COLLECTION_MEMBER_ROLE,
    COLLECTION_PRODUCTION_CONTRACT_ID,
    EXECUTION_COMPLETION_SCHEMA_ID,
    collection_production_contract,
    validate_member_completion_requirement,
)
from riverhog_protocol.collection_workflows import ArtifactDispositionSetIdentity
from riverhog_protocol.errors import (
    BadRequest,
    Conflict,
    Forbidden,
    MaterializationDecisionRequired,
    NotFound,
)
from riverhog_protocol.output_collection_policy import OutputCollectionPolicy
from riverhog_protocol.pack_ingress import canonical_json_bytes
from riverhog_protocol.paths import (
    normalize_collection_id,
    text_search_key,
)
from riverhog_protocol.raw_ingress import (
    RawSourceDigestSummary,
    advance_raw_part_commitment,
    raw_volume_part_span,
)
from riverhog_protocol.transport import (
    COLLECTION_UPLOAD_ARTIFACT_BATCH_MAX,
    COLLECTION_UPLOAD_PROVENANCE_APPEND_BYTES_MAX,
)
from riverhog_provenance import (
    CanonicalCorpusValidator,
    JournalSummary,
    MemberHistoryClosure,
    ProvenanceValidationError,
    external_reference,
    validate_journal_chunks,
)
from sqlalchemy import asc, case, desc, exists, func, insert, or_, select, true
from sqlalchemy.orm import Session, selectinload
from state_schema import read_snapshot
from time_formats import (
    add_utc_timestamp,
    format_utc_timestamp,
    parse_utc_timestamp,
    utc_epoch_ns_now,
    utc_now,
    utc_timestamp_now,
)

from riverhog_core.app_permissions import (
    ALL_RESOURCES,
    ARCHIVES_MANAGE,
    COLLECTION_TAGS_MANAGE,
    COLLECTIONS_CREATE,
    COLLECTIONS_DELETE,
    Principal,
    normalize_access,
    tag_resource,
)
from riverhog_core.archive_manifest import (
    build_collection_archive_root_manifest,
    build_collection_archive_terminal_document,
    build_collection_archive_volume_document,
)
from riverhog_core.archive_provenance import (
    ArchiveProvenancePublisher,
    SealedArchiveProvenance,
)
from riverhog_core.archive_recovery_descriptor import (
    ArchiveRecoveryDescriptorPublisher,
)
from riverhog_core.archive_root import (
    ArchiveRootPublisher,
    SealedArchiveVolumeMetadata,
)
from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.browse import bounded_page, keyset_statement, validate_page_size
from riverhog_core.canonical_discovery_index import (
    begin_index_build,
    complete_index_build,
    publish_index_build,
    stage_assertion_page,
    stage_entry_page,
    stage_member,
    stage_membership_page,
    stage_snapshot_header,
)
from riverhog_core.canonical_discovery_relevance import (
    member_relevance,
    relevance_row_keys,
    snapshots_for_relevance,
)
from riverhog_core.canonical_discovery_rows import iter_index_assertions
from riverhog_core.catalog_db import SessionFactory, make_session_factory, session_scope
from riverhog_core.catalog_events import (
    begin_catalog_event,
    open_catalog_tag_visibility,
    publish_catalog_event,
)
from riverhog_core.catalog_models import (
    AppKeyAccessGrantRecord,
    AppKeyRecord,
    ArchiveCopyJobRecord,
    CollectionArchiveArtifactObjectRecord,
    CollectionArchiveCopyRecord,
    CollectionArchiveObjectRecord,
    CollectionArchiveObjectUploadRecord,
    CollectionArtifactProvenanceRecord,
    CollectionArtifactRecord,
    CollectionDescriptionPublicationRecord,
    CollectionProvenanceJournalRecord,
    CollectionProvenanceJournalSegmentRecord,
    CollectionRecord,
    CollectionTagMembershipRecord,
    CollectionTagNodeRecord,
    CollectionTagPublicationFrontierRecord,
    CollectionTagPublicationRecord,
    CollectionTagPublishedNodeRecord,
    CollectionTagRecord,
    CollectionTagRevisionRecord,
    CollectionUploadArtifactMaterializationDecisionRecord,
    CollectionUploadArtifactProvenanceBindingRecord,
    CollectionUploadArtifactRecord,
    CollectionUploadArtifactVolumeRecord,
    CollectionUploadCopyIntentRecord,
    CollectionUploadMemberHistoryRecord,
    CollectionUploadProvenanceArchiveVolumeRecord,
    CollectionUploadProvenanceCustodyObjectRecord,
    CollectionUploadProvenanceJournalChunkRecord,
    CollectionUploadProvenanceJournalRecord,
    CollectionUploadProvenanceStructureRecord,
    CollectionUploadRawPartDigestRecord,
    CollectionUploadRecord,
    CollectionUploadTagPublicationFrontierRecord,
    CollectionUploadTagRecord,
    RetrievalCacheLeaseRecord,
)
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexMembershipRecord,
    CollectionProvenanceIndexSnapshotRecord,
)
from riverhog_core.catalog_workflow_models import (
    CollectionProcessingClaimArtifactRecord,
    CollectionProcessingClaimInputRecord,
    CollectionProcessingClaimRecord,
    CollectionProcessingDispositionOutputRecord,
    CollectionProcessingDispositionSetRecord,
)
from riverhog_core.checkpoint_sha256 import CheckpointSHA256
from riverhog_core.collection_access import (
    collection_ids,
    permission_resources,
    require_collection_access,
    require_collection_create_access,
    tag_hashes,
)
from riverhog_core.collection_creation_identity import (
    CollectionUploadCreationIdentityDocument,
    CollectionUploadCreationIdentityPayload,
)
from riverhog_core.collection_plan import CollectionVolumePolicy
from riverhog_core.collection_production_validation import (
    validate_collection_production_records,
)
from riverhog_core.domain.archive import (
    ArchiveArtifact,
    PackVolumePlan,
    RawVolumePlan,
    SealedPackVolume,
    SealedRawVolume,
    StoredArchivePart,
)
from riverhog_core.incremental_plan import (
    OrderedArchiveArtifact,
    advance_incremental_volume_plan,
    incremental_volume_planner_checkpoint_bytes,
    new_incremental_volume_planner,
    parse_incremental_volume_planner_checkpoint,
)
from riverhog_core.pack_upload import PackUploadCheckpoint, PackVolumeUploader
from riverhog_core.pack_volume import (
    pack_unit_descriptors,
    pack_volume_plan_bytes,
    parse_pack_volume_plan,
)
from riverhog_core.placement_choices import resolve_use_cache
from riverhog_core.ports.archive_objects import ArchiveResumableObjectStore
from riverhog_core.ports.retrieval_cache import RetrievalCache
from riverhog_core.provenance_binding import verify_member_binding
from riverhog_core.provenance_catalog import admission_provenance_catalog
from riverhog_core.provenance_custody_set import ProvenanceCustodySet
from riverhog_core.raw_upload import RawUploadCheckpoint, RawVolumeUploader
from riverhog_core.raw_volume import parse_raw_volume_plan, raw_volume_plan_bytes
from riverhog_core.retrieval_cache_receipts import (
    parse_retrieval_cache_receipt,
    retrieval_cache_receipt_payload,
)
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services.collection_tags import (
    advance_collection_tag_set,
    collection_tag_sha256,
)
from riverhog_core.services.lifecycle_events import (
    SqlAlchemyLifecycleEventService,
    event_context_json,
)
from riverhog_core.services.operation_plans import (
    PLAN_TTL,
    challenge_expiry,
    challenge_has_shape,
    plan_challenge,
)
from riverhog_core.services.retrieval_cache import register_cache_ready
from riverhog_core.stores.mirrored_archive_resumable_object_store import (
    MirroredArchiveResumableObjectStore,
)
from riverhog_core.stores.sqlalchemy_archive_upload_checkpoints import (
    SqlAlchemyArchiveUploadCheckpointStore,
)
from riverhog_core.streaming_age import ResumableAgeSessionCache
from riverhog_core.throughput import (
    ArchiveThroughputTuning,
    ArchiveTransferResources,
    log_transfer_timing,
)

_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_PROVENANCE_JOURNAL_CHUNK_BYTES = 1024 * 1024
_FINALIZATION_FILE_BATCH = 1024
_LOG = logging.getLogger("riverhog_core.collection_uploads")
_DISCARD_CHALLENGE_PREFIX = "discard-upload"
_UPLOAD_SORT_FIELDS = closed_literal_values(CollectionUploadSort)
_UPLOAD_STATES = closed_literal_values(CollectionUploadState)
_SORT_ORDERS = closed_literal_values(SortOrder)
_CUSTODY_LOSS_WARNING = (
    "This permanently destroys Riverhog-custodied artifacts from an incomplete upload session."
)


class _RegisteredArtifact(TypedDict):
    artifact_id: str
    bytes: int
    sha256: str
    raw_part_plaintext_bytes: int | None
    raw_part_count: int | None
    raw_part_ordered_sha256: str | None


class _InitialDescriptionReceipt(TypedDict):
    object_path: str
    provider_revision: str | None
    published_at: str
    stored_bytes: int
    stored_sha256: str


class _InitialTagReceipt(TypedDict):
    object_path: str
    provider_revision: str | None
    published_at: str
    stored_bytes: int
    stored_sha256: str


class SqlAlchemyCollectionUploadService:
    """Own direct-to-final collection ingress and its final catalog transaction."""

    def __init__(
        self,
        config: RuntimeConfig,
        archive_stores: ArchiveStoreRegistry,
        *,
        retrieval_cache: RetrievalCache | None = None,
        policy: CollectionVolumePolicy | None = None,
        session_factory: SessionFactory | None = None,
        throughput_tuning: ArchiveThroughputTuning | None = None,
        transfer_resources: ArchiveTransferResources | None = None,
    ) -> None:
        self._config = config
        self._archive_stores = archive_stores
        self._retrieval_cache = retrieval_cache
        self._policy = policy or config.volume_policy
        self._session_factory = session_factory or make_session_factory(config.database_url)
        self._checkpoints = SqlAlchemyArchiveUploadCheckpointStore(
            config,
            session_factory=self._session_factory,
        )
        self._events = SqlAlchemyLifecycleEventService(
            config,
            session_factory=self._session_factory,
        )
        tuning = throughput_tuning or config.throughput_tuning
        self._throughput = tuning
        self._resources = transfer_resources or ArchiveTransferResources.from_tuning(tuning)
        self._age_sessions = {
            passphrase_id: ResumableAgeSessionCache(
                passphrase,
                max_entries=tuning.age_session_cache_entries,
                derivation_gate=self._resources.age_derivations,
            )
            for passphrase_id, passphrase in config.archive_passphrases.items()
        }

    def create_or_resume(
        self,
        *,
        idempotency_key: str,
        ingest_source: str | None,
        description: CollectionDescription | None = None,
        tags: Sequence[CollectionTag] = (),
        initial_tag_set_identity: str | None = None,
        archive_store: str | None,
        use_cache: bool | None = None,
        copy_to: Sequence[str] | None = None,
        initiator: Principal,
        event_context: Mapping[str, object] | None,
        custody_mode: str = "producer-retained",
    ) -> dict[str, object]:
        key = _normalize_idempotency_key(idempotency_key)
        canonical_tags = _canonical_tag_batch(tags, allow_empty=True)
        if initial_tag_set_identity is None:
            initial_set = CollectionTagSet(MemoryCollectionTagNodeStore())
            for tag in canonical_tags:
                initial_set = initial_set.insert(tag)
            initial_tag_set_identity = initial_set.identity
        if _SHA256_RE.fullmatch(initial_tag_set_identity) is None:
            raise BadRequest("initial collection tag-set identity is invalid")
        require_collection_create_access(initiator, COLLECTIONS_CREATE, tags=canonical_tags)
        _require_tag_assignment_access(initiator, canonical_tags)
        context_json = event_context_json(event_context)
        normalized_custody_mode = _normalize_custody_mode(custody_mode)
        with session_scope(self._session_factory) as session:
            _require_transform_output_intent(
                session,
                initiator=initiator,
                idempotency_key=key,
                ingest_source=ingest_source,
                archive_store=archive_store,
                use_cache=use_cache,
                copy_to=copy_to,
                tags=canonical_tags,
                initial_tag_set_identity=initial_tag_set_identity,
            )
            collection = session.scalar(
                select(CollectionRecord)
                .options(selectinload(CollectionRecord.archive_copies))
                .where(
                    CollectionRecord.created_by_principal_id == initiator.id,
                    CollectionRecord.creation_idempotency_key == key,
                    CollectionRecord.is_published.is_(True),
                )
            )
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(
                    CollectionUploadRecord.initiated_by_principal_id == initiator.id,
                    CollectionUploadRecord.idempotency_key == key,
                )
                .with_for_update()
            )
            persisted_store = (
                collection.creation_archive_store
                if collection is not None
                else upload.archive_store
                if upload is not None
                else None
            )
            store_name = archive_store or persisted_store or self._config.archive_write_store
            persisted_cache = (
                collection.creation_use_cache
                if collection is not None
                else upload.use_cache
                if upload is not None
                else None
            )
            if collection is not None:
                resolved_cache = use_cache if use_cache is not None else persisted_cache
                if not isinstance(resolved_cache, bool):
                    raise BadRequest("use_cache must be a boolean")
            else:
                try:
                    archive_binding = self._archive_stores.require(store_name)
                except ValueError as exc:
                    raise BadRequest(str(exc)) from exc
                resolved_cache = resolve_use_cache(
                    requested=use_cache if use_cache is not None else persisted_cache,
                    store_name=store_name,
                    config=self._config,
                    archive_stores=self._archive_stores,
                    retrieval_cache=self._retrieval_cache,
                )
            if copy_to is None:
                destinations = tuple(
                    json.loads(
                        collection.creation_copy_to_json
                        if collection is not None
                        else upload.copy_to_json
                        if upload is not None
                        else "[]"
                    )
                )
            else:
                destinations = _normalize_copy_destinations(
                    copy_to,
                    source_store=store_name,
                    archive_stores=self._archive_stores,
                    require_availability=collection is None,
                )
            creation_identity = _collection_upload_creation_identity(
                ingest_source=ingest_source,
                description=description,
                initial_tag_set_identity=initial_tag_set_identity,
                archive_store=store_name,
                use_cache=resolved_cache,
                copy_to=destinations,
                event_context_json=context_json,
                custody_mode=normalized_custody_mode,
            )
            if destinations:
                accepted_key = (
                    collection.created_by_key_id
                    if collection is not None
                    else upload.initiated_by_key_id
                    if upload is not None
                    else initiator.key_id
                )
                if accepted_key != initiator.key_id:
                    raise Conflict("collection upload copy intent initiator changed")
            if collection is not None:
                if collection.creation_identity_sha256 != (
                    creation_identity.creation_identity_sha256
                ):
                    raise Conflict("collection upload idempotency identity changed")
                return _finalized_payload(
                    session,
                    collection,
                    store_name=store_name,
                    resumed=True,
                )
            if upload is not None:
                if upload.creation_identity_sha256 != creation_identity.creation_identity_sha256:
                    raise Conflict("collection upload idempotency identity changed")
                if upload.state == "orphaned":
                    checkpoint = _planner_checkpoint(upload)
                    upload.state = "closing" if checkpoint.closed else "open"
                    upload.orphaned_at = None
                    resumed_at = utc_timestamp_now()
                    _touch_upload(upload, config=self._config, now=resumed_at)
                    upload.archive_phase = (
                        "uploading" if checkpoint.closed or upload.archive_objects else "planning"
                    )
                    upload.archive_phase_updated_at = resumed_at
                    upload.archive_failure = None
                    upload.archive_next_attempt_at = None
                elif upload.state == "discarding":
                    raise Conflict("collection upload discard is in progress")
                return _upload_payload(session, upload, resumed=True)

            for destination in destinations:
                resolve_use_cache(
                    requested=resolved_cache,
                    store_name=destination,
                    config=self._config,
                    archive_stores=self._archive_stores,
                    retrieval_cache=self._retrieval_cache,
                )

            if destinations and not initiator.id.startswith("processing:"):
                if initiator.key_id is None:
                    raise BadRequest("copy_to requires an attributable application key")
                key_record = session.get(AppKeyRecord, initiator.key_id, with_for_update=True)
                if (
                    key_record is None
                    or key_record.app != initiator.id
                    or key_record.revoked_at is not None
                    or (
                        key_record.expires_at is not None
                        and key_record.expires_at <= utc_timestamp_now()
                    )
                ):
                    raise NotFound("copy_to initiator is no longer authorized")
                grants = session.scalars(
                    select(AppKeyAccessGrantRecord)
                    .where(AppKeyAccessGrantRecord.key_id == initiator.key_id)
                    .with_for_update()
                ).all()
                current_principal = Principal(
                    id=initiator.id,
                    key_id=initiator.key_id,
                    access=frozenset(
                        normalize_access((grant.permission, grant.resource) for grant in grants)
                    ),
                )
                require_collection_create_access(
                    current_principal, COLLECTIONS_CREATE, tags=canonical_tags
                )
                require_collection_create_access(
                    current_principal, ARCHIVES_MANAGE, tags=canonical_tags
                )
            now = utc_timestamp_now()
            checkpoint = new_incremental_volume_planner(policy=self._policy)
            upload = CollectionUploadRecord(
                idempotency_key=key,
                creation_identity_sha256=creation_identity.creation_identity_sha256,
                initial_tag_set_identity=initial_tag_set_identity,
                archive_generation=secrets.token_hex(32),
                ingest_source=ingest_source,
                description=description,
                search_text=text_search_key(ingest_source or ""),
                encryption_format=self._config.archive_active_encryption.format,
                passphrase_id=self._config.archive_active_encryption.passphrase_id,
                initiated_by_principal_id=initiator.id,
                initiated_by_key_id=initiator.key_id,
                event_context_json=context_json,
                state="open",
                custody_mode=normalized_custody_mode,
                lease_expires_at=(
                    _custody_lease_expiry(self._config)
                    if normalized_custody_mode == "custody-transfer"
                    else None
                ),
                archive_store=store_name,
                archive_incarnation_id=archive_binding.incarnation_id,
                use_cache=resolved_cache,
                copy_to_json=json.dumps(destinations, separators=(",", ":")),
                opened_at=now,
                last_activity_at=now,
                archive_phase="planning",
                archive_phase_updated_at=now,
                archive_storage_prefix=(
                    archive_binding.store.new_collection_archive_storage_prefix()
                ),
                planner_checkpoint_json=(
                    incremental_volume_planner_checkpoint_bytes(checkpoint).decode("utf-8")
                ),
                derivative_provenance_state=(
                    "discovering" if initiator.id.startswith("processing:") else "not-required"
                ),
            )
            session.add(upload)
            session.flush()
            for destination in destinations:
                session.add(
                    CollectionUploadCopyIntentRecord(
                        collection_id=upload.collection_id,
                        destination_store=destination,
                        destination_incarnation_id=self._archive_stores.incarnation_id(destination),
                        source_store=store_name,
                        source_incarnation_id=archive_binding.incarnation_id,
                        initiated_by_app=initiator.id,
                        initiated_by_key_id=initiator.key_id,
                        event_context_json=context_json,
                        use_cache=resolved_cache,
                        state="accepted",
                        accepted_at=now,
                    )
                )
            staged_set = advance_collection_tag_set(
                session,
                root_sha256=None,
                tags=canonical_tags,
                retain_for_collection_id=upload.collection_id,
            )
            upload.tag_staging_root_sha256 = staged_set.root.root_sha256
            upload.tag_staging_set_identity = staged_set.identity
            session.add_all(
                CollectionUploadTagRecord(
                    collection_id=upload.collection_id,
                    tag_sha256=collection_tag_sha256(tag),
                    tag=tag,
                    added_at=now,
                )
                for tag in canonical_tags
            )
            session.flush()
            return _upload_payload(session, upload, resumed=False)

    def require_access(self, collection_id: int, principal: Principal) -> None:
        normalized = _collection_id(collection_id)
        with session_scope(self._session_factory) as session:
            upload = session.get(CollectionUploadRecord, normalized)
            if upload is not None:
                if upload.initiated_by_principal_id != principal.id:
                    raise NotFound(f"collection upload not found: {normalized}")
                _require_upload_create_access(session, normalized, principal)
                return
            collection = session.get(CollectionRecord, normalized)
            if collection is None or collection.created_by_principal_id != principal.id:
                raise NotFound(f"collection upload not found: {normalized}")
            require_collection_create_access(principal, COLLECTIONS_CREATE)

    def add_tags(
        self,
        collection_id: int,
        tags: Sequence[CollectionTag],
        *,
        principal: Principal,
    ) -> dict[str, object]:
        normalized = _collection_id(collection_id)
        canonical = _canonical_tag_batch(tags, allow_empty=False)
        _require_tag_assignment_access(principal, canonical)
        with session_scope(self._session_factory) as session:
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == normalized)
                .with_for_update()
            )
            if upload is None or upload.initiated_by_principal_id != principal.id:
                raise NotFound(f"collection upload not found: {normalized}")
            existing_tag_count = _require_upload_create_access(
                session,
                normalized,
                principal,
                additional_tags=canonical,
            )
            if upload.state != "open":
                raise Conflict(f"collection upload session is not open: {normalized}")
            now = utc_timestamp_now()
            added = 0
            new_tags: list[str] = []
            for tag in canonical:
                digest = collection_tag_sha256(tag)
                existing = session.get(CollectionUploadTagRecord, (normalized, digest))
                if existing is not None:
                    if existing.tag != tag:
                        raise RuntimeError("collection tag SHA-256 collision")
                    continue
                session.add(
                    CollectionUploadTagRecord(
                        collection_id=normalized,
                        tag_sha256=digest,
                        tag=tag,
                        added_at=now,
                    )
                )
                added += 1
                new_tags.append(tag)
            if new_tags:
                staged_set = advance_collection_tag_set(
                    session,
                    root_sha256=upload.tag_staging_root_sha256,
                    tags=new_tags,
                    retain_for_collection_id=normalized,
                )
                upload.tag_staging_root_sha256 = staged_set.root.root_sha256
                upload.tag_staging_set_identity = staged_set.identity
            _touch_upload(upload, config=self._config, now=now)
            session.flush()
            return {
                "collection_id": format_scalar("sequence63", normalized),
                "added": added,
                "tag_count": existing_tag_count + added,
            }

    def require_read_access(self, collection_id: int, principal: Principal) -> None:
        """Allow the owning producer or a collection-scoped deletion operator to inspect."""

        normalized = _collection_id(collection_id)
        with session_scope(self._session_factory) as session:
            upload = session.get(CollectionUploadRecord, normalized)
            if upload is not None:
                if upload.initiated_by_principal_id == principal.id:
                    _require_upload_create_access(session, normalized, principal)
                    return
                if _upload_visible_to_deleter(session, upload, principal):
                    return
                raise NotFound(f"collection upload not found: {normalized}")
            collection = session.get(CollectionRecord, normalized)
            if collection is None:
                raise NotFound(f"collection upload not found: {normalized}")
            if collection.created_by_principal_id == principal.id:
                require_collection_create_access(principal, COLLECTIONS_CREATE)
                return
            require_collection_access(
                session,
                principal,
                COLLECTIONS_DELETE,
                normalized,
            )

    def require_discard_access(
        self,
        collection_id: int,
        principal: Principal,
    ) -> None:
        """Require deletion authority scoped to this upload identity."""

        normalized = _collection_id(collection_id)
        with session_scope(self._session_factory) as session:
            upload = session.get(CollectionUploadRecord, normalized)
            if upload is None or not _upload_visible_to_deleter(session, upload, principal):
                raise NotFound(f"collection upload not found: {normalized}")

    def register_artifacts(
        self,
        collection_id: int,
        artifacts: Sequence[Mapping[str, object]],
    ) -> dict[str, object]:
        normalized_id = _collection_id(collection_id)
        if not artifacts or len(artifacts) > COLLECTION_UPLOAD_ARTIFACT_BATCH_MAX:
            raise BadRequest(
                "collection upload artifact batch must contain "
                f"1 to {COLLECTION_UPLOAD_ARTIFACT_BATCH_MAX} artifacts"
            )

        with session_scope(self._session_factory) as session:
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == normalized_id)
                .with_for_update()
            )
            if upload is None:
                raise NotFound(f"collection upload session not found: {normalized_id}")
            if upload.state != "open":
                raise Conflict(f"collection upload session is not open: {normalized_id}")
            checkpoint = _planner_checkpoint(upload)
            try:
                batch_document = CollectionUploadArtifactBatchDocument.model_validate(
                    {"artifacts": list(artifacts)}
                )
                constraints_document = (
                    CollectionUploadRegistrationConstraintsDocument.model_validate(
                        _registration_constraints_payload(checkpoint.policy)
                    )
                )
                validate_collection_upload_batch_against_registration_constraints(
                    batch_document,
                    constraints_document,
                )
            except ValueError as exc:
                raise BadRequest(str(exc)) from exc
            normalized_artifacts = tuple(
                _normalize_artifact(
                    value,
                    constraints=constraints_document,
                )
                for value in batch_document.artifacts
            )
            requested_ids = tuple(current["artifact_id"] for current in normalized_artifacts)
            existing = {
                row.artifact_id: row
                for row in session.scalars(
                    select(CollectionUploadArtifactRecord).where(
                        CollectionUploadArtifactRecord.collection_id == normalized_id,
                        CollectionUploadArtifactRecord.artifact_id.in_(requested_ids),
                    )
                )
            }
            new_artifacts: list[_RegisteredArtifact] = []
            for current in normalized_artifacts:
                row = existing.get(current["artifact_id"])
                if row is not None:
                    if _registered_artifact_identity(row) != current:
                        raise Conflict(
                            "collection upload artifact already has different metadata: "
                            f"{current['artifact_id']}"
                        )
                    continue
                new_artifacts.append(current)
            ordered: list[OrderedArchiveArtifact] = []
            next_order = checkpoint.next_artifact_order
            for current in new_artifacts:
                session.add(
                    CollectionUploadArtifactRecord(
                        collection_id=normalized_id,
                        artifact_id=current["artifact_id"],
                        artifact_order=next_order,
                        bytes=current["bytes"],
                        sha256=current["sha256"],
                        raw_part_plaintext_bytes=current["raw_part_plaintext_bytes"],
                        raw_part_count=current["raw_part_count"],
                        raw_part_ordered_sha256=current["raw_part_ordered_sha256"],
                        raw_parts_accepted=0,
                        raw_part_commitment_sha256=None,
                    )
                )
                ordered.append(
                    OrderedArchiveArtifact(
                        order=next_order,
                        artifact=ArchiveArtifact(
                            artifact_id=current["artifact_id"],
                            bytes=current["bytes"],
                            sha256=current["sha256"],
                        ),
                    )
                )
                next_order += 1
            upload.artifact_count += len(new_artifacts)
            upload.artifact_bytes += sum(current["bytes"] for current in new_artifacts)
            batch = advance_incremental_volume_plan(checkpoint, ordered)
            _persist_plan_batch(session, upload=upload, batch=batch)
            upload.planner_checkpoint_json = incremental_volume_planner_checkpoint_bytes(
                batch.checkpoint
            ).decode("utf-8")
            _touch_upload(upload, config=self._config)
            upload.archive_phase = "uploading" if upload.archive_objects else "planning"
            upload.archive_phase_updated_at = upload.last_activity_at
            session.flush()
            records = [
                session.get(CollectionUploadArtifactRecord, (normalized_id, current["artifact_id"]))
                for current in normalized_artifacts
            ]
            return {
                "collection_id": format_scalar("sequence63", normalized_id),
                "ingest_source": upload.ingest_source,
                "archive_store": upload.archive_store,
                "encryption_format": upload.encryption_format,
                "passphrase_id": upload.passphrase_id,
                "state": upload.state,
                "artifacts": [_artifact_payload(row) for row in records if row is not None],
                "volumes": [_volume_summary(row) for row in batch.volumes],
            }

    def register_raw_part_digests(
        self,
        collection_id: int,
        batch: CollectionUploadRawDigestBatchDocument,
    ) -> dict[str, object]:
        """Append one bounded exact slice to a registered raw-source authority."""

        normalized_id = _collection_id(collection_id)
        with session_scope(self._session_factory) as session:
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == normalized_id)
                .with_for_update()
            )
            if upload is None:
                raise NotFound(f"collection upload session not found: {normalized_id}")
            if upload.state != "open":
                raise Conflict("collection upload no longer accepts raw source digests")
            artifact = session.scalar(
                select(CollectionUploadArtifactRecord)
                .where(
                    CollectionUploadArtifactRecord.collection_id == normalized_id,
                    CollectionUploadArtifactRecord.artifact_id == batch.artifact_id,
                )
                .with_for_update()
            )
            if (
                artifact is None
                or artifact.raw_part_count is None
                or artifact.raw_part_ordered_sha256 is None
            ):
                raise NotFound(f"registered raw upload artifact not found: {batch.artifact_id}")
            accepted = int(artifact.raw_parts_accepted)
            end = batch.first_part + len(batch.sha256s)
            if end > artifact.raw_part_count:
                raise BadRequest("raw source digest batch exceeds its registered part count")
            if batch.first_part < accepted:
                if end > accepted:
                    raise Conflict("raw source digest batch overlaps committed progress")
                existing = tuple(
                    session.scalars(
                        select(CollectionUploadRawPartDigestRecord.sha256)
                        .where(
                            CollectionUploadRawPartDigestRecord.collection_id == normalized_id,
                            CollectionUploadRawPartDigestRecord.artifact_id == batch.artifact_id,
                            CollectionUploadRawPartDigestRecord.part_number >= batch.first_part,
                            CollectionUploadRawPartDigestRecord.part_number < end,
                        )
                        .order_by(CollectionUploadRawPartDigestRecord.part_number)
                    )
                )
                if existing != tuple(batch.sha256s):
                    raise Conflict("raw source digest retry differs from committed bytes")
                return _raw_digest_progress(artifact)
            if batch.first_part != accepted:
                raise Conflict(
                    f"raw source digest offset differs: expected {accepted}, "
                    f"received {batch.first_part}"
                )
            next_part, commitment = advance_raw_part_commitment(
                artifact.raw_part_commitment_sha256,
                first_part=batch.first_part,
                part_sha256s=batch.sha256s,
            )
            for offset, sha256 in enumerate(batch.sha256s):
                session.add(
                    CollectionUploadRawPartDigestRecord(
                        collection_id=normalized_id,
                        artifact_id=batch.artifact_id,
                        part_number=batch.first_part + offset,
                        sha256=sha256,
                    )
                )
            artifact.raw_parts_accepted = next_part
            artifact.raw_part_commitment_sha256 = commitment
            if (
                next_part == artifact.raw_part_count
                and commitment != artifact.raw_part_ordered_sha256
            ):
                raise BadRequest("raw source digest sequence differs from its registered authority")
            _touch_upload(upload, config=self._config)
            session.flush()
            return _raw_digest_progress(artifact)

    def set_completion_requirement(
        self,
        collection_id: int,
        requirement: CollectionCompletionRequirementDocument,
    ) -> CollectionCompletionRequirementDocument:
        """Accept an immutable construction obligation before early custody."""

        normalized_id = _collection_id(collection_id)
        encoded = canonical_json_bytes(requirement.model_dump(mode="json")).decode("utf-8")
        with session_scope(self._session_factory) as session:
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == normalized_id)
                .with_for_update()
            )
            if upload is None:
                raise NotFound("collection construction is unavailable")
            if upload.completion_requirement_json is not None:
                if upload.completion_requirement_json != encoded:
                    raise Conflict("accepted completion requirement cannot be replaced")
                return requirement
            if upload.state != "open" or upload.safe_release_artifact_count:
                raise Conflict("completion must be declared before early custody receipts")
            if upload.initiated_by_principal_id.startswith("processing:"):
                claim = session.scalar(
                    select(CollectionProcessingClaimRecord).where(
                        CollectionProcessingClaimRecord.execution_id == requirement.execution_id
                    )
                )
                if (
                    claim is None
                    or claim.state != "active"
                    or claim.plan_sealed_at is None
                    or upload.initiated_by_principal_id != "processing:" + requirement.execution_id
                    or claim.controller_evidence_sha256 != requirement.controller_evidence_sha256
                    or parse_utc_timestamp(claim.expires_at) <= utc_epoch_ns_now()
                ):
                    raise Conflict("completion declaration differs from the accepted claim plan")
            upload.completion_requirement_json = encoded
            _touch_upload(upload, config=self._config)
        return requirement

    def create_provenance_journal(
        self,
        collection_id: int,
        journal_id: str,
        authority: CollectionUploadProvenanceJournalCreateDocument,
    ) -> dict[str, object]:
        normalized_id = _collection_id(collection_id)
        with session_scope(self._session_factory) as session:
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == normalized_id)
                .with_for_update()
            )
            if upload is None:
                raise NotFound(f"collection upload session not found: {normalized_id}")
            if upload.state != "open":
                raise Conflict("collection upload does not accept provenance journals")
            if authority.selection_role == "completion":
                if upload.completion_requirement_json is None:
                    raise Conflict("completion journal has no accepted construction requirement")
                if upload.completion_recorded_at is None:
                    raise Conflict("completion recording context has not been accepted")
                if upload.completion_journal_id not in (None, journal_id):
                    raise Conflict("collection upload completion journal is already selected")
                upload.completion_journal_id = journal_id
            elif upload.completion_journal_id == journal_id:
                raise Conflict("completion journal retry must retain its root role")
            existing = session.get(
                CollectionUploadProvenanceJournalRecord,
                (normalized_id, journal_id),
            )
            if existing is not None:
                dependency = authority.selection_role == "history-dependency"
                if existing.history_dependency != dependency:
                    raise Conflict("canonical journal retry changes its construction role")
                if existing.sha256 != authority.sha256 or existing.bytes != authority.bytes:
                    if (
                        not dependency
                        or existing.state != "sealed"
                        or authority.bytes <= existing.bytes
                    ):
                        raise Conflict("provenance journal authority already differs")
                    # A longer authenticated source snapshot can share an earlier
                    # prefix. Accepted octets and selected anchors stay immutable.
                    existing.bytes = authority.bytes
                    existing.sha256 = authority.sha256
                    existing.state = "accepting"
                    existing.terminal_entry_id = None
                    existing.terminal_sequence = None
                    existing.terminal_json_sha256 = None
                    _touch_upload(upload, config=self._config)
                return _journal_payload(existing)
            record = CollectionUploadProvenanceJournalRecord(
                collection_id=normalized_id,
                journal_id=journal_id,
                bytes=authority.bytes,
                sha256=authority.sha256,
                state="accepting",
                history_dependency=authority.selection_role == "history-dependency",
                accepted_bytes=0,
                next_chunk_ordinal=0,
                content_hash_state=CheckpointSHA256().export_state(),
                validation_byte_offset=0,
                validation_sequence=0,
            )
            session.add(record)
            _touch_upload(upload, config=self._config)
            session.flush()
            return _journal_payload(record)

    def reserve_completion_recording(
        self, collection_id: int, request: CollectionCompletionRecordingRequestDocument
    ) -> CollectionCompletionRecordingDocument:
        """Checkpoint metadata only after exact preimage identities are available.

        The independently attributed recorder can reproduce identical bytes
        after a crash without retaining or rereading disposable payload files.
        """
        with session_scope(self._session_factory) as session:
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == _collection_id(collection_id))
                .with_for_update()
            )
            if upload is None or upload.completion_requirement_json is None:
                raise NotFound("execution construction has no accepted completion requirement")
            requirement = CollectionCompletionRequirementDocument.model_validate_json(
                upload.completion_requirement_json
            )
            if request.requirement_sha256 != requirement.identity:
                raise Conflict("recording requirement differs from accepted construction")
            if upload.completion_records_sha256 not in (None, request.records_sha256):
                raise Conflict("accepted recording preimages cannot be replaced")
            if upload.completion_recorded_at is None:
                if upload.state != "open" or upload.completion_journal_id is not None:
                    raise Conflict("construction no longer accepts a completion recording context")
                upload.completion_journal_id = "urn:uuid:" + str(uuid.uuid4())
                upload.completion_recorded_at = utc_timestamp_now()
                upload.completion_records_sha256 = request.records_sha256
                _touch_upload(upload, config=self._config)
            return CollectionCompletionRecordingDocument.model_validate(
                {
                    **request.model_dump(mode="json"),
                    "journal_id": upload.completion_journal_id,
                    "recorded_at": upload.completion_recorded_at,
                }
            )

    def bind_artifact_provenance(
        self,
        collection_id: int,
        batch: CollectionArtifactProvenanceBindingBatchDocument,
    ) -> dict[str, object]:
        """Stage exact member-to-journal selections independently of registration."""

        normalized_id = _collection_id(collection_id)
        with session_scope(self._session_factory) as session:
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == normalized_id)
                .with_for_update()
            )
            if upload is None:
                raise NotFound(f"collection upload session not found: {normalized_id}")
            if upload.state != "open":
                raise Conflict("collection upload no longer accepts provenance bindings")
            for binding in batch.bindings:
                artifact_id = str(binding.artifact_id)
                if (
                    session.get(CollectionUploadArtifactRecord, (normalized_id, artifact_id))
                    is None
                ):
                    raise NotFound(f"registered upload artifact not found: {artifact_id}")
                journal = session.get(
                    CollectionUploadProvenanceJournalRecord,
                    (normalized_id, binding.journal.journal_id),
                )
                if journal is None or journal.state != "sealed":
                    raise Conflict("primary canonical journal must be sealed before binding")
                if journal.history_dependency:
                    raise Conflict(
                        "an imported journal cannot replace a member's own early primary"
                    )
                if binding.journal.prefix_bytes > journal.bytes:
                    raise BadRequest("primary journal anchor exceeds its sealed octets")
                values = {
                    "journal_id": binding.journal.journal_id,
                    "through_entry_id": binding.journal.through.entry_id,
                    "through_sequence": binding.journal.through.sequence,
                    "through_json_sha256": binding.journal.through.json_sha256,
                    "prefix_sha256": binding.journal.prefix_sha256,
                    "prefix_bytes": binding.journal.prefix_bytes,
                    "delivery_association_id": binding.delivery_association_id,
                }
                existing = session.get(
                    CollectionUploadArtifactProvenanceBindingRecord,
                    (normalized_id, artifact_id),
                )
                if existing is None:
                    session.add(
                        CollectionUploadArtifactProvenanceBindingRecord(
                            collection_id=normalized_id,
                            artifact_id=artifact_id,
                            **values,
                        )
                    )
                elif any(getattr(existing, name) != value for name, value in values.items()):
                    raise Conflict("artifact provenance binding retry differs from accepted anchor")
            _touch_upload(upload, config=self._config)
        return {
            "bindings": [binding.model_dump(mode="json") for binding in batch.bindings],
        }

    def get_artifact_provenance_binding(
        self, collection_id: int, artifact_id: ArtifactId
    ) -> dict[str, object]:
        """Return the exact accepted primary binding for a restarted producer."""

        normalized_id = _collection_id(collection_id)
        with read_snapshot(self._session_factory) as session:
            row = session.get(
                CollectionUploadArtifactProvenanceBindingRecord,
                (normalized_id, str(artifact_id)),
            )
            if row is None:
                raise NotFound(f"artifact provenance binding not found: {artifact_id}")
            return _provenance_binding_row(row)

    def set_member_history_inputs(
        self, collection_id: int, artifact_id: ArtifactId, authority: RecordSetRef
    ) -> RecordSetRef:
        """Accept an exact input-history transfer before acknowledging early output custody."""
        if authority.schema_id != MEMBER_HISTORY_IMPORTS_SCHEMA:
            raise BadRequest("input history set has the wrong record schema")
        normalized_id = _collection_id(collection_id)
        encoded = canonical_json_bytes(authority.to_mapping()).decode("utf-8")
        with session_scope(self._session_factory) as session:
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == normalized_id)
                .with_for_update()
            )
            binding = session.get(
                CollectionUploadArtifactProvenanceBindingRecord, (normalized_id, str(artifact_id))
            )
            if upload is None or binding is None:
                raise NotFound("input selection has no accepted member primary")
            if binding.history_imports_ref_json is not None:
                if binding.history_imports_ref_json != encoded:
                    raise Conflict("accepted input-history extent cannot be replaced")
                return authority
            if upload.state != "open":
                raise Conflict("construction is closed to new input-history selections")
            artifact = session.get(
                CollectionUploadArtifactRecord, (normalized_id, str(artifact_id))
            )
            if artifact is None or artifact.custody_receipt_json is not None:
                raise Conflict("acknowledged early custody cannot gain new input requirements")
            verify_record_pages(
                authority, _iter_staged_history_pages(session, normalized_id, authority)
            )
            with _staged_history_closure(session, normalized_id) as closure:
                for page in _iter_staged_history_pages(session, normalized_id, authority):
                    for row in page.records:
                        imported = MemberHistoryImport.from_mapping(row["value"])
                        if imported.key != row["key"]:
                            raise Conflict(
                                "input-history selection key differs from its source scope"
                            )
                        closure.resolve_import(imported)
                        if upload.initiated_by_principal_id.startswith("processing:"):
                            claim = session.scalar(
                                select(CollectionProcessingClaimRecord).where(
                                    CollectionProcessingClaimRecord.execution_id
                                    == upload.initiated_by_principal_id.removeprefix("processing:")
                                )
                            )
                            accepted = (
                                None
                                if claim is None
                                else session.get(
                                    CollectionProcessingClaimInputRecord,
                                    (claim.id, imported.source_collection_id),
                                )
                            )
                            member = (
                                None
                                if claim is None
                                else session.get(
                                    CollectionProcessingClaimArtifactRecord,
                                    (
                                        claim.id,
                                        imported.source_collection_id,
                                        str(imported.source_artifact_id),
                                    ),
                                )
                            )
                            proof = closure.store.source_proof(imported)
                            if (
                                accepted is None
                                or member is None
                                or accepted.archive_root_sha256
                                != imported.source_archive_root_sha256
                                or accepted.artifact_set_identity
                                != imported.source_artifact_set_sha256
                                or member.bytes != proof.binding.bytes
                                or member.sha256 != proof.binding.sha256
                            ):
                                raise Forbidden(
                                    "input-history selection is outside the sealed claim"
                                )
            binding.history_imports_ref_json = encoded
            _touch_upload(upload, config=self._config)
        return authority

    def get_member_history_inputs(
        self, collection_id: int, artifact_id: ArtifactId
    ) -> RecordSetRef:
        with read_snapshot(self._session_factory) as session:
            row = session.get(
                CollectionUploadArtifactProvenanceBindingRecord,
                (_collection_id(collection_id), str(artifact_id)),
            )
            if row is None or row.history_imports_ref_json is None:
                raise NotFound("member has no accepted input-history selection")
            return RecordSetRef.from_mapping(json.loads(row.history_imports_ref_json))

    def stage_history_structure(self, collection_id: int, content: bytes) -> dict[str, object]:
        """Durably stage one exact bounded object without granting it semantic authority."""

        normalized_id = _collection_id(collection_id)
        try:
            identity = provenance_structure_identity(content)
        except ValueError as exc:
            raise BadRequest(str(exc)) from exc
        with session_scope(self._session_factory) as session:
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == normalized_id)
                .with_for_update()
            )
            if upload is None:
                raise NotFound(f"collection upload session not found: {normalized_id}")
            if upload.state != "open":
                raise Conflict("collection upload no longer accepts history structure")
            _stage_provenance_structure(
                session,
                collection_id=normalized_id,
                object_id=identity.object_id,
                kind=identity.kind,
                relative_path=identity.relative_path,
                content=content,
            )
            _touch_upload(upload, config=self._config)
        return {
            "object_id": identity.object_id,
            "kind": identity.kind,
            "bytes": format_scalar("nonnegative", identity.bytes),
            "sha256": identity.sha256,
        }

    def bind_member_histories(
        self, collection_id: int, batch: MemberHistoryBindingBatchDocument
    ) -> dict[str, object]:
        """Accept final explicit selections while retaining the immutable early primary."""

        normalized_id = _collection_id(collection_id)
        with session_scope(self._session_factory) as session:
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == normalized_id)
                .with_for_update()
            )
            if upload is None:
                raise NotFound(f"collection upload session not found: {normalized_id}")
            if upload.state != "open":
                raise Conflict("collection upload no longer accepts final history bindings")
            for value in batch.bindings:
                binding = MemberHistoryBinding.from_mapping(value.model_dump(mode="json"))
                artifact = session.get(
                    CollectionUploadArtifactRecord, (normalized_id, binding.artifact_id)
                )
                early = session.get(
                    CollectionUploadArtifactProvenanceBindingRecord,
                    (normalized_id, binding.artifact_id),
                )
                structure = session.get(
                    CollectionUploadProvenanceStructureRecord,
                    (normalized_id, "provenance-history-" + binding.history_sha256),
                )
                if artifact is None or early is None or structure is None:
                    raise Conflict(
                        "final history requires a registered member, primary and descriptor"
                    )
                if (binding.bytes, binding.sha256) != (artifact.bytes, artifact.sha256):
                    raise Conflict("final history differs from the registered member")
                history = binding.verify_descriptor(structure.content)
                primary = MemberHistoryPrimary.from_mapping(
                    {
                        "journal": _provenance_binding_row(early)["journal"],
                        "delivery_association_id": early.delivery_association_id,
                    }
                )
                if history.primary != primary:
                    raise Conflict("final history cannot replace the accepted early primary")
                if upload.completion_requirement_json is not None and early.history_imports_ref_json is None:
                    raise Conflict("final execution history lacks its accepted input selection")
                if (
                    early.history_imports_ref_json is not None
                    and history.imports
                    != RecordSetRef.from_mapping(json.loads(early.history_imports_ref_json))
                ):
                    raise Conflict("final history substitutes the accepted input-history extent")
                existing = session.get(
                    CollectionUploadMemberHistoryRecord, (normalized_id, binding.artifact_id)
                )
                encoded = canonical_json_bytes(binding.to_mapping()).decode("utf-8")
                if existing is None:
                    session.add(
                        CollectionUploadMemberHistoryRecord(
                            collection_id=normalized_id,
                            artifact_id=binding.artifact_id,
                            history_sha256=binding.history_sha256,
                            history_bytes=binding.history_bytes,
                            binding_json=encoded,
                        )
                    )
                elif existing.binding_json != encoded:
                    raise Conflict("final member history already differs")
            _touch_upload(upload, config=self._config)
        return batch.model_dump(mode="json")

    def set_artifact_materialization_decisions(
        self,
        collection_id: int,
        batch: ArtifactMaterializationDecisionBatchDocument,
    ) -> dict[str, object]:
        """Accept one immutable, explicit publication decision per member."""

        normalized_id = _collection_id(collection_id)
        with session_scope(self._session_factory) as session:
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == normalized_id)
                .with_for_update()
            )
            if upload is None:
                raise NotFound(f"collection upload session not found: {normalized_id}")
            if upload.state != "open":
                raise Conflict("collection upload no longer accepts materialization decisions")
            for decision in batch.decisions:
                artifact_id = str(decision.artifact_id)
                hint_json = (
                    canonical_json_bytes(
                        decision.materialization_hint.model_dump(mode="json")
                    ).decode("utf-8")
                    if decision.materialization_hint is not None
                    else None
                )
                if (
                    session.get(CollectionUploadArtifactRecord, (normalized_id, artifact_id))
                    is None
                ):
                    raise NotFound(f"registered upload artifact not found: {artifact_id}")
                existing = session.get(
                    CollectionUploadArtifactMaterializationDecisionRecord,
                    (normalized_id, artifact_id),
                )
                if existing is None:
                    session.add(
                        CollectionUploadArtifactMaterializationDecisionRecord(
                            collection_id=normalized_id,
                            artifact_id=artifact_id,
                            hint_json=hint_json,
                            allow_missing_materialization_hint=(
                                decision.allow_missing_materialization_hint
                            ),
                        )
                    )
                elif (
                    existing.allow_missing_materialization_hint
                    != decision.allow_missing_materialization_hint
                    or existing.hint_json != hint_json
                ):
                    raise Conflict("materialization decision retry differs from accepted choice")
            _touch_upload(upload, config=self._config)
        return batch.model_dump(mode="json")

    def append_provenance_journal(
        self,
        collection_id: int,
        journal_id: str,
        *,
        offset: int,
        content: bytes,
    ) -> dict[str, object]:
        normalized_id = _collection_id(collection_id)
        chunk = bytes(content)
        if offset < 0 or not chunk or len(chunk) > COLLECTION_UPLOAD_PROVENANCE_APPEND_BYTES_MAX:
            raise BadRequest("provenance append is outside its bounded transport contract")
        with session_scope(self._session_factory) as session:
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == normalized_id)
                .with_for_update()
            )
            record = session.scalar(
                select(CollectionUploadProvenanceJournalRecord)
                .where(
                    CollectionUploadProvenanceJournalRecord.collection_id == normalized_id,
                    CollectionUploadProvenanceJournalRecord.journal_id == journal_id,
                )
                .with_for_update()
            )
            if upload is None or record is None:
                raise NotFound(f"collection upload provenance journal not found: {journal_id}")
            if upload.state != "open":
                raise Conflict(f"collection upload session is not open: {normalized_id}")
            if record.state != "accepting":
                raise Conflict(f"provenance journal is {record.state}")
            if offset < record.accepted_bytes:
                existing = session.scalar(
                    select(CollectionUploadProvenanceJournalChunkRecord).where(
                        CollectionUploadProvenanceJournalChunkRecord.collection_id == normalized_id,
                        CollectionUploadProvenanceJournalChunkRecord.journal_id == journal_id,
                        CollectionUploadProvenanceJournalChunkRecord.byte_offset == offset,
                    )
                )
                if existing is None or existing.content != chunk:
                    raise Conflict("provenance append retry differs from committed bytes")
                return _journal_payload(record)
            if offset != record.accepted_bytes:
                raise Conflict(
                    f"provenance append offset differs: expected {record.accepted_bytes}, "
                    f"received {offset}"
                )
            if offset + len(chunk) > record.bytes:
                raise BadRequest("provenance append exceeds its declared authority")
            if (
                offset + len(chunk) < record.bytes
                and len(chunk) != COLLECTION_UPLOAD_PROVENANCE_APPEND_BYTES_MAX
            ):
                raise BadRequest(
                    "every non-final provenance append must fill one transport segment"
                )
            ordinal = record.next_chunk_ordinal
            digest = CheckpointSHA256.from_state(record.content_hash_state)
            digest.update(chunk)
            session.add(
                CollectionUploadProvenanceJournalChunkRecord(
                    collection_id=normalized_id,
                    journal_id=journal_id,
                    ordinal=ordinal,
                    byte_offset=offset,
                    content=chunk,
                )
            )
            record.accepted_bytes += len(chunk)
            record.next_chunk_ordinal += 1
            record.content_hash_state = digest.export_state()
            _touch_upload(upload, config=self._config)
            return _journal_payload(record)

    def seal_provenance_journal(
        self,
        collection_id: int,
        journal_id: str,
    ) -> dict[str, object]:
        normalized_id = _collection_id(collection_id)
        with session_scope(self._session_factory) as session:
            record = session.scalar(
                select(CollectionUploadProvenanceJournalRecord)
                .where(
                    CollectionUploadProvenanceJournalRecord.collection_id == normalized_id,
                    CollectionUploadProvenanceJournalRecord.journal_id == journal_id,
                )
                .with_for_update()
            )
            if record is None:
                raise NotFound(f"collection upload provenance journal not found: {journal_id}")
            if record.state in {"sealed", "failed"}:
                return _journal_payload(record)
            if record.state == "accepting":
                digest = CheckpointSHA256.from_state(record.content_hash_state)
                if record.accepted_bytes != record.bytes or digest.hexdigest() != record.sha256:
                    raise Conflict("provenance journal content does not match its exact authority")
                record.state = "validating"
            try:
                with session.begin_nested():
                    _validate_next_upload_journal_entry(session, record)
            except (ProvenanceValidationError, ValueError) as exc:
                record.state = "failed"
                record.failure = str(exc)[:1000]
            return _journal_payload(record)

    def get_provenance_journal(
        self,
        collection_id: int,
        journal_id: str,
    ) -> dict[str, object]:
        with read_snapshot(self._session_factory) as session:
            record = session.get(
                CollectionUploadProvenanceJournalRecord,
                (_collection_id(collection_id), journal_id),
            )
            if record is None:
                raise NotFound(f"collection upload provenance journal not found: {journal_id}")
            return _journal_payload(record)

    def iter_sealed_provenance_journal(
        self, collection_id: int, journal_id: str
    ) -> Iterator[bytes]:
        """Stream the exact staged journal to its owning producer for resumed recording."""

        with read_snapshot(self._session_factory) as session:
            record = session.get(
                CollectionUploadProvenanceJournalRecord,
                (_collection_id(collection_id), journal_id),
            )
            if record is None:
                raise NotFound(f"collection upload provenance journal not found: {journal_id}")
            if record.state != "sealed":
                raise Conflict("canonical provenance journal is not sealed")
            yield from _iter_upload_journal_chunks(session, record)

    def process_due_provenance_journal_validations(self, *, limit: int = 1) -> int:
        processed = 0
        for _ in range(max(0, limit)):
            with session_scope(self._session_factory) as session:
                record = session.scalar(
                    select(CollectionUploadProvenanceJournalRecord)
                    .where(CollectionUploadProvenanceJournalRecord.state == "validating")
                    .order_by(
                        CollectionUploadProvenanceJournalRecord.collection_id,
                        CollectionUploadProvenanceJournalRecord.journal_id,
                    )
                    .with_for_update(skip_locked=True)
                    .limit(1)
                )
                if record is None:
                    break
                collection_id = record.collection_id
                try:
                    with session.begin_nested():
                        _validate_next_upload_journal_entry(session, record)
                except (ProvenanceValidationError, ValueError) as exc:
                    record.state = "failed"
                    record.failure = str(exc)[:1000]
            self._schedule_finalization_if_ready(collection_id)
            processed += 1
        return processed

    def complete(
        self,
        collection_id: int,
    ) -> dict[str, object]:
        normalized_id = _collection_id(collection_id)
        with session_scope(self._session_factory) as session:
            collection = session.scalar(
                select(CollectionRecord).where(
                    CollectionRecord.id == normalized_id,
                    CollectionRecord.is_published.is_(True),
                )
            )
            if collection is not None:
                return _finalized_payload(
                    session,
                    collection,
                    store_name=collection.creation_archive_store,
                )
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == normalized_id)
                .with_for_update()
            )
            if upload is None:
                raise NotFound(f"collection upload session not found: {normalized_id}")
            if upload.state not in {"open", "closing", "uploading", "finalizing"}:
                raise Conflict(f"collection upload session is {upload.state}: {normalized_id}")
            checkpoint = _planner_checkpoint(upload)
            if upload.state == "open":
                if upload.tag_staging_set_identity != upload.initial_tag_set_identity:
                    raise Conflict("initial collection tags are incomplete")
                _seal_open_collection_upload(
                    session,
                    upload,
                    checkpoint=checkpoint,
                    config=self._config,
                )
        self._schedule_finalization_if_ready(normalized_id)
        return self.get(normalized_id)

    def list_artifacts(
        self,
        collection_id: int,
        *,
        page_size: int,
        position: tuple[str | int | bool | bytes | None, ...] | None,
    ) -> dict[str, object]:
        normalized_id = _collection_id(collection_id)
        with read_snapshot(self._session_factory) as session:
            if session.get(CollectionUploadRecord, normalized_id) is None:
                raise NotFound(f"collection upload session not found: {normalized_id}")
            if position is None:
                frontier = session.scalar(
                    select(func.max(CollectionUploadArtifactRecord.artifact_order)).where(
                        CollectionUploadArtifactRecord.collection_id == normalized_id
                    )
                )
                after: tuple[str | int | bool | bytes | None, ...] | None = None
            else:
                if (
                    len(position) != 2
                    or not isinstance(position[0], int)
                    or not isinstance(position[1], int)
                ):
                    raise ValueError("upload registration page token is invalid")
                after = (position[0],)
                frontier = position[1]
            statement = select(CollectionUploadArtifactRecord).where(
                CollectionUploadArtifactRecord.collection_id == normalized_id,
                CollectionUploadArtifactRecord.artifact_order
                <= (frontier if frontier is not None else -1),
            )
            rows, next_position = bounded_page(
                list(
                    session.scalars(
                        keyset_statement(
                            statement,
                            columns=(CollectionUploadArtifactRecord.artifact_order,),
                            position=after,
                            order="asc",
                            page_size=page_size,
                        )
                    )
                ),
                page_size=page_size,
                position_of=lambda row: (row.artifact_order,),
            )
            return {
                "collection_id": format_scalar("sequence63", normalized_id),
                "page_size": page_size,
                "_next_position": (
                    (*next_position, frontier)
                    if next_position is not None and frontier is not None
                    else None
                ),
                "artifacts": [_artifact_payload(row) for row in rows],
            }

    def iter_artifacts(self, collection_id: int) -> Iterator[dict[str, object]]:
        normalized_id = _collection_id(collection_id)
        with read_snapshot(self._session_factory) as session:
            if session.get(CollectionUploadRecord, normalized_id) is None:
                raise NotFound(f"collection upload session not found: {normalized_id}")
            statement = (
                select(CollectionUploadArtifactRecord)
                .where(CollectionUploadArtifactRecord.collection_id == normalized_id)
                .order_by(CollectionUploadArtifactRecord.artifact_order)
                .execution_options(yield_per=100)
            )
            for row in session.scalars(statement):
                yield _artifact_payload(row)

    def list_volumes(self, collection_id: int) -> dict[str, object]:
        normalized_id = _collection_id(collection_id)
        with session_scope(self._session_factory) as session:
            upload = session.get(CollectionUploadRecord, normalized_id)
            if upload is None:
                if session.get(CollectionRecord, normalized_id) is not None:
                    return {
                        "collection_id": format_scalar("sequence63", normalized_id),
                        "volumes": [],
                    }
                raise NotFound(f"collection upload session not found: {normalized_id}")
            volumes = list(
                session.scalars(
                    select(CollectionArchiveObjectUploadRecord)
                    .where(CollectionArchiveObjectUploadRecord.collection_id == normalized_id)
                    .order_by(CollectionArchiveObjectUploadRecord.sequence)
                )
            )
            return {
                "collection_id": format_scalar("sequence63", normalized_id),
                "volumes": [_volume_work_payload(row) for row in volumes],
            }

    def acquire_work(self, collection_id: int, *, limit: int) -> dict[str, object]:
        """Acquire one bounded unit per actionable volume without scanning sealed work."""

        normalized_id = _collection_id(collection_id)
        if limit < 1 or limit > 64:
            raise BadRequest("collection upload work limit must be between 1 and 64")
        with read_snapshot(self._session_factory) as session:
            upload = session.get(CollectionUploadRecord, normalized_id)
            if upload is None:
                if session.get(CollectionRecord, normalized_id) is not None:
                    return {
                        "collection_id": format_scalar("sequence63", normalized_id),
                        "planning_complete": True,
                        "complete": True,
                        "committed_payload_bytes": "0",
                        "work": [],
                    }
                raise NotFound(f"collection upload session not found: {normalized_id}")
            planning_complete = bool(_planner_checkpoint(upload).closed)
            volumes = list(
                session.scalars(
                    select(CollectionArchiveObjectUploadRecord)
                    .where(
                        CollectionArchiveObjectUploadRecord.collection_id == normalized_id,
                        CollectionArchiveObjectUploadRecord.state != "sealed",
                        or_(
                            CollectionArchiveObjectUploadRecord.kind == "pack",
                            exists(
                                select(CollectionUploadArtifactRecord.artifact_id).where(
                                    CollectionUploadArtifactRecord.collection_id == normalized_id,
                                    CollectionUploadArtifactRecord.artifact_id
                                    == CollectionArchiveObjectUploadRecord.source_artifact_id,
                                    CollectionUploadArtifactRecord.raw_parts_accepted
                                    >= (
                                        CollectionArchiveObjectUploadRecord.source_first_part
                                        + CollectionArchiveObjectUploadRecord.source_part_count
                                    ),
                                )
                            ),
                        ),
                    )
                    .order_by(CollectionArchiveObjectUploadRecord.sequence)
                    .limit(limit)
                )
            )
            work = [_unit_assignment_payload(row) for row in volumes]
            return {
                "collection_id": format_scalar("sequence63", normalized_id),
                "planning_complete": planning_complete,
                "complete": planning_complete and not work,
                "committed_payload_bytes": format_scalar(
                    "nonnegative", upload.uploaded_payload_bytes
                ),
                "work": work,
            }

    def get_volume(self, collection_id: int, volume_id: str) -> dict[str, object]:
        normalized_id = _collection_id(collection_id)
        with session_scope(self._session_factory) as session:
            record = session.get(
                CollectionArchiveObjectUploadRecord,
                (normalized_id, volume_id),
            )
            if record is None:
                raise NotFound(f"collection upload volume not found: {volume_id}")
            return _volume_work_payload(record)

    def upload_unit(
        self,
        collection_id: int,
        volume_id: str,
        unit: int,
        *,
        plan_sha256: str,
        content: bytes,
    ) -> dict[str, object]:
        normalized_id = _collection_id(collection_id)
        with session_scope(self._session_factory) as session:
            upload = session.get(CollectionUploadRecord, normalized_id)
            record = session.get(
                CollectionArchiveObjectUploadRecord,
                (normalized_id, volume_id),
            )
            if upload is None or record is None:
                raise NotFound(f"collection upload volume not found: {volume_id}")
            if upload.state not in {"open", "closing", "uploading"}:
                raise Conflict(f"collection upload session is {upload.state}: {normalized_id}")
            if plan_sha256 != record.plan_sha256:
                raise Conflict("archive upload unit plan identity changed")
            store_name = upload.archive_store
            passphrase_id = upload.passphrase_id
            kind = record.kind
            object_path = record.object_path
            relative_path = record.relative_path
            plan_json = record.plan_json
            unit_plaintext_bytes = record.unit_plaintext_bytes

        receipt: SealedPackVolume | SealedRawVolume | None
        try:
            if kind == "pack":
                pack_plan = parse_pack_volume_plan(plan_json)
                descriptor = pack_unit_descriptors(pack_plan)[unit]
                if len(content) != descriptor.payload_bytes:
                    raise ValueError("pack upload unit payload length mismatch")
                pack_uploader = self._pack_uploader(
                    self._volume_object_store(
                        store_name=store_name,
                        collection_id=normalized_id,
                        object_id=volume_id,
                    ),
                    passphrase_id=passphrase_id,
                )
                pack_checkpoint = pack_uploader.open(
                    collection_id=normalized_id,
                    plan=pack_plan,
                    object_path=object_path,
                    relative_path=relative_path,
                )
                pack_checkpoint = pack_uploader.upload_unit(
                    plan=pack_plan,
                    checkpoint=pack_checkpoint,
                    unit_number=unit,
                    payload_chunks=(content,),
                )
                receipt = (
                    pack_uploader.sealed_receipt(
                        plan=pack_plan,
                        checkpoint=pack_checkpoint,
                    )
                    if pack_checkpoint.completed is not None
                    else None
                )
            elif kind == "segment":
                raw_plan = parse_raw_volume_plan(plan_json)
                expected = self._raw_volume_digests(normalized_id, raw_plan)
                if unit < 0 or unit >= len(expected):
                    raise ValueError("raw upload unit number is outside the plan")
                expected_bytes = min(
                    unit_plaintext_bytes,
                    raw_plan.plaintext_bytes - unit * unit_plaintext_bytes,
                )
                if len(content) != expected_bytes:
                    raise ValueError("raw upload unit payload length mismatch")
                raw_uploader = self._raw_uploader(
                    self._volume_object_store(
                        store_name=store_name,
                        collection_id=normalized_id,
                        object_id=volume_id,
                    ),
                    passphrase_id=passphrase_id,
                )
                raw_checkpoint = raw_uploader.open(
                    collection_id=normalized_id,
                    plan=raw_plan,
                    object_path=object_path,
                    relative_path=relative_path,
                    target_part_plaintext_bytes=unit_plaintext_bytes,
                    expected_part_sha256s=expected,
                )
                raw_checkpoint = raw_uploader.upload_unit(
                    plan=raw_plan,
                    checkpoint=raw_checkpoint,
                    unit_number=unit + 1,
                    plaintext=content,
                )
                receipt = (
                    raw_uploader.sealed_receipt(raw_checkpoint)
                    if raw_checkpoint.completed
                    else None
                )
            else:
                raise RuntimeError(f"unsupported archive volume kind: {kind}")
        except (IndexError, ValueError) as exc:
            raise BadRequest(str(exc)) from exc

        if receipt is not None:
            self._record_sealed_volume(normalized_id, receipt)
        payload = self.get_unit(normalized_id, volume_id, unit)
        self._schedule_finalization_if_ready(normalized_id)
        return payload

    def get_unit(self, collection_id: int, volume_id: str, unit: int) -> dict[str, object]:
        normalized_id = _collection_id(collection_id)
        with read_snapshot(self._session_factory) as session:
            record = session.get(
                CollectionArchiveObjectUploadRecord,
                (normalized_id, volume_id),
            )
            if record is None or unit < 0 or unit >= record.total_units:
                raise NotFound(f"collection upload unit not found: {unit}")
            return _unit_work_payload(record, unit)

    def get(self, collection_id: int) -> dict[str, object]:
        normalized_id = _collection_id(collection_id)
        with session_scope(self._session_factory) as session:
            upload = session.get(CollectionUploadRecord, normalized_id)
            if upload is not None:
                return _upload_payload(session, upload)
            collection = session.scalar(
                select(CollectionRecord)
                .options(selectinload(CollectionRecord.archive_copies))
                .where(
                    CollectionRecord.id == normalized_id,
                    CollectionRecord.is_published.is_(True),
                )
            )
            if collection is None:
                raise NotFound(f"collection upload not found: {normalized_id}")
            return _finalized_payload(
                session,
                collection,
                store_name=collection.creation_archive_store,
            )

    def heartbeat(self, collection_id: int) -> dict[str, object]:
        """Renew one active custody-transfer construction lease."""

        normalized_id = _collection_id(collection_id)
        with session_scope(self._session_factory) as session:
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == normalized_id)
                .with_for_update()
            )
            if upload is None:
                raise NotFound(f"collection upload session not found: {normalized_id}")
            if upload.custody_mode != "custody-transfer":
                raise Conflict("collection upload does not have a custody-transfer lease")
            if upload.state not in {"open", "closing"}:
                raise Conflict(f"collection upload session is {upload.state}: {normalized_id}")
            _touch_upload(upload, config=self._config)
            return _upload_payload(session, upload)

    def reap_expired_custody_transfers(self, *, limit: int = 100) -> int:
        """Retain expired transferred custody as visible, resumable orphan state."""

        if limit < 1:
            return 0
        now = utc_timestamp_now()
        with session_scope(self._session_factory) as session:
            uploads = list(
                session.scalars(
                    select(CollectionUploadRecord)
                    .where(
                        CollectionUploadRecord.custody_mode == "custody-transfer",
                        CollectionUploadRecord.state.in_(("open", "closing")),
                        CollectionUploadRecord.lease_expires_at.is_not(None),
                        CollectionUploadRecord.lease_expires_at <= now,
                    )
                    .order_by(
                        CollectionUploadRecord.lease_expires_at,
                        CollectionUploadRecord.collection_id,
                    )
                    .limit(limit)
                    .with_for_update(skip_locked=True)
                )
            )
            for upload in uploads:
                upload.state = "orphaned"
                upload.orphaned_at = now
                upload.lease_expires_at = None
                upload.last_activity_at = now
                upload.archive_phase = "orphaned"
                upload.archive_phase_updated_at = now
            return len(uploads)

    def list(
        self,
        *,
        page_size: int,
        position: tuple[str | int | bool | bytes | None, ...] | None,
        q: str | None,
        state: str | None,
        sort: str,
        order: str,
        principal: Principal,
    ) -> dict[str, object]:
        _validate_upload_list(page_size=page_size, sort=sort, order=order)
        with read_snapshot(self._session_factory) as session:
            statement, key_columns = _upload_list_statement(
                q=q, state=state, sort=sort, order=order, principal=principal
            )
            rows, next_position = bounded_page(
                list(
                    session.execute(
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
                position_of=lambda row: _upload_list_position(row[0], sort=sort),
            )
            return {
                "page_size": page_size,
                "_next_position": next_position,
                "sort": sort,
                "order": order,
                "query": q,
                "filters": {"state": state},
                "uploads": [
                    _upload_list_payload(
                        session,
                        upload,
                        files=int(files or 0),
                        byte_count=int(byte_count or 0),
                    )
                    for upload, files, byte_count in rows
                ],
            }

    def iter_uploads(
        self,
        *,
        q: str | None,
        state: str | None,
        sort: str,
        order: str,
        principal: Principal,
    ) -> Iterator[dict[str, object]]:
        _validate_upload_list(page_size=100, sort=sort, order=order)
        statement, key_columns = _upload_list_statement(
            q=q, state=state, sort=sort, order=order, principal=principal
        )
        direction = asc if order == "asc" else desc
        statement = statement.order_by(
            *(direction(column) for column in key_columns)
        ).execution_options(yield_per=100)
        with read_snapshot(self._session_factory) as session:
            for upload, files, byte_count in session.execute(statement):
                yield _upload_list_payload(
                    session,
                    upload,
                    files=int(files or 0),
                    byte_count=int(byte_count or 0),
                )

    def cancel(self, collection_id: int) -> dict[str, object]:
        normalized_id = _collection_id(collection_id)
        with session_scope(self._session_factory) as session:
            if session.get(CollectionRecord, normalized_id) is not None:
                raise Conflict(f"collection is already finalized: {normalized_id}")
            upload = session.get(CollectionUploadRecord, normalized_id)
            if upload is None:
                raise NotFound(f"collection upload session not found: {normalized_id}")
            if any(current.state == "sealed" for current in upload.archive_objects):
                raise Conflict("collection upload with sealed archive volumes cannot be canceled")
            payload = _upload_payload(session, upload, state="canceled")
            payload.update(
                {
                    "latest_failure": None,
                    "archive_phase": "canceled",
                    "archive_phase_updated_at": utc_timestamp_now(),
                    "archive_next_attempt_at": None,
                }
            )
            store_name = upload.archive_store
            prefix = upload.archive_storage_prefix
            passphrase_id = upload.passphrase_id
            checkpoints = [
                (current.kind, current.checkpoint_json)
                for current in upload.archive_objects
                if current.checkpoint_json
            ]
        for kind, checkpoint_json in checkpoints:
            if checkpoint_json is None:
                continue
            if kind == "pack":
                pack_checkpoint = PackUploadCheckpoint.from_json(checkpoint_json)
                if pack_checkpoint.completed is None:
                    self._pack_uploader(
                        self._volume_object_store(
                            store_name=store_name,
                            collection_id=normalized_id,
                            object_id=pack_checkpoint.volume_id,
                        ),
                        passphrase_id=passphrase_id,
                    ).abort(pack_checkpoint)
            else:
                raw_checkpoint = RawUploadCheckpoint.from_json(checkpoint_json)
                if raw_checkpoint.completed is None:
                    self._raw_uploader(
                        self._volume_object_store(
                            store_name=store_name,
                            collection_id=normalized_id,
                            object_id=raw_checkpoint.volume_id,
                        ),
                        passphrase_id=passphrase_id,
                    ).abort(raw_checkpoint)
        with session_scope(self._session_factory) as session:
            upload = session.get(CollectionUploadRecord, normalized_id)
            if upload is not None:
                _cancel_copy_intents(session, normalized_id)
                session.delete(upload)
        if prefix:
            self._archive_stores.require(store_name).store.discard_collection_archive_upload(
                archive_storage_prefix=prefix
            )
        return payload

    def plan_orphan_discard(self, collection_id: int) -> dict[str, object]:
        normalized_id = _collection_id(collection_id)
        expires = (utc_now() + PLAN_TTL).replace(microsecond=0)
        with session_scope(self._session_factory) as session:
            plan = _orphan_discard_plan(
                session,
                collection_id=normalized_id,
                expires_at=format_utc_timestamp(expires),
            )
        plan["challenge"] = (
            None if plan["blockers"] else plan_challenge(_DISCARD_CHALLENGE_PREFIX, plan, expires)
        )
        return plan

    def discard_orphan(self, collection_id: int, *, challenge: str) -> dict[str, object]:
        normalized_id = _collection_id(collection_id)
        supplied = challenge.strip()
        if not supplied:
            raise BadRequest("collection upload discard challenge is required")
        expires = challenge_expiry(
            supplied,
            prefix=_DISCARD_CHALLENGE_PREFIX,
            operation="collection upload discard",
        )
        if utc_now() > expires:
            raise Conflict("collection upload discard plan has expired; request a new plan")
        checkpoints: list[tuple[str, str]] = []
        store_name = ""
        prefix = ""
        passphrase_id = ""
        with session_scope(self._session_factory) as session:
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == normalized_id)
                .with_for_update()
            )
            if upload is None:
                if not challenge_has_shape(supplied, prefix=_DISCARD_CHALLENGE_PREFIX):
                    raise NotFound(f"collection upload session not found: {normalized_id}")
                return {
                    "status": "already_absent",
                    "collection_id": format_scalar("sequence63", normalized_id),
                    "files": 0,
                    "bytes": 0,
                    "custody": {"state": "complete"},
                    "archive_objects": 0,
                }
            plan = _orphan_discard_plan(
                session,
                collection_id=normalized_id,
                expires_at=format_utc_timestamp(expires),
            )
            if not secrets.compare_digest(
                plan_challenge(_DISCARD_CHALLENGE_PREFIX, plan, expires),
                supplied,
            ):
                raise Conflict("collection upload discard plan changed; request a new plan")
            blockers_value = plan["blockers"]
            if not isinstance(blockers_value, list):
                raise RuntimeError("collection upload discard plan blockers are invalid")
            blockers = [str(value) for value in blockers_value]
            if blockers:
                raise Conflict("collection upload discard is blocked: " + "; ".join(blockers))
            checkpoints = [
                (current.kind, current.checkpoint_json)
                for current in upload.archive_objects
                if current.checkpoint_json is not None
            ]
            store_name = upload.archive_store
            prefix = upload.archive_storage_prefix
            passphrase_id = upload.passphrase_id
            result = {
                "status": "discarded",
                "collection_id": format_scalar("sequence63", normalized_id),
                "files": plan["files"],
                "bytes": plan["bytes"],
                "custody": plan["custody"],
                "archive_objects": plan["archive_objects"],
            }
            now = utc_timestamp_now()
            upload.state = "discarding"
            upload.archive_phase = "discarding"
            upload.archive_phase_updated_at = now
            upload.last_activity_at = now
        try:
            for kind, checkpoint_json in checkpoints:
                if kind == "pack":
                    pack_checkpoint = PackUploadCheckpoint.from_json(checkpoint_json)
                    if pack_checkpoint.completed is None:
                        self._pack_uploader(
                            self._volume_object_store(
                                store_name=store_name,
                                collection_id=normalized_id,
                                object_id=pack_checkpoint.volume_id,
                            ),
                            passphrase_id=passphrase_id,
                        ).abort(pack_checkpoint)
                else:
                    raw_checkpoint = RawUploadCheckpoint.from_json(checkpoint_json)
                    if raw_checkpoint.completed is None:
                        self._raw_uploader(
                            self._volume_object_store(
                                store_name=store_name,
                                collection_id=normalized_id,
                                object_id=raw_checkpoint.volume_id,
                            ),
                            passphrase_id=passphrase_id,
                        ).abort(raw_checkpoint)
            self._archive_stores.require(store_name).store.discard_collection_archive_upload(
                archive_storage_prefix=prefix
            )
        except Exception as exc:
            with session_scope(self._session_factory) as session:
                upload = session.get(CollectionUploadRecord, normalized_id)
                if upload is not None and upload.state == "discarding":
                    now = utc_timestamp_now()
                    upload.state = "orphaned"
                    upload.orphaned_at = now
                    upload.archive_phase = "orphaned"
                    upload.archive_phase_updated_at = now
                    upload.archive_failure = str(exc)
            raise
        with session_scope(self._session_factory) as session:
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == normalized_id)
                .with_for_update()
            )
            if upload is not None:
                if upload.state != "discarding":
                    raise RuntimeError("collection upload discard state changed unexpectedly")
                _cancel_copy_intents(session, normalized_id)
                session.delete(upload)
        return result

    def _pack_uploader(
        self,
        object_store: ArchiveResumableObjectStore,
        *,
        passphrase_id: str,
    ) -> PackVolumeUploader:
        passphrase = self._config.archive_passphrase_for(passphrase_id)
        return PackVolumeUploader(
            object_store=object_store,
            checkpoint_store=self._checkpoints,
            passphrase=passphrase,
            scrypt_log_n=self._config.archive_scrypt_work_factor,
            source_read_chunk_bytes=self._throughput.source_read_chunk_bytes,
            resources=self._resources,
            timing_observer=log_transfer_timing,
            session_cache=self._age_sessions[passphrase_id],
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
                select(CollectionUploadRecord.use_cache).where(
                    CollectionUploadRecord.collection_id == collection_id
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
            owner=f"collection-upload:{collection_id}:{object_id}",
        )

    def _raw_uploader(
        self,
        object_store: ArchiveResumableObjectStore,
        *,
        passphrase_id: str,
    ) -> RawVolumeUploader:
        passphrase = self._config.archive_passphrase_for(passphrase_id)
        return RawVolumeUploader(
            object_store=object_store,
            checkpoint_store=self._checkpoints,
            passphrase=passphrase,
            scrypt_log_n=self._config.archive_scrypt_work_factor,
            source_read_chunk_bytes=self._throughput.source_read_chunk_bytes,
            resources=self._resources,
            timing_observer=log_transfer_timing,
            session_cache=self._age_sessions[passphrase_id],
        )

    def _raw_volume_digests(
        self,
        collection_id: int,
        plan: RawVolumePlan,
    ) -> tuple[str, ...]:
        with session_scope(self._session_factory) as session:
            artifact = session.get(
                CollectionUploadArtifactRecord, (collection_id, plan.artifact_id)
            )
            if (
                artifact is None
                or artifact.raw_part_plaintext_bytes is None
                or artifact.raw_part_count is None
                or artifact.raw_part_ordered_sha256 is None
            ):
                raise RuntimeError("raw volume source digest authority is missing")
            summary = RawSourceDigestSummary(
                artifact_id=ArtifactId(artifact.artifact_id),
                bytes=artifact.bytes,
                sha256=artifact.sha256,
                part_plaintext_bytes=artifact.raw_part_plaintext_bytes,
                part_count=artifact.raw_part_count,
                ordered_part_sha256=artifact.raw_part_ordered_sha256,
            )
            first, count = raw_volume_part_span(
                summary,
                file_offset=plan.artifact_offset,
                plaintext_bytes=plan.plaintext_bytes,
            )
            values = tuple(
                session.scalars(
                    select(CollectionUploadRawPartDigestRecord.sha256)
                    .where(
                        CollectionUploadRawPartDigestRecord.collection_id == collection_id,
                        CollectionUploadRawPartDigestRecord.artifact_id == plan.artifact_id,
                        CollectionUploadRawPartDigestRecord.part_number >= first,
                        CollectionUploadRawPartDigestRecord.part_number < first + count,
                    )
                    .order_by(CollectionUploadRawPartDigestRecord.part_number)
                )
            )
        if len(values) != count:
            raise RuntimeError("raw volume source digest rows are incomplete")
        return values

    def _record_sealed_volume(
        self,
        collection_id: int,
        receipt: SealedPackVolume | SealedRawVolume,
    ) -> None:
        now = utc_timestamp_now()
        with session_scope(self._session_factory) as session:
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == collection_id)
                .with_for_update()
            )
            record = session.get(
                CollectionArchiveObjectUploadRecord,
                (collection_id, receipt.volume_id),
            )
            if record is None:
                raise RuntimeError("sealed archive volume plan disappeared")
            encoded = _sealed_volume_json(receipt)
            if record.sealed_receipt_json not in {None, encoded}:
                raise RuntimeError("sealed archive volume receipt changed")
            record.sealed_receipt_json = encoded
            record.state = "sealed"
            record.sealed_at = receipt.completed_at
            record.updated_at = now
            record.uploaded_units = len(receipt.parts)
            record.uploaded_bytes = receipt.stored_bytes
            if upload is not None:
                _touch_upload(upload, config=self._config, now=now)
                upload.archive_phase_updated_at = now
                _record_payload_custody_progress(session, upload, record, now=now)

    def requeue_interrupted_finalizations_for_startup(self, *, limit: int = 100) -> int:
        if limit < 1:
            return 0
        now = utc_timestamp_now()
        requeued = 0
        with session_scope(self._session_factory) as session:
            uploads = list(
                session.scalars(
                    select(CollectionUploadRecord)
                    .where(CollectionUploadRecord.state.in_(("closing", "uploading", "finalizing")))
                    .order_by(
                        CollectionUploadRecord.archive_phase_updated_at,
                        CollectionUploadRecord.collection_id,
                    )
                    .with_for_update(skip_locked=True)
                    .limit(limit)
                )
            )
            for upload in uploads:
                interrupted = upload.state == "finalizing" and upload.archive_phase == "finalizing"
                if not interrupted and not _ready_for_finalization(session, upload):
                    continue
                upload.state = "finalizing"
                upload.archive_phase = "retry_wait" if interrupted else "finalization_queued"
                upload.archive_next_attempt_at = now
                upload.archive_phase_updated_at = now
                if interrupted:
                    upload.archive_failure = "archive finalization interrupted before completion"
                else:
                    upload.archive_failure = None
                requeued += 1
        return requeued

    def requeue_interrupted_orphan_discards_for_startup(self, *, limit: int = 100) -> int:
        """Return interrupted guarded cleanup to visible, retryable orphan state."""

        if limit < 1:
            return 0
        now = utc_timestamp_now()
        with session_scope(self._session_factory) as session:
            uploads = list(
                session.scalars(
                    select(CollectionUploadRecord)
                    .where(CollectionUploadRecord.state == "discarding")
                    .order_by(CollectionUploadRecord.collection_id)
                    .with_for_update(skip_locked=True)
                    .limit(limit)
                )
            )
            for upload in uploads:
                upload.state = "orphaned"
                upload.orphaned_at = now
                upload.archive_phase = "orphaned"
                upload.archive_phase_updated_at = now
                upload.archive_failure = "orphan discard interrupted before cleanup completed"
            return len(uploads)

    def process_due_finalizations(self, *, limit: int = 1) -> int:
        if limit < 1:
            return 0
        self._schedule_ready_finalizations(limit=max(100, limit))
        processed = 0
        for _ in range(limit):
            collection_id = self._claim_due_finalization()
            if collection_id is None:
                break
            try:
                self._reconcile_sealed_volume_receipts(collection_id)
                self._finalize(collection_id)
            except Exception as exc:
                self._record_finalization_retry(collection_id, exc)
                _LOG.exception(
                    "collection archive finalization failed; retry scheduled: collection_id=%s",
                    collection_id,
                )
            processed += 1
        return processed

    def _schedule_ready_finalizations(self, *, limit: int) -> int:
        now = utc_timestamp_now()
        scheduled = 0
        with session_scope(self._session_factory) as session:
            uploads = list(
                session.scalars(
                    select(CollectionUploadRecord)
                    .where(CollectionUploadRecord.state.in_(("closing", "uploading")))
                    .order_by(
                        CollectionUploadRecord.archive_phase_updated_at,
                        CollectionUploadRecord.collection_id,
                    )
                    .with_for_update(skip_locked=True)
                    .limit(limit)
                )
            )
            for upload in uploads:
                if not _ready_for_finalization(session, upload):
                    continue
                _mark_finalization_ready(upload, now=now)
                scheduled += 1
        return scheduled

    def _claim_due_finalization(self) -> int | None:
        now = utc_timestamp_now()
        with session_scope(self._session_factory) as session:
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(
                    CollectionUploadRecord.state == "finalizing",
                    CollectionUploadRecord.archive_phase.in_(("finalization_queued", "retry_wait")),
                    CollectionUploadRecord.archive_next_attempt_at.is_not(None),
                    CollectionUploadRecord.archive_next_attempt_at <= now,
                )
                .order_by(
                    CollectionUploadRecord.archive_next_attempt_at,
                    CollectionUploadRecord.collection_id,
                )
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if upload is None:
                return None
            if not _ready_for_finalization(session, upload):
                upload.state = "uploading"
                upload.archive_phase = "uploading"
                upload.archive_next_attempt_at = None
                upload.archive_phase_updated_at = now
                return None
            upload.state = "finalizing"
            upload.archive_phase = "finalizing"
            upload.archive_phase_updated_at = now
            upload.archive_last_attempt_at = now
            upload.archive_next_attempt_at = None
            upload.archive_failure = None
            return upload.collection_id

    def _schedule_finalization_if_ready(self, collection_id: int) -> None:
        now = utc_timestamp_now()
        with session_scope(self._session_factory) as session:
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == collection_id)
                .with_for_update()
            )
            if upload is None or not _ready_for_finalization(session, upload):
                return
            _mark_finalization_ready(upload, now=now)

    def _reconcile_sealed_volume_receipts(self, collection_id: int) -> None:
        with session_scope(self._session_factory) as session:
            upload = session.get(CollectionUploadRecord, collection_id)
            if upload is None:
                return
            store_name = upload.archive_store
            passphrase_id = upload.passphrase_id
            pending = list(
                session.execute(
                    select(
                        CollectionArchiveObjectUploadRecord.object_id,
                        CollectionArchiveObjectUploadRecord.kind,
                        CollectionArchiveObjectUploadRecord.plan_json,
                        CollectionArchiveObjectUploadRecord.checkpoint_json,
                    )
                    .where(
                        CollectionArchiveObjectUploadRecord.collection_id == collection_id,
                        CollectionArchiveObjectUploadRecord.state == "sealed",
                        CollectionArchiveObjectUploadRecord.sealed_receipt_json.is_(None),
                    )
                    .order_by(CollectionArchiveObjectUploadRecord.sequence)
                    .limit(64)
                )
            )
        for volume_id, kind, plan_json, checkpoint_json in pending:
            if checkpoint_json is None:
                raise RuntimeError(f"sealed archive volume has no checkpoint: {volume_id}")
            if kind == "pack":
                checkpoint = PackUploadCheckpoint.from_json(checkpoint_json)
                if checkpoint.completed is None:
                    raise RuntimeError(f"sealed pack volume is incomplete: {volume_id}")
                receipt: SealedPackVolume | SealedRawVolume = self._pack_uploader(
                    self._volume_object_store(
                        store_name=store_name,
                        collection_id=collection_id,
                        object_id=volume_id,
                    ),
                    passphrase_id=passphrase_id,
                ).sealed_receipt(
                    plan=parse_pack_volume_plan(plan_json),
                    checkpoint=checkpoint,
                )
            elif kind == "segment":
                raw_checkpoint = RawUploadCheckpoint.from_json(checkpoint_json)
                if not raw_checkpoint.completed:
                    raise RuntimeError(f"sealed raw volume is incomplete: {volume_id}")
                receipt = self._raw_uploader(
                    self._volume_object_store(
                        store_name=store_name,
                        collection_id=collection_id,
                        object_id=volume_id,
                    ),
                    passphrase_id=passphrase_id,
                ).sealed_receipt(raw_checkpoint)
            else:
                raise RuntimeError(f"unsupported archive volume kind: {kind}")
            self._record_sealed_volume(collection_id, receipt)

    def _record_finalization_retry(self, collection_id: int, exc: Exception) -> None:
        now = utc_timestamp_now()
        with session_scope(self._session_factory) as session:
            upload = session.get(CollectionUploadRecord, collection_id)
            if upload is None:
                return
            delay = min(3600, 2 ** min(upload.archive_attempt_count, 10))
            upload.state = "finalizing"
            upload.archive_phase = "retry_wait"
            upload.archive_phase_updated_at = now
            upload.archive_next_attempt_at = format_utc_timestamp(
                utc_now() + timedelta(seconds=delay)
            )
            upload.archive_failure = f"{type(exc).__name__}: {exc}"[:1000]

    def _finalize(self, collection_id: int) -> None:
        if self._stage_next_member_histories(collection_id):
            self._requeue_finalization_step(collection_id)
            return
        if self._advance_provenance_closure_validation(collection_id):
            self._requeue_finalization_step(collection_id)
            return
        if self._publish_next_provenance_structure(collection_id):
            self._requeue_finalization_step(collection_id)
            return
        if self._advance_archive_tree_checkpoint(collection_id):
            self._requeue_finalization_step(collection_id)
            return
        if self._publish_next_archive_volume_metadata(collection_id):
            self._requeue_finalization_step(collection_id)
            return
        if self._publish_next_provenance_archive_object(collection_id):
            self._requeue_finalization_step(collection_id)
            return
        if self._publish_final_authority(collection_id):
            self._requeue_finalization_step(collection_id)
            return
        if self._publish_custody_receipts(collection_id):
            self._requeue_finalization_step(collection_id)
            return
        if self._publish_initial_tags(collection_id):
            self._requeue_finalization_step(collection_id)
            return
        if self._publish_initial_description(collection_id):
            self._requeue_finalization_step(collection_id)
            return
        if self._advance_catalog_projection(collection_id):
            self._requeue_finalization_step(collection_id)

    def _publish_final_authority(self, collection_id: int) -> bool:
        """Publish the bounded immutable root once and persist its exact receipts."""

        with session_scope(self._session_factory) as session:
            upload = session.get(CollectionUploadRecord, collection_id)
            if upload is None or upload.final_authority_json is not None:
                return False
            if (
                upload.archive_tree_sha256 is None
                or upload.archive_ordered_volume_sha256 is None
                or upload.archive_terminal_receipt_json is None
            ):
                raise RuntimeError("archive authority checkpoints are incomplete")
            store_name = upload.archive_store
            prefix = upload.archive_storage_prefix
            encryption_format = upload.encryption_format
            passphrase_id = upload.passphrase_id
            sealed_provenance = _sealed_upload_provenance(upload)
            manifest = build_collection_archive_root_manifest(
                archive_generation=upload.archive_generation,
                artifact_set={
                    "count": int(upload.artifact_count),
                    "bytes": int(upload.artifact_bytes),
                    "sha256": upload.archive_tree_sha256,
                },
                ordered_volume_sha256=upload.archive_ordered_volume_sha256,
                provenance_identity=sealed_provenance.identity,
                provenance_objects=(sealed_provenance.root,),
            )
        if not prefix:
            raise RuntimeError("collection archive storage prefix is missing")
        self._begin_final_publication_attempt(collection_id)
        passphrase = self._config.archive_passphrase_for(passphrase_id)
        archive_store = self._archive_stores.require(store_name)
        root = ArchiveRootPublisher(
            object_store=archive_store.immutable_objects,
            passphrase=passphrase,
            scrypt_log_n=self._config.archive_scrypt_work_factor,
        ).publish_root_manifest(archive_storage_prefix=prefix, manifest=manifest)
        recovery = ArchiveRecoveryDescriptorPublisher(
            object_store=archive_store.immutable_objects
        ).publish(
            archive_storage_prefix=prefix,
            root=root,
            encryption=CollectionEncryptionBinding(
                format=encryption_format,
                passphrase_id=passphrase_id,
            ),
        )
        authority = {
            "root": {
                "object_path": root.object_path,
                "relative_path": root.relative_path,
                "revision": root.revision,
                "plaintext_bytes": root.plaintext_bytes,
                "plaintext_sha256": root.plaintext_sha256,
                "stored_bytes": root.stored_bytes,
                "stored_sha256": root.stored_sha256,
                "artifact_set_sha256": root.artifact_set_sha256,
                "artifact_count": root.artifact_count,
                "artifact_bytes": root.artifact_bytes,
                "completed_at": root.completed_at,
            },
            "recovery": {
                "object_path": recovery.object_path,
                "relative_path": recovery.relative_path,
                "revision": recovery.revision,
                "bytes": recovery.bytes,
                "sha256": recovery.sha256,
                "completed_at": recovery.completed_at,
            },
        }
        encoded = json.dumps(authority, sort_keys=True, separators=(",", ":"))
        with session_scope(self._session_factory) as session:
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == collection_id)
                .with_for_update()
            )
            if upload is not None:
                if upload.final_authority_json not in {None, encoded}:
                    raise RuntimeError("final archive authority receipt changed")
                upload.final_authority_json = encoded
        return True

    def _publish_custody_receipts(self, collection_id: int) -> bool:
        """Finalize remaining receipts through the same early custody rail."""
        with read_snapshot(self._session_factory) as session:
            artifact_id = session.scalar(
                select(CollectionUploadArtifactRecord.artifact_id)
                .where(
                    CollectionUploadArtifactRecord.collection_id == collection_id,
                    CollectionUploadArtifactRecord.custody_receipt_json.is_(None),
                )
                .order_by(CollectionUploadArtifactRecord.artifact_id)
                .limit(1)
            )
        if artifact_id is None:
            return False
        return self._advance_artifact_custody(collection_id, str(artifact_id))

    def process_due_custody_receipts(self, *, limit: int = 1) -> int:
        progressed = 0
        with read_snapshot(self._session_factory) as session:
            candidates = list(
                session.execute(
                    select(
                        CollectionUploadArtifactRecord.collection_id,
                        CollectionUploadArtifactRecord.artifact_id,
                    )
                    .join(
                        CollectionUploadRecord,
                        CollectionUploadRecord.collection_id
                        == CollectionUploadArtifactRecord.collection_id,
                    )
                    .join(
                        CollectionUploadArtifactProvenanceBindingRecord,
                        (
                            CollectionUploadArtifactProvenanceBindingRecord.collection_id
                            == CollectionUploadArtifactRecord.collection_id
                        )
                        & (
                            CollectionUploadArtifactProvenanceBindingRecord.artifact_id
                            == CollectionUploadArtifactRecord.artifact_id
                        ),
                    )
                    .where(
                        CollectionUploadArtifactRecord.payload_sealed_at.is_not(None),
                        CollectionUploadArtifactRecord.custody_receipt_json.is_(None),
                        or_(
                            ~CollectionUploadRecord.initiated_by_principal_id.startswith(
                                "processing:"
                            ),
                            CollectionUploadRecord.completion_requirement_json.is_not(None),
                        ),
                        or_(
                            CollectionUploadRecord.completion_requirement_json.is_(None),
                            CollectionUploadArtifactProvenanceBindingRecord.history_imports_ref_json.is_not(
                                None
                            ),
                        ),
                        CollectionUploadRecord.state.in_(
                            ("open", "closing", "uploading", "finalizing")
                        ),
                    )
                    .order_by(
                        CollectionUploadArtifactRecord.collection_id,
                        CollectionUploadArtifactRecord.artifact_id,
                    )
                    .limit(max(0, limit))
                )
            )
        for collection_id, artifact_id in candidates:
            try:
                progressed += int(
                    self._advance_artifact_custody(int(collection_id), str(artifact_id))
                )
            except Exception:
                _LOG.exception(
                    "early artifact custody remains pending: collection_id=%s artifact_id=%s",
                    collection_id,
                    artifact_id,
                )
        return progressed

    def _advance_artifact_custody(self, collection_id: int, artifact_id: str) -> bool:
        """Seal one required object, or acknowledge a fully verified early primary.

        Store writes precede durable receipt publication. Each step rechecks the
        immutable construction; retries reuse content addresses and never claim
        completion, discoverable publication or source-retirement permission.
        """
        pending_segment = None
        pending_structure = None
        receipt = None
        with read_snapshot(self._session_factory) as session:
            upload = session.get(CollectionUploadRecord, collection_id)
            artifact = session.get(CollectionUploadArtifactRecord, (collection_id, artifact_id))
            binding_row = session.get(
                CollectionUploadArtifactProvenanceBindingRecord, (collection_id, artifact_id)
            )
            if (
                upload is None
                or artifact is None
                or binding_row is None
                or upload.state not in {"open", "closing", "uploading", "finalizing"}
                or artifact.payload_sealed_at is None
                or artifact.custody_receipt_json is not None
            ):
                return False
            requirement = (
                None
                if upload.completion_requirement_json is None
                else CollectionCompletionRequirementDocument.model_validate_json(
                    upload.completion_requirement_json
                )
            )
            if upload.initiated_by_principal_id.startswith("processing:") and requirement is None:
                return False
            if requirement is not None and binding_row.history_imports_ref_json is None:
                return False
            primary = CollectionArtifactProvenanceBindingDocument.model_validate(
                _provenance_binding_row(binding_row)
            )
            accepted_inputs = binding_row.history_imports_ref_json
            accepted_requirement = upload.completion_requirement_json
            anchor = HistoryJournalAnchor.from_mapping(primary.journal.model_dump(mode="json"))
            prefix, store_name = upload.archive_storage_prefix, upload.archive_store
            passphrase = self._config.archive_passphrase_for(upload.passphrase_id)
            with (
                _staged_history_closure(session, collection_id) as closure,
                ProvenanceCustodySet() as objects,
            ):
                summary = closure.summary_at(anchor)
                verify_member_binding(
                    member=ArtifactMemberIdentityDocument.model_validate(
                        {
                            "artifact_id": artifact_id,
                            "bytes": str(artifact.bytes),
                            "sha256": artifact.sha256,
                        }
                    ),
                    binding=primary,
                    summary=summary,
                    delivery_context_id=upload.delivery_context_id,
                )
                validate_collection_production_records(
                    summary.graph, delivery_context_id=upload.delivery_context_id
                )
                association = summary.graph_validation.objects[primary.delivery_association_id]
                output_id = validate_member_completion_requirement(
                    summary.graph,
                    delivery_context_id=upload.delivery_context_id,
                    state_id=association["state"]["object_id"],
                    requirement=requirement,
                )
                closure.resolve_snapshot(anchor)
                if binding_row.history_imports_ref_json is not None:
                    authority = RecordSetRef.from_mapping(
                        json.loads(binding_row.history_imports_ref_json)
                    )
                    verify_record_pages(
                        authority, _iter_staged_history_pages(session, collection_id, authority)
                    )
                    for page in _iter_staged_history_pages(session, collection_id, authority):
                        structure = session.get(
                            CollectionUploadProvenanceStructureRecord,
                            (
                                collection_id,
                                provenance_structure_identity(page.to_json_bytes()).object_id,
                            ),
                        )
                        if structure is None or structure.receipt_json is None:
                            pending_structure = provenance_structure_identity(
                                page.to_json_bytes()
                            ).object_id
                            break
                        objects.add(_provenance_custody_object(structure.receipt_json))
                        for row in page.records:
                            closure.resolve_import(MemberHistoryImport.from_mapping(row["value"]))
                if pending_structure is None:
                    for content in closure.structure_objects():
                        identity = provenance_structure_identity(content)
                        structure = session.get(
                            CollectionUploadProvenanceStructureRecord,
                            (collection_id, identity.object_id),
                        )
                        if structure is None or structure.receipt_json is None:
                            pending_structure = identity.object_id
                            break
                        objects.add(_provenance_custody_object(structure.receipt_json))
                if pending_structure is None:
                    for selected in closure.journal_anchors():
                        journal = session.get(
                            CollectionUploadProvenanceJournalRecord,
                            (collection_id, selected.journal_id),
                        )
                        if journal is None or journal.state != "sealed":
                            raise Conflict("early primary dependency lacks sealed custody")
                        for offset in range(
                            0, selected.prefix_bytes, PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX
                        ):
                            size = min(
                                PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX, selected.prefix_bytes - offset
                            )
                            content = _upload_journal_range_bytes(
                                session, collection_id, journal.journal_id, offset=offset, size=size
                            )
                            segment_sha256 = hashlib.sha256(content).hexdigest()
                            sealed = session.get(
                                CollectionUploadProvenanceCustodyObjectRecord,
                                (collection_id, journal.journal_id, offset, segment_sha256),
                            )
                            if sealed is None:
                                pending_segment = (
                                    journal.journal_id,
                                    journal.sha256,
                                    offset,
                                    content,
                                )
                                break
                            objects.add(_provenance_custody_object(sealed.receipt_json))
                        if pending_segment is not None:
                            break
                if pending_structure is None and pending_segment is None:

                    def archive_objects() -> Iterator[CollectionUploadCustodyObjectDocument]:
                        for volume in session.scalars(
                            select(CollectionArchiveObjectUploadRecord)
                            .join(
                                CollectionUploadArtifactVolumeRecord,
                                (
                                    CollectionUploadArtifactVolumeRecord.collection_id
                                    == CollectionArchiveObjectUploadRecord.collection_id
                                )
                                & (
                                    CollectionUploadArtifactVolumeRecord.object_id
                                    == CollectionArchiveObjectUploadRecord.object_id
                                ),
                            )
                            .where(
                                CollectionUploadArtifactVolumeRecord.collection_id == collection_id,
                                CollectionUploadArtifactVolumeRecord.artifact_id == artifact_id,
                            )
                            .order_by(CollectionArchiveObjectUploadRecord.object_id)
                            .execution_options(yield_per=16)
                        ):
                            if volume.state != "sealed" or volume.sealed_receipt_json is None:
                                raise Conflict(
                                    "early payload lacks complete durable volume custody"
                                )
                            yield CollectionUploadCustodyObjectDocument(
                                volume_id=volume.object_id,
                                sealed_receipt_sha256=hashlib.sha256(
                                    volume.sealed_receipt_json.encode("utf-8")
                                ).hexdigest(),
                            )

                    receipt = CollectionUploadArtifactCustodyReceiptDocument.seal(
                        collection_id=collection_id,
                        artifact_id=ArtifactId(artifact_id),
                        bytes=artifact.bytes,
                        sha256=artifact.sha256,
                        primary=primary,
                        completion_requirement_sha256=None
                        if requirement is None
                        else requirement.identity,
                        archive_objects=archive_objects(),
                        provenance_objects=objects.objects(),
                    )
        if pending_structure is not None:
            return self._publish_next_provenance_structure(
                collection_id, selected_object_id=pending_structure
            )
        if pending_segment is not None:
            journal_id, journal_sha256, offset, content = pending_segment
            publisher = ArchiveProvenancePublisher(
                object_store=self._archive_stores.require(store_name).immutable_objects,
                passphrase=passphrase,
                scrypt_log_n=self._config.archive_scrypt_work_factor,
            )
            sealed_segment = publisher.publish_journal_segment(
                archive_storage_prefix=prefix, content=content
            )
            with session_scope(self._session_factory) as session:
                journal = session.get(
                    CollectionUploadProvenanceJournalRecord, (collection_id, journal_id)
                )
                if journal is None or journal.sha256 != journal_sha256:
                    raise Conflict("canonical custody journal changed during durable publication")
                old = session.get(
                    CollectionUploadProvenanceCustodyObjectRecord,
                    (collection_id, journal_id, offset, sealed_segment.plaintext_sha256),
                )
                if old is None:
                    session.add(
                        CollectionUploadProvenanceCustodyObjectRecord(
                            collection_id=collection_id,
                            journal_id=journal_id,
                            byte_offset=offset,
                            plaintext_bytes=len(content),
                            plaintext_sha256=sealed_segment.plaintext_sha256,
                            relative_path=sealed_segment.relative_path,
                            receipt_json=_sealed_provenance_object_json(sealed_segment),
                        )
                    )
            return True
        if receipt is None:
            return False
        with session_scope(self._session_factory) as session:
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == collection_id)
                .with_for_update()
            )
            artifact = session.get(CollectionUploadArtifactRecord, (collection_id, artifact_id))
            binding_row = session.get(
                CollectionUploadArtifactProvenanceBindingRecord, (collection_id, artifact_id)
            )
            if (
                upload is None
                or artifact is None
                or binding_row is None
                or upload.state not in {"open", "closing", "uploading", "finalizing"}
                or _provenance_binding_row(binding_row) != receipt.primary.model_dump(mode="json")
                or upload.completion_requirement_json != accepted_requirement
                or binding_row.history_imports_ref_json != accepted_inputs
            ):
                raise Conflict("early primary construction changed before receipt publication")
            if artifact.custody_receipt_json is None:
                artifact.custody_receipt_json = receipt.model_dump_json()
                binding_row.completion_output_id = output_id
                upload.safe_release_artifact_count += 1
                upload.safe_release_artifact_bytes += artifact.bytes
        return True

    def _publish_initial_description(self, collection_id: int) -> bool:
        """Establish the initial description state after the root and before catalog publish."""

        with session_scope(self._session_factory) as session:
            upload = session.get(CollectionUploadRecord, collection_id)
            if upload is None or upload.final_authority_json is None:
                return False
            if upload.description_identity is not None:
                return False
            authority = _final_authority(upload)
            archive_root_sha256 = str(authority["root"]["plaintext_sha256"])
            revision = 1 if upload.description is not None else 0
            identity = collection_description_identity(
                archive_root_sha256=archive_root_sha256,
                revision=revision,
                description=upload.description,
            )
            if revision == 0:
                upload.description_revision = revision
                upload.description_identity = identity
                return True
            if not upload.archive_storage_prefix:
                raise RuntimeError("collection archive storage prefix is missing")
            document = CollectionDescriptionDocument(
                archive_root_sha256=archive_root_sha256,
                revision=revision,
                description=upload.description,
                description_identity=identity,
            ).to_json_bytes()
            store_name = upload.archive_store
            prefix = upload.archive_storage_prefix
            passphrase_id = upload.passphrase_id

        receipt = self._archive_stores.require(store_name).store.publish_collection_description(
            collection_id=collection_id,
            archive_storage_prefix=prefix,
            document=document,
            passphrase_id=passphrase_id,
        )
        receipt_json = json.dumps(
            {
                "object_path": receipt.object_path,
                "provider_revision": receipt.revision,
                "stored_bytes": receipt.stored_bytes,
                "stored_sha256": receipt.stored_sha256,
                "published_at": receipt.published_at,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        with session_scope(self._session_factory) as session:
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == collection_id)
                .with_for_update()
            )
            if upload is None:
                return False
            if upload.description_identity not in {None, identity}:
                raise RuntimeError("initial collection description identity changed")
            upload.description_revision = revision
            upload.description_identity = identity
            upload.description_publication_receipt_json = receipt_json
        return True

    def _publish_initial_tags(self, collection_id: int) -> bool:
        """Advance one bounded step of revision-one tag-authority publication."""

        with session_scope(self._session_factory) as session:
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == collection_id)
                .with_for_update()
            )
            if upload is None or upload.final_authority_json is None:
                return False
            if upload.tag_publication_receipt_json is not None:
                return False
            if upload.tag_set_identity is None:
                expected_staging_identity = CollectionTagSetRoot.seal(
                    upload.tag_staging_root_sha256
                ).tag_set_identity
                if upload.tag_staging_set_identity != expected_staging_identity:
                    raise RuntimeError("staged collection tag identity is inconsistent")
                authority = _final_authority(upload)
                head = CollectionTagHeadDocument.seal(
                    archive_root_sha256=str(authority["root"]["plaintext_sha256"]),
                    revision=1,
                    root_sha256=upload.tag_staging_root_sha256,
                )
                upload.tag_revision = head.revision
                upload.tag_root_sha256 = head.root_sha256
                upload.tag_set_identity = head.tag_set_identity
                upload.tag_head_identity = head.head_identity
                if head.root_sha256 is not None:
                    session.add(
                        CollectionUploadTagPublicationFrontierRecord(
                            collection_id=collection_id,
                            node_digest=head.root_sha256,
                        )
                    )
                return True

            frontier = session.scalar(
                select(CollectionUploadTagPublicationFrontierRecord)
                .where(
                    CollectionUploadTagPublicationFrontierRecord.collection_id == collection_id,
                    CollectionUploadTagPublicationFrontierRecord.expanded.is_(False),
                )
                .order_by(CollectionUploadTagPublicationFrontierRecord.node_digest)
                .limit(1)
            )
            if frontier is not None:
                node_record = session.get(CollectionTagNodeRecord, frontier.node_digest)
                if node_record is None:
                    raise RuntimeError("initial collection tag node is unavailable")
                node = decode_collection_tag_node(node_record.encoded)
                for child in node.children:
                    existing = session.get(
                        CollectionUploadTagPublicationFrontierRecord,
                        (collection_id, child.digest),
                    )
                    if existing is None:
                        session.add(
                            CollectionUploadTagPublicationFrontierRecord(
                                collection_id=collection_id,
                                node_digest=child.digest,
                            )
                        )
                frontier.expanded = True
                return True

            frontier = session.scalar(
                select(CollectionUploadTagPublicationFrontierRecord)
                .where(
                    CollectionUploadTagPublicationFrontierRecord.collection_id == collection_id,
                    CollectionUploadTagPublicationFrontierRecord.published.is_(False),
                )
                .order_by(CollectionUploadTagPublicationFrontierRecord.node_digest)
                .limit(1)
            )
            if frontier is not None:
                node_record = session.get(CollectionTagNodeRecord, frontier.node_digest)
                if node_record is None:
                    raise RuntimeError("initial collection tag node is unavailable")
                if not upload.archive_storage_prefix:
                    raise RuntimeError("collection archive storage prefix is missing")
                node_digest = frontier.node_digest
                encoded_node = node_record.encoded
                store_name = upload.archive_store
                prefix = upload.archive_storage_prefix
                passphrase_id = upload.passphrase_id
            else:
                node_digest = None
                encoded_node = None
                store_name = upload.archive_store
                prefix = upload.archive_storage_prefix
                passphrase_id = upload.passphrase_id

        store = self._archive_stores.require(store_name).store
        if node_digest is not None and encoded_node is not None:
            receipt = store.publish_collection_tag_node(
                collection_id=collection_id,
                archive_storage_prefix=prefix,
                digest=node_digest,
                encoded=encoded_node,
                passphrase_id=passphrase_id,
            )
            with session_scope(self._session_factory) as session:
                frontier = session.get(
                    CollectionUploadTagPublicationFrontierRecord,
                    (collection_id, node_digest),
                )
                if frontier is not None:
                    frontier.published = True
                    frontier.object_path = receipt.object_path
                    frontier.provider_revision = receipt.revision
                    frontier.stored_bytes = receipt.stored_bytes
                    frontier.stored_sha256 = receipt.stored_sha256
                    frontier.published_at = receipt.published_at
            return True

        with session_scope(self._session_factory) as session:
            upload = session.get(CollectionUploadRecord, collection_id)
            if upload is None or upload.tag_head_identity is None:
                return False
            authority = _final_authority(upload)
            head = CollectionTagHeadDocument.seal(
                archive_root_sha256=str(authority["root"]["plaintext_sha256"]),
                revision=upload.tag_revision or 1,
                root_sha256=upload.tag_root_sha256,
            )
            if head.head_identity != upload.tag_head_identity:
                raise RuntimeError("initial collection tag head changed")

        receipt = store.publish_collection_tag_head(
            collection_id=collection_id,
            archive_storage_prefix=prefix,
            document=head.to_json_bytes(),
            passphrase_id=passphrase_id,
        )
        receipt_json = json.dumps(
            {
                "object_path": receipt.object_path,
                "provider_revision": receipt.revision,
                "stored_bytes": receipt.stored_bytes,
                "stored_sha256": receipt.stored_sha256,
                "published_at": receipt.published_at,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        with session_scope(self._session_factory) as session:
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == collection_id)
                .with_for_update()
            )
            if upload is None:
                return False
            if upload.tag_set_identity != head.tag_set_identity:
                raise RuntimeError("initial collection tag identity changed")
            upload.tag_publication_receipt_json = receipt_json
        return True

    def _advance_catalog_projection(self, collection_id: int) -> bool:
        """Advance one bounded durable catalog-projection transaction."""

        with session_scope(self._session_factory) as session:
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == collection_id)
                .with_for_update()
            )
            if upload is None:
                return False
            phase = upload.catalog_phase
            if phase == "complete":
                return False
            if phase in {"artifact-set-identity", "inventory-identity"}:
                _advance_catalog_identity(session, upload)
            elif phase == "collection":
                self._create_catalog_collection(session, upload)
            elif phase == "tags":
                _advance_catalog_tags(session, upload)
            elif phase == "artifacts":
                _advance_catalog_artifacts(session, upload)
            elif phase == "journals":
                _advance_catalog_journals(session, upload)
            elif phase == "bindings":
                _advance_catalog_bindings(session, upload)
            elif phase == "provenance-segments":
                _advance_catalog_provenance_segments(session, upload)
            elif phase == "archive-objects":
                self._advance_catalog_archive_objects(session, upload)
            elif phase == "artifact-objects":
                _advance_catalog_artifact_objects(session, upload)
            elif phase == "index":
                _advance_catalog_canonical_index(session, upload)
            elif phase == "terminal":
                self._publish_catalog_collection(session, upload)
            else:  # pragma: no cover - constrained durable state
                raise RuntimeError(f"unknown catalog finalization phase: {phase}")
            return True

    def _create_catalog_collection(
        self,
        session: Session,
        upload: CollectionUploadRecord,
    ) -> None:
        if (
            upload.catalog_artifact_set_identity is None
            or upload.catalog_inventory_identity is None
        ):
            raise RuntimeError("catalog identities are incomplete")
        if upload.catalog_artifact_set_identity != upload.archive_tree_sha256:
            raise RuntimeError("catalog artifact set differs from the sealed archive")
        if upload.provenance_identity is None:
            raise RuntimeError("canonical provenance authority is incomplete")
        if session.get(CollectionRecord, upload.collection_id) is None:
            now = utc_timestamp_now()
            authority = _final_authority(upload)
            if upload.description_revision is None or upload.description_identity is None:
                raise RuntimeError("initial collection description state is unavailable")
            if (
                upload.tag_revision is None
                or upload.tag_set_identity is None
                or upload.tag_head_identity is None
            ):
                raise RuntimeError("initial collection tag state is unavailable")
            session.add(
                CollectionRecord(
                    id=upload.collection_id,
                    creation_idempotency_key=upload.idempotency_key,
                    creation_identity_sha256=upload.creation_identity_sha256,
                    creation_custody_mode=upload.custody_mode,
                    creation_archive_store=upload.archive_store,
                    creation_use_cache=upload.use_cache,
                    creation_copy_to_json=upload.copy_to_json,
                    archive_generation=upload.archive_generation,
                    delivery_context_id=upload.delivery_context_id,
                    artifact_set_identity=upload.catalog_artifact_set_identity,
                    encryption_format=upload.encryption_format,
                    passphrase_id=upload.passphrase_id,
                    provenance_identity=upload.provenance_identity,
                    inventory_identity=upload.catalog_inventory_identity,
                    archive_root_sha256=str(authority["root"]["plaintext_sha256"]),
                    ingest_source=upload.ingest_source,
                    description=upload.description,
                    description_search=text_search_key(upload.description or ""),
                    description_revision=upload.description_revision,
                    description_identity=upload.description_identity,
                    tag_revision=upload.tag_revision,
                    tag_root_sha256=upload.tag_root_sha256,
                    tag_set_identity=upload.tag_set_identity,
                    tag_head_identity=upload.tag_head_identity,
                    created_by_principal_id=upload.initiated_by_principal_id,
                    created_by_key_id=upload.initiated_by_key_id,
                    created_at=upload.opened_at or now,
                    is_published=False,
                    artifact_count=upload.artifact_count,
                    artifact_bytes=upload.artifact_bytes,
                )
            )
            session.flush()
            copy = CollectionArchiveCopyRecord(
                collection_id=upload.collection_id,
                store=upload.archive_store,
                incarnation_id=upload.archive_incarnation_id,
                state="uploaded",
                archive_storage_prefix=upload.archive_storage_prefix,
                last_uploaded_at=now,
                last_verified_at=now,
            )
            session.add(copy)
            session.flush()
            receipt = _initial_description_receipt(upload)
            session.add(
                CollectionDescriptionPublicationRecord(
                    collection_id=upload.collection_id,
                    store=upload.archive_store,
                    incarnation_id=upload.archive_incarnation_id,
                    desired_revision=upload.description_revision,
                    desired_identity=upload.description_identity,
                    published_revision=upload.description_revision,
                    published_identity=upload.description_identity,
                    state="published",
                    next_attempt_at=None,
                    object_path=(str(receipt["object_path"]) if receipt is not None else None),
                    provider_revision=(
                        str(receipt["provider_revision"])
                        if receipt is not None and receipt["provider_revision"] is not None
                        else None
                    ),
                    stored_bytes=(int(receipt["stored_bytes"]) if receipt is not None else None),
                    stored_sha256=(str(receipt["stored_sha256"]) if receipt is not None else None),
                    published_at=(str(receipt["published_at"]) if receipt is not None else None),
                )
            )
            session.add(
                CollectionTagRevisionRecord(
                    collection_id=upload.collection_id,
                    revision=upload.tag_revision,
                    root_sha256=upload.tag_root_sha256,
                    tag_set_identity=upload.tag_set_identity,
                    head_identity=upload.tag_head_identity,
                    created_at=now,
                )
            )
            tag_receipt = _initial_tag_receipt(upload)
            session.add(
                CollectionTagPublicationRecord(
                    collection_id=upload.collection_id,
                    store=upload.archive_store,
                    incarnation_id=upload.archive_incarnation_id,
                    desired_revision=upload.tag_revision,
                    desired_tag_set_identity=upload.tag_set_identity,
                    desired_head_identity=upload.tag_head_identity,
                    published_revision=upload.tag_revision,
                    published_tag_set_identity=upload.tag_set_identity,
                    published_head_identity=upload.tag_head_identity,
                    state="published",
                    next_attempt_at=None,
                    failure=None,
                    head_object_path=str(tag_receipt["object_path"]),
                    head_provider_revision=(
                        str(tag_receipt["provider_revision"])
                        if tag_receipt["provider_revision"] is not None
                        else None
                    ),
                    head_stored_bytes=int(tag_receipt["stored_bytes"]),
                    head_stored_sha256=str(tag_receipt["stored_sha256"]),
                    published_at=str(tag_receipt["published_at"]),
                )
            )
        upload.catalog_phase = "tags"
        upload.catalog_cursor_json = "{}"

    def _advance_catalog_archive_objects(
        self,
        session: Session,
        upload: CollectionUploadRecord,
    ) -> None:
        cursor = _catalog_cursor(upload)
        section = str(cursor.get("section", "volumes"))
        total_volumes = _planner_checkpoint(upload).next_sequence
        now = utc_timestamp_now()
        if section == "volumes":
            sequence = _cursor_nonnegative_int(cursor, "sequence")
            if sequence < total_volumes:
                record = session.scalar(
                    select(CollectionArchiveObjectUploadRecord).where(
                        CollectionArchiveObjectUploadRecord.collection_id == upload.collection_id,
                        CollectionArchiveObjectUploadRecord.sequence == sequence,
                    )
                )
                if record is None or record.sealed_receipt_json is None:
                    raise RuntimeError("catalog archive volume receipt is unavailable")
                volume = (
                    _parse_sealed_pack(record.sealed_receipt_json)
                    if record.kind == "pack"
                    else _parse_sealed_raw(record.sealed_receipt_json)
                )
                session.add(
                    CollectionArchiveObjectRecord(
                        collection_id=upload.collection_id,
                        store=upload.archive_store,
                        object_id=volume.volume_id,
                        object_order=sequence,
                        kind=record.kind,
                        object_path=f"{upload.archive_storage_prefix}/{volume.relative_path}",
                        plaintext_bytes=volume.plaintext_bytes,
                        stored_bytes=volume.stored_bytes,
                        sha256=None,
                        stored_sha256=None,
                        revision=volume.revision,
                        age_state_json=volume.age_state_json,
                        archive_parts_json=_catalog_archive_parts_json(volume.parts),
                        plan_sha256=(
                            volume.plan_sha256 if isinstance(volume, SealedPackVolume) else None
                        ),
                        index_sha256=(
                            volume.index_sha256 if isinstance(volume, SealedPackVolume) else None
                        ),
                        uploaded_at=volume.completed_at,
                        verified_at=now,
                    )
                )
                if record.metadata_receipt_json is None:
                    raise RuntimeError("catalog archive volume metadata receipt is unavailable")
                metadata = _parse_archive_volume_metadata_receipt(record.metadata_receipt_json)
                session.add(
                    CollectionArchiveObjectRecord(
                        collection_id=upload.collection_id,
                        store=upload.archive_store,
                        object_id=f"volume-metadata-{format_archive_sequence(sequence)}",
                        object_order=total_volumes + sequence,
                        kind="volume-metadata",
                        object_path=metadata.object_path,
                        plaintext_bytes=metadata.plaintext_bytes,
                        stored_bytes=metadata.stored_bytes,
                        sha256=metadata.plaintext_sha256,
                        stored_sha256=metadata.stored_sha256,
                        revision=metadata.revision,
                        uploaded_at=metadata.completed_at,
                        verified_at=now,
                    )
                )
                if volume.retrieval_cache is not None:
                    receipt = volume.retrieval_cache
                    if receipt.stored_bytes != volume.stored_bytes or (
                        receipt.stored_sha256 is not None and len(receipt.stored_sha256) != 64
                    ):
                        raise RuntimeError(
                            "retrieval cache receipt does not match its sealed archive volume"
                        )
                    register_cache_ready(
                        session,
                        source_store=upload.archive_store,
                        collection_id=upload.collection_id,
                        object_id=volume.volume_id,
                        receipt=receipt,
                    )
                    session.flush()
                    session.add(
                        RetrievalCacheLeaseRecord(
                            owner="new-archive",
                            source_store=upload.archive_store,
                            collection_id=upload.collection_id,
                            object_id=volume.volume_id,
                            expires_at=format_utc_timestamp(
                                utc_now() + self._config.retrieval_cache_new_archive_lease
                            ),
                        )
                    )
                _set_catalog_cursor(upload, {"section": "volumes", "sequence": sequence + 1})
                return
            if upload.archive_terminal_receipt_json is None:
                raise RuntimeError("catalog archive terminal receipt is unavailable")
            metadata = _parse_archive_volume_metadata_receipt(upload.archive_terminal_receipt_json)
            session.add(
                CollectionArchiveObjectRecord(
                    collection_id=upload.collection_id,
                    store=upload.archive_store,
                    object_id=(f"volume-terminal-{format_archive_sequence(total_volumes)}"),
                    object_order=2 * total_volumes,
                    kind="volume-terminal",
                    object_path=metadata.object_path,
                    plaintext_bytes=metadata.plaintext_bytes,
                    stored_bytes=metadata.stored_bytes,
                    sha256=metadata.plaintext_sha256,
                    stored_sha256=metadata.stored_sha256,
                    revision=metadata.revision,
                    uploaded_at=metadata.completed_at,
                    verified_at=now,
                )
            )
            _set_catalog_cursor(upload, {"section": "provenance", "sequence": 0})
            return
        provenance_count = int(upload.provenance_archive_next_sequence)
        if section == "provenance":
            sequence = _cursor_nonnegative_int(cursor, "sequence")
            if sequence < provenance_count:
                row = session.get(
                    CollectionUploadProvenanceArchiveVolumeRecord,
                    (upload.collection_id, sequence),
                )
                if row is None:
                    raise RuntimeError("catalog provenance archive volume is unavailable")
                base_order = 2 * total_volumes + 1 + 2 * sequence
                for offset, current in enumerate(
                    (
                        _parse_sealed_provenance_object(row.payload_receipt_json),
                        _parse_sealed_provenance_object(row.metadata_receipt_json),
                    )
                ):
                    session.add(
                        _catalog_small_archive_object(
                            upload=upload,
                            current=current,
                            object_order=base_order + offset,
                            verified_at=now,
                        )
                    )
                _set_catalog_cursor(upload, {"section": "provenance", "sequence": sequence + 1})
                return
            if upload.provenance_archive_terminal_receipt_json is None:
                raise RuntimeError("catalog provenance terminal receipt is unavailable")
            terminal = _parse_sealed_provenance_object(
                upload.provenance_archive_terminal_receipt_json
            )
            session.add(
                _catalog_small_archive_object(
                    upload=upload,
                    current=terminal,
                    object_order=2 * total_volumes + 1 + 2 * provenance_count,
                    verified_at=now,
                )
            )
            _set_catalog_cursor(
                upload,
                {
                    "section": "structure",
                    "next_order": 2 * total_volumes + 1 + 2 * provenance_count + 1,
                },
            )
            return
        if section == "structure":
            statement = select(CollectionUploadProvenanceStructureRecord).where(
                CollectionUploadProvenanceStructureRecord.collection_id == upload.collection_id
            )
            after_object_id = cursor.get("after_object_id")
            if after_object_id is not None:
                statement = statement.where(
                    CollectionUploadProvenanceStructureRecord.object_id > after_object_id
                )
            rows = list(
                session.scalars(
                    statement.order_by(CollectionUploadProvenanceStructureRecord.object_id).limit(
                        _FINALIZATION_FILE_BATCH
                    )
                )
            )
            order = _cursor_nonnegative_int(cursor, "next_order")
            for structure_row in rows:
                if structure_row.receipt_json is None:
                    raise RuntimeError("catalog provenance structure receipt is unavailable")
                current = _parse_sealed_provenance_object(structure_row.receipt_json)
                session.add(
                    _catalog_small_archive_object(
                        upload=upload,
                        current=current,
                        object_order=order,
                        verified_at=now,
                    )
                )
                order += 1
            if rows:
                _set_catalog_cursor(
                    upload,
                    {
                        "section": "structure",
                        "after_object_id": rows[-1].object_id,
                        "next_order": order,
                    },
                )
            else:
                _set_catalog_cursor(upload, {"section": "roots", "next_order": order})
            return
        if section == "roots":
            authority = _final_authority(upload)
            order = _cursor_nonnegative_int(cursor, "next_order")
            sealed = _sealed_upload_provenance(upload)
            session.add(
                _catalog_small_archive_object(
                    upload=upload,
                    current=sealed.root,
                    object_order=order,
                    verified_at=now,
                )
            )
            order += 1
            for object_id, kind, value in (
                ("manifest", "manifest", authority["root"]),
                ("recovery-descriptor", "recovery-descriptor", authority["recovery"]),
            ):
                session.add(
                    CollectionArchiveObjectRecord(
                        collection_id=upload.collection_id,
                        store=upload.archive_store,
                        object_id=object_id,
                        object_order=order,
                        kind=kind,
                        object_path=str(value["object_path"]),
                        plaintext_bytes=_mapping_nonnegative_int(
                            value, "plaintext_bytes", fallback="bytes"
                        ),
                        stored_bytes=_mapping_nonnegative_int(
                            value, "stored_bytes", fallback="bytes"
                        ),
                        sha256=str(value.get("plaintext_sha256", value.get("sha256"))),
                        stored_sha256=str(value.get("stored_sha256", value.get("sha256"))),
                        revision=(
                            str(value["revision"]) if value.get("revision") is not None else None
                        ),
                        uploaded_at=str(value["completed_at"]),
                        verified_at=now,
                    )
                )
                order += 1
            upload.catalog_phase = "artifact-objects"
            upload.catalog_cursor_json = "{}"
            return
        raise RuntimeError("catalog archive-object cursor section is invalid")

    def _publish_catalog_collection(
        self,
        session: Session,
        upload: CollectionUploadRecord,
    ) -> None:
        collection = session.get(CollectionRecord, upload.collection_id)
        if collection is None:
            raise RuntimeError("catalog collection projection is unavailable")
        now = utc_timestamp_now()
        catalog_event = begin_catalog_event(
            session,
            change="created",
            collection_id=upload.collection_id,
            occurred_at=now,
            inventory_identity=collection.inventory_identity,
            before_tag_revision=None,
            after_tag_revision=collection.tag_revision,
        )
        publish_catalog_event(session, event=catalog_event)
        authority = _final_authority(upload)
        self._events.emit_collection(
            type="collection.finalized",
            collection_id=upload.collection_id,
            details={
                "files_total": int(upload.artifact_count),
                "bytes_total": int(upload.artifact_bytes),
                "archive_root_sha256": str(authority["root"]["plaintext_sha256"]),
            },
            terminal=True,
            session=session,
        )
        collection.is_published = True
        for intent in session.scalars(
            select(CollectionUploadCopyIntentRecord).where(
                CollectionUploadCopyIntentRecord.collection_id == upload.collection_id,
                CollectionUploadCopyIntentRecord.state == "accepted",
            )
        ):
            intent.state = "pending"
            intent.next_attempt_at = now
        upload.catalog_phase = "complete"
        session.delete(upload)

    def _stage_next_member_histories(self, collection_id: int) -> bool:
        """Freeze final selections in bounded transactions before root publication."""

        with session_scope(self._session_factory) as session:
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == collection_id)
                .with_for_update()
            )
            if upload is None or upload.provenance_histories_sealed:
                return False
            statement = select(CollectionUploadArtifactRecord).where(
                CollectionUploadArtifactRecord.collection_id == collection_id
            )
            if upload.provenance_history_after_artifact_id is not None:
                statement = statement.where(
                    CollectionUploadArtifactRecord.artifact_id
                    > upload.provenance_history_after_artifact_id
                )
            artifacts = list(
                session.scalars(
                    statement.order_by(CollectionUploadArtifactRecord.artifact_id).limit(
                        _FINALIZATION_FILE_BATCH
                    )
                )
            )
            if not artifacts:
                count = session.scalar(
                    select(func.count())
                    .select_from(CollectionUploadMemberHistoryRecord)
                    .where(CollectionUploadMemberHistoryRecord.collection_id == collection_id)
                )
                if count != upload.artifact_count:
                    raise Conflict("final member histories do not cover the artifact set")
                upload.provenance_histories_sealed = True
                return True
            completion = None
            if (
                upload.completion_requirement_json is not None
                and upload.completion_journal_id is None
            ):
                raise Conflict("accepted execution completion journal is missing")
            if upload.completion_journal_id is not None:
                journal = session.get(
                    CollectionUploadProvenanceJournalRecord,
                    (collection_id, upload.completion_journal_id),
                )
                if journal is None or journal.state != "sealed":
                    raise Conflict("required completion journal is not sealed")
                completion = _sealed_journal_history_anchor(journal)
            for artifact in artifacts:
                existing_history = session.get(
                    CollectionUploadMemberHistoryRecord, (collection_id, artifact.artifact_id)
                )
                if existing_history is not None:
                    upload.provenance_history_after_artifact_id = artifact.artifact_id
                    continue
                early = session.get(
                    CollectionUploadArtifactProvenanceBindingRecord,
                    (collection_id, artifact.artifact_id),
                )
                if early is None:
                    raise Conflict("member history has no accepted primary binding")
                primary = MemberHistoryPrimary.from_mapping(
                    {
                        "journal": _provenance_binding_row(early)["journal"],
                        "delivery_association_id": early.delivery_association_id,
                    }
                )
                roots = [MemberHistoryRoot(primary.journal, "bound")]
                if completion is not None:
                    roots.append(MemberHistoryRoot(completion, "bound"))
                ordered = sorted(roots, key=lambda root: root.key)
                commitment = RecordSetCommitment(MEMBER_HISTORY_ROOTS_SCHEMA)
                root_rows: tuple[dict[str, Any], ...] = tuple(
                    {"key": root.key, "value": root.to_mapping()} for root in ordered
                )
                for root in ordered:
                    commitment.update(root.key, root.to_mapping())
                root_ref = commitment.ref()
                if early.history_imports_ref_json is None:
                    if upload.completion_requirement_json is not None:
                        raise Conflict(
                            "execution member lacks its accepted input-history selection"
                        )
                    import_ref = RecordSetCommitment(MEMBER_HISTORY_IMPORTS_SCHEMA).ref()
                else:
                    import_ref = RecordSetRef.from_mapping(
                        json.loads(early.history_imports_ref_json)
                    )
                history = MemberHistoryDocument(
                    artifact_id=artifact.artifact_id,
                    bytes=artifact.bytes,
                    sha256=artifact.sha256,
                    primary=primary,
                    roots=root_ref,
                    imports=import_ref,
                )
                root_pages = (
                    RecordPage(root_ref, 0, root_rows, False),
                    RecordPage(root_ref, 1, (), True),
                )
                import_pages = (
                    (RecordPage(import_ref, 0, (), True),)
                    if early.history_imports_ref_json is None
                    else _iter_staged_history_pages(session, collection_id, import_ref)
                )
                verify_member_history_sets(
                    history, root_pages=root_pages, import_pages=import_pages
                )
                new_pages = (
                    root_pages
                    if early.history_imports_ref_json is not None
                    else (*root_pages, RecordPage(import_ref, 0, (), True))
                )
                for page in new_pages:
                    _stage_provenance_structure(
                        session,
                        collection_id=collection_id,
                        object_id="provenance-record-page-"
                        + page.authority.records_sha256
                        + "-"
                        + format_archive_sequence(page.ordinal),
                        kind="record-page",
                        relative_path=history_record_page_object_path(
                            page.authority.records_sha256, page.ordinal
                        ),
                        content=page.to_json_bytes(),
                    )
                content = history.to_json_bytes()
                binding = MemberHistoryBinding(
                    artifact.artifact_id,
                    artifact.bytes,
                    artifact.sha256,
                    history.identity,
                    len(content),
                )
                _stage_provenance_structure(
                    session,
                    collection_id=collection_id,
                    object_id="provenance-history-" + history.identity,
                    kind="history",
                    relative_path=member_history_object_path(history.identity),
                    content=content,
                )
                existing = session.get(
                    CollectionUploadMemberHistoryRecord, (collection_id, artifact.artifact_id)
                )
                binding_json = canonical_json_bytes(binding.to_mapping()).decode("utf-8")
                if existing is None:
                    session.add(
                        CollectionUploadMemberHistoryRecord(
                            collection_id=collection_id,
                            artifact_id=artifact.artifact_id,
                            history_sha256=history.identity,
                            history_bytes=len(content),
                            binding_json=binding_json,
                        )
                    )
                elif existing.binding_json != binding_json:
                    raise Conflict("member history retry differs from its sealed selection")
                upload.provenance_history_after_artifact_id = artifact.artifact_id
            return True

    def _publish_next_provenance_structure(
        self, collection_id: int, *, selected_object_id: str | None = None
    ) -> bool:
        """Publish one exact staged structural object and checkpoint its receipt."""

        with read_snapshot(self._session_factory) as session:
            upload = session.get(CollectionUploadRecord, collection_id)
            if upload is None or upload.state not in {"open", "closing", "uploading", "finalizing"}:
                return False
            if selected_object_id is None and not upload.provenance_closure_validated:
                return False
            statement = select(CollectionUploadProvenanceStructureRecord).where(
                CollectionUploadProvenanceStructureRecord.collection_id == collection_id,
                CollectionUploadProvenanceStructureRecord.receipt_json.is_(None),
            )
            if selected_object_id is not None:
                statement = statement.where(
                    CollectionUploadProvenanceStructureRecord.object_id == selected_object_id
                )
            pending = session.scalar(
                statement.order_by(CollectionUploadProvenanceStructureRecord.object_id).limit(1)
            )
            if pending is None:
                return False
            object_id, kind, content = pending.object_id, pending.kind, pending.content
            prefix, store_name = upload.archive_storage_prefix, upload.archive_store
            passphrase = self._config.archive_passphrase_for(upload.passphrase_id)
        publisher = ArchiveProvenancePublisher(
            object_store=self._archive_stores.require(store_name).immutable_objects,
            passphrase=passphrase,
            scrypt_log_n=self._config.archive_scrypt_work_factor,
        )
        if kind == "history":
            history = MemberHistoryDocument.from_json_bytes(content)
            sealed = publisher.publish_member_history(
                archive_storage_prefix=prefix,
                binding=MemberHistoryBinding(
                    history.artifact_id,
                    history.bytes,
                    history.sha256,
                    history.identity,
                    len(content),
                ),
                content=content,
            )
        elif kind == "record-page":
            sealed = publisher.publish_record_page(
                archive_storage_prefix=prefix, page=RecordPage.from_json_bytes(content)
            )
        elif kind == "source-proof":
            sealed = publisher.publish_source_binding_proof(
                archive_storage_prefix=prefix,
                proof=SourceMemberHistoryBindingProof.from_json_bytes(content),
            )
        else:
            raise Conflict("unsupported staged provenance structure")
        with session_scope(self._session_factory) as session:
            row = session.get(CollectionUploadProvenanceStructureRecord, (collection_id, object_id))
            if row is None or row.content != content:
                raise Conflict("staged provenance structure changed during publication")
            if row.receipt_json is None:
                row.receipt_json = _sealed_provenance_object_json(sealed)
        return True

    def _advance_provenance_closure_validation(self, collection_id: int) -> bool:
        """Resolve each member against its exact primary canonical snapshot."""

        with session_scope(self._session_factory) as session:
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == collection_id)
                .with_for_update()
            )
            if upload is None or upload.provenance_closure_validated:
                return False
            if not upload.provenance_histories_sealed:
                raise Conflict("final member history selections are not sealed")
            statement = select(CollectionUploadArtifactRecord).where(
                CollectionUploadArtifactRecord.collection_id == collection_id
            )
            if upload.provenance_validation_after_artifact_id is not None:
                statement = statement.where(
                    CollectionUploadArtifactRecord.artifact_id
                    > upload.provenance_validation_after_artifact_id
                )
            rows = list(
                session.scalars(
                    statement.order_by(CollectionUploadArtifactRecord.artifact_id).limit(
                        _FINALIZATION_FILE_BATCH
                    )
                )
            )
            if rows:
                for row in rows:
                    binding_row = session.get(
                        CollectionUploadArtifactProvenanceBindingRecord,
                        (collection_id, row.artifact_id),
                    )
                    if binding_row is None:
                        raise Conflict(
                            f"canonical provenance binding is missing: {row.artifact_id}"
                        )
                    journal = session.get(
                        CollectionUploadProvenanceJournalRecord,
                        (collection_id, binding_row.journal_id),
                    )
                    if journal is None or journal.state != "sealed":
                        raise Conflict(
                            f"primary canonical journal is not sealed: {row.artifact_id}"
                        )
                    binding = CollectionArtifactProvenanceBindingDocument.model_validate(
                        _provenance_binding_row(binding_row)
                    )
                    summary = validate_journal_chunks(
                        _iter_upload_journal_chunks(
                            session, journal, through_bytes=binding.journal.prefix_bytes
                        ),
                        catalog=admission_provenance_catalog(),
                        expected_anchor=binding.journal.model_dump(mode="json"),
                        require_exact_tail=True,
                        require_profiles=False,
                    )
                    member = ArtifactMemberIdentityDocument.model_validate(
                        {
                            "artifact_id": row.artifact_id,
                            "bytes": format_scalar("sequence63", row.bytes),
                            "sha256": row.sha256,
                        }
                    )
                    verified = verify_member_binding(
                        member=member,
                        binding=binding,
                        summary=summary,
                        delivery_context_id=upload.delivery_context_id,
                    )
                    final = session.get(
                        CollectionUploadMemberHistoryRecord, (collection_id, row.artifact_id)
                    )
                    if final is None:
                        raise Conflict("final member history is missing")
                    structural = session.get(
                        CollectionUploadProvenanceStructureRecord,
                        (collection_id, "provenance-history-" + final.history_sha256),
                    )
                    if structural is None:
                        raise Conflict("final member history descriptor bytes are missing")
                    history = _member_history_binding_row(final).verify_descriptor(
                        structural.content
                    )
                    expected_primary = MemberHistoryPrimary.from_mapping(
                        {
                            "journal": binding.journal.model_dump(mode="json"),
                            "delivery_association_id": binding.delivery_association_id,
                        }
                    )
                    if history.primary != expected_primary:
                        raise Conflict(
                            "final member history differs from the accepted early primary"
                        )
                    verify_member_history_sets(
                        history,
                        root_pages=_iter_staged_history_pages(
                            session, collection_id, history.roots
                        ),
                        import_pages=_iter_staged_history_pages(
                            session, collection_id, history.imports
                        ),
                    )
                    for page in _iter_staged_history_pages(session, collection_id, history.roots):
                        for selected in page.records:
                            selected_anchor = MemberHistoryRoot.from_mapping(
                                selected["value"]
                            ).journal
                            if selected_anchor == history.primary.journal:
                                continue
                            selected_journal = session.get(
                                CollectionUploadProvenanceJournalRecord,
                                (collection_id, selected_anchor.journal_id),
                            )
                            if selected_journal is None or selected_journal.state != "sealed":
                                raise Conflict("selected member history root is absent or unsealed")
                            validate_journal_chunks(
                                _iter_upload_journal_chunks(
                                    session,
                                    selected_journal,
                                    through_bytes=selected_anchor.prefix_bytes,
                                ),
                                catalog=admission_provenance_catalog(),
                                expected_anchor=selected_anchor.to_mapping(),
                                require_exact_tail=True,
                                require_profiles=False,
                            )
                    validate_collection_production_records(
                        summary.graph, delivery_context_id=upload.delivery_context_id
                    )
                    association = summary.graph_validation.objects[binding.delivery_association_id]
                    binding_row.completion_output_id = validate_member_completion_requirement(
                        summary.graph,
                        delivery_context_id=upload.delivery_context_id,
                        state_id=association["state"]["object_id"],
                        requirement=(
                            None
                            if upload.completion_requirement_json is None
                            else CollectionCompletionRequirementDocument.model_validate_json(
                                upload.completion_requirement_json
                            )
                        ),
                    )
                    decision = session.get(
                        CollectionUploadArtifactMaterializationDecisionRecord,
                        (collection_id, row.artifact_id),
                    )
                    if decision is None:
                        raise MaterializationDecisionRequired(
                            f"materialization decision is missing: {row.artifact_id}"
                        )
                    expected_hint = (
                        None
                        if decision.hint_json is None
                        else tuple(json.loads(decision.hint_json)["components"])
                    )
                    if expected_hint != verified.materialization_hint:
                        raise Conflict("publication hint differs from the delivered Occurrence")
                    if (expected_hint is None) != decision.allow_missing_materialization_hint:
                        raise MaterializationDecisionRequired(
                            "publication requires exactly one hint or explicit omission"
                        )
                    upload.provenance_validation_after_artifact_id = row.artifact_id
                    upload.provenance_validation_next_artifact_order += 1
                return True
            if upload.provenance_validation_next_artifact_order != upload.artifact_count:
                raise Conflict("canonical provenance bindings do not cover the artifact set")
            _validate_staged_canonical_journal_set(session, upload)
            upload.provenance_closure_validated = True
            return True

    def _publish_next_provenance_archive_object(self, collection_id: int) -> bool:
        """Publish one bounded structural page, journal segment, terminal or root."""

        with session_scope(self._session_factory) as session:
            upload = session.get(CollectionUploadRecord, collection_id)
            if upload is None or not upload.provenance_closure_validated:
                return False
            artifact_set_sha256 = upload.archive_tree_sha256
            if (
                artifact_set_sha256 is None
                or upload.provenance_archive_root_receipt_json is not None
            ):
                return False
            sequence = int(upload.provenance_archive_next_sequence)
            prefix = upload.archive_storage_prefix
            store_name = upload.archive_store
            archive_generation = upload.archive_generation
            passphrase = self._config.archive_passphrase_for(upload.passphrase_id)
            document: ProvenanceVolumeDocument | None = None
            terminal: ProvenanceTerminalDocument | None = None
            root: ProvenanceRootDocument | None = None
            payload = b""
            after_artifact_id: str | None = None
            journal_id: str | None = None
            next_journal_offset = 0
            next_artifact_order = int(upload.provenance_archive_next_artifact_order)

            if next_artifact_order < upload.artifact_count:
                statement = select(CollectionUploadMemberHistoryRecord).where(
                    CollectionUploadMemberHistoryRecord.collection_id == collection_id
                )
                if upload.provenance_archive_after_artifact_id is not None:
                    statement = statement.where(
                        CollectionUploadMemberHistoryRecord.artifact_id
                        > upload.provenance_archive_after_artifact_id
                    )
                rows = list(
                    session.scalars(
                        statement.order_by(CollectionUploadMemberHistoryRecord.artifact_id).limit(
                            PROVENANCE_BINDING_PAGE_MEMBERS_MAX
                        )
                    )
                )
                if not rows:
                    raise RuntimeError("provenance binding pages do not cover the member set")
                bindings = [_member_history_binding_row(row).to_mapping() for row in rows]
                validate_member_history_binding_page(
                    bindings, max_members=PROVENANCE_BINDING_PAGE_MEMBERS_MAX
                )
                payload = canonical_json_bytes(
                    {"format": PROVENANCE_BINDINGS_FORMAT, "bindings": bindings}
                )
                if len(payload) > PROVENANCE_BINDING_PAGE_BYTES_MAX:
                    raise RuntimeError("bounded binding page exceeds its byte contract")
                document = _provenance_volume_document(
                    archive_generation=archive_generation,
                    artifact_set_sha256=artifact_set_sha256,
                    sequence=sequence,
                    payload=payload,
                    first_artifact_id=rows[0].artifact_id,
                    last_artifact_id=rows[-1].artifact_id,
                    binding_count=len(rows),
                )
                next_artifact_order += len(rows)
                after_artifact_id = rows[-1].artifact_id
            else:
                journal = _next_provenance_publication_journal(session, upload)
                if journal is not None:
                    offset = int(upload.provenance_archive_current_journal_offset)
                    payload = _upload_journal_range_bytes(
                        session,
                        collection_id,
                        journal.journal_id,
                        offset=offset,
                        size=min(PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX, journal.bytes - offset),
                    )
                    document = _provenance_volume_document(
                        archive_generation=archive_generation,
                        artifact_set_sha256=artifact_set_sha256,
                        sequence=sequence,
                        payload=payload,
                        journal=journal,
                        journal_offset=offset,
                    )
                    journal_id = journal.journal_id
                    next_journal_offset = offset + len(payload)
                elif upload.provenance_archive_terminal_receipt_json is None:
                    terminal = ProvenanceTerminalDocument(
                        archive_generation=archive_generation,
                        artifact_set_sha256=artifact_set_sha256,
                        sequence=sequence,
                    )
                else:
                    if upload.provenance_archive_ordered_sha256 is None:
                        raise RuntimeError("provenance terminal has no ordered commitment")
                    journal_count = int(
                        session.scalar(
                            select(func.count())
                            .select_from(CollectionUploadProvenanceJournalRecord)
                            .where(
                                CollectionUploadProvenanceJournalRecord.collection_id
                                == collection_id
                            )
                        )
                        or 0
                    )
                    root = ProvenanceRootDocument(
                        archive_generation=archive_generation,
                        artifact_set_sha256=artifact_set_sha256,
                        delivery_context_id=upload.delivery_context_id,
                        binding_count=upload.artifact_count,
                        binding_tree_sha256=binding_tree_commitment(
                            _member_history_binding_row(row)
                            for row in session.scalars(
                                select(CollectionUploadMemberHistoryRecord)
                                .where(
                                    CollectionUploadMemberHistoryRecord.collection_id
                                    == collection_id
                                )
                                .order_by(CollectionUploadMemberHistoryRecord.artifact_id)
                                .execution_options(yield_per=256)
                            )
                        ).root_sha256,
                        journal_count=journal_count,
                        ordered_volume_sha256=upload.provenance_archive_ordered_sha256,
                    )

        publisher = ArchiveProvenancePublisher(
            object_store=self._archive_stores.require(store_name).immutable_objects,
            passphrase=passphrase,
            scrypt_log_n=self._config.archive_scrypt_work_factor,
        )
        if root is not None:
            sealed_root = publisher.publish_root(archive_storage_prefix=prefix, root=root)
            with session_scope(self._session_factory) as session:
                upload = session.scalar(
                    select(CollectionUploadRecord)
                    .where(CollectionUploadRecord.collection_id == collection_id)
                    .with_for_update()
                )
                if upload is None:
                    return False
                if upload.provenance_archive_root_receipt_json is None:
                    upload.provenance_identity = sealed_root.identity
                    upload.provenance_archive_root_receipt_json = _sealed_provenance_json(
                        sealed_root
                    )
            return True
        if terminal is not None:
            sealed_terminal = publisher.publish_terminal(
                archive_storage_prefix=prefix, terminal=terminal
            )
            with session_scope(self._session_factory) as session:
                upload = session.scalar(
                    select(CollectionUploadRecord)
                    .where(CollectionUploadRecord.collection_id == collection_id)
                    .with_for_update()
                )
                if upload is None:
                    return False
                if upload.provenance_archive_terminal_receipt_json is None:
                    digest = _provenance_commitment(upload)
                    update_provenance_commitment(digest, terminal)
                    upload.provenance_archive_terminal_receipt_json = (
                        _sealed_provenance_object_json(sealed_terminal)
                    )
                    upload.provenance_archive_ordered_sha256 = digest.hexdigest()
                    upload.provenance_archive_hash_state = None
            return True
        assert document is not None
        sealed = publisher.publish_volume(
            archive_storage_prefix=prefix, document=document, payload=payload
        )
        with session_scope(self._session_factory) as session:
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == collection_id)
                .with_for_update()
            )
            if upload is None:
                return False
            if upload.provenance_archive_next_sequence != sequence:
                return True
            digest = _provenance_commitment(upload)
            update_provenance_commitment(digest, document)
            session.add(
                CollectionUploadProvenanceArchiveVolumeRecord(
                    collection_id=collection_id,
                    sequence=sequence,
                    kind=document.payload.kind,
                    document_json=document.to_json_bytes().decode("utf-8"),
                    payload_receipt_json=_sealed_provenance_object_json(sealed.payload),
                    metadata_receipt_json=_sealed_provenance_object_json(sealed.metadata),
                )
            )
            upload.provenance_archive_next_sequence = sequence + 1
            upload.provenance_archive_hash_state = digest.export_state()
            upload.provenance_archive_next_artifact_order = next_artifact_order
            if after_artifact_id is not None:
                upload.provenance_archive_after_artifact_id = after_artifact_id
            if journal_id is not None:
                if upload.provenance_archive_current_journal_id not in {None, journal_id}:
                    raise RuntimeError("provenance journal publication changed identity")
                if document.journal_bytes == next_journal_offset:
                    upload.provenance_archive_last_journal_id = journal_id
                    upload.provenance_archive_current_journal_id = None
                    upload.provenance_archive_current_journal_offset = 0
                else:
                    upload.provenance_archive_current_journal_id = journal_id
                    upload.provenance_archive_current_journal_offset = next_journal_offset
            return True

    def _advance_archive_tree_checkpoint(self, collection_id: int) -> bool:
        """Commit the ID-ordered artifact set in bounded catalog scans."""

        with session_scope(self._session_factory) as session:
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == collection_id)
                .with_for_update()
            )
            if upload is None or upload.archive_tree_sha256 is not None:
                return False
            digest = (
                CheckpointSHA256.from_state(upload.archive_tree_hash_state)
                if upload.archive_tree_hash_state is not None
                else CheckpointSHA256()
            )
            if upload.archive_tree_next_artifact_order == 0:
                digest.update(b'{"artifacts":[')
            statement = select(CollectionUploadArtifactRecord).where(
                CollectionUploadArtifactRecord.collection_id == collection_id
            )
            if upload.archive_tree_after_artifact_id is not None:
                statement = statement.where(
                    CollectionUploadArtifactRecord.artifact_id
                    > upload.archive_tree_after_artifact_id
                )
            rows = list(
                session.scalars(
                    statement.order_by(CollectionUploadArtifactRecord.artifact_id).limit(
                        _FINALIZATION_FILE_BATCH
                    )
                )
            )
            if not rows:
                if (
                    upload.archive_tree_next_artifact_order != upload.artifact_count
                    or upload.artifact_count < 1
                ):
                    raise RuntimeError("artifact set does not cover registered artifacts")
                digest.update(b'],"format":"riverhog-artifact-set/v1"}')
                upload.archive_tree_sha256 = digest.hexdigest()
                upload.archive_tree_hash_state = None
                return True
            expected = upload.archive_tree_next_artifact_order
            for row in rows:
                if expected:
                    digest.update(b",")
                digest.update(
                    canonical_json_bytes(
                        {
                            "artifact_id": row.artifact_id,
                            "bytes": format_scalar("nonnegative", row.bytes),
                            "sha256": row.sha256,
                        }
                    )
                )
                expected += 1
            upload.archive_tree_next_artifact_order = expected
            upload.archive_tree_after_artifact_id = rows[-1].artifact_id
            if expected == upload.artifact_count:
                digest.update(b'],"format":"riverhog-artifact-set/v1"}')
                upload.archive_tree_sha256 = digest.hexdigest()
                upload.archive_tree_hash_state = None
            else:
                upload.archive_tree_hash_state = digest.export_state()
            return True

    def _publish_next_archive_volume_metadata(self, collection_id: int) -> bool:
        """Publish and checkpoint one bounded volume document in sequence order."""

        with session_scope(self._session_factory) as session:
            upload = session.get(CollectionUploadRecord, collection_id)
            if upload is None or upload.archive_tree_sha256 is None:
                return False
            total_volumes = _planner_checkpoint(upload).next_sequence
            sequence = upload.archive_volume_next_sequence
            if sequence >= total_volumes:
                if sequence != total_volumes:
                    raise RuntimeError("archive volume metadata checkpoint exceeds its authority")
                if upload.archive_terminal_receipt_json is not None:
                    if upload.archive_ordered_volume_sha256 is None:
                        raise RuntimeError("archive terminal has no ordered commitment")
                    return False
                prefix = upload.archive_storage_prefix
                store_name = upload.archive_store
                archive_generation = upload.archive_generation
                passphrase = self._config.archive_passphrase_for(upload.passphrase_id)
                artifact_set_sha256 = upload.archive_tree_sha256
                terminal = build_collection_archive_terminal_document(
                    archive_generation=archive_generation,
                    artifact_set_sha256=artifact_set_sha256,
                    sequence=sequence,
                )
                terminal_mode = True
            else:
                terminal_mode = False
            if terminal_mode:
                record = None
            else:
                record = session.scalar(
                    select(CollectionArchiveObjectUploadRecord).where(
                        CollectionArchiveObjectUploadRecord.collection_id == collection_id,
                        CollectionArchiveObjectUploadRecord.sequence == sequence,
                    )
                )
                if record is None or record.sealed_receipt_json is None:
                    raise RuntimeError("archive volume metadata source is not sealed")
                prefix = upload.archive_storage_prefix
                store_name = upload.archive_store
                archive_generation = upload.archive_generation
                passphrase = self._config.archive_passphrase_for(upload.passphrase_id)
                artifact_set_sha256 = upload.archive_tree_sha256
                receipt: SealedPackVolume | SealedRawVolume
                plan: PackVolumePlan | None
                if record.kind == "pack":
                    plan = parse_pack_volume_plan(record.plan_json)
                    receipt = _parse_sealed_pack(record.sealed_receipt_json)
                elif record.kind == "segment":
                    plan = None
                    receipt = _parse_sealed_raw(record.sealed_receipt_json)
                else:
                    raise RuntimeError(f"unsupported archive volume kind: {record.kind}")
        publisher = ArchiveRootPublisher(
            object_store=self._archive_stores.require(store_name).immutable_objects,
            passphrase=passphrase,
            scrypt_log_n=self._config.archive_scrypt_work_factor,
        )
        document: CollectionArchiveTerminalDocument | CollectionArchiveVolumeDocument
        if terminal_mode:
            document = terminal
            published = publisher.publish_terminal_metadata(
                archive_storage_prefix=prefix, document=terminal
            )
        else:
            volume_document = build_collection_archive_volume_document(
                archive_generation=archive_generation,
                artifact_set_sha256=artifact_set_sha256,
                plan=plan,
                receipt=receipt,
            )
            document = volume_document
            published = publisher.publish_volume_metadata(
                archive_storage_prefix=prefix,
                document=volume_document,
            )
        with session_scope(self._session_factory) as session:
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == collection_id)
                .with_for_update()
            )
            record = (
                session.scalar(
                    select(CollectionArchiveObjectUploadRecord)
                    .where(
                        CollectionArchiveObjectUploadRecord.collection_id == collection_id,
                        CollectionArchiveObjectUploadRecord.sequence == sequence,
                    )
                    .with_for_update()
                )
                if not terminal_mode
                else None
            )
            if upload is None or (not terminal_mode and record is None):
                return False
            if upload.archive_volume_next_sequence != sequence:
                return True
            digest = (
                CheckpointSHA256.from_state(upload.archive_volume_hash_state)
                if upload.archive_volume_hash_state is not None
                else CheckpointSHA256()
            )
            update_archive_sequence_commitment(digest, document)
            if terminal_mode:
                upload.archive_terminal_receipt_json = _archive_volume_metadata_receipt_json(
                    published
                )
                upload.archive_ordered_volume_sha256 = digest.hexdigest()
                upload.archive_volume_hash_state = None
            else:
                assert record is not None
                record.metadata_receipt_json = _archive_volume_metadata_receipt_json(published)
                upload.archive_volume_next_sequence = sequence + 1
                upload.archive_volume_hash_state = digest.export_state()
            return True

    def _requeue_finalization_step(self, collection_id: int) -> None:
        now = utc_timestamp_now()
        with session_scope(self._session_factory) as session:
            upload = session.get(CollectionUploadRecord, collection_id)
            if upload is None:
                return
            upload.state = "finalizing"
            upload.archive_phase = "finalization_queued"
            upload.archive_phase_updated_at = now
            upload.archive_next_attempt_at = now
            upload.archive_failure = None

    def _begin_final_publication_attempt(self, collection_id: int) -> None:
        with session_scope(self._session_factory) as session:
            upload = session.get(CollectionUploadRecord, collection_id)
            if upload is None:
                return
            upload.archive_attempt_count += 1


def _catalog_cursor(upload: CollectionUploadRecord) -> dict[str, object]:
    try:
        value = json.loads(upload.catalog_cursor_json)
    except json.JSONDecodeError as exc:  # pragma: no cover - durable corruption
        raise RuntimeError("catalog finalization cursor is invalid") from exc
    if not isinstance(value, dict):
        raise RuntimeError("catalog finalization cursor is not an object")
    return value


def _set_catalog_cursor(upload: CollectionUploadRecord, value: Mapping[str, object]) -> None:
    upload.catalog_cursor_json = json.dumps(value, sort_keys=True, separators=(",", ":"))


def _cursor_nonnegative_int(cursor: Mapping[str, object], key: str) -> int:
    value = cursor.get(key, 0)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RuntimeError(f"catalog cursor {key} is invalid")
    return value


def _mapping_nonnegative_int(
    value: Mapping[str, object],
    key: str,
    *,
    fallback: str,
) -> int:
    current = value.get(key, value.get(fallback, 0))
    if isinstance(current, bool) or not isinstance(current, int) or current < 0:
        raise RuntimeError(f"archive authority {key} is invalid")
    return current


def _advance_catalog_identity(session: Session, upload: CollectionUploadRecord) -> None:
    cursor = _catalog_cursor(upload)
    artifacts_seen = _cursor_nonnegative_int(cursor, "artifacts_seen")
    after_id = cursor.get("after_artifact_id")
    if after_id is not None and (
        not isinstance(after_id, str) or _SHA256_RE.fullmatch(after_id) is None
    ):
        raise RuntimeError("catalog artifact identity cursor is invalid")
    digest = (
        CheckpointSHA256.from_state(upload.catalog_hash_state)
        if upload.catalog_hash_state is not None
        else CheckpointSHA256()
    )
    if upload.catalog_phase == "artifact-set-identity" and artifacts_seen == 0:
        digest.update(b'{"artifacts":[')
    if upload.catalog_phase == "inventory-identity" and artifacts_seen == 0:
        if upload.catalog_artifact_set_identity is None:
            raise RuntimeError("portable inventory has no artifact-set identity")
        if upload.provenance_identity is None:
            raise RuntimeError("portable inventory has no canonical provenance identity")
        header = PortableCollectionHeader.model_validate(
            dict(
                collection=format_scalar("sequence63", upload.collection_id),
                artifact_set_identity=upload.catalog_artifact_set_identity,
                encryption_format=upload.encryption_format,
                passphrase_id=upload.passphrase_id,
                provenance_identity=upload.provenance_identity,
            )
        )
        digest.update(canonical_json_bytes(header.model_dump(mode="json")))
    statement = select(CollectionUploadArtifactRecord).where(
        CollectionUploadArtifactRecord.collection_id == upload.collection_id
    )
    if after_id is not None:
        statement = statement.where(CollectionUploadArtifactRecord.artifact_id > after_id)
    rows = list(
        session.scalars(
            statement.order_by(CollectionUploadArtifactRecord.artifact_id).limit(
                _FINALIZATION_FILE_BATCH
            )
        )
    )
    if rows:
        for row in rows:
            if upload.catalog_phase == "artifact-set-identity":
                if artifacts_seen:
                    digest.update(b",")
                digest.update(
                    canonical_json_bytes(
                        {
                            "artifact_id": row.artifact_id,
                            "bytes": format_scalar("nonnegative", row.bytes),
                            "sha256": row.sha256,
                        }
                    )
                )
            else:
                encoded = canonical_json_bytes(
                    PortableCollectionArtifact(
                        artifact_id=ArtifactId(row.artifact_id),
                        bytes=row.bytes,
                        sha256=row.sha256,
                    ).to_mapping()
                )
                digest.update(len(encoded).to_bytes(8, "big"))
                digest.update(encoded)
            artifacts_seen += 1
        upload.catalog_hash_state = digest.export_state()
        _set_catalog_cursor(
            upload,
            {
                "artifacts_seen": artifacts_seen,
                "after_artifact_id": rows[-1].artifact_id,
            },
        )
        return
    if artifacts_seen != upload.artifact_count:
        raise RuntimeError("catalog identity does not cover every registered artifact")
    if upload.catalog_phase == "artifact-set-identity":
        digest.update(b'],"format":"riverhog-artifact-set/v1"}')
        upload.catalog_artifact_set_identity = digest.hexdigest()
        upload.catalog_phase = "inventory-identity"
    else:
        upload.catalog_inventory_identity = digest.hexdigest()
        upload.catalog_phase = "collection"
    upload.catalog_hash_state = None
    upload.catalog_cursor_json = "{}"


def _advance_catalog_tags(session: Session, upload: CollectionUploadRecord) -> None:
    """Project one bounded batch from staging into the searchable tag catalog."""

    if upload.tag_revision is None:
        raise RuntimeError("initial collection tag revision is unavailable")
    tag_revision = upload.tag_revision
    cursor = _catalog_cursor(upload)
    section = str(cursor.get("section", "nodes"))
    if section == "nodes":
        after_node = cursor.get("after_node_digest")
        if after_node is not None and (
            not isinstance(after_node, str)
            or len(after_node) != 64
            or any(character not in "0123456789abcdef" for character in after_node)
        ):
            raise RuntimeError("catalog tag-node cursor is invalid")
        statement = select(CollectionUploadTagPublicationFrontierRecord).where(
            CollectionUploadTagPublicationFrontierRecord.collection_id == upload.collection_id,
            CollectionUploadTagPublicationFrontierRecord.published.is_(True),
        )
        if after_node is not None:
            statement = statement.where(
                CollectionUploadTagPublicationFrontierRecord.node_digest > after_node
            )
        nodes = list(
            session.scalars(
                statement.order_by(CollectionUploadTagPublicationFrontierRecord.node_digest).limit(
                    _FINALIZATION_FILE_BATCH
                )
            )
        )
        if nodes:
            for node in nodes:
                if (
                    node.object_path is None
                    or node.stored_bytes is None
                    or node.stored_sha256 is None
                    or node.published_at is None
                ):
                    raise RuntimeError("initial tag-node publication receipt is incomplete")
                session.add(
                    CollectionTagPublishedNodeRecord(
                        collection_id=upload.collection_id,
                        store=upload.archive_store,
                        node_digest=node.node_digest,
                        object_path=node.object_path,
                        provider_revision=node.provider_revision,
                        stored_bytes=node.stored_bytes,
                        stored_sha256=node.stored_sha256,
                        published_at=node.published_at,
                    )
                )
                session.add(
                    CollectionTagPublicationFrontierRecord(
                        collection_id=upload.collection_id,
                        store=upload.archive_store,
                        head_identity=str(upload.tag_head_identity),
                        node_digest=node.node_digest,
                        expanded=True,
                        published=True,
                    )
                )
            _set_catalog_cursor(
                upload,
                {"section": "nodes", "after_node_digest": nodes[-1].node_digest},
            )
            return
        cursor = {"section": "members"}
        _set_catalog_cursor(upload, cursor)
        return
    if section != "members":
        raise RuntimeError("catalog tag projection section is invalid")
    after_digest = cursor.get("after_tag_sha256")
    if after_digest is not None and (
        not isinstance(after_digest, str)
        or len(after_digest) != 64
        or any(character not in "0123456789abcdef" for character in after_digest)
    ):
        raise RuntimeError("catalog tag cursor is invalid")
    tag_statement = select(CollectionUploadTagRecord).where(
        CollectionUploadTagRecord.collection_id == upload.collection_id
    )
    if after_digest is not None:
        tag_statement = tag_statement.where(CollectionUploadTagRecord.tag_sha256 > after_digest)
    rows = list(
        session.scalars(
            tag_statement.order_by(CollectionUploadTagRecord.tag_sha256).limit(
                _FINALIZATION_FILE_BATCH
            )
        )
    )
    if not rows:
        upload.catalog_phase = "artifacts"
        upload.catalog_cursor_json = "{}"
        return
    now = utc_timestamp_now()
    for staged_tag in rows:
        tag_record = session.get(CollectionTagRecord, staged_tag.tag_sha256)
        if tag_record is None:
            tag_record = CollectionTagRecord(
                tag_sha256=staged_tag.tag_sha256,
                tag=staged_tag.tag,
                search_text=text_search_key(staged_tag.tag),
                created_at=now,
                updated_at=now,
                collection_count=0,
            )
            session.add(tag_record)
            session.flush()
        elif tag_record.tag != staged_tag.tag:
            raise RuntimeError("collection tag SHA-256 collision")
        session.add(
            CollectionTagMembershipRecord(
                collection_id=upload.collection_id,
                tag_sha256=staged_tag.tag_sha256,
                added_at=now,
            )
        )
        open_catalog_tag_visibility(
            session,
            collection_id=upload.collection_id,
            tag_sha256=staged_tag.tag_sha256,
            revision=tag_revision,
        )
        tag_record.collection_count += 1
    _set_catalog_cursor(
        upload,
        {"section": "members", "after_tag_sha256": rows[-1].tag_sha256},
    )


def _advance_catalog_artifacts(session: Session, upload: CollectionUploadRecord) -> None:
    cursor = _catalog_cursor(upload)
    after_id = cursor.get("after_artifact_id")
    if after_id is not None and (
        not isinstance(after_id, str) or _SHA256_RE.fullmatch(after_id) is None
    ):
        raise RuntimeError("catalog artifact projection cursor is invalid")
    statement = select(CollectionUploadArtifactRecord).where(
        CollectionUploadArtifactRecord.collection_id == upload.collection_id
    )
    if after_id is not None:
        statement = statement.where(CollectionUploadArtifactRecord.artifact_id > after_id)
    rows = list(
        session.scalars(
            statement.order_by(CollectionUploadArtifactRecord.artifact_id).limit(
                _FINALIZATION_FILE_BATCH
            )
        )
    )
    if not rows:
        if _cursor_nonnegative_int(cursor, "artifacts_seen") != upload.artifact_count:
            raise RuntimeError("catalog artifact projection is incomplete")
        upload.catalog_phase = "journals"
        upload.catalog_cursor_json = "{}"
        return
    session.execute(
        insert(CollectionArtifactRecord),
        [
            {
                "collection_id": upload.collection_id,
                "artifact_id": row.artifact_id,
                "bytes": row.bytes,
                "sha256": row.sha256,
            }
            for row in rows
        ],
    )
    _set_catalog_cursor(
        upload,
        {
            "after_artifact_id": rows[-1].artifact_id,
            "artifacts_seen": _cursor_nonnegative_int(cursor, "artifacts_seen") + len(rows),
        },
    )


def _advance_catalog_journals(session: Session, upload: CollectionUploadRecord) -> None:
    """Project only exact journal summaries; archive custody owns the octets."""

    cursor = _catalog_cursor(upload)
    after_id = cursor.get("after_journal_id")
    if after_id is not None and not isinstance(after_id, str):
        raise RuntimeError("catalog journal cursor is invalid")
    statement = select(CollectionUploadProvenanceJournalRecord).where(
        CollectionUploadProvenanceJournalRecord.collection_id == upload.collection_id
    )
    if after_id is not None:
        statement = statement.where(CollectionUploadProvenanceJournalRecord.journal_id > after_id)
    journals = list(
        session.scalars(
            statement.order_by(CollectionUploadProvenanceJournalRecord.journal_id).limit(
                _FINALIZATION_FILE_BATCH
            )
        )
    )
    if not journals:
        upload.catalog_phase = "bindings"
        upload.catalog_cursor_json = "{}"
        return
    rows: list[dict[str, object]] = []
    for journal in journals:
        if (
            journal.state != "sealed"
            or journal.terminal_entry_id is None
            or journal.terminal_sequence is None
            or journal.terminal_json_sha256 is None
        ):
            raise RuntimeError("catalog journal projection requires a sealed canonical tail")
        rows.append(
            {
                "collection_id": upload.collection_id,
                "journal_id": journal.journal_id,
                "bytes": journal.bytes,
                "sha256": journal.sha256,
                "entries": journal.validation_sequence,
                "terminal_entry_id": journal.terminal_entry_id,
                "terminal_sequence": journal.terminal_sequence,
                "terminal_json_sha256": journal.terminal_json_sha256,
            }
        )
    session.execute(insert(CollectionProvenanceJournalRecord), rows)
    _set_catalog_cursor(upload, {"after_journal_id": journals[-1].journal_id})


def _advance_catalog_bindings(session: Session, upload: CollectionUploadRecord) -> None:
    cursor = _catalog_cursor(upload)
    after_id = cursor.get("after_artifact_id")
    if after_id is not None and (
        not isinstance(after_id, str) or _SHA256_RE.fullmatch(after_id) is None
    ):
        raise RuntimeError("catalog binding cursor is invalid")
    statement = select(CollectionUploadArtifactProvenanceBindingRecord).where(
        CollectionUploadArtifactProvenanceBindingRecord.collection_id == upload.collection_id
    )
    if after_id is not None:
        statement = statement.where(
            CollectionUploadArtifactProvenanceBindingRecord.artifact_id > after_id
        )
    rows = list(
        session.scalars(
            statement.order_by(CollectionUploadArtifactProvenanceBindingRecord.artifact_id).limit(
                _FINALIZATION_FILE_BATCH
            )
        )
    )
    if not rows:
        if _cursor_nonnegative_int(cursor, "bindings_seen") != upload.artifact_count:
            raise RuntimeError("catalog provenance bindings are incomplete")
        upload.catalog_phase = "provenance-segments"
        upload.catalog_cursor_json = "{}"
        return
    histories = {
        row.artifact_id: session.get(
            CollectionUploadMemberHistoryRecord, (upload.collection_id, row.artifact_id)
        )
        for row in rows
    }
    if any(history is None for history in histories.values()):
        raise RuntimeError("catalog member history selection is unavailable")
    session.execute(
        insert(CollectionArtifactProvenanceRecord),
        [
            {
                "collection_id": upload.collection_id,
                "artifact_id": row.artifact_id,
                "journal_id": row.journal_id,
                "through_entry_id": row.through_entry_id,
                "through_sequence": row.through_sequence,
                "through_json_sha256": row.through_json_sha256,
                "prefix_sha256": row.prefix_sha256,
                "prefix_bytes": row.prefix_bytes,
                "delivery_association_id": row.delivery_association_id,
                "history_sha256": cast(
                    CollectionUploadMemberHistoryRecord, histories[row.artifact_id]
                ).history_sha256,
                "history_bytes": cast(
                    CollectionUploadMemberHistoryRecord, histories[row.artifact_id]
                ).history_bytes,
            }
            for row in rows
        ],
    )
    _set_catalog_cursor(
        upload,
        {
            "after_artifact_id": rows[-1].artifact_id,
            "bindings_seen": _cursor_nonnegative_int(cursor, "bindings_seen") + len(rows),
        },
    )


def _advance_catalog_provenance_segments(session: Session, upload: CollectionUploadRecord) -> None:
    """Project bounded encrypted segment locations, never a journal byte replica."""

    cursor = _catalog_cursor(upload)
    sequence = _cursor_nonnegative_int(cursor, "sequence")
    last_journal_id = cursor.get("journal_id")
    last_offset = _cursor_nonnegative_int(cursor, "journal_offset")
    if last_journal_id is not None and not isinstance(last_journal_id, str):
        raise RuntimeError("catalog provenance segment cursor is invalid")
    if sequence == upload.provenance_archive_next_sequence:
        if last_journal_id is not None:
            last = session.get(
                CollectionProvenanceJournalRecord, (upload.collection_id, last_journal_id)
            )
            if last is None or last_offset != last.bytes:
                raise RuntimeError("catalog journal segments do not cover the exact journal")
        upload.catalog_phase = "archive-objects"
        upload.catalog_cursor_json = "{}"
        return
    if sequence > upload.provenance_archive_next_sequence:
        raise RuntimeError("catalog provenance segment cursor exceeds the archive")
    record = session.get(
        CollectionUploadProvenanceArchiveVolumeRecord, (upload.collection_id, sequence)
    )
    if record is None:
        raise RuntimeError("catalog provenance volume is missing")
    document = ProvenanceVolumeDocument.from_json_bytes(record.document_json.encode("utf-8"))
    if document.sequence != sequence or document.archive_generation != upload.archive_generation:
        raise RuntimeError("catalog provenance volume changed archive authority")
    if document.artifact_set_sha256 != upload.archive_tree_sha256:
        raise RuntimeError("catalog provenance volume changed the artifact set")
    if document.payload.kind == "journal":
        journal_id = document.journal_id
        offset = document.journal_offset
        if journal_id is None or offset is None or document.journal_bytes is None:
            raise RuntimeError("catalog journal segment is incomplete")
        journal = session.get(CollectionProvenanceJournalRecord, (upload.collection_id, journal_id))
        if (
            journal is None
            or document.journal_bytes != journal.bytes
            or document.journal_sha256 != journal.sha256
        ):
            raise RuntimeError("catalog journal segment changed journal authority")
        if journal_id != last_journal_id:
            if last_journal_id is not None:
                previous = session.get(
                    CollectionProvenanceJournalRecord, (upload.collection_id, last_journal_id)
                )
                if previous is None or last_offset != previous.bytes:
                    raise RuntimeError("catalog journal segments are incomplete")
            if offset != 0:
                raise RuntimeError("catalog journal segment does not start at zero")
        elif offset != last_offset:
            raise RuntimeError("catalog journal segments are not contiguous")
        sealed = _parse_sealed_provenance_object(record.payload_receipt_json)
        if (
            sealed.plaintext_bytes != document.payload.bytes
            or sealed.plaintext_sha256 != document.payload.sha256
        ):
            raise RuntimeError("catalog journal segment differs from its sealed receipt")
        session.add(
            CollectionProvenanceJournalSegmentRecord(
                collection_id=upload.collection_id,
                journal_id=journal_id,
                sequence=sequence,
                byte_offset=offset,
                bytes=document.payload.bytes,
                sha256=document.payload.sha256,
                object_id=sealed.object_id,
            )
        )
        last_journal_id = journal_id
        last_offset = offset + document.payload.bytes
    _set_catalog_cursor(
        upload,
        {"sequence": sequence + 1, "journal_id": last_journal_id, "journal_offset": last_offset},
    )


def _final_authority(upload: CollectionUploadRecord) -> dict[str, dict[str, object]]:
    if upload.final_authority_json is None:
        raise RuntimeError("final archive authority is unavailable")
    try:
        value = json.loads(upload.final_authority_json)
    except json.JSONDecodeError as exc:
        raise RuntimeError("final archive authority receipt is invalid") from exc
    if not isinstance(value, dict) or any(
        not isinstance(value.get(key), dict) for key in ("root", "recovery")
    ):
        raise RuntimeError("final archive authority receipt is incomplete")
    return cast(dict[str, dict[str, object]], value)


def _initial_description_receipt(
    upload: CollectionUploadRecord,
) -> _InitialDescriptionReceipt | None:
    if upload.description_publication_receipt_json is None:
        if upload.description_revision == 0:
            return None
        raise RuntimeError("initial collection description receipt is unavailable")
    value = json.loads(upload.description_publication_receipt_json)
    if not isinstance(value, dict) or set(value) != {
        "object_path",
        "provider_revision",
        "published_at",
        "stored_bytes",
        "stored_sha256",
    }:
        raise RuntimeError("initial collection description receipt is invalid")
    if (
        not isinstance(value["object_path"], str)
        or value["provider_revision"] is not None
        and not isinstance(value["provider_revision"], str)
        or not isinstance(value["published_at"], str)
        or not isinstance(value["stored_bytes"], int)
        or isinstance(value["stored_bytes"], bool)
        or not isinstance(value["stored_sha256"], str)
    ):
        raise RuntimeError("initial collection description receipt values are invalid")
    return cast(_InitialDescriptionReceipt, value)


def _initial_tag_receipt(upload: CollectionUploadRecord) -> _InitialTagReceipt:
    if upload.tag_publication_receipt_json is None:
        raise RuntimeError("initial collection tag-head receipt is unavailable")
    value = json.loads(upload.tag_publication_receipt_json)
    if not isinstance(value, dict) or set(value) != {
        "object_path",
        "provider_revision",
        "published_at",
        "stored_bytes",
        "stored_sha256",
    }:
        raise RuntimeError("initial collection tag-head receipt is invalid")
    if (
        not isinstance(value["object_path"], str)
        or value["provider_revision"] is not None
        and not isinstance(value["provider_revision"], str)
        or not isinstance(value["published_at"], str)
        or not isinstance(value["stored_bytes"], int)
        or isinstance(value["stored_bytes"], bool)
        or not isinstance(value["stored_sha256"], str)
    ):
        raise RuntimeError("initial collection tag-head receipt values are invalid")
    return cast(_InitialTagReceipt, value)


def _catalog_archive_parts_json(parts: Sequence[StoredArchivePart]) -> str:
    return canonical_json_bytes([_part_payload(part) for part in parts]).decode("utf-8")


def _catalog_small_archive_object(
    *,
    upload: CollectionUploadRecord,
    current: Any,
    object_order: int,
    verified_at: str,
) -> CollectionArchiveObjectRecord:
    return CollectionArchiveObjectRecord(
        collection_id=upload.collection_id,
        store=upload.archive_store,
        object_id=str(current.object_id),
        object_order=object_order,
        kind=str(current.kind),
        object_path=f"{upload.archive_storage_prefix}/{current.relative_path}",
        plaintext_bytes=int(current.plaintext_bytes),
        stored_bytes=int(current.stored_bytes),
        sha256=str(current.plaintext_sha256),
        stored_sha256=str(current.stored_sha256),
        revision=current.revision,
        uploaded_at=str(current.completed_at),
        verified_at=verified_at,
    )


def _advance_catalog_artifact_objects(session: Session, upload: CollectionUploadRecord) -> None:
    cursor = _catalog_cursor(upload)
    sequence = _cursor_nonnegative_int(cursor, "sequence")
    total = _planner_checkpoint(upload).next_sequence
    if sequence >= total:
        if sequence != total:
            raise RuntimeError("catalog artifact-object cursor exceeds archive authority")
        upload.catalog_phase = "index"
        upload.catalog_cursor_json = "{}"
        return
    record = session.scalar(
        select(CollectionArchiveObjectUploadRecord).where(
            CollectionArchiveObjectUploadRecord.collection_id == upload.collection_id,
            CollectionArchiveObjectUploadRecord.sequence == sequence,
        )
    )
    if record is None or record.sealed_receipt_json is None:
        raise RuntimeError("catalog artifact-object source is unavailable")
    if record.kind == "pack":
        plan = parse_pack_volume_plan(record.plan_json)
        session.execute(
            insert(CollectionArchiveArtifactObjectRecord),
            [
                {
                    "collection_id": format_scalar("sequence63", upload.collection_id),
                    "store": upload.archive_store,
                    "artifact_id": member.artifact_id,
                    "sequence": 0,
                    "object_id": plan.volume_id,
                    "artifact_offset": 0,
                    "object_offset": member.data_offset,
                    "bytes": member.bytes,
                    "member": f"artifacts/{member.artifact_id}",
                }
                for member in plan.members
            ],
        )
    elif record.kind == "segment":
        volume = _parse_sealed_raw(record.sealed_receipt_json)
        if record.source_first_part is None:
            raise RuntimeError("raw archive segment has no source sequence")
        session.add(
            CollectionArchiveArtifactObjectRecord(
                collection_id=upload.collection_id,
                store=upload.archive_store,
                artifact_id=volume.artifact_id,
                sequence=int(record.source_first_part),
                object_id=volume.volume_id,
                artifact_offset=volume.artifact_offset,
                object_offset=0,
                bytes=volume.plaintext_bytes,
                member=None,
            )
        )
    else:
        raise RuntimeError("catalog artifact-object source kind is invalid")
    _set_catalog_cursor(upload, {"sequence": sequence + 1})


class _StagedCanonicalCorpus(Mapping[str, JournalSummary]):
    """Read exact sealed upload journals on demand while indexing one member."""

    def __init__(self, session: Session, collection_id: int) -> None:
        self.session = session
        self.collection_id = collection_id
        self.cache: OrderedDict[str, JournalSummary] = OrderedDict()

    def __getitem__(self, journal_id: str) -> JournalSummary:
        cached = self.cache.get(journal_id)
        if cached is not None:
            self.cache.move_to_end(journal_id)
            return cached
        record = self.session.get(
            CollectionUploadProvenanceJournalRecord, (self.collection_id, journal_id)
        )
        if record is None or record.state != "sealed":
            raise KeyError(journal_id)
        summary = validate_journal_chunks(
            _iter_upload_journal_chunks(self.session, record),
            catalog=admission_provenance_catalog(),
            require_profiles=False,
        )
        if summary.journal_sha256 != record.sha256 or summary.journal_bytes != record.bytes:
            raise ProvenanceValidationError("staged canonical journal identity changed")
        self.cache[journal_id] = summary
        if len(self.cache) > 2:
            self.cache.popitem(last=False)
        return summary

    def __iter__(self) -> Iterator[str]:
        yield from self.session.scalars(
            select(CollectionUploadProvenanceJournalRecord.journal_id)
            .where(CollectionUploadProvenanceJournalRecord.collection_id == self.collection_id)
            .order_by(CollectionUploadProvenanceJournalRecord.journal_id)
        )

    def __len__(self) -> int:
        return int(
            self.session.scalar(
                select(func.count())
                .select_from(CollectionUploadProvenanceJournalRecord)
                .where(CollectionUploadProvenanceJournalRecord.collection_id == self.collection_id)
            )
            or 0
        )


def _advance_catalog_canonical_index(session: Session, upload: CollectionUploadRecord) -> None:
    """Stage one member's exact canonical discovery support before publication."""

    cursor = _catalog_cursor(upload)
    build_id = cursor.get("build_id")
    if build_id is None:
        build_id = begin_index_build(session, collection_id=upload.collection_id)
        _set_catalog_cursor(
            upload,
            {"build_id": build_id, "members_indexed": 0, "memberships_indexed": 0},
        )
        return
    if not isinstance(build_id, str):
        raise RuntimeError("canonical discovery build cursor is invalid")
    indexed = _cursor_nonnegative_int(cursor, "members_indexed")
    memberships_indexed = _cursor_nonnegative_int(cursor, "memberships_indexed")
    after_id = cursor.get("after_artifact_id")
    if after_id is not None and (
        not isinstance(after_id, str) or _SHA256_RE.fullmatch(after_id) is None
    ):
        raise RuntimeError("canonical discovery member cursor is invalid")
    statement = select(CollectionUploadArtifactRecord).where(
        CollectionUploadArtifactRecord.collection_id == upload.collection_id
    )
    if after_id is not None:
        statement = statement.where(CollectionUploadArtifactRecord.artifact_id > after_id)
    member_record = session.scalar(
        statement.order_by(CollectionUploadArtifactRecord.artifact_id).limit(1)
    )
    if member_record is None:
        if indexed != upload.artifact_count:
            raise RuntimeError("canonical discovery did not cover every artifact")
        snapshot_count = int(
            session.scalar(
                select(func.count())
                .select_from(CollectionProvenanceIndexSnapshotRecord)
                .where(CollectionProvenanceIndexSnapshotRecord.build_id == build_id)
            )
            or 0
        )
        actual_memberships = int(
            session.scalar(
                select(func.count())
                .select_from(CollectionProvenanceIndexMembershipRecord)
                .where(CollectionProvenanceIndexMembershipRecord.build_id == build_id)
            )
            or 0
        )
        if actual_memberships != memberships_indexed:
            raise RuntimeError("canonical discovery membership cursor differs")
        complete_index_build(
            session,
            build_id=build_id,
            expected_snapshots=snapshot_count,
            expected_members=indexed,
            expected_memberships=memberships_indexed,
        )
        publish_index_build(session, build_id=build_id)
        upload.catalog_phase = "terminal"
        upload.catalog_cursor_json = "{}"
        return

    binding_record = session.get(
        CollectionUploadArtifactProvenanceBindingRecord,
        (upload.collection_id, member_record.artifact_id),
    )
    if binding_record is None:
        raise RuntimeError("canonical discovery member has no exact provenance binding")
    binding = CollectionArtifactProvenanceBindingDocument.model_validate(
        _provenance_binding_row(binding_record)
    )
    corpus = _StagedCanonicalCorpus(session, upload.collection_id)
    full_primary = corpus[binding.journal.journal_id]
    primary = validate_journal_chunks(
        (
            frame.encoded
            for frame in full_primary.frames[: int(binding.journal.through.sequence) + 1]
        ),
        catalog=admission_provenance_catalog(),
        expected_anchor=binding.journal.model_dump(mode="json"),
        require_exact_tail=True,
        require_profiles=False,
    )
    member = ArtifactMemberIdentityDocument.model_validate(
        {
            "artifact_id": member_record.artifact_id,
            "bytes": format_scalar("sequence63", member_record.bytes),
            "sha256": member_record.sha256,
        }
    )
    relevance = member_relevance(
        member=member,
        binding=binding,
        primary=primary,
        corpus=corpus,
        delivery_context_id=upload.delivery_context_id,
        catalog=admission_provenance_catalog(),
    )
    for summary in snapshots_for_relevance(
        relevance, corpus=corpus, catalog=admission_provenance_catalog()
    ):
        snapshot_key = (build_id, summary.journal_id, summary.journal_sha256)
        if session.get(CollectionProvenanceIndexSnapshotRecord, snapshot_key) is not None:
            continue
        stage_snapshot_header(session, build_id=build_id, summary=summary)
        session.flush()
        for start in range(0, len(summary.frames), 128):
            stage_entry_page(session, build_id=build_id, summary=summary, start=start)
        session.flush()
        rows = iter_index_assertions(summary)
        while batch := tuple(islice(rows, 2)):
            stage_assertion_page(session, build_id=build_id, rows=batch)
    stage_member(
        session,
        build_id=build_id,
        artifact_id=member_record.artifact_id,
        bytes=member_record.bytes,
        sha256=member_record.sha256,
        journal_id=primary.journal_id,
        prefix_sha256=primary.journal_sha256,
        delivery_association_id=binding.delivery_association_id,
    )
    session.flush()
    membership_rows = (
        (member_record.artifact_id, row_key, scope)
        for row_key, scope in relevance_row_keys(relevance)
    )
    while membership_batch := tuple(islice(membership_rows, 1024)):
        stage_membership_page(session, build_id=build_id, rows=membership_batch)
        memberships_indexed += len(membership_batch)
    _set_catalog_cursor(
        upload,
        {
            "build_id": build_id,
            "after_artifact_id": member_record.artifact_id,
            "members_indexed": indexed + 1,
            "memberships_indexed": memberships_indexed,
        },
    )


def _collection_id(value: int) -> int:
    try:
        return int(normalize_collection_id(value))
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc


def _normalize_idempotency_key(value: str) -> str:
    normalized = value.strip()
    if not normalized or normalized != value or len(normalized) > 200:
        raise BadRequest("idempotency_key is invalid")
    return normalized


def _canonical_tag_batch(
    values: Sequence[CollectionTag], *, allow_empty: bool
) -> tuple[CollectionTag, ...]:
    if (not values and not allow_empty) or len(values) > COLLECTION_TAG_REQUEST_MEMBERS_MAX:
        minimum = 0 if allow_empty else 1
        raise BadRequest(
            f"collection tag batch must contain {minimum} to "
            f"{COLLECTION_TAG_REQUEST_MEMBERS_MAX} tags"
        )
    try:
        canonical = tuple(validate_collection_tag(value) for value in values)
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    if len(set(canonical)) != len(canonical):
        raise BadRequest("collection tag batch must not contain duplicates")
    return tuple(sorted(canonical, key=lambda value: value.encode("utf-8")))


def _require_tag_assignment_access(principal: Principal, tags: Sequence[str]) -> None:
    for tag in tags:
        if not principal.allows(COLLECTION_TAGS_MANAGE, tag_resource(tag)):
            raise NotFound("collection tag assignment is not available")


def _upload_tag_count(session: Session, collection_id: int) -> int:
    return int(
        session.scalar(
            select(func.count(CollectionUploadTagRecord.tag_sha256)).where(
                CollectionUploadTagRecord.collection_id == collection_id
            )
        )
        or 0
    )


def _require_upload_create_access(
    session: Session,
    collection_id: int,
    principal: Principal,
    *,
    additional_tags: Sequence[CollectionTag] = (),
) -> int:
    """Authorize one staged tag set without materializing all members at once."""

    existing_count = _upload_tag_count(session, collection_id)
    resources = permission_resources(principal, COLLECTIONS_CREATE)
    if ALL_RESOURCES in resources:
        return existing_count
    allowed_tag_hashes = tag_hashes(resources)
    additional_hashes = tuple(collection_tag_sha256(tag) for tag in additional_tags)
    if (
        (existing_count == 0 and not additional_hashes)
        or not allowed_tag_hashes
        or any(digest not in allowed_tag_hashes for digest in additional_hashes)
    ):
        raise NotFound("collection creation is not available")
    staged_hashes = session.scalars(
        select(CollectionUploadTagRecord.tag_sha256)
        .where(CollectionUploadTagRecord.collection_id == collection_id)
        .order_by(CollectionUploadTagRecord.tag_sha256)
        .execution_options(yield_per=COLLECTION_TAG_REQUEST_MEMBERS_MAX)
    )
    for digest in staged_hashes:
        if digest not in allowed_tag_hashes:
            raise NotFound("collection creation is not available")
    return existing_count


def _collection_upload_creation_identity(
    *,
    ingest_source: str | None,
    description: CollectionDescription | None,
    initial_tag_set_identity: str,
    archive_store: str,
    use_cache: bool,
    copy_to: tuple[str, ...],
    event_context_json: str | None,
    custody_mode: CollectionUploadCustodyMode,
) -> CollectionUploadCreationIdentityDocument:
    event_context = json.loads(event_context_json) if event_context_json is not None else None
    if event_context is not None and not isinstance(event_context, dict):  # pragma: no cover
        raise RuntimeError("normalized upload event context is not an object")
    return CollectionUploadCreationIdentityDocument.seal(
        CollectionUploadCreationIdentityPayload(
            ingest_source=ingest_source,
            description=description,
            initial_tag_set_identity=initial_tag_set_identity,
            archive_store=archive_store,
            use_cache=use_cache,
            copy_to=list(copy_to),
            event_context=event_context,
            custody_mode=custody_mode,
        )
    )


def _normalize_copy_destinations(
    values: Sequence[str],
    *,
    source_store: str,
    archive_stores: ArchiveStoreRegistry,
    require_availability: bool = True,
) -> tuple[str, ...]:
    if isinstance(values, str):
        raise BadRequest("copy_to must be a list of archive store names")
    destinations: list[str] = []
    for value in values:
        if require_availability:
            try:
                archive_stores.require(value)
            except ValueError as exc:
                raise BadRequest(str(exc)) from exc
        if value == source_store:
            raise BadRequest("copy_to destination must differ from archive_store")
        destinations.append(value)
    if len(destinations) != len(set(destinations)):
        raise BadRequest("copy_to destinations must be unique")
    return tuple(sorted(destinations))


def _cancel_copy_intents(session: Session, collection_id: int) -> None:
    for intent in session.scalars(
        select(CollectionUploadCopyIntentRecord).where(
            CollectionUploadCopyIntentRecord.collection_id == collection_id,
            CollectionUploadCopyIntentRecord.state == "accepted",
        )
    ):
        intent.state = "canceled"
        intent.next_attempt_at = None


def _require_transform_output_intent(
    session: Session,
    *,
    initiator: Principal,
    idempotency_key: str,
    ingest_source: str | None,
    archive_store: str | None,
    use_cache: bool | None,
    copy_to: Sequence[str] | None,
    tags: Sequence[CollectionTag],
    initial_tag_set_identity: str,
) -> None:
    # The transform namespace is reserved for claim-scoped capability principals.
    prefix = "processing:"
    if not initiator.id.startswith(prefix):
        return
    execution_id = initiator.id.removeprefix(prefix)
    if _SHA256_RE.fullmatch(execution_id) is None:
        raise Forbidden("transform output collections require a scoped capability")
    claim = session.scalar(
        select(CollectionProcessingClaimRecord)
        .where(CollectionProcessingClaimRecord.execution_id == execution_id)
        .with_for_update()
    )
    if (
        claim is None
        or claim.state != "active"
        or claim.plan_sealed_at is None
        or parse_utc_timestamp(claim.expires_at) <= utc_epoch_ns_now()
        or initiator.key_id != claim.consumer_key_id
        or claim.output_policy_json is None
    ):
        raise Forbidden("transform output intent is not active")
    policy = OutputCollectionPolicy.model_validate_json(claim.output_policy_json)
    tag_set = CollectionTagSet(MemoryCollectionTagNodeStore())
    for tag in policy.tags:
        tag_set = tag_set.insert(tag)
    if (
        idempotency_key != execution_id
        or ingest_source != f"processing:{execution_id}"
        or archive_store != policy.archive_store
        or use_cache != policy.use_cache
        or tuple(copy_to or ()) != policy.copy_to
        or tuple(tags) != policy.tags[:COLLECTION_TAG_REQUEST_MEMBERS_MAX]
        or initial_tag_set_identity != tag_set.identity
    ):
        raise Forbidden("collection upload differs from the sealed transform output intent")


def _require_transform_output_disposition_coverage(
    session: Session,
    upload: CollectionUploadRecord,
) -> None:
    """Require exact coverage of sealed disposition outputs by staged members.

    Target artifacts enter Riverhog custody incrementally before the producer
    can seal its complete production and disposition evidence. Completion
    is the first boundary where Riverhog can require the exact bijection; file
    registration deliberately remains resumable construction state.
    """

    prefix = "processing:"
    if not upload.initiated_by_principal_id.startswith(prefix):
        return
    if upload.completion_requirement_json is None:
        raise Conflict("transform completion requires its accepted completion declaration")
    execution_id = upload.initiated_by_principal_id.removeprefix(prefix)
    claim = session.scalar(
        select(CollectionProcessingClaimRecord).where(
            CollectionProcessingClaimRecord.execution_id == execution_id
        )
    )
    if claim is None:
        raise Conflict("transform output claim is unavailable")
    disposition_set = session.get(CollectionProcessingDispositionSetRecord, claim.id)
    if disposition_set is None or disposition_set.state != "sealed":
        raise Conflict("transform completion requires a sealed disposition set")
    missing_edge = session.scalar(
        select(CollectionUploadArtifactRecord.artifact_id)
        .where(
            CollectionUploadArtifactRecord.collection_id == upload.collection_id,
            ~exists().where(
                CollectionProcessingDispositionOutputRecord.claim_id == claim.id,
                CollectionProcessingDispositionOutputRecord.output_artifact_id
                == CollectionUploadArtifactRecord.artifact_id,
            ),
        )
        .limit(1)
    )
    if missing_edge is not None:
        raise Conflict(f"transform output member has no exact disposition edge: {missing_edge}")
    missing_member = session.scalar(
        select(CollectionProcessingDispositionOutputRecord.output_artifact_id)
        .where(
            CollectionProcessingDispositionOutputRecord.claim_id == claim.id,
            ~exists().where(
                CollectionUploadArtifactRecord.collection_id == upload.collection_id,
                CollectionUploadArtifactRecord.artifact_id
                == CollectionProcessingDispositionOutputRecord.output_artifact_id,
            ),
        )
        .limit(1)
    )
    if missing_member is not None:
        raise Conflict(f"transform disposition output member is absent: {missing_member}")


def _normalize_artifact(
    value: CollectionUploadArtifactIn,
    *,
    constraints: CollectionUploadRegistrationConstraintsDocument,
) -> _RegisteredArtifact:
    byte_count = value.bytes
    sha256 = value.sha256
    raw_manifest = collection_upload_raw_digest_summary(value, constraints)
    return {
        "artifact_id": str(value.artifact_id),
        "bytes": byte_count,
        "sha256": sha256,
        "raw_part_plaintext_bytes": (
            raw_manifest.part_plaintext_bytes if raw_manifest is not None else None
        ),
        "raw_part_count": raw_manifest.part_count if raw_manifest is not None else None,
        "raw_part_ordered_sha256": (
            raw_manifest.ordered_part_sha256 if raw_manifest is not None else None
        ),
    }


def _registered_artifact_identity(record: CollectionUploadArtifactRecord) -> _RegisteredArtifact:
    return {
        "artifact_id": record.artifact_id,
        "bytes": record.bytes,
        "sha256": record.sha256,
        "raw_part_plaintext_bytes": record.raw_part_plaintext_bytes,
        "raw_part_count": record.raw_part_count,
        "raw_part_ordered_sha256": record.raw_part_ordered_sha256,
    }


def _provenance_binding_row(
    row: CollectionUploadArtifactProvenanceBindingRecord,
) -> dict[str, object]:
    document = CollectionArtifactProvenanceBindingBatchDocument.model_validate(
        {
            "bindings": [
                {
                    "artifact_id": row.artifact_id,
                    "journal": {
                        "journal_id": row.journal_id,
                        "through": {
                            "entry_id": row.through_entry_id,
                            "sequence": format_scalar("sequence63", row.through_sequence),
                            "json_sha256": row.through_json_sha256,
                        },
                        "prefix_sha256": row.prefix_sha256,
                        "prefix_bytes": format_scalar("nonnegative", row.prefix_bytes),
                    },
                    "delivery_association_id": row.delivery_association_id,
                }
            ]
        }
    )
    return document.bindings[0].model_dump(mode="json")


def _sealed_journal_history_anchor(
    journal: CollectionUploadProvenanceJournalRecord,
) -> HistoryJournalAnchor:
    if journal.state != "sealed" or any(
        value is None
        for value in (
            journal.terminal_entry_id,
            journal.terminal_sequence,
            journal.terminal_json_sha256,
        )
    ):
        raise Conflict("member history selects an unsealed journal")
    return HistoryJournalAnchor(
        journal_id=journal.journal_id,
        through_entry_id=cast(str, journal.terminal_entry_id),
        through_sequence=cast(int, journal.terminal_sequence),
        through_json_sha256=cast(str, journal.terminal_json_sha256),
        prefix_sha256=journal.sha256,
        prefix_bytes=journal.bytes,
    )


def _stage_provenance_structure(
    session: Session,
    *,
    collection_id: int,
    object_id: str,
    kind: str,
    relative_path: str,
    content: bytes,
) -> None:
    existing = session.get(CollectionUploadProvenanceStructureRecord, (collection_id, object_id))
    if existing is None:
        session.add(
            CollectionUploadProvenanceStructureRecord(
                collection_id=collection_id,
                object_id=object_id,
                kind=kind,
                relative_path=relative_path,
                content=content,
            )
        )
    elif (existing.kind, existing.relative_path, existing.content) != (
        kind,
        relative_path,
        content,
    ):
        raise Conflict("provenance structure retry differs from accepted bytes")


def _member_history_binding_row(row: CollectionUploadMemberHistoryRecord) -> MemberHistoryBinding:
    return MemberHistoryBinding.from_mapping(json.loads(row.binding_json))


def _iter_staged_history_pages(
    session: Session, collection_id: int, authority: RecordSetRef
) -> Iterator[RecordPage]:
    for ordinal in range(authority.record_count + 1):
        object_id = (
            "provenance-record-page-"
            + authority.records_sha256
            + "-"
            + format_archive_sequence(ordinal)
        )
        row = session.get(CollectionUploadProvenanceStructureRecord, (collection_id, object_id))
        if row is None:
            raise Conflict("required member history page is missing")
        page = RecordPage.from_json_bytes(row.content)
        yield page
        if page.terminal:
            return
    raise Conflict("member history set has no terminal")


def _staged_history_closure(session: Session, collection_id: int) -> MemberHistoryClosure:
    def read_structure(path: str) -> Iterator[bytes]:
        row = session.get(
            CollectionUploadProvenanceStructureRecord,
            (collection_id, provenance_structure_object_id(path)),
        )
        if row is None or row.relative_path != path:
            raise Conflict("required history structure is missing from staged custody")
        yield row.content

    def read_journal(journal_id: str, end: int | None) -> Iterator[bytes]:
        row = session.get(CollectionUploadProvenanceJournalRecord, (collection_id, journal_id))
        if row is None or row.state != "sealed":
            raise Conflict("required history journal is absent or unsealed")
        yield from _iter_upload_journal_chunks(session, row, through_bytes=end)

    return MemberHistoryClosure(
        MemberHistoryStore(read_structure),
        read_journal,
        member_role=COLLECTION_MEMBER_ROLE,
        catalog=admission_provenance_catalog(),
    )


def _validate_staged_canonical_journal_set(
    session: Session, upload: CollectionUploadRecord
) -> None:
    """Resolve explicit member roots/imports and exact documentary dependencies."""
    collection_id = upload.collection_id
    with (
        _staged_history_closure(session, collection_id) as closure,
        CanonicalCorpusValidator() as corpus,
    ):
        for final in session.scalars(
            select(CollectionUploadMemberHistoryRecord)
            .where(CollectionUploadMemberHistoryRecord.collection_id == collection_id)
            .order_by(CollectionUploadMemberHistoryRecord.artifact_id)
            .execution_options(yield_per=_FINALIZATION_FILE_BATCH)
        ):
            # Publication retains every selected local documentary root. Each
            # inherited selection keeps its own exact accepted transfer extent.
            closure.resolve(_member_history_binding_row(final), extent=RETAINED_HISTORY_EXTENT)
        count = 0
        for record in session.scalars(
            select(CollectionUploadProvenanceJournalRecord)
            .where(CollectionUploadProvenanceJournalRecord.collection_id == collection_id)
            .order_by(CollectionUploadProvenanceJournalRecord.journal_id)
            .execution_options(yield_per=_FINALIZATION_FILE_BATCH)
        ):
            if record.state != "sealed":
                raise Conflict("canonical provenance corpus contains an unsealed journal")
            if not closure.contains_journal(record.journal_id):
                raise Conflict("canonical provenance corpus contains unrelated journals")
            corpus.add(
                validate_journal_chunks(
                    _iter_upload_journal_chunks(session, record),
                    catalog=admission_provenance_catalog(),
                    require_profiles=False,
                )
            )
            count += 1
        if count == 0:
            raise Conflict("canonical provenance corpus is absent")
        corpus.validate()
        _validate_staged_execution_completion(session, upload, closure)


def _validate_staged_execution_completion(
    session: Session, upload: CollectionUploadRecord, closure: MemberHistoryClosure
) -> None:
    if upload.completion_requirement_json is None:
        if upload.completion_journal_id is not None:
            raise Conflict("completion selection lacks an accepted requirement")
        return
    requirement = CollectionCompletionRequirementDocument.model_validate_json(
        upload.completion_requirement_json
    )
    journal = session.get(
        CollectionUploadProvenanceJournalRecord,
        (upload.collection_id, upload.completion_journal_id),
    )
    if journal is None or journal.state != "sealed":
        raise Conflict("accepted completion journal is missing or unsealed")
    completion_anchor = _sealed_journal_history_anchor(journal)
    summary = closure.summary_at(completion_anchor)
    if upload.completion_recorded_at is None or upload.completion_records_sha256 is None:
        raise Conflict("completion lacks its accepted immutable recording checkpoint")
    if any(
        frame.document["recorded_at"] != upload.completion_recorded_at for frame in summary.frames
    ):
        raise Conflict("completion metadata differs from its accepted recording context")
    facts = [
        row
        for row in summary.graph.get("extensions", ())
        if row["property"] == COLLECTION_PRODUCTION_CONTRACT_ID + "/execution-completion"
    ]
    if len(facts) != 1:
        raise Conflict("required canonical execution completion is absent or ambiguous")
    fact = facts[0]
    subject = fact["subject"]
    activity = summary.graph_validation.objects.get(subject["object_id"])
    if (
        subject["scope"] != "local"
        or subject["object_type"] != "activity"
        or activity is None
        or activity["kind"] != "recording"
        or activity["outcome"] != "success"
    ):
        raise Conflict("completion is not attached to its distinct successful recording activity")
    pack = collection_production_contract()
    if fact["value"]["type"] != "json" or fact["value"]["value"]["profile"] != {
        "contract_id": pack.contract_id,
        "contract_sha256": pack.contract_sha256,
        "schema_id": EXECUTION_COMPLETION_SCHEMA_ID,
    }:
        raise Conflict("required completion profile pin differs")
    claim = session.scalar(
        select(CollectionProcessingClaimRecord).where(
            CollectionProcessingClaimRecord.execution_id == requirement.execution_id
        )
    )
    disposition_row = (
        None if claim is None else session.get(CollectionProcessingDispositionSetRecord, claim.id)
    )
    if (
        disposition_row is None
        or disposition_row.state != "sealed"
        or disposition_row.identity_sha256 is None
    ):
        raise Conflict("execution completion lacks accepted sealed dispositions")
    disposition = ArtifactDispositionSetIdentity(
        disposition_row.disposition_count,
        disposition_row.output_edge_count,
        disposition_row.output_artifact_count,
        disposition_row.identity_sha256,
    )

    def outputs() -> Iterator[Mapping[str, Any]]:
        for binding in session.scalars(
            select(CollectionUploadArtifactProvenanceBindingRecord)
            .where(
                CollectionUploadArtifactProvenanceBindingRecord.collection_id
                == upload.collection_id
            )
            .order_by(CollectionUploadArtifactProvenanceBindingRecord.artifact_id)
            .execution_options(yield_per=_FINALIZATION_FILE_BATCH)
        ):
            final = session.get(
                CollectionUploadMemberHistoryRecord, (upload.collection_id, binding.artifact_id)
            )
            member = session.get(
                CollectionUploadArtifactRecord, (upload.collection_id, binding.artifact_id)
            )
            if final is None or member is None or binding.completion_output_id is None:
                raise Conflict("completion output has no exact primary/key/history correspondence")
            history = closure.store.descriptor(_member_history_binding_row(final))
            if not any(
                root.journal == completion_anchor
                for root in closure.store.roots(
                    _member_history_binding_row(final), extent=BOUND_HISTORY_EXTENT
                )
            ):
                raise Conflict("member history omits its required exact completion snapshot")
            primary = closure.summary_at(history.primary.journal)
            association = primary.graph_validation.objects[binding.delivery_association_id]
            yield {
                "output_id": binding.completion_output_id,
                "artifact_id": binding.artifact_id,
                "bytes": str(member.bytes),
                "sha256": member.sha256,
                "primary": history.primary.to_mapping(),
                "state": external_reference(primary, association["state"]["object_id"]),
            }

    def imports() -> Iterator[Mapping[str, Any]]:
        for final in session.scalars(
            select(CollectionUploadMemberHistoryRecord)
            .where(CollectionUploadMemberHistoryRecord.collection_id == upload.collection_id)
            .order_by(CollectionUploadMemberHistoryRecord.artifact_id)
            .execution_options(yield_per=_FINALIZATION_FILE_BATCH)
        ):
            history = closure.store.descriptor(_member_history_binding_row(final))
            yield {"artifact_id": final.artifact_id, "imports": history.imports.to_mapping()}

    validate_completion_preimages(
        requirement=requirement,
        completion=fact["value"]["value"]["data"],
        extensions=summary.graph["extensions"],
        subject=subject,
        expected_construction={
            "format": "riverhog-execution-construction/v1",
            "collection_id": str(upload.collection_id),
            "construction_identity_sha256": upload.creation_identity_sha256,
            "delivery_context_id": upload.delivery_context_id,
            "completion_requirement": requirement.model_dump(mode="json"),
        },
        expected_outputs=outputs(),
        expected_imports=imports(),
        disposition=disposition,
        expected_records_sha256=upload.completion_records_sha256,
    )


def _provenance_volume_document(
    *,
    archive_generation: str,
    artifact_set_sha256: str,
    sequence: int,
    payload: bytes,
    first_artifact_id: str | None = None,
    last_artifact_id: str | None = None,
    binding_count: int | None = None,
    journal: CollectionUploadProvenanceJournalRecord | None = None,
    journal_offset: int | None = None,
) -> ProvenanceVolumeDocument:
    kind: Literal["bindings", "journal"] = "journal" if journal is not None else "bindings"
    identity = ProvenancePayload(
        kind=kind,
        sequence=sequence,
        bytes=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
    )
    if journal is None:
        return ProvenanceVolumeDocument(
            archive_generation=archive_generation,
            artifact_set_sha256=artifact_set_sha256,
            sequence=sequence,
            payload=identity,
            first_artifact_id=first_artifact_id,
            last_artifact_id=last_artifact_id,
            binding_count=binding_count,
        )
    return ProvenanceVolumeDocument(
        archive_generation=archive_generation,
        artifact_set_sha256=artifact_set_sha256,
        sequence=sequence,
        payload=identity,
        journal_id=journal.journal_id,
        journal_offset=journal_offset,
        journal_bytes=journal.bytes,
        journal_sha256=journal.sha256,
    )


def _provenance_commitment(upload: CollectionUploadRecord) -> CheckpointSHA256:
    if upload.provenance_archive_hash_state is not None:
        return CheckpointSHA256.from_state(upload.provenance_archive_hash_state)
    if upload.provenance_archive_next_sequence != 0:
        raise RuntimeError("provenance volume commitment state is missing")
    digest = CheckpointSHA256()
    digest.update(PROVENANCE_SEQUENCE_DOMAIN)
    return digest


def _next_provenance_publication_journal(
    session: Session,
    upload: CollectionUploadRecord,
) -> CollectionUploadProvenanceJournalRecord | None:
    if upload.provenance_archive_current_journal_id is not None:
        journal = session.get(
            CollectionUploadProvenanceJournalRecord,
            (upload.collection_id, upload.provenance_archive_current_journal_id),
        )
    else:
        statement = select(CollectionUploadProvenanceJournalRecord).where(
            CollectionUploadProvenanceJournalRecord.collection_id == upload.collection_id
        )
        if upload.provenance_archive_last_journal_id is not None:
            statement = statement.where(
                CollectionUploadProvenanceJournalRecord.journal_id
                > upload.provenance_archive_last_journal_id
            )
        journal = session.scalar(
            statement.order_by(CollectionUploadProvenanceJournalRecord.journal_id).limit(1)
        )
    if journal is not None and journal.state != "sealed":
        raise Conflict(f"provenance journal is not sealed: {journal.journal_id}")
    return journal


def _upload_journal_range_bytes(
    session: Session,
    collection_id: int,
    journal_id: str,
    *,
    offset: int,
    size: int,
) -> bytes:
    if size < 1 or size > PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX:
        raise RuntimeError("provenance journal publication range is invalid")
    rows = session.execute(
        select(
            CollectionUploadProvenanceJournalChunkRecord.byte_offset,
            CollectionUploadProvenanceJournalChunkRecord.content,
        )
        .where(
            CollectionUploadProvenanceJournalChunkRecord.collection_id == collection_id,
            CollectionUploadProvenanceJournalChunkRecord.journal_id == journal_id,
            CollectionUploadProvenanceJournalChunkRecord.byte_offset
            + func.length(CollectionUploadProvenanceJournalChunkRecord.content)
            > offset,
            CollectionUploadProvenanceJournalChunkRecord.byte_offset < offset + size,
        )
        .order_by(CollectionUploadProvenanceJournalChunkRecord.byte_offset)
    )
    content = bytearray()
    expected = offset
    for row in rows:
        raw = bytes(row.content)
        row_offset = int(row.byte_offset)
        start = max(0, expected - row_offset)
        if row_offset > expected or start >= len(raw):
            raise RuntimeError("provenance journal chunks are not contiguous")
        take = min(len(raw) - start, size - len(content))
        content.extend(raw[start : start + take])
        expected += take
        if len(content) == size:
            break
    if len(content) != size:
        raise RuntimeError("provenance journal publication range is unavailable")
    return bytes(content)


def _planner_checkpoint(upload: CollectionUploadRecord) -> Any:
    if not upload.planner_checkpoint_json:
        raise RuntimeError("collection upload planner checkpoint is missing")
    return parse_incremental_volume_planner_checkpoint(upload.planner_checkpoint_json)


def _seal_open_collection_upload(
    session: Session,
    upload: CollectionUploadRecord,
    *,
    checkpoint: Any,
    config: RuntimeConfig,
) -> None:
    collection_id = upload.collection_id
    incomplete_raw = session.scalar(
        select(CollectionUploadArtifactRecord.artifact_id)
        .where(
            CollectionUploadArtifactRecord.collection_id == collection_id,
            CollectionUploadArtifactRecord.raw_part_count.is_not(None),
            or_(
                CollectionUploadArtifactRecord.raw_parts_accepted
                != CollectionUploadArtifactRecord.raw_part_count,
                CollectionUploadArtifactRecord.raw_part_commitment_sha256
                != CollectionUploadArtifactRecord.raw_part_ordered_sha256,
            ),
        )
        .limit(1)
    )
    if incomplete_raw is not None:
        raise Conflict(f"raw source digest sequence is incomplete: {incomplete_raw}")
    incomplete_provenance = session.scalar(
        select(CollectionUploadProvenanceJournalRecord.journal_id)
        .where(
            CollectionUploadProvenanceJournalRecord.collection_id == collection_id,
            CollectionUploadProvenanceJournalRecord.state != "sealed",
        )
        .limit(1)
    )
    if incomplete_provenance is not None:
        raise Conflict(f"provenance journal is not sealed: {incomplete_provenance}")
    _require_transform_output_disposition_coverage(session, upload)
    if upload.artifact_count < 1:
        raise Conflict("collection upload has no registered artifacts")
    _require_pending_pack_matches_registration(session, upload, checkpoint)
    batch = advance_incremental_volume_plan(checkpoint, (), final=True)
    sealed = batch.checkpoint
    if (
        not sealed.closed
        or sealed.artifacts_seen != upload.artifact_count
        or sealed.bytes_seen != upload.artifact_bytes
    ):
        raise Conflict("collection upload planner differs from registered artifacts")
    _persist_plan_batch(session, upload=upload, batch=batch)
    upload.planner_checkpoint_json = incremental_volume_planner_checkpoint_bytes(sealed).decode(
        "utf-8"
    )
    upload.catalog_artifact_set_identity = None
    upload.catalog_phase = "artifact-set-identity"
    upload.catalog_cursor_json = "{}"
    upload.catalog_hash_state = None
    custody_pending = upload.custody_mode == "custody-transfer" and not _has_complete_payload_seal(
        upload
    )
    upload.state = "closing" if custody_pending else "uploading"
    if custody_pending:
        _touch_upload(upload, config=config)
    else:
        upload.lease_expires_at = None
    upload.provenance_identity = None
    upload.closed_at = utc_timestamp_now()
    upload.last_activity_at = upload.closed_at
    upload.archive_phase = "uploading"
    upload.archive_phase_updated_at = upload.closed_at
    upload.archive_next_attempt_at = None
    upload.archive_failure = None
    session.flush()


def _persist_plan_batch(session: Session, *, upload: CollectionUploadRecord, batch: Any) -> None:
    if not upload.archive_storage_prefix:
        raise RuntimeError("collection archive storage prefix is missing")
    now = utc_timestamp_now()
    coverage: list[tuple[str, str]] = []
    for plan in batch.packs:
        plan_json = pack_volume_plan_bytes(plan).decode("utf-8")
        relative = f"volumes/{plan.volume_id}.tar.age"
        upload.archive_objects.append(
            CollectionArchiveObjectUploadRecord(
                collection_id=upload.collection_id,
                object_id=plan.volume_id,
                sequence=plan.sequence,
                kind="pack",
                relative_path=relative,
                object_path=f"{upload.archive_storage_prefix}/{relative}",
                plaintext_bytes=plan.plaintext_bytes,
                source_bytes=sum(current.bytes for current in plan.members),
                unit_plaintext_bytes=plan.part_plaintext_bytes,
                plan_json=plan_json,
                plan_sha256=plan.plan_sha256,
                state="planned",
                uploaded_bytes=0,
                uploaded_units=0,
                total_units=len(plan.units),
                updated_at=now,
            )
        )
        for member in plan.members:
            coverage.append((member.artifact_id, plan.volume_id))
    for plan in batch.raw_volumes:
        plan_json = raw_volume_plan_bytes(plan).decode("utf-8")
        relative = f"volumes/{plan.volume_id}.bin.age"
        upload.archive_objects.append(
            CollectionArchiveObjectUploadRecord(
                collection_id=upload.collection_id,
                object_id=plan.volume_id,
                sequence=plan.sequence,
                kind="segment",
                relative_path=relative,
                object_path=f"{upload.archive_storage_prefix}/{relative}",
                plaintext_bytes=plan.plaintext_bytes,
                source_bytes=plan.plaintext_bytes,
                source_artifact_id=plan.artifact_id,
                source_first_part=(
                    plan.artifact_offset // batch.checkpoint.policy.raw_part_plaintext_bytes
                ),
                source_part_count=max(
                    1,
                    (plan.plaintext_bytes + batch.checkpoint.policy.raw_part_plaintext_bytes - 1)
                    // batch.checkpoint.policy.raw_part_plaintext_bytes,
                ),
                unit_plaintext_bytes=batch.checkpoint.policy.raw_part_plaintext_bytes,
                plan_json=plan_json,
                plan_sha256=hashlib.sha256(plan_json.encode()).hexdigest(),
                state="planned",
                uploaded_bytes=0,
                uploaded_units=0,
                total_units=max(
                    1,
                    (plan.plaintext_bytes + batch.checkpoint.policy.raw_part_plaintext_bytes - 1)
                    // batch.checkpoint.policy.raw_part_plaintext_bytes,
                ),
                updated_at=now,
            )
        )
        coverage.append((plan.artifact_id, plan.volume_id))
    # The archive objects are appended through a relationship. Flush their
    # primary keys before adding the independent coverage rows with two FKs.
    session.flush()
    for artifact_id, volume_id in coverage:
        session.add(
            CollectionUploadArtifactVolumeRecord(
                collection_id=upload.collection_id,
                artifact_id=artifact_id,
                object_id=volume_id,
            )
        )
    session.flush()


def _require_pending_pack_matches_registration(
    session: Session,
    upload: CollectionUploadRecord,
    checkpoint: Any,
) -> None:
    """Verify the only planner state not yet sealed into immutable volume plans."""

    pending = checkpoint.pending_pack_artifacts
    if not pending:
        return
    first_order = checkpoint.artifacts_seen - len(pending)
    rows = list(
        session.execute(
            select(
                CollectionUploadArtifactRecord.artifact_id,
                CollectionUploadArtifactRecord.bytes,
                CollectionUploadArtifactRecord.sha256,
            )
            .where(
                CollectionUploadArtifactRecord.collection_id == upload.collection_id,
                CollectionUploadArtifactRecord.artifact_order >= first_order,
            )
            .order_by(CollectionUploadArtifactRecord.artifact_order)
            .limit(len(pending) + 1)
        )
    )
    expected = [(current.artifact_id, current.bytes, current.sha256) for current in pending]
    actual = [(row.artifact_id, row.bytes, row.sha256) for row in rows]
    if actual != expected:
        raise Conflict("collection upload volume plans differ from registered artifacts")


def _ready_for_finalization(session: Session, upload: CollectionUploadRecord) -> bool:
    checkpoint = _planner_checkpoint(upload)
    sealed = int(
        session.scalar(
            select(func.count(CollectionArchiveObjectUploadRecord.object_id)).where(
                CollectionArchiveObjectUploadRecord.collection_id == upload.collection_id,
                CollectionArchiveObjectUploadRecord.state == "sealed",
            )
        )
        or 0
    )
    return bool(
        checkpoint.closed
        and checkpoint.next_sequence > 0
        and sealed == checkpoint.next_sequence
        and _has_complete_payload_seal(upload)
    )


def _has_complete_payload_seal(upload: CollectionUploadRecord) -> bool:
    return bool(
        upload.artifact_count > 0
        and upload.payload_sealed_artifact_count == upload.artifact_count
        and upload.payload_sealed_artifact_bytes == upload.artifact_bytes
    )


def _mark_finalization_ready(upload: CollectionUploadRecord, *, now: str) -> None:
    upload.state = "finalizing"
    upload.lease_expires_at = None
    upload.archive_phase = "finalization_queued"
    upload.archive_phase_updated_at = now
    upload.archive_next_attempt_at = now
    upload.archive_failure = None


def _registration_constraints_payload(policy: CollectionVolumePolicy) -> dict[str, str]:
    return {
        "pack_member_bytes": format_scalar("nonnegative", policy.pack_member_bytes),
        "raw_part_plaintext_bytes": format_scalar("nonnegative", policy.raw_part_plaintext_bytes),
    }


def _artifact_payload(record: CollectionUploadArtifactRecord) -> dict[str, object]:
    return {
        "artifact_id": record.artifact_id,
        "bytes": format_scalar("nonnegative", record.bytes),
        "sha256": record.sha256,
        "payload_sealed": record.payload_sealed_at is not None,
        "custody_receipt": (
            CollectionUploadArtifactCustodyReceiptDocument.model_validate_json(
                record.custody_receipt_json
            )
            if record.custody_receipt_json is not None
            else None
        ),
    }


def _raw_digest_progress(record: CollectionUploadArtifactRecord) -> dict[str, object]:
    if record.raw_part_count is None:
        raise TypeError("raw digest progress requires a raw upload artifact")
    accepted = int(record.raw_parts_accepted)
    expected = int(record.raw_part_count)
    return {
        "artifact_id": record.artifact_id,
        "accepted_parts": format_scalar("nonnegative", accepted),
        "expected_parts": format_scalar("nonnegative", expected),
        "complete": accepted == expected,
    }


def _journal_payload(
    record: CollectionUploadProvenanceJournalRecord,
) -> dict[str, object]:
    anchor = None
    if record.state == "sealed":
        if (
            record.terminal_entry_id is None
            or record.terminal_sequence is None
            or record.terminal_json_sha256 is None
        ):
            raise RuntimeError("sealed canonical journal has no exact tail anchor")
        anchor = {
            "journal_id": record.journal_id,
            "through": {
                "entry_id": record.terminal_entry_id,
                "sequence": format_scalar("sequence63", record.terminal_sequence),
                "json_sha256": record.terminal_json_sha256,
            },
            "prefix_sha256": record.sha256,
            "prefix_bytes": format_scalar("nonnegative", record.bytes),
        }
    return CollectionUploadProvenanceJournalStatusDocument.model_validate(
        {
            "journal_id": record.journal_id,
            "state": record.state,
            "bytes": format_scalar("nonnegative", record.bytes),
            "sha256": record.sha256,
            "accepted_bytes": format_scalar("nonnegative", record.accepted_bytes),
            "failure": record.failure,
            "anchor": anchor,
        }
    ).model_dump(mode="json")


def _iter_upload_journal_chunks(
    session: Session,
    record: CollectionUploadProvenanceJournalRecord,
    *,
    through_bytes: int | None = None,
) -> Iterator[bytes]:
    """Read exact contiguous staged octets without constructing a giant byte string."""

    limit = record.bytes if through_bytes is None else through_bytes
    if type(limit) is not int or not 1 <= limit <= record.bytes:
        raise ProvenanceValidationError("journal prefix byte extent is invalid")
    statement = (
        select(CollectionUploadProvenanceJournalChunkRecord)
        .where(
            CollectionUploadProvenanceJournalChunkRecord.collection_id == record.collection_id,
            CollectionUploadProvenanceJournalChunkRecord.journal_id == record.journal_id,
        )
        .order_by(CollectionUploadProvenanceJournalChunkRecord.ordinal)
        .execution_options(yield_per=16)
    )
    offset = 0
    ordinal = 0
    for chunk in session.scalars(statement):
        if offset >= limit:
            break
        raw = bytes(chunk.content)
        if chunk.byte_offset != offset or chunk.ordinal != ordinal or not raw:
            raise ProvenanceValidationError("staged canonical journal chunks are discontinuous")
        selected = raw[: limit - offset]
        yield selected
        offset += len(selected)
        ordinal += 1
    if offset != limit:
        raise ProvenanceValidationError("staged canonical journal prefix is incomplete")


def _validate_next_upload_journal_entry(
    session: Session,
    record: CollectionUploadProvenanceJournalRecord,
) -> None:
    """Validate the exact staged canonical journal before it can be bound."""

    if record.state != "validating" or record.accepted_bytes != record.bytes:
        raise ProvenanceValidationError("canonical journal is not complete for validation")
    summary = validate_journal_chunks(
        _iter_upload_journal_chunks(session, record),
        catalog=admission_provenance_catalog(),
        require_profiles=False,
    )
    if (
        summary.journal_id != record.journal_id
        or summary.journal_sha256 != record.sha256
        or summary.journal_bytes != record.bytes
    ):
        raise ProvenanceValidationError("staged canonical journal differs from its authority")
    record.validation_byte_offset = record.bytes
    record.validation_sequence = len(summary.frames)
    record.terminal_entry_id = summary.tail.reference["entry_id"]
    record.terminal_sequence = int(summary.tail.reference["sequence"])
    record.terminal_json_sha256 = summary.tail.reference["json_sha256"]
    record.state = "sealed"
    record.failure = None


def _volume_summary(plan: PackVolumePlan | RawVolumePlan) -> dict[str, object]:
    return {
        "volume_id": plan.volume_id,
        "sequence": format_archive_sequence(plan.sequence),
        "kind": "pack" if isinstance(plan, PackVolumePlan) else "segment",
    }


def _part_payload(part: StoredArchivePart) -> dict[str, object]:
    return {
        "number": part.number,
        "plaintext_start": part.plaintext_start,
        "plaintext_bytes": part.plaintext_bytes,
        "plaintext_sha256": part.plaintext_sha256,
        "stored_bytes": part.stored_bytes,
        "stored_sha256": part.stored_sha256,
    }


def _unit_states(record: CollectionArchiveObjectUploadRecord) -> set[int]:
    if not record.checkpoint_json:
        return set()
    checkpoint = (
        PackUploadCheckpoint.from_json(record.checkpoint_json)
        if record.kind == "pack"
        else RawUploadCheckpoint.from_json(record.checkpoint_json)
    )
    return {current.number - 1 for current in checkpoint.archive_parts}


def _volume_work_payload(record: CollectionArchiveObjectUploadRecord) -> dict[str, object]:
    committed = _unit_states(record)
    if record.kind == "pack":
        pack_plan = parse_pack_volume_plan(record.plan_json)
        units = [
            {
                "unit": format_scalar("nonnegative", current.unit),
                "payload_bytes": format_scalar("nonnegative", current.payload_bytes),
                "plaintext_bytes": format_scalar("nonnegative", current.plaintext_bytes),
                "sources": [
                    {
                        "artifact_id": source.artifact_id,
                        "offset": "0",
                        "bytes": format_scalar("nonnegative", source.bytes),
                        "artifact_sha256": source.sha256,
                    }
                    for source in current.sources
                ],
                "state": "committed" if current.unit in committed else "pending",
            }
            for current in pack_unit_descriptors(pack_plan)
        ]
    else:
        raw_plan = parse_raw_volume_plan(record.plan_json)
        raw_part_bytes = record.unit_plaintext_bytes
        units = []
        for unit in range(record.total_units):
            byte_count = min(
                raw_part_bytes,
                raw_plan.plaintext_bytes - unit * raw_part_bytes,
            )
            units.append(
                {
                    "unit": format_scalar("nonnegative", unit),
                    "payload_bytes": format_scalar("nonnegative", byte_count),
                    "plaintext_bytes": format_scalar("nonnegative", byte_count),
                    "sources": [
                        {
                            "artifact_id": raw_plan.artifact_id,
                            "offset": format_scalar(
                                "nonnegative", raw_plan.artifact_offset + unit * raw_part_bytes
                            ),
                            "bytes": format_scalar("nonnegative", byte_count),
                            "artifact_sha256": raw_plan.artifact_sha256,
                        }
                    ],
                    "state": "committed" if unit in committed else "pending",
                }
            )
    return {
        "volume_id": record.object_id,
        "sequence": format_archive_sequence(record.sequence),
        "kind": record.kind,
        "state": record.state,
        "plan_sha256": record.plan_sha256,
        "plaintext_bytes": format_scalar("nonnegative", record.plaintext_bytes),
        "source_bytes": format_scalar("nonnegative", record.source_bytes),
        "units": units,
    }


def _unit_work_payload(
    record: CollectionArchiveObjectUploadRecord,
    unit: int,
) -> dict[str, object]:
    committed = unit < record.uploaded_units or record.state == "sealed"
    if record.kind == "pack":
        descriptors = pack_unit_descriptors(parse_pack_volume_plan(record.plan_json))
        if unit < 0 or unit >= len(descriptors):
            raise NotFound(f"collection upload unit not found: {unit}")
        current = descriptors[unit]
        return {
            "unit": format_scalar("nonnegative", current.unit),
            "payload_bytes": format_scalar("nonnegative", current.payload_bytes),
            "plaintext_bytes": format_scalar("nonnegative", current.plaintext_bytes),
            "sources": [
                {
                    "artifact_id": source.artifact_id,
                    "offset": "0",
                    "bytes": format_scalar("nonnegative", source.bytes),
                    "artifact_sha256": source.sha256,
                }
                for source in current.sources
            ],
            "state": "committed" if committed else "pending",
        }
    if record.kind == "segment":
        plan = parse_raw_volume_plan(record.plan_json)
        if unit < 0 or unit >= record.total_units:
            raise NotFound(f"collection upload unit not found: {unit}")
        byte_count = min(
            record.unit_plaintext_bytes,
            plan.plaintext_bytes - unit * record.unit_plaintext_bytes,
        )
        return {
            "unit": format_scalar("nonnegative", unit),
            "payload_bytes": format_scalar("nonnegative", byte_count),
            "plaintext_bytes": format_scalar("nonnegative", byte_count),
            "sources": [
                {
                    "artifact_id": plan.artifact_id,
                    "offset": format_scalar(
                        "nonnegative", plan.artifact_offset + unit * record.unit_plaintext_bytes
                    ),
                    "bytes": format_scalar("nonnegative", byte_count),
                    "artifact_sha256": plan.artifact_sha256,
                }
            ],
            "state": "committed" if committed else "pending",
        }
    raise RuntimeError(f"unsupported archive volume kind: {record.kind}")


def _unit_assignment_payload(record: CollectionArchiveObjectUploadRecord) -> dict[str, object]:
    if record.uploaded_units >= record.total_units:
        raise RuntimeError("unsealed archive volume has no actionable upload unit")
    return {
        "volume": {
            "volume_id": record.object_id,
            "sequence": format_archive_sequence(record.sequence),
            "kind": record.kind,
        },
        "plan_sha256": record.plan_sha256,
        "unit": _unit_work_payload(record, record.uploaded_units),
    }


def _sealed_volume_json(receipt: SealedPackVolume | SealedRawVolume) -> str:
    common: dict[str, object] = {
        "volume_id": receipt.volume_id,
        "sequence": receipt.sequence,
        "relative_path": receipt.relative_path,
        "plaintext_bytes": receipt.plaintext_bytes,
        "age_state": json.loads(receipt.age_state_json),
        "parts": [_part_payload(current) for current in receipt.parts],
        "revision": receipt.revision,
        "completed_at": receipt.completed_at,
        "retrieval_cache": retrieval_cache_receipt_payload(receipt.retrieval_cache),
    }
    if isinstance(receipt, SealedPackVolume):
        common.update(
            {
                "kind": "pack",
                "artifacts": receipt.artifacts,
                "source_bytes": receipt.source_bytes,
                "index_sha256": receipt.index_sha256,
                "plan_sha256": receipt.plan_sha256,
            }
        )
    else:
        common.update(
            {
                "kind": "segment",
                "artifact_id": receipt.artifact_id,
                "artifact_offset": receipt.artifact_offset,
                "artifact_bytes": receipt.artifact_bytes,
                "artifact_sha256": receipt.artifact_sha256,
            }
        )
    return json.dumps(common, sort_keys=True, separators=(",", ":"))


def _archive_volume_metadata_receipt_json(
    receipt: SealedArchiveVolumeMetadata,
) -> str:
    return json.dumps(
        {
            "sequence": receipt.sequence,
            "object_path": receipt.object_path,
            "relative_path": receipt.relative_path,
            "revision": receipt.revision,
            "plaintext_bytes": receipt.plaintext_bytes,
            "plaintext_sha256": receipt.plaintext_sha256,
            "stored_bytes": receipt.stored_bytes,
            "stored_sha256": receipt.stored_sha256,
            "completed_at": receipt.completed_at,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _parse_archive_volume_metadata_receipt(
    content: str,
) -> SealedArchiveVolumeMetadata:
    value = json.loads(content)
    return SealedArchiveVolumeMetadata(
        sequence=_stored_int(value["sequence"], "metadata sequence"),
        object_path=str(value["object_path"]),
        relative_path=str(value["relative_path"]),
        revision=str(value["revision"]) if value["revision"] is not None else None,
        plaintext_bytes=_stored_int(value["plaintext_bytes"], "metadata plaintext bytes"),
        plaintext_sha256=str(value["plaintext_sha256"]),
        stored_bytes=_stored_int(value["stored_bytes"], "metadata stored bytes"),
        stored_sha256=str(value["stored_sha256"]),
        completed_at=str(value["completed_at"]),
    )


def _sealed_provenance_object_json(receipt: Any) -> str:
    return json.dumps(
        {
            "object_id": receipt.object_id,
            "kind": receipt.kind,
            "relative_path": receipt.relative_path,
            "plaintext_bytes": receipt.plaintext_bytes,
            "plaintext_sha256": receipt.plaintext_sha256,
            "stored_bytes": receipt.stored_bytes,
            "stored_sha256": receipt.stored_sha256,
            "revision": receipt.revision,
            "completed_at": receipt.completed_at,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _parse_sealed_provenance_object(content: str) -> Any:
    from riverhog_core.domain.archive import SealedProvenanceObject

    value = json.loads(content)
    return SealedProvenanceObject(
        object_id=str(value["object_id"]),
        kind=str(value["kind"]),
        relative_path=str(value["relative_path"]),
        plaintext_bytes=_stored_int(value["plaintext_bytes"], "provenance plaintext bytes"),
        plaintext_sha256=str(value["plaintext_sha256"]),
        stored_bytes=_stored_int(value["stored_bytes"], "provenance stored bytes"),
        stored_sha256=str(value["stored_sha256"]),
        revision=str(value["revision"]) if value["revision"] is not None else None,
        completed_at=str(value["completed_at"]),
    )


def _provenance_custody_object(content: str) -> CollectionUploadProvenanceCustodyObjectDocument:
    receipt = _parse_sealed_provenance_object(content)
    return CollectionUploadProvenanceCustodyObjectDocument.model_validate(
        {
            "object_id": receipt.object_id,
            "relative_path": receipt.relative_path,
            "plaintext_bytes": format_scalar("nonnegative", receipt.plaintext_bytes),
            "plaintext_sha256": receipt.plaintext_sha256,
            "sealed_receipt_sha256": hashlib.sha256(content.encode("utf-8")).hexdigest(),
        }
    )


def _sealed_provenance_json(receipt: SealedArchiveProvenance) -> str:
    return json.dumps(
        {
            "identity": receipt.identity,
            "root": json.loads(_sealed_provenance_object_json(receipt.root)),
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _sealed_upload_provenance(
    upload: CollectionUploadRecord,
) -> SealedArchiveProvenance:
    if upload.provenance_archive_root_receipt_json is None:
        raise RuntimeError("canonical provenance root is not published")
    value = json.loads(upload.provenance_archive_root_receipt_json)
    root = _parse_sealed_provenance_object(
        json.dumps(value["root"], sort_keys=True, separators=(",", ":"))
    )
    receipt = SealedArchiveProvenance(identity=str(value["identity"]), root=root)
    if receipt.identity != upload.provenance_identity:
        raise RuntimeError("provenance root identity differs from upload state")
    return receipt


def _parse_parts(values: Sequence[Mapping[str, object]]) -> tuple[StoredArchivePart, ...]:
    return tuple(
        StoredArchivePart(
            number=_stored_int(value["number"], "part number"),
            plaintext_start=_stored_int(value["plaintext_start"], "part plaintext start"),
            plaintext_bytes=_stored_int(value["plaintext_bytes"], "part plaintext bytes"),
            plaintext_sha256=str(value["plaintext_sha256"]),
            stored_bytes=_stored_int(value["stored_bytes"], "part stored bytes"),
            stored_sha256=str(value["stored_sha256"]),
        )
        for value in values
    )


def _stored_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label} must be a non-negative integer")
    return value


def _parse_sealed_pack(content: str) -> SealedPackVolume:
    value = json.loads(content)
    return SealedPackVolume(
        volume_id=str(value["volume_id"]),
        sequence=int(value["sequence"]),
        relative_path=str(value["relative_path"]),
        artifacts=int(value["artifacts"]),
        source_bytes=int(value["source_bytes"]),
        plaintext_bytes=int(value["plaintext_bytes"]),
        age_state_json=json.dumps(value["age_state"], sort_keys=True, separators=(",", ":")),
        index_sha256=str(value["index_sha256"]),
        plan_sha256=str(value["plan_sha256"]),
        parts=_parse_parts(value["parts"]),
        revision=str(value["revision"]) if value["revision"] is not None else None,
        completed_at=str(value["completed_at"]),
        retrieval_cache=parse_retrieval_cache_receipt(value.get("retrieval_cache")),
    )


def _parse_sealed_raw(content: str) -> SealedRawVolume:
    value = json.loads(content)
    return SealedRawVolume(
        volume_id=str(value["volume_id"]),
        sequence=int(value["sequence"]),
        relative_path=str(value["relative_path"]),
        artifact_id=str(value["artifact_id"]),
        artifact_offset=int(value["artifact_offset"]),
        plaintext_bytes=int(value["plaintext_bytes"]),
        artifact_bytes=int(value["artifact_bytes"]),
        artifact_sha256=str(value["artifact_sha256"]),
        age_state_json=json.dumps(value["age_state"], sort_keys=True, separators=(",", ":")),
        parts=_parse_parts(value["parts"]),
        revision=str(value["revision"]) if value["revision"] is not None else None,
        completed_at=str(value["completed_at"]),
        retrieval_cache=parse_retrieval_cache_receipt(value.get("retrieval_cache")),
    )


def _custody_stats(session: Session, collection_id: int) -> tuple[int, int]:
    upload = session.get(CollectionUploadRecord, collection_id)
    if upload is None:
        return 0, 0
    return int(upload.safe_release_artifact_count), int(upload.safe_release_artifact_bytes)


def _custody_payload(
    *,
    files: int,
    byte_count: int,
    custodied_files: int,
    custodied_bytes: int,
) -> dict[str, object]:
    """Project exact completion separately from non-authoritative progress counters."""

    if (
        custodied_files < 0
        or custodied_bytes < 0
        or custodied_files > files
        or custodied_bytes > byte_count
        or (custodied_files == 0 and custodied_bytes != 0)
    ):
        raise RuntimeError("collection upload custody projection is inconsistent")
    if (custodied_files, custodied_bytes) == (files, byte_count):
        return {"state": "complete"}
    return {
        "state": "pending",
        "files": custodied_files,
        "bytes": custodied_bytes,
    }


def _validate_upload_list(*, page_size: int, sort: str, order: str) -> None:
    validate_page_size(page_size)
    if sort not in _UPLOAD_SORT_FIELDS:
        raise BadRequest("invalid collection upload sort")
    if order not in _SORT_ORDERS:
        raise BadRequest("collection upload order must be asc or desc")


def _upload_list_statement(
    *,
    q: str | None,
    state: str | None,
    sort: str,
    order: str,
    principal: Principal,
) -> tuple[Any, tuple[Any, ...]]:
    if state is not None and state not in _UPLOAD_STATES:
        raise BadRequest("invalid collection upload state")
    filters: list[Any] = [_upload_read_filter(principal)]
    if q:
        pattern = f"%{text_search_key(q)}%"
        matching_ids = select(CollectionUploadRecord.collection_id).where(
            CollectionUploadRecord.search_text.like(pattern)
        )
        filters.append(CollectionUploadRecord.collection_id.in_(matching_ids))
    if state:
        filters.append(CollectionUploadRecord.state == state)
    statement = select(
        CollectionUploadRecord,
        CollectionUploadRecord.artifact_count.label("files"),
        CollectionUploadRecord.artifact_bytes.label("bytes"),
    ).where(*filters)
    direction = asc if order == "asc" else desc
    sort_column = {
        "id": CollectionUploadRecord.collection_id,
        "created_at": CollectionUploadRecord.opened_at,
        "state": CollectionUploadRecord.state,
        "bytes": CollectionUploadRecord.artifact_bytes,
        "files": CollectionUploadRecord.artifact_count,
    }[sort]
    del direction
    return statement, (
        (CollectionUploadRecord.collection_id,)
        if sort == "id"
        else (sort_column, CollectionUploadRecord.collection_id)
    )


def _upload_list_position(
    upload: CollectionUploadRecord,
    *,
    sort: str,
) -> tuple[BrowseScalar, ...]:
    if sort == "id":
        value: BrowseScalar = upload.collection_id
    elif sort == "created_at":
        value = upload.opened_at or ""
    elif sort == "state":
        value = upload.state
    elif sort == "bytes":
        value = upload.artifact_bytes
    else:
        value = upload.artifact_count
    return (value,) if sort == "id" else (value, upload.collection_id)


def _upload_list_payload(
    session: Session,
    upload: CollectionUploadRecord,
    *,
    files: int,
    byte_count: int,
) -> dict[str, object]:
    custodied_files, custodied_bytes = _custody_stats(session, upload.collection_id)
    tag_count = _upload_tag_count(session, upload.collection_id)
    return {
        "collection_id": format_scalar("sequence63", upload.collection_id),
        "created_at": upload.opened_at,
        "ingest_source": upload.ingest_source,
        "description": upload.description,
        "description_revision": upload.description_revision,
        "description_identity": upload.description_identity,
        "description_publication": (
            "pending"
            if upload.description_identity is None
            else "not_required"
            if upload.description_revision == 0
            else "current"
        ),
        "tag_revision": upload.tag_revision,
        "tag_set_identity": upload.tag_set_identity,
        "tag_publication": (
            "current" if upload.tag_publication_receipt_json is not None else "pending"
        ),
        "tag_count": tag_count,
        "archive_store": upload.archive_store,
        "encryption_format": upload.encryption_format,
        "passphrase_id": upload.passphrase_id,
        "state": upload.state,
        "custody_mode": upload.custody_mode,
        "files": files,
        "bytes": byte_count,
        "custody": _custody_payload(
            files=files,
            byte_count=byte_count,
            custodied_files=custodied_files,
            custodied_bytes=custodied_bytes,
        ),
        "upload_state_expires_at": upload.lease_expires_at,
        "orphaned_at": upload.orphaned_at,
    }


def _normalize_custody_mode(value: str) -> CollectionUploadCustodyMode:
    normalized = str(value or "")
    if normalized not in {"producer-retained", "custody-transfer"}:
        raise BadRequest("collection upload custody mode is invalid")
    return cast(CollectionUploadCustodyMode, normalized)


def _upload_visible_to_deleter(
    session: Session,
    upload: CollectionUploadRecord,
    principal: Principal,
) -> bool:
    del session
    return principal.allows_collection(COLLECTIONS_DELETE, upload.collection_id)


def _upload_read_filter(principal: Principal) -> Any:
    owner = CollectionUploadRecord.initiated_by_principal_id == principal.id
    resources = permission_resources(principal, COLLECTIONS_DELETE)
    if ALL_RESOURCES in resources:
        return true()
    filters = [owner]
    allowed_collections = collection_ids(resources)
    if allowed_collections:
        filters.append(CollectionUploadRecord.collection_id.in_(allowed_collections))
    return or_(*filters)


def _orphan_discard_plan(
    session: Session,
    *,
    collection_id: int,
    expires_at: str,
) -> dict[str, object]:
    upload = session.get(CollectionUploadRecord, collection_id)
    if upload is None:
        raise NotFound(f"collection upload session not found: {collection_id}")
    files = upload.artifact_count
    byte_count = upload.artifact_bytes
    custodied_files, custodied_bytes = _custody_stats(session, collection_id)
    archive_objects = int(
        session.scalar(
            select(func.count(CollectionArchiveObjectUploadRecord.object_id)).where(
                CollectionArchiveObjectUploadRecord.collection_id == collection_id
            )
        )
        or 0
    )
    blockers = [] if upload.state == "orphaned" else [f"upload session is {upload.state}"]
    processing_prefix = "processing:"
    if upload.initiated_by_principal_id.startswith(processing_prefix):
        execution_id = upload.initiated_by_principal_id.removeprefix(processing_prefix)
        claim = session.scalar(
            select(CollectionProcessingClaimRecord).where(
                CollectionProcessingClaimRecord.execution_id == execution_id
            )
        )
        if (
            claim is not None
            and claim.state == "active"
            and parse_utc_timestamp(claim.expires_at) > utc_epoch_ns_now()
        ):
            blockers.append("owning processing claim remains active until " + claim.expires_at)
    return {
        "status": "blocked" if blockers else "ready",
        "collection_id": format_scalar("sequence63", collection_id),
        "warning": _CUSTODY_LOSS_WARNING,
        "expires_at": expires_at,
        "state": upload.state,
        "files": int(files),
        "bytes": int(byte_count),
        "custody": _custody_payload(
            files=int(files),
            byte_count=int(byte_count),
            custodied_files=custodied_files,
            custodied_bytes=custodied_bytes,
        ),
        "archive_objects": archive_objects,
        "blockers": blockers,
    }


def _custody_lease_expiry(config: RuntimeConfig, *, now: str | None = None) -> str:
    current = now if now is not None else utc_timestamp_now()
    return add_utc_timestamp(current, config.collection_upload_custody_lease)


def _touch_upload(
    upload: CollectionUploadRecord,
    *,
    config: RuntimeConfig,
    now: str | None = None,
) -> None:
    current = now or utc_timestamp_now()
    upload.last_activity_at = current
    if upload.custody_mode == "custody-transfer" and upload.state in {"open", "closing"}:
        upload.lease_expires_at = _custody_lease_expiry(config, now=current)


def _record_payload_custody_progress(
    session: Session,
    upload: CollectionUploadRecord,
    sealed_volume: CollectionArchiveObjectUploadRecord,
    *,
    now: str,
) -> None:
    """Track payload-only sealing so finalization can start without releasing sources."""

    newly_sealed_artifacts = 0
    newly_sealed_bytes = 0
    artifact_ids = tuple(
        session.scalars(
            select(CollectionUploadArtifactVolumeRecord.artifact_id).where(
                CollectionUploadArtifactVolumeRecord.collection_id == upload.collection_id,
                CollectionUploadArtifactVolumeRecord.object_id == sealed_volume.object_id,
            )
        )
    )
    for artifact_id in artifact_ids:
        artifact = session.get(CollectionUploadArtifactRecord, (upload.collection_id, artifact_id))
        if artifact is None:
            raise RuntimeError("payload volume names an unregistered artifact")
        if artifact.payload_sealed_at is not None:
            continue
        volumes = list(
            session.scalars(
                select(CollectionArchiveObjectUploadRecord)
                .join(
                    CollectionUploadArtifactVolumeRecord,
                    (
                        CollectionUploadArtifactVolumeRecord.collection_id
                        == CollectionArchiveObjectUploadRecord.collection_id
                    )
                    & (
                        CollectionUploadArtifactVolumeRecord.object_id
                        == CollectionArchiveObjectUploadRecord.object_id
                    ),
                )
                .where(
                    CollectionUploadArtifactVolumeRecord.collection_id == upload.collection_id,
                    CollectionUploadArtifactVolumeRecord.artifact_id == artifact_id,
                )
            )
        )
        if not volumes or any(
            volume.state != "sealed" or volume.sealed_receipt_json is None for volume in volumes
        ):
            continue
        artifact.payload_sealed_at = now
        newly_sealed_artifacts += 1
        newly_sealed_bytes += artifact.bytes
    upload.payload_sealed_artifact_count += newly_sealed_artifacts
    upload.payload_sealed_artifact_bytes += newly_sealed_bytes


def _upload_payload(
    session: Session,
    upload: CollectionUploadRecord,
    *,
    state: str | None = None,
    resumed: bool | None = None,
) -> dict[str, object]:
    files_total = upload.artifact_count
    bytes_total = upload.artifact_bytes
    custodied_files, custodied_bytes = _custody_stats(session, upload.collection_id)
    tag_count = _upload_tag_count(session, upload.collection_id)
    archive_progress = session.execute(
        select(
            func.coalesce(func.sum(CollectionArchiveObjectUploadRecord.uploaded_bytes), 0),
            func.coalesce(func.sum(CollectionArchiveObjectUploadRecord.uploaded_units), 0),
            func.coalesce(func.sum(CollectionArchiveObjectUploadRecord.total_units), 0),
        ).where(CollectionArchiveObjectUploadRecord.collection_id == upload.collection_id)
    ).one()
    payload: dict[str, object] = {
        "collection_id": format_scalar("sequence63", upload.collection_id),
        "created_at": upload.opened_at,
        "ingest_source": upload.ingest_source,
        "description": upload.description,
        "description_revision": upload.description_revision,
        "description_identity": upload.description_identity,
        "description_publication": (
            "pending"
            if upload.description_identity is None
            else "not_required"
            if upload.description_revision == 0
            else "current"
        ),
        "tag_revision": upload.tag_revision,
        "tag_set_identity": upload.tag_set_identity,
        "tag_publication": (
            "current" if upload.tag_publication_receipt_json is not None else "pending"
        ),
        "tag_count": tag_count,
        "provenance_identity": None,
        "delivery_context_id": upload.delivery_context_id,
        "construction_identity_sha256": upload.creation_identity_sha256,
        "completion_journal_id": upload.completion_journal_id,
        "archive_store": upload.archive_store,
        "use_cache": upload.use_cache,
        "copy_to": json.loads(upload.copy_to_json),
        "copy_intents": _copy_intent_payloads(
            session, upload.collection_id, canceled=state == "canceled"
        ),
        "encryption_format": upload.encryption_format,
        "passphrase_id": upload.passphrase_id,
        "state": state or upload.state,
        "custody_mode": upload.custody_mode,
        "registration_constraints": _registration_constraints_payload(
            _planner_checkpoint(upload).policy
        ),
        "files_total": int(files_total),
        "bytes_total": int(bytes_total),
        "upload_state_expires_at": None if state == "canceled" else upload.lease_expires_at,
        "custody": _custody_payload(
            files=int(files_total),
            byte_count=int(bytes_total),
            custodied_files=custodied_files,
            custodied_bytes=custodied_bytes,
        ),
        "orphaned_at": None if state == "canceled" else upload.orphaned_at,
        "latest_failure": upload.archive_failure,
        "archive_phase": upload.archive_phase,
        "archive_phase_updated_at": upload.archive_phase_updated_at,
        "archive_next_attempt_at": upload.archive_next_attempt_at,
        "archive_storage_prefix": upload.archive_storage_prefix,
        "archive_uploaded_bytes": int(archive_progress[0]),
        "archive_total_bytes": None,
        "archive_uploaded_units": int(archive_progress[1]),
        "archive_total_units": int(archive_progress[2]),
        "collection": None,
    }
    if resumed is not None:
        payload["resumed"] = resumed
    return payload


def _finalized_payload(
    session: Session,
    collection: CollectionRecord,
    *,
    store_name: str,
    resumed: bool | None = None,
) -> dict[str, object]:
    copy = session.scalar(
        select(CollectionArchiveCopyRecord)
        .where(CollectionArchiveCopyRecord.collection_id == collection.id)
        .order_by(
            case((CollectionArchiveCopyRecord.store == store_name, 0), else_=1),
            CollectionArchiveCopyRecord.store,
        )
        .limit(1)
    )
    archive_copy_count = int(
        session.scalar(
            select(func.count())
            .select_from(CollectionArchiveCopyRecord)
            .where(CollectionArchiveCopyRecord.collection_id == collection.id)
        )
        or 0
    )
    stored_bytes = int(
        session.scalar(
            select(func.coalesce(func.sum(CollectionArchiveObjectRecord.stored_bytes), 0)).where(
                CollectionArchiveObjectRecord.collection_id == collection.id,
                CollectionArchiveObjectRecord.store == (copy.store if copy else store_name),
            )
        )
        or 0
    )
    manifest_sha256 = session.scalar(
        select(CollectionArchiveObjectRecord.sha256).where(
            CollectionArchiveObjectRecord.collection_id == collection.id,
            CollectionArchiveObjectRecord.store == (copy.store if copy else store_name),
            CollectionArchiveObjectRecord.object_id == "manifest",
        )
    )
    if manifest_sha256 is None:
        raise RuntimeError("finalized collection has no immutable archive-root identity")
    summary = {
        "id": format_scalar("sequence63", collection.id),
        "created_at": collection.created_at,
        "description": collection.description,
        "description_revision": collection.description_revision,
        "description_identity": collection.description_identity,
        "description_publication": (
            "not_required" if collection.description_revision == 0 else "current"
        ),
        "tag_revision": collection.tag_revision,
        "tag_set_identity": collection.tag_set_identity,
        "tag_publication": "current",
        "artifact_set_identity": collection.artifact_set_identity,
        "archive_root_sha256": manifest_sha256,
        "encryption_format": collection.encryption_format,
        "passphrase_id": collection.passphrase_id,
        "files": int(collection.artifact_count),
        "bytes": int(collection.artifact_bytes),
        "remote_storage_bytes": stored_bytes,
        "archive_copy_count": archive_copy_count,
    }
    payload: dict[str, object] = {
        "collection_id": format_scalar("sequence63", collection.id),
        "created_at": collection.created_at,
        "ingest_source": collection.ingest_source,
        "description": collection.description,
        "description_revision": collection.description_revision,
        "description_identity": collection.description_identity,
        "description_publication": (
            "not_required" if collection.description_revision == 0 else "current"
        ),
        "tag_revision": collection.tag_revision,
        "tag_set_identity": collection.tag_set_identity,
        "tag_publication": "current",
        "tag_count": int(
            session.scalar(
                select(func.count(CollectionTagMembershipRecord.tag_sha256)).where(
                    CollectionTagMembershipRecord.collection_id == collection.id
                )
            )
            or 0
        ),
        "provenance_identity": collection.provenance_identity,
        "artifact_set_identity": collection.artifact_set_identity,
        "delivery_context_id": collection.delivery_context_id,
        "construction_identity_sha256": collection.creation_identity_sha256,
        "archive_root_sha256": manifest_sha256,
        "archive_store": collection.creation_archive_store,
        "use_cache": collection.creation_use_cache,
        "copy_to": json.loads(collection.creation_copy_to_json),
        "copy_intents": _copy_intent_payloads(session, collection.id),
        "encryption_format": collection.encryption_format,
        "passphrase_id": collection.passphrase_id,
        "state": "finalized",
        "custody_mode": collection.creation_custody_mode,
        "registration_constraints": None,
        "files_total": summary["files"],
        "bytes_total": summary["bytes"],
        "upload_state_expires_at": None,
        "custody": {"state": "complete"},
        "orphaned_at": None,
        "latest_failure": None,
        "archive_phase": "completed",
        "archive_phase_updated_at": copy.last_verified_at if copy else None,
        "archive_next_attempt_at": None,
        "archive_storage_prefix": copy.archive_storage_prefix if copy else None,
        "archive_uploaded_bytes": stored_bytes,
        "archive_total_bytes": stored_bytes,
        "archive_uploaded_units": None,
        "archive_total_units": None,
        "collection": summary,
    }
    if resumed is not None:
        payload["resumed"] = resumed
    return payload


def _copy_intent_payloads(
    session: Session, collection_id: int, *, canceled: bool = False
) -> list[dict[str, object]]:
    intents = session.scalars(
        select(CollectionUploadCopyIntentRecord)
        .where(CollectionUploadCopyIntentRecord.collection_id == collection_id)
        .order_by(CollectionUploadCopyIntentRecord.destination_store)
    ).all()
    result: list[dict[str, object]] = []
    for intent in intents:
        job = (
            session.get(ArchiveCopyJobRecord, (collection_id, intent.destination_store))
            if intent.state == "handed_off"
            else None
        )
        result.append(
            {
                "destination_store": intent.destination_store,
                "state": "canceled" if canceled else intent.state,
                "failure_code": intent.failure_code,
                "job_state": job.state if job is not None else None,
                "job_created": intent.job_created,
            }
        )
    return result
