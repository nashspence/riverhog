# riverhog_client

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client:c7633d0c7c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [python](index.md) |
| Family | [modules](index.md#f-b8f1c0c242de) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-4a5160e00f84"></a>
| Field | Shape |
|---|---|
| <a id="s-e6371e386358"></a>`distribution` | "riverhog-client" |
| <a id="s-c8f3f6e535e0"></a>`exports` | additional keys=`ApiClient`, `ApplicationPermission`, `ApplicationResource`, `BadRequest`, `COLLECTION_UPLOAD_REGISTRATION_BATCH_FILES`, `CatalogReplica`, `CatalogSyncApi`, `CatalogSyncCursorExpired`, `CatalogSyncHistoryExpired`, `CatalogSyncSourceChanged`, `CatalogSyncViewChanged`, `CollectionProducer`, `CollectionUploadIdempotencyKey`, `Conflict`, `DownloadAllowanceExceeded`, `Forbidden`, `HashMismatch`, `IncrementalCollectionProducer`, `InvalidPath`, `InvalidRange`, `InvalidState`, `NotFound`, `ProducedCollection`, `ProducerArtifactCustody`, `ProducerArtifactIdentity`, `ProducerFile`, `ProducerInput`, `ProducerStream`, `ProvenanceMode`, `RangeReader`, `RawSourceHash`, `RestorePolicy`, `RetrievalDownload`, `RetrievalPlanIdempotencyKey`, `RiverhogError`, `ServiceUnavailable`, `Unauthorized`, `configured_download_concurrency`, `configured_download_window`, `configured_upload_concurrency`, `configured_upload_window`, `create_or_resume_with_initial_collection_tags`, `download_retrieval_files`, `hash_raw_source_chunks`, `put_collection_upload_unit`, `upload_collection_units` |
| <a id="s-33f00431444c"></a>`module` | "riverhog_client" |

## Governing policies

- <a id="pa-b7a8de4ef576"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba506)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c7102) — `packages/riverhog-client/src/riverhog_client/__init__.py::<module>`

### Machine authority

- `/external_contract/python/5`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4cbff58a3b10914d3775b2d8604b4d91299ce41fb8695ff9734b34094c47da35 -->

```json
{
  "distribution": "riverhog-client",
  "exports": {
    "ApiClient": {
      "kind": "class",
      "members": {
        "acknowledge_retrieval_job": {
          "kind": "method",
          "signature": "(self, job_id: 'str') -> 'dict[str, Any]'"
        },
        "acquire_collection_upload_session_work": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', *, limit: 'int' = 16) -> 'CollectionUploadWorkBatchDocument'"
        },
        "add_app_key_access": {
          "kind": "method",
          "signature": "(self, app: 'ApplicationName', key_id: 'ApplicationKeyId', *, permission: 'ApplicationPermission', resource: 'ApplicationResource') -> 'dict[str, Any]'"
        },
        "add_collection_tag": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', *, tag: 'CollectionTag', operation_id: 'str', expected_revision: 'int', expected_tag_set_identity: 'str') -> 'dict[str, Any]'"
        },
        "add_collection_upload_session_tags": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', tags: 'Sequence[CollectionTag]') -> 'dict[str, Any]'"
        },
        "advance_retrieval_plan": {
          "kind": "method",
          "signature": "(self, plan_id: 'str') -> 'dict[str, Any]'"
        },
        "append_collection_upload_session_provenance_journal": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', journal_id: 'ProvenanceJournalId', *, offset: 'int', content: 'bytes') -> 'CollectionUploadProvenanceJournalStatusDocument'"
        },
        "cancel_archive_copy_job": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', *, destination_store: 'ArchiveStoreName') -> 'dict[str, Any]'"
        },
        "cancel_collection_provenance_verification": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId') -> 'dict[str, Any]'"
        },
        "cancel_collection_upload_session": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId') -> 'dict[str, Any]'"
        },
        "cancel_retrieval_job": {
          "kind": "method",
          "signature": "(self, job_id: 'str') -> 'dict[str, Any]'"
        },
        "collection_contains_tag": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', *, tag: 'CollectionTag', revision: 'int', tag_set_identity: 'str') -> 'dict[str, Any]'"
        },
        "collection_provenance_journal_metadata": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', journal_id: 'ProvenanceJournalId') -> 'tuple[int, str]'"
        },
        "complete_collection_upload_session": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId') -> 'dict[str, Any]'"
        },
        "create_app_key": {
          "kind": "method",
          "signature": "(self, app: 'ApplicationName', *, access: 'Sequence[Mapping[str, str]]', expires_in_seconds: 'int | None' = None) -> 'dict[str, Any]'"
        },
        "create_catalog_sync_checkpoint": {
          "kind": "method",
          "signature": "(self) -> 'CatalogSyncCheckpoint'"
        },
        "create_collection_upload_session_provenance_journal": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', journal_id: 'ProvenanceJournalId', *, byte_count: 'int', sha256: 'str') -> 'CollectionUploadProvenanceJournalStatusDocument'"
        },
        "create_or_resume_archive_copy": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', *, destination_store: 'ArchiveStoreName', source_store: 'ArchiveStoreName | None' = None, event_context: 'Mapping[str, Any] | None' = None) -> 'dict[str, Any]'"
        },
        "create_or_resume_collection_upload_session": {
          "kind": "method",
          "signature": "(self, idempotency_key: 'CollectionUploadIdempotencyKey', *, ingest_source: 'str | None' = None, description: 'CollectionDescription | None' = None, tags: 'Sequence[CollectionTag]' = (), initial_tag_set_identity: 'str', archive_store: 'ArchiveStoreName | None' = None, event_context: 'Mapping[str, Any] | None' = None, provenance_mode: 'ProvenanceMode' = 'captured', provenance_omission_reason: 'str | None' = None, custody_mode: 'CollectionUploadCustodyMode' = 'producer-retained') -> 'dict[str, Any]'"
        },
        "create_retrieval_job": {
          "kind": "method",
          "signature": "(self, plan_id: 'str', *, plan_etag: 'str', event_context: 'Mapping[str, Any] | None' = None) -> 'dict[str, Any]'"
        },
        "delete_collection": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', *, challenge: 'str', retirement_claim_id: 'ProcessingClaimId | None' = None, event_context: 'Mapping[str, Any] | None' = None) -> 'dict[str, Any]'"
        },
        "discard_collection_upload": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', *, challenge: 'str') -> 'dict[str, Any]'"
        },
        "download_collection_provenance_journal": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', journal_id: 'ProvenanceJournalId', *, output: 'Path') -> 'tuple[int, str]'"
        },
        "download_retrieval_file": {
          "kind": "method",
          "signature": "(self, job_id: 'str', *, collection_id: 'CollectionId', path: 'str', output: 'Path', expected_bytes: 'int', expected_sha256: 'str', progress: 'DownloadProgress | None' = None) -> 'int'"
        },
        "get_archive_copy_job": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', *, destination_store: 'ArchiveStoreName') -> 'dict[str, Any]'"
        },
        "get_archive_store": {
          "kind": "method",
          "signature": "(self, store: 'ArchiveStoreName') -> 'dict[str, Any]'"
        },
        "get_collection": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId') -> 'dict[str, Any]'"
        },
        "get_collection_file_provenance": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', path: 'str') -> 'dict[str, Any]'"
        },
        "get_collection_provenance_verification": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId') -> 'dict[str, Any]'"
        },
        "get_collection_upload_session": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId') -> 'dict[str, Any]'"
        },
        "get_collection_upload_session_provenance_journal": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', journal_id: 'ProvenanceJournalId') -> 'CollectionUploadProvenanceJournalStatusDocument'"
        },
        "get_collection_upload_session_unit": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', volume_id: 'CollectionUploadVolumeId', unit: 'CollectionUploadUnitNumber') -> 'CollectionUploadUnitWorkDocument'"
        },
        "get_download_quota": {
          "kind": "method",
          "signature": "(self) -> 'dict[str, Any]'"
        },
        "get_portable_collection_inventory": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', *, cursor: 'str | None' = None, limit: 'int' = 100, inventory_identity: 'str | None' = None) -> 'PortableCollectionInventoryPage'"
        },
        "get_retrieval_cache_object": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', source_store: 'ArchiveStoreName', object_id: 'str') -> 'dict[str, Any]'"
        },
        "get_retrieval_job": {
          "kind": "method",
          "signature": "(self, job_id: 'str') -> 'dict[str, Any]'"
        },
        "get_retrieval_plan": {
          "kind": "method",
          "signature": "(self, plan_id: 'str') -> 'dict[str, Any]'"
        },
        "heartbeat_collection_upload_session": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId') -> 'dict[str, Any]'"
        },
        "list_app_key_access": {
          "kind": "method",
          "signature": "(self, *, page_size: 'int' = 25, page_token: 'str | None' = None, q: 'str | None' = None, sort: 'ApplicationAccessSort' = 'permission', order: 'SortOrder' = 'asc', app: 'ApplicationName | None' = None, key_id: 'ApplicationKeyId | None' = None, permission: 'ApplicationPermission | None' = None, resource: 'ApplicationResource | None' = None, active: 'bool | None' = None) -> 'dict[str, Any]'"
        },
        "list_app_keys": {
          "kind": "method",
          "signature": "(self, app: 'ApplicationName', *, page_size: 'int' = 25, page_token: 'str | None' = None, q: 'str | None' = None, sort: 'ApplicationKeySort' = 'created_at', order: 'SortOrder' = 'desc', active: 'bool | None' = None) -> 'dict[str, Any]'"
        },
        "list_apps": {
          "kind": "method",
          "signature": "(self, *, page_size: 'int' = 25, page_token: 'str | None' = None, q: 'str | None' = None, sort: 'ApplicationSort' = 'name', order: 'SortOrder' = 'asc', active: 'bool | None' = None) -> 'dict[str, Any]'"
        },
        "list_archive_copy_jobs": {
          "kind": "method",
          "signature": "(self, *, page_size: 'int' = 25, page_token: 'str | None' = None, q: 'str | None' = None, state: 'ArchiveCopyState | None' = None, sort: 'ArchiveCopySort' = 'requested_at', order: 'SortOrder' = 'desc') -> 'dict[str, Any]'"
        },
        "list_archive_stores": {
          "kind": "method",
          "signature": "(self, *, page_size: 'int' = 25, page_token: 'str | None' = None, q: 'str | None' = None, sort: 'ArchiveStoreSort' = 'store', order: 'SortOrder' = 'asc') -> 'dict[str, Any]'"
        },
        "list_catalog_sync_changes": {
          "kind": "method",
          "signature": "(self, cursor: 'str', *, limit: 'int' = 100) -> 'CatalogSyncChangePage'"
        },
        "list_catalog_sync_collections": {
          "kind": "method",
          "signature": "(self, cursor: 'str', *, limit: 'int' = 100) -> 'CatalogSyncCollectionPage'"
        },
        "list_collection_archive_copies": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', *, page_size: 'int' = 25, page_token: 'str | None' = None) -> 'dict[str, Any]'"
        },
        "list_collection_provenance": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', *, page_size: 'int' = 25, page_token: 'str | None' = None, q: 'str | None' = None, status: 'ProvenanceStatus | None' = None, sort: 'ProvenanceSort' = 'path', order: 'SortOrder' = 'asc') -> 'dict[str, Any]'"
        },
        "list_collection_provenance_journal_agents": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', journal_id: 'ProvenanceJournalId', *, page_size: 'int' = 25, page_token: 'str | None' = None) -> 'dict[str, Any]'"
        },
        "list_collection_tags": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', *, revision: 'int', tag_set_identity: 'str', page_size: 'int' = 25, page_token: 'str | None' = None) -> 'dict[str, Any]'"
        },
        "list_collection_upload_session_files": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', *, page_size: 'int' = 25, page_token: 'str | None' = None) -> 'dict[str, Any]'"
        },
        "list_collection_upload_sessions": {
          "kind": "method",
          "signature": "(self, *, page_size: 'int' = 25, page_token: 'str | None' = None, q: 'str | None' = None, state: 'CollectionUploadState | None' = None, sort: 'CollectionUploadSort' = 'created_at', order: 'SortOrder' = 'desc') -> 'dict[str, Any]'"
        },
        "list_collections": {
          "kind": "method",
          "signature": "(self, *, page_size: 'int' = 25, page_token: 'str | None' = None, q: 'str | None' = None, tags: 'Sequence[CollectionTag]' = (), encryption_format: 'str | None' = None, passphrase_id: 'str | None' = None, sort: 'CollectionSort' = 'id', order: 'SortOrder' = 'asc') -> 'dict[str, Any]'"
        },
        "list_download_quotas": {
          "kind": "method",
          "signature": "(self, *, page_size: 'int' = 25, page_token: 'str | None' = None, q: 'str | None' = None, sort: 'DownloadQuotaSort' = 'app', order: 'SortOrder' = 'asc', app: 'ApplicationName | None' = None, active: 'bool | None' = None) -> 'dict[str, Any]'"
        },
        "list_lifecycle_events": {
          "kind": "method",
          "signature": "(self, *, after: 'LifecycleEventCursor | None' = None, limit: 'int' = 100) -> 'RiverhogEventPage'"
        },
        "list_retrieval_cache_objects": {
          "kind": "method",
          "signature": "(self, *, page_size: 'int' = 25, page_token: 'str | None' = None, q: 'str | None' = None, collection_id: 'CollectionId | None' = None, source_store: 'ArchiveStoreName | None' = None, cache_store: 'RetrievalCacheStoreName | None' = None, state: 'RetrievalCacheState | None' = None, protection: 'RetrievalCacheProtection | None' = None, expires_before: 'str | None' = None, expires_after: 'str | None' = None, sort: 'RetrievalCacheSort' = 'cached_at', order: 'SortOrder' = 'desc') -> 'dict[str, Any]'"
        },
        "list_retrieval_plan_files": {
          "kind": "method",
          "signature": "(self, plan_id: 'str', *, plan_etag: 'str', start_ordinal: 'int' = 0, page_size: 'int' = 100) -> 'dict[str, Any]'"
        },
        "list_tags": {
          "kind": "method",
          "signature": "(self, *, page_size: 'int' = 25, page_token: 'str | None' = None, q: 'str | None' = None) -> 'dict[str, Any]'"
        },
        "plan_archive_copy_retirement": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', *, store: 'ArchiveStoreName') -> 'dict[str, Any]'"
        },
        "plan_collection_deletion": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', *, retirement_claim_id: 'ProcessingClaimId | None' = None) -> 'dict[str, Any]'"
        },
        "plan_collection_upload_discard": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId') -> 'dict[str, Any]'"
        },
        "plan_retrieval": {
          "kind": "method",
          "signature": "(self, files: 'Sequence[tuple[int, str]]', *, idempotency_key: 'RetrievalPlanIdempotencyKey | None' = None, lease_seconds: 'int | None' = None, restore_policy: 'RestorePolicy' = 'allow') -> 'dict[str, Any]'"
        },
        "put_collection_upload_session_unit": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', volume_id: 'CollectionUploadVolumeId', unit: 'CollectionUploadUnitNumber', *, plan_sha256: 'str', content: 'bytes') -> 'CollectionUploadUnitWorkDocument'"
        },
        "register_collection_upload_session_files": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', files: 'Sequence[CollectionUploadFileIn | Mapping[str, Any]]', *, registration_constraints: 'CollectionUploadRegistrationConstraintsDocument | Mapping[str, Any]') -> 'dict[str, Any]'"
        },
        "register_collection_upload_session_raw_part_digests": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', batch: 'CollectionUploadRawDigestBatchDocument | Mapping[str, Any]') -> 'CollectionUploadRawDigestProgressDocument'"
        },
        "remove_app_key_access": {
          "kind": "method",
          "signature": "(self, app: 'ApplicationName', key_id: 'ApplicationKeyId', *, permission: 'ApplicationPermission', resource: 'ApplicationResource') -> 'dict[str, Any]'"
        },
        "remove_collection_tag": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', *, tag: 'CollectionTag', operation_id: 'str', expected_revision: 'int', expected_tag_set_identity: 'str') -> 'dict[str, Any]'"
        },
        "renew_retrieval_job": {
          "kind": "method",
          "signature": "(self, job_id: 'str', *, lease_seconds: 'int') -> 'dict[str, Any]'"
        },
        "replace_app_key_access": {
          "kind": "method",
          "signature": "(self, app: 'ApplicationName', key_id: 'ApplicationKeyId', *, access: 'Sequence[Mapping[str, str]]') -> 'dict[str, Any]'"
        },
        "replace_collection_description": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', description: 'CollectionDescription | None', *, expected_identity: 'str') -> 'dict[str, Any]'"
        },
        "request_collection_provenance_verification": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId') -> 'dict[str, Any]'"
        },
        "retire_archive_copy": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', *, store: 'ArchiveStoreName', challenge: 'str') -> 'dict[str, Any]'"
        },
        "retrieval_cache_status": {
          "kind": "method",
          "signature": "(self) -> 'dict[str, Any]'"
        },
        "revoke_app_key": {
          "kind": "method",
          "signature": "(self, app: 'ApplicationName', key_id: 'ApplicationKeyId') -> 'dict[str, Any]'"
        },
        "rotate_app_key": {
          "kind": "method",
          "signature": "(self, app: 'ApplicationName', key_id: 'ApplicationKeyId') -> 'dict[str, Any]'"
        },
        "seal_collection_upload_session_provenance_journal": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', journal_id: 'ProvenanceJournalId') -> 'CollectionUploadProvenanceJournalStatusDocument'"
        },
        "search": {
          "kind": "method",
          "signature": "(self, query: 'str | None' = None, *, page_size: 'int' = 25, page_token: 'str | None' = None, sort: 'SearchSort' = 'file_ref', order: 'SortOrder' = 'asc', collection: 'CollectionId | None' = None) -> 'dict[str, Any]'"
        },
        "set_app_key_download_quota": {
          "kind": "method",
          "signature": "(self, app: 'ApplicationName', key_id: 'ApplicationKeyId', *, monthly_bytes: 'MonthlyDownloadQuotaBytes | None') -> 'dict[str, Any]'"
        },
        "spawn": {
          "kind": "method",
          "signature": "(self) -> 'ApiClient'"
        },
        "stream_collection_provenance_journal": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', journal_id: 'ProvenanceJournalId', *, start: 'int' = 0, end: 'int | None' = None, expected_bytes: 'int | None' = None, expected_sha256: 'str | None' = None, chunk_size: 'int' = 8388608) -> 'Iterator[Iterator[bytes]]'"
        },
        "stream_retrieval_file": {
          "kind": "method",
          "signature": "(self, job_id: 'str', *, collection_id: 'CollectionId', path: 'str', expected_bytes: 'int', expected_sha256: 'str', start: 'int' = 0, end: 'int | None' = None, chunk_size: 'int' = 8388608) -> 'Iterator[Iterator[bytes]]'"
        },
        "trace_collection_file_provenance": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', path: 'str', *, page_size: 'int' = 25, page_token: 'str | None' = None) -> 'dict[str, Any]'"
        },
        "upload_collection_upload_session_provenance_journal": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', journal_id: 'ProvenanceJournalId', *, content: 'Iterable[bytes]', byte_count: 'int', sha256: 'str') -> 'CollectionUploadProvenanceJournalStatusDocument'"
        }
      },
      "signature": "(base_url: 'str | None' = None, token: 'str | None' = None, *, allow_insecure_http: 'bool | None' = None) -> 'None'"
    },
    "ApplicationPermission": {
      "kind": "type-alias",
      "value": "typing.Literal['*', 'catalog:read', 'retrieval:manage', 'collections:create', 'collection-descriptions:manage', 'collection-transforms:control', 'collection-transforms:execute', 'collection-tags:manage', 'collections:delete', 'archives:read', 'archives:manage', 'keys:manage', 'quotas:manage', 'events:read', 'events:read_all', 'provenance:read', 'provenance:export']"
    },
    "ApplicationResource": {
      "kind": "type-alias",
      "value": "typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^(?:\\\\*|tag:.+|collection:[1-9][0-9]*)$', ascii_only=None), AfterValidator(func=<function validate_application_resource>)]"
    },
    "BadRequest": {
      "kind": "class",
      "signature": "(message: 'str', *, code: 'str | None' = None, observed_status: 'int | None' = None, details: 'dict[str, Any] | None' = None) -> 'None'"
    },
    "COLLECTION_UPLOAD_REGISTRATION_BATCH_FILES": {
      "kind": "constant",
      "value": 16
    },
    "CatalogReplica": {
      "kind": "class",
      "members": {
        "get": {
          "kind": "method",
          "signature": "(self, collection_id: 'int') -> 'CatalogSyncDescriptor | None'"
        },
        "page": {
          "kind": "method",
          "signature": "(self, *, after: 'int' = 0, limit: 'int' = 100, tags: 'Sequence[str]' = ()) -> 'list[CatalogSyncDescriptor]'"
        },
        "reclaim": {
          "kind": "method",
          "signature": "(self, *, limit: 'int' = 100) -> 'int'"
        },
        "start": {
          "kind": "method",
          "signature": "(self, api: 'CatalogSyncApi') -> 'dict[str, object]'"
        },
        "status": {
          "kind": "method",
          "signature": "(self) -> 'dict[str, object]'"
        },
        "step": {
          "kind": "method",
          "signature": "(self, api: 'CatalogSyncApi', *, limit: 'int' = 100) -> 'dict[str, object]'"
        },
        "tag_page": {
          "kind": "method",
          "signature": "(self, collection_id: 'int', *, after: 'str | None' = None, limit: 'int' = 100) -> 'list[str]'"
        }
      },
      "signature": "(database: 'str | Path') -> 'None'"
    },
    "CatalogSyncApi": {
      "kind": "class",
      "members": {
        "create_catalog_sync_checkpoint": {
          "kind": "method",
          "signature": "(self) -> 'CatalogSyncCheckpoint'"
        },
        "list_catalog_sync_changes": {
          "kind": "method",
          "signature": "(self, cursor: 'str', *, limit: 'int' = 100) -> 'CatalogSyncChangePage'"
        },
        "list_catalog_sync_collections": {
          "kind": "method",
          "signature": "(self, cursor: 'str', *, limit: 'int' = 100) -> 'CatalogSyncCollectionPage'"
        },
        "list_collection_tags": {
          "kind": "method",
          "signature": "(self, collection_id: 'int', *, revision: 'int', tag_set_identity: 'str', page_size: 'int' = 25, page_token: 'str | None' = None) -> 'dict[str, Any]'"
        }
      },
      "signature": "(*args, **kwargs)"
    },
    "CatalogSyncCursorExpired": {
      "kind": "class",
      "signature": "(message: 'str', *, code: 'str | None' = None, observed_status: 'int | None' = None, details: 'dict[str, Any] | None' = None) -> 'None'"
    },
    "CatalogSyncHistoryExpired": {
      "kind": "class",
      "signature": "(message: 'str', *, code: 'str | None' = None, observed_status: 'int | None' = None, details: 'dict[str, Any] | None' = None) -> 'None'"
    },
    "CatalogSyncSourceChanged": {
      "kind": "class",
      "signature": "(message: 'str', *, code: 'str | None' = None, observed_status: 'int | None' = None, details: 'dict[str, Any] | None' = None) -> 'None'"
    },
    "CatalogSyncViewChanged": {
      "kind": "class",
      "signature": "(message: 'str', *, code: 'str | None' = None, observed_status: 'int | None' = None, details: 'dict[str, Any] | None' = None) -> 'None'"
    },
    "CollectionProducer": {
      "kind": "class",
      "members": {
        "publish": {
          "kind": "method",
          "signature": "(self, files: 'Iterable[ProducerFile]', *, source_event_id: 'str', source_context: 'Mapping[str, object] | None' = None, provenance_journals: 'Iterable[tuple[str, bytes]] | None' = None, idempotency_key: 'str | None' = None, event_context: 'Mapping[str, object] | None' = None, poll_seconds: 'float' = 2.0, timeout_seconds: 'float' = 86400, progress: 'ReadProgress | None' = None) -> 'ProducedCollection'"
        },
        "publish_inputs": {
          "kind": "method",
          "signature": "(self, files: 'Iterable[ProducerInput]', *, source_event_id: 'str', source_context: 'Mapping[str, object] | None' = None, provenance_journals: 'Iterable[tuple[str, bytes]] | None' = None, idempotency_key: 'str | None' = None, event_context: 'Mapping[str, object] | None' = None, poll_seconds: 'float' = 2.0, timeout_seconds: 'float' = 86400, progress: 'ReadProgress | None' = None) -> 'ProducedCollection'"
        }
      },
      "signature": "(api: 'ApiClient', *, producer_app: 'str', adapter_id: 'str', adapter_version: 'str', ingest_source: 'str', archive_store: 'ArchiveStoreName | None' = None, description: 'CollectionDescription | None' = None, tags: 'Sequence[CollectionTag]' = (), provenance_mode: \"Literal['captured', 'omitted']\" = 'omitted', provenance_omission_reason: 'str' = 'Producer did not receive host provenance; immutable producer evidence records the source boundary.', server_generated_provenance: 'bool' = False) -> 'None'"
    },
    "CollectionUploadIdempotencyKey": {
      "kind": "type-alias",
      "value": "typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=None, pattern='^\\\\S(?:[\\\\s\\\\S]*\\\\S)?$', ascii_only=None), FieldInfo(annotation=NoneType, required=True, metadata=[MaxLen(max_length=200)])]"
    },
    "Conflict": {
      "kind": "class",
      "signature": "(message: 'str', *, code: 'str | None' = None, observed_status: 'int | None' = None, details: 'dict[str, Any] | None' = None) -> 'None'"
    },
    "DownloadAllowanceExceeded": {
      "kind": "class",
      "signature": "(message: 'str', *, code: 'str | None' = None, observed_status: 'int | None' = None, details: 'dict[str, Any] | None' = None) -> 'None'"
    },
    "Forbidden": {
      "kind": "class",
      "signature": "(message: 'str', *, code: 'str | None' = None, observed_status: 'int | None' = None, details: 'dict[str, Any] | None' = None) -> 'None'"
    },
    "HashMismatch": {
      "kind": "class",
      "signature": "(message: 'str', *, code: 'str | None' = None, observed_status: 'int | None' = None, details: 'dict[str, Any] | None' = None) -> 'None'"
    },
    "IncrementalCollectionProducer": {
      "kind": "class",
      "members": {
        "append_derivation_evidence": {
          "kind": "method",
          "signature": "(self, path: 'str', content: 'bytes') -> 'ProducerArtifactCustody | None'"
        },
        "append_inputs": {
          "kind": "method",
          "signature": "(self, inputs: 'Sequence[ProducerInput]', *, provenance_journals: 'Mapping[str, bytes] | None' = None, expected_identities: 'Mapping[str, ProducerArtifactIdentity] | None' = None) -> 'tuple[ProducerArtifactCustody, ...]'"
        },
        "finish": {
          "kind": "method",
          "signature": "(self, *, terminal_evidence: 'Mapping[str, bytes]', provenance_journals: 'Mapping[str, bytes] | None' = None, poll_seconds: 'float' = 2.0, timeout_seconds: 'float' = 86400) -> 'ProducedCollection'"
        },
        "heartbeat": {
          "kind": "method",
          "signature": "(self) -> 'None'"
        },
        "stage_provenance_journals": {
          "kind": "method",
          "signature": "(self, journals: 'Iterable[tuple[str, bytes]]') -> 'None'"
        },
        "stop": {
          "kind": "method",
          "signature": "(self) -> 'None'"
        }
      },
      "signature": "(api: 'ApiClient', *, producer_app: 'str', adapter_id: 'str', adapter_version: 'str', ingest_source: 'str', source_event_id: 'str', source_context: 'Mapping[str, object] | None' = None, idempotency_key: 'str | None' = None, archive_store: 'ArchiveStoreName | None' = None, description: 'CollectionDescription | None' = None, tags: 'Sequence[CollectionTag]' = (), event_context: 'Mapping[str, object] | None' = None, provenance_mode: \"Literal['captured', 'omitted']\" = 'omitted', server_generated_provenance: 'bool' = False, provenance_omission_reason: 'str' = 'Producer did not receive host provenance; immutable producer evidence records the source boundary.', progress: 'ReadProgress | None' = None) -> 'None'"
    },
    "InvalidPath": {
      "kind": "class",
      "signature": "(message: 'str', *, code: 'str | None' = None, observed_status: 'int | None' = None, details: 'dict[str, Any] | None' = None) -> 'None'"
    },
    "InvalidRange": {
      "kind": "class",
      "signature": "(message: 'str', *, code: 'str | None' = None, observed_status: 'int | None' = None, details: 'dict[str, Any] | None' = None) -> 'None'"
    },
    "InvalidState": {
      "kind": "class",
      "signature": "(message: 'str', *, code: 'str | None' = None, observed_status: 'int | None' = None, details: 'dict[str, Any] | None' = None) -> 'None'"
    },
    "NotFound": {
      "kind": "class",
      "signature": "(message: 'str', *, code: 'str | None' = None, observed_status: 'int | None' = None, details: 'dict[str, Any] | None' = None) -> 'None'"
    },
    "ProducedCollection": {
      "fields": [
        {
          "default": "required",
          "name": "collection_id",
          "type": "'CollectionId'"
        },
        {
          "default": "required",
          "name": "archive_root_sha256",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "content_identity",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "receipt",
          "type": "'dict[str, Any]'"
        }
      ],
      "kind": "class",
      "signature": "(collection_id: 'CollectionId', archive_root_sha256: 'str', content_identity: 'str', receipt: 'dict[str, Any]') -> None"
    },
    "ProducerArtifactCustody": {
      "fields": [
        {
          "default": "required",
          "name": "artifact",
          "type": "'ProducerArtifactIdentity'"
        },
        {
          "default": "required",
          "name": "receipt",
          "type": "'CollectionUploadArtifactCustodyReceiptDocument'"
        }
      ],
      "kind": "class",
      "signature": "(artifact: 'ProducerArtifactIdentity', receipt: 'CollectionUploadArtifactCustodyReceiptDocument') -> None"
    },
    "ProducerArtifactIdentity": {
      "fields": [
        {
          "default": "required",
          "name": "path",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "bytes",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "sha256",
          "type": "'str'"
        }
      ],
      "kind": "class",
      "signature": "(path: 'str', bytes: 'int', sha256: 'str') -> None"
    },
    "ProducerFile": {
      "fields": [
        {
          "default": "required",
          "name": "source",
          "type": "'Path'"
        },
        {
          "default": "required",
          "name": "path",
          "type": "'str'"
        },
        {
          "default": "None",
          "name": "provenance",
          "type": "'Mapping[str, object] | None'"
        }
      ],
      "kind": "class",
      "signature": "(source: 'Path', path: 'str', provenance: 'Mapping[str, object] | None' = None) -> None"
    },
    "ProducerInput": {
      "kind": "object",
      "type": "types.UnionType"
    },
    "ProducerStream": {
      "fields": [
        {
          "default": "required",
          "name": "path",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "bytes",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "sha256",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "read_range",
          "type": "'RangeReader'"
        },
        {
          "default": "None",
          "name": "provenance",
          "type": "'Mapping[str, object] | None'"
        }
      ],
      "kind": "class",
      "signature": "(path: 'str', bytes: 'int', sha256: 'str', read_range: 'RangeReader', provenance: 'Mapping[str, object] | None' = None) -> None"
    },
    "ProvenanceMode": {
      "kind": "type-alias",
      "value": "typing.Literal['captured', 'omitted']"
    },
    "RangeReader": {
      "kind": "object",
      "type": "collections.abc._CallableGenericAlias"
    },
    "RawSourceHash": {
      "fields": [
        {
          "default": "required",
          "name": "summary",
          "type": "'RawSourceDigestSummary'"
        },
        {
          "default": "required",
          "name": "_parts",
          "type": "'BinaryIO'"
        }
      ],
      "kind": "class",
      "members": {
        "close": {
          "kind": "method",
          "signature": "(self) -> 'None'"
        },
        "iter_batches": {
          "kind": "method",
          "signature": "(self, *, limit: 'int' = 1024) -> 'Iterator[tuple[int, tuple[str, ...]]]'"
        }
      },
      "signature": "(summary: 'RawSourceDigestSummary', _parts: 'BinaryIO') -> None"
    },
    "RestorePolicy": {
      "kind": "type-alias",
      "value": "typing.Literal['allow', 'never']"
    },
    "RetrievalDownload": {
      "fields": [
        {
          "default": "required",
          "name": "collection_id",
          "type": "'CollectionId'"
        },
        {
          "default": "required",
          "name": "path",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "output",
          "type": "'Path'"
        },
        {
          "default": "required",
          "name": "expected_bytes",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "expected_sha256",
          "type": "'str'"
        }
      ],
      "kind": "class",
      "signature": "(collection_id: 'CollectionId', path: 'str', output: 'Path', expected_bytes: 'int', expected_sha256: 'str') -> None"
    },
    "RetrievalPlanIdempotencyKey": {
      "kind": "type-alias",
      "value": "typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=None, pattern='^\\\\S(?:[\\\\s\\\\S]*\\\\S)?$', ascii_only=None), FieldInfo(annotation=NoneType, required=True, metadata=[MaxLen(max_length=200)])]"
    },
    "RiverhogError": {
      "kind": "class",
      "signature": "(message: 'str', *, code: 'str | None' = None, observed_status: 'int | None' = None, details: 'dict[str, Any] | None' = None) -> 'None'"
    },
    "ServiceUnavailable": {
      "kind": "class",
      "signature": "(message: 'str', *, code: 'str | None' = None, observed_status: 'int | None' = None, details: 'dict[str, Any] | None' = None) -> 'None'"
    },
    "Unauthorized": {
      "kind": "class",
      "signature": "(message: 'str', *, code: 'str | None' = None, observed_status: 'int | None' = None, details: 'dict[str, Any] | None' = None) -> 'None'"
    },
    "configured_download_concurrency": {
      "kind": "function",
      "signature": "(values: 'Mapping[str, str] | None' = None) -> 'int'"
    },
    "configured_download_window": {
      "kind": "function",
      "signature": "(values: 'Mapping[str, str] | None' = None, *, concurrency: 'int | None' = None) -> 'int'"
    },
    "configured_upload_concurrency": {
      "kind": "function",
      "signature": "(values: 'Mapping[str, str] | None' = None) -> 'int'"
    },
    "configured_upload_window": {
      "kind": "function",
      "signature": "(values: 'Mapping[str, str] | None' = None, *, concurrency: 'int | None' = None) -> 'int'"
    },
    "create_or_resume_with_initial_collection_tags": {
      "kind": "function",
      "signature": "(tags: 'Iterable[str]', *, create_or_resume: 'Callable[[Sequence[CollectionTag], str], Mapping[str, Any]]', add_tags: 'Callable[[int, Sequence[CollectionTag]], object]') -> 'dict[str, Any]'"
    },
    "download_retrieval_files": {
      "kind": "function",
      "signature": "(api: 'RetrievalDownloadApi', job_id: 'str', downloads: 'Sequence[RetrievalDownload]', *, concurrency: 'int', window: 'int', client_factory: 'Callable[[], RetrievalDownloadApi] | None' = None, on_downloaded: 'DownloadProgress | None' = None, heartbeat: 'DownloadHeartbeat | None' = None, heartbeat_interval_seconds: 'float' = 60.0) -> 'int'"
    },
    "hash_raw_source_chunks": {
      "kind": "function",
      "signature": "(*, path: 'str', chunks: 'Iterable[bytes]', expected_bytes: 'int', part_plaintext_bytes: 'int') -> 'RawSourceHash'"
    },
    "put_collection_upload_unit": {
      "kind": "function",
      "signature": "(api: 'CollectionUnitApi', collection_id: 'CollectionId', assignment: 'CollectionUploadUnitAssignmentDocument', *, content_for_unit: 'UnitContent', retry_notice: 'RetryNotice | None' = None, retry_initial_delay_seconds: 'float' = 1.0, retry_max_delay_seconds: 'float' = 10.0) -> 'int'"
    },
    "upload_collection_units": {
      "kind": "function",
      "signature": "(api: 'CollectionUnitApi', collection_id: 'CollectionId', *, content_for_unit: 'UnitContent', concurrency: 'int', window: 'int', client_factory: 'Callable[[], CollectionUnitApi] | None' = None, on_committed: 'UploadProgress | None' = None, on_resumed: 'UploadProgress | None' = None, retry_notice: 'RetryNotice | None' = None, cancel_check: 'Callable[[], None] | None' = None) -> 'int'"
    }
  },
  "module": "riverhog_client"
}
```
