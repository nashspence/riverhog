# V1 contract policies

[Atlas](../index.md)

Policies are defined once here and referenced from every dossier where they are proven to apply. Implementation-correctness witnesses remain outside this contract freeze artifact.

## Boundary

| Policy | Applications |
|---|---:|
| `boundary/frozen-authority/v1` | 85 |

### Definitions

#### `boundary/frozen-authority/v1`

The authority and extension boundary is maintainer-frozen for v1.

- Applicability: `["/boundaries"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - `generator:contract-projection`
  - `release:release.toml`

Applications: **85**

## Compatibility

| Policy | Applications |
|---|---:|
| `compatibility/archive/v1` | 1 |
| `compatibility/cli/v1` | 315 |
| `compatibility/components/v1` | 240 |
| `compatibility/configuration/v1` | 127 |
| `compatibility/http-api/v1` | 692 |
| `compatibility/python-api/v1` | 26 |
| `compatibility/recovery/v1` | 1 |

### Definitions

#### `compatibility/archive/v1`

A valid v1 archive remains independently recoverable throughout the supported v1 lifetime. Pack construction, ingress checkpointing, and resumable-write strategies are not archive-format compatibility promises.

- Applicability: `["/external_contract/release/compatibility/archive"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - `generator:contract-projection`
  - `release:release.toml`

Applications: **1**
#### `compatibility/cli/v1`

Command names, options, exit status, and structured output remain backward compatible throughout v1; prose output is for people.

- Applicability: `["/external_contract/release/compatibility/cli"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - `cli:gogurt`
  - `cli:mango-fish`
  - `cli:piggity`
  - `cli:riverhog-ftp-adapter`
  - `cli:riverhog-recover`
  - `cli:riverhog-storage-adapter-conformance`
  - `cli:riverhog-storage-adapter-filesystem-materialize`
  - `cli:riverhog-storage-adapter-schemas`
  - `cli:stove0`
  - `cli:stove0-observer-conformance`
  - `cli:stove0-observer-schemas`
  - `cli:stove0-review-planning`
  - `cli:stove0-review-sampler-conformance`
  - `cli:stove0-review-sampler-schemas`
  - `cli:stove0-target-conformance`
  - `cli:stove0-target-schemas`
  - `generator:contract-projection`
  - `operations:operation-matrix`
  - `release:release.toml`

Applications: **315**
#### `compatibility/components/v1`

A supported deployment runs components from one coordinated product version.

- Applicability: `["/external_contract/release/compatibility/components"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - `generator:contract-projection`
  - `operations:operation-matrix`
  - `protocol:generated:riverhog-storage-adapter`
  - `protocol:generated:stove0-observer`
  - `protocol:generated:stove0-review-sampler`
  - `protocol:generated:stove0-target`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-attributes.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-flags.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-stat.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-file-stat.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fs-flags.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fsxattr.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-mount-context.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-statx-attributes.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/macos-volume-context.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/observation-policy.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/sparse-map.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-backup-stream-info.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-compression-state.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-attributes.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-stat.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-integrity-info.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-object-id.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-reparse-point.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-security-descriptor.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-usn-record.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-volume-context.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-manifest-v1.schema.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-terminal-v1.schema.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-volume-v1.schema.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-collection-description-v1.schema.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-bindings-v1.schema.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-root-v1.schema.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-terminal-v1.schema.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-volume-v1.schema.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-recovery-descriptor-v1.schema.json`
  - `release:release.toml`
  - `state:gogurt-listener`
  - `state:mango-fish-cursor`
  - `state:piggity-local`
  - `state:riverhog-catalog`
  - `state:riverhog-ftp-custody`
  - `state:riverhog-provenance-installation`
  - `state:stove0-control`
  - `state:stove0-target-jobs`

Applications: **240**
#### `compatibility/configuration/v1`

Accepted v1 configuration remains valid throughout v1 unless an unsafe value must be rejected.

- Applicability: `["/external_contract/release/compatibility/configuration"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - `configuration-environment:GOGURT_LISTENER_HOST_PROVIDER`
  - `configuration-environment:GOGURT_MOUNTED_VOLUME_PROVIDER`
  - `configuration-environment:PIGGITY_LOCAL_DATABASE`
  - `configuration-environment:PIGGITY_LOCAL_ROOT`
  - `configuration-environment:PIGGITY_PLAIN`
  - `configuration-environment:PIGGITY_PROVENANCE_OBSERVER`
  - `configuration-environment:PIGGITY_UPLOAD_FILE_LOG_BYTES`
  - `configuration-environment:PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS`
  - `configuration-environment:PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS`
  - `configuration-environment:RIVERHOG_AGE_SESSION_CACHE_ENTRIES`
  - `configuration-environment:RIVERHOG_AGE_SESSION_DERIVATION_CONCURRENCY`
  - `configuration-environment:RIVERHOG_ALLOW_INSECURE_HTTP`
  - `configuration-environment:RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID`
  - `configuration-environment:RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES`
  - `configuration-environment:RIVERHOG_ARCHIVE_PASSPHRASES_JSON`
  - `configuration-environment:RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY`
  - `configuration-environment:RIVERHOG_ARCHIVE_READ_ORDER`
  - `configuration-environment:RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR`
  - `configuration-environment:RIVERHOG_ARCHIVE_STORES`
  - `configuration-environment:RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY`
  - `configuration-environment:RIVERHOG_ARCHIVE_UPLOAD_SWEEP_INTERVAL`
  - `configuration-environment:RIVERHOG_ARCHIVE_WRITE_CONCURRENCY`
  - `configuration-environment:RIVERHOG_ARCHIVE_WRITE_STORE`
  - `configuration-environment:RIVERHOG_BASE_URL`
  - `configuration-environment:RIVERHOG_BOOTSTRAP_TOKEN`
  - `configuration-environment:RIVERHOG_BROWSE_TOKEN_LIFETIME`
  - `configuration-environment:RIVERHOG_BROWSE_TOKEN_SIGNING_KEY`
  - `configuration-environment:RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME`
  - `configuration-environment:RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME`
  - `configuration-environment:RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE`
  - `configuration-environment:RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION`
  - `configuration-environment:RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX`
  - `configuration-environment:RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE`
  - `configuration-environment:RIVERHOG_DATABASE_URL`
  - `configuration-environment:RIVERHOG_DOWNLOAD_FILE_CONCURRENCY`
  - `configuration-environment:RIVERHOG_DOWNLOAD_FILE_WINDOW`
  - `configuration-environment:RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS`
  - `configuration-environment:RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE`
  - `configuration-environment:RIVERHOG_EVENT_CONTEXT_RETENTION`
  - `configuration-environment:RIVERHOG_EVENT_SOURCE`
  - `configuration-environment:RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP`
  - `configuration-environment:RIVERHOG_FTP_ADAPTER_BASE_URL`
  - `configuration-environment:RIVERHOG_FTP_ADAPTER_CONFIG`
  - `configuration-environment:RIVERHOG_FTP_ADAPTER_HTTP2`
  - `configuration-environment:RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS`
  - `configuration-environment:RIVERHOG_FTP_ADAPTER_TOKEN`
  - `configuration-environment:RIVERHOG_HOST_HEADER`
  - `configuration-environment:RIVERHOG_HTTP2`
  - `configuration-environment:RIVERHOG_HTTP_TIMEOUT_SECONDS`
  - `configuration-environment:RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES`
  - `configuration-environment:RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES`
  - `configuration-environment:RIVERHOG_LOG_LEVEL`
  - `configuration-environment:RIVERHOG_PACK_FILES`
  - `configuration-environment:RIVERHOG_PACK_MEMBER_BYTES`
  - `configuration-environment:RIVERHOG_PACK_SOURCE_BYTES`
  - `configuration-environment:RIVERHOG_PROVENANCE_STATE_HOME`
  - `configuration-environment:RIVERHOG_PUBLIC_BASE_URL`
  - `configuration-environment:RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES`
  - `configuration-environment:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_ENABLED`
  - `configuration-environment:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE`
  - `configuration-environment:RIVERHOG_RETRIEVAL_CACHE_STORES`
  - `configuration-environment:RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL`
  - `configuration-environment:RIVERHOG_RETRIEVAL_CACHE_WRITE_SEGMENT_BYTES`
  - `configuration-environment:RIVERHOG_RETRIEVAL_DEFAULT_LEASE`
  - `configuration-environment:RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY`
  - `configuration-environment:RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES`
  - `configuration-environment:RIVERHOG_RETRIEVAL_MAX_LEASE`
  - `configuration-environment:RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES`
  - `configuration-environment:RIVERHOG_RETRIEVAL_PENDING_TIMEOUT`
  - `configuration-environment:RIVERHOG_RETRIEVAL_RANGE_BILLING_MODE`
  - `configuration-environment:RIVERHOG_RETRIEVAL_RANGE_MERGE_GAP_BYTES`
  - `configuration-environment:RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES`
  - `configuration-environment:RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY`
  - `configuration-environment:RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL`
  - `configuration-environment:RIVERHOG_TOKEN`
  - `configuration-environment:RIVERHOG_UPLOAD_FILE_CONCURRENCY`
  - `configuration-environment:RIVERHOG_UPLOAD_FILE_WINDOW`
  - `configuration-environment:RIVERHOG_UPLOAD_TIMEOUT_SECONDS`
  - `configuration-environment:STOVE0_ADMISSIONS_PATH`
  - `configuration-environment:STOVE0_ALLOW_INSECURE_HTTP`
  - `configuration-environment:STOVE0_API_TOKEN`
  - `configuration-environment:STOVE0_BASE_URL`
  - `configuration-environment:STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS`
  - `configuration-environment:STOVE0_BROWSE_TOKEN_SIGNING_KEY`
  - `configuration-environment:STOVE0_CAPABILITY_TTL_SECONDS`
  - `configuration-environment:STOVE0_CLAIM_LEASE_SECONDS`
  - `configuration-environment:STOVE0_DATABASE_URL`
  - `configuration-environment:STOVE0_EXIFTOOL_BIN`
  - `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_HOST`
  - `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_IMAGE_DIGEST`
  - `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_PORT`
  - `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION`
  - `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_TOKEN`
  - `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE`
  - `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_WORKSPACE`
  - `configuration-environment:STOVE0_FFMPEG_BIN`
  - `configuration-environment:STOVE0_FFPROBE_BIN`
  - `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST`
  - `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST`
  - `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT`
  - `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION`
  - `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN`
  - `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE`
  - `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_WORKSPACE`
  - `configuration-environment:STOVE0_HTTP2`
  - `configuration-environment:STOVE0_HTTP_TIMEOUT_SECONDS`
  - `configuration-environment:STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD`
  - `configuration-environment:STOVE0_OBSERVERS_JSON`
  - `configuration-environment:STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS`
  - `configuration-environment:STOVE0_RECIPES_PATH`
  - `configuration-environment:STOVE0_SCHEDULER_INTERVAL_SECONDS`
  - `configuration-environment:STOVE0_TARGETS_JSON`
  - `configuration-environment:STOVE0_TARGET_AUTHORITY_BATCH_SIZE`
  - `configuration-environment:STOVE0_TARGET_CALLBACK_ALLOW_INSECURE_HTTP`
  - `configuration-environment:STOVE0_TARGET_CALLBACK_BASE_URL`
  - `configuration-environment:STOVE0_TARGET_CALLBACK_SIGNING_KEY`
  - `configuration-environment:STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS`
  - `configuration-environment:STOVE0_TOKEN`
  - `configuration-environment:STOVE0_WORKSPACE_ASSURANCE`
  - `configuration-environment:inventory`
  - `configuration:gogurt-routes`
  - `configuration:mango-fish`
  - `configuration:riverhog-ftp-adapter`
  - `configuration:stove0-recipes`
  - `configuration:stove0-review-target`
  - `configuration:stove0-review-target-sampler`
  - `generator:contract-projection`
  - `release:release.toml`

Applications: **127**
#### `compatibility/http-api/v1`

Published v1 HTTP and CloudEvents contracts remain backward compatible throughout v1.

- Applicability: `["/external_contract/release/compatibility/http_api"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - `generator:contract-projection`
  - `openapi:riverhog`
  - `openapi:riverhog-ftp-adapter`
  - `openapi:stove0`
  - `operations:operation-matrix`
  - `release:release.toml`

Applications: **692**
#### `compatibility/python-api/v1`

Reusable-library declared public-module exports and public signatures remain backward compatible throughout v1; undeclared implementation submodules and other component roles are not Python API promises.

- Applicability: `["/external_contract/release/compatibility/python_api"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - `generator:contract-projection`
  - `python:gogurt-core`
  - `python:gogurt-listener-runtime`
  - `python:http-api-contracts`
  - `python:lifecycle-events`
  - `python:riverhog-age`
  - `python:riverhog-application-access`
  - `python:riverhog-archive-contracts`
  - `python:riverhog-client:riverhog_client`
  - `python:riverhog-client:riverhog_client.transform`
  - `python:riverhog-protocol`
  - `python:riverhog-provenance`
  - `python:riverhog-provenance-contracts`
  - `python:riverhog-storage-adapter-asgi-support`
  - `python:riverhog-storage-adapter-protocol`
  - `python:riverhog-storage-adapter-support`
  - `python:stove0-api-client`
  - `python:stove0-observer-client`
  - `python:stove0-observer-protocol`
  - `python:stove0-observer-support`
  - `python:stove0-operator-contracts`
  - `python:stove0-protocol`
  - `python:stove0-recipe-config`
  - `python:stove0-target-client`
  - `python:stove0-target-protocol`
  - `python:stove0-target-support`
  - `release:release.toml`

Applications: **26**
#### `compatibility/recovery/v1`

Every later v1 recovery release reads every valid earlier v1 archive and provenance set.

- Applicability: `["/external_contract/release/compatibility/recovery"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - `generator:contract-projection`
  - `release:release.toml`

Applications: **1**

## Extent Principles

| Policy | Applications |
|---|---:|
| `extent-principle/bounded-work/v1` | 1 |
| `extent-principle/configuration/v1` | 1 |
| `extent-principle/implementation-privacy/v1` | 1 |
| `extent-principle/logical-totals/v1` | 4 |
| `extent-principle/operational-capacity/v1` | 1 |

### Definitions

#### `extent-principle/bounded-work/v1`

Large logical totals cross bounded pages, segments, or restartable work steps; a carrier bound does not redefine the logical total.

- Applicability: `["/external_contract/extents"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - `extent:extent-contract`
  - `generator:contract-projection`

Applications: **1**
#### `extent-principle/configuration/v1`

Hardware- or environment-dependent limits that observably affect accepted work are operator-configurable and source-linked.

- Applicability: `["/external_contract/extents"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - `extent:extent-contract`
  - `generator:contract-projection`

Applications: **1**
#### `extent-principle/implementation-privacy/v1`

Buffers, provider mechanics, database layout, and other non-observable implementation extents are not frozen here.

- Applicability: `["/external_contract/extents"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - `extent:extent-contract`
  - `generator:contract-projection`

Applications: **1**
#### `extent-principle/logical-totals/v1`

A finite logical total has no product-level semantic maximum unless its owning contract declares one.

- Applicability: `["/external_contract/extents"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - `extent:extent-contract`
  - `generator:contract-projection`

Applications: **4**
#### `extent-principle/operational-capacity/v1`

Capacity policy may explicitly reject, defer, or throttle work, but must not silently truncate it or become an undocumented semantic ceiling.

- Applicability: `["/external_contract/extents"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - `extent:extent-contract`
  - `generator:contract-projection`

Applications: **1**

## Extent Rules

| Policy | Applications |
|---|---:|
| `extent-rule/bounded-segment/v1` | 15 |
| `extent-rule/configuration-composition/v1` | 6 |
| `extent-rule/configured-capacity/v1` | 51 |
| `extent-rule/extension-contract/v1` | 17 |
| `extent-rule/no-semantic-maximum/v1` | 166 |
| `extent-rule/route-progression/v1` | 67 |
| `extent-rule/schema-bound/v1` | 453 |

### Definitions

#### `extent-rule/bounded-segment/v1`

| Rule field | Value |
|---|---|
| `authority` | the owning schema's x-riverhog-extent declaration |
| `completion` | the owner-declared progression or repeated-work contract |
| `exceeded` | bounded-carrier-validation-error |
| `policy` | segmented_no_total_max |
| `semantic_maximum` | None |

- Applicability: `["/external_contract/extents/decisions"]`
- Observable result or violation: `{"completion": "the owner-declared progression or repeated-work contract", "exceeded": "bounded-carrier-validation-error"}`
- Executable authorities:
  - `extent:extent-contract`
  - `generator:contract-projection`
  - `openapi:riverhog`
  - `protocol:generated:riverhog-storage-adapter`
  - `protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-volume-v1.schema.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-bindings-v1.schema.json`

Applications: **15**
#### `extent-rule/configuration-composition/v1`

| Rule field | Value |
|---|---|
| `authority` | the owning validated deployment configuration document |
| `declared_operational_maximum` | None |
| `hidden_maximum` | forbidden |
| `policy` | operational_policy |
| `semantic_maximum` | None |
| `silent_truncation` | forbidden |

- Applicability: `["/external_contract/extents/decisions"]`
- Observable result or violation: `{"hidden_maximum": "forbidden", "silent_truncation": "forbidden"}`
- Executable authorities:
  - `configuration:gogurt-routes`
  - `configuration:mango-fish`
  - `configuration:riverhog-ftp-adapter`
  - `configuration:stove0-recipes`
  - `configuration:stove0-review-target`
  - `extent:extent-contract`
  - `generator:contract-projection`

Applications: **6**
#### `extent-rule/configured-capacity/v1`

| Rule field | Value |
|---|---|
| `authority` | the source-linked operator configuration field |
| `capacity_behavior` | explicit-reject-defer-or-throttle |
| `policy` | operational_policy |
| `silent_truncation` | forbidden |

- Applicability: `["/external_contract/extents/decisions"]`
- Observable result or violation: `{"capacity_behavior": "explicit-reject-defer-or-throttle", "silent_truncation": "forbidden"}`
- Executable authorities:
  - `configuration-environment:PIGGITY_UPLOAD_FILE_LOG_BYTES`
  - `configuration-environment:PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS`
  - `configuration-environment:PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS`
  - `configuration-environment:RIVERHOG_AGE_SESSION_CACHE_ENTRIES`
  - `configuration-environment:RIVERHOG_AGE_SESSION_DERIVATION_CONCURRENCY`
  - `configuration-environment:RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES`
  - `configuration-environment:RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY`
  - `configuration-environment:RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY`
  - `configuration-environment:RIVERHOG_ARCHIVE_UPLOAD_SWEEP_INTERVAL`
  - `configuration-environment:RIVERHOG_ARCHIVE_WRITE_CONCURRENCY`
  - `configuration-environment:RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE`
  - `configuration-environment:RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION`
  - `configuration-environment:RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX`
  - `configuration-environment:RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE`
  - `configuration-environment:RIVERHOG_DOWNLOAD_FILE_CONCURRENCY`
  - `configuration-environment:RIVERHOG_DOWNLOAD_FILE_WINDOW`
  - `configuration-environment:RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS`
  - `configuration-environment:RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE`
  - `configuration-environment:RIVERHOG_EVENT_CONTEXT_RETENTION`
  - `configuration-environment:RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS`
  - `configuration-environment:RIVERHOG_HTTP_TIMEOUT_SECONDS`
  - `configuration-environment:RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES`
  - `configuration-environment:RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES`
  - `configuration-environment:RIVERHOG_PACK_MEMBER_BYTES`
  - `configuration-environment:RIVERHOG_PACK_SOURCE_BYTES`
  - `configuration-environment:RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES`
  - `configuration-environment:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE`
  - `configuration-environment:RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL`
  - `configuration-environment:RIVERHOG_RETRIEVAL_CACHE_WRITE_SEGMENT_BYTES`
  - `configuration-environment:RIVERHOG_RETRIEVAL_DEFAULT_LEASE`
  - `configuration-environment:RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES`
  - `configuration-environment:RIVERHOG_RETRIEVAL_MAX_LEASE`
  - `configuration-environment:RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES`
  - `configuration-environment:RIVERHOG_RETRIEVAL_PENDING_TIMEOUT`
  - `configuration-environment:RIVERHOG_RETRIEVAL_RANGE_MERGE_GAP_BYTES`
  - `configuration-environment:RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES`
  - `configuration-environment:RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY`
  - `configuration-environment:RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL`
  - `configuration-environment:RIVERHOG_UPLOAD_FILE_CONCURRENCY`
  - `configuration-environment:RIVERHOG_UPLOAD_FILE_WINDOW`
  - `configuration-environment:RIVERHOG_UPLOAD_TIMEOUT_SECONDS`
  - `configuration-environment:STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS`
  - `configuration-environment:STOVE0_CAPABILITY_TTL_SECONDS`
  - `configuration-environment:STOVE0_CLAIM_LEASE_SECONDS`
  - `configuration-environment:STOVE0_HTTP_TIMEOUT_SECONDS`
  - `configuration-environment:STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS`
  - `configuration-environment:STOVE0_SCHEDULER_INTERVAL_SECONDS`
  - `configuration-environment:STOVE0_TARGET_AUTHORITY_BATCH_SIZE`
  - `configuration-environment:STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS`
  - `configuration-environment:inventory`
  - `extent:extent-contract`
  - `generator:contract-projection`

Applications: **51**
#### `extent-rule/extension-contract/v1`

| Rule field | Value |
|---|---|
| `authority` | the independently versioned extension contract |
| `core_semantic_maximum` | None |
| `policy` | extension_owned |

- Applicability: `["/external_contract/extents/decisions"]`
- Observable result or violation: `{}`
- Executable authorities:
  - `extent:extent-contract`
  - `generator:contract-projection`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-attributes.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-flags.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-stat.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-file-stat.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fs-flags.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fsxattr.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-mount-context.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-statx-attributes.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/macos-volume-context.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-backup-stream-info.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-compression-state.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-attributes.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-stat.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-integrity-info.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-usn-record.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-volume-context.json`

Applications: **17**
#### `extent-rule/no-semantic-maximum/v1`

| Rule field | Value |
|---|---|
| `authority` | the owning schema's deliberate absence of a semantic maximum |
| `declared_operational_maximum` | None |
| `future_capacity_behavior` | explicit-configured-reject-defer-or-throttle |
| `hidden_maximum` | forbidden |
| `policy` | operational_policy |
| `semantic_maximum` | None |
| `silent_truncation` | forbidden |

- Applicability: `["/external_contract/extents/decisions"]`
- Observable result or violation: `{"hidden_maximum": "forbidden", "silent_truncation": "forbidden"}`
- Executable authorities:
  - `cli:piggity`
  - `extent:extent-contract`
  - `generator:contract-projection`
  - `openapi:riverhog`
  - `openapi:riverhog-ftp-adapter`
  - `openapi:stove0`
  - `protocol:generated:riverhog-storage-adapter`
  - `protocol:generated:stove0-observer`
  - `protocol:generated:stove0-review-sampler`
  - `protocol:generated:stove0-target`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/observation-policy.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/sparse-map.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-manifest-v1.schema.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-volume-v1.schema.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-bindings-v1.schema.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-volume-v1.schema.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-recovery-descriptor-v1.schema.json`

Applications: **166**
#### `extent-rule/route-progression/v1`

| Rule field | Value |
|---|---|
| `authority` | the route-owned x-riverhog-read-collection declaration |
| `completion` | the owning progression contract |
| `policy` | segmented_no_total_max |

- Applicability: `["/external_contract/extents/decisions"]`
- Observable result or violation: `{"completion": "the owning progression contract"}`
- Executable authorities:
  - `extent:extent-contract`
  - `generator:contract-projection`
  - `openapi:riverhog`
  - `openapi:riverhog-ftp-adapter`
  - `openapi:stove0`

Applications: **67**
#### `extent-rule/schema-bound/v1`

| Rule field | Value |
|---|---|
| `authority` | the projected JSON Schema constraint |
| `exceeded` | schema-validation-error |
| `policy` | fixed-or-contract-max |
| `requirement` | a non-fixed set maximum carries an owning reason declaration |

- Applicability: `["/external_contract/extents/decisions"]`
- Observable result or violation: `{"exceeded": "schema-validation-error"}`
- Executable authorities:
  - `cli:gogurt`
  - `cli:mango-fish`
  - `cli:piggity`
  - `cli:riverhog-ftp-adapter`
  - `cli:riverhog-recover`
  - `cli:riverhog-storage-adapter-conformance`
  - `cli:riverhog-storage-adapter-filesystem-materialize`
  - `cli:riverhog-storage-adapter-schemas`
  - `cli:stove0`
  - `cli:stove0-observer-schemas`
  - `cli:stove0-review-sampler-conformance`
  - `cli:stove0-review-sampler-schemas`
  - `cli:stove0-target-schemas`
  - `configuration:mango-fish`
  - `configuration:riverhog-ftp-adapter`
  - `configuration:stove0-recipes`
  - `configuration:stove0-review-target`
  - `configuration:stove0-review-target-sampler`
  - `extent:extent-contract`
  - `generator:contract-projection`
  - `openapi:riverhog`
  - `openapi:riverhog-ftp-adapter`
  - `openapi:stove0`
  - `protocol:generated:riverhog-storage-adapter`
  - `protocol:generated:stove0-observer`
  - `protocol:generated:stove0-review-sampler`
  - `protocol:generated:stove0-target`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-object-id.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-manifest-v1.schema.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-terminal-v1.schema.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-volume-v1.schema.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-collection-description-v1.schema.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-bindings-v1.schema.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-root-v1.schema.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-terminal-v1.schema.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-volume-v1.schema.json`
  - `protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-recovery-descriptor-v1.schema.json`

Applications: **453**

## Exclusion

| Policy | Applications |
|---|---:|
| `exclusion/process-launcher-not-cli/v1` | 13 |

### Definitions

#### `exclusion/process-launcher-not-cli/v1`

The installed entry point starts a separately inventoried process protocol and does not expose an independently maintained human or JSON CLI.

- Applicability: `"Installed service, adapter, observer, target, sampler, and effect launchers."`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - `release:release.toml`

Applications: **13**
