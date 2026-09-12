# V1 contract policies

[Atlas](../index.md)

Policies are defined once here and referenced from every dossier where they are proven to apply. Implementation-correctness witnesses remain outside this contract freeze artifact.

## Boundary

| Policy | Applications |
|---|---:|
| [boundary/frozen-authority/v1](#p-61994d3f0f) | 84 |

### Definitions

<a id="p-61994d3f0f"></a>
#### `boundary/frozen-authority/v1`

The authority and extension boundary is maintainer-frozen for v1.

- Applicability: `["/boundaries"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)
  - [release:release.toml](../evidence/sources.md#src-c5380dbe5f)

Applications: **84**

## Compatibility

| Policy | Applications |
|---|---:|
| [compatibility/archive/v1](#p-915b8756ae) | 1 |
| [compatibility/cli/v1](#p-48a89776de) | 315 |
| [compatibility/components/v1](#p-95e9a12259) | 239 |
| [compatibility/configuration/v1](#p-8dc08bb461) | 135 |
| [compatibility/http-api/v1](#p-5bc717c2c0) | 692 |
| [compatibility/python-api/v1](#p-e574772ba5) | 26 |
| [compatibility/recovery/v1](#p-04aa4508f1) | 1 |

### Definitions

<a id="p-915b8756ae"></a>
#### `compatibility/archive/v1`

A valid v1 archive remains independently recoverable throughout the supported v1 lifetime. Pack construction, ingress checkpointing, and resumable-write strategies are not archive-format compatibility promises.

- Applicability: `["/external_contract/release/compatibility/archive"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)
  - [release:release.toml](../evidence/sources.md#src-c5380dbe5f)

Applications: **1**
<a id="p-48a89776de"></a>
#### `compatibility/cli/v1`

Command names, options, exit status, and structured output remain backward compatible throughout v1; prose output is for people.

- Applicability: `["/external_contract/release/compatibility/cli"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [cli:gogurt](../evidence/sources.md#src-3b2297c37d)
  - [cli:mango-fish](../evidence/sources.md#src-3dcd5eedf2)
  - [cli:piggity](../evidence/sources.md#src-094022231f)
  - [cli:riverhog-ftp-adapter](../evidence/sources.md#src-303f765bca)
  - [cli:riverhog-recover](../evidence/sources.md#src-375119d633)
  - [cli:riverhog-storage-adapter-conformance](../evidence/sources.md#src-7ab923569b)
  - [cli:riverhog-storage-adapter-filesystem-materialize](../evidence/sources.md#src-c89790480b)
  - [cli:riverhog-storage-adapter-schemas](../evidence/sources.md#src-b90a9d08ff)
  - [cli:stove0](../evidence/sources.md#src-6203ae7d88)
  - [cli:stove0-observer-conformance](../evidence/sources.md#src-5719d140a8)
  - [cli:stove0-observer-schemas](../evidence/sources.md#src-e6175e3ae2)
  - [cli:stove0-review-planning](../evidence/sources.md#src-ae789ab860)
  - [cli:stove0-review-sampler-conformance](../evidence/sources.md#src-5796b3dff4)
  - [cli:stove0-review-sampler-schemas](../evidence/sources.md#src-a75c35f0c8)
  - [cli:stove0-target-conformance](../evidence/sources.md#src-7a44eec01b)
  - [cli:stove0-target-schemas](../evidence/sources.md#src-71d64b87b5)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)
  - [operations:operation-matrix](../evidence/sources.md#src-b032bdc56b)
  - [release:release.toml](../evidence/sources.md#src-c5380dbe5f)

Applications: **315**
<a id="p-95e9a12259"></a>
#### `compatibility/components/v1`

A supported deployment runs components from one coordinated product version.

- Applicability: `["/external_contract/release/compatibility/components"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)
  - [operations:operation-matrix](../evidence/sources.md#src-b032bdc56b)
  - [protocol:generated:riverhog-storage-adapter](../evidence/sources.md#src-ef281f2471)
  - [protocol:generated:stove0-observer](../evidence/sources.md#src-dcc0b5485b)
  - [protocol:generated:stove0-review-sampler](../evidence/sources.md#src-b47f3f4d7b)
  - [protocol:generated:stove0-target](../evidence/sources.md#src-2c42f9d39a)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json](../evidence/sources.md#src-819610ba95)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-attributes.json](../evidence/sources.md#src-8191cd2baf)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-flags.json](../evidence/sources.md#src-cb9d75fa32)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-stat.json](../evidence/sources.md#src-7295b2b671)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-file-stat.json](../evidence/sources.md#src-79a949b157)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fs-flags.json](../evidence/sources.md#src-5f8f37c8a2)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fsxattr.json](../evidence/sources.md#src-8ed956c86a)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-mount-context.json](../evidence/sources.md#src-e3afb2680c)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-statx-attributes.json](../evidence/sources.md#src-ea93e415ba)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/macos-volume-context.json](../evidence/sources.md#src-86983ef410)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/observation-policy.json](../evidence/sources.md#src-9581f745b7)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/sparse-map.json](../evidence/sources.md#src-5ae7bcd86c)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-backup-stream-info.json](../evidence/sources.md#src-40a5d23088)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-compression-state.json](../evidence/sources.md#src-40e7549720)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-attributes.json](../evidence/sources.md#src-01ce873e85)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-stat.json](../evidence/sources.md#src-232ad4e832)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-integrity-info.json](../evidence/sources.md#src-c4a2d3a892)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-object-id.json](../evidence/sources.md#src-76f06f0f43)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-reparse-point.json](../evidence/sources.md#src-7b71865af2)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-security-descriptor.json](../evidence/sources.md#src-1bd0c5feb8)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-usn-record.json](../evidence/sources.md#src-72281b7ad5)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-volume-context.json](../evidence/sources.md#src-025e48b0ab)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-manifest-v1.schema.json](../evidence/sources.md#src-fc0a3e3cab)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-terminal-v1.schema.json](../evidence/sources.md#src-c45ad44582)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-volume-v1.schema.json](../evidence/sources.md#src-c83e346f06)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-collection-description-v1.schema.json](../evidence/sources.md#src-57bde0d90d)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-bindings-v1.schema.json](../evidence/sources.md#src-c8e0251dd9)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-root-v1.schema.json](../evidence/sources.md#src-7c2ebe352d)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-terminal-v1.schema.json](../evidence/sources.md#src-a639df0ba1)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-volume-v1.schema.json](../evidence/sources.md#src-a3bfff3737)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-recovery-descriptor-v1.schema.json](../evidence/sources.md#src-bd3602393c)
  - [release:release.toml](../evidence/sources.md#src-c5380dbe5f)
  - [state:gogurt-listener](../evidence/sources.md#src-6b3ecfced3)
  - [state:mango-fish-cursor](../evidence/sources.md#src-b1cc215b8d)
  - [state:piggity-local](../evidence/sources.md#src-f6a1289f67)
  - [state:riverhog-catalog](../evidence/sources.md#src-d8b4a14670)
  - [state:riverhog-ftp-custody](../evidence/sources.md#src-54f88a3a47)
  - [state:riverhog-provenance-installation](../evidence/sources.md#src-080b970190)
  - [state:stove0-control](../evidence/sources.md#src-45e44b17fd)
  - [state:stove0-target-jobs](../evidence/sources.md#src-7b4138829a)

Applications: **239**
<a id="p-8dc08bb461"></a>
#### `compatibility/configuration/v1`

Accepted v1 configuration remains valid throughout v1 unless an unsafe value must be rejected.

- Applicability: `["/external_contract/release/compatibility/configuration"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [configuration-environment-pattern:riverhog-server:RIVERHOG_ARCHIVE_STORE_{store}_{setting}](../evidence/sources.md#src-3071ba44b3)
  - [configuration-environment:gogurt:GOGURT_LISTENER_HOST_PROVIDER](../evidence/sources.md#src-a33048bae0)
  - [configuration-environment:gogurt:GOGURT_MOUNTED_VOLUME_PROVIDER](../evidence/sources.md#src-e97a418da2)
  - [configuration-environment:inventory](../evidence/sources.md#src-26b33461d2)
  - [configuration-environment:piggity:PIGGITY_LOCAL_DATABASE](../evidence/sources.md#src-ab1d0074de)
  - [configuration-environment:piggity:PIGGITY_LOCAL_ROOT](../evidence/sources.md#src-7c4314c2cb)
  - [configuration-environment:piggity:PIGGITY_PLAIN](../evidence/sources.md#src-88f78b70a7)
  - [configuration-environment:piggity:PIGGITY_PROVENANCE_OBSERVER](../evidence/sources.md#src-df0bfb7f70)
  - [configuration-environment:piggity:PIGGITY_UPLOAD_FILE_LOG_BYTES](../evidence/sources.md#src-283d9ea8ad)
  - [configuration-environment:piggity:PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS](../evidence/sources.md#src-2057e6d1cc)
  - [configuration-environment:piggity:PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS](../evidence/sources.md#src-40c863880d)
  - [configuration-environment:riverhog-client:RIVERHOG_ALLOW_INSECURE_HTTP](../evidence/sources.md#src-6501ce08a3)
  - [configuration-environment:riverhog-client:RIVERHOG_BASE_URL](../evidence/sources.md#src-475671fe74)
  - [configuration-environment:riverhog-client:RIVERHOG_DOWNLOAD_FILE_CONCURRENCY](../evidence/sources.md#src-bb20193981)
  - [configuration-environment:riverhog-client:RIVERHOG_DOWNLOAD_FILE_WINDOW](../evidence/sources.md#src-4629108138)
  - [configuration-environment:riverhog-client:RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS](../evidence/sources.md#src-7d03d4b83f)
  - [configuration-environment:riverhog-client:RIVERHOG_HOST_HEADER](../evidence/sources.md#src-9a32401dbf)
  - [configuration-environment:riverhog-client:RIVERHOG_HTTP2](../evidence/sources.md#src-1774dabb11)
  - [configuration-environment:riverhog-client:RIVERHOG_HTTP_TIMEOUT_SECONDS](../evidence/sources.md#src-908bba2b9b)
  - [configuration-environment:riverhog-client:RIVERHOG_TOKEN](../evidence/sources.md#src-cc55a54933)
  - [configuration-environment:riverhog-client:RIVERHOG_UPLOAD_FILE_CONCURRENCY](../evidence/sources.md#src-5d0adeaf5d)
  - [configuration-environment:riverhog-client:RIVERHOG_UPLOAD_FILE_WINDOW](../evidence/sources.md#src-93e4082935)
  - [configuration-environment:riverhog-client:RIVERHOG_UPLOAD_TIMEOUT_SECONDS](../evidence/sources.md#src-6e31cc159d)
  - [configuration-environment:riverhog-ftp-adapter-api-client:RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP](../evidence/sources.md#src-85f3c43f72)
  - [configuration-environment:riverhog-ftp-adapter-api-client:RIVERHOG_FTP_ADAPTER_BASE_URL](../evidence/sources.md#src-aa9adbb30a)
  - [configuration-environment:riverhog-ftp-adapter-api-client:RIVERHOG_FTP_ADAPTER_HTTP2](../evidence/sources.md#src-dd720ed781)
  - [configuration-environment:riverhog-ftp-adapter-api-client:RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS](../evidence/sources.md#src-97bfcc2b77)
  - [configuration-environment:riverhog-ftp-adapter-api-client:RIVERHOG_FTP_ADAPTER_TOKEN](../evidence/sources.md#src-5a52b017b3)
  - [configuration-environment:riverhog-ftp-adapter:RIVERHOG_ALLOW_INSECURE_HTTP](../evidence/sources.md#src-8be0f96180)
  - [configuration-environment:riverhog-ftp-adapter:RIVERHOG_BASE_URL](../evidence/sources.md#src-b93808cfe5)
  - [configuration-environment:riverhog-ftp-adapter:RIVERHOG_FTP_ADAPTER_CONFIG](../evidence/sources.md#src-67c239e18e)
  - [configuration-environment:riverhog-provenance:RIVERHOG_PROVENANCE_STATE_HOME](../evidence/sources.md#src-9fe04e9d42)
  - [configuration-environment:riverhog-server:RIVERHOG_AGE_SESSION_CACHE_ENTRIES](../evidence/sources.md#src-d1018a4c53)
  - [configuration-environment:riverhog-server:RIVERHOG_AGE_SESSION_DERIVATION_CONCURRENCY](../evidence/sources.md#src-b3f6d318c3)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID](../evidence/sources.md#src-59ce1b7699)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES](../evidence/sources.md#src-309bc75357)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_PASSPHRASES_JSON](../evidence/sources.md#src-44d728aefc)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY](../evidence/sources.md#src-36a45a4230)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_READ_ORDER](../evidence/sources.md#src-b743528fa3)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR](../evidence/sources.md#src-3434cbb4f1)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_STORES](../evidence/sources.md#src-b4408185b7)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY](../evidence/sources.md#src-b5ab0e168b)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_UPLOAD_SWEEP_INTERVAL](../evidence/sources.md#src-19a319fc4e)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_WRITE_CONCURRENCY](../evidence/sources.md#src-66ed403dd1)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_WRITE_STORE](../evidence/sources.md#src-2a3a35733e)
  - [configuration-environment:riverhog-server:RIVERHOG_BOOTSTRAP_TOKEN](../evidence/sources.md#src-46bdbba342)
  - [configuration-environment:riverhog-server:RIVERHOG_BROWSE_TOKEN_LIFETIME](../evidence/sources.md#src-9cc099a19b)
  - [configuration-environment:riverhog-server:RIVERHOG_BROWSE_TOKEN_SIGNING_KEY](../evidence/sources.md#src-70d116fdbc)
  - [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME](../evidence/sources.md#src-bb4e9ed43a)
  - [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME](../evidence/sources.md#src-afa71403d4)
  - [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE](../evidence/sources.md#src-e04d565c18)
  - [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION](../evidence/sources.md#src-735831a8d4)
  - [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX](../evidence/sources.md#src-65433124fb)
  - [configuration-environment:riverhog-server:RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE](../evidence/sources.md#src-4cfc3a8358)
  - [configuration-environment:riverhog-server:RIVERHOG_DATABASE_URL](../evidence/sources.md#src-c84b874748)
  - [configuration-environment:riverhog-server:RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE](../evidence/sources.md#src-fef401b6a9)
  - [configuration-environment:riverhog-server:RIVERHOG_EVENT_CONTEXT_RETENTION](../evidence/sources.md#src-42756a2b0a)
  - [configuration-environment:riverhog-server:RIVERHOG_EVENT_SOURCE](../evidence/sources.md#src-cc7e1cd9d7)
  - [configuration-environment:riverhog-server:RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES](../evidence/sources.md#src-e1d4295958)
  - [configuration-environment:riverhog-server:RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES](../evidence/sources.md#src-bf9ecd1b0a)
  - [configuration-environment:riverhog-server:RIVERHOG_LOG_LEVEL](../evidence/sources.md#src-cb4528636e)
  - [configuration-environment:riverhog-server:RIVERHOG_PACK_FILES](../evidence/sources.md#src-7d59fe1eef)
  - [configuration-environment:riverhog-server:RIVERHOG_PACK_MEMBER_BYTES](../evidence/sources.md#src-f8aff5063d)
  - [configuration-environment:riverhog-server:RIVERHOG_PACK_SOURCE_BYTES](../evidence/sources.md#src-b2ca118143)
  - [configuration-environment:riverhog-server:RIVERHOG_PUBLIC_BASE_URL](../evidence/sources.md#src-fbbd173a78)
  - [configuration-environment:riverhog-server:RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES](../evidence/sources.md#src-3fb717c8c3)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_ENABLED](../evidence/sources.md#src-257c42be6d)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE](../evidence/sources.md#src-22c1f9d04c)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_STORES](../evidence/sources.md#src-51dd06072e)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL](../evidence/sources.md#src-d858623918)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_WRITE_SEGMENT_BYTES](../evidence/sources.md#src-26efc77b40)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_DEFAULT_LEASE](../evidence/sources.md#src-4fd9a92a9d)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY](../evidence/sources.md#src-63c6f042b8)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES](../evidence/sources.md#src-cb29acf59b)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_MAX_LEASE](../evidence/sources.md#src-c44aeb9ce3)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES](../evidence/sources.md#src-1e662f4cb2)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_PENDING_TIMEOUT](../evidence/sources.md#src-01bd40bbb3)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_RANGE_BILLING_MODE](../evidence/sources.md#src-d46028cd7f)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_RANGE_MERGE_GAP_BYTES](../evidence/sources.md#src-972a8351e5)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES](../evidence/sources.md#src-236f08fcd9)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY](../evidence/sources.md#src-a586c2cf3e)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL](../evidence/sources.md#src-4c5aea570f)
  - [configuration-environment:stove0-api-client:STOVE0_ALLOW_INSECURE_HTTP](../evidence/sources.md#src-a9f40fdb75)
  - [configuration-environment:stove0-api-client:STOVE0_BASE_URL](../evidence/sources.md#src-34ec96f5f9)
  - [configuration-environment:stove0-api-client:STOVE0_HTTP2](../evidence/sources.md#src-29a943f529)
  - [configuration-environment:stove0-api-client:STOVE0_HTTP_TIMEOUT_SECONDS](../evidence/sources.md#src-a470f49fc7)
  - [configuration-environment:stove0-api-client:STOVE0_TOKEN](../evidence/sources.md#src-c052f8f5ca)
  - [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_BIN](../evidence/sources.md#src-db3b5f3ab1)
  - [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_OBSERVER_HOST](../evidence/sources.md#src-8dfcd7e460)
  - [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_OBSERVER_IMAGE_DIGEST](../evidence/sources.md#src-e2f881a54b)
  - [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_OBSERVER_PORT](../evidence/sources.md#src-ee07e99c6c)
  - [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION](../evidence/sources.md#src-29eef6aa4c)
  - [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_OBSERVER_TOKEN](../evidence/sources.md#src-5cf4e00bd2)
  - [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE](../evidence/sources.md#src-7e8e1ce139)
  - [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_OBSERVER_WORKSPACE](../evidence/sources.md#src-98428ceea4)
  - [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_BIN](../evidence/sources.md#src-2e8c92027a)
  - [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST](../evidence/sources.md#src-1ede0d200b)
  - [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST](../evidence/sources.md#src-64a7dc58ec)
  - [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT](../evidence/sources.md#src-b6e1a506b4)
  - [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION](../evidence/sources.md#src-776ca532af)
  - [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN](../evidence/sources.md#src-42afc1d184)
  - [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE](../evidence/sources.md#src-476999fdb4)
  - [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_SAMPLING_OBSERVER_WORKSPACE](../evidence/sources.md#src-c36f19fb69)
  - [configuration-environment:stove0-nvenc-av1-opus-review-sampler:STOVE0_FFMPEG_BIN](../evidence/sources.md#src-f77354177a)
  - [configuration-environment:stove0-nvenc-av1-opus-target:STOVE0_FFMPEG_BIN](../evidence/sources.md#src-648abac914)
  - [configuration-environment:stove0-nvenc-av1-opus-target:STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD](../evidence/sources.md#src-6bc691e964)
  - [configuration-environment:stove0-opus-review-sampler:STOVE0_FFMPEG_BIN](../evidence/sources.md#src-194d028459)
  - [configuration-environment:stove0-opus-target:STOVE0_FFMPEG_BIN](../evidence/sources.md#src-854b16672a)
  - [configuration-environment:stove0-server:RIVERHOG_ALLOW_INSECURE_HTTP](../evidence/sources.md#src-ab009ffd92)
  - [configuration-environment:stove0-server:RIVERHOG_BASE_URL](../evidence/sources.md#src-94aba7e378)
  - [configuration-environment:stove0-server:RIVERHOG_TOKEN](../evidence/sources.md#src-a2d6243ee8)
  - [configuration-environment:stove0-server:STOVE0_ADMISSIONS_PATH](../evidence/sources.md#src-3e52273877)
  - [configuration-environment:stove0-server:STOVE0_API_TOKEN](../evidence/sources.md#src-b54731a17b)
  - [configuration-environment:stove0-server:STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS](../evidence/sources.md#src-8e79bc10a5)
  - [configuration-environment:stove0-server:STOVE0_BROWSE_TOKEN_SIGNING_KEY](../evidence/sources.md#src-f552366814)
  - [configuration-environment:stove0-server:STOVE0_CAPABILITY_TTL_SECONDS](../evidence/sources.md#src-0e9a09a14d)
  - [configuration-environment:stove0-server:STOVE0_CLAIM_LEASE_SECONDS](../evidence/sources.md#src-1997f656ba)
  - [configuration-environment:stove0-server:STOVE0_DATABASE_URL](../evidence/sources.md#src-d05255fc63)
  - [configuration-environment:stove0-server:STOVE0_OBSERVERS_JSON](../evidence/sources.md#src-5e770d1e0f)
  - [configuration-environment:stove0-server:STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS](../evidence/sources.md#src-024a6340ee)
  - [configuration-environment:stove0-server:STOVE0_RECIPES_PATH](../evidence/sources.md#src-afbf7c2b57)
  - [configuration-environment:stove0-server:STOVE0_SCHEDULER_INTERVAL_SECONDS](../evidence/sources.md#src-1ba44b59b5)
  - [configuration-environment:stove0-server:STOVE0_TARGETS_JSON](../evidence/sources.md#src-05b1eb365e)
  - [configuration-environment:stove0-server:STOVE0_TARGET_AUTHORITY_BATCH_SIZE](../evidence/sources.md#src-86bb163042)
  - [configuration-environment:stove0-server:STOVE0_TARGET_CALLBACK_ALLOW_INSECURE_HTTP](../evidence/sources.md#src-00eaae2681)
  - [configuration-environment:stove0-server:STOVE0_TARGET_CALLBACK_BASE_URL](../evidence/sources.md#src-074108cdde)
  - [configuration-environment:stove0-server:STOVE0_TARGET_CALLBACK_SIGNING_KEY](../evidence/sources.md#src-543ac98fff)
  - [configuration-environment:stove0-server:STOVE0_WORKSPACE_ASSURANCE](../evidence/sources.md#src-26ac73d782)
  - [configuration-environment:stove0-target-support:STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS](../evidence/sources.md#src-a4a7b5aed4)
  - [configuration:gogurt-routes](../evidence/sources.md#src-2066f471a7)
  - [configuration:mango-fish](../evidence/sources.md#src-168bf78fe3)
  - [configuration:riverhog-ftp-adapter](../evidence/sources.md#src-4d54b4c8a0)
  - [configuration:stove0-recipes](../evidence/sources.md#src-49c5fe0ba6)
  - [configuration:stove0-review-target](../evidence/sources.md#src-d3261b60a1)
  - [configuration:stove0-review-target-sampler](../evidence/sources.md#src-2ef831d421)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)
  - [release:release.toml](../evidence/sources.md#src-c5380dbe5f)

Applications: **135**
<a id="p-5bc717c2c0"></a>
#### `compatibility/http-api/v1`

Published v1 HTTP and CloudEvents contracts remain backward compatible throughout v1.

- Applicability: `["/external_contract/release/compatibility/http_api"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)
  - [openapi:riverhog](../evidence/sources.md#src-c42f268fc9)
  - [openapi:riverhog-ftp-adapter](../evidence/sources.md#src-c3a51ac29a)
  - [openapi:stove0](../evidence/sources.md#src-52e6e32124)
  - [operations:operation-matrix](../evidence/sources.md#src-b032bdc56b)
  - [release:release.toml](../evidence/sources.md#src-c5380dbe5f)

Applications: **692**
<a id="p-e574772ba5"></a>
#### `compatibility/python-api/v1`

Reusable-library declared public-module exports and public signatures remain backward compatible throughout v1; undeclared implementation submodules and other component roles are not Python API promises.

- Applicability: `["/external_contract/release/compatibility/python_api"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)
  - [python:gogurt-core](../evidence/sources.md#src-13281d4333)
  - [python:gogurt-listener-runtime](../evidence/sources.md#src-d3bf6489d6)
  - [python:http-api-contracts](../evidence/sources.md#src-721cfe4d9d)
  - [python:lifecycle-events](../evidence/sources.md#src-feb6b7ae70)
  - [python:riverhog-age](../evidence/sources.md#src-bd475bcd01)
  - [python:riverhog-application-access](../evidence/sources.md#src-8a8a1adad7)
  - [python:riverhog-archive-contracts](../evidence/sources.md#src-f696df2e90)
  - [python:riverhog-client:riverhog_client](../evidence/sources.md#src-c149020c71)
  - [python:riverhog-client:riverhog_client.transform](../evidence/sources.md#src-7a247bb534)
  - [python:riverhog-protocol](../evidence/sources.md#src-49f8b8ce2a)
  - [python:riverhog-provenance](../evidence/sources.md#src-6473d62ee2)
  - [python:riverhog-provenance-contracts](../evidence/sources.md#src-7722019950)
  - [python:riverhog-storage-adapter-asgi-support](../evidence/sources.md#src-de39c18ba5)
  - [python:riverhog-storage-adapter-protocol](../evidence/sources.md#src-4e78cc641f)
  - [python:riverhog-storage-adapter-support](../evidence/sources.md#src-ce06974b7f)
  - [python:stove0-api-client](../evidence/sources.md#src-f412d556d5)
  - [python:stove0-observer-client](../evidence/sources.md#src-657eb556bb)
  - [python:stove0-observer-protocol](../evidence/sources.md#src-74fcb7b8cc)
  - [python:stove0-observer-support](../evidence/sources.md#src-56a43288ef)
  - [python:stove0-operator-contracts](../evidence/sources.md#src-8f69eee57b)
  - [python:stove0-protocol](../evidence/sources.md#src-f43134520a)
  - [python:stove0-recipe-config](../evidence/sources.md#src-aabd9aa973)
  - [python:stove0-target-client](../evidence/sources.md#src-23a6624235)
  - [python:stove0-target-protocol](../evidence/sources.md#src-d1ead8f0ab)
  - [python:stove0-target-support](../evidence/sources.md#src-86c8847b4f)
  - [release:release.toml](../evidence/sources.md#src-c5380dbe5f)

Applications: **26**
<a id="p-04aa4508f1"></a>
#### `compatibility/recovery/v1`

Every later v1 recovery release reads every valid earlier v1 archive and provenance set.

- Applicability: `["/external_contract/release/compatibility/recovery"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)
  - [release:release.toml](../evidence/sources.md#src-c5380dbe5f)

Applications: **1**

## Extent Principles

| Policy | Applications |
|---|---:|
| [extent-principle/bounded-work/v1](#p-a16724dfa0) | 1 |
| [extent-principle/configuration/v1](#p-aa533611e9) | 1 |
| [extent-principle/implementation-privacy/v1](#p-5f9f32ebd9) | 1 |
| [extent-principle/logical-totals/v1](#p-cfe2e12ee6) | 1 |
| [extent-principle/operational-capacity/v1](#p-fac1e46f10) | 1 |

### Definitions

<a id="p-a16724dfa0"></a>
#### `extent-principle/bounded-work/v1`

Large logical totals cross bounded pages, segments, or restartable work steps; a carrier bound does not redefine the logical total.

- Applicability: `["/external_contract/extents"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)

Applications: **1**
<a id="p-aa533611e9"></a>
#### `extent-principle/configuration/v1`

Hardware- or environment-dependent limits that observably affect accepted work are operator-configurable and source-linked.

- Applicability: `["/external_contract/extents"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)

Applications: **1**
<a id="p-5f9f32ebd9"></a>
#### `extent-principle/implementation-privacy/v1`

Buffers, provider mechanics, database layout, and other non-observable implementation extents are not frozen here.

- Applicability: `["/external_contract/extents"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)

Applications: **1**
<a id="p-cfe2e12ee6"></a>
#### `extent-principle/logical-totals/v1`

A finite logical total has no product-level semantic maximum unless its owning contract declares one.

- Applicability: `["/external_contract/extents"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)

Applications: **1**
<a id="p-fac1e46f10"></a>
#### `extent-principle/operational-capacity/v1`

Capacity policy may explicitly reject, defer, or throttle work, but must not silently truncate it or become an undocumented semantic ceiling.

- Applicability: `["/external_contract/extents"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)

Applications: **1**

## Extent Rules

| Policy | Applications |
|---|---:|
| [extent-rule/bounded-segment/v1](#p-2b3f3f1594) | 15 |
| [extent-rule/configuration-composition/v1](#p-dcd344e8e5) | 6 |
| [extent-rule/configured-capacity/v1](#p-3ebc61fc99) | 51 |
| [extent-rule/extension-contract/v1](#p-75a89f9d1c) | 17 |
| [extent-rule/no-semantic-maximum/v1](#p-574724b48a) | 166 |
| [extent-rule/route-progression/v1](#p-6b76b527cb) | 67 |
| [extent-rule/schema-bound/v1](#p-c0db822fc0) | 453 |

### Definitions

<a id="p-2b3f3f1594"></a>
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
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)
  - [openapi:riverhog](../evidence/sources.md#src-c42f268fc9)
  - [protocol:generated:riverhog-storage-adapter](../evidence/sources.md#src-ef281f2471)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-volume-v1.schema.json](../evidence/sources.md#src-c83e346f06)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-bindings-v1.schema.json](../evidence/sources.md#src-c8e0251dd9)

Applications: **15**
<a id="p-dcd344e8e5"></a>
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
  - [configuration:gogurt-routes](../evidence/sources.md#src-2066f471a7)
  - [configuration:mango-fish](../evidence/sources.md#src-168bf78fe3)
  - [configuration:riverhog-ftp-adapter](../evidence/sources.md#src-4d54b4c8a0)
  - [configuration:stove0-recipes](../evidence/sources.md#src-49c5fe0ba6)
  - [configuration:stove0-review-target](../evidence/sources.md#src-d3261b60a1)
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)

Applications: **6**
<a id="p-3ebc61fc99"></a>
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
  - [configuration-environment-pattern:riverhog-server:RIVERHOG_ARCHIVE_STORE_{store}_{setting}](../evidence/sources.md#src-3071ba44b3)
  - [configuration-environment:inventory](../evidence/sources.md#src-26b33461d2)
  - [configuration-environment:piggity:PIGGITY_UPLOAD_FILE_LOG_BYTES](../evidence/sources.md#src-283d9ea8ad)
  - [configuration-environment:piggity:PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS](../evidence/sources.md#src-2057e6d1cc)
  - [configuration-environment:piggity:PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS](../evidence/sources.md#src-40c863880d)
  - [configuration-environment:riverhog-client:RIVERHOG_DOWNLOAD_FILE_CONCURRENCY](../evidence/sources.md#src-bb20193981)
  - [configuration-environment:riverhog-client:RIVERHOG_DOWNLOAD_FILE_WINDOW](../evidence/sources.md#src-4629108138)
  - [configuration-environment:riverhog-client:RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS](../evidence/sources.md#src-7d03d4b83f)
  - [configuration-environment:riverhog-client:RIVERHOG_HTTP_TIMEOUT_SECONDS](../evidence/sources.md#src-908bba2b9b)
  - [configuration-environment:riverhog-client:RIVERHOG_UPLOAD_FILE_CONCURRENCY](../evidence/sources.md#src-5d0adeaf5d)
  - [configuration-environment:riverhog-client:RIVERHOG_UPLOAD_FILE_WINDOW](../evidence/sources.md#src-93e4082935)
  - [configuration-environment:riverhog-client:RIVERHOG_UPLOAD_TIMEOUT_SECONDS](../evidence/sources.md#src-6e31cc159d)
  - [configuration-environment:riverhog-ftp-adapter-api-client:RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS](../evidence/sources.md#src-97bfcc2b77)
  - [configuration-environment:riverhog-server:RIVERHOG_AGE_SESSION_CACHE_ENTRIES](../evidence/sources.md#src-d1018a4c53)
  - [configuration-environment:riverhog-server:RIVERHOG_AGE_SESSION_DERIVATION_CONCURRENCY](../evidence/sources.md#src-b3f6d318c3)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES](../evidence/sources.md#src-309bc75357)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY](../evidence/sources.md#src-36a45a4230)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY](../evidence/sources.md#src-b5ab0e168b)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_UPLOAD_SWEEP_INTERVAL](../evidence/sources.md#src-19a319fc4e)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_WRITE_CONCURRENCY](../evidence/sources.md#src-66ed403dd1)
  - [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE](../evidence/sources.md#src-e04d565c18)
  - [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION](../evidence/sources.md#src-735831a8d4)
  - [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX](../evidence/sources.md#src-65433124fb)
  - [configuration-environment:riverhog-server:RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE](../evidence/sources.md#src-4cfc3a8358)
  - [configuration-environment:riverhog-server:RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE](../evidence/sources.md#src-fef401b6a9)
  - [configuration-environment:riverhog-server:RIVERHOG_EVENT_CONTEXT_RETENTION](../evidence/sources.md#src-42756a2b0a)
  - [configuration-environment:riverhog-server:RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES](../evidence/sources.md#src-e1d4295958)
  - [configuration-environment:riverhog-server:RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES](../evidence/sources.md#src-bf9ecd1b0a)
  - [configuration-environment:riverhog-server:RIVERHOG_PACK_MEMBER_BYTES](../evidence/sources.md#src-f8aff5063d)
  - [configuration-environment:riverhog-server:RIVERHOG_PACK_SOURCE_BYTES](../evidence/sources.md#src-b2ca118143)
  - [configuration-environment:riverhog-server:RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES](../evidence/sources.md#src-3fb717c8c3)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE](../evidence/sources.md#src-22c1f9d04c)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL](../evidence/sources.md#src-d858623918)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_WRITE_SEGMENT_BYTES](../evidence/sources.md#src-26efc77b40)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_DEFAULT_LEASE](../evidence/sources.md#src-4fd9a92a9d)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES](../evidence/sources.md#src-cb29acf59b)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_MAX_LEASE](../evidence/sources.md#src-c44aeb9ce3)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES](../evidence/sources.md#src-1e662f4cb2)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_PENDING_TIMEOUT](../evidence/sources.md#src-01bd40bbb3)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_RANGE_MERGE_GAP_BYTES](../evidence/sources.md#src-972a8351e5)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES](../evidence/sources.md#src-236f08fcd9)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY](../evidence/sources.md#src-a586c2cf3e)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL](../evidence/sources.md#src-4c5aea570f)
  - [configuration-environment:stove0-api-client:STOVE0_HTTP_TIMEOUT_SECONDS](../evidence/sources.md#src-a470f49fc7)
  - [configuration-environment:stove0-server:STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS](../evidence/sources.md#src-8e79bc10a5)
  - [configuration-environment:stove0-server:STOVE0_CAPABILITY_TTL_SECONDS](../evidence/sources.md#src-0e9a09a14d)
  - [configuration-environment:stove0-server:STOVE0_CLAIM_LEASE_SECONDS](../evidence/sources.md#src-1997f656ba)
  - [configuration-environment:stove0-server:STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS](../evidence/sources.md#src-024a6340ee)
  - [configuration-environment:stove0-server:STOVE0_SCHEDULER_INTERVAL_SECONDS](../evidence/sources.md#src-1ba44b59b5)
  - [configuration-environment:stove0-server:STOVE0_TARGET_AUTHORITY_BATCH_SIZE](../evidence/sources.md#src-86bb163042)
  - [configuration-environment:stove0-target-support:STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS](../evidence/sources.md#src-a4a7b5aed4)
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)

Applications: **51**
<a id="p-75a89f9d1c"></a>
#### `extent-rule/extension-contract/v1`

| Rule field | Value |
|---|---|
| `authority` | the independently versioned extension contract |
| `core_semantic_maximum` | None |
| `policy` | extension_owned |

- Applicability: `["/external_contract/extents/decisions"]`
- Observable result or violation: `{}`
- Executable authorities:
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-attributes.json](../evidence/sources.md#src-8191cd2baf)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-flags.json](../evidence/sources.md#src-cb9d75fa32)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-stat.json](../evidence/sources.md#src-7295b2b671)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-file-stat.json](../evidence/sources.md#src-79a949b157)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fs-flags.json](../evidence/sources.md#src-5f8f37c8a2)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fsxattr.json](../evidence/sources.md#src-8ed956c86a)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-mount-context.json](../evidence/sources.md#src-e3afb2680c)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-statx-attributes.json](../evidence/sources.md#src-ea93e415ba)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/macos-volume-context.json](../evidence/sources.md#src-86983ef410)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-backup-stream-info.json](../evidence/sources.md#src-40a5d23088)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-compression-state.json](../evidence/sources.md#src-40e7549720)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-attributes.json](../evidence/sources.md#src-01ce873e85)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-stat.json](../evidence/sources.md#src-232ad4e832)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-integrity-info.json](../evidence/sources.md#src-c4a2d3a892)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-usn-record.json](../evidence/sources.md#src-72281b7ad5)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-volume-context.json](../evidence/sources.md#src-025e48b0ab)

Applications: **17**
<a id="p-574724b48a"></a>
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
  - [cli:piggity](../evidence/sources.md#src-094022231f)
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)
  - [openapi:riverhog](../evidence/sources.md#src-c42f268fc9)
  - [openapi:riverhog-ftp-adapter](../evidence/sources.md#src-c3a51ac29a)
  - [openapi:stove0](../evidence/sources.md#src-52e6e32124)
  - [protocol:generated:riverhog-storage-adapter](../evidence/sources.md#src-ef281f2471)
  - [protocol:generated:stove0-observer](../evidence/sources.md#src-dcc0b5485b)
  - [protocol:generated:stove0-review-sampler](../evidence/sources.md#src-b47f3f4d7b)
  - [protocol:generated:stove0-target](../evidence/sources.md#src-2c42f9d39a)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json](../evidence/sources.md#src-819610ba95)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/observation-policy.json](../evidence/sources.md#src-9581f745b7)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/sparse-map.json](../evidence/sources.md#src-5ae7bcd86c)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-manifest-v1.schema.json](../evidence/sources.md#src-fc0a3e3cab)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-volume-v1.schema.json](../evidence/sources.md#src-c83e346f06)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-bindings-v1.schema.json](../evidence/sources.md#src-c8e0251dd9)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-volume-v1.schema.json](../evidence/sources.md#src-a3bfff3737)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-recovery-descriptor-v1.schema.json](../evidence/sources.md#src-bd3602393c)

Applications: **166**
<a id="p-6b76b527cb"></a>
#### `extent-rule/route-progression/v1`

| Rule field | Value |
|---|---|
| `authority` | the route-owned x-riverhog-read-collection declaration |
| `completion` | the owning progression contract |
| `policy` | segmented_no_total_max |

- Applicability: `["/external_contract/extents/decisions"]`
- Observable result or violation: `{"completion": "the owning progression contract"}`
- Executable authorities:
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)
  - [openapi:riverhog](../evidence/sources.md#src-c42f268fc9)
  - [openapi:riverhog-ftp-adapter](../evidence/sources.md#src-c3a51ac29a)
  - [openapi:stove0](../evidence/sources.md#src-52e6e32124)

Applications: **67**
<a id="p-c0db822fc0"></a>
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
  - [cli:gogurt](../evidence/sources.md#src-3b2297c37d)
  - [cli:mango-fish](../evidence/sources.md#src-3dcd5eedf2)
  - [cli:piggity](../evidence/sources.md#src-094022231f)
  - [cli:riverhog-ftp-adapter](../evidence/sources.md#src-303f765bca)
  - [cli:riverhog-recover](../evidence/sources.md#src-375119d633)
  - [cli:riverhog-storage-adapter-conformance](../evidence/sources.md#src-7ab923569b)
  - [cli:riverhog-storage-adapter-filesystem-materialize](../evidence/sources.md#src-c89790480b)
  - [cli:riverhog-storage-adapter-schemas](../evidence/sources.md#src-b90a9d08ff)
  - [cli:stove0](../evidence/sources.md#src-6203ae7d88)
  - [cli:stove0-observer-schemas](../evidence/sources.md#src-e6175e3ae2)
  - [cli:stove0-review-sampler-conformance](../evidence/sources.md#src-5796b3dff4)
  - [cli:stove0-review-sampler-schemas](../evidence/sources.md#src-a75c35f0c8)
  - [cli:stove0-target-schemas](../evidence/sources.md#src-71d64b87b5)
  - [configuration:mango-fish](../evidence/sources.md#src-168bf78fe3)
  - [configuration:riverhog-ftp-adapter](../evidence/sources.md#src-4d54b4c8a0)
  - [configuration:stove0-recipes](../evidence/sources.md#src-49c5fe0ba6)
  - [configuration:stove0-review-target](../evidence/sources.md#src-d3261b60a1)
  - [configuration:stove0-review-target-sampler](../evidence/sources.md#src-2ef831d421)
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)
  - [openapi:riverhog](../evidence/sources.md#src-c42f268fc9)
  - [openapi:riverhog-ftp-adapter](../evidence/sources.md#src-c3a51ac29a)
  - [openapi:stove0](../evidence/sources.md#src-52e6e32124)
  - [protocol:generated:riverhog-storage-adapter](../evidence/sources.md#src-ef281f2471)
  - [protocol:generated:stove0-observer](../evidence/sources.md#src-dcc0b5485b)
  - [protocol:generated:stove0-review-sampler](../evidence/sources.md#src-b47f3f4d7b)
  - [protocol:generated:stove0-target](../evidence/sources.md#src-2c42f9d39a)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json](../evidence/sources.md#src-819610ba95)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-object-id.json](../evidence/sources.md#src-76f06f0f43)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-manifest-v1.schema.json](../evidence/sources.md#src-fc0a3e3cab)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-terminal-v1.schema.json](../evidence/sources.md#src-c45ad44582)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-volume-v1.schema.json](../evidence/sources.md#src-c83e346f06)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-collection-description-v1.schema.json](../evidence/sources.md#src-57bde0d90d)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-bindings-v1.schema.json](../evidence/sources.md#src-c8e0251dd9)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-root-v1.schema.json](../evidence/sources.md#src-7c2ebe352d)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-terminal-v1.schema.json](../evidence/sources.md#src-a639df0ba1)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-volume-v1.schema.json](../evidence/sources.md#src-a3bfff3737)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-recovery-descriptor-v1.schema.json](../evidence/sources.md#src-bd3602393c)

Applications: **453**

## Exclusion

| Policy | Applications |
|---|---:|
| [exclusion/process-launcher-not-cli/v1](#p-572523784c) | 13 |

### Definitions

<a id="p-572523784c"></a>
#### `exclusion/process-launcher-not-cli/v1`

The installed entry point starts a separately inventoried process protocol and does not expose an independently maintained human or JSON CLI.

- Applicability: `"Installed service, adapter, observer, target, sampler, and effect launchers."`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [release:release.toml](../evidence/sources.md#src-c5380dbe5f)

Applications: **13**
