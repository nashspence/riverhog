# V1 contract policies

[Atlas](../index.md)

Policies are defined once here and referenced from every dossier where they are proven to apply. Implementation-correctness witnesses remain outside this contract freeze artifact.

## Boundary

| Policy | Applications |
|---|---:|
| [boundary/frozen-authority/v1](#p-61994d3f0fa8) | 85 |

### Definitions

<a id="p-61994d3f0fa8"></a>
#### `boundary/frozen-authority/v1`

The authority and extension boundary is maintainer-frozen for v1.

- Applicability: `["/boundaries"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4ffa)
  - [release:release.toml](../evidence/sources.md#src-c5380dbe5fe0)

Applications: **85**

## Compatibility

| Policy | Applications |
|---|---:|
| [compatibility/archive/v1](#p-915b8756ae46) | 1 |
| [compatibility/cli/v1](#p-48a89776de7f) | 315 |
| [compatibility/components/v1](#p-95e9a1225947) | 240 |
| [compatibility/configuration/v1](#p-8dc08bb46173) | 127 |
| [compatibility/http-api/v1](#p-5bc717c2c0ba) | 692 |
| [compatibility/python-api/v1](#p-e574772ba506) | 26 |
| [compatibility/recovery/v1](#p-04aa4508f139) | 1 |

### Definitions

<a id="p-915b8756ae46"></a>
#### `compatibility/archive/v1`

A valid v1 archive remains independently recoverable throughout the supported v1 lifetime. Pack construction, ingress checkpointing, and resumable-write strategies are not archive-format compatibility promises.

- Applicability: `["/external_contract/release/compatibility/archive"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4ffa)
  - [release:release.toml](../evidence/sources.md#src-c5380dbe5fe0)

Applications: **1**
<a id="p-48a89776de7f"></a>
#### `compatibility/cli/v1`

Command names, options, exit status, and structured output remain backward compatible throughout v1; prose output is for people.

- Applicability: `["/external_contract/release/compatibility/cli"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [cli:gogurt](../evidence/sources.md#src-3b2297c37dfe)
  - [cli:mango-fish](../evidence/sources.md#src-3dcd5eedf2a0)
  - [cli:piggity](../evidence/sources.md#src-094022231f2c)
  - [cli:riverhog-ftp-adapter](../evidence/sources.md#src-303f765bca1e)
  - [cli:riverhog-recover](../evidence/sources.md#src-375119d633c0)
  - [cli:riverhog-storage-adapter-conformance](../evidence/sources.md#src-7ab923569bbb)
  - [cli:riverhog-storage-adapter-filesystem-materialize](../evidence/sources.md#src-c89790480bd6)
  - [cli:riverhog-storage-adapter-schemas](../evidence/sources.md#src-b90a9d08ff5b)
  - [cli:stove0](../evidence/sources.md#src-6203ae7d8812)
  - [cli:stove0-observer-conformance](../evidence/sources.md#src-5719d140a8e0)
  - [cli:stove0-observer-schemas](../evidence/sources.md#src-e6175e3ae267)
  - [cli:stove0-review-planning](../evidence/sources.md#src-ae789ab8608f)
  - [cli:stove0-review-sampler-conformance](../evidence/sources.md#src-5796b3dff482)
  - [cli:stove0-review-sampler-schemas](../evidence/sources.md#src-a75c35f0c851)
  - [cli:stove0-target-conformance](../evidence/sources.md#src-7a44eec01bb9)
  - [cli:stove0-target-schemas](../evidence/sources.md#src-71d64b87b598)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4ffa)
  - [operations:operation-matrix](../evidence/sources.md#src-b032bdc56b3f)
  - [release:release.toml](../evidence/sources.md#src-c5380dbe5fe0)

Applications: **315**
<a id="p-95e9a1225947"></a>
#### `compatibility/components/v1`

A supported deployment runs components from one coordinated product version.

- Applicability: `["/external_contract/release/compatibility/components"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4ffa)
  - [operations:operation-matrix](../evidence/sources.md#src-b032bdc56b3f)
  - [protocol:generated:riverhog-storage-adapter](../evidence/sources.md#src-ef281f2471a9)
  - [protocol:generated:stove0-observer](../evidence/sources.md#src-dcc0b5485b73)
  - [protocol:generated:stove0-review-sampler](../evidence/sources.md#src-b47f3f4d7b7f)
  - [protocol:generated:stove0-target](../evidence/sources.md#src-2c42f9d39a0b)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json](../evidence/sources.md#src-819610ba95e0)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-attributes.json](../evidence/sources.md#src-8191cd2baf94)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-flags.json](../evidence/sources.md#src-cb9d75fa3252)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-stat.json](../evidence/sources.md#src-7295b2b671f0)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-file-stat.json](../evidence/sources.md#src-79a949b157c8)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fs-flags.json](../evidence/sources.md#src-5f8f37c8a227)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fsxattr.json](../evidence/sources.md#src-8ed956c86aee)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-mount-context.json](../evidence/sources.md#src-e3afb2680c06)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-statx-attributes.json](../evidence/sources.md#src-ea93e415ba1d)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/macos-volume-context.json](../evidence/sources.md#src-86983ef4106a)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/observation-policy.json](../evidence/sources.md#src-9581f745b7a5)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/sparse-map.json](../evidence/sources.md#src-5ae7bcd86c0a)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-backup-stream-info.json](../evidence/sources.md#src-40a5d2308830)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-compression-state.json](../evidence/sources.md#src-40e754972038)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-attributes.json](../evidence/sources.md#src-01ce873e8527)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-stat.json](../evidence/sources.md#src-232ad4e8328f)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-integrity-info.json](../evidence/sources.md#src-c4a2d3a89289)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-object-id.json](../evidence/sources.md#src-76f06f0f43dc)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-reparse-point.json](../evidence/sources.md#src-7b71865af2c5)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-security-descriptor.json](../evidence/sources.md#src-1bd0c5feb8ca)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-usn-record.json](../evidence/sources.md#src-72281b7ad5bf)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-volume-context.json](../evidence/sources.md#src-025e48b0abb0)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-manifest-v1.schema.json](../evidence/sources.md#src-fc0a3e3cabcf)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-terminal-v1.schema.json](../evidence/sources.md#src-c45ad445827a)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-volume-v1.schema.json](../evidence/sources.md#src-c83e346f0656)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-collection-description-v1.schema.json](../evidence/sources.md#src-57bde0d90d0d)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-bindings-v1.schema.json](../evidence/sources.md#src-c8e0251dd95d)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-root-v1.schema.json](../evidence/sources.md#src-7c2ebe352d46)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-terminal-v1.schema.json](../evidence/sources.md#src-a639df0ba137)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-volume-v1.schema.json](../evidence/sources.md#src-a3bfff3737b0)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-recovery-descriptor-v1.schema.json](../evidence/sources.md#src-bd3602393c5d)
  - [release:release.toml](../evidence/sources.md#src-c5380dbe5fe0)
  - [state:gogurt-listener](../evidence/sources.md#src-6b3ecfced37f)
  - [state:mango-fish-cursor](../evidence/sources.md#src-b1cc215b8d66)
  - [state:piggity-local](../evidence/sources.md#src-f6a1289f6703)
  - [state:riverhog-catalog](../evidence/sources.md#src-d8b4a1467008)
  - [state:riverhog-ftp-custody](../evidence/sources.md#src-54f88a3a472f)
  - [state:riverhog-provenance-installation](../evidence/sources.md#src-080b9701904d)
  - [state:stove0-control](../evidence/sources.md#src-45e44b17fd52)
  - [state:stove0-target-jobs](../evidence/sources.md#src-7b4138829a38)

Applications: **240**
<a id="p-8dc08bb46173"></a>
#### `compatibility/configuration/v1`

Accepted v1 configuration remains valid throughout v1 unless an unsafe value must be rejected.

- Applicability: `["/external_contract/release/compatibility/configuration"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [configuration-environment:GOGURT_LISTENER_HOST_PROVIDER](../evidence/sources.md#src-640a7c2dcf8c)
  - [configuration-environment:GOGURT_MOUNTED_VOLUME_PROVIDER](../evidence/sources.md#src-3ca721321c24)
  - [configuration-environment:PIGGITY_LOCAL_DATABASE](../evidence/sources.md#src-66ec61668844)
  - [configuration-environment:PIGGITY_LOCAL_ROOT](../evidence/sources.md#src-a3bab32771f4)
  - [configuration-environment:PIGGITY_PLAIN](../evidence/sources.md#src-a479b918495f)
  - [configuration-environment:PIGGITY_PROVENANCE_OBSERVER](../evidence/sources.md#src-8d51cac50a25)
  - [configuration-environment:PIGGITY_UPLOAD_FILE_LOG_BYTES](../evidence/sources.md#src-31ede303d6f7)
  - [configuration-environment:PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS](../evidence/sources.md#src-2d353fa23bb2)
  - [configuration-environment:PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS](../evidence/sources.md#src-f1be4bc4acee)
  - [configuration-environment:RIVERHOG_AGE_SESSION_CACHE_ENTRIES](../evidence/sources.md#src-0641b64e86a6)
  - [configuration-environment:RIVERHOG_AGE_SESSION_DERIVATION_CONCURRENCY](../evidence/sources.md#src-099e7315076a)
  - [configuration-environment:RIVERHOG_ALLOW_INSECURE_HTTP](../evidence/sources.md#src-50282d297a88)
  - [configuration-environment:RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID](../evidence/sources.md#src-103aaaade7fe)
  - [configuration-environment:RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES](../evidence/sources.md#src-d85dbd67f9ec)
  - [configuration-environment:RIVERHOG_ARCHIVE_PASSPHRASES_JSON](../evidence/sources.md#src-c4962bbade4b)
  - [configuration-environment:RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY](../evidence/sources.md#src-1510b0ce213d)
  - [configuration-environment:RIVERHOG_ARCHIVE_READ_ORDER](../evidence/sources.md#src-7716b4ded259)
  - [configuration-environment:RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR](../evidence/sources.md#src-8e32132922fb)
  - [configuration-environment:RIVERHOG_ARCHIVE_STORES](../evidence/sources.md#src-9f73907a836c)
  - [configuration-environment:RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY](../evidence/sources.md#src-7abda774e4ef)
  - [configuration-environment:RIVERHOG_ARCHIVE_UPLOAD_SWEEP_INTERVAL](../evidence/sources.md#src-c0a58107091d)
  - [configuration-environment:RIVERHOG_ARCHIVE_WRITE_CONCURRENCY](../evidence/sources.md#src-f7649955151c)
  - [configuration-environment:RIVERHOG_ARCHIVE_WRITE_STORE](../evidence/sources.md#src-755098156c20)
  - [configuration-environment:RIVERHOG_BASE_URL](../evidence/sources.md#src-0396e6ba9f28)
  - [configuration-environment:RIVERHOG_BOOTSTRAP_TOKEN](../evidence/sources.md#src-914d97b76beb)
  - [configuration-environment:RIVERHOG_BROWSE_TOKEN_LIFETIME](../evidence/sources.md#src-fe8e77928379)
  - [configuration-environment:RIVERHOG_BROWSE_TOKEN_SIGNING_KEY](../evidence/sources.md#src-4e8fa6daa7ce)
  - [configuration-environment:RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME](../evidence/sources.md#src-ec14a484889b)
  - [configuration-environment:RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME](../evidence/sources.md#src-00e8bb321ec5)
  - [configuration-environment:RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE](../evidence/sources.md#src-8c377756e4ca)
  - [configuration-environment:RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION](../evidence/sources.md#src-ac7f42ab580b)
  - [configuration-environment:RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX](../evidence/sources.md#src-4be5bf418d0e)
  - [configuration-environment:RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE](../evidence/sources.md#src-993ad537df67)
  - [configuration-environment:RIVERHOG_DATABASE_URL](../evidence/sources.md#src-e557a24df858)
  - [configuration-environment:RIVERHOG_DOWNLOAD_FILE_CONCURRENCY](../evidence/sources.md#src-2d3f1d8a6694)
  - [configuration-environment:RIVERHOG_DOWNLOAD_FILE_WINDOW](../evidence/sources.md#src-a5ca05826c27)
  - [configuration-environment:RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS](../evidence/sources.md#src-ef9582fda499)
  - [configuration-environment:RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE](../evidence/sources.md#src-066eb69a6c18)
  - [configuration-environment:RIVERHOG_EVENT_CONTEXT_RETENTION](../evidence/sources.md#src-7bf791cfe76d)
  - [configuration-environment:RIVERHOG_EVENT_SOURCE](../evidence/sources.md#src-6c9accca57ed)
  - [configuration-environment:RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP](../evidence/sources.md#src-3bd2b96688c7)
  - [configuration-environment:RIVERHOG_FTP_ADAPTER_BASE_URL](../evidence/sources.md#src-b54fad905c74)
  - [configuration-environment:RIVERHOG_FTP_ADAPTER_CONFIG](../evidence/sources.md#src-b14eb675d528)
  - [configuration-environment:RIVERHOG_FTP_ADAPTER_HTTP2](../evidence/sources.md#src-4a3517eac73c)
  - [configuration-environment:RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS](../evidence/sources.md#src-f4c02d915f95)
  - [configuration-environment:RIVERHOG_FTP_ADAPTER_TOKEN](../evidence/sources.md#src-0680cd1210d2)
  - [configuration-environment:RIVERHOG_HOST_HEADER](../evidence/sources.md#src-d40a0dd07986)
  - [configuration-environment:RIVERHOG_HTTP2](../evidence/sources.md#src-1b5671172bae)
  - [configuration-environment:RIVERHOG_HTTP_TIMEOUT_SECONDS](../evidence/sources.md#src-52020dc34588)
  - [configuration-environment:RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES](../evidence/sources.md#src-5c35b8da9816)
  - [configuration-environment:RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES](../evidence/sources.md#src-2e9902b11ab1)
  - [configuration-environment:RIVERHOG_LOG_LEVEL](../evidence/sources.md#src-180b30299f1b)
  - [configuration-environment:RIVERHOG_PACK_FILES](../evidence/sources.md#src-193a002a357a)
  - [configuration-environment:RIVERHOG_PACK_MEMBER_BYTES](../evidence/sources.md#src-993ef7621f52)
  - [configuration-environment:RIVERHOG_PACK_SOURCE_BYTES](../evidence/sources.md#src-a27d02099131)
  - [configuration-environment:RIVERHOG_PROVENANCE_STATE_HOME](../evidence/sources.md#src-6026192e04a4)
  - [configuration-environment:RIVERHOG_PUBLIC_BASE_URL](../evidence/sources.md#src-d5b1b6177dca)
  - [configuration-environment:RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES](../evidence/sources.md#src-14e5e08535ef)
  - [configuration-environment:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_ENABLED](../evidence/sources.md#src-79d80fdb1b6c)
  - [configuration-environment:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE](../evidence/sources.md#src-9737015df19c)
  - [configuration-environment:RIVERHOG_RETRIEVAL_CACHE_STORES](../evidence/sources.md#src-0b55d52d12ac)
  - [configuration-environment:RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL](../evidence/sources.md#src-eefaf953b4b4)
  - [configuration-environment:RIVERHOG_RETRIEVAL_CACHE_WRITE_SEGMENT_BYTES](../evidence/sources.md#src-2d54647873a5)
  - [configuration-environment:RIVERHOG_RETRIEVAL_DEFAULT_LEASE](../evidence/sources.md#src-aae2e4657a0b)
  - [configuration-environment:RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY](../evidence/sources.md#src-cc14c4b603da)
  - [configuration-environment:RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES](../evidence/sources.md#src-3b0410e3d66c)
  - [configuration-environment:RIVERHOG_RETRIEVAL_MAX_LEASE](../evidence/sources.md#src-c8d096f38ce8)
  - [configuration-environment:RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES](../evidence/sources.md#src-a99d583099d6)
  - [configuration-environment:RIVERHOG_RETRIEVAL_PENDING_TIMEOUT](../evidence/sources.md#src-607321e537da)
  - [configuration-environment:RIVERHOG_RETRIEVAL_RANGE_BILLING_MODE](../evidence/sources.md#src-dcbfe988b4a9)
  - [configuration-environment:RIVERHOG_RETRIEVAL_RANGE_MERGE_GAP_BYTES](../evidence/sources.md#src-4f7d119a279c)
  - [configuration-environment:RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES](../evidence/sources.md#src-05c3c82f547f)
  - [configuration-environment:RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY](../evidence/sources.md#src-5712c8aa5917)
  - [configuration-environment:RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL](../evidence/sources.md#src-f295b011d5c4)
  - [configuration-environment:RIVERHOG_TOKEN](../evidence/sources.md#src-30337f36d13d)
  - [configuration-environment:RIVERHOG_UPLOAD_FILE_CONCURRENCY](../evidence/sources.md#src-81e97d3c7b35)
  - [configuration-environment:RIVERHOG_UPLOAD_FILE_WINDOW](../evidence/sources.md#src-c97645ded238)
  - [configuration-environment:RIVERHOG_UPLOAD_TIMEOUT_SECONDS](../evidence/sources.md#src-403ba6184b66)
  - [configuration-environment:STOVE0_ADMISSIONS_PATH](../evidence/sources.md#src-6962872e1dcd)
  - [configuration-environment:STOVE0_ALLOW_INSECURE_HTTP](../evidence/sources.md#src-06df4fb5c3a4)
  - [configuration-environment:STOVE0_API_TOKEN](../evidence/sources.md#src-0cfef1474b7b)
  - [configuration-environment:STOVE0_BASE_URL](../evidence/sources.md#src-2df277ee96a3)
  - [configuration-environment:STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS](../evidence/sources.md#src-6490df2f5a1e)
  - [configuration-environment:STOVE0_BROWSE_TOKEN_SIGNING_KEY](../evidence/sources.md#src-b93955cc1213)
  - [configuration-environment:STOVE0_CAPABILITY_TTL_SECONDS](../evidence/sources.md#src-8bcfbbd2c374)
  - [configuration-environment:STOVE0_CLAIM_LEASE_SECONDS](../evidence/sources.md#src-a964a6d069ac)
  - [configuration-environment:STOVE0_DATABASE_URL](../evidence/sources.md#src-f3edb540b3c0)
  - [configuration-environment:STOVE0_EXIFTOOL_BIN](../evidence/sources.md#src-35bf242d8aa2)
  - [configuration-environment:STOVE0_EXIFTOOL_OBSERVER_HOST](../evidence/sources.md#src-ca32efa9d753)
  - [configuration-environment:STOVE0_EXIFTOOL_OBSERVER_IMAGE_DIGEST](../evidence/sources.md#src-37e81b0b87c2)
  - [configuration-environment:STOVE0_EXIFTOOL_OBSERVER_PORT](../evidence/sources.md#src-74ef86c74a47)
  - [configuration-environment:STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION](../evidence/sources.md#src-eb5066e04186)
  - [configuration-environment:STOVE0_EXIFTOOL_OBSERVER_TOKEN](../evidence/sources.md#src-0b63b9007c19)
  - [configuration-environment:STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE](../evidence/sources.md#src-9aa84ad536b1)
  - [configuration-environment:STOVE0_EXIFTOOL_OBSERVER_WORKSPACE](../evidence/sources.md#src-2a804ca9148c)
  - [configuration-environment:STOVE0_FFMPEG_BIN](../evidence/sources.md#src-a352d58c1520)
  - [configuration-environment:STOVE0_FFPROBE_BIN](../evidence/sources.md#src-2599a68a211b)
  - [configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST](../evidence/sources.md#src-e883355c399d)
  - [configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST](../evidence/sources.md#src-0ec6f3c354a6)
  - [configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT](../evidence/sources.md#src-40fb82896300)
  - [configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION](../evidence/sources.md#src-5e0f54d6573d)
  - [configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN](../evidence/sources.md#src-6184c716d493)
  - [configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE](../evidence/sources.md#src-dc1fbdf12277)
  - [configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_WORKSPACE](../evidence/sources.md#src-2c94d247c4a3)
  - [configuration-environment:STOVE0_HTTP2](../evidence/sources.md#src-16c8d7c0b86c)
  - [configuration-environment:STOVE0_HTTP_TIMEOUT_SECONDS](../evidence/sources.md#src-e0ab08e60c6b)
  - [configuration-environment:STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD](../evidence/sources.md#src-2745a75884b1)
  - [configuration-environment:STOVE0_OBSERVERS_JSON](../evidence/sources.md#src-25d3f3d25634)
  - [configuration-environment:STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS](../evidence/sources.md#src-218c436ac137)
  - [configuration-environment:STOVE0_RECIPES_PATH](../evidence/sources.md#src-c9dfc46efb3c)
  - [configuration-environment:STOVE0_SCHEDULER_INTERVAL_SECONDS](../evidence/sources.md#src-1f626435d140)
  - [configuration-environment:STOVE0_TARGETS_JSON](../evidence/sources.md#src-92e26dacb557)
  - [configuration-environment:STOVE0_TARGET_AUTHORITY_BATCH_SIZE](../evidence/sources.md#src-1792d6b9fd51)
  - [configuration-environment:STOVE0_TARGET_CALLBACK_ALLOW_INSECURE_HTTP](../evidence/sources.md#src-ef417bf18193)
  - [configuration-environment:STOVE0_TARGET_CALLBACK_BASE_URL](../evidence/sources.md#src-3c07f476633d)
  - [configuration-environment:STOVE0_TARGET_CALLBACK_SIGNING_KEY](../evidence/sources.md#src-f5bba573ec08)
  - [configuration-environment:STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS](../evidence/sources.md#src-4dced914a586)
  - [configuration-environment:STOVE0_TOKEN](../evidence/sources.md#src-6341e5d86da9)
  - [configuration-environment:STOVE0_WORKSPACE_ASSURANCE](../evidence/sources.md#src-c446ab90a283)
  - [configuration-environment:inventory](../evidence/sources.md#src-26b33461d20b)
  - [configuration:gogurt-routes](../evidence/sources.md#src-2066f471a772)
  - [configuration:mango-fish](../evidence/sources.md#src-168bf78fe3f0)
  - [configuration:riverhog-ftp-adapter](../evidence/sources.md#src-4d54b4c8a0ea)
  - [configuration:stove0-recipes](../evidence/sources.md#src-49c5fe0ba636)
  - [configuration:stove0-review-target](../evidence/sources.md#src-d3261b60a10f)
  - [configuration:stove0-review-target-sampler](../evidence/sources.md#src-2ef831d42179)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4ffa)
  - [release:release.toml](../evidence/sources.md#src-c5380dbe5fe0)

Applications: **127**
<a id="p-5bc717c2c0ba"></a>
#### `compatibility/http-api/v1`

Published v1 HTTP and CloudEvents contracts remain backward compatible throughout v1.

- Applicability: `["/external_contract/release/compatibility/http_api"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4ffa)
  - [openapi:riverhog](../evidence/sources.md#src-c42f268fc960)
  - [openapi:riverhog-ftp-adapter](../evidence/sources.md#src-c3a51ac29ac7)
  - [openapi:stove0](../evidence/sources.md#src-52e6e3212451)
  - [operations:operation-matrix](../evidence/sources.md#src-b032bdc56b3f)
  - [release:release.toml](../evidence/sources.md#src-c5380dbe5fe0)

Applications: **692**
<a id="p-e574772ba506"></a>
#### `compatibility/python-api/v1`

Reusable-library declared public-module exports and public signatures remain backward compatible throughout v1; undeclared implementation submodules and other component roles are not Python API promises.

- Applicability: `["/external_contract/release/compatibility/python_api"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4ffa)
  - [python:gogurt-core](../evidence/sources.md#src-13281d4333a7)
  - [python:gogurt-listener-runtime](../evidence/sources.md#src-d3bf6489d6c7)
  - [python:http-api-contracts](../evidence/sources.md#src-721cfe4d9d0d)
  - [python:lifecycle-events](../evidence/sources.md#src-feb6b7ae7098)
  - [python:riverhog-age](../evidence/sources.md#src-bd475bcd01e4)
  - [python:riverhog-application-access](../evidence/sources.md#src-8a8a1adad73e)
  - [python:riverhog-archive-contracts](../evidence/sources.md#src-f696df2e9014)
  - [python:riverhog-client:riverhog_client](../evidence/sources.md#src-c149020c7102)
  - [python:riverhog-client:riverhog_client.transform](../evidence/sources.md#src-7a247bb534f6)
  - [python:riverhog-protocol](../evidence/sources.md#src-49f8b8ce2a31)
  - [python:riverhog-provenance](../evidence/sources.md#src-6473d62ee263)
  - [python:riverhog-provenance-contracts](../evidence/sources.md#src-7722019950e5)
  - [python:riverhog-storage-adapter-asgi-support](../evidence/sources.md#src-de39c18ba5e0)
  - [python:riverhog-storage-adapter-protocol](../evidence/sources.md#src-4e78cc641f18)
  - [python:riverhog-storage-adapter-support](../evidence/sources.md#src-ce06974b7fef)
  - [python:stove0-api-client](../evidence/sources.md#src-f412d556d5a6)
  - [python:stove0-observer-client](../evidence/sources.md#src-657eb556bb16)
  - [python:stove0-observer-protocol](../evidence/sources.md#src-74fcb7b8cc2a)
  - [python:stove0-observer-support](../evidence/sources.md#src-56a43288ef52)
  - [python:stove0-operator-contracts](../evidence/sources.md#src-8f69eee57b84)
  - [python:stove0-protocol](../evidence/sources.md#src-f43134520a26)
  - [python:stove0-recipe-config](../evidence/sources.md#src-aabd9aa97357)
  - [python:stove0-target-client](../evidence/sources.md#src-23a662423507)
  - [python:stove0-target-protocol](../evidence/sources.md#src-d1ead8f0abf7)
  - [python:stove0-target-support](../evidence/sources.md#src-86c8847b4f27)
  - [release:release.toml](../evidence/sources.md#src-c5380dbe5fe0)

Applications: **26**
<a id="p-04aa4508f139"></a>
#### `compatibility/recovery/v1`

Every later v1 recovery release reads every valid earlier v1 archive and provenance set.

- Applicability: `["/external_contract/release/compatibility/recovery"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4ffa)
  - [release:release.toml](../evidence/sources.md#src-c5380dbe5fe0)

Applications: **1**

## Extent Principles

| Policy | Applications |
|---|---:|
| [extent-principle/bounded-work/v1](#p-a16724dfa091) | 1 |
| [extent-principle/configuration/v1](#p-aa533611e954) | 1 |
| [extent-principle/implementation-privacy/v1](#p-5f9f32ebd9e1) | 1 |
| [extent-principle/logical-totals/v1](#p-cfe2e12ee677) | 4 |
| [extent-principle/operational-capacity/v1](#p-fac1e46f10c9) | 1 |

### Definitions

<a id="p-a16724dfa091"></a>
#### `extent-principle/bounded-work/v1`

Large logical totals cross bounded pages, segments, or restartable work steps; a carrier bound does not redefine the logical total.

- Applicability: `["/external_contract/extents"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12e8)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4ffa)

Applications: **1**
<a id="p-aa533611e954"></a>
#### `extent-principle/configuration/v1`

Hardware- or environment-dependent limits that observably affect accepted work are operator-configurable and source-linked.

- Applicability: `["/external_contract/extents"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12e8)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4ffa)

Applications: **1**
<a id="p-5f9f32ebd9e1"></a>
#### `extent-principle/implementation-privacy/v1`

Buffers, provider mechanics, database layout, and other non-observable implementation extents are not frozen here.

- Applicability: `["/external_contract/extents"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12e8)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4ffa)

Applications: **1**
<a id="p-cfe2e12ee677"></a>
#### `extent-principle/logical-totals/v1`

A finite logical total has no product-level semantic maximum unless its owning contract declares one.

- Applicability: `["/external_contract/extents"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12e8)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4ffa)

Applications: **4**
<a id="p-fac1e46f10c9"></a>
#### `extent-principle/operational-capacity/v1`

Capacity policy may explicitly reject, defer, or throttle work, but must not silently truncate it or become an undocumented semantic ceiling.

- Applicability: `["/external_contract/extents"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12e8)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4ffa)

Applications: **1**

## Extent Rules

| Policy | Applications |
|---|---:|
| [extent-rule/bounded-segment/v1](#p-2b3f3f1594af) | 15 |
| [extent-rule/configuration-composition/v1](#p-dcd344e8e519) | 6 |
| [extent-rule/configured-capacity/v1](#p-3ebc61fc9972) | 51 |
| [extent-rule/extension-contract/v1](#p-75a89f9d1c36) | 17 |
| [extent-rule/no-semantic-maximum/v1](#p-574724b48af0) | 166 |
| [extent-rule/route-progression/v1](#p-6b76b527cb21) | 67 |
| [extent-rule/schema-bound/v1](#p-c0db822fc034) | 453 |

### Definitions

<a id="p-2b3f3f1594af"></a>
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
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12e8)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4ffa)
  - [openapi:riverhog](../evidence/sources.md#src-c42f268fc960)
  - [protocol:generated:riverhog-storage-adapter](../evidence/sources.md#src-ef281f2471a9)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-volume-v1.schema.json](../evidence/sources.md#src-c83e346f0656)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-bindings-v1.schema.json](../evidence/sources.md#src-c8e0251dd95d)

Applications: **15**
<a id="p-dcd344e8e519"></a>
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
  - [configuration:gogurt-routes](../evidence/sources.md#src-2066f471a772)
  - [configuration:mango-fish](../evidence/sources.md#src-168bf78fe3f0)
  - [configuration:riverhog-ftp-adapter](../evidence/sources.md#src-4d54b4c8a0ea)
  - [configuration:stove0-recipes](../evidence/sources.md#src-49c5fe0ba636)
  - [configuration:stove0-review-target](../evidence/sources.md#src-d3261b60a10f)
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12e8)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4ffa)

Applications: **6**
<a id="p-3ebc61fc9972"></a>
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
  - [configuration-environment:PIGGITY_UPLOAD_FILE_LOG_BYTES](../evidence/sources.md#src-31ede303d6f7)
  - [configuration-environment:PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS](../evidence/sources.md#src-2d353fa23bb2)
  - [configuration-environment:PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS](../evidence/sources.md#src-f1be4bc4acee)
  - [configuration-environment:RIVERHOG_AGE_SESSION_CACHE_ENTRIES](../evidence/sources.md#src-0641b64e86a6)
  - [configuration-environment:RIVERHOG_AGE_SESSION_DERIVATION_CONCURRENCY](../evidence/sources.md#src-099e7315076a)
  - [configuration-environment:RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES](../evidence/sources.md#src-d85dbd67f9ec)
  - [configuration-environment:RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY](../evidence/sources.md#src-1510b0ce213d)
  - [configuration-environment:RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY](../evidence/sources.md#src-7abda774e4ef)
  - [configuration-environment:RIVERHOG_ARCHIVE_UPLOAD_SWEEP_INTERVAL](../evidence/sources.md#src-c0a58107091d)
  - [configuration-environment:RIVERHOG_ARCHIVE_WRITE_CONCURRENCY](../evidence/sources.md#src-f7649955151c)
  - [configuration-environment:RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE](../evidence/sources.md#src-8c377756e4ca)
  - [configuration-environment:RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION](../evidence/sources.md#src-ac7f42ab580b)
  - [configuration-environment:RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX](../evidence/sources.md#src-4be5bf418d0e)
  - [configuration-environment:RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE](../evidence/sources.md#src-993ad537df67)
  - [configuration-environment:RIVERHOG_DOWNLOAD_FILE_CONCURRENCY](../evidence/sources.md#src-2d3f1d8a6694)
  - [configuration-environment:RIVERHOG_DOWNLOAD_FILE_WINDOW](../evidence/sources.md#src-a5ca05826c27)
  - [configuration-environment:RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS](../evidence/sources.md#src-ef9582fda499)
  - [configuration-environment:RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE](../evidence/sources.md#src-066eb69a6c18)
  - [configuration-environment:RIVERHOG_EVENT_CONTEXT_RETENTION](../evidence/sources.md#src-7bf791cfe76d)
  - [configuration-environment:RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS](../evidence/sources.md#src-f4c02d915f95)
  - [configuration-environment:RIVERHOG_HTTP_TIMEOUT_SECONDS](../evidence/sources.md#src-52020dc34588)
  - [configuration-environment:RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES](../evidence/sources.md#src-5c35b8da9816)
  - [configuration-environment:RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES](../evidence/sources.md#src-2e9902b11ab1)
  - [configuration-environment:RIVERHOG_PACK_MEMBER_BYTES](../evidence/sources.md#src-993ef7621f52)
  - [configuration-environment:RIVERHOG_PACK_SOURCE_BYTES](../evidence/sources.md#src-a27d02099131)
  - [configuration-environment:RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES](../evidence/sources.md#src-14e5e08535ef)
  - [configuration-environment:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE](../evidence/sources.md#src-9737015df19c)
  - [configuration-environment:RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL](../evidence/sources.md#src-eefaf953b4b4)
  - [configuration-environment:RIVERHOG_RETRIEVAL_CACHE_WRITE_SEGMENT_BYTES](../evidence/sources.md#src-2d54647873a5)
  - [configuration-environment:RIVERHOG_RETRIEVAL_DEFAULT_LEASE](../evidence/sources.md#src-aae2e4657a0b)
  - [configuration-environment:RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES](../evidence/sources.md#src-3b0410e3d66c)
  - [configuration-environment:RIVERHOG_RETRIEVAL_MAX_LEASE](../evidence/sources.md#src-c8d096f38ce8)
  - [configuration-environment:RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES](../evidence/sources.md#src-a99d583099d6)
  - [configuration-environment:RIVERHOG_RETRIEVAL_PENDING_TIMEOUT](../evidence/sources.md#src-607321e537da)
  - [configuration-environment:RIVERHOG_RETRIEVAL_RANGE_MERGE_GAP_BYTES](../evidence/sources.md#src-4f7d119a279c)
  - [configuration-environment:RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES](../evidence/sources.md#src-05c3c82f547f)
  - [configuration-environment:RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY](../evidence/sources.md#src-5712c8aa5917)
  - [configuration-environment:RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL](../evidence/sources.md#src-f295b011d5c4)
  - [configuration-environment:RIVERHOG_UPLOAD_FILE_CONCURRENCY](../evidence/sources.md#src-81e97d3c7b35)
  - [configuration-environment:RIVERHOG_UPLOAD_FILE_WINDOW](../evidence/sources.md#src-c97645ded238)
  - [configuration-environment:RIVERHOG_UPLOAD_TIMEOUT_SECONDS](../evidence/sources.md#src-403ba6184b66)
  - [configuration-environment:STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS](../evidence/sources.md#src-6490df2f5a1e)
  - [configuration-environment:STOVE0_CAPABILITY_TTL_SECONDS](../evidence/sources.md#src-8bcfbbd2c374)
  - [configuration-environment:STOVE0_CLAIM_LEASE_SECONDS](../evidence/sources.md#src-a964a6d069ac)
  - [configuration-environment:STOVE0_HTTP_TIMEOUT_SECONDS](../evidence/sources.md#src-e0ab08e60c6b)
  - [configuration-environment:STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS](../evidence/sources.md#src-218c436ac137)
  - [configuration-environment:STOVE0_SCHEDULER_INTERVAL_SECONDS](../evidence/sources.md#src-1f626435d140)
  - [configuration-environment:STOVE0_TARGET_AUTHORITY_BATCH_SIZE](../evidence/sources.md#src-1792d6b9fd51)
  - [configuration-environment:STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS](../evidence/sources.md#src-4dced914a586)
  - [configuration-environment:inventory](../evidence/sources.md#src-26b33461d20b)
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12e8)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4ffa)

Applications: **51**
<a id="p-75a89f9d1c36"></a>
#### `extent-rule/extension-contract/v1`

| Rule field | Value |
|---|---|
| `authority` | the independently versioned extension contract |
| `core_semantic_maximum` | None |
| `policy` | extension_owned |

- Applicability: `["/external_contract/extents/decisions"]`
- Observable result or violation: `{}`
- Executable authorities:
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12e8)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4ffa)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-attributes.json](../evidence/sources.md#src-8191cd2baf94)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-flags.json](../evidence/sources.md#src-cb9d75fa3252)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-stat.json](../evidence/sources.md#src-7295b2b671f0)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-file-stat.json](../evidence/sources.md#src-79a949b157c8)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fs-flags.json](../evidence/sources.md#src-5f8f37c8a227)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fsxattr.json](../evidence/sources.md#src-8ed956c86aee)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-mount-context.json](../evidence/sources.md#src-e3afb2680c06)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-statx-attributes.json](../evidence/sources.md#src-ea93e415ba1d)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/macos-volume-context.json](../evidence/sources.md#src-86983ef4106a)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-backup-stream-info.json](../evidence/sources.md#src-40a5d2308830)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-compression-state.json](../evidence/sources.md#src-40e754972038)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-attributes.json](../evidence/sources.md#src-01ce873e8527)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-stat.json](../evidence/sources.md#src-232ad4e8328f)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-integrity-info.json](../evidence/sources.md#src-c4a2d3a89289)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-usn-record.json](../evidence/sources.md#src-72281b7ad5bf)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-volume-context.json](../evidence/sources.md#src-025e48b0abb0)

Applications: **17**
<a id="p-574724b48af0"></a>
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
  - [cli:piggity](../evidence/sources.md#src-094022231f2c)
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12e8)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4ffa)
  - [openapi:riverhog](../evidence/sources.md#src-c42f268fc960)
  - [openapi:riverhog-ftp-adapter](../evidence/sources.md#src-c3a51ac29ac7)
  - [openapi:stove0](../evidence/sources.md#src-52e6e3212451)
  - [protocol:generated:riverhog-storage-adapter](../evidence/sources.md#src-ef281f2471a9)
  - [protocol:generated:stove0-observer](../evidence/sources.md#src-dcc0b5485b73)
  - [protocol:generated:stove0-review-sampler](../evidence/sources.md#src-b47f3f4d7b7f)
  - [protocol:generated:stove0-target](../evidence/sources.md#src-2c42f9d39a0b)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json](../evidence/sources.md#src-819610ba95e0)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/observation-policy.json](../evidence/sources.md#src-9581f745b7a5)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/sparse-map.json](../evidence/sources.md#src-5ae7bcd86c0a)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-manifest-v1.schema.json](../evidence/sources.md#src-fc0a3e3cabcf)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-volume-v1.schema.json](../evidence/sources.md#src-c83e346f0656)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-bindings-v1.schema.json](../evidence/sources.md#src-c8e0251dd95d)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-volume-v1.schema.json](../evidence/sources.md#src-a3bfff3737b0)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-recovery-descriptor-v1.schema.json](../evidence/sources.md#src-bd3602393c5d)

Applications: **166**
<a id="p-6b76b527cb21"></a>
#### `extent-rule/route-progression/v1`

| Rule field | Value |
|---|---|
| `authority` | the route-owned x-riverhog-read-collection declaration |
| `completion` | the owning progression contract |
| `policy` | segmented_no_total_max |

- Applicability: `["/external_contract/extents/decisions"]`
- Observable result or violation: `{"completion": "the owning progression contract"}`
- Executable authorities:
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12e8)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4ffa)
  - [openapi:riverhog](../evidence/sources.md#src-c42f268fc960)
  - [openapi:riverhog-ftp-adapter](../evidence/sources.md#src-c3a51ac29ac7)
  - [openapi:stove0](../evidence/sources.md#src-52e6e3212451)

Applications: **67**
<a id="p-c0db822fc034"></a>
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
  - [cli:gogurt](../evidence/sources.md#src-3b2297c37dfe)
  - [cli:mango-fish](../evidence/sources.md#src-3dcd5eedf2a0)
  - [cli:piggity](../evidence/sources.md#src-094022231f2c)
  - [cli:riverhog-ftp-adapter](../evidence/sources.md#src-303f765bca1e)
  - [cli:riverhog-recover](../evidence/sources.md#src-375119d633c0)
  - [cli:riverhog-storage-adapter-conformance](../evidence/sources.md#src-7ab923569bbb)
  - [cli:riverhog-storage-adapter-filesystem-materialize](../evidence/sources.md#src-c89790480bd6)
  - [cli:riverhog-storage-adapter-schemas](../evidence/sources.md#src-b90a9d08ff5b)
  - [cli:stove0](../evidence/sources.md#src-6203ae7d8812)
  - [cli:stove0-observer-schemas](../evidence/sources.md#src-e6175e3ae267)
  - [cli:stove0-review-sampler-conformance](../evidence/sources.md#src-5796b3dff482)
  - [cli:stove0-review-sampler-schemas](../evidence/sources.md#src-a75c35f0c851)
  - [cli:stove0-target-schemas](../evidence/sources.md#src-71d64b87b598)
  - [configuration:mango-fish](../evidence/sources.md#src-168bf78fe3f0)
  - [configuration:riverhog-ftp-adapter](../evidence/sources.md#src-4d54b4c8a0ea)
  - [configuration:stove0-recipes](../evidence/sources.md#src-49c5fe0ba636)
  - [configuration:stove0-review-target](../evidence/sources.md#src-d3261b60a10f)
  - [configuration:stove0-review-target-sampler](../evidence/sources.md#src-2ef831d42179)
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12e8)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4ffa)
  - [openapi:riverhog](../evidence/sources.md#src-c42f268fc960)
  - [openapi:riverhog-ftp-adapter](../evidence/sources.md#src-c3a51ac29ac7)
  - [openapi:stove0](../evidence/sources.md#src-52e6e3212451)
  - [protocol:generated:riverhog-storage-adapter](../evidence/sources.md#src-ef281f2471a9)
  - [protocol:generated:stove0-observer](../evidence/sources.md#src-dcc0b5485b73)
  - [protocol:generated:stove0-review-sampler](../evidence/sources.md#src-b47f3f4d7b7f)
  - [protocol:generated:stove0-target](../evidence/sources.md#src-2c42f9d39a0b)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json](../evidence/sources.md#src-819610ba95e0)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-object-id.json](../evidence/sources.md#src-76f06f0f43dc)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-manifest-v1.schema.json](../evidence/sources.md#src-fc0a3e3cabcf)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-terminal-v1.schema.json](../evidence/sources.md#src-c45ad445827a)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-volume-v1.schema.json](../evidence/sources.md#src-c83e346f0656)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-collection-description-v1.schema.json](../evidence/sources.md#src-57bde0d90d0d)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-bindings-v1.schema.json](../evidence/sources.md#src-c8e0251dd95d)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-root-v1.schema.json](../evidence/sources.md#src-7c2ebe352d46)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-terminal-v1.schema.json](../evidence/sources.md#src-a639df0ba137)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-volume-v1.schema.json](../evidence/sources.md#src-a3bfff3737b0)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-recovery-descriptor-v1.schema.json](../evidence/sources.md#src-bd3602393c5d)

Applications: **453**

## Exclusion

| Policy | Applications |
|---|---:|
| [exclusion/process-launcher-not-cli/v1](#p-572523784c3c) | 13 |

### Definitions

<a id="p-572523784c3c"></a>
#### `exclusion/process-launcher-not-cli/v1`

The installed entry point starts a separately inventoried process protocol and does not expose an independently maintained human or JSON CLI.

- Applicability: `"Installed service, adapter, observer, target, sampler, and effect launchers."`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [release:release.toml](../evidence/sources.md#src-c5380dbe5fe0)

Applications: **13**
