# Executable sources and qualification routes

[Atlas](../index.md)

Every dossier names its executable source authorities and applicable qualification routes. This index summarizes that exact proof routing without making implementation witnesses part of the semantic contract.

## Qualification-route applications

| Qualification route | Count |
|---|---:|
| `make build` | 200 |
| `make compose-smoke` | 670 |
| `make contract-freeze` | 15 |
| `make database-qualification` | 9 |
| `make dist-smoke` | 270 |
| `make operation-qualification` | 873 |
| `make release-check` | 106 |
| `make unit` | 126 |

## Source-authority applications

Source authorities: **217**

| Source authority | Applications | Executable location |
|---|---:|---|
| `cli:gogurt` | 21 | `reference/gogurt/application/src/gogurt/cli.py::<module>` |
| `cli:mango-fish` | 5 | `reference/riverhog/applications/mango-fish/src/mango_fish/cli.py::<module>` |
| `cli:piggity` | 86 | `reference/riverhog/applications/piggity/src/piggity/main.py::<module>` |
| `cli:riverhog-ftp-adapter` | 7 | `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/app.py::<module>` |
| `cli:riverhog-recover` | 1 | `reference/riverhog/recovery/src/riverhog_recover/cli.py::<module>` |
| `cli:riverhog-storage-adapter-conformance` | 1 | `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/conformance.py::<module>` |
| `cli:riverhog-storage-adapter-filesystem-materialize` | 1 | `reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/materialize_cli.py::<module>` |
| `cli:riverhog-storage-adapter-schemas` | 1 | `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::<module>` |
| `cli:stove0` | 37 | `reference/stove0/application/client/src/stove0_cli/main.py::<module>` |
| `cli:stove0-observer-conformance` | 1 | `reference/stove0/packages/observer-support/src/stove0_observer_support/conformance.py::<module>` |
| `cli:stove0-observer-schemas` | 1 | `reference/stove0/packages/observer-support/src/stove0_observer_support/schemas.py::<module>` |
| `cli:stove0-review-planning` | 1 | `reference/stove0/targets/review/planning/src/stove0_review_planning/conformance.py::<module>` |
| `cli:stove0-review-sampler-conformance` | 1 | `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/conformance.py::<module>` |
| `cli:stove0-review-sampler-schemas` | 1 | `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/schemas.py::<module>` |
| `cli:stove0-target-conformance` | 1 | `reference/stove0/packages/target-support/src/stove0_target_support/conformance.py::<module>` |
| `cli:stove0-target-schemas` | 1 | `reference/stove0/packages/target-support/src/stove0_target_support/schemas.py::<module>` |
| `configuration-environment:GOGURT_LISTENER_HOST_PROVIDER` | 1 | `configuration-environment:GOGURT_LISTENER_HOST_PROVIDER` |
| `configuration-environment:GOGURT_MOUNTED_VOLUME_PROVIDER` | 1 | `configuration-environment:GOGURT_MOUNTED_VOLUME_PROVIDER` |
| `configuration-environment:PIGGITY_LOCAL_DATABASE` | 1 | `configuration-environment:PIGGITY_LOCAL_DATABASE` |
| `configuration-environment:PIGGITY_LOCAL_ROOT` | 1 | `configuration-environment:PIGGITY_LOCAL_ROOT` |
| `configuration-environment:PIGGITY_PLAIN` | 1 | `configuration-environment:PIGGITY_PLAIN` |
| `configuration-environment:PIGGITY_PROVENANCE_OBSERVER` | 1 | `configuration-environment:PIGGITY_PROVENANCE_OBSERVER` |
| `configuration-environment:PIGGITY_UPLOAD_FILE_LOG_BYTES` | 1 | `configuration-environment:PIGGITY_UPLOAD_FILE_LOG_BYTES` |
| `configuration-environment:PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS` | 1 | `configuration-environment:PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS` |
| `configuration-environment:PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS` | 1 | `configuration-environment:PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS` |
| `configuration-environment:RIVERHOG_AGE_SESSION_CACHE_ENTRIES` | 1 | `configuration-environment:RIVERHOG_AGE_SESSION_CACHE_ENTRIES` |
| `configuration-environment:RIVERHOG_AGE_SESSION_DERIVATION_CONCURRENCY` | 1 | `configuration-environment:RIVERHOG_AGE_SESSION_DERIVATION_CONCURRENCY` |
| `configuration-environment:RIVERHOG_ALLOW_INSECURE_HTTP` | 1 | `configuration-environment:RIVERHOG_ALLOW_INSECURE_HTTP` |
| `configuration-environment:RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID` | 1 | `configuration-environment:RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID` |
| `configuration-environment:RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES` | 1 | `configuration-environment:RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES` |
| `configuration-environment:RIVERHOG_ARCHIVE_PASSPHRASES_JSON` | 1 | `configuration-environment:RIVERHOG_ARCHIVE_PASSPHRASES_JSON` |
| `configuration-environment:RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY` | 1 | `configuration-environment:RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY` |
| `configuration-environment:RIVERHOG_ARCHIVE_READ_ORDER` | 1 | `configuration-environment:RIVERHOG_ARCHIVE_READ_ORDER` |
| `configuration-environment:RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR` | 1 | `configuration-environment:RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR` |
| `configuration-environment:RIVERHOG_ARCHIVE_STORES` | 1 | `configuration-environment:RIVERHOG_ARCHIVE_STORES` |
| `configuration-environment:RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY` | 1 | `configuration-environment:RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY` |
| `configuration-environment:RIVERHOG_ARCHIVE_UPLOAD_SWEEP_INTERVAL` | 1 | `configuration-environment:RIVERHOG_ARCHIVE_UPLOAD_SWEEP_INTERVAL` |
| `configuration-environment:RIVERHOG_ARCHIVE_WRITE_CONCURRENCY` | 1 | `configuration-environment:RIVERHOG_ARCHIVE_WRITE_CONCURRENCY` |
| `configuration-environment:RIVERHOG_ARCHIVE_WRITE_STORE` | 1 | `configuration-environment:RIVERHOG_ARCHIVE_WRITE_STORE` |
| `configuration-environment:RIVERHOG_BASE_URL` | 1 | `configuration-environment:RIVERHOG_BASE_URL` |
| `configuration-environment:RIVERHOG_BOOTSTRAP_TOKEN` | 1 | `configuration-environment:RIVERHOG_BOOTSTRAP_TOKEN` |
| `configuration-environment:RIVERHOG_BROWSE_TOKEN_LIFETIME` | 1 | `configuration-environment:RIVERHOG_BROWSE_TOKEN_LIFETIME` |
| `configuration-environment:RIVERHOG_BROWSE_TOKEN_SIGNING_KEY` | 1 | `configuration-environment:RIVERHOG_BROWSE_TOKEN_SIGNING_KEY` |
| `configuration-environment:RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME` | 1 | `configuration-environment:RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME` |
| `configuration-environment:RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME` | 1 | `configuration-environment:RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME` |
| `configuration-environment:RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE` | 1 | `configuration-environment:RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE` |
| `configuration-environment:RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION` | 1 | `configuration-environment:RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION` |
| `configuration-environment:RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX` | 1 | `configuration-environment:RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX` |
| `configuration-environment:RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE` | 1 | `configuration-environment:RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE` |
| `configuration-environment:RIVERHOG_DATABASE_URL` | 1 | `configuration-environment:RIVERHOG_DATABASE_URL` |
| `configuration-environment:RIVERHOG_DOWNLOAD_FILE_CONCURRENCY` | 1 | `configuration-environment:RIVERHOG_DOWNLOAD_FILE_CONCURRENCY` |
| `configuration-environment:RIVERHOG_DOWNLOAD_FILE_WINDOW` | 1 | `configuration-environment:RIVERHOG_DOWNLOAD_FILE_WINDOW` |
| `configuration-environment:RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS` | 1 | `configuration-environment:RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS` |
| `configuration-environment:RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE` | 1 | `configuration-environment:RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE` |
| `configuration-environment:RIVERHOG_EVENT_CONTEXT_RETENTION` | 1 | `configuration-environment:RIVERHOG_EVENT_CONTEXT_RETENTION` |
| `configuration-environment:RIVERHOG_EVENT_SOURCE` | 1 | `configuration-environment:RIVERHOG_EVENT_SOURCE` |
| `configuration-environment:RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP` | 1 | `configuration-environment:RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP` |
| `configuration-environment:RIVERHOG_FTP_ADAPTER_BASE_URL` | 1 | `configuration-environment:RIVERHOG_FTP_ADAPTER_BASE_URL` |
| `configuration-environment:RIVERHOG_FTP_ADAPTER_CONFIG` | 1 | `configuration-environment:RIVERHOG_FTP_ADAPTER_CONFIG` |
| `configuration-environment:RIVERHOG_FTP_ADAPTER_HTTP2` | 1 | `configuration-environment:RIVERHOG_FTP_ADAPTER_HTTP2` |
| `configuration-environment:RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS` | 1 | `configuration-environment:RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS` |
| `configuration-environment:RIVERHOG_FTP_ADAPTER_TOKEN` | 1 | `configuration-environment:RIVERHOG_FTP_ADAPTER_TOKEN` |
| `configuration-environment:RIVERHOG_HOST_HEADER` | 1 | `configuration-environment:RIVERHOG_HOST_HEADER` |
| `configuration-environment:RIVERHOG_HTTP2` | 1 | `configuration-environment:RIVERHOG_HTTP2` |
| `configuration-environment:RIVERHOG_HTTP_TIMEOUT_SECONDS` | 1 | `configuration-environment:RIVERHOG_HTTP_TIMEOUT_SECONDS` |
| `configuration-environment:RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES` | 1 | `configuration-environment:RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES` |
| `configuration-environment:RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES` | 1 | `configuration-environment:RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES` |
| `configuration-environment:RIVERHOG_LOG_LEVEL` | 1 | `configuration-environment:RIVERHOG_LOG_LEVEL` |
| `configuration-environment:RIVERHOG_PACK_FILES` | 1 | `configuration-environment:RIVERHOG_PACK_FILES` |
| `configuration-environment:RIVERHOG_PACK_MEMBER_BYTES` | 1 | `configuration-environment:RIVERHOG_PACK_MEMBER_BYTES` |
| `configuration-environment:RIVERHOG_PACK_SOURCE_BYTES` | 1 | `configuration-environment:RIVERHOG_PACK_SOURCE_BYTES` |
| `configuration-environment:RIVERHOG_PROVENANCE_STATE_HOME` | 1 | `configuration-environment:RIVERHOG_PROVENANCE_STATE_HOME` |
| `configuration-environment:RIVERHOG_PUBLIC_BASE_URL` | 1 | `configuration-environment:RIVERHOG_PUBLIC_BASE_URL` |
| `configuration-environment:RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES` | 1 | `configuration-environment:RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES` |
| `configuration-environment:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_ENABLED` | 1 | `configuration-environment:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_ENABLED` |
| `configuration-environment:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE` | 1 | `configuration-environment:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE` |
| `configuration-environment:RIVERHOG_RETRIEVAL_CACHE_STORES` | 1 | `configuration-environment:RIVERHOG_RETRIEVAL_CACHE_STORES` |
| `configuration-environment:RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL` | 1 | `configuration-environment:RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL` |
| `configuration-environment:RIVERHOG_RETRIEVAL_CACHE_WRITE_SEGMENT_BYTES` | 1 | `configuration-environment:RIVERHOG_RETRIEVAL_CACHE_WRITE_SEGMENT_BYTES` |
| `configuration-environment:RIVERHOG_RETRIEVAL_DEFAULT_LEASE` | 1 | `configuration-environment:RIVERHOG_RETRIEVAL_DEFAULT_LEASE` |
| `configuration-environment:RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY` | 1 | `configuration-environment:RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY` |
| `configuration-environment:RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES` | 1 | `configuration-environment:RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES` |
| `configuration-environment:RIVERHOG_RETRIEVAL_MAX_LEASE` | 1 | `configuration-environment:RIVERHOG_RETRIEVAL_MAX_LEASE` |
| `configuration-environment:RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES` | 1 | `configuration-environment:RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES` |
| `configuration-environment:RIVERHOG_RETRIEVAL_PENDING_TIMEOUT` | 1 | `configuration-environment:RIVERHOG_RETRIEVAL_PENDING_TIMEOUT` |
| `configuration-environment:RIVERHOG_RETRIEVAL_RANGE_BILLING_MODE` | 1 | `configuration-environment:RIVERHOG_RETRIEVAL_RANGE_BILLING_MODE` |
| `configuration-environment:RIVERHOG_RETRIEVAL_RANGE_MERGE_GAP_BYTES` | 1 | `configuration-environment:RIVERHOG_RETRIEVAL_RANGE_MERGE_GAP_BYTES` |
| `configuration-environment:RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES` | 1 | `configuration-environment:RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES` |
| `configuration-environment:RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY` | 1 | `configuration-environment:RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY` |
| `configuration-environment:RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL` | 1 | `configuration-environment:RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL` |
| `configuration-environment:RIVERHOG_TOKEN` | 1 | `configuration-environment:RIVERHOG_TOKEN` |
| `configuration-environment:RIVERHOG_UPLOAD_FILE_CONCURRENCY` | 1 | `configuration-environment:RIVERHOG_UPLOAD_FILE_CONCURRENCY` |
| `configuration-environment:RIVERHOG_UPLOAD_FILE_WINDOW` | 1 | `configuration-environment:RIVERHOG_UPLOAD_FILE_WINDOW` |
| `configuration-environment:RIVERHOG_UPLOAD_TIMEOUT_SECONDS` | 1 | `configuration-environment:RIVERHOG_UPLOAD_TIMEOUT_SECONDS` |
| `configuration-environment:STOVE0_ADMISSIONS_PATH` | 1 | `configuration-environment:STOVE0_ADMISSIONS_PATH` |
| `configuration-environment:STOVE0_ALLOW_INSECURE_HTTP` | 1 | `configuration-environment:STOVE0_ALLOW_INSECURE_HTTP` |
| `configuration-environment:STOVE0_API_TOKEN` | 1 | `configuration-environment:STOVE0_API_TOKEN` |
| `configuration-environment:STOVE0_BASE_URL` | 1 | `configuration-environment:STOVE0_BASE_URL` |
| `configuration-environment:STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS` | 1 | `configuration-environment:STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS` |
| `configuration-environment:STOVE0_BROWSE_TOKEN_SIGNING_KEY` | 1 | `configuration-environment:STOVE0_BROWSE_TOKEN_SIGNING_KEY` |
| `configuration-environment:STOVE0_CAPABILITY_TTL_SECONDS` | 1 | `configuration-environment:STOVE0_CAPABILITY_TTL_SECONDS` |
| `configuration-environment:STOVE0_CLAIM_LEASE_SECONDS` | 1 | `configuration-environment:STOVE0_CLAIM_LEASE_SECONDS` |
| `configuration-environment:STOVE0_DATABASE_URL` | 1 | `configuration-environment:STOVE0_DATABASE_URL` |
| `configuration-environment:STOVE0_EXIFTOOL_BIN` | 1 | `configuration-environment:STOVE0_EXIFTOOL_BIN` |
| `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_HOST` | 1 | `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_HOST` |
| `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_IMAGE_DIGEST` | 1 | `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_IMAGE_DIGEST` |
| `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_PORT` | 1 | `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_PORT` |
| `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION` | 1 | `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION` |
| `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_TOKEN` | 1 | `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_TOKEN` |
| `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE` | 1 | `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE` |
| `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_WORKSPACE` | 1 | `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_WORKSPACE` |
| `configuration-environment:STOVE0_FFMPEG_BIN` | 1 | `configuration-environment:STOVE0_FFMPEG_BIN` |
| `configuration-environment:STOVE0_FFPROBE_BIN` | 1 | `configuration-environment:STOVE0_FFPROBE_BIN` |
| `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST` | 1 | `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST` |
| `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST` | 1 | `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST` |
| `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT` | 1 | `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT` |
| `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION` | 1 | `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION` |
| `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN` | 1 | `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN` |
| `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE` | 1 | `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE` |
| `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_WORKSPACE` | 1 | `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_WORKSPACE` |
| `configuration-environment:STOVE0_HTTP2` | 1 | `configuration-environment:STOVE0_HTTP2` |
| `configuration-environment:STOVE0_HTTP_TIMEOUT_SECONDS` | 1 | `configuration-environment:STOVE0_HTTP_TIMEOUT_SECONDS` |
| `configuration-environment:STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD` | 1 | `configuration-environment:STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD` |
| `configuration-environment:STOVE0_OBSERVERS_JSON` | 1 | `configuration-environment:STOVE0_OBSERVERS_JSON` |
| `configuration-environment:STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS` | 1 | `configuration-environment:STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS` |
| `configuration-environment:STOVE0_RECIPES_PATH` | 1 | `configuration-environment:STOVE0_RECIPES_PATH` |
| `configuration-environment:STOVE0_SCHEDULER_INTERVAL_SECONDS` | 1 | `configuration-environment:STOVE0_SCHEDULER_INTERVAL_SECONDS` |
| `configuration-environment:STOVE0_TARGETS_JSON` | 1 | `configuration-environment:STOVE0_TARGETS_JSON` |
| `configuration-environment:STOVE0_TARGET_AUTHORITY_BATCH_SIZE` | 1 | `configuration-environment:STOVE0_TARGET_AUTHORITY_BATCH_SIZE` |
| `configuration-environment:STOVE0_TARGET_CALLBACK_ALLOW_INSECURE_HTTP` | 1 | `configuration-environment:STOVE0_TARGET_CALLBACK_ALLOW_INSECURE_HTTP` |
| `configuration-environment:STOVE0_TARGET_CALLBACK_BASE_URL` | 1 | `configuration-environment:STOVE0_TARGET_CALLBACK_BASE_URL` |
| `configuration-environment:STOVE0_TARGET_CALLBACK_SIGNING_KEY` | 1 | `configuration-environment:STOVE0_TARGET_CALLBACK_SIGNING_KEY` |
| `configuration-environment:STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS` | 1 | `configuration-environment:STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS` |
| `configuration-environment:STOVE0_TOKEN` | 1 | `configuration-environment:STOVE0_TOKEN` |
| `configuration-environment:STOVE0_WORKSPACE_ASSURANCE` | 1 | `configuration-environment:STOVE0_WORKSPACE_ASSURANCE` |
| `configuration-environment:inventory` | 1 | `scripts/contract_freeze.py::_environment_inventory` |
| `configuration:gogurt-routes` | 1 | `reference/gogurt/packages/core/src/gogurt_core/__init__.py::<module>` |
| `configuration:mango-fish` | 1 | `reference/riverhog/applications/mango-fish/src/mango_fish/relay.py::MangoFishConfig` |
| `configuration:riverhog-ftp-adapter` | 1 | `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/config.py::FtpAdapterConfig` |
| `configuration:stove0-recipes` | 1 | `reference/stove0/packages/recipe-config/src/stove0_recipe_config/models.py::RecipeCatalog` |
| `configuration:stove0-review-target` | 1 | `reference/stove0/targets/review/support/src/stove0_review_target_support/app.py::ReviewTargetConfig` |
| `configuration:stove0-review-target-sampler` | 1 | `reference/stove0/targets/review/support/src/stove0_review_target_support/app.py::SamplerConfig` |
| `extent:extent-contract` | 15 | `scripts/extent_contract.py::extent_projection` |
| `generator:contract-projection` | 1208 | `scripts/contract_freeze.py::contract_projection` |
| `openapi:riverhog` | 362 | `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI` |
| `openapi:riverhog-ftp-adapter` | 12 | `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI` |
| `openapi:stove0` | 170 | `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI` |
| `operations:operation-matrix` | 147 | `scripts/operation_qualification.py::operation_matrix` |
| `protocol:generated:riverhog-storage-adapter` | 24 | `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle` |
| `protocol:generated:stove0-observer` | 8 | `reference/stove0/packages/observer-support/src/stove0_observer_support/schemas.py::observer_schema_bundle` |
| `protocol:generated:stove0-review-sampler` | 6 | `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/schemas.py::sampler_schema_bundle` |
| `protocol:generated:stove0-target` | 9 | `reference/stove0/packages/target-support/src/stove0_target_support/schemas.py::target_schema_bundle` |
| `protocol:https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json` | 1 | `packages/riverhog-provenance/src/riverhog_provenance/schemas/riverhog-provenance-v1-journal-entry.schema.json` |
| `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-attributes.json` | 1 | `reference/riverhog/provenance/contracts/macos/src/riverhog_provenance_macos_contracts/schemas/darwin-file-attributes.schema.json` |
| `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-flags.json` | 1 | `reference/riverhog/provenance/contracts/macos/src/riverhog_provenance_macos_contracts/schemas/darwin-file-flags.schema.json` |
| `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-stat.json` | 1 | `reference/riverhog/provenance/contracts/macos/src/riverhog_provenance_macos_contracts/schemas/darwin-file-stat.schema.json` |
| `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-file-stat.json` | 1 | `reference/riverhog/provenance/contracts/linux/src/riverhog_provenance_linux_contracts/schemas/linux-file-stat.schema.json` |
| `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fs-flags.json` | 1 | `reference/riverhog/provenance/contracts/linux/src/riverhog_provenance_linux_contracts/schemas/linux-fs-flags.schema.json` |
| `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fsxattr.json` | 1 | `reference/riverhog/provenance/contracts/linux/src/riverhog_provenance_linux_contracts/schemas/linux-fsxattr.schema.json` |
| `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-mount-context.json` | 1 | `reference/riverhog/provenance/contracts/linux/src/riverhog_provenance_linux_contracts/schemas/linux-mount-context.schema.json` |
| `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-statx-attributes.json` | 1 | `reference/riverhog/provenance/contracts/linux/src/riverhog_provenance_linux_contracts/schemas/linux-statx-attributes.schema.json` |
| `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/macos-volume-context.json` | 1 | `reference/riverhog/provenance/contracts/macos/src/riverhog_provenance_macos_contracts/schemas/macos-volume-context.schema.json` |
| `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/observation-policy.json` | 1 | `packages/riverhog-provenance/src/riverhog_provenance/schemas/observation-policy.schema.json` |
| `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/sparse-map.json` | 1 | `packages/riverhog-provenance/src/riverhog_provenance/schemas/sparse-map.schema.json` |
| `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-backup-stream-info.json` | 1 | `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-backup-stream-info.schema.json` |
| `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-compression-state.json` | 1 | `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-compression-state.schema.json` |
| `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-attributes.json` | 1 | `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-file-attributes.schema.json` |
| `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-stat.json` | 1 | `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-file-stat.schema.json` |
| `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-integrity-info.json` | 1 | `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-integrity-info.schema.json` |
| `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-object-id.json` | 1 | `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-object-id.schema.json` |
| `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-reparse-point.json` | 1 | `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-reparse-point.schema.json` |
| `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-security-descriptor.json` | 1 | `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-security-descriptor.schema.json` |
| `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-usn-record.json` | 1 | `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-usn-record.schema.json` |
| `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-volume-context.json` | 1 | `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-volume-context.schema.json` |
| `protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-manifest-v1.schema.json` | 1 | `packages/riverhog-archive-contracts/schemas/collection-archive-manifest-v1.schema.json` |
| `protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-terminal-v1.schema.json` | 1 | `packages/riverhog-archive-contracts/schemas/collection-archive-terminal-v1.schema.json` |
| `protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-volume-v1.schema.json` | 1 | `packages/riverhog-archive-contracts/schemas/collection-archive-volume-v1.schema.json` |
| `protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-collection-description-v1.schema.json` | 1 | `packages/riverhog-protocol/schemas/riverhog-collection-description-v1.schema.json` |
| `protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-bindings-v1.schema.json` | 1 | `packages/riverhog-provenance/src/riverhog_provenance/schemas/riverhog-provenance-bindings-v1.schema.json` |
| `protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-root-v1.schema.json` | 1 | `packages/riverhog-provenance/src/riverhog_provenance/schemas/riverhog-provenance-root-v1.schema.json` |
| `protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-terminal-v1.schema.json` | 1 | `packages/riverhog-provenance/src/riverhog_provenance/schemas/riverhog-provenance-terminal-v1.schema.json` |
| `protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-volume-v1.schema.json` | 1 | `packages/riverhog-provenance/src/riverhog_provenance/schemas/riverhog-provenance-volume-v1.schema.json` |
| `protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-recovery-descriptor-v1.schema.json` | 1 | `packages/riverhog-archive-contracts/schemas/riverhog-recovery-descriptor-v1.schema.json` |
| `python:gogurt-core` | 1 | `reference/gogurt/packages/core/src/gogurt_core/__init__.py::<module>` |
| `python:gogurt-listener-runtime` | 1 | `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py::<module>` |
| `python:http-api-contracts` | 1 | `packages/http-api-contracts/src/http_api_contracts/__init__.py::<module>` |
| `python:lifecycle-events` | 1 | `packages/lifecycle-events/src/lifecycle_events/__init__.py::<module>` |
| `python:riverhog-age` | 1 | `packages/riverhog-age/src/riverhog_age/__init__.py::<module>` |
| `python:riverhog-application-access` | 1 | `packages/riverhog-application-access/src/riverhog_application_access/__init__.py::<module>` |
| `python:riverhog-archive-contracts` | 1 | `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py::<module>` |
| `python:riverhog-client:riverhog_client` | 1 | `packages/riverhog-client/src/riverhog_client/__init__.py::<module>` |
| `python:riverhog-client:riverhog_client.transform` | 1 | `packages/riverhog-client/src/riverhog_client/transform/__init__.py::<module>` |
| `python:riverhog-protocol` | 1 | `packages/riverhog-protocol/src/riverhog_protocol/__init__.py::<module>` |
| `python:riverhog-provenance` | 1 | `packages/riverhog-provenance/src/riverhog_provenance/__init__.py::<module>` |
| `python:riverhog-provenance-contracts` | 1 | `packages/riverhog-provenance-contracts/src/riverhog_provenance_contracts/__init__.py::<module>` |
| `python:riverhog-storage-adapter-asgi-support` | 1 | `packages/riverhog-storage-adapter-asgi-support/src/riverhog_storage_adapter_asgi_support/__init__.py::<module>` |
| `python:riverhog-storage-adapter-protocol` | 1 | `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py::<module>` |
| `python:riverhog-storage-adapter-support` | 1 | `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py::<module>` |
| `python:stove0-api-client` | 1 | `reference/stove0/packages/api-client/src/stove0_api_client/__init__.py::<module>` |
| `python:stove0-observer-client` | 1 | `reference/stove0/packages/observer-client/src/stove0_observer_client/__init__.py::<module>` |
| `python:stove0-observer-protocol` | 1 | `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py::<module>` |
| `python:stove0-observer-support` | 1 | `reference/stove0/packages/observer-support/src/stove0_observer_support/__init__.py::<module>` |
| `python:stove0-operator-contracts` | 1 | `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py::<module>` |
| `python:stove0-protocol` | 1 | `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py::<module>` |
| `python:stove0-recipe-config` | 1 | `reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py::<module>` |
| `python:stove0-target-client` | 1 | `reference/stove0/packages/target-client/src/stove0_target_client/__init__.py::<module>` |
| `python:stove0-target-protocol` | 1 | `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py::<module>` |
| `python:stove0-target-support` | 1 | `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py::<module>` |
| `release:release.toml` | 110 | `release.toml` |
| `state:gogurt-listener` | 1 | `state:gogurt-listener` |
| `state:mango-fish-cursor` | 1 | `state:mango-fish-cursor` |
| `state:piggity-local` | 1 | `state:piggity-local` |
| `state:riverhog-catalog` | 1 | `state:riverhog-catalog` |
| `state:riverhog-ftp-custody` | 1 | `state:riverhog-ftp-custody` |
| `state:riverhog-provenance-installation` | 1 | `state:riverhog-provenance-installation` |
| `state:stove0-control` | 1 | `state:stove0-control` |
| `state:stove0-target-jobs` | 1 | `state:stove0-target-jobs` |
