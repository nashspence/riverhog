"""One validated operator document for the Riverhog server."""

from __future__ import annotations

import json
import os
from importlib import resources
from pathlib import Path
from typing import Literal

from config_validation import load_validated_yaml_config, read_secret_file
from pydantic import BaseModel, ConfigDict, Field
from time_formats import parse_duration

from riverhog_core.collection_plan import CollectionVolumePolicy
from riverhog_core.pack_retrieval import PackRangeRetrievalPolicy
from riverhog_core.runtime_config import (
    DEFAULT_ARCHIVE_SCRYPT_WORK_FACTOR,
    DEFAULT_RETRIEVAL_CACHE_WRITE_SEGMENT_BYTES,
    DEFAULT_STORAGE_ADAPTER_MAX_CONNECTIONS,
    DEFAULT_STORAGE_ADAPTER_TIMEOUT_SECONDS,
    RetrievalCacheStoreRegistration,
    RuntimeConfig,
    StorageAdapterRegistration,
    _parse_bytes,
)
from riverhog_core.throughput import ArchiveThroughputTuning

_VOLUME = CollectionVolumePolicy()
_THROUGHPUT = ArchiveThroughputTuning()
_RANGE = PackRangeRetrievalPolicy()


class _Document(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class AdapterDocument(_Document):
    base_url: str = Field(min_length=1)
    token_file: Path
    allow_insecure_http: bool = False
    maximum_connections: int = Field(default=DEFAULT_STORAGE_ADAPTER_MAX_CONNECTIONS, ge=1)
    timeout_seconds: float = Field(default=DEFAULT_STORAGE_ADAPTER_TIMEOUT_SECONDS, gt=0)


class ArchiveStoreDocument(AdapterDocument):
    monthly_download_allowance_bytes: str | None = None
    download_safety_buffer_bytes: str = "0B"


class CacheStoreDocument(AdapterDocument):
    admission_enabled: bool = True
    admission_budget_bytes: str | None = None


class VolumePolicyDocument(_Document):
    pack_source_bytes: str = str(_VOLUME.pack_source_bytes)
    pack_files: int = Field(default=_VOLUME.pack_files, ge=1)
    pack_member_bytes: str = str(_VOLUME.pack_member_bytes)
    pack_part_plaintext_bytes: str = str(_VOLUME.pack_part_plaintext_bytes)
    raw_volume_plaintext_bytes: str = str(_VOLUME.raw_volume_plaintext_bytes)
    raw_part_plaintext_bytes: str = str(_VOLUME.raw_part_plaintext_bytes)

    def policy(self) -> CollectionVolumePolicy:
        return CollectionVolumePolicy(
            pack_source_bytes=_bytes(self.pack_source_bytes, "volume_policy.pack_source_bytes", 1),
            pack_files=self.pack_files,
            pack_member_bytes=_bytes(self.pack_member_bytes, "volume_policy.pack_member_bytes", 1),
            pack_part_plaintext_bytes=_bytes(
                self.pack_part_plaintext_bytes, "volume_policy.pack_part_plaintext_bytes", 1
            ),
            raw_volume_plaintext_bytes=_bytes(
                self.raw_volume_plaintext_bytes, "volume_policy.raw_volume_plaintext_bytes", 1
            ),
            raw_part_plaintext_bytes=_bytes(
                self.raw_part_plaintext_bytes, "volume_policy.raw_part_plaintext_bytes", 1
            ),
        )


class ThroughputDocument(_Document):
    upload_prepare_concurrency: int = Field(default=_THROUGHPUT.upload_prepare_concurrency, ge=1)
    write_concurrency: int = Field(default=_THROUGHPUT.write_concurrency, ge=1)
    upload_request_concurrency: int = Field(default=_THROUGHPUT.upload_request_concurrency, ge=1)
    upload_max_inflight_bytes: str = str(_THROUGHPUT.upload_max_inflight_bytes)
    source_read_chunk_bytes: str = str(_THROUGHPUT.source_read_chunk_bytes)
    retrieval_request_concurrency: int = Field(
        default=_THROUGHPUT.retrieval_request_concurrency, ge=1
    )
    retrieval_max_inflight_bytes: str = str(_THROUGHPUT.retrieval_max_inflight_bytes)
    retrieval_read_chunk_bytes: str = str(_THROUGHPUT.retrieval_read_chunk_bytes)
    age_session_cache_entries: int = Field(default=_THROUGHPUT.age_session_cache_entries, ge=0)
    age_derivation_concurrency: int = Field(default=_THROUGHPUT.age_derivation_concurrency, ge=1)

    def tuning(self) -> ArchiveThroughputTuning:
        return ArchiveThroughputTuning(
            upload_prepare_concurrency=self.upload_prepare_concurrency,
            write_concurrency=self.write_concurrency,
            upload_request_concurrency=self.upload_request_concurrency,
            upload_max_inflight_bytes=_bytes(
                self.upload_max_inflight_bytes, "throughput.upload_max_inflight_bytes", 1
            ),
            source_read_chunk_bytes=_bytes(
                self.source_read_chunk_bytes, "throughput.source_read_chunk_bytes", 1
            ),
            retrieval_request_concurrency=self.retrieval_request_concurrency,
            retrieval_max_inflight_bytes=_bytes(
                self.retrieval_max_inflight_bytes, "throughput.retrieval_max_inflight_bytes", 1
            ),
            retrieval_read_chunk_bytes=_bytes(
                self.retrieval_read_chunk_bytes, "throughput.retrieval_read_chunk_bytes", 1
            ),
            age_session_cache_entries=self.age_session_cache_entries,
            age_derivation_concurrency=self.age_derivation_concurrency,
        )


class RangePolicyDocument(_Document):
    merge_gap_ciphertext_bytes: str = str(_RANGE.merge_gap_ciphertext_bytes)
    max_request_ciphertext_bytes: str = str(_RANGE.max_request_ciphertext_bytes)
    billing_mode: Literal["returned_bytes", "whole_object"] = "returned_bytes"

    def policy(self) -> PackRangeRetrievalPolicy:
        return PackRangeRetrievalPolicy(
            merge_gap_ciphertext_bytes=_bytes(
                self.merge_gap_ciphertext_bytes, "range_policy.merge_gap_ciphertext_bytes", 0
            ),
            max_request_ciphertext_bytes=_bytes(
                self.max_request_ciphertext_bytes, "range_policy.max_request_ciphertext_bytes", 1
            ),
            billing_mode=self.billing_mode,
        )


class RiverhogDocument(_Document):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        json_schema_extra={
            "$id": "https://nashspence.github.io/riverhog/v1/config/riverhog-server.schema.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
        },
    )

    database_url_file: Path
    bootstrap_token_file: Path
    browse_token_signing_key_file: Path
    archive_passphrase_files: dict[str, Path] = Field(min_length=1)
    archive_active_passphrase_id: str = Field(min_length=1)
    archive_write_store: str = Field(min_length=1)
    archive_stores: dict[str, ArchiveStoreDocument] = Field(min_length=1)
    archive_read_order: list[str] = Field(default_factory=list)
    retrieval_cache_stores: dict[str, CacheStoreDocument] = Field(default_factory=dict)
    log_level: str = "INFO"
    public_base_url: str | None = None
    archive_scrypt_work_factor: int = Field(default=DEFAULT_ARCHIVE_SCRYPT_WORK_FACTOR, ge=1, le=22)
    archive_upload_sweep_interval: str = "30s"
    collection_upload_custody_lease: str = "1h"
    retrieval_restore_poll_interval: str = "5m"
    retrieval_estimated_latency: str = "48h"
    retrieval_cache_write_segment_bytes: str = str(DEFAULT_RETRIEVAL_CACHE_WRITE_SEGMENT_BYTES)
    retrieval_cache_new_archive_enabled: bool = True
    retrieval_cache_new_archive_lease: str = "72h"
    retrieval_default_lease: str = "24h"
    retrieval_max_lease: str = "7d"
    retrieval_pending_timeout: str = "72h"
    retrieval_cache_sweep_interval: str = "5m"
    event_context_retention: str = "30d"
    event_context_reap_batch_size: int = Field(default=100, ge=1)
    browse_token_lifetime: str = "24h"
    catalog_sync_bootstrap_lifetime: str = "7d"
    catalog_sync_cursor_lifetime: str = "24h"
    catalog_sync_history_retention: str = "30d"
    catalog_sync_page_size_max: int = Field(default=100, ge=1, le=100)
    catalog_sync_history_reap_batch_size: int = Field(default=100, ge=1)
    volume_policy: VolumePolicyDocument = Field(default_factory=VolumePolicyDocument)
    throughput: ThroughputDocument = Field(default_factory=ThroughputDocument)
    range_policy: RangePolicyDocument = Field(default_factory=RangePolicyDocument)
    range_policy_by_store: dict[str, RangePolicyDocument] = Field(default_factory=dict)


def generated_config_schema() -> dict[str, object]:
    return RiverhogDocument.model_json_schema()


def _bytes(value: str, name: str, minimum: int) -> int:
    return _parse_bytes(value, name=name, minimum=minimum)


def _adapter(name: str, document: AdapterDocument) -> StorageAdapterRegistration:
    read_secret_file(document.token_file, label=f"storage {name} token_file")
    return StorageAdapterRegistration(
        name=name,
        base_url=document.base_url,
        token_file=document.token_file,
        allow_insecure_http=document.allow_insecure_http,
        maximum_connections=document.maximum_connections,
        timeout_seconds=document.timeout_seconds,
        monthly_download_allowance_bytes=(
            _bytes(
                document.monthly_download_allowance_bytes,
                f"archive_stores.{name}.monthly_download_allowance_bytes",
                1,
            )
            if isinstance(document, ArchiveStoreDocument)
            and document.monthly_download_allowance_bytes is not None
            else None
        ),
        download_safety_buffer_bytes=(
            _bytes(
                document.download_safety_buffer_bytes,
                f"archive_stores.{name}.download_safety_buffer_bytes",
                0,
            )
            if isinstance(document, ArchiveStoreDocument)
            else 0
        ),
    )


def load_runtime_document(path: Path) -> RuntimeConfig:
    schema = json.loads(resources.files("riverhog_core").joinpath("config.schema.json").read_text())
    document = RiverhogDocument.model_validate(load_validated_yaml_config(path, schema))
    archive_stores = {
        name: _adapter(name, store) for name, store in document.archive_stores.items()
    }
    cache_stores = {
        name: RetrievalCacheStoreRegistration(
            name=name,
            adapter=_adapter(name, store),
            admission_enabled=store.admission_enabled,
            admission_budget_bytes=(
                _bytes(
                    store.admission_budget_bytes,
                    f"retrieval_cache_stores.{name}.admission_budget_bytes",
                    1,
                )
                if store.admission_budget_bytes is not None
                else None
            ),
        )
        for name, store in document.retrieval_cache_stores.items()
    }
    database_url = read_secret_file(document.database_url_file, label="database_url_file")
    if database_url.strip().split(":", 1)[0].split("+", 1)[0] != "postgresql":
        raise ValueError("Riverhog database URL must use postgresql")
    return RuntimeConfig(
        database_url=database_url,
        bootstrap_token=read_secret_file(
            document.bootstrap_token_file, label="bootstrap_token_file"
        ),
        browse_token_signing_key=read_secret_file(
            document.browse_token_signing_key_file, label="browse_token_signing_key_file"
        ),
        archive_passphrases={
            name: read_secret_file(secret_path, label=f"archive_passphrase_files.{name}")
            for name, secret_path in document.archive_passphrase_files.items()
        },
        archive_active_passphrase_id=document.archive_active_passphrase_id,
        archive_write_store=document.archive_write_store,
        archive_read_order=tuple(document.archive_read_order or document.archive_stores),
        archive_stores=archive_stores,
        retrieval_cache_stores=cache_stores,
        log_level=document.log_level,
        public_base_url=document.public_base_url,
        archive_scrypt_work_factor=document.archive_scrypt_work_factor,
        archive_upload_sweep_interval=parse_duration(document.archive_upload_sweep_interval),
        collection_upload_custody_lease=parse_duration(document.collection_upload_custody_lease),
        retrieval_restore_poll_interval=parse_duration(document.retrieval_restore_poll_interval),
        retrieval_estimated_latency=parse_duration(document.retrieval_estimated_latency),
        retrieval_cache_write_segment_bytes=_bytes(
            document.retrieval_cache_write_segment_bytes, "retrieval_cache_write_segment_bytes", 1
        ),
        retrieval_cache_new_archive_enabled=document.retrieval_cache_new_archive_enabled,
        retrieval_cache_new_archive_lease=parse_duration(
            document.retrieval_cache_new_archive_lease
        ),
        retrieval_default_lease=parse_duration(document.retrieval_default_lease),
        retrieval_max_lease=parse_duration(document.retrieval_max_lease),
        retrieval_pending_timeout=parse_duration(document.retrieval_pending_timeout),
        retrieval_cache_sweep_interval=parse_duration(document.retrieval_cache_sweep_interval),
        event_context_retention=parse_duration(document.event_context_retention),
        event_context_reap_batch_size=document.event_context_reap_batch_size,
        browse_token_lifetime=parse_duration(document.browse_token_lifetime),
        catalog_sync_bootstrap_lifetime=parse_duration(document.catalog_sync_bootstrap_lifetime),
        catalog_sync_cursor_lifetime=parse_duration(document.catalog_sync_cursor_lifetime),
        catalog_sync_history_retention=parse_duration(document.catalog_sync_history_retention),
        catalog_sync_page_size_max=document.catalog_sync_page_size_max,
        catalog_sync_history_reap_batch_size=document.catalog_sync_history_reap_batch_size,
        volume_policy=document.volume_policy.policy(),
        throughput_tuning=document.throughput.tuning(),
        range_policy=document.range_policy.policy(),
        range_policy_by_store={
            name: policy.policy() for name, policy in document.range_policy_by_store.items()
        },
    )


def database_url_from_document(path: Path) -> str:
    schema = json.loads(resources.files("riverhog_core").joinpath("config.schema.json").read_text())
    document = RiverhogDocument.model_validate(load_validated_yaml_config(path, schema))
    value = read_secret_file(document.database_url_file, label="database_url_file")
    if value.strip().split(":", 1)[0].split("+", 1)[0] != "postgresql":
        raise ValueError("Riverhog database URL must use postgresql")
    return value


def load_runtime_config() -> RuntimeConfig:
    config_path = os.environ.get("RIVERHOG_CONFIG", "").strip()
    if not config_path:
        raise ValueError("RIVERHOG_CONFIG must name a YAML configuration document")
    return load_runtime_document(Path(config_path))
