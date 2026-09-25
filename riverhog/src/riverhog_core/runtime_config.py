from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import timedelta
from pathlib import Path
from typing import Any, Self
from urllib.parse import urlsplit

from http_api_contracts import safe_http_base_url
from riverhog_archive_contracts import (
    ARCHIVE_ENCRYPTION_FORMAT,
    CollectionEncryptionBinding,
    normalize_passphrase_id,
)
from riverhog_protocol import CATALOG_SYNC_PAGE_SIZE_MAX

from riverhog_core.collection_plan import CollectionVolumePolicy
from riverhog_core.pack_retrieval import PackRangeRetrievalPolicy
from riverhog_core.throughput import ArchiveThroughputTuning

_BYTES_RE = re.compile(r"^(\d+(?:_\d+)*)([kmgt]i?b?|b)?$", re.IGNORECASE)
TEST_ARCHIVE_PASSPHRASE = "riverhog-test-archive-passphrase"
TEST_ARCHIVE_PASSPHRASE_ID = "riverhog-test-key-v1"
TEST_BROWSE_TOKEN_SIGNING_KEY = "riverhog-test-browse-token-signing-key-v1"
DEFAULT_DATABASE_URL = "postgresql+psycopg://riverhog:riverhog@127.0.0.1:5432/riverhog"
DEFAULT_RETRIEVAL_CACHE_WRITE_SEGMENT_BYTES = 64 * 1024 * 1024
DEFAULT_STORAGE_ADAPTER_MAX_CONNECTIONS = 32
DEFAULT_STORAGE_ADAPTER_TIMEOUT_SECONDS = 300.0
DEFAULT_ARCHIVE_SCRYPT_WORK_FACTOR = 18
DEFAULT_LOG_LEVEL = "INFO"


def _parse_bytes(value: str, *, name: str, minimum: int = 0) -> int:
    raw = value.strip().replace(" ", "")
    match = _BYTES_RE.fullmatch(raw)
    if match is None:
        raise ValueError(f"invalid {name} {value!r}: expected bytes like '500GB' or '536870912000'")
    amount = int(match.group(1).replace("_", ""))
    unit = (match.group(2) or "b").casefold()
    scale = {
        "b": 1,
        "kb": 1_000,
        "k": 1_000,
        "mb": 1_000_000,
        "m": 1_000_000,
        "gb": 1_000_000_000,
        "g": 1_000_000_000,
        "tb": 1_000_000_000_000,
        "t": 1_000_000_000_000,
        "kib": 1024,
        "mib": 1024**2,
        "gib": 1024**3,
        "tib": 1024**4,
    }[unit]
    parsed = amount * scale
    if parsed < minimum:
        raise ValueError(f"invalid {name} {value!r}: expected >= {minimum}")
    return parsed


def _normalize_archive_store_name(value: str) -> str:
    name = value.strip().casefold()
    if not name or not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", name):
        raise ValueError(
            f"invalid archive store name {value!r}: expected lowercase letters, digits, and dashes"
        )
    return name


@dataclass(frozen=True, slots=True)
class StorageAdapterRegistration:
    name: str
    base_url: str
    token_file: Path
    allow_insecure_http: bool = False
    maximum_connections: int = DEFAULT_STORAGE_ADAPTER_MAX_CONNECTIONS
    timeout_seconds: float = DEFAULT_STORAGE_ADAPTER_TIMEOUT_SECONDS
    monthly_download_allowance_bytes: int | None = None
    download_safety_buffer_bytes: int = 0


@dataclass(frozen=True, slots=True)
class RetrievalCacheStoreRegistration:
    name: str
    adapter: StorageAdapterRegistration
    admission_enabled: bool = True
    admission_budget_bytes: int | None = None


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    database_url: str = ""
    log_level: str = DEFAULT_LOG_LEVEL
    archive_write_store: str = "archive"
    archive_read_order: tuple[str, ...] = ("archive",)
    archive_stores: Mapping[str, StorageAdapterRegistration] = field(
        default_factory=lambda: {
            "archive": StorageAdapterRegistration(
                name="archive",
                base_url="http://127.0.0.1:9081",
                token_file=Path("/run/secrets/riverhog_archive_adapter_token"),
            )
        }
    )
    retrieval_cache_write_segment_bytes: int = DEFAULT_RETRIEVAL_CACHE_WRITE_SEGMENT_BYTES
    retrieval_cache_stores: Mapping[str, RetrievalCacheStoreRegistration] = field(
        default_factory=dict
    )
    retrieval_cache_new_archive_enabled: bool = True
    retrieval_cache_new_archive_lease: timedelta = field(
        default_factory=lambda: timedelta(hours=72)
    )
    retrieval_default_lease: timedelta = field(default_factory=lambda: timedelta(hours=24))
    retrieval_max_lease: timedelta = field(default_factory=lambda: timedelta(days=7))
    retrieval_pending_timeout: timedelta = field(default_factory=lambda: timedelta(hours=72))
    retrieval_cache_sweep_interval: timedelta = field(default_factory=lambda: timedelta(minutes=5))
    archive_passphrases: Mapping[str, str] = field(default_factory=dict, repr=False)
    archive_active_passphrase_id: str = ""
    archive_scrypt_work_factor: int = DEFAULT_ARCHIVE_SCRYPT_WORK_FACTOR
    archive_upload_sweep_interval: timedelta = field(default_factory=lambda: timedelta(seconds=30))
    collection_upload_custody_lease: timedelta = field(default_factory=lambda: timedelta(hours=1))
    retrieval_restore_poll_interval: timedelta = field(default_factory=lambda: timedelta(minutes=5))
    retrieval_estimated_latency: timedelta = field(default_factory=lambda: timedelta(hours=48))
    public_base_url: str | None = None
    event_context_retention: timedelta = field(default_factory=lambda: timedelta(days=30))
    event_context_reap_batch_size: int = 100
    browse_token_signing_key: str = field(default="", repr=False)
    browse_token_lifetime: timedelta = field(default_factory=lambda: timedelta(hours=24))
    catalog_sync_bootstrap_lifetime: timedelta = field(default_factory=lambda: timedelta(days=7))
    catalog_sync_cursor_lifetime: timedelta = field(default_factory=lambda: timedelta(hours=24))
    catalog_sync_history_retention: timedelta = field(default_factory=lambda: timedelta(days=30))
    catalog_sync_page_size_max: int = CATALOG_SYNC_PAGE_SIZE_MAX
    catalog_sync_history_reap_batch_size: int = 100
    bootstrap_token: str = field(default="", repr=False)
    volume_policy: CollectionVolumePolicy = field(default_factory=CollectionVolumePolicy)
    throughput_tuning: ArchiveThroughputTuning = field(default_factory=ArchiveThroughputTuning)
    range_policy: PackRangeRetrievalPolicy = field(default_factory=PackRangeRetrievalPolicy)
    range_policy_by_store: Mapping[str, PackRangeRetrievalPolicy] = field(default_factory=dict)

    def range_policy_for_store(self, name: str) -> PackRangeRetrievalPolicy:
        return self.range_policy_by_store.get(name, self.range_policy)

    @classmethod
    def for_testing(cls, **values: Any) -> Self:
        """Construct an explicitly selected configuration with dummy test secrets."""

        values.setdefault(
            "archive_passphrases",
            {TEST_ARCHIVE_PASSPHRASE_ID: TEST_ARCHIVE_PASSPHRASE},
        )
        values.setdefault("archive_active_passphrase_id", TEST_ARCHIVE_PASSPHRASE_ID)
        values.setdefault("browse_token_signing_key", TEST_BROWSE_TOKEN_SIGNING_KEY)
        return cls(**values)

    def __post_init__(self) -> None:
        if len(self.browse_token_signing_key.encode("utf-8")) < 32:
            raise ValueError("browse_token_signing_key_file must contain at least 32 bytes")
        if self.browse_token_lifetime.total_seconds() < 1:
            raise ValueError("browse_token_lifetime must be positive")
        if self.catalog_sync_history_retention.total_seconds() <= 0:
            raise ValueError("catalog_sync_history_retention must be positive")
        for name, lifetime in (
            ("browse_token_lifetime", self.browse_token_lifetime),
            ("catalog_sync_bootstrap_lifetime", self.catalog_sync_bootstrap_lifetime),
            ("catalog_sync_cursor_lifetime", self.catalog_sync_cursor_lifetime),
        ):
            if lifetime.total_seconds() <= 0:
                raise ValueError(f"{name} must be positive")
            if lifetime > self.catalog_sync_history_retention:
                raise ValueError(f"{name} must not exceed catalog synchronization retention")
        if not 1 <= self.catalog_sync_page_size_max <= CATALOG_SYNC_PAGE_SIZE_MAX:
            raise ValueError("catalog_sync_page_size_max must be within the v1 wire bound")
        if self.catalog_sync_history_reap_batch_size < 1:
            raise ValueError("catalog_sync_history_reap_batch_size must be positive")
        if self.event_context_retention.total_seconds() <= 0:
            raise ValueError("event_context_retention must be > 0")
        if self.event_context_reap_batch_size < 1:
            raise ValueError("event_context_reap_batch_size must be positive")
        if self.collection_upload_custody_lease.total_seconds() <= 0:
            raise ValueError("collection_upload_custody_lease must be > 0")
        if not self.database_url:
            object.__setattr__(self, "database_url", DEFAULT_DATABASE_URL)
        log_level = self.log_level.strip().upper()
        if log_level not in {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}:
            raise ValueError("log_level must be one of CRITICAL, ERROR, WARNING, INFO, or DEBUG")
        object.__setattr__(self, "log_level", log_level)
        if self.public_base_url is not None:
            public_base_url = self.public_base_url.strip().rstrip("/")
            parsed_public_base_url = urlsplit(public_base_url)
            if (
                parsed_public_base_url.scheme not in {"http", "https"}
                or not parsed_public_base_url.hostname
                or parsed_public_base_url.username is not None
                or parsed_public_base_url.password is not None
                or parsed_public_base_url.query
                or parsed_public_base_url.fragment
            ):
                raise ValueError(
                    "public_base_url must be an HTTP(S) URL without credentials, query, or fragment"
                )
            object.__setattr__(self, "public_base_url", public_base_url)
        archive_write_store = _normalize_archive_store_name(self.archive_write_store)
        normalized_archive_stores: dict[str, StorageAdapterRegistration] = {}
        for raw_name, store in self.archive_stores.items():
            name = _normalize_archive_store_name(raw_name)
            if store.name != name:
                raise ValueError(f"archive store mapping key must match its name: {raw_name!r}")
            store = replace(
                store,
                base_url=safe_http_base_url(
                    store.base_url,
                    setting=f"archive store {name} adapter URL",
                    allow_insecure_http=store.allow_insecure_http,
                ),
            )
            if not store.token_file.is_absolute():
                raise ValueError(f"archive store {name} adapter token file must be absolute")
            if store.maximum_connections < 1:
                raise ValueError(
                    f"archive store {name} adapter maximum connections must be positive"
                )
            if store.timeout_seconds <= 0:
                raise ValueError(f"archive store {name} adapter timeout must be positive")
            if store.monthly_download_allowance_bytes is not None:
                if store.monthly_download_allowance_bytes <= 0:
                    raise ValueError(
                        f"archive store {name} monthly download allowance must be positive"
                    )
                if store.download_safety_buffer_bytes >= store.monthly_download_allowance_bytes:
                    raise ValueError(
                        f"archive store {name} download safety buffer must be smaller than "
                        "its monthly download allowance"
                    )
            elif store.download_safety_buffer_bytes != 0:
                raise ValueError(
                    f"archive store {name} download safety buffer requires a monthly "
                    "download allowance"
                )
            normalized_archive_stores[name] = store
        metered_sources: dict[tuple[str, ...], list[str]] = {}
        for name, store in normalized_archive_stores.items():
            metered_sources.setdefault((store.base_url.casefold(),), []).append(name)
        duplicate_metered_sources = [
            names
            for names in metered_sources.values()
            if len(names) > 1
            and any(
                normalized_archive_stores[name].monthly_download_allowance_bytes is not None
                for name in names
            )
        ]
        if duplicate_metered_sources:
            aliases = ", ".join(sorted(duplicate_metered_sources[0]))
            raise ValueError(
                "a metered archive download source must have one store name; "
                f"duplicate aliases: {aliases}"
            )
        if archive_write_store not in normalized_archive_stores:
            raise ValueError(f"archive write store is not configured: {archive_write_store}")
        object.__setattr__(self, "archive_write_store", archive_write_store)
        read_order = tuple(
            dict.fromkeys(_normalize_archive_store_name(name) for name in self.archive_read_order)
        )
        unknown_read_stores = set(read_order) - set(normalized_archive_stores)
        if unknown_read_stores:
            raise ValueError(
                "archive read order contains unconfigured stores: "
                f"{', '.join(sorted(unknown_read_stores))}"
            )
        object.__setattr__(
            self,
            "archive_read_order",
            (*read_order, *[name for name in normalized_archive_stores if name not in read_order]),
        )
        object.__setattr__(self, "archive_stores", normalized_archive_stores)
        unknown_range_stores = set(self.range_policy_by_store) - set(normalized_archive_stores)
        if unknown_range_stores:
            raise ValueError(
                "range policy names unconfigured archive stores: "
                + ", ".join(sorted(unknown_range_stores))
            )
        normalized_cache_stores: dict[str, RetrievalCacheStoreRegistration] = {}
        for raw_name, registration in self.retrieval_cache_stores.items():
            name = _normalize_archive_store_name(raw_name)
            if registration.name != name or registration.adapter.name != name:
                raise ValueError(
                    f"retrieval cache store mapping key must match its name: {raw_name!r}"
                )
            cache = replace(
                registration.adapter,
                base_url=safe_http_base_url(
                    registration.adapter.base_url,
                    setting=f"retrieval cache store {name} adapter URL",
                    allow_insecure_http=registration.adapter.allow_insecure_http,
                ),
            )
            if not cache.token_file.is_absolute():
                raise ValueError(
                    f"retrieval cache store {name} adapter token file must be absolute"
                )
            if cache.maximum_connections < 1:
                raise ValueError(
                    f"retrieval cache store {name} adapter maximum connections must be positive"
                )
            if cache.timeout_seconds <= 0:
                raise ValueError(f"retrieval cache store {name} adapter timeout must be positive")
            if (
                cache.monthly_download_allowance_bytes is not None
                or cache.download_safety_buffer_bytes != 0
            ):
                raise ValueError(f"retrieval cache store {name} does not accept archive allowances")
            if (
                registration.admission_budget_bytes is not None
                and registration.admission_budget_bytes < 1
            ):
                raise ValueError(f"retrieval cache store {name} admission budget must be positive")
            normalized_cache_stores[name] = replace(registration, adapter=cache)
        object.__setattr__(self, "retrieval_cache_stores", normalized_cache_stores)
        overlapping_names = set(normalized_archive_stores) & set(normalized_cache_stores)
        if overlapping_names:
            raise ValueError(
                "archive and retrieval cache stores must have distinct names: "
                + ", ".join(sorted(overlapping_names))
            )
        if self.retrieval_cache_write_segment_bytes < 1:
            raise ValueError("retrieval_cache_write_segment_bytes must be >= 1")
        if self.retrieval_cache_new_archive_lease.total_seconds() <= 0:
            raise ValueError("retrieval_cache_new_archive_lease must be > 0")
        if self.retrieval_default_lease.total_seconds() <= 0:
            raise ValueError("retrieval_default_lease must be > 0")
        if self.retrieval_max_lease < self.retrieval_default_lease:
            raise ValueError("retrieval_max_lease must be at least retrieval_default_lease")
        if self.retrieval_pending_timeout.total_seconds() <= 0:
            raise ValueError("retrieval_pending_timeout must be > 0")
        if self.retrieval_cache_sweep_interval.total_seconds() <= 0:
            raise ValueError("retrieval_cache_sweep_interval must be > 0")
        if self.retrieval_restore_poll_interval.total_seconds() <= 0:
            raise ValueError("retrieval_restore_poll_interval must be > 0")
        if self.archive_scrypt_work_factor < 1 or self.archive_scrypt_work_factor > 22:
            raise ValueError("archive_scrypt_work_factor must be in 1..22")
        archive_passphrases: dict[str, str] = {}
        for passphrase_id, passphrase in self.archive_passphrases.items():
            try:
                normalized_id = normalize_passphrase_id(passphrase_id)
            except ValueError as exc:
                raise ValueError(f"invalid archive passphrase ID: {passphrase_id!r}") from exc
            if not isinstance(passphrase, str) or not passphrase:
                raise ValueError(f"archive passphrase {normalized_id!r} must not be empty")
            archive_passphrases[normalized_id] = passphrase
        if not archive_passphrases:
            raise ValueError("archive_passphrase_files must define at least one key")
        if len(set(archive_passphrases.values())) != len(archive_passphrases):
            raise ValueError("archive passphrase IDs must identify distinct secrets")
        try:
            active_passphrase_id = normalize_passphrase_id(self.archive_active_passphrase_id)
        except ValueError as exc:
            raise ValueError("archive_active_passphrase_id is invalid") from exc
        if active_passphrase_id not in archive_passphrases:
            raise ValueError(
                "archive_active_passphrase_id is not present in archive_passphrase_files"
            )
        object.__setattr__(self, "archive_passphrases", archive_passphrases)
        object.__setattr__(self, "archive_active_passphrase_id", active_passphrase_id)

    def archive_store(self, name: str) -> StorageAdapterRegistration:
        normalized = _normalize_archive_store_name(name)
        try:
            return self.archive_stores[normalized]
        except KeyError as exc:
            raise ValueError(f"archive store is not configured: {normalized}") from exc

    @property
    def archive_active_encryption(self) -> CollectionEncryptionBinding:
        return CollectionEncryptionBinding(
            format=ARCHIVE_ENCRYPTION_FORMAT,
            passphrase_id=self.archive_active_passphrase_id,
        )

    def archive_passphrase_for(self, passphrase_id: str) -> str:
        normalized = normalize_passphrase_id(passphrase_id)
        try:
            return self.archive_passphrases[normalized]
        except KeyError as exc:
            raise ValueError(f"archive passphrase ID is not configured: {normalized}") from exc
