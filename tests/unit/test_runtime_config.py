from __future__ import annotations

import ast
import json
from dataclasses import fields, replace
from datetime import timedelta
from pathlib import Path

import pytest
import yaml
from riverhog_api import deps
from riverhog_core.collection_plan import CollectionVolumePolicy
from riverhog_core.pack_retrieval import PackRangeRetrievalPolicy
from riverhog_core.runtime_config import (
    RetrievalCacheStoreRegistration,
    RuntimeConfig,
    StorageAdapterRegistration,
)
from riverhog_core.runtime_document import generated_config_schema, load_runtime_config
from riverhog_core.throughput import ArchiveThroughputTuning
from riverhog_storage_adapter_support import StorageAdapterClient

from tests.unit.db_helpers import sqlite_url

_SERVER_SOURCE = Path(__file__).parents[2] / "riverhog" / "src"
_SCHEMA = _SERVER_SOURCE / "riverhog_core/config.schema.json"


def _config(tmp_path: Path, **overrides: object) -> RuntimeConfig:
    return RuntimeConfig.for_testing(
        database_url=sqlite_url(tmp_path / "state.sqlite3"), **overrides
    )


def _document(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, dict[str, object]]:
    secrets = {
        "database_url_file": "postgresql+psycopg://riverhog:riverhog@postgres:5432/riverhog",
        "bootstrap_token_file": "riverhog-bootstrap-token",
        "browse_token_signing_key_file": "riverhog-test-browse-token-signing-key-v1",
        "archive_passphrase_file": "archive-secret",
        "adapter_token_file": "adapter-secret",
    }
    paths: dict[str, str] = {}
    for name, value in secrets.items():
        path = tmp_path / name
        path.write_text(value + "\n", encoding="utf-8")
        paths[name] = str(path)
    document: dict[str, object] = {
        "database_url_file": paths["database_url_file"],
        "bootstrap_token_file": paths["bootstrap_token_file"],
        "browse_token_signing_key_file": paths["browse_token_signing_key_file"],
        "archive_passphrase_files": {"runtime-test-key-v1": paths["archive_passphrase_file"]},
        "archive_active_passphrase_id": "runtime-test-key-v1",
        "archive_write_store": "archive",
        "archive_stores": {
            "archive": {
                "base_url": "https://archive.invalid",
                "token_file": paths["adapter_token_file"],
            }
        },
    }
    path = tmp_path / "riverhog.yaml"
    _write(path, document)
    monkeypatch.setenv("RIVERHOG_CONFIG", str(path))
    return path, document


def _write(path: Path, document: dict[str, object]) -> None:
    path.write_text(yaml.safe_dump(document, sort_keys=True), encoding="utf-8")


def test_published_config_schema_matches_parser() -> None:
    assert json.loads(_SCHEMA.read_text(encoding="utf-8")) == generated_config_schema()


def test_storage_names_are_global_and_token_paths_are_absolute(tmp_path: Path) -> None:
    archive = StorageAdapterRegistration(
        name="shared", base_url="https://storage.invalid", token_file=tmp_path / "token"
    )
    cache = RetrievalCacheStoreRegistration(name="shared", adapter=archive)
    with pytest.raises(ValueError, match="distinct names"):
        _config(
            tmp_path,
            archive_write_store="shared",
            archive_read_order=("shared",),
            archive_stores={"shared": archive},
            retrieval_cache_stores={"shared": cache},
        )
    with pytest.raises(ValueError, match="token file must be absolute"):
        _config(
            tmp_path,
            archive_write_store="shared",
            archive_read_order=("shared",),
            archive_stores={"shared": replace(archive, token_file=Path("~/token"))},
        )


def test_adapter_secret_path_has_one_loader_and_client_interpretation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path, document = _document(tmp_path, monkeypatch)
    archive = document["archive_stores"]
    assert isinstance(archive, dict)
    archive["archive"]["token_file"] = "~/adapter.token"
    _write(path, document)
    with pytest.raises(ValueError, match="absolute secret-file path"):
        load_runtime_config()

    archive["archive"]["token_file"] = str(tmp_path / "adapter_token_file")
    _write(path, document)
    config = load_runtime_config()
    observed: list[Path] = []
    monkeypatch.setattr(
        StorageAdapterClient,
        "from_token_file",
        lambda _base_url, *, token_file, **_kwargs: observed.append(token_file),
    )
    deps._adapter_client(config.archive_stores["archive"])
    assert observed == [tmp_path / "adapter_token_file"]


def test_runtime_configuration_fields_have_explicit_production_consumers() -> None:
    consumed = {
        node.attr
        for path in _SERVER_SOURCE.rglob("*.py")
        if path.name != "runtime_config.py"
        for node in ast.walk(ast.parse(path.read_text()))
        if isinstance(node, ast.Attribute)
    }
    configuration_fields = {
        field.name
        for model in (
            RuntimeConfig,
            StorageAdapterRegistration,
            RetrievalCacheStoreRegistration,
            CollectionVolumePolicy,
            PackRangeRetrievalPolicy,
            ArchiveThroughputTuning,
        )
        for field in fields(model)
    }
    assert configuration_fields <= consumed


def test_public_base_url_is_normalized_and_rejects_ambiguous_authority(tmp_path: Path) -> None:
    assert (
        _config(tmp_path, public_base_url="http://riverhog.example.test/prefix/").public_base_url
        == "http://riverhog.example.test/prefix"
    )
    with pytest.raises(ValueError, match="public_base_url"):
        _config(tmp_path, public_base_url="https://user@riverhog.example.test")


def test_runtime_secret_semantics_and_repr() -> None:
    with pytest.raises(ValueError, match="browse_token_signing_key_file"):
        RuntimeConfig()
    with pytest.raises(ValueError, match="archive_passphrase_files"):
        RuntimeConfig(browse_token_signing_key="x" * 32)
    config = RuntimeConfig.for_testing(bootstrap_token="bootstrap-secret")
    assert "bootstrap-secret" not in repr(config)
    assert "archive-passphrase" not in repr(config)


def test_catalog_sync_cursor_lifetimes_fit_the_retained_history(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="must not exceed"):
        _config(
            tmp_path,
            catalog_sync_cursor_lifetime=timedelta(days=2),
            catalog_sync_history_retention=timedelta(days=1),
        )
    with pytest.raises(ValueError, match="must not exceed"):
        _config(
            tmp_path,
            browse_token_lifetime=timedelta(days=2),
            catalog_sync_history_retention=timedelta(days=1),
        )


def test_yaml_connects_security_storage_and_runtime_policy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path, document = _document(tmp_path, monkeypatch)
    archive = document["archive_stores"]
    assert isinstance(archive, dict)
    archive["archive"].update(
        monthly_download_allowance_bytes="1TB",
        download_safety_buffer_bytes="50GB",
    )
    document.update(
        archive_scrypt_work_factor=12,
        archive_upload_sweep_interval="17s",
        collection_upload_custody_lease="45m",
        browse_token_lifetime="2h",
        event_context_retention="14d",
        event_context_reap_batch_size=37,
        catalog_sync_bootstrap_lifetime="6d",
        catalog_sync_cursor_lifetime="12h",
        catalog_sync_history_retention="21d",
        catalog_sync_page_size_max=37,
        catalog_sync_history_reap_batch_size=41,
        retrieval_cache_write_segment_bytes="7MiB",
        retrieval_estimated_latency="6h",
        retrieval_cache_new_archive_enabled=False,
        retrieval_cache_new_archive_lease="3d",
        retrieval_default_lease="2d",
        retrieval_max_lease="20d",
        retrieval_pending_timeout="4d",
        retrieval_cache_sweep_interval="7m",
        retrieval_restore_poll_interval="11m",
        log_level="debug",
        volume_policy={"pack_files": 23},
        throughput={"write_concurrency": 7},
        range_policy={"billing_mode": "whole_object"},
        range_policy_by_store={"archive": {"merge_gap_ciphertext_bytes": "64KiB"}},
    )
    _write(path, document)
    config = load_runtime_config()
    assert config.database_url.startswith("postgresql+")
    assert config.bootstrap_token == "riverhog-bootstrap-token"
    assert config.archive_passphrase_for("runtime-test-key-v1") == "archive-secret"
    assert config.archive_scrypt_work_factor == 12
    assert config.archive_upload_sweep_interval == timedelta(seconds=17)
    assert config.collection_upload_custody_lease == timedelta(minutes=45)
    assert config.archive_store("archive").monthly_download_allowance_bytes == 1_000_000_000_000
    assert config.archive_store("archive").download_safety_buffer_bytes == 50_000_000_000
    assert config.browse_token_lifetime == timedelta(hours=2)
    assert config.event_context_retention == timedelta(days=14)
    assert config.event_context_reap_batch_size == 37
    assert config.catalog_sync_bootstrap_lifetime == timedelta(days=6)
    assert config.catalog_sync_cursor_lifetime == timedelta(hours=12)
    assert config.catalog_sync_history_retention == timedelta(days=21)
    assert config.catalog_sync_page_size_max == 37
    assert config.catalog_sync_history_reap_batch_size == 41
    assert config.retrieval_cache_write_segment_bytes == 7 * 1024 * 1024
    assert config.retrieval_estimated_latency == timedelta(hours=6)
    assert config.retrieval_cache_new_archive_enabled is False
    assert config.retrieval_cache_new_archive_lease == timedelta(days=3)
    assert config.retrieval_default_lease == timedelta(days=2)
    assert config.retrieval_max_lease == timedelta(days=20)
    assert config.retrieval_pending_timeout == timedelta(days=4)
    assert config.retrieval_cache_sweep_interval == timedelta(minutes=7)
    assert config.retrieval_restore_poll_interval == timedelta(minutes=11)
    assert config.log_level == "DEBUG"
    assert config.volume_policy.pack_files == 23
    assert config.throughput_tuning.write_concurrency == 7
    assert config.range_policy.billing_mode == "whole_object"
    assert config.range_policy_for_store("archive").merge_gap_ciphertext_bytes == 65536


def test_named_storage_and_cache_use_secret_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path, document = _document(tmp_path, monkeypatch)
    token = tmp_path / "adapter_token_file"
    document["archive_stores"] = {
        "deep": {"base_url": "https://deep.invalid", "token_file": str(token)},
        "b2": {
            "base_url": "https://b2.invalid/",
            "token_file": str(token),
            "maximum_connections": 48,
            "timeout_seconds": 75.5,
        },
    }
    document["archive_write_store"] = "deep"
    document["archive_read_order"] = ["b2", "deep"]
    document["retrieval_cache_stores"] = {
        "local": {
            "base_url": "https://cache.invalid/adapter/",
            "token_file": str(token),
            "maximum_connections": 24,
            "timeout_seconds": 90,
            "admission_enabled": False,
            "admission_budget_bytes": "1MiB",
        }
    }
    _write(path, document)
    config = load_runtime_config()
    assert config.archive_write_store == "deep"
    assert config.archive_read_order == ("b2", "deep")
    assert config.archive_store("b2") == StorageAdapterRegistration(
        name="b2",
        base_url="https://b2.invalid",
        token_file=token,
        maximum_connections=48,
        timeout_seconds=75.5,
    )
    assert config.retrieval_cache_stores["local"].admission_budget_bytes == 1024 * 1024


def test_missing_secret_and_unknown_field_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path, document = _document(tmp_path, monkeypatch)
    document["archive_passphrase_files"] = {"runtime-test-key-v1": str(tmp_path / "missing")}
    _write(path, document)
    with pytest.raises(ValueError, match="cannot be read"):
        load_runtime_config()
    document["archive_passphrase_files"] = {
        "runtime-test-key-v1": str(tmp_path / "archive_passphrase_file")
    }
    document["unknown_config_field"] = "legacy"
    _write(path, document)
    with pytest.raises(ValueError, match="Additional properties are not allowed"):
        load_runtime_config()


def test_cross_field_storage_and_retention_rules(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path, document = _document(tmp_path, monkeypatch)
    store = document["archive_stores"]
    assert isinstance(store, dict)
    store["archive"]["download_safety_buffer_bytes"] = "50GB"
    _write(path, document)
    with pytest.raises(ValueError, match="safety buffer requires a monthly download allowance"):
        load_runtime_config()
    store["archive"]["monthly_download_allowance_bytes"] = "50GB"
    _write(path, document)
    with pytest.raises(ValueError, match="safety buffer must be smaller"):
        load_runtime_config()
    store["archive"].pop("download_safety_buffer_bytes")
    store["archive"].pop("monthly_download_allowance_bytes")
    document["catalog_sync_history_retention"] = "1d"
    document["browse_token_lifetime"] = "2d"
    _write(path, document)
    with pytest.raises(ValueError, match="must not exceed"):
        load_runtime_config()


def test_storage_adapter_http_requires_explicit_opt_in(tmp_path: Path) -> None:
    registration = StorageAdapterRegistration(
        name="archive",
        base_url="http://adapter.example.test",
        token_file=tmp_path / "adapter.token",
    )
    with pytest.raises(ValueError, match="archive store archive adapter URL"):
        _config(tmp_path, archive_stores={"archive": registration})
    configured = _config(
        tmp_path,
        archive_stores={"archive": replace(registration, allow_insecure_http=True)},
    )
    assert configured.archive_store("archive").base_url == "http://adapter.example.test"


def test_metered_download_source_cannot_be_bypassed_through_store_alias(tmp_path: Path) -> None:
    archive = _config(tmp_path).archive_store("archive")
    metered = replace(
        archive, monthly_download_allowance_bytes=1_000, download_safety_buffer_bytes=100
    )
    alias = replace(archive, name="alias")
    with pytest.raises(ValueError, match="metered archive download source.*alias, archive"):
        _config(tmp_path, archive_stores={"archive": metered, "alias": alias})


def test_retrieval_max_lease_covers_default_lease(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="retrieval_max_lease must be at least"):
        _config(tmp_path, retrieval_default_lease=timedelta(days=8))
