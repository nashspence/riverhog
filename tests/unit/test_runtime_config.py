from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

from arc_core.runtime_config import RuntimeConfig


def _base_runtime_config(tmp_path: Path, **overrides: object) -> RuntimeConfig:
    return RuntimeConfig(
        object_store="s3",
        s3_endpoint_url="http://example.invalid:9000",
        s3_region="us-east-1",
        s3_bucket="riverhog",
        s3_access_key_id="test-access",
        s3_secret_access_key="test-secret",
        s3_force_path_style=True,
        tusd_base_url="http://example.invalid:1080/files",
        tusd_hook_secret="hook-secret",
        sqlite_path=tmp_path / "state.sqlite3",
        **overrides,
    )


def test_runtime_config_rejects_recovery_ready_ttl_shorter_than_retry_window(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        ValueError,
        match="ARC_GLACIER_RECOVERY_READY_TTL must be at least",
    ):
        _base_runtime_config(
            tmp_path,
            glacier_recovery_webhook_url="http://example.invalid/webhooks/recovery",
            glacier_recovery_ready_ttl=timedelta(seconds=10),
            glacier_recovery_webhook_retry_delay=timedelta(seconds=1),
        )


def test_runtime_config_allows_recovery_ready_ttl_matching_timeout_plus_retry(
    tmp_path: Path,
) -> None:
    config = _base_runtime_config(
        tmp_path,
        glacier_recovery_webhook_url="http://example.invalid/webhooks/recovery",
        glacier_recovery_ready_ttl=timedelta(seconds=11),
        glacier_recovery_webhook_retry_delay=timedelta(seconds=1),
    )

    assert config.glacier_recovery_webhook_timeout == timedelta(seconds=10)


def test_runtime_config_does_not_enforce_recovery_timing_without_webhook_url(
    tmp_path: Path,
) -> None:
    config = _base_runtime_config(
        tmp_path,
        glacier_recovery_ready_ttl=timedelta(seconds=4),
        glacier_recovery_webhook_retry_delay=timedelta(seconds=1),
    )

    assert config.glacier_recovery_webhook_url is None
