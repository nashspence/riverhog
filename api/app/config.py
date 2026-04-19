from __future__ import annotations

import os
from pathlib import Path

GiB = 1024**3
MANAGED_DIRECTORY_MODE = 0o2775


def _get_env(name: str, default: str) -> str:
    value = os.getenv(name, default).strip()
    if not value:
        raise RuntimeError(f"{name} must not be empty")
    return value


def _get_optional_env(name: str) -> str | None:
    value = os.getenv(name, "").strip()
    return value or None


def _gb_env(name: str, default: str) -> int:
    return int(float(_get_env(name, default)) * GiB)


ARCHIVE_ROOT = Path(_get_env("ARCHIVE_ROOT", "/var/lib/archive"))
UPLOADS_ROOT = Path(_get_env("UPLOADS_ROOT", "/var/lib/uploads"))
SQLITE_PATH = Path(_get_env("SQLITE_PATH", str(ARCHIVE_ROOT / "catalog" / "catalog.sqlite3")))
API_BASE_URL = _get_env("API_BASE_URL", "http://localhost:8080").rstrip("/")
API_TOKEN = _get_env("API_TOKEN", "change-me")
ISO_AUTHORING_COMMAND = _get_env("ISO_AUTHORING_COMMAND", "xorriso")
AGE_CLI = _get_env("AGE_CLI", "age")
AGE_BATCHPASS_PASSPHRASE = _get_env("AGE_BATCHPASS_PASSPHRASE", "change-me")
AGE_BATCHPASS_WORK_FACTOR = _get_env("AGE_BATCHPASS_WORK_FACTOR", "18")
AGE_BATCHPASS_MAX_WORK_FACTOR = _get_env("AGE_BATCHPASS_MAX_WORK_FACTOR", "30")
OTS_CLIENT_COMMAND = _get_env("OTS_CLIENT_COMMAND", "ots")
PREFERRED_UID = int(_get_env("PREFERRED_UID", str(os.getuid())))
PREFERRED_GID = int(_get_env("PREFERRED_GID", str(os.getgid())))

CONTAINER_TARGET = _gb_env("CONTAINER_TARGET_GB", "50")
CONTAINER_FILL = _gb_env("CONTAINER_FILL_GB", "45")
CONTAINER_SPILL_FILL = _gb_env("CONTAINER_SPILL_FILL_GB", "35")
CONTAINER_BUFFER_MAX = _gb_env("CONTAINER_BUFFER_MAX_GB", "250")

CATALOG_DIR = SQLITE_PATH.parent

BUFFERED_COLLECTIONS_ROOT = ARCHIVE_ROOT / "buffered-collections"
ACTIVATION_STAGING_ROOT = ARCHIVE_ROOT / "activation-staging"
ACTIVE_CONTAINERS_ROOT = ARCHIVE_ROOT / "activated-containers"
MATERIALIZED_COLLECTIONS_ROOT = ARCHIVE_ROOT / "materialized-collections"
COLLECTION_EXPORTS_ROOT = ARCHIVE_ROOT / "collection-exports"
CONTAINER_STATE_DIR = ARCHIVE_ROOT / "container-state"
CONTAINER_ROOTS_DIR = ARCHIVE_ROOT / "container-roots"
REGISTERED_ISOS_ROOT = ARCHIVE_ROOT / "registered-isos"
COLLECTION_HASHES_ROOT = ARCHIVE_ROOT / "collection-hashes"

CONTAINER_FINALIZATION_WEBHOOK_URL = _get_optional_env("CONTAINER_FINALIZATION_WEBHOOK_URL")
CONTAINER_FINALIZATION_REMINDER_INTERVAL_SECONDS = (
    int(value)
    if (value := _get_optional_env("CONTAINER_FINALIZATION_REMINDER_INTERVAL_SECONDS")) is not None
    else None
)
CONTAINER_WEBHOOK_DISPATCH_INTERVAL_SECONDS = float(_get_env("CONTAINER_WEBHOOK_DISPATCH_INTERVAL_SECONDS", "5"))
CONTAINER_WEBHOOK_TIMEOUT_SECONDS = float(_get_env("CONTAINER_WEBHOOK_TIMEOUT_SECONDS", "10"))
CONTAINER_WEBHOOK_RETRY_SECONDS = float(_get_env("CONTAINER_WEBHOOK_RETRY_SECONDS", "60"))


CONTAINER_CFG = {
    "target": CONTAINER_TARGET,
    "fill": CONTAINER_FILL,
    "spill_fill": CONTAINER_SPILL_FILL,
    "buffer_max": CONTAINER_BUFFER_MAX,
}


def ensure_managed_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    try:
        os.chown(path, PREFERRED_UID, PREFERRED_GID)
    except PermissionError:
        pass
    path.chmod(MANAGED_DIRECTORY_MODE)


def ensure_directories() -> None:
    for path in [
        UPLOADS_ROOT,
        CATALOG_DIR,
        BUFFERED_COLLECTIONS_ROOT,
        ACTIVATION_STAGING_ROOT,
        ACTIVE_CONTAINERS_ROOT,
        MATERIALIZED_COLLECTIONS_ROOT,
        COLLECTION_EXPORTS_ROOT,
        CONTAINER_STATE_DIR,
        CONTAINER_ROOTS_DIR,
        REGISTERED_ISOS_ROOT,
        COLLECTION_HASHES_ROOT,
    ]:
        ensure_managed_directory(path)
