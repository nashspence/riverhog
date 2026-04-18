from __future__ import annotations

import os


def _get_env(name: str, default: str) -> str:
    value = os.getenv(name, default).strip()
    if not value:
        raise RuntimeError(f"{name} must not be empty")
    return value


RIVERHOG_API_BASE_URL = _get_env("RIVERHOG_API_BASE_URL", "http://localhost:8080").rstrip("/")
RIVERHOG_API_TOKEN = _get_env("RIVERHOG_API_TOKEN", "change-me")
REQUEST_TIMEOUT_SECONDS = float(_get_env("RIVERHOG_REQUEST_TIMEOUT_SECONDS", "300"))
