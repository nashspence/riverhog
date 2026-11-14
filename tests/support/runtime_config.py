"""Write the explicit disposable configuration shared by tests and native profiles."""

from __future__ import annotations

from pathlib import Path

import yaml


def write_test_runtime_config(tmp_path: Path) -> Path:
    """Write dummy fixture credentials; never reuse deployed application settings."""
    secrets = {
        "database-url": "postgresql+psycopg://riverhog:riverhog@127.0.0.1:5432/riverhog",
        "bootstrap-token": "riverhog-pytest-bootstrap-token",
        "browse-key": "riverhog-pytest-browse-token-signing-key-v1",
        "archive-passphrase": "riverhog-pytest-archive-passphrase",
        "adapter-token": "riverhog-pytest-adapter-token",
    }
    for name, value in secrets.items():
        (tmp_path / name).write_text(value + "\n", encoding="utf-8")
    config = {
        "database_url_file": str(tmp_path / "database-url"),
        "bootstrap_token_file": str(tmp_path / "bootstrap-token"),
        "browse_token_signing_key_file": str(tmp_path / "browse-key"),
        "archive_passphrase_files": {
            "riverhog-pytest-key-v1": str(tmp_path / "archive-passphrase")
        },
        "archive_active_passphrase_id": "riverhog-pytest-key-v1",
        "archive_write_store": "archive",
        "archive_stores": {
            "archive": {
                "base_url": "https://archive.invalid",
                "token_file": str(tmp_path / "adapter-token"),
            }
        },
    }
    path = tmp_path / "riverhog.yaml"
    path.write_text(yaml.safe_dump(config, sort_keys=True), encoding="utf-8")
    return path
