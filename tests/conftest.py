from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml


@pytest.fixture(scope="session")
def checked_contract_closure() -> dict[str, Any]:
    """Load and validate the immutable checked v1 closure once per test session."""

    repo_root = Path(__file__).resolve().parents[1]
    scripts = repo_root / "scripts"
    if str(scripts) not in sys.path:
        sys.path.insert(0, str(scripts))
    contract_atlas = importlib.import_module("contract_atlas")
    checked = contract_atlas.load_atlas(repo_root / "qualification/contracts/riverhog-v1.json")
    return {
        "atlas": checked,
        "projection": contract_atlas.reassemble_projection(checked),
        "trace": contract_atlas.reassemble_trace(checked),
    }


@pytest.fixture(autouse=True)
def _explicit_riverhog_test_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Give isolated tests an explicit disposable config document."""

    if os.environ.get("RIVERHOG_CONFIG"):
        return
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
    monkeypatch.setenv("RIVERHOG_CONFIG", str(path))
