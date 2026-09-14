from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any

import pytest


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
def _explicit_riverhog_test_secrets(monkeypatch: pytest.MonkeyPatch) -> None:
    """Select disposable Riverhog secrets explicitly for the test process."""

    monkeypatch.setenv(
        "RIVERHOG_ARCHIVE_PASSPHRASES_JSON",
        '{"riverhog-pytest-key-v1":"riverhog-pytest-archive-passphrase"}',
    )
    monkeypatch.setenv(
        "RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID",
        "riverhog-pytest-key-v1",
    )
    monkeypatch.setenv(
        "RIVERHOG_BROWSE_TOKEN_SIGNING_KEY",
        "riverhog-pytest-browse-token-signing-key-v1",
    )
