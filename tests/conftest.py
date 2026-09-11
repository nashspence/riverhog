from __future__ import annotations

import pytest


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
