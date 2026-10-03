from __future__ import annotations

import hashlib
import importlib
import os
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
import yaml


@pytest.fixture(scope="session")
def generated_contract_closure(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    """Generate native records once, without a checked-product bootstrap dependency."""

    repo_root = Path(__file__).resolve().parents[1]
    scripts = repo_root / "scripts"
    if str(scripts) not in sys.path:
        sys.path.insert(0, str(scripts))
    generation = importlib.import_module("contract_atlas.generation")
    candidate = generation.build_candidate()
    output = tmp_path_factory.mktemp("generated-contract")
    candidate.write(output)
    return {
        "discovered": candidate.discovered,
        "bundle": candidate.bundle,
        "projection": candidate.projection,
        "trace": candidate.trace,
        "candidate": candidate,
        "root": output,
    }


@pytest.fixture(scope="session")
def release_contract_factory(generated_contract_closure, tmp_path_factory):
    """Isolated release witnesses reuse discovery and exercise native rendering/binding."""

    import contract_freeze
    from contract_atlas.html_rendering import render_contract
    from contract_atlas.model import canonical_bytes

    original = generated_contract_closure["candidate"]

    def create(source_sha="1" * 40, *, documentation=None):
        document = binding = None
        extras = {}
        if documentation is not None:
            document, binding = documentation.bind(
                original.bundle.closure,
                original.bundle.audit,
                contract_freeze._cli_parsers(),
                source_sha,
            )
            extras = {
                "documentation.json": documentation.payload,
                "documentation-record.json": canonical_bytes(document),
            }
        files = {
            **{
                name: value
                for name, value in original.files.items()
                if not name.startswith("riverhog-v1/")
            },
            **extras,
            **render_contract(
                original.bundle.closure, original.bundle.audit, document, source_revision=source_sha
            ),
        }
        manifest = {
            **original.manifest,
            "source_sha": source_sha,
            "build_scope": "revision",
            "documentation": binding,
            "files": {name: hashlib.sha256(value).hexdigest() for name, value in files.items()},
        }
        candidate = replace(original, files=files, manifest=manifest)
        root = tmp_path_factory.mktemp("synthetic-release-contract")
        candidate.write(root)
        return root

    return create


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
