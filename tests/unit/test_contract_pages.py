from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

from contract_atlas.model import ContractAtlasError  # noqa: E402
from contract_pages import build_pages  # noqa: E402


def test_pages_preview_contains_the_checked_candidate_under_its_project_path(
    tmp_path: Path,
) -> None:
    source = REPO_ROOT / "qualification/contracts"
    destination = tmp_path / "site"
    source_sha = "a" * 40

    build = build_pages(source, destination, source_sha)

    assert build["source_sha"] == source_sha
    assert build["path"] == "contract-candidate/riverhog-v1/"
    assert (destination / "index.html").is_file()
    assert (destination / ".nojekyll").is_file()
    preview = destination / "contract-candidate"
    assert (preview / "riverhog-v1/index.html").is_file()
    assert "riverhog-v1/" in (preview / "index.html").read_text()
    for name in ("riverhog-v1.json", "riverhog-v1-audit.json"):
        assert (preview / name).read_bytes() == (source / name).read_bytes()
    assert json.loads((preview / "build-manifest.json").read_bytes()) == build


def test_pages_preview_requires_an_exact_source_commit(tmp_path: Path) -> None:
    with pytest.raises(ContractAtlasError, match="exact source commit"):
        build_pages(REPO_ROOT / "qualification/contracts", tmp_path / "site", "main")
