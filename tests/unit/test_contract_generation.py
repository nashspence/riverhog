from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from contract_atlas.documentation import (  # noqa: E402
    AuthoredDocumentation,
)
from contract_atlas.generation import (  # noqa: E402
    source_revision,
    verify_candidate,
)
from contract_atlas.model import ContractAtlasError, canonical_bytes, canonical_sha256  # noqa: E402
from contract_freeze import _cli_parsers  # noqa: E402


def test_source_generation_needs_no_checked_products_and_retains_native_integrity(
    generated_contract_closure: dict[str, Any],
) -> None:
    assert not (ROOT / "qualification/contracts/riverhog-v1.json").exists()
    assert not (ROOT / "qualification/contracts/riverhog-v1-audit.json").exists()
    assert not (ROOT / "qualification/contracts/riverhog-v1").exists()
    candidate = generated_contract_closure["candidate"]
    manifest = verify_candidate(generated_contract_closure["root"])
    assert manifest["closure_sha256"] == canonical_sha256(candidate.bundle.closure)
    assert manifest["audit_sha256"] == canonical_sha256(candidate.bundle.audit)
    assert candidate.bundle.audit["counts"]["contract_elements"] == len(
        candidate.bundle.closure["elements"]
    )
    assert not any(candidate.bundle.audit["discovery"]["anomalies"].values())
    assert manifest["documentation"] is None
    assert 'id="docs-mode"' not in candidate.files["riverhog-v1/index.html"].decode()
    assert manifest["renderer"] and manifest["toolchain"]["inputs"]


@pytest.mark.parametrize("mutation", ["record", "render", "extra", "missing"])
def test_candidate_reuse_rejects_mutation_and_inventory_changes(
    mutation: str, generated_contract_closure: dict[str, Any], tmp_path: Path
) -> None:
    candidate = generated_contract_closure["candidate"]
    output = tmp_path / "candidate"
    candidate.write(output)
    if mutation == "record":
        path = output / "riverhog-v1.json"
        path.write_bytes(path.read_bytes() + b" ")
    elif mutation == "render":
        path = output / "riverhog-v1/index.html"
        path.write_bytes(path.read_bytes().replace(b"<title>", b"<title>changed ", 1))
    elif mutation == "extra":
        (output / "unexpected.txt").write_text("not a generated candidate input")
    else:
        (output / "riverhog-v1/index.html").unlink()
    with pytest.raises(ContractAtlasError, match="candidate (file identity|file set)"):
        verify_candidate(output)


def test_candidate_reuse_requires_expected_source(
    generated_contract_closure: dict[str, Any],
) -> None:
    with pytest.raises(ContractAtlasError, match="expected commit"):
        verify_candidate(generated_contract_closure["root"], expected_source="a" * 40)


def test_replace_refuses_non_candidate_data(
    generated_contract_closure: dict[str, Any], tmp_path: Path
) -> None:
    output = tmp_path / "authored"
    output.mkdir()
    (output / "keep.txt").write_text("authored data")
    with pytest.raises(ContractAtlasError, match="build manifest"):
        generated_contract_closure["candidate"].write(output, replace=True)
    assert (output / "keep.txt").read_text() == "authored data"


def test_replacement_accepts_owned_previous_semantics_without_reusing_them(
    generated_contract_closure: dict[str, Any], tmp_path: Path
) -> None:
    candidate = generated_contract_closure["candidate"]
    output = tmp_path / "previous"
    candidate.write(output)
    closure = output / "riverhog-v1.json"
    closure.write_bytes(b"{}")
    manifest = json.loads((output / "build-manifest.json").read_bytes())
    manifest["files"][closure.name] = hashlib.sha256(closure.read_bytes()).hexdigest()
    (output / "build-manifest.json").write_bytes(canonical_bytes(manifest))
    candidate.write(output, replace=True)
    assert verify_candidate(output)["closure_sha256"] == candidate.manifest["closure_sha256"]


def test_documentation_binding_preserves_markdown_bytes_and_semantics(generated_contract_closure):
    from tests.documentation_fixtures import document

    closure = generated_contract_closure["bundle"].closure
    audit = generated_contract_closure["bundle"].audit
    identity = closure["elements"][0]["id"]
    raw = document(
        [
            {
                "target": {"element_id": identity, "pointer": ""},
                "summary": "Selected release expression.",
            }
        ]
    )
    selected = AuthoredDocumentation("v1.0.0", "b" * 40, {"reference/current.md": raw})
    before = canonical_bytes(closure)
    record, binding = selected.bind(closure, audit, _cli_parsers(), "a" * 40)
    assert selected.files["reference/current.md"] == raw
    assert binding["sha256"] == hashlib.sha256(selected.payload).hexdigest()
    assert (
        record["compiled"]["source_files"]["reference/current.md"]["sha256"]
        == hashlib.sha256(raw).hexdigest()
    )
    assert binding["documentation_record_sha256"] == canonical_sha256(record)
    assert canonical_bytes(closure) == before
    changed = AuthoredDocumentation(
        selected.tag,
        selected.commit,
        {
            "reference/current.md": raw.replace(
                b"Selected release expression.", b"Revised release expression."
            )
        },
    )
    changed_record, changed_binding = changed.bind(closure, audit, _cli_parsers(), "a" * 40)
    assert changed_binding["closure_sha256"] == binding["closure_sha256"]
    assert changed_binding["compiled_sha256"] != binding["compiled_sha256"]
    assert changed_record["requirements"] == record["requirements"]


def _git(repository: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(repository), *args], text=True).strip()


def test_documentation_uses_selected_git_bytes_after_authoring_branch_advances(
    tmp_path: Path,
) -> None:
    tmp_path = tmp_path / "authoring"
    tmp_path.mkdir()
    _git(tmp_path, "init", "-q")
    _git(tmp_path, "config", "user.name", "Fixture")
    _git(tmp_path, "config", "user.email", "fixture@example.invalid")
    path = tmp_path / "v1.0.0/reference/current.md"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"original exact bytes\n")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-qm", "authoring fixture")
    first = _git(tmp_path, "rev-parse", "HEAD")
    path.write_bytes(b"later different bytes\n")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-qm", "advance authoring fixture")
    selected = AuthoredDocumentation.resolve(tmp_path, "v1.0.0", first)
    assert selected.files["reference/current.md"] == b"original exact bytes\n"
    assert selected.commit == first


def test_exact_source_generation_rejects_tracked_and_untracked_drift(tmp_path: Path) -> None:
    _git(tmp_path, "init", "-q")
    _git(tmp_path, "config", "user.name", "Fixture")
    _git(tmp_path, "config", "user.email", "fixture@example.invalid")
    source = tmp_path / "source.py"
    source.write_text("original")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-qm", "source fixture")
    sha = _git(tmp_path, "rev-parse", "HEAD")
    assert source_revision(tmp_path, sha) == sha
    source.write_text("changed")
    assert source_revision(tmp_path) is None
    with pytest.raises(ContractAtlasError, match="clean HEAD"):
        source_revision(tmp_path, sha)
    source.write_text("original")
    (tmp_path / "untracked.py").write_text("new source")
    with pytest.raises(ContractAtlasError, match="clean HEAD"):
        source_revision(tmp_path, sha)


def test_documentation_authoring_rejects_code_and_shared_ancestry(tmp_path):
    from contract_atlas.documentation import validate_authoring_tree

    repository = tmp_path / "isolated"
    repository.mkdir()
    _git(repository, "init", "-q")
    _git(repository, "config", "user.name", "Fixture")
    _git(repository, "config", "user.email", "fixture@example.invalid")
    (repository / "README.md").write_text("authoring metadata")
    _git(repository, "add", ".")
    _git(repository, "commit", "-qm", "metadata fixture")
    shared = _git(repository, "rev-parse", "HEAD")
    validate_authoring_tree(repository, shared)
    with pytest.raises(ContractAtlasError, match="ancestry-independent"):
        validate_authoring_tree(repository, shared, shared)
    (repository / "source.py").write_text("pass\n")
    _git(repository, "add", ".")
    _git(repository, "commit", "-qm", "code fixture")
    with pytest.raises(ContractAtlasError, match="non-documentation path"):
        validate_authoring_tree(repository, _git(repository, "rev-parse", "HEAD"))
