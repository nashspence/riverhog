from __future__ import annotations

import copy
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
    SOURCE_FORMAT,
    AuthoredDocumentation,
    validate_authored_documentation,
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


def test_authored_documentation_requires_utf8() -> None:
    payload = json.dumps({"format": SOURCE_FORMAT, "explanations": [], "guides": []})
    with pytest.raises(ContractAtlasError, match="UTF-8"):
        validate_authored_documentation(payload.encode("utf-16"), {"elements": []})


@pytest.fixture
def authored(generated_contract_closure: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    closure = generated_contract_closure["bundle"].closure
    identity = closure["elements"][0]["id"]
    return closure, {
        "format": SOURCE_FORMAT,
        "explanations": [
            {"element_id": identity, "text": "Human explanation <script>text</script>"}
        ],
        "guides": [
            {
                "id": "first-use",
                "title": "First use",
                "text": "Follow the contract.",
                "subjects": [identity],
            }
        ],
    }


@pytest.mark.parametrize(
    "inventory",
    ["cli_commands", "packages", "images", "artifacts", "release", "defaults", "schema"],
)
def test_authored_documentation_excludes_source_owned_reference_inventories(
    inventory: str, authored: tuple[dict[str, Any], dict[str, Any]]
) -> None:
    closure, document = authored
    document[inventory] = []
    with pytest.raises(ContractAtlasError, match="only explanations and guides"):
        validate_authored_documentation(canonical_bytes(document), closure)


@pytest.mark.parametrize(
    "mutation", ["duplicate", "stale", "wrong_type", "empty", "duplicate_subject"]
)
def test_authored_documentation_rejects_invalid_or_stale_references(
    mutation: str, authored: tuple[dict[str, Any], dict[str, Any]]
) -> None:
    closure, document = authored
    if mutation == "duplicate":
        document["explanations"] *= 2
    elif mutation == "stale":
        document["explanations"][0]["element_id"] = "nonexistent"
    elif mutation == "wrong_type":
        document["explanations"][0]["text"] = 42
    elif mutation == "empty":
        document["guides"][0]["text"] = " "
    else:
        document["guides"][0]["subjects"] *= 2
    with pytest.raises(ContractAtlasError):
        validate_authored_documentation(canonical_bytes(document), closure)


def test_authored_documentation_rejects_duplicate_json_fields() -> None:
    with pytest.raises(ContractAtlasError, match="duplicate field"):
        validate_authored_documentation(b'{"format":"one","format":"two"}', {"elements": []})


def test_documentation_binding_preserves_exact_authored_bytes_and_generated_help(
    authored: tuple[dict[str, Any], dict[str, Any]], generated_contract_closure: dict[str, Any]
) -> None:
    closure, document = authored
    audit = generated_contract_closure["bundle"].audit
    closure_before, audit_before = canonical_bytes(closure), canonical_bytes(audit)
    raw = (json.dumps(document, indent=2, ensure_ascii=False) + "\n").encode()
    selected = AuthoredDocumentation("v1.0.0", "b" * 40, "v1.0.0/documentation.json", raw)
    record, binding = selected.bind(closure, audit, _cli_parsers(), "a" * 40)
    assert selected.payload == raw
    assert binding["sha256"] == hashlib.sha256(raw).hexdigest()
    assert binding["documentation_record_sha256"] == canonical_sha256(record)
    assert record["explanations"] == document["explanations"]
    assert record["cli_commands"]
    assert canonical_bytes(closure) == closure_before and canonical_bytes(audit) == audit_before
    changed = copy.deepcopy(document)
    changed["guides"][0]["text"] = "A revised guide."
    changed_record, changed_binding = AuthoredDocumentation(
        selected.tag, selected.commit, selected.path, canonical_bytes(changed)
    ).bind(closure, audit, _cli_parsers(), "a" * 40)
    assert changed_record["cli_commands"] == record["cli_commands"]
    assert changed_binding["closure_sha256"] == binding["closure_sha256"]
    assert changed_binding["audit_sha256"] == binding["audit_sha256"]
    assert changed_binding["documentation_record_sha256"] != binding["documentation_record_sha256"]


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
    path = tmp_path / "v1.0.0/documentation.json"
    path.parent.mkdir()
    path.write_bytes(b"original exact bytes\n")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-qm", "authoring fixture")
    first = _git(tmp_path, "rev-parse", "HEAD")
    path.write_bytes(b"later different bytes\n")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-qm", "advance authoring fixture")
    selected = AuthoredDocumentation.resolve(tmp_path, "v1.0.0", first)
    assert selected.payload == b"original exact bytes\n"
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
