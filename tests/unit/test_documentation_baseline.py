"""A comparison needs authenticated upstream custody and the original archived policy."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from contract_atlas import documentation_baseline as baseline  # noqa: E402
from contract_atlas.model import ContractAtlasError, canonical_bytes, canonical_sha256  # noqa: E402


class AuthenticatedProduct:
    def __init__(self, manifest, record):
        self.manifest = manifest
        self.record = record

    def snapshot(self):
        return {"products": [{"tag": "v1.0.0"}]}

    def load_product(self, selected, destination):
        root = destination / "candidate"
        root.mkdir(parents=True)
        (root / "build-manifest.json").write_bytes(canonical_bytes(self.manifest))
        (root / "documentation-audit.json").write_bytes(canonical_bytes(self.record))
        return {
            "root": root,
            "release_manifest_sha256": "a" * 64,
            "release_id": 17,
            "attestation_sha256": "b" * 64,
            "assets": {"contract": "sha256:" + "c" * 64},
        }


@pytest.fixture
def requested_baseline(tmp_path, monkeypatch):
    manifest = {"source_sha": "a" * 40, "documentation": {"tag": "v1.0.0"}}
    # The custody boundary is injected; this test protects selection/reconciliation,
    # while publication tests verify the real signed artifact bytes.
    monkeypatch.setattr(
        baseline,
        "verify_published_candidate",
        lambda root: json.loads((root / "build-manifest.json").read_bytes()),
    )
    current = {"policy": {"format": "archived-original-policy"}, "historical": "exact evidence"}
    record = {
        "format": "riverhog-documentation-audit/v1",
        "stage": "prepared",
        "state": "REVIEW",
        "current": current,
    }
    root = tmp_path / "requested"
    root.mkdir()
    (root / "build-manifest.json").write_bytes(canonical_bytes(manifest))
    (tmp_path / baseline.PROVENANCE_FILE).write_bytes(
        canonical_bytes({"kind": "published-product", "tag": "v1.0.0"})
    )
    return root, AuthenticatedProduct(manifest, record), current


def test_explicit_initial_is_not_an_unavailable_baseline():
    assert baseline.resolve(None, initial=True) == (None, None)
    with pytest.raises(ContractAtlasError, match="explicit initial"):
        baseline.resolve(None, initial=False)


def test_authenticated_baseline_preserves_archived_snapshot_and_policy(requested_baseline):
    root, remote, current = requested_baseline
    captured, custody = baseline.resolve(root, initial=False, remote=remote)
    assert captured == current
    assert custody["snapshot_sha256"] == canonical_sha256(current)
    assert custody["verification"]["attestation_sha256"] == "b" * 64
    assert custody["kind"] == "published-product"


@pytest.mark.parametrize(
    "mutation",
    [
        "missing-selector",
        "self-digest",
        "missing-upstream",
        "different-source",
        "preview",
        "unsupported",
    ],
)
def test_requested_baseline_never_accepts_local_assertions_or_becomes_initial(
    requested_baseline, mutation
):
    root, remote, _ = requested_baseline
    selector = root.parent / baseline.PROVENANCE_FILE
    if mutation == "missing-selector":
        selector.unlink()
    elif mutation == "self-digest":
        selector.write_text(json.dumps({"kind": "author-approved", "sha256": "a" * 64}))
    elif mutation == "missing-upstream":
        selector.write_text(json.dumps({"kind": "published-product", "tag": "v1.9.0"}))
    elif mutation == "different-source":
        remote.manifest = {**remote.manifest, "source_sha": "f" * 40}
    elif mutation == "preview":
        remote.record = {**remote.record, "stage": "preview"}
    elif mutation == "unsupported":
        remote.record = {**remote.record, "format": "unknown/v9"}
    with pytest.raises(ContractAtlasError):
        baseline.resolve(root, initial=False, remote=remote)


def test_initial_cannot_override_selected_baseline(requested_baseline):
    root, remote, _ = requested_baseline
    with pytest.raises(ContractAtlasError, match="mutually exclusive"):
        baseline.resolve(root, initial=True, remote=remote)
