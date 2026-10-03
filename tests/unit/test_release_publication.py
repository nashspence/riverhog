from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import github_governance  # noqa: E402
import release  # noqa: E402
from contract_atlas.model import ContractAtlasError, canonical_bytes  # noqa: E402
from contract_atlas.publication import file_sha256  # noqa: E402
from contract_atlas.release_publication import (  # noqa: E402
    collect_qualification,
    publication_assets,
    publish_prepared_release,
    validate_qualification,
)


def _qualified():
    return {
        "format": "riverhog-release-qualification/v1",
        "source_sha": "1" * 40,
        "version": "1.0.0",
        "qualification_mode": "prospective",
        "published": False,
        "required_checks": "passed",
        "operation_matrix": "passed",
        "database_contract": "passed",
        "release_evidence": "passed",
        "github_governance": "actions-observable-passed",
    }


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_sha", "2" * 40),
        ("version", "1.1.0"),
        ("qualification_mode", "historical"),
        ("published", True),
        ("operation_matrix", "blocked"),
        ("database_contract", "failed"),
        ("required_checks", "pending"),
        ("github_governance", "unchecked"),
    ],
)
def test_preparation_requires_complete_matching_qualification(field, value):
    record = _qualified()
    record[field] = value
    with pytest.raises(ContractAtlasError, match="qualification"):
        validate_qualification(record, "1" * 40, "1.0.0")


def test_release_assets_cannot_silently_flatten_colliding_paths(tmp_path):
    (tmp_path / "nested").mkdir()
    (tmp_path / "manifest.json").write_bytes(b"first")
    (tmp_path / "nested/manifest.json").write_bytes(b"second")
    with pytest.raises(ContractAtlasError, match="colliding"):
        publication_assets(tmp_path)


class _TrustedArtifacts:
    repository = "nashspence/riverhog"

    def __init__(self, prepared, qualified):
        self.prepared = prepared
        self.qualified = qualified
        self.publications = []
        self.qualification_path = ".github/workflows/release-qualification.yml"

    def api(self, endpoint):
        run = int(endpoint.rsplit("/", 1)[1])
        return {
            "path": self.qualification_path
            if run == 11
            else ".github/workflows/release-preparation.yml",
            "status": "completed",
            "conclusion": "success",
            "repository": {"full_name": self.repository},
        }

    def tag_commit(self, tag):
        return "1" * 40

    def releases(self):
        return []

    def command(self, *args):
        assert args[:2] == ("run", "download")
        source = self.qualified if args[2] == "11" else self.prepared
        shutil.copytree(source, Path(args[args.index("--dir") + 1]))
        return b""

    def publish_asset_set(self, *args, **kwargs):
        self.publications.append(args)
        return {"immutable": True}


@pytest.mark.parametrize(
    "mutate", ["evidence", "key", "offline-signature", "offline-public-key", "none"]
)
def test_protected_publication_uses_the_selected_trusted_preparation_bytes(
    tmp_path,
    monkeypatch,
    mutate,
):
    trusted = tmp_path / "trusted"
    qualified = tmp_path / "qualified"
    (trusted / "evidence").mkdir(parents=True)
    qualified.mkdir()
    record = qualified / "qualification.json"
    record.write_bytes(canonical_bytes(_qualified()))
    manifest = {
        "tag": "v1.0.0",
        "version": "1.0.0",
        "source_sha": "1" * 40,
        "qualification": {
            "file": "qualification.json",
            "sha256": file_sha256(record),
            "run_id": 11,
        },
        "contract": {
            "documentation": {"commit": "2" * 40},
            "preparation": {"release_version": "1.0.0"},
        },
    }
    (trusted / "evidence/release-manifest.json").write_bytes(canonical_bytes(manifest))
    shutil.copyfile(record, trusted / "evidence/qualification.json")
    (trusted / "evidence/SHA256SUMS").write_bytes(b"the exact reviewed checksum payload\n")
    (trusted / "evidence.preparation.pub").write_bytes(b"selected trusted public key")
    local = tmp_path / "local"
    shutil.copytree(trusted, local)
    if mutate == "evidence":
        (local / "evidence/qualification.json").write_bytes(b"substituted record")
    elif mutate == "key":
        (local / "evidence.preparation.pub").write_bytes(b"substituted key")
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_SHA", "1" * 40)
    monkeypatch.setenv("GITHUB_REF", "refs/tags/v1.0.0")
    monkeypatch.setattr(github_governance, "check", lambda **kwargs: None)
    monkeypatch.setattr(release, "_source_sha", lambda root: "1" * 40)
    verifications = []
    signs = []
    monkeypatch.setattr(release, "_sign_checksums", lambda *args, **kwargs: signs.append(kwargs))
    remote = _TrustedArtifacts(trusted, qualified)
    signature = tmp_path / "offline.minisig"
    public_key = tmp_path / "external-pub"
    offline_key = tmp_path / "offline-key"
    subprocess.run(
        ["minisign", "-G", "-W", "-s", str(offline_key), "-p", str(public_key)],
        check=True,
        capture_output=True,
    )
    signed_message = trusted / "evidence/SHA256SUMS"
    if mutate == "offline-signature":
        signed_message = tmp_path / "other-checksums"
        signed_message.write_bytes(b"a different checksum payload\n")
    subprocess.run(
        ["minisign", "-S", "-s", str(offline_key), "-m", str(signed_message), "-x", str(signature)],
        check=True,
        capture_output=True,
    )
    offline_key.unlink()
    if mutate == "offline-public-key":
        public_key.unlink()
        subprocess.run(
            ["minisign", "-G", "-W", "-s", str(offline_key), "-p", str(public_key)],
            check=True,
            capture_output=True,
        )
        offline_key.unlink()

    def verify(*args, **kwargs):
        verifications.append(kwargs)
        # The native evidence verifier is independently covered. Exercise its
        # real cryptographic boundary here, after the trusted-input fences.
        if kwargs["public_key"] == public_key:
            release._run(
                [
                    "minisign",
                    "-V",
                    "-p",
                    str(public_key),
                    "-m",
                    str(local / "evidence/SHA256SUMS"),
                    "-x",
                    str(local / "evidence/SHA256SUMS.minisig"),
                ],
                cwd=local,
                capture=True,
            )

    monkeypatch.setattr(release, "verify_release_evidence", verify)
    kwargs = {
        "checksums_signature": signature,
        "public_key": public_key,
        "preparation_public_key": local / "evidence.preparation.pub",
        "preparation_run": 10,
        "qualification_run": 11,
        "remote": remote,
    }
    if mutate in {"evidence", "key"}:
        with pytest.raises(ContractAtlasError, match="trusted artifact"):
            publish_prepared_release(ROOT, local / "evidence", **kwargs)
        assert not signs and not remote.publications and not verifications
    elif mutate.startswith("offline-"):
        with pytest.raises(subprocess.CalledProcessError):
            publish_prepared_release(ROOT, local / "evidence", **kwargs)
        assert not signs and not remote.publications
    else:
        report = publish_prepared_release(ROOT, local / "evidence", **kwargs)
        assert report["immutable"] and not signs and len(remote.publications) == 1
        assert verifications[0]["public_key"] == kwargs["preparation_public_key"]
        assert verifications[1]["public_key"] == kwargs["public_key"]
        assert (
            file_sha256(local / "evidence/qualification.json")
            == manifest["qualification"]["sha256"]
        )
        assert (local / "evidence/SHA256SUMS.minisig").read_bytes() == signature.read_bytes()
        assert remote.publications[0][2]["release.pub"].name == "release.pub"


def test_publication_workflow_uses_only_the_offline_signature_and_protected_public_key():
    workflow = yaml.load(
        (ROOT / ".github/workflows/release-publication.yml").read_text(), Loader=yaml.BaseLoader
    )
    assert set(workflow["on"]["workflow_dispatch"]["inputs"]) == {
        "preparation_run",
        "qualification_run",
        "checksums_signature",
    }
    job = workflow["jobs"]["publish"]
    assert job["environment"]["name"] == "release-publication"
    approval = next(step for step in job["steps"] if "CHECKSUMS_SIGNATURE" in step.get("env", {}))
    assert approval["env"] == {
        "GH_TOKEN": "${{ secrets.RELEASE_GITHUB_TOKEN }}",
        "CHECKSUMS_SIGNATURE": "${{ inputs.checksums_signature }}",
        "RELEASE_PUBLIC_KEY": "${{ vars.RELEASE_MINISIGN_PUBLIC_KEY }}",
        "PREPARATION_RUN": "${{ inputs.preparation_run }}",
        "QUALIFICATION_RUN": "${{ inputs.qualification_run }}",
    }
    assert '--checksums-signature "$approval/SHA256SUMS.minisig"' in approval["run"]


def test_qualification_rejects_success_from_an_unrelated_workflow(tmp_path):
    remote = _TrustedArtifacts(tmp_path / "unused", tmp_path / "unused")
    remote.qualification_path = ".github/workflows/unrelated.yml"
    with pytest.raises(ContractAtlasError, match="repository qualification"):
        collect_qualification(remote, 11, "1" * 40, "1.0.0", tmp_path / "download")
    assert not (tmp_path / "download").exists()
