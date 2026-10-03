"""Synthetic final-release path; no remote product tag or release is created."""

from __future__ import annotations

import hashlib
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import github_governance  # noqa: E402
import release  # noqa: E402
from contract_atlas.documentation import SOURCE_FORMAT, AuthoredDocumentation  # noqa: E402
from contract_atlas.model import ContractAtlasError, canonical_bytes  # noqa: E402
from contract_atlas.publication import file_sha256  # noqa: E402
from contract_atlas.release_publication import (  # noqa: E402
    collect_qualification,
    publish_prepared_release,
)
from contract_pages import build_pages, require_requested_product  # noqa: E402

from tests.actions_evidence import ArtifactRemote, execution  # noqa: E402
from tests.release_index import make_index  # noqa: E402


class _Publication(ArtifactRemote):
    def __init__(self, directory, source_sha):
        super().__init__(directory / "actions")
        self.source_sha = source_sha
        self.store = directory / "release-assets"
        self.store.mkdir()
        self.created = self.published = False
        self.uploaded = {}
        self.calls = []

    def tag_commit(self, tag):
        assert tag == "v1.0.0"
        return self.source_sha

    def releases(self):
        return (
            [
                {
                    "id": 101,
                    "tag_name": "v1.0.0",
                    "draft": not self.published,
                    "prerelease": False,
                    "immutable": self.published,
                }
            ]
            if self.created
            else []
        )

    def assets(self, release_id):
        assert release_id == 101
        return list(self.uploaded.values())

    def api(self, endpoint):
        if endpoint == "immutable-releases":
            return {"enabled": True}
        if endpoint == "releases/tags/v1.0.0":
            return {"id": 101, "draft": not self.published, "immutable": self.published}
        return super().api(endpoint)

    def command(self, *args):
        self.calls.append(args)
        if args[:2] == ("release", "create"):
            assert "--draft" in args
            self.created = True
        elif args[:2] == ("release", "upload"):
            file = Path(args[3])
            assert not self.published
            shutil.copyfile(file, self.store / file.name)
            self.uploaded[file.name] = {
                "id": len(self.uploaded) + 1,
                "name": file.name,
                "digest": "sha256:" + file_sha256(file),
                "size": file.stat().st_size,
            }
        elif args[:2] == ("release", "edit"):
            assert "--draft=false" in args
            self.published = True
        elif args[:2] == ("release", "verify"):
            assert self.published
        elif args[:2] == ("release", "verify-asset"):
            assert self.published
            file = Path(args[3])
            assert file_sha256(file) == self.uploaded[file.name]["digest"].removeprefix("sha256:")
        else:
            assert args[:3] == ("api", "--method", "POST")
            assert "ref=main" in args
            assert self.published
        return b'{"verified":true}'

    def download_asset(self, tag, asset, destination):
        assert self.published
        shutil.copyfile(self.store / asset["name"], destination)
        assert "sha256:" + file_sha256(destination) == asset["digest"]
        assert destination.stat().st_size == asset["size"]
        self.command("release", "verify-asset", tag, str(destination), "--repo", self.repository)


def _key(directory, name):
    secret, public = directory / (name + ".key"), directory / (name + ".pub")
    subprocess.run(
        ["minisign", "-G", "-W", "-s", str(secret), "-p", str(public)],
        check=True,
        capture_output=True,
    )
    return secret, public


def test_trusted_preparation_offline_signature_immutable_history_and_main_pages(
    tmp_path, monkeypatch, release_contract_factory, generated_contract_closure
):
    # Select authored bytes from a real independent Git history.
    repo = tmp_path / "source"
    repo.mkdir()

    def git(*args):
        return (
            subprocess.check_output(
                [
                    "git",
                    "-C",
                    str(repo),
                    "-c",
                    "user.name=Release test",
                    "-c",
                    "user.email=release@example.invalid",
                    "-c",
                    "commit.gpgsign=false",
                    *args,
                ],
                stderr=subprocess.DEVNULL,
            )
            .decode()
            .strip()
        )

    git("init", "-b", "main")
    (repo / "README.md").write_text("Synthetic product source\n")
    git("add", "README.md")
    git("commit", "-m", "Source")
    source_sha = git("rev-parse", "HEAD")
    git("checkout", "--orphan", "release-documentation")
    git("read-tree", "--empty")
    (repo / "v1.0.0").mkdir()
    document = canonical_bytes({"format": SOURCE_FORMAT, "explanations": [], "guides": []})
    (repo / "v1.0.0/documentation.json").write_bytes(document)
    git("add", "v1.0.0/documentation.json")
    git("commit", "-m", "Independent authored input")
    documentation_commit = git("rev-parse", "HEAD")
    authored = AuthoredDocumentation.resolve(
        repo, "v1.0.0", documentation_commit, source_sha=source_sha
    )
    candidate = release_contract_factory(
        source_sha,
        documentation=authored,
        preparation={
            "release_version": "1.0.0",
            "source_epoch": 0,
            "source_archive_sha256": hashlib.sha256(b"synthetic source").hexdigest(),
            "inputs_sha256": hashlib.sha256(b"synthetic inputs").hexdigest(),
        },
    )
    qualified = tmp_path / "qualification"
    qualified.mkdir()
    record = {
        "format": "riverhog-release-qualification/v1",
        "source_sha": source_sha,
        "version": "1.0.0",
        "qualification_mode": "prospective",
        "published": False,
        "required_checks": "passed",
        "operation_matrix": "passed",
        "database_contract": "passed",
        "release_evidence": "passed",
        "github_governance": "actions-observable-passed",
        "workflow_execution": execution("qualification", source_sha=source_sha),
    }
    (qualified / "qualification.json").write_bytes(canonical_bytes(record))
    remote = _Publication(tmp_path / "github", source_sha)
    remote.register("qualification", qualified, source_sha=source_sha)
    collected = collect_qualification(remote, 11, source_sha, "1.0.0", tmp_path / "qualified-input")
    prepared = tmp_path / "prepared"
    prepared.mkdir()
    output = prepared / "evidence"
    installation, install_manifest, _ = make_index(output, "v1.0.0", source_sha)
    # The test's unpacked index is a query projection, not a release asset.
    shutil.rmtree(installation["root"])
    # Build systems and platform lock production have their own qualification;
    # this witness exercises native release evidence and the exact index seam.
    monkeypatch.setattr(release.installation, "verify_installation_artifacts", lambda *args: None)
    signing_key, preparation_key = _key(tmp_path, "preparation")
    shutil.copyfile(preparation_key, prepared / "evidence.preparation.pub")
    records = [
        {
            "kind": "install-index",
            "name": "riverhog-python-index-v1.0.0.tar.gz",
            "sha256": install_manifest["index"]["snapshot_sha256"],
            "size": (output / "riverhog-python-index-v1.0.0.tar.gz").stat().st_size,
            "version": "1.0.0",
            "license": "NOASSERTION",
            "dependencies": [],
        }
    ]
    publication = {"distributions": {}, "runtime_images": {}}
    release._generate_release_evidence(
        ROOT,
        output,
        records,
        version="1.0.0",
        source_sha=source_sha,
        source_epoch=0,
        spdx_created="1970-01-01T00:00:00Z",
        contract_candidate=candidate,
        install_manifest=install_manifest,
        signing_key=signing_key,
        public_key=preparation_key,
        publication=publication,
        qualification_record=collected,
        qualification_run=11,
    )
    signing_key.unlink()
    remote.register("preparation", prepared, source_sha=source_sha)
    offline_key, public_key = _key(tmp_path, "maintainer")
    signature = tmp_path / "reviewed.minisig"
    subprocess.run(
        [
            "minisign",
            "-S",
            "-s",
            str(offline_key),
            "-m",
            str(output / "SHA256SUMS"),
            "-x",
            str(signature),
        ],
        check=True,
        capture_output=True,
    )
    offline_key.unlink()
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_SHA", source_sha)
    monkeypatch.setenv("GITHUB_REF", "refs/tags/v1.0.0")
    monkeypatch.setattr(github_governance, "check", lambda **kwargs: None)
    monkeypatch.setattr(release, "_source_sha", lambda _root: source_sha)
    verify = release.verify_release_evidence
    monkeypatch.setattr(
        release,
        "verify_release_evidence",
        lambda *args, **kwargs: verify(*args, **kwargs, publication=publication),
    )
    published = publish_prepared_release(
        ROOT,
        output,
        checksums_signature=signature,
        public_key=public_key,
        preparation_public_key=prepared / "evidence.preparation.pub",
        preparation_run=10,
        qualification_run=11,
        remote=remote,
    )
    assert published["immutable"] and published["pages_request"]["source_sha"] == "3" * 40
    snapshot = remote.snapshot()
    requested = published["pages_request"]
    require_requested_product(
        snapshot,
        requested["release_tag"],
        requested["release_id"],
        requested["release_manifest_sha256"],
    )
    dispatches = [call for call in remote.calls if call[:3] == ("api", "--method", "POST")]
    assert len(dispatches) == 1 and "ref=main" in dispatches[0]
    # Historical ingestion executes neither current discovery nor rendering.
    import contract_atlas.html_rendering as rendering

    monkeypatch.setattr(
        rendering, "render_contract", lambda *args, **kwargs: pytest.fail("historical re-render")
    )
    loaded = remote.load_product(snapshot["products"][0], tmp_path / "historical")
    site = build_pages(
        generated_contract_closure["root"],
        tmp_path / "site",
        None,
        releases=[loaded],
        snapshot=snapshot,
    )
    assert site["inputs"][1]["documentation"]["commit"] == documentation_commit
    assert (tmp_path / "site/v1.0.0/documentation.json").read_bytes() == document
    assert (tmp_path / "site/artifacts/v1.0.0/simple/a-riverhog-cli/index.html").is_file()
    assert (
        site["inputs"][1]["installation"]["snapshot_sha256"]
        == install_manifest["index"]["snapshot_sha256"]
    )
    assert (
        loaded["assets"]["install-manifest.json"]
        == remote.uploaded["install-manifest.json"]["digest"]
    )
    assert (remote.store / "SHA256SUMS.minisig").read_bytes() == signature.read_bytes()
    bad = {**requested, "release_id": 999}
    with pytest.raises(ContractAtlasError, match="absent or changed"):
        require_requested_product(
            snapshot, bad["release_tag"], bad["release_id"], bad["release_manifest_sha256"]
        )
