from __future__ import annotations

import hashlib
import io
import json
import sys
import tarfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from contract_atlas.github_publication import (  # noqa: E402
    GitHubPublication,
    require_fresh_snapshot,
    select_products,
)
from contract_atlas.model import ContractAtlasError  # noqa: E402
from contract_atlas.publication import (  # noqa: E402
    extract_verified_tar,
    verify_published_candidate,
)


def _product(tag, **extra):
    return {
        "tag_name": tag,
        "draft": False,
        "prerelease": False,
        "immutable": True,
        "assets": [{"name": "release-manifest.json"}],
        **extra,
    }


def test_product_enumeration_excludes_maintenance_candidates_and_drafts_and_orders_semantically():
    selected = select_products(
        [
            _product("v1.2.0"),
            _product("v1.10.0"),
            _product("v1.1.0", draft=True),
            _product("v1.11.0-rc.1", prerelease=True),
            _product("repository-history-rebaseline-2026-10-03"),
            _product("unrelated"),
            _product("v01.1.0"),
        ]
    )
    assert [row["tag_name"] for row in selected] == ["v1.10.0", "v1.2.0"]


@pytest.mark.parametrize(
    "rows",
    [
        [_product("v1.0.0", immutable=False)],
        [_product("v1.0.0", prerelease=True)],
        [_product("v1.0.0"), _product("v1.0.0")],
        [_product("v1.0.0", assets=[])],
        [_product("v1.0.0", assets=[{"name": "release-manifest.json"}] * 2)],
    ],
)
def test_required_product_input_failure_is_visible_instead_of_skipping_a_version(rows):
    with pytest.raises(ContractAtlasError):
        select_products(rows)


def test_release_reader_collects_every_api_page_and_does_not_use_latest(monkeypatch):
    remote = GitHubPublication("nashspence/riverhog")
    calls = []
    pages = [
        [_product("repository-history-rebaseline-2026-10-03")] * 100,
        [_product("v1.0.0")],
        [_product("v1.10.0")],
    ]

    def command(*args):
        calls.append(args)
        return b"\n".join(json.dumps(page).encode() for page in pages)

    monkeypatch.setattr(remote, "command", command)
    assert [r["tag_name"] for r in select_products(remote.releases())] == ["v1.10.0", "v1.0.0"]
    assert calls == [("api", "--paginate", "repos/nashspence/riverhog/releases?per_page=100")]


@pytest.mark.parametrize("changed", ["main", "products"])
def test_stale_aggregate_snapshot_stops_publication(changed):
    snapshot = {"main": "a" * 40, "products": []}
    current = {**snapshot, changed: "b" * 40 if changed == "main" else [{"tag": "v1.0.0"}]}
    with pytest.raises(ContractAtlasError, match="changed"):
        require_fresh_snapshot(snapshot, current)
    require_fresh_snapshot(snapshot, snapshot)


def _archive(path, names, *, kind=tarfile.REGTYPE, payload=b"bounded data"):
    with tarfile.open(path, "w:gz") as archive:
        for name in names:
            member = tarfile.TarInfo(name)
            member.type = kind
            member.size = len(payload) if kind == tarfile.REGTYPE else 0
            member.linkname = "../outside"
            archive.addfile(member, io.BytesIO(payload) if member.isfile() else None)
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.parametrize(
    "names,kind",
    [
        (["../outside"], tarfile.REGTYPE),
        (["/absolute"], tarfile.REGTYPE),
        (["a/./b"], tarfile.REGTYPE),
        (["a\\b"], tarfile.REGTYPE),
        (["C:/absolute"], tarfile.REGTYPE),
        (["same", "same"], tarfile.REGTYPE),
        (["a", "a/b"], tarfile.REGTYPE),
        (["link"], tarfile.SYMTYPE),
        (["link"], tarfile.LNKTYPE),
        (["device"], tarfile.CHRTYPE),
        (["fifo"], tarfile.FIFOTYPE),
    ],
)
def test_archive_paths_links_collisions_and_special_files_are_rejected_without_partial_output(
    tmp_path,
    names,
    kind,
):
    path = tmp_path / "asset.tar.gz"
    digest = _archive(path, names, kind=kind)
    destination = tmp_path / "unpacked"
    with pytest.raises(ContractAtlasError):
        extract_verified_tar(path, digest, destination)
    assert not destination.exists()
    assert not (tmp_path / "outside").exists()


def test_archive_fixity_and_expansion_budget_are_checked(tmp_path):
    path = tmp_path / "asset.tar.gz"
    digest = _archive(path, ["expanded"], payload=b"x" * 100_000)
    with pytest.raises(ContractAtlasError, match="identity"):
        extract_verified_tar(path, "0" * 64, tmp_path / "wrong")
    with pytest.raises(ContractAtlasError, match="expansion"):
        extract_verified_tar(path, digest, tmp_path / "over-budget", budget=10_000)
    assert not (tmp_path / "over-budget").exists()
    extract_verified_tar(path, digest, tmp_path / "valid", budget=200_000)
    assert (tmp_path / "valid/expanded").read_bytes() == b"x" * 100_000


def test_archive_metadata_is_bounded_before_tar_parser_allocates_it(tmp_path):
    path = tmp_path / "metadata.tar.gz"
    with tarfile.open(path, "w:gz") as stream:
        member = tarfile.TarInfo("small")
        member.pax_headers = {"comment": "x" * 200_000}
        stream.addfile(member, io.BytesIO(b""))
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(ContractAtlasError, match="expansion"):
        extract_verified_tar(path, digest, tmp_path / "metadata", budget=10_000)
    assert not (tmp_path / "metadata").exists()


def test_published_inputs_are_verified_without_running_current_source_or_renderer(
    release_contract_factory,
    monkeypatch,
):
    root = release_contract_factory()
    import contract_atlas.generation as generation
    import contract_atlas.html_rendering as rendering

    def forbidden(*args, **kwargs):
        raise AssertionError("historical publication must consume pinned bytes")

    monkeypatch.setattr(generation, "build_candidate", forbidden)
    monkeypatch.setattr(rendering, "render_contract", forbidden)
    assert verify_published_candidate(root)["source_sha"] == "1" * 40
    path = root / "riverhog-v1/index.html"
    path.write_bytes(path.read_bytes() + b"tampered")
    with pytest.raises(ContractAtlasError, match="file identity"):
        verify_published_candidate(root)


class _DraftPublication(GitHubPublication):
    def __init__(self, files, *, corrupt=False):
        super().__init__("nashspence/riverhog")
        self.files = files
        self.corrupt = corrupt
        self.created = False
        self.published = False
        self.uploaded = {}
        self.calls = []

    def tag_commit(self, tag):
        return "1" * 40

    def releases(self):
        return [{"tag_name": "v1.0.0"}] if self.created else []

    def api(self, endpoint):
        if endpoint == "immutable-releases":
            return {"enabled": True}
        return {"id": 1, "draft": not self.published, "immutable": self.published}

    def assets(self, release_id):
        return list(self.uploaded.values())

    def command(self, *args):
        self.calls.append(args)
        if args[:2] == ("release", "create"):
            self.created = True
        elif args[:2] == ("release", "upload"):
            path = Path(args[3])
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            self.uploaded[path.name] = {
                "name": path.name,
                "size": path.stat().st_size,
                "digest": "sha256:" + ("0" * 64 if self.corrupt else digest),
            }
        elif args[:2] == ("release", "edit"):
            assert set(self.uploaded) == set(self.files)
            self.published = True
        return b"{}"


def test_publication_completes_draft_verifies_every_asset_then_publishes_immutable(tmp_path):
    paths = {name: tmp_path / name for name in ("record.json", "render.tar.gz")}
    for path in paths.values():
        path.write_bytes(path.name.encode())
    notes = tmp_path / "notes.md"
    notes.write_text("Synthetic publication witness")
    remote = _DraftPublication(paths)
    report = remote.publish_asset_set("v1.0.0", "1" * 40, paths, title="Fixture", notes=notes)
    assert report["immutable"] is True
    assert all("--clobber" not in call for call in remote.calls)
    edit = next(i for i, call in enumerate(remote.calls) if call[:2] == ("release", "edit"))
    assert all(i < edit for i, call in enumerate(remote.calls) if call[:2] == ("release", "upload"))
    assert len([call for call in remote.calls if call[:2] == ("release", "verify-asset")]) == 2
    # An exact repeat verifies published custody without changing immutable assets.
    uploads = len([call for call in remote.calls if call[:2] == ("release", "upload")])
    remote.publish_asset_set("v1.0.0", "1" * 40, paths, title="Fixture", notes=notes)
    assert len([call for call in remote.calls if call[:2] == ("release", "upload")]) == uploads


def test_publication_never_publishes_a_mismatched_or_incomplete_draft(tmp_path):
    path = tmp_path / "asset.json"
    path.write_bytes(b"exact bytes")
    remote = _DraftPublication({path.name: path}, corrupt=True)
    with pytest.raises(ContractAtlasError, match="draft release failed"):
        remote.publish_asset_set("v1.0.0", "1" * 40, {path.name: path}, title="Fixture", notes=path)
    assert not remote.published
    assert not any(call[:2] == ("release", "edit") for call in remote.calls)
    remote.corrupt = False
    with pytest.raises(ContractAtlasError, match="existing release assets differ"):
        remote.publish_asset_set("v1.0.0", "1" * 40, {path.name: path}, title="Fixture", notes=path)
    assert not remote.published
