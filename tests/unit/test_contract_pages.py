from __future__ import annotations

import hashlib
import json
import sys
import threading
from contextlib import contextmanager
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import urlopen

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

from contract_atlas.model import ContractAtlasError  # noqa: E402
from contract_pages import build_pages  # noqa: E402


def test_pages_contains_the_source_generated_candidate_under_its_project_path(
    tmp_path: Path,
    generated_contract_closure,
) -> None:
    source = generated_contract_closure["root"]
    destination = tmp_path / "site"
    source_sha = None
    candidate_manifest = json.loads((source / "build-manifest.json").read_bytes())

    build = build_pages(source, destination, source_sha)

    assert build["inputs"][0]["source_sha"] == candidate_manifest["source_sha"]
    assert build["latest_product_release"] is None
    assert "No final product releases" in (destination / "index.html").read_text()
    assert (destination / "index.html").is_file()
    assert (destination / ".nojekyll").is_file()
    preview = destination / "contract-candidate"
    assert (preview / "riverhog-v1/index.html").is_file()
    assert "riverhog-v1/" in (preview / "index.html").read_text()
    for name in ("riverhog-v1.json", "riverhog-v1-audit.json"):
        assert (preview / name).read_bytes() == (source / name).read_bytes()
    assert json.loads((destination / "site-manifest.json").read_bytes()) == build
    assert build["site_bytes"] == sum(
        p.stat().st_size for p in destination.rglob("*") if p.is_file()
    )
    assert json.loads((preview / "build-manifest.json").read_bytes())["documentation"] is None


def test_pages_preview_requires_an_exact_source_commit(tmp_path: Path) -> None:
    with pytest.raises(ContractAtlasError, match="exact source commit"):
        build_pages(tmp_path / "unused", tmp_path / "site", "main")


def test_pages_capacity_fails_before_publishing_and_keeps_all_versions(
    tmp_path, generated_contract_closure
):
    with pytest.raises(ContractAtlasError, match="operational budget"):
        build_pages(generated_contract_closure["root"], tmp_path / "site", None, budget=1)
    assert not (tmp_path / "site").exists()


def test_pages_does_not_trust_a_claimed_revision(tmp_path, generated_contract_closure):
    with pytest.raises(ContractAtlasError, match="source does not match"):
        build_pages(generated_contract_closure["root"], tmp_path / "site", "b" * 40)


def test_pages_assembles_versions_semantically_and_preserves_published_bytes(
    tmp_path,
    generated_contract_closure,
    release_contract_factory,
):
    from contract_atlas.documentation import SOURCE_FORMAT, AuthoredDocumentation
    from contract_atlas.model import canonical_bytes

    from tests.release_index import make_index

    root = release_contract_factory(
        documentation=AuthoredDocumentation(
            "v1.2.0",
            "b" * 40,
            "v1.2.0/documentation.json",
            canonical_bytes({"format": SOURCE_FORMAT, "explanations": [], "guides": []}),
        )
    )
    authored = AuthoredDocumentation(
        "v1.10.0",
        "c" * 40,
        "v1.10.0/documentation.json",
        canonical_bytes({"format": SOURCE_FORMAT, "explanations": [], "guides": []}),
    )
    documented = release_contract_factory("2" * 40, documentation=authored)
    entries = [
        {
            "root": selected,
            "tag": tag,
            "release_id": number,
            "release_manifest_sha256": "d" * 64,
            "attestation_sha256": "e" * 64,
            "assets": {"fixture": "sha256:" + "f" * 64},
            "installation": make_index(tmp_path / tag, tag, "1" * 40 if number == 1 else "2" * 40)[
                0
            ],
        }
        for number, (tag, selected) in enumerate((("v1.2.0", root), ("v1.10.0", documented)), 1)
    ]
    manifest = build_pages(
        generated_contract_closure["root"], tmp_path / "site", None, releases=entries
    )
    assert manifest["latest_product_release"] == "v1.10.0"
    assert [item["version"] for item in manifest["inputs"]] == ["development", "v1.10.0", "v1.2.0"]
    assert 'href="../v1.2.0/"' in (tmp_path / "site/v1.10.0/index.html").read_text()
    assert "Documentation" in (tmp_path / "site/v1.10.0/index.html").read_text()
    assert "Documentation" in (tmp_path / "site/v1.2.0/index.html").read_text()
    assert all(
        (tmp_path / "site/v1.10.0" / path.relative_to(documented)).read_bytes() == path.read_bytes()
        for path in documented.rglob("*")
        if path.is_file()
    )
    assert (tmp_path / "site/v1.10.0/documentation.json").read_bytes() == authored.payload

    # Serve the actual aggregate at its project mount; use each generated index URL.
    class MountedSite(SimpleHTTPRequestHandler):
        def do_GET(self):
            assert self.path.startswith("/riverhog/")
            self.path = self.path.removeprefix("/riverhog")
            super().do_GET()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(
        ("127.0.0.1", 0), partial(MountedSite, directory=tmp_path / "site")
    )
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        for entry in entries:
            installation = entry["installation"]
            base = f"http://127.0.0.1:{server.server_port}" + urlsplit(installation["url"]).path
            with urlopen(base, timeout=5) as response:
                assert (
                    response.read()
                    == (installation["root"] / installation["path"] / "index.html").read_bytes()
                )
            with urlopen(base + "a-riverhog-cli/", timeout=5) as response:
                page = response.read()
                assert f"/releases/download/{entry['tag']}/".encode() in page
                assert b"#sha256=" in page
            assert all(name in manifest["files"] for name in installation["files"])
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def test_final_product_without_documentation_is_not_ingested(
    tmp_path, generated_contract_closure, release_contract_factory
):
    with pytest.raises(ContractAtlasError, match="requires bound documentation"):
        build_pages(
            generated_contract_closure["root"],
            tmp_path / "site",
            None,
            releases=[{"tag": "v1.0.0", "root": release_contract_factory()}],
        )
    assert not (tmp_path / "site").exists()


@pytest.mark.parametrize("mutation", ["before-read", "after-read"])
def test_installation_assembly_publishes_only_the_bytes_it_verified(
    tmp_path, monkeypatch, generated_contract_closure, release_contract_factory, mutation
):
    from contract_atlas.documentation import SOURCE_FORMAT, AuthoredDocumentation
    from contract_atlas.model import canonical_bytes

    from tests.release_index import make_index

    tag = "v1.0.0"
    root = release_contract_factory(
        documentation=AuthoredDocumentation(
            tag,
            "b" * 40,
            f"{tag}/documentation.json",
            canonical_bytes({"format": SOURCE_FORMAT, "explanations": [], "guides": []}),
        )
    )
    installation = make_index(tmp_path / "installation", tag, "1" * 40)[0]
    name = installation["path"] + "index.html"
    watched = installation["root"] / name
    approved = watched.read_bytes()
    changed = approved + b"<!-- changed after verification -->"
    original_open = Path.open
    mutated = False

    @contextmanager
    def mutate_after_read(stream):
        nonlocal mutated
        with stream:
            yield stream
        mutated = True
        watched.write_bytes(changed)

    def open_with_mutation(path, *args, **kwargs):
        stream = original_open(path, *args, **kwargs)
        mode = args[0] if args else kwargs.get("mode", "r")
        if path == watched and mode == "rb" and not mutated:
            return mutate_after_read(stream)
        return stream

    if mutation == "before-read":
        watched.write_bytes(changed)
    else:
        monkeypatch.setattr(Path, "open", open_with_mutation)
    entry = {
        "root": root,
        "tag": tag,
        "release_id": 1,
        "release_manifest_sha256": "d" * 64,
        "attestation_sha256": "e" * 64,
        "assets": {"fixture": "sha256:" + "f" * 64},
        "installation": installation,
    }
    destination = tmp_path / "site"
    if mutation == "before-read":
        with pytest.raises(ContractAtlasError, match="installation index changed"):
            build_pages(generated_contract_closure["root"], destination, None, releases=[entry])
        assert not destination.exists()
        return
    manifest = build_pages(generated_contract_closure["root"], destination, None, releases=[entry])
    assert mutated and watched.read_bytes() == changed
    assert (destination / name).read_bytes() == approved
    approved_sha = hashlib.sha256(approved).hexdigest()
    assert manifest["files"][name] == approved_sha
    assert manifest["inputs"][1]["installation"]["files"][name] == approved_sha
