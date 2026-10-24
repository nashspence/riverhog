from __future__ import annotations

import hashlib
import io
import json
import sys
import threading
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import unquote, urlsplit

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import documentation_publish as publication  # noqa: E402
from contract_atlas.model import ContractAtlasError, canonical_bytes  # noqa: E402
from contract_atlas.web_publication import Site, build_relay, public_base_url  # noqa: E402


def make_site(root: Path) -> Site:
    files = {
        "index.html": b"<!doctype html><title>Contracts</title>",
        "development/element.html": b"<!doctype html><title>Exact element</title>",
        "contract-candidate/element.html": b"<!doctype html><title>Exact element</title>",
        "schema/protocol.json": b'{"type":"object"}\n',
        "artifacts/v1.2.3/simple/index.html": b'<a href="package/">Package</a>',
        "artifacts/v1.2.3/simple/package/index.html": b'<a href="https://example.org/wheel">Wheel</a>',
        "v1.2.3/audit.json": b'{"version":"v1.2.3"}\n',
        ".nojekyll": b"",
    }
    root.mkdir()
    for name, body in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    manifest = {
        "format": "riverhog-contract-pages-site/v1",
        "inputs": [{"version": "development", "source_sha": "a" * 40}],
        "snapshot": None,
        "files": {name: hashlib.sha256(body).hexdigest() for name, body in files.items()},
        "site_bytes": 0,
        "hosting_budget_bytes": 10_000_000_000,
    }
    payload_size = sum(map(len, files.values()))
    while manifest["site_bytes"] != payload_size + len(canonical_bytes(manifest)):
        manifest["site_bytes"] = payload_size + len(canonical_bytes(manifest))
    (root / "site-manifest.json").write_bytes(canonical_bytes(manifest))
    return Site.read(root)


@pytest.mark.parametrize(
    "change", ["changed", "missing", "extra", "link", "accounting", "canonical"]
)
def test_publication_rejects_incomplete_or_changed_inventory(tmp_path, change):
    root = tmp_path / "site"
    make_site(root)
    if change == "changed":
        (root / "index.html").write_bytes(b"changed")
    elif change == "missing":
        (root / "index.html").unlink()
    elif change == "extra":
        (root / "unexpected").write_bytes(b"extra")
    elif change == "link":
        (root / "index.html").unlink()
        (root / "index.html").symlink_to(root / ".nojekyll")
    elif change == "accounting":
        manifest = json.loads((root / "site-manifest.json").read_bytes())
        manifest["site_bytes"] += 1
        (root / "site-manifest.json").write_bytes(canonical_bytes(manifest))
    else:
        path = root / "site-manifest.json"
        path.write_bytes(path.read_bytes() + b"\n")
    with pytest.raises(ContractAtlasError):
        Site.read(root)


@pytest.mark.parametrize(
    "url",
    [
        "http://docs.example/",
        "https://user:password@docs.example/",
        "https://docs.example/path",
        "https://docs.example/a/../",
        "https://docs.example/?query=1",
        "https://docs.example/#fragment",
    ],
)
def test_publication_requires_an_unambiguous_https_mount(url):
    with pytest.raises(ContractAtlasError):
        public_base_url(url)


def test_routing_preserves_schema_installer_and_records_without_changing_source(tmp_path):
    root = tmp_path / "site"
    site = make_site(root)
    before = {name: (root / name).read_bytes() for name in site.entries}
    relay = tmp_path / "relay"
    result = build_relay(root, relay, "https://docs.example/riverhog/")
    assert result["site_manifest_sha256"] == site.manifest_sha256
    assert result["site_bytes"] == sum(
        path.stat().st_size for path in relay.rglob("*") if path.is_file()
    )
    for name, original in before.items():
        if name.endswith(".html") and not name.startswith("artifacts/"):
            redirected = (relay / name).read_text()
            assert "https://docs.example/riverhog/" + name in redirected
            assert "location.search+location.hash" in redirected
        else:
            assert (relay / name).read_bytes() == original
        assert (root / name).read_bytes() == original
    assert len(site.blobs) < len(site.entries)
    module = site.worker_module()
    records = json.loads(module.split(b"\nexport default createWorker(")[1][:-3])
    assert dict(records) == {name: list(value) for name, value in site.entries.items()}


def test_routing_capacity_never_leaves_partial_output(tmp_path):
    root = tmp_path / "site"
    make_site(root)
    destination = tmp_path / "relay"
    with pytest.raises(ContractAtlasError, match="Pages budget"):
        build_relay(root, destination, "https://docs.example/", budget=1)
    assert not destination.exists()


class MemoryStorage:
    base_url = "https://storage.example/public/objects/"

    def __init__(self):
        self.content = {}
        self.calls = []
        self.failure = None
        self.lock = threading.Lock()

    def upload(self, digest, path, size):
        if digest == self.failure:
            raise publication.PublicationError("injected upload failure")
        body = path.read_bytes()
        assert len(body) == size and hashlib.sha256(body).hexdigest() == digest
        with self.lock:
            self.calls.append(digest)
            existed = digest in self.content
            self.content.setdefault(digest, body)
        return not existed


class MemoryWorkers:
    def __init__(self, storage):
        self.storage = storage
        self.active = {}
        self.deployments = []
        self.removals = []
        self.corrupt = None
        self.fail_after_promotion = False

    def url(self, name):
        return f"https://{name}.example"

    def deploy(self, name, module, origin, mount, generation):
        records = json.loads(module.split(b"\nexport default createWorker(")[1][:-3])
        assert origin == self.storage.base_url
        self.active[name] = {"files": dict(records), "mount": mount, "generation": generation}
        self.deployments.append(name)
        if "-verify-" not in name and self.fail_after_promotion:
            raise publication.ServiceError("injected ambiguous promotion")

    def remove(self, name, generation):
        assert "-verify-" in name and self.active[name]["generation"] == generation
        del self.active[name]
        self.removals.append(name)

    def verify(self, url, digest, size):
        parsed = urlsplit(url)
        name = parsed.hostname.removesuffix(".example")
        selected = self.active[name]
        path = unquote(parsed.path).removeprefix(selected["mount"])
        actual, length, _ = selected["files"][path]
        body = self.storage.content[actual]
        if actual == self.corrupt:
            body += b"changed"
        if actual != digest or length != size or hashlib.sha256(body).hexdigest() != digest:
            raise publication.PublicationError("public content identity differs")


def execute(site, storage, workers, **kwargs):
    return publication.publish(
        site,
        storage,
        workers,
        "riverhog",
        "https://riverhog.example/riverhog/",
        fresh=kwargs.pop("fresh", lambda: None),
        verify=workers.verify,
        jobs=2,
        **kwargs,
    )


def test_publication_verifies_every_unique_object_before_atomic_promotion_and_reuses_content(
    tmp_path,
):
    site = make_site(tmp_path / "site")
    storage = MemoryStorage()
    workers = MemoryWorkers(storage)
    result = execute(site, storage, workers)
    assert result["uploaded_objects"] == result["verified_objects"] == len(site.blobs)
    assert result["file_count"] == len(site.entries)
    assert set(workers.active) == {"riverhog"}
    assert workers.active["riverhog"]["files"] == {
        name: list(value) for name, value in site.entries.items()
    }
    assert workers.deployments[-1] == "riverhog" and len(workers.removals) == 1
    repeated = execute(site, storage, workers)
    assert repeated["uploaded_objects"] == 0
    assert repeated["existing_objects"] == repeated["verified_objects"] == len(site.blobs)


def test_failed_upload_cannot_deploy_or_discard_confirmed_content(tmp_path):
    site = make_site(tmp_path / "site")
    storage = MemoryStorage()
    storage.failure = sorted(site.blobs)[-1]
    workers = MemoryWorkers(storage)
    with pytest.raises(publication.PublicationError, match="upload failure"):
        execute(site, storage, workers)
    assert not workers.deployments
    assert storage.content


def test_failed_byte_verification_never_promotes_and_removes_only_its_verification_worker(tmp_path):
    site = make_site(tmp_path / "site")
    storage = MemoryStorage()
    workers = MemoryWorkers(storage)
    workers.corrupt = site.entries["development/element.html"][0]
    with pytest.raises(publication.PublicationError, match="identity differs"):
        execute(site, storage, workers)
    assert all("-verify-" in name for name in workers.deployments)
    assert not workers.active and len(workers.removals) == 1
    assert set(storage.content) == set(site.blobs)


def test_owner_change_after_verification_cannot_promote(tmp_path):
    site = make_site(tmp_path / "site")
    storage = MemoryStorage()
    workers = MemoryWorkers(storage)
    calls = []

    def fresh():
        calls.append(None)
        if len(calls) == 3:
            raise publication.PublicationError("main snapshot changed")

    with pytest.raises(publication.PublicationError, match="snapshot changed"):
        execute(site, storage, workers, fresh=fresh)
    assert len(calls) == 3
    assert not workers.active and len(workers.removals) == 1


def test_staging_qualifies_without_changing_the_production_worker(tmp_path):
    site = make_site(tmp_path / "site")
    storage = MemoryStorage()
    workers = MemoryWorkers(storage)
    result = execute(site, storage, workers, staging_only=True)
    assert result["staging_only"] is True
    assert not workers.active
    assert all("-verify-" in name for name in workers.deployments)


def test_uncertain_promotion_retains_complete_content_and_can_be_reconciled(tmp_path):
    site = make_site(tmp_path / "site")
    storage = MemoryStorage()
    workers = MemoryWorkers(storage)
    workers.fail_after_promotion = True
    with pytest.raises(publication.ServiceError, match="ambiguous promotion"):
        execute(site, storage, workers)
    assert set(storage.content) == set(site.blobs)
    assert set(workers.active) == {"riverhog"}
    workers.fail_after_promotion = False
    receipt = execute(site, storage, workers)
    assert receipt["uploaded_objects"] == 0
    assert receipt["verified_objects"] == len(site.blobs)


def test_provider_errors_cannot_print_tokens_urls_or_response_bodies(monkeypatch):
    sensitive = "credential-must-never-be-reported"

    def rejected(*args, **kwargs):
        raise HTTPError(
            "https://api.example/" + sensitive,
            403,
            sensitive,
            {},
            io.BytesIO(sensitive.encode()),
        )

    monkeypatch.setattr(publication, "urlopen", rejected)
    with pytest.raises(publication.ServiceError) as caught:
        publication._json_request(
            "authorization", "https://api.example/", headers={"Authorization": sensitive}
        )
    assert str(caught.value) == "authorization failed (HTTP 403)"
    assert sensitive not in str(caught.value)


def test_b2_retry_reconciles_an_uncertain_upload_instead_of_writing_again(tmp_path, monkeypatch):
    path = tmp_path / "payload"
    path.write_bytes(b"exact")
    digest = hashlib.sha256(b"exact").hexdigest()
    store = object.__new__(publication.B2Storage)
    store.api = "https://api.example"
    store.base_url = "https://storage.example/objects/"
    store.headers = {"Authorization": "fixture"}
    store.bucket_id = "fixture-bucket"
    store.prefix = "objects/"
    store.local = threading.local()
    outcomes = iter((False, True))
    monkeypatch.setattr(store, "_exists", lambda *_: next(outcomes))
    operations = []

    def request(operation, *args, **kwargs):
        operations.append(operation)
        if operation == "upload authorization":
            return {"uploadUrl": "https://upload.example/", "authorizationToken": "fixture"}
        assert kwargs["headers"]["X-Bz-Info-b2-cache-control"] == (
            "public%2C%20max-age%3D31536000%2C%20immutable"
        )
        assert b"".join(kwargs["body"]) == b"exact"
        raise publication.ServiceError("content upload")

    monkeypatch.setattr(publication, "_json_request", request)
    assert store.upload(digest, path, 5) is False
    assert operations == ["upload authorization", "content upload"]


def test_upload_chunks_reject_source_changes():
    digest = hashlib.sha256(b"expected").hexdigest()
    with pytest.raises(publication.PublicationError, match="changed during upload"):
        list(publication._verified_chunks(io.BytesIO(b"changed!"), digest, 8))


def test_public_download_retries_transient_routes_but_never_accepts_wrong_bytes(monkeypatch):
    calls = []
    sleeps = []
    data = b"exact"

    def transient(request, **kwargs):
        calls.append(request)
        if len(calls) == 1:
            raise HTTPError("https://docs.example/file", 404, "propagating", {}, io.BytesIO())
        response = io.BytesIO(data)
        response.status = 200
        return response

    monkeypatch.setattr(publication, "urlopen", transient)
    monkeypatch.setattr(publication.time, "sleep", sleeps.append)
    publication.verify_download("https://docs.example/file", hashlib.sha256(data).hexdigest(), 5)
    assert len(calls) == 2 and sleeps == [1]
    calls.clear()
    sleeps.clear()
    with pytest.raises(publication.PublicationError, match="identity differs"):
        publication.verify_download(
            "https://docs.example/file", hashlib.sha256(b"wrong").hexdigest(), 5
        )
    assert len(calls) == 2 and sleeps == [1]


def test_cleanup_command_removes_only_the_manifest_bound_verification_worker(tmp_path, monkeypatch):
    site = make_site(tmp_path / "site")
    storage = MemoryStorage()
    workers = MemoryWorkers(storage)
    candidate = publication.verification_name("riverhog", site.manifest_sha256)
    workers.deploy(
        candidate, site.worker_module(), storage.base_url, "/riverhog/", site.manifest_sha256
    )
    monkeypatch.setattr(publication, "CloudflareWorkers", lambda *_: workers)
    monkeypatch.setattr(publication, "_credential", lambda *_: "fixture")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "documentation_publish.py",
            "cleanup",
            "--site",
            str(tmp_path / "site"),
            "--worker",
            "riverhog",
        ],
    )
    assert publication.main() == 0
    assert not workers.active and workers.removals == [candidate]


def test_cloudflare_cleanup_cannot_remove_the_production_worker():
    worker = object.__new__(publication.CloudflareWorkers)
    with pytest.raises(publication.PublicationError, match="only a verification"):
        worker.remove("riverhog", "a" * 64)
