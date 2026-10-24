"""Verified static-site inventory, immutable content, and existing-URL routing."""

from __future__ import annotations

import hashlib
import json
import mimetypes
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit

from .model import ContractAtlasError, canonical_bytes
from .publication import directory_files, file_sha256, safe_relative

SITE_FILENAME = "site-manifest.json"
RELAY_FILENAME = "relay-manifest.json"
PAGES_LIMIT = 1_000_000_000
WORKER_MODULE_LIMIT = 64 * 1024 * 1024


def public_base_url(value: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or not parsed.path.endswith("/")
        or "\\" in value
        or any(ord(character) < 33 for character in value)
        or any(part in {".", ".."} for part in parsed.path.split("/"))
    ):
        raise ContractAtlasError("publication requires an HTTPS base URL ending in a slash")
    return value


def content_type(name: str) -> str:
    suffix = Path(name).suffix.lower()
    known = {
        ".html": "text/html; charset=utf-8",
        ".json": "application/json",
        ".js": "text/javascript; charset=utf-8",
        ".mjs": "text/javascript; charset=utf-8",
        ".css": "text/css; charset=utf-8",
        ".svg": "image/svg+xml",
        ".txt": "text/plain; charset=utf-8",
    }
    return known.get(suffix) or mimetypes.guess_type(name)[0] or "application/octet-stream"


@dataclass(frozen=True)
class Site:
    manifest: dict[str, Any]
    manifest_sha256: str
    entries: dict[str, tuple[str, int, str]]
    blobs: dict[str, Path]

    @classmethod
    def read(cls, root: Path) -> Site:
        files = directory_files(root)
        try:
            raw = files[SITE_FILENAME].read_bytes()
            manifest = json.loads(raw)
            expected = manifest["files"]
            if (
                manifest["format"] != "riverhog-contract-pages-site/v1"
                or raw != canonical_bytes(manifest)
                or not isinstance(expected, dict)
                or set(files) != set(expected) | {SITE_FILENAME}
                or SITE_FILENAME in expected
            ):
                raise ContractAtlasError("aggregate publication inventory differs")
            expected = dict(expected, **{SITE_FILENAME: hashlib.sha256(raw).hexdigest()})
            entries: dict[str, tuple[str, int, str]] = {}
            blobs: dict[str, Path] = {}
            total = 0
            for name, path in sorted(files.items()):
                safe_relative(name)
                digest = file_sha256(path)
                if digest != expected[name]:
                    raise ContractAtlasError("aggregate publication file identity differs")
                size = path.stat().st_size
                total += size
                entries[name] = (digest, size, content_type(name))
                blobs.setdefault(digest, path)
            if total != manifest["site_bytes"] or not 0 < total <= manifest["hosting_budget_bytes"]:
                raise ContractAtlasError("aggregate publication byte accounting differs")
            return cls(manifest, expected[SITE_FILENAME], entries, blobs)
        except (KeyError, TypeError, ValueError) as exc:
            raise ContractAtlasError("aggregate publication inventory is invalid") from exc

    def worker_module(self) -> bytes:
        template = Path(__file__).with_name("documentation_worker.js").read_bytes()
        entries = [[name, list(value)] for name, value in sorted(self.entries.items())]
        module = template + b"\nexport default createWorker(" + canonical_bytes(entries) + b");\n"
        if len(module) > WORKER_MODULE_LIMIT:
            raise ContractAtlasError("complete publication exceeds the Worker module limit")
        return module


def build_relay(
    source: Path,
    destination: Path,
    canonical_url: str,
    *,
    budget: int = PAGES_LIMIT,
) -> dict[str, Any]:
    """Keep machine-readable responses exact and redirect rendered documents."""

    from contract_pages import _redirect

    canonical_url = public_base_url(canonical_url)
    site = Site.read(source)
    if (
        not 0 < budget <= PAGES_LIMIT
        or destination.is_symlink()
        or (destination.exists() and any(destination.iterdir()))
        or RELAY_FILENAME in site.entries
    ):
        raise ContractAtlasError("Pages routing requires an empty output within its hosting limit")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".riverhog-routing-", dir=destination.parent
    ) as temporary:
        stage = Path(temporary) / "site"
        stage.mkdir()
        for name in site.entries:
            target = stage / name
            target.parent.mkdir(parents=True, exist_ok=True)
            if name.endswith(".html") and not name.startswith("artifacts/"):
                target.write_bytes(_redirect(canonical_url + quote(name, safe="/")))
            else:
                shutil.copyfile(source / name, target)
        files = directory_files(stage)
        manifest = {
            "format": "riverhog-documentation-routing/v1",
            "canonical_url": canonical_url,
            "site_manifest_sha256": site.manifest_sha256,
            "files": {name: file_sha256(path) for name, path in sorted(files.items())},
            "site_bytes": 0,
        }
        payload_bytes = sum(path.stat().st_size for path in files.values())
        while True:
            size = payload_bytes + len(canonical_bytes(manifest))
            if manifest["site_bytes"] == size:
                break
            manifest["site_bytes"] = size
        if size > budget:
            raise ContractAtlasError("complete existing-URL routing exceeds the Pages budget")
        (stage / RELAY_FILENAME).write_bytes(canonical_bytes(manifest))
        if destination.exists():
            destination.rmdir()
        stage.rename(destination)
    return manifest
