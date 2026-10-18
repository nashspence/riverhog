"""Verify and preserve a release's exact Python Simple index snapshot."""

from __future__ import annotations

import hashlib
import json
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import quote

from packaging.utils import canonicalize_name

from .model import ContractAtlasError, canonical_bytes
from .publication import directory_files, extract_verified_tar, file_sha256, safe_relative


class _Anchors(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "a":
            self.hrefs.extend(value for key, value in attrs if key == "href" and value is not None)


def _links(path: Path) -> list[str]:
    parser = _Anchors()
    parser.feed(path.read_text(encoding="utf-8"))
    return parser.hrefs


def unpack_installation_index(
    manifest_path: Path,
    archive: Path,
    destination: Path,
    *,
    repository: str,
    tag: str,
    source_sha: str,
    assets: dict[str, dict[str, Any]],
    budget: int,
) -> dict[str, Any]:
    from release_installation import INSTALLATION_FORMAT

    raw = manifest_path.read_bytes()
    manifest = json.loads(raw)
    index = manifest["index"]
    path = f"artifacts/{tag}/simple/"
    owner, project = repository.split("/")
    url = f"https://{owner}.github.io/{project}/{path}"
    projects = index["first_party_projects"]
    names = [str(canonicalize_name(name, validate=True)) for name in projects]
    if (
        raw != canonical_bytes(manifest)
        or manifest.get("format") != INSTALLATION_FORMAT
        or manifest.get("tag") != tag
        or manifest.get("version") != tag[1:]
        or manifest.get("source_sha") != source_sha
        or index.get("path") != path
        or index.get("url") != url
        or not names
        or len(names) != len(set(names))
        or set(projects) != set(manifest["wheels"])
        or index["snapshot_asset"] != archive.name
    ):
        raise ContractAtlasError("published installation index differs from its product binding")
    extract_verified_tar(archive, index["snapshot_sha256"], destination, budget=budget)
    files = directory_files(destination)
    expected = {path + "index.html", *(path + name + "/index.html" for name in names)}
    if set(files) != expected or sorted(_links(files[path + "index.html"])) != sorted(
        name + "/" for name in names
    ):
        raise ContractAtlasError("published installation index has a different project inventory")
    base = f"https://github.com/{repository}/releases/download/{tag}/"
    for project_name, name in zip(projects, names, strict=True):
        wheel = manifest["wheels"][project_name]
        asset = safe_relative(wheel["asset"])
        if (
            asset not in assets
            or assets[asset]["digest"] != "sha256:" + wheel["sha256"]
            or assets[asset]["size"] != wheel["size"]
            or _links(files[path + name + "/index.html"])
            != [base + quote(asset) + "#sha256=" + wheel["sha256"]]
        ):
            raise ContractAtlasError("published installation index differs from its wheel assets")
    return {
        "root": destination,
        "path": path,
        "url": url,
        "source_sha": source_sha,
        "tag": tag,
        "manifest_sha256": hashlib.sha256(raw).hexdigest(),
        "snapshot_sha256": file_sha256(archive),
        "files": {name: file_sha256(file) for name, file in files.items()},
    }
