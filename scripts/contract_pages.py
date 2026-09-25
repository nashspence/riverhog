#!/usr/bin/env python3
"""Package the checked contract candidate for its protected Pages preview."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
from pathlib import Path
from typing import cast

from contract_atlas.html_rendering import validate_render
from contract_atlas.model import ContractAtlasError, canonical_bytes, canonical_sha256
from contract_atlas.records import AUDIT_FILENAME, load_bundle

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "qualification/contracts"
RENDER_DIRECTORY = "riverhog-v1"
PREVIEW_DIRECTORY = "contract-candidate"
SOURCE_SHA = re.compile(r"[0-9a-f]{40}\Z")


def build_pages(source: Path, destination: Path, source_sha: str) -> dict[str, object]:
    """Copy one exact checked candidate under the project-site preview path."""

    if not SOURCE_SHA.fullmatch(source_sha):
        raise ContractAtlasError("Pages candidate requires an exact source commit SHA")
    bundle = load_bundle(source / "riverhog-v1.json")
    render = source / RENDER_DIRECTORY
    manifest_path = render / "manifest.json"
    try:
        manifest = json.loads(manifest_path.read_bytes())
    except (OSError, json.JSONDecodeError) as exc:
        raise ContractAtlasError("contract render manifest is unavailable") from exc
    if (
        not isinstance(manifest, dict)
        or manifest.get("format") != "riverhog-contract-render-manifest/v1"
        or manifest.get("closure_sha256") != canonical_sha256(bundle.closure)
        or manifest.get("audit_sha256") != canonical_sha256(bundle.audit)
        or not isinstance(manifest.get("files"), dict)
    ):
        raise ContractAtlasError("contract render manifest has mismatched inputs")
    expected = cast(dict[str, str], manifest["files"])
    actual = {
        path.relative_to(render).as_posix(): path
        for path in render.rglob("*")
        if path.is_file() and path != manifest_path
    }
    if set(actual) != set(expected):
        raise ContractAtlasError("contract render file set differs from its manifest")
    files = {f"{RENDER_DIRECTORY}/{name}": path.read_bytes() for name, path in actual.items()}
    for name, payload in files.items():
        if (
            hashlib.sha256(payload).hexdigest()
            != expected[name.removeprefix(f"{RENDER_DIRECTORY}/")]
        ):
            raise ContractAtlasError(f"contract render file identity mismatch: {name}")
    files[f"{RENDER_DIRECTORY}/manifest.json"] = manifest_path.read_bytes()
    validate_render(files)
    if destination.exists() and any(destination.iterdir()):
        raise ContractAtlasError("Pages destination must be empty")
    destination.mkdir(parents=True, exist_ok=True)
    preview = destination / PREVIEW_DIRECTORY
    preview.mkdir()
    shutil.copy2(source / "riverhog-v1.json", preview / "riverhog-v1.json")
    shutil.copy2(source / AUDIT_FILENAME, preview / AUDIT_FILENAME)
    shutil.copytree(render, preview / RENDER_DIRECTORY)
    root_page = (
        '<!doctype html><html lang="en"><meta charset="utf-8">'
        "<title>Riverhog contract candidate</title><h1>Riverhog contract candidate</h1>"
        "<p>This candidate has not been accepted as a v1 release.</p>"
        '<a href="contract-candidate/riverhog-v1/">Read the Contract Render</a></html>\n'
    )
    (destination / "index.html").write_text(root_page, encoding="utf-8")
    (preview / "index.html").write_text(
        '<!doctype html><html lang="en"><meta charset="utf-8">'
        "<title>Riverhog contract candidate</title>"
        '<a href="riverhog-v1/">Read the Contract Render</a></html>\n',
        encoding="utf-8",
    )
    (destination / ".nojekyll").touch()
    build = {
        "format": "riverhog-contract-preview-build/v1",
        "source_sha": source_sha,
        "closure_sha256": canonical_sha256(bundle.closure),
        "audit_sha256": canonical_sha256(bundle.audit),
        "render_manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        "path": f"{PREVIEW_DIRECTORY}/{RENDER_DIRECTORY}/",
    }
    (preview / "build-manifest.json").write_bytes(canonical_bytes(build))
    return build


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        print(json.dumps(build_pages(SOURCE, args.output, args.source_sha), sort_keys=True))
    except ContractAtlasError as exc:
        parser.exit(2, f"Pages candidate failed: {exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
