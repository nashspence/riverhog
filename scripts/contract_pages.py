#!/usr/bin/env python3
"""Assemble one verified development and immutable-version Pages site."""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
import subprocess
import tempfile
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

from contract_atlas.generation import BUILD_FILENAME, DEFAULT_OUTPUT, verify_candidate
from contract_atlas.github_publication import PRODUCT_TAG, GitHubPublication, require_fresh_snapshot
from contract_atlas.model import ContractAtlasError, canonical_bytes
from contract_atlas.publication import (
    DEFAULT_SITE_BUDGET,
    directory_files,
    verify_published_candidate,
)

ROOT = Path(__file__).resolve().parents[1]
PREVIEW_DIRECTORY = "contract-candidate"
RENDER_DIRECTORY = "riverhog-v1"
SITE_FILENAME = "site-manifest.json"


def _page(title: str, body: str) -> bytes:
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        f"<title>{html.escape(title)}</title><style>:root{{color-scheme:light dark}}"
        "body{font:1rem system-ui;max-width:70rem;margin:auto;padding:1rem}"
        "nav{display:grid;grid-template-columns:repeat(auto-fit,minmax(12rem,1fr));gap:1rem}"
        "nav a{display:block;border:1px solid;padding:1rem;border-radius:.5rem}"
        f"</style></head><body><h1>{html.escape(title)}</h1>{body}</body></html>"
    ).encode()


def _redirect(target: str) -> bytes:
    return (
        '<!doctype html><meta charset="utf-8">'
        f'<meta http-equiv="refresh" content="0;url={html.escape(target, quote=True)}">'
        f'<a href="{html.escape(target, quote=True)}">Continue</a>'
        f"<script>location.replace({json.dumps(target)}+location.search+location.hash)</script>"
    ).encode()


class _Links(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        for name, value in attrs:
            if value is not None and name in {"href", "src"} and tag != "link":
                self.links.append(value)


def validate_site_links(root: Path) -> None:
    for path in root.rglob("*.html"):
        parser = _Links()
        parser.feed(path.read_text(encoding="utf-8"))
        for link in parser.links:
            parts = urlsplit(link)
            if parts.scheme or parts.netloc or not parts.path:
                continue
            relative = unquote(parts.path)
            if relative.startswith("/riverhog/"):
                target = root / relative.removeprefix("/riverhog/")
            elif relative.startswith("/"):
                target = root / relative.lstrip("/")
            else:
                target = path.parent / relative
            target = target.resolve()
            if target.is_dir():
                target /= "index.html"
            if not target.is_relative_to(root.resolve()) or not target.is_file():
                raise ContractAtlasError(
                    f"aggregate site link does not resolve: {path.relative_to(root)} -> {link}"
                )


def _copy_verified(source: Path, target: Path, build: dict[str, Any]) -> None:
    files = directory_files(source)
    expected = {
        **build["files"],
        BUILD_FILENAME: hashlib.sha256(canonical_bytes(build)).hexdigest(),
    }
    if set(files) != set(expected):
        raise ContractAtlasError("candidate file set changed during site assembly")
    for name, path in files.items():
        payload = path.read_bytes()
        if hashlib.sha256(payload).hexdigest() != expected[name]:
            raise ContractAtlasError("candidate bytes changed during site assembly")
        output = target / name
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(payload)


def build_pages(
    source: Path,
    destination: Path,
    source_sha: str | None,
    *,
    releases: list[dict[str, Any]] | None = None,
    snapshot: dict[str, Any] | None = None,
    budget: int = DEFAULT_SITE_BUDGET,
) -> dict[str, Any]:
    """Assemble all verified versions without changing their rendered bytes."""

    if source_sha is not None and re.fullmatch(r"[0-9a-f]{40}", source_sha) is None:
        raise ContractAtlasError("Pages development requires an exact source commit SHA")
    development = verify_candidate(source, expected_source=source_sha)
    if development["documentation"] is not None:
        raise ContractAtlasError("development Pages must not include release Documentation Mode")
    if (
        not 0 < budget < 1_000_000_000
        or (destination.exists() and any(destination.iterdir()))
        or destination.is_symlink()
    ):
        raise ContractAtlasError(
            "Pages requires an empty output and a budget below the hosting limit"
        )
    products = sorted(
        releases or [], key=lambda entry: tuple(map(int, entry["tag"][1:].split("."))), reverse=True
    )
    tags = [entry["tag"] for entry in products]
    if len(set(tags)) != len(tags) or any(not PRODUCT_TAG.fullmatch(tag) for tag in tags):
        raise ContractAtlasError("aggregate site has duplicate or non-product versions")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".riverhog-site-", dir=destination.parent) as temporary:
        stage = Path(temporary) / "site"
        stage.mkdir()
        _copy_verified(source, stage / "development", development)
        inputs = [
            {
                "version": "development",
                "source_sha": development["source_sha"],
                "documentation": None,
                "renderer": development["renderer"],
                "toolchain": development["toolchain"],
                "closure_sha256": development["closure_sha256"],
                "audit_sha256": development["audit_sha256"],
                "build_manifest_sha256": hashlib.sha256(canonical_bytes(development)).hexdigest(),
            }
        ]
        for entry in products:
            build = verify_published_candidate(entry["root"])
            if build["documentation"] is None or build["documentation"]["tag"] != entry["tag"]:
                raise ContractAtlasError("historical final product requires bound documentation")
            _copy_verified(entry["root"], stage / entry["tag"], build)
            installation = entry["installation"]
            index_files = directory_files(installation["root"])
            if (
                installation["path"] != f"artifacts/{entry['tag']}/simple/"
                or installation["tag"] != entry["tag"]
                or installation["source_sha"] != build["source_sha"]
                or set(index_files) != set(installation["files"])
            ):
                raise ContractAtlasError("historical installation index has a different binding")
            for name, path in index_files.items():
                payload = path.read_bytes()
                if (
                    not name.startswith(installation["path"])
                    or hashlib.sha256(payload).hexdigest() != installation["files"][name]
                ):
                    raise ContractAtlasError(
                        "historical installation index changed during assembly"
                    )
                output = stage / name
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_bytes(payload)
            inputs.append(
                {
                    **{
                        key: entry[key]
                        for key in (
                            "release_id",
                            "release_manifest_sha256",
                            "attestation_sha256",
                            "assets",
                        )
                    },
                    "version": entry["tag"],
                    "installation": {
                        key: value for key, value in installation.items() if key != "root"
                    },
                    **{
                        key: build[key]
                        for key in (
                            "source_sha",
                            "documentation",
                            "renderer",
                            "toolchain",
                            "closure_sha256",
                            "audit_sha256",
                        )
                    },
                    "build_manifest_sha256": hashlib.sha256(canonical_bytes(build)).hexdigest(),
                }
            )
        for version in ["development", *tags]:
            links = "".join(
                f'<a href="../{tag}/">{html.escape(tag)}</a>' for tag in ["development", *tags]
            )
            body = "<p>Development is unreleased.</p>" if version == "development" else ""
            body += '<p><a href="riverhog-v1/">Contract</a></p>'
            if (
                next(item for item in inputs if item["version"] == version)["documentation"]
                is not None
            ):
                body += '<p><a href="riverhog-v1/?docs=1">Documentation</a></p>'
            body += '<nav aria-label="Versions">' + links + "</nav>"
            (stage / version / "index.html").write_bytes(_page(f"Riverhog {version}", body))
        (stage / "index.html").write_bytes(
            _page(
                "Riverhog contracts",
                "<p>Development is unreleased.</p>"
                + ("" if products else "<p>No final product releases have been published.</p>")
                + '<nav aria-label="Versions">'
                + "".join(f'<a href="{tag}/">{tag}</a>' for tag in ["development", *tags])
                + "</nav>",
            )
        )
        preview = stage / PREVIEW_DIRECTORY
        _copy_verified(source, preview, development)
        (preview / "index.html").write_bytes(_redirect("riverhog-v1/"))
        canonical = stage / (tags[0] if tags else "development")
        identifiers = canonical / "identifiers"
        if identifiers.exists():
            for name, path in directory_files(identifiers).items():
                target = stage / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(path.read_bytes())
        (stage / "v1").mkdir(exist_ok=True)
        (stage / "v1/index.html").write_bytes(
            _redirect("../" + (tags[0] if tags else "development") + "/")
        )
        (stage / ".nojekyll").touch()
        validate_site_links(stage)
        manifest = {
            "format": "riverhog-contract-pages-site/v1",
            "inputs": inputs,
            "latest_product_release": tags[0] if tags else None,
            "snapshot": snapshot,
            "hosting_budget_bytes": budget,
            "site_bytes": 0,
            "files": {
                name: hashlib.sha256(path.read_bytes()).hexdigest()
                for name, path in directory_files(stage).items()
            },
        }
        payload_bytes = sum(path.stat().st_size for path in directory_files(stage).values())
        while True:
            total = payload_bytes + len(canonical_bytes(manifest))
            if manifest["site_bytes"] == total:
                break
            manifest["site_bytes"] = total
        if total > budget:
            raise ContractAtlasError(
                f"assembled Pages site is {total} bytes; operational budget is {budget}; "
                "no versions omitted"
            )
        (stage / SITE_FILENAME).write_bytes(canonical_bytes(manifest))
        if destination.exists():
            destination.rmdir()
        stage.rename(destination)
    return manifest


def require_requested_product(
    snapshot: dict[str, Any], tag: str | None, release_id: int | None, digest: str | None
) -> None:
    if tag is None and release_id is None and digest is None:
        return
    products = [
        entry for entry in snapshot["products"] if entry["tag"] == tag and entry["id"] == release_id
    ]
    if (
        len(products) != 1
        or not re.fullmatch(r"[0-9a-f]{64}", str(digest))
        or not any(
            asset["name"] == "release-manifest.json" and asset["digest"] == "sha256:" + str(digest)
            for asset in products[0]["assets"]
        )
    ):
        raise ContractAtlasError("requested immutable product is absent or changed")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-sha")
    parser.add_argument("--candidate", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--repository", default="nashspence/riverhog")
    parser.add_argument("--gh", default="gh")
    parser.add_argument("--budget", type=int, default=DEFAULT_SITE_BUDGET)
    parser.add_argument("--check-fresh", type=Path)
    parser.add_argument("--release-tag")
    parser.add_argument("--release-id", type=int)
    parser.add_argument("--release-manifest-sha256")
    args = parser.parse_args()
    try:
        remote = GitHubPublication(args.repository, executable=args.gh)
        if args.check_fresh is not None:
            manifest = json.loads(args.check_fresh.read_bytes())
            require_fresh_snapshot(manifest["snapshot"], remote.snapshot())
            return 0
        if args.source_sha is None or args.output is None:
            raise ContractAtlasError("Pages publication requires source SHA and output")
        snapshot = remote.snapshot()
        require_requested_product(
            snapshot, args.release_tag, args.release_id, args.release_manifest_sha256
        )
        if snapshot["main"] != args.source_sha:
            raise ContractAtlasError(
                "development source is stale; publish the current green main commit"
            )
        with tempfile.TemporaryDirectory(prefix="riverhog-published-versions-") as temporary:
            releases = [
                remote.load_product(entry, Path(temporary) / entry["tag"], budget=args.budget)
                for entry in snapshot["products"]
            ]
            site = build_pages(
                args.candidate,
                args.output,
                args.source_sha,
                releases=releases,
                snapshot=snapshot,
                budget=args.budget,
            )
        require_fresh_snapshot(snapshot, remote.snapshot())
        print(
            canonical_bytes(
                {key: site[key] for key in ("format", "latest_product_release", "site_bytes")}
            ).decode()
        )
    except (
        ContractAtlasError,
        subprocess.CalledProcessError,
        OSError,
        KeyError,
        TypeError,
        ValueError,
    ) as exc:
        parser.exit(2, f"Pages assembly failed: {exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
