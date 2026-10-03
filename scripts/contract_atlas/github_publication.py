"""Read product releases through GitHub's immutable release and asset attestations."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from pathlib import Path
from typing import Any

from .model import ContractAtlasError, canonical_bytes
from .publication import DEFAULT_SITE_BUDGET, file_sha256, safe_relative, unpack_release_contract

PRODUCT_TAG = re.compile(r"v1\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\Z")


def select_products(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Select every final product release, including ones beyond the first API page."""

    products: dict[str, dict[str, Any]] = {}
    for row in rows:
        tag = row.get("tag_name")
        if not isinstance(tag, str) or not PRODUCT_TAG.fullmatch(tag) or row.get("draft") is True:
            continue
        if row.get("prerelease") is not False or row.get("immutable") is not True:
            raise ContractAtlasError(f"final product release is not immutable: {tag}")
        if tag in products:
            raise ContractAtlasError("product release enumeration repeats a tag")
        names = [asset["name"] for asset in row["assets"]]
        if len(names) != len(set(names)) or "release-manifest.json" not in names:
            raise ContractAtlasError(f"product release asset inventory is incomplete: {tag}")
        products[tag] = row
    return sorted(
        products.values(),
        key=lambda row: tuple(map(int, row["tag_name"][1:].split("."))),
        reverse=True,
    )


class GitHubPublication:
    def __init__(self, repository: str, *, executable: str = "gh") -> None:
        if re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository) is None:
            raise ContractAtlasError("GitHub publication requires an explicit repository")
        self.repository = repository
        self.executable = executable

    def command(self, *args: str) -> bytes:
        return subprocess.check_output([self.executable, *args])

    def api(self, endpoint: str) -> Any:
        return json.loads(self.command("api", f"repos/{self.repository}/{endpoint}"))

    def releases(self) -> list[dict[str, Any]]:
        return self._pages("releases?per_page=100")

    def assets(self, release_id: int) -> list[dict[str, Any]]:
        return self._pages(f"releases/{release_id}/assets?per_page=100")

    def _pages(self, endpoint: str) -> list[dict[str, Any]]:
        payload = self.command("api", "--paginate", f"repos/{self.repository}/{endpoint}").decode()
        decoder, rows = json.JSONDecoder(), []
        while payload.strip():
            page, end = decoder.raw_decode(payload.lstrip())
            if not isinstance(page, list):
                raise ContractAtlasError("GitHub release pagination did not return arrays")
            rows.extend(page)
            payload = payload.lstrip()[end:]
        return rows

    def tag_commit(self, tag: str) -> str:
        obj = self.api("git/ref/tags/" + tag)["object"]
        seen: set[str] = set()
        while obj["type"] == "tag":
            if obj["sha"] in seen:
                raise ContractAtlasError("GitHub tag contains a reference cycle")
            seen.add(obj["sha"])
            obj = self.api("git/tags/" + obj["sha"])["object"]
        if obj["type"] != "commit" or re.fullmatch(r"[0-9a-f]{40}", obj["sha"]) is None:
            raise ContractAtlasError("published tag does not identify an exact commit")
        return str(obj["sha"])

    def snapshot(self) -> dict[str, Any]:
        rows = self.releases()
        for row in rows:
            if PRODUCT_TAG.fullmatch(str(row.get("tag_name"))) and row.get("draft") is False:
                row["assets"] = self.assets(row["id"])
        products = select_products(rows)
        return {
            "main": self.api("git/ref/heads/main")["object"]["sha"],
            "products": [
                {
                    "tag": row["tag_name"],
                    "id": row["id"],
                    "source_sha": self.tag_commit(row["tag_name"]),
                    "assets": sorted(
                        (
                            {key: asset[key] for key in ("id", "name", "digest", "size")}
                            for asset in row["assets"]
                        ),
                        key=lambda asset: asset["name"],
                    ),
                }
                for row in products
            ],
        }

    def download_asset(self, tag: str, asset: dict[str, Any], destination: Path) -> None:
        safe_relative(asset["name"])
        with destination.open("xb") as stream:
            subprocess.run(
                [
                    self.executable,
                    "api",
                    "-H",
                    "Accept: application/octet-stream",
                    f"repos/{self.repository}/releases/assets/{asset['id']}",
                ],
                stdout=stream,
                check=True,
            )
        digest = file_sha256(destination)
        if asset["digest"] != "sha256:" + digest or destination.stat().st_size != asset["size"]:
            raise ContractAtlasError(
                "downloaded release asset differs from the selected immutable input"
            )
        self.command("release", "verify-asset", tag, str(destination), "--repo", self.repository)

    def load_product(
        self, selected: dict[str, Any], destination: Path, *, budget: int = DEFAULT_SITE_BUDGET
    ) -> dict[str, Any]:
        """Verify upstream attestations before unpacking the bound historical render."""

        tag = selected["tag"]
        attestation = self.command(
            "release", "verify", tag, "--repo", self.repository, "--format", "json"
        )
        destination.mkdir(parents=True)
        assets = {item["name"]: item for item in selected["assets"]}
        if assets["release-manifest.json"]["size"] > budget:
            raise ContractAtlasError("published manifest exceeds the operational hosting budget")
        self.download_asset(
            tag, assets["release-manifest.json"], destination / "release-manifest.json"
        )
        raw = (destination / "release-manifest.json").read_bytes()
        manifest = json.loads(raw)
        if (
            raw != canonical_bytes(manifest)
            or manifest.get("format") != "riverhog-release/v1"
            or manifest.get("tag") != tag
            or manifest.get("version") != tag[1:]
            or manifest.get("source_sha") != selected["source_sha"]
        ):
            raise ContractAtlasError("published product manifest differs from its immutable tag")
        contract = manifest["contract"]
        required = (contract["file"], contract["render"]["file"])
        if contract["source_sha"] != selected["source_sha"] or any(
            name not in assets for name in required
        ):
            raise ContractAtlasError("published product lacks bound contract/render assets")
        for name in required:
            if assets[name]["size"] > budget:
                raise ContractAtlasError("published asset exceeds the operational hosting budget")
            self.download_asset(tag, assets[name], destination / safe_relative(name))
        candidate = destination / "candidate"
        build = unpack_release_contract(destination, contract, candidate, budget=budget)
        if build["documentation"] is not None and build["documentation"]["tag"] != tag:
            raise ContractAtlasError("published documentation belongs to another product version")
        return {
            "tag": tag,
            "root": candidate,
            "release_id": selected["id"],
            "release_manifest_sha256": hashlib.sha256(raw).hexdigest(),
            "attestation_sha256": hashlib.sha256(attestation).hexdigest(),
            "assets": {
                name: assets[name]["digest"] for name in ("release-manifest.json", *required)
            },
        }

    def publish_asset_set(
        self,
        tag: str,
        source_sha: str,
        files: dict[str, Path],
        *,
        title: str,
        notes: Path,
        latest: bool = False,
    ) -> dict[str, Any]:
        """Complete and verify a draft before immutable publication; resume only exact inputs."""

        if self.tag_commit(tag) != source_sha:
            raise ContractAtlasError("publication tag differs from the approved source commit")
        if self.api("immutable-releases").get("enabled") is not True:
            raise ContractAtlasError("immutable releases must be enabled before publication")
        expected = {}
        for name, path in files.items():
            if safe_relative(name) != path.name or not path.is_file() or path.is_symlink():
                raise ContractAtlasError("release assets require unique regular basenames")
            expected[name] = {"digest": "sha256:" + file_sha256(path), "size": path.stat().st_size}
        if not expected:
            raise ContractAtlasError("release publication requires a complete nonempty asset set")
        existing = [row for row in self.releases() if row["tag_name"] == tag]
        if len(existing) > 1:
            raise ContractAtlasError("release publication repeats a tag")
        if not existing:
            self.command(
                "release",
                "create",
                tag,
                "--repo",
                self.repository,
                "--verify-tag",
                "--draft",
                "--title",
                title,
                "--notes-file",
                str(notes),
                "--latest=false",
            )
        selected = self.api("releases/tags/" + tag)
        observed = {
            asset["name"]: {"digest": asset["digest"], "size": asset["size"]}
            for asset in self.assets(selected["id"])
        }
        if any(name not in expected or value != expected[name] for name, value in observed.items()):
            raise ContractAtlasError(
                "existing release assets differ; no remote assets were overwritten"
            )
        if selected["draft"] is not True:
            if selected.get("immutable") is not True or observed != expected:
                raise ContractAtlasError(
                    "published release differs from the complete immutable asset set"
                )
        else:
            for name in sorted(set(expected) - set(observed)):
                self.command("release", "upload", tag, str(files[name]), "--repo", self.repository)
            selected = self.api("releases/tags/" + tag)
            observed = {
                asset["name"]: {"digest": asset["digest"], "size": asset["size"]}
                for asset in self.assets(selected["id"])
            }
            if observed != expected or self.tag_commit(tag) != source_sha:
                raise ContractAtlasError("draft release failed complete asset/tag verification")
            self.command(
                "release",
                "edit",
                tag,
                "--draft=false",
                "--repo",
                self.repository,
                "--latest=" + str(latest).lower(),
            )
        published = self.api("releases/tags/" + tag)
        if published.get("immutable") is not True or published.get("draft") is not False:
            raise ContractAtlasError("publication did not produce an immutable release")
        final_assets = {
            asset["name"]: {"digest": asset["digest"], "size": asset["size"]}
            for asset in self.assets(published["id"])
        }
        if final_assets != expected or self.tag_commit(tag) != source_sha:
            raise ContractAtlasError(
                "immutable publication differs from the reviewed asset/tag set"
            )
        self.command("release", "verify", tag, "--repo", self.repository)
        for name in sorted(files):
            self.command(
                "release", "verify-asset", tag, str(files[name]), "--repo", self.repository
            )
        return {
            "tag": tag,
            "source_sha": source_sha,
            "release_id": published["id"],
            "immutable": True,
            "assets": expected,
        }


def require_fresh_snapshot(built: dict[str, Any], current: dict[str, Any]) -> None:
    if canonical_bytes(built) != canonical_bytes(current):
        raise ContractAtlasError(
            "main or the complete published product set changed; rebuild the aggregate site"
        )
