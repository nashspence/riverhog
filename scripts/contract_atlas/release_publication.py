"""Source-qualified preparation and protected immutable release publication."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .github_publication import GitHubPublication, select_products
from .model import ContractAtlasError, canonical_bytes
from .publication import directory_files, file_sha256


def validate_qualification(record: dict[str, Any], source_sha: str, version: str) -> None:
    if (
        record.get("format") != "riverhog-release-qualification/v1"
        or record.get("source_sha") != source_sha
        or record.get("version") != version
        or record.get("qualification_mode") != "prospective"
        or record.get("published") is not False
        or any(
            record.get(key) != "passed"
            for key in (
                "required_checks",
                "operation_matrix",
                "database_contract",
                "release_evidence",
            )
        )
        or record.get("github_governance") != "actions-observable-passed"
    ):
        raise ContractAtlasError("release preparation requires complete exact-source qualification")


def collect_qualification(
    remote: GitHubPublication,
    run_id: int,
    source_sha: str,
    version: str,
    destination: Path,
) -> Path:
    run = remote.api(f"actions/runs/{run_id}")
    if (
        run.get("path") != ".github/workflows/release-qualification.yml"
        or run.get("status") != "completed"
        or run.get("conclusion") != "success"
        or run.get("repository", {}).get("full_name") != remote.repository
    ):
        raise ContractAtlasError(
            "selected qualification run is not a successful repository qualification"
        )
    remote.command(
        "run",
        "download",
        str(run_id),
        "--repo",
        remote.repository,
        "--name",
        "release-qualification-" + source_sha,
        "--dir",
        str(destination),
    )
    path = destination / "qualification.json"
    validate_qualification(json.loads(path.read_bytes()), source_sha, version)
    return path


def publication_assets(directory: Path) -> dict[str, Path]:
    """GitHub assets are flat; preserve the signed logical tree in its checksum paths."""

    paths = directory_files(directory)
    assets: dict[str, Path] = {}
    for path in paths.values():
        if path.name in assets:
            raise ContractAtlasError("release evidence has colliding GitHub asset basenames")
        assets[path.name] = path
    return assets


def collect_history(
    remote: GitHubPublication, version: str, destination: Path, *, exclude_tag: str | None = None
) -> tuple[dict[str, str] | None, tuple[Path, ...]]:
    """Resolve immutable predecessors from product manifests, independent of Actions retention."""

    rows = remote.releases()
    for row in rows:
        if str(row.get("tag_name", "")).startswith("v1.") and row.get("draft") is False:
            row["assets"] = remote.assets(row["id"])
    products = [row for row in select_products(rows) if row["tag_name"] != exclude_tag]
    destination.mkdir(parents=True)
    manifests = []
    current = tuple(map(int, version.split(".")))
    for row in products:
        tag = row["tag_name"]
        if tuple(map(int, tag[1:].split("."))) >= current:
            raise ContractAtlasError(
                "prepared version must follow the current immutable product head"
            )
        remote.command("release", "verify", tag, "--repo", remote.repository)
        asset = next(asset for asset in row["assets"] if asset["name"] == "release-manifest.json")
        path = destination / (tag + ".json")
        remote.download_asset(tag, asset, path)
        raw = path.read_bytes()
        manifest = json.loads(raw)
        if (
            raw != canonical_bytes(manifest)
            or manifest.get("tag") != tag
            or manifest.get("source_sha") != remote.tag_commit(tag)
        ):
            raise ContractAtlasError("immutable predecessor manifest differs from its tag")
        manifests.append(path)
    previous = (
        None
        if not manifests
        else {"tag": products[0]["tag_name"], "manifest_sha256": file_sha256(manifests[0])}
    )
    return previous, tuple(manifests)


def publish_prepared_release(
    root: Path,
    evidence: Path,
    *,
    signing_key: Path,
    public_key: Path,
    preparation_public_key: Path,
    preparation_run: int,
    qualification_run: int,
    expected_previous: dict[str, str] | None = None,
    historical_manifests: tuple[Path, ...] = (),
    remote: GitHubPublication | None = None,
) -> dict[str, Any]:
    import tempfile

    import github_governance
    import release

    selected = remote or GitHubPublication(
        str(release._load_config(root)["governance"]["repository"])
    )
    manifest = json.loads((evidence / "release-manifest.json").read_bytes())
    tag, source_sha, version = manifest["tag"], manifest["source_sha"], manifest["version"]
    if (
        os.environ.get("GITHUB_ACTIONS") != "true"
        or os.environ.get("GITHUB_SHA") != source_sha
        or os.environ.get("GITHUB_REF") != "refs/tags/" + tag
    ):
        raise ContractAtlasError(
            "product publication must run from its tag in the protected release workflow"
        )
    github_governance.check(scope="complete")
    preparation = selected.api(f"actions/runs/{preparation_run}")
    if (
        preparation.get("path") != ".github/workflows/release-preparation.yml"
        or preparation.get("conclusion") != "success"
        or preparation.get("status") != "completed"
        or preparation.get("repository", {}).get("full_name") != selected.repository
    ):
        raise ContractAtlasError(
            "prepared artifacts lack successful repository preparation provenance"
        )
    if release._source_sha(root) != source_sha or selected.tag_commit(tag) != source_sha:
        raise ContractAtlasError("prepared source differs from the publication checkout/tag")
    with tempfile.TemporaryDirectory(prefix="riverhog-publication-proof-") as temporary:
        scratch = Path(temporary)
        previous, history = collect_history(selected, version, scratch / "history", exclude_tag=tag)
        if expected_previous is not None and expected_previous != previous:
            raise ContractAtlasError(
                "publication predecessor differs from immutable product history"
            )
        expected_previous, historical_manifests = previous, history
        selected.command(
            "run",
            "download",
            str(preparation_run),
            "--repo",
            selected.repository,
            "--name",
            "release-preparation-" + source_sha,
            "--dir",
            str(scratch / "prepared"),
        )
        trusted = directory_files(scratch / "prepared/evidence")
        observed = directory_files(evidence)
        if (
            set(trusted) != set(observed)
            or any(file_sha256(trusted[name]) != file_sha256(observed[name]) for name in trusted)
            or preparation_public_key.read_bytes()
            != (scratch / "prepared/evidence.preparation.pub").read_bytes()
        ):
            raise ContractAtlasError("prepared bytes/key differ from the selected trusted artifact")
        release.verify_release_evidence(
            root,
            evidence,
            public_key=preparation_public_key,
            expected_previous=expected_previous,
            historical_manifest_paths=historical_manifests,
        )
        qualified = collect_qualification(
            selected, qualification_run, source_sha, version, scratch / "qualified"
        )
        recorded = manifest["qualification"]
        if (
            recorded["run_id"] != qualification_run
            or recorded["sha256"] != file_sha256(qualified)
            or file_sha256(evidence / recorded["file"]) != file_sha256(qualified)
        ):
            raise ContractAtlasError("prepared qualification differs from the selected trusted run")
        if (
            manifest["contract"]["documentation"] is None
            or manifest["contract"]["preparation"] is None
        ):
            raise ContractAtlasError(
                "product publication requires bound authored and prepared-source inputs"
            )
        assets = publication_assets(evidence)
        # Re-sign the same reviewed checksum payload with the external publication key.
        release._sign_checksums(
            evidence, signing_key=signing_key, version=version, source_sha=source_sha
        )
        release.verify_release_evidence(
            root,
            evidence,
            public_key=public_key,
            expected_previous=expected_previous,
            historical_manifest_paths=historical_manifests,
        )
        notes = scratch / "release-notes.md"
        notes.write_text(
            f"Riverhog {tag}\n\nSource: `{source_sha}`.\n\n"
            "The attached signed manifest binds the contract, authored documentation, "
            "pinned render and qualification evidence.\n",
            encoding="utf-8",
        )
        result = selected.publish_asset_set(
            tag, source_sha, assets, title="Riverhog " + tag, notes=notes, latest=True
        )
    return {
        **result,
        "preparation_run_id": preparation_run,
        "qualification_run_id": qualification_run,
        "release_manifest_sha256": file_sha256(evidence / "release-manifest.json"),
    }
