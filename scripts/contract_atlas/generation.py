"""One source-only contract generation API for workspaces and exact releases."""

from __future__ import annotations

import hashlib
import io
import json
import platform
import re
import subprocess
import tarfile
import tempfile
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import cast

from . import build_discovered_contract
from .documentation import (
    DOCUMENTATION_FILENAME,
    AuthoredDocumentation,
    validate_authored_documentation,
)
from .html_rendering import render_contract, validate_render
from .model import ContractAtlasError, DiscoveredContract, canonical_bytes, canonical_sha256
from .records import AUDIT_FILENAME, ContractBundle, build_records, load_bundle

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT = ROOT / "build/contracts"
CLOSURE_FILENAME = "riverhog-v1.json"
BUILD_FILENAME = "build-manifest.json"
BUILD_FORMAT = "riverhog-contract-candidate-build/v1"
_SHA = re.compile(r"[0-9a-f]{40}\Z")


def source_revision(root: Path = ROOT, requested: str | None = None) -> str | None:
    """Workspace builds make no exact-revision claim when tracked source is dirty."""

    try:
        head = subprocess.check_output(
            ["git", "-C", str(root), "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL
        ).strip()
        dirty = subprocess.check_output(
            ["git", "-C", str(root), "status", "--porcelain", "--untracked-files=normal"], text=True
        )
    except subprocess.CalledProcessError:
        if requested is not None:
            raise ContractAtlasError("exact-source generation requires its Git checkout") from None
        return None
    if requested is not None and (not _SHA.fullmatch(requested) or head != requested or dirty):
        raise ContractAtlasError("exact-source generation requires the requested clean HEAD")
    return None if dirty else head


def _identities(paths: tuple[str, ...]) -> dict[str, str]:
    return {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in paths}


def prepared_source(
    repository: Path, revision: str, version: str, source_archive: Path
) -> dict[str, object]:
    """Verify a release snapshot against its own revision's deterministic preparation."""

    import release

    source_revision(repository, revision)
    epoch = int(
        subprocess.check_output(
            ["git", "-C", str(repository), "show", "-s", "--format=%ct", revision], text=True
        )
    )
    archive = subprocess.check_output(
        ["git", "-C", str(repository), "archive", "--format=tar", revision]
    )
    with tempfile.TemporaryDirectory(prefix="riverhog-prepared-source-") as temporary:
        expected = Path(temporary) / "source"
        expected.mkdir()
        with tarfile.open(fileobj=io.BytesIO(archive)) as stream:
            stream.extractall(expected, filter="data")
        release.apply_release_version(expected, version)
        subprocess.run(["uv", "lock", "--offline"], cwd=expected, check=True)
        identities: dict[str, object] = {}
        for path in sorted(expected.rglob("*")):
            if not path.is_file():
                continue
            name = path.relative_to(expected).as_posix()
            actual = ROOT / name
            if (
                path.is_symlink()
                or actual.is_symlink()
                or not actual.is_file()
                or path.read_bytes() != actual.read_bytes()
                or bool(path.stat().st_mode & 0o111) != bool(actual.stat().st_mode & 0o111)
            ):
                raise ContractAtlasError(f"prepared release source differs: {name}")
            identities[name] = {
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "executable": bool(path.stat().st_mode & 0o111),
            }
        caches = {".venv", "__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache"}
        actual_files = {
            path.relative_to(ROOT).as_posix()
            for path in ROOT.rglob("*")
            if path.is_file() and not caches.intersection(path.relative_to(ROOT).parts)
        }
        if actual_files != set(identities):
            raise ContractAtlasError("prepared release source has an unexpected file inventory")
        reproduced = Path(temporary) / "source.tar.gz"
        release._write_source_archive(expected, reproduced, version=version, source_epoch=epoch)
        digest = hashlib.sha256(reproduced.read_bytes()).hexdigest()
        if digest != hashlib.sha256(source_archive.read_bytes()).hexdigest():
            raise ContractAtlasError(
                "prepared release source archive differs from exact preparation"
            )
    return {
        "release_version": version,
        "source_epoch": epoch,
        "source_archive_sha256": digest,
        "inputs_sha256": canonical_sha256(identities),
    }


@dataclass(frozen=True)
class GeneratedCandidate:
    """Native discovery, validated products, and non-circular build provenance."""

    projection: dict[str, object]
    trace: dict[str, object]
    discovered: DiscoveredContract
    bundle: ContractBundle
    files: dict[str, bytes]
    manifest: dict[str, object]

    def write(self, destination: Path, *, replace: bool = False) -> dict[str, object]:
        destination = destination.absolute()
        if destination.is_symlink():
            raise ContractAtlasError("candidate output must not be a symbolic link")
        if destination.exists() and any(destination.iterdir()):
            if not replace:
                raise ContractAtlasError("candidate output must be empty")
            previous = verify_inventory(destination)
            if not {CLOSURE_FILENAME, AUDIT_FILENAME, "riverhog-v1/manifest.json"} <= set(
                cast(dict[str, str], previous["files"])
            ):
                raise ContractAtlasError("replace requires owned generated contract output")
        destination.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".contract-", dir=destination.parent) as temporary:
            stage = Path(temporary) / "candidate"
            stage.mkdir()
            for name, payload in self.files.items():
                path = stage / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(payload)
            (stage / BUILD_FILENAME).write_bytes(canonical_bytes(self.manifest))
            if destination.exists():
                destination.rename(Path(temporary) / "previous")
            stage.rename(destination)
        return self.manifest


def build_candidate(
    *,
    revision: str | None = None,
    documentation: AuthoredDocumentation | None = None,
    preparation: dict[str, object] | None = None,
) -> GeneratedCandidate:
    """Discover from executable authorities without reading prior contract products."""

    import contract_freeze

    if preparation is None:
        revision = source_revision(requested=revision)
    elif revision is None:
        raise ContractAtlasError("prepared source generation requires an exact code commit")
    if revision is not None and not _SHA.fullmatch(revision):
        raise ContractAtlasError("candidate source revision must be an exact commit")
    projection = contract_freeze.contract_projection()
    trace = contract_freeze.trace_projection(projection)
    discovered = build_discovered_contract(projection, trace)
    closure, audit = build_records(discovered)
    document = None
    binding = None
    extras: dict[str, bytes] = {}
    if documentation is not None:
        if revision is None:
            raise ContractAtlasError("release documentation requires exact code source")
        document, binding = documentation.bind(
            closure, audit, contract_freeze._cli_parsers(), revision
        )
        extras = {
            DOCUMENTATION_FILENAME: documentation.payload,
            "documentation-record.json": canonical_bytes(document),
        }
    files = {
        CLOSURE_FILENAME: canonical_bytes(closure),
        AUDIT_FILENAME: canonical_bytes(audit),
        **extras,
        **render_contract(closure, audit, document, source_revision=revision),
    }
    prefix = "https://nashspence.github.io/riverhog/"
    schemas = cast(
        dict[str, object], cast(dict[str, object], closure["external_contract"])["protocol_schemas"]
    )
    for identifier, schema in schemas.items():
        if identifier.startswith(prefix):
            from .publication import safe_relative

            files["identifiers/" + safe_relative(identifier.removeprefix(prefix))] = (
                canonical_bytes(schema)
            )
    validate_render(files)
    manifest: dict[str, object] = {
        "format": BUILD_FORMAT,
        "source_sha": revision,
        "build_scope": "revision" if revision is not None else "workspace",
        "preparation": preparation,
        "source_identities_sha256": canonical_sha256(audit["source_identities"]),
        "closure_sha256": canonical_sha256(closure),
        "audit_sha256": canonical_sha256(audit),
        "documentation": binding,
        "renderer": _identities(
            (
                "scripts/contract_atlas/html_rendering.py",
                "scripts/contract_atlas/human_contract.py",
                "scripts/contract_atlas/model.py",
                "scripts/contract_atlas/cli_documentation.py",
            )
        ),
        "toolchain": {
            "python": platform.python_version(),
            "inputs": _identities(("mise.toml", "mise.lock", "uv.lock")),
        },
        "files": {name: hashlib.sha256(payload).hexdigest() for name, payload in files.items()},
    }
    return GeneratedCandidate(
        projection, trace, discovered, ContractBundle(closure, audit), files, manifest
    )


def verify_inventory(directory: Path, *, expected_source: str | None = None) -> dict[str, object]:
    """Verify byte ownership without applying today's semantic rules to prior output."""

    try:
        raw = (directory / BUILD_FILENAME).read_bytes()
        manifest = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        raise ContractAtlasError("candidate build manifest is unavailable") from exc
    if (
        not isinstance(manifest, dict)
        or manifest.get("format") != BUILD_FORMAT
        or not isinstance(manifest.get("files"), dict)
        or raw != canonical_bytes(manifest)
    ):
        raise ContractAtlasError("candidate build manifest has an invalid format")
    if expected_source is not None and manifest.get("source_sha") != expected_source:
        raise ContractAtlasError("candidate source does not match the expected commit")
    expected = cast(dict[str, str], manifest["files"])
    paths = {p.relative_to(directory).as_posix(): p for p in directory.rglob("*") if p.is_file()}
    if (
        directory.is_symlink()
        or any(p.is_symlink() or not (p.is_file() or p.is_dir()) for p in directory.rglob("*"))
        or set(paths) != set(expected) | {BUILD_FILENAME}
    ):
        raise ContractAtlasError("candidate file set differs from its manifest")
    for name, digest in expected.items():
        if hashlib.sha256(paths[name].read_bytes()).hexdigest() != digest:
            raise ContractAtlasError(f"candidate file identity differs: {name}")
    return manifest


def verify_candidate(directory: Path, *, expected_source: str | None = None) -> dict[str, object]:
    """Check generated bytes and native record/render integrity before reuse."""

    manifest = verify_inventory(directory, expected_source=expected_source)
    expected = cast(dict[str, str], manifest["files"])
    paths = {p.relative_to(directory).as_posix(): p for p in directory.rglob("*") if p.is_file()}
    bundle = load_bundle(directory / CLOSURE_FILENAME)
    if (
        manifest.get("closure_sha256") != canonical_sha256(bundle.closure)
        or manifest.get("audit_sha256") != canonical_sha256(bundle.audit)
        or manifest.get("source_identities_sha256")
        != canonical_sha256(bundle.audit["source_identities"])
    ):
        raise ContractAtlasError("candidate native identities differ from its build")
    validate_render(
        {name: path.read_bytes() for name, path in paths.items() if name.startswith("riverhog-v1/")}
    )
    render_manifest = json.loads((directory / "riverhog-v1/manifest.json").read_bytes())
    if any(
        render_manifest[key] != manifest[other]
        for key, other in (
            ("closure_sha256", "closure_sha256"),
            ("audit_sha256", "audit_sha256"),
            ("source_revision", "source_sha"),
        )
    ):
        raise ContractAtlasError("candidate render identities differ from its build")
    binding = manifest.get("documentation")
    if binding is None:
        if render_manifest["documentation_sha256"] is not None or DOCUMENTATION_FILENAME in paths:
            raise ContractAtlasError("development candidate carries release documentation")
    else:
        if not isinstance(binding, dict) or DOCUMENTATION_FILENAME not in paths:
            raise ContractAtlasError("release candidate lacks bound authored documentation")
        if (
            binding.get("source_sha") != manifest.get("source_sha")
            or binding.get("closure_sha256") != manifest.get("closure_sha256")
            or binding.get("audit_sha256") != manifest.get("audit_sha256")
            or binding.get("sha256") != expected[DOCUMENTATION_FILENAME]
            or binding.get("documentation_record_sha256") != render_manifest["documentation_sha256"]
            or binding.get("documentation_record_sha256")
            != expected.get("documentation-record.json")
        ):
            raise ContractAtlasError("release documentation binding differs from candidate")
        validate_authored_documentation(paths[DOCUMENTATION_FILENAME].read_bytes(), bundle.closure)
    return manifest


@cache
def ensure_candidate(directory: Path = DEFAULT_OUTPUT) -> Path:
    """Rebuild an owned disposable candidate from current source for a local caller."""

    candidate = build_candidate(revision=source_revision())
    candidate.write(directory, replace=True)
    return directory / CLOSURE_FILENAME
