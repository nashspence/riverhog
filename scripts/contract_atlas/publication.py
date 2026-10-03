"""Verify immutable contract artifacts and extract them within a hosting budget."""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import re
import tarfile
import tempfile
from pathlib import Path, PurePosixPath
from typing import Any

from .generation import BUILD_FILENAME, BUILD_FORMAT
from .model import ContractAtlasError, canonical_bytes

DEFAULT_SITE_BUDGET = 900_000_000
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")


def file_sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


class _BudgetedStream(io.BufferedIOBase):
    def __init__(self, source: gzip.GzipFile, budget: int) -> None:
        self.source = source
        self.remaining = budget

    def read(self, size: int | None = -1) -> bytes:
        payload = self.source.read(
            min(size if size is not None and size >= 0 else self.remaining + 1, self.remaining + 1)
        )
        self.remaining -= len(payload)
        if self.remaining < 0:
            raise ContractAtlasError("archive expansion exceeds the operational budget")
        return payload


def safe_relative(name: str) -> str:
    path = PurePosixPath(name)
    if (
        not name
        or path.is_absolute()
        or path.as_posix() != name
        or any(part in {"", ".", ".."} for part in name.split("/"))
        or "\\" in name
        or ":" in name
        or any(ord(c) < 32 for c in name)
    ):
        raise ContractAtlasError("artifact has an unsafe relative path")
    return name


def directory_files(root: Path) -> dict[str, Path]:
    if root.is_symlink() or not root.is_dir():
        raise ContractAtlasError("artifact directory must be a regular directory")
    result: dict[str, Path] = {}
    for path in root.rglob("*"):
        if path.is_symlink() or not (path.is_file() or path.is_dir()):
            raise ContractAtlasError("artifact contains a link or special file")
        if path.is_file():
            result[safe_relative(path.relative_to(root).as_posix())] = path
    return result


def verify_published_candidate(root: Path, *, source_sha: str | None = None) -> dict[str, Any]:
    """Check historical bytes and bindings without rediscovery or current re-rendering."""

    files = directory_files(root)
    try:
        raw = files[BUILD_FILENAME].read_bytes()
        build = json.loads(raw)
        if (
            not isinstance(build, dict)
            or build.get("format") != BUILD_FORMAT
            or raw != canonical_bytes(build)
            or not isinstance(build.get("files"), dict)
            or set(files) != set(build["files"]) | {BUILD_FILENAME}
            or re.fullmatch(r"[0-9a-f]{40}", str(build.get("source_sha"))) is None
            or (source_sha is not None and build["source_sha"] != source_sha)
        ):
            raise ContractAtlasError("published contract has mismatched build provenance")
        for name, digest in build["files"].items():
            safe_relative(name)
            if (
                not isinstance(digest, str)
                or not _DIGEST.fullmatch(digest)
                or hashlib.sha256(files[name].read_bytes()).hexdigest() != digest
            ):
                raise ContractAtlasError(f"published contract file identity differs: {name}")
        for name, key in (
            ("riverhog-v1.json", "closure_sha256"),
            ("riverhog-v1-audit.json", "audit_sha256"),
        ):
            if build[key] != build["files"][name]:
                raise ContractAtlasError("published contract identities differ from its records")
        render = json.loads(files["riverhog-v1/manifest.json"].read_bytes())
        if any(
            render[key] != build[other]
            for key, other in (
                ("source_revision", "source_sha"),
                ("closure_sha256", "closure_sha256"),
                ("audit_sha256", "audit_sha256"),
            )
        ) or render["files"] != {
            name.removeprefix("riverhog-v1/"): digest
            for name, digest in build["files"].items()
            if name.startswith("riverhog-v1/") and name != "riverhog-v1/manifest.json"
        }:
            raise ContractAtlasError("published render is not bound to the exact contract")
        binding = build.get("documentation")
        if binding is None:
            if render["documentation_sha256"] is not None or any(
                name in files for name in ("documentation.json", "documentation-record.json")
            ):
                raise ContractAtlasError("published base render carries unbound documentation")
        elif (
            not isinstance(binding, dict)
            or binding["source_sha"] != build["source_sha"]
            or binding["path"] != binding["tag"] + "/documentation.json"
            or not re.fullmatch(r"[0-9a-f]{40}", binding["commit"])
            or binding["closure_sha256"] != build["closure_sha256"]
            or binding["audit_sha256"] != build["audit_sha256"]
            or binding["sha256"] != build["files"].get("documentation.json")
            or binding["documentation_record_sha256"]
            != build["files"].get("documentation-record.json")
            or render["documentation_sha256"] != binding["documentation_record_sha256"]
        ):
            raise ContractAtlasError("published documentation binding differs from exact bytes")
        if (
            not isinstance(build.get("renderer"), dict)
            or not build["renderer"]
            or not isinstance(build.get("toolchain"), dict)
        ):
            raise ContractAtlasError("published contract lacks renderer/toolchain provenance")
        return build
    except ContractAtlasError:
        raise
    except (KeyError, TypeError, ValueError, OSError) as exc:
        raise ContractAtlasError("published contract metadata is malformed or incomplete") from exc


def extract_verified_tar(
    archive: Path, digest: str, destination: Path, *, budget: int = DEFAULT_SITE_BUDGET
) -> None:
    """Extract only regular files and directories; refuse unsafe or excessive input."""

    if budget <= 0 or archive.stat().st_size > budget:
        raise ContractAtlasError("archive exceeds the operational extraction budget")
    if not _DIGEST.fullmatch(digest) or file_sha256(archive) != digest:
        raise ContractAtlasError("archive identity differs from the verified release asset")
    if destination.exists():
        raise ContractAtlasError("archive destination must not exist")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".release-contract-", dir=destination.parent
    ) as temporary:
        stage = Path(temporary) / "contents"
        stage.mkdir()
        seen: set[str] = set()
        expanded = 0
        try:
            with (
                gzip.open(archive, "rb") as plaintext,
                tarfile.open(
                    name=None, fileobj=_BudgetedStream(plaintext, budget), mode="r|"
                ) as stream,
            ):
                for member in stream:
                    name = safe_relative(member.name)
                    if name in seen or member.size < 0 or not (member.isfile() or member.isdir()):
                        raise ContractAtlasError(
                            "archive repeats a path or contains a link/special file"
                        )
                    seen.add(name)
                    expanded += 512 + len(canonical_bytes(member.pax_headers)) + member.size
                    if expanded > budget:
                        raise ContractAtlasError("archive expansion exceeds the operational budget")
                    target = stage / name
                    if member.isdir():
                        target.mkdir(parents=True, exist_ok=True)
                    else:
                        target.parent.mkdir(parents=True, exist_ok=True)
                        source = stream.extractfile(member)
                        if source is None:
                            raise ContractAtlasError("archive regular file has no payload")
                        with source, target.open("xb") as output:
                            remaining = member.size
                            while remaining:
                                payload = source.read(min(remaining, 1_048_576))
                                if not payload:
                                    raise ContractAtlasError("archive payload is truncated")
                                output.write(payload)
                                remaining -= len(payload)
            stage.rename(destination)
        except (OSError, tarfile.TarError) as exc:
            raise ContractAtlasError("archive contains malformed or colliding paths") from exc


def contract_binding(
    root: Path, asset_name: str, asset_digest: str, render_name: str, render_digest: str
) -> dict[str, Any]:
    build = verify_published_candidate(root)
    return {
        "file": asset_name,
        "sha256": asset_digest,
        "render": {"file": render_name, "sha256": render_digest},
        "build_manifest_sha256": hashlib.sha256((root / BUILD_FILENAME).read_bytes()).hexdigest(),
        **{
            key: build[key]
            for key in (
                "source_sha",
                "closure_sha256",
                "audit_sha256",
                "documentation",
                "preparation",
                "renderer",
                "toolchain",
            )
        },
    }


def unpack_release_contract(
    assets: Path,
    binding: dict[str, Any],
    destination: Path,
    *,
    budget: int = DEFAULT_SITE_BUDGET,
) -> dict[str, Any]:
    """Join verified record and pinned render assets without overlapping their files."""

    if destination.exists():
        raise ContractAtlasError("release candidate destination must not exist")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".release-inputs-", dir=destination.parent
    ) as temporary:
        stage = Path(temporary)
        extract_verified_tar(
            assets / safe_relative(binding["file"]),
            binding["sha256"],
            stage / "records",
            budget=budget,
        )
        render = binding["render"]
        extract_verified_tar(
            assets / safe_relative(render["file"]),
            render["sha256"],
            stage / "render",
            budget=budget,
        )
        record_files, render_files = (
            directory_files(stage / "records"),
            directory_files(stage / "render"),
        )
        if (
            set(record_files) & set(render_files)
            or any(not name.startswith(("riverhog-v1/", "identifiers/")) for name in render_files)
            or any(name.startswith(("riverhog-v1/", "identifiers/")) for name in record_files)
        ):
            raise ContractAtlasError(
                "release record/render assets overlap or contain unexpected files"
            )
        if (
            sum(path.stat().st_size for path in (*record_files.values(), *render_files.values()))
            > budget
        ):
            raise ContractAtlasError("release candidate exceeds the operational budget")
        (stage / "render/riverhog-v1").rename(stage / "records/riverhog-v1")
        if (stage / "render/identifiers").exists():
            (stage / "render/identifiers").rename(stage / "records/identifiers")
        actual = contract_binding(
            stage / "records", binding["file"], binding["sha256"], render["file"], render["sha256"]
        )
        if actual != binding:
            raise ContractAtlasError("release manifest differs from the complete contract binding")
        (stage / "records").rename(destination)
    return verify_published_candidate(destination, source_sha=binding["source_sha"])
