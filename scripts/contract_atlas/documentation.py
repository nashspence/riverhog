"""Exact independent Markdown authoring capture and generated documentation binding."""

from __future__ import annotations

import hashlib
import re
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .cli_documentation import build_cli_documentation_record
from .documentation_markdown import compile_corpus, path_name
from .documentation_requirements import build_requirements
from .model import ContractAtlasError, canonical_bytes, canonical_sha256

DOCUMENTATION_FILENAME = "documentation-source.json"
SOURCE_CAPTURE_FORMAT = "riverhog-documentation-source-capture/v1"
_SHA = re.compile(r"[0-9a-f]{40}\Z")
_TAG = re.compile(r"v1\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-rc\.[1-9][0-9]*)?\Z")


def _git(repository: Path, *args: str, data: bytes | None = None) -> bytes:
    try:
        return subprocess.run(
            ["git", "-C", str(repository), *args],
            input=data,
            check=True,
            capture_output=True,
        ).stdout
    except subprocess.CalledProcessError as exc:
        raise ContractAtlasError("exact documentation Git source is unavailable") from exc


def validate_authoring_tree(repository: Path, commit: str, source_sha: str | None = None) -> None:
    """Self-contained version subtrees; no executable source or generated authoring registry."""
    if not _SHA.fullmatch(commit):
        raise ContractAtlasError("authoring tree requires an exact commit")
    for entry in _git(repository, "ls-tree", "-rz", commit).split(b"\0"):
        if not entry:
            continue
        metadata, raw_path = entry.split(b"\t", 1)
        mode, kind, _ = metadata.decode().split()
        path = path_name(raw_path.decode("utf-8"))
        tag, _, relative = path.partition("/")
        if (
            mode != "100644"
            or kind != "blob"
            or not (
                path in {"README.md", "REUSE.toml", ".gitattributes"}
                or (_TAG.fullmatch(tag) and relative.endswith((".md", ".png")))
            )
        ):
            raise ContractAtlasError(f"authoring history contains a non-documentation path: {path}")
    if source_sha is not None:
        result = subprocess.run(
            ["git", "-C", str(repository), "merge-base", commit, source_sha],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if result.returncode != 1:
            raise ContractAtlasError(
                "release documentation history must be ancestry-independent of code"
            )


def source_ledger(files: Mapping[str, bytes]) -> dict[str, Any]:
    return {
        name: {"sha256": hashlib.sha256(data).hexdigest(), "size": len(data)}
        for name, data in sorted(files.items())
    }


def validate_frozen_subtree(files: Mapping[str, bytes], published: Mapping[str, Any]) -> None:
    """A published same-version subtree can only be copied as its original exact bytes."""
    if source_ledger(files) != published:
        raise ContractAtlasError(
            "published documentation subtree changed; select a later version or scoped erratum"
        )


def verify_compilation(
    closure: Mapping[str, Any], files: Mapping[str, bytes], document: Mapping[str, Any]
) -> None:
    """Current verification derives obligations and prose from source, not claimed coverage."""
    from .html_rendering import _element_file

    requirements = build_requirements(closure)
    if canonical_sha256(requirements) != canonical_sha256(document["requirements"]):
        raise ContractAtlasError("documentation obligations differ from native source policy")
    compiled = compile_corpus(
        files,
        requirements,
        routes={item["id"]: _element_file(item["id"]) for item in closure["elements"]},
    )
    if canonical_sha256(compiled) != canonical_sha256(document["compiled"]):
        raise ContractAtlasError("compiled documentation differs from captured Markdown")


@dataclass(frozen=True)
class AuthoredDocumentation:
    """Captured regular file bytes from one exact independent authoring commit."""

    tag: str
    commit: str
    files: Mapping[str, bytes]

    @property
    def path(self) -> str:
        return self.tag + "/"

    @property
    def payload(self) -> bytes:
        return canonical_bytes(
            {
                "format": SOURCE_CAPTURE_FORMAT,
                "commit": self.commit,
                "subtree": self.path,
                "files": source_ledger(self.files),
            }
        )

    @classmethod
    def resolve(
        cls,
        repository: Path,
        tag: str,
        commit: str,
        *,
        source_sha: str | None = None,
        published: Mapping[str, Any] | None = None,
    ) -> AuthoredDocumentation:
        if not _TAG.fullmatch(tag) or not _SHA.fullmatch(commit):
            raise ContractAtlasError(
                "documentation requires an exact v1 version and authoring commit"
            )
        validate_authoring_tree(repository, commit, source_sha)
        entries = []
        for row in _git(repository, "ls-tree", "-rz", commit, "--", tag + "/").split(b"\0"):
            if not row:
                continue
            metadata, raw = row.split(b"\t", 1)
            mode, kind, oid = metadata.decode().split()
            name = raw.decode("utf-8")
            if mode != "100644" or kind != "blob" or not name.startswith(tag + "/"):
                raise ContractAtlasError("documentation source must contain regular files")
            entries.append((path_name(name[len(tag) + 1 :]), oid))
        if not entries or len(entries) > 10_000:
            raise ContractAtlasError(
                "selected corpus is empty or exceeds its operational file budget"
            )
        # One captured batch, never dirty worktree bytes or a moving branch read per file.
        raw = _git(
            repository,
            "cat-file",
            "--batch",
            data="".join(oid + "\n" for _, oid in entries).encode(),
        )
        cursor, files = 0, {}
        for name, oid in entries:
            end = raw.index(b"\n", cursor)
            actual_oid, kind, size = raw[cursor:end].decode().split()
            length = int(size)
            if actual_oid != oid or kind != "blob" or length > 8_000_000:
                raise ContractAtlasError(f"invalid or oversized documentation blob: {name}")
            files[name] = raw[end + 1 : end + 1 + length]
            cursor = end + length + 2
        if sum(map(len, files.values())) > 32_000_000:
            raise ContractAtlasError("documentation capture operational byte budget exceeded")
        if published is not None:
            validate_frozen_subtree(files, published)
        return cls(tag, commit, files)

    def compile(
        self, closure: Mapping[str, Any], *, final: bool = False
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        from .html_rendering import _element_file

        requirements = build_requirements(closure)
        routes = {item["id"]: _element_file(item["id"]) for item in closure["elements"]}
        compiled = compile_corpus(self.files, requirements, final=final, routes=routes)
        return compiled, requirements

    def bind(
        self,
        closure: Mapping[str, Any],
        audit: Mapping[str, Any],
        parsers: Mapping[str, Any],
        source_sha: str | None,
        *,
        preview: bool = False,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        if (
            (not preview and (source_sha is None or not _SHA.fullmatch(source_sha)))
            or not _SHA.fullmatch(self.commit)
            or not _TAG.fullmatch(self.tag)
        ):
            raise ContractAtlasError("documentation binding has incomplete release identities")
        compiled, requirements = self.compile(closure)
        from .documentation_native import apply_cli_prose

        apply_cli_prose(parsers, compiled, requirements)
        document = build_cli_documentation_record(closure, parsers)
        document.update(
            release_scope=self.tag,
            build_scope="workspace" if source_sha is None else "release",
            source_revision=source_sha or "uncommitted-workspace",
            compiled=compiled,
            requirements=requirements,
            explanations=[
                {
                    "element_id": requirements["subjects"][key]["target"]["element_id"],
                    "text": entry["summary"],
                }
                for key, entry in compiled["resolved"].items()
                if requirements["subjects"][key]["target"].get("pointer") == ""
            ],
            guides=[
                {
                    "id": guide["id"],
                    "title": guide["title"],
                    "text": compiled["pages"][guide["body"]]["plain"],
                    "subjects": sorted({t["element_id"] for t in guide["subjects"]}),
                }
                for guide in compiled["guides"]
            ],
        )
        return document, {
            "tag": self.tag,
            "source_sha": source_sha,
            "commit": self.commit,
            "path": self.path,
            "sha256": hashlib.sha256(self.payload).hexdigest(),
            "source_files": source_ledger(self.files),
            "closure_sha256": canonical_sha256(closure),
            "audit_sha256": canonical_sha256(audit),
            "requirements_sha256": canonical_sha256(requirements),
            "compiled_sha256": canonical_sha256(compiled),
            "documentation_record_sha256": canonical_sha256(document),
        }
