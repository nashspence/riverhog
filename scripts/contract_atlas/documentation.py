"""Validate authored release prose and bind exact Git bytes to generated facts."""

from __future__ import annotations

import copy
import hashlib
import json
import re
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from .cli_documentation import build_cli_documentation_record
from .model import ContractAtlasError, canonical_sha256

DOCUMENTATION_FILENAME = "documentation.json"
SOURCE_FORMAT = "riverhog-release-documentation-source/v1"
_SHA = re.compile(r"[0-9a-f]{40}\Z")
_TAG = re.compile(r"v1\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)(?:-rc\.[1-9][0-9]*)?\Z")


def validate_authoring_tree(repository: Path, commit: str, source_sha: str | None = None) -> None:
    """The independent authoring history contains prose and authoring metadata only."""

    if not _SHA.fullmatch(commit):
        raise ContractAtlasError("authoring tree requires an exact commit")
    tree = subprocess.check_output(["git", "-C", str(repository), "ls-tree", "-rz", commit])
    for entry in tree.split(b"\0"):
        if not entry:
            continue
        metadata, raw_path = entry.split(b"\t", 1)
        mode, kind, _object_id = metadata.decode().split()
        path = raw_path.decode("utf-8")
        tag, _, basename = path.partition("/")
        if (
            mode != "100644"
            or kind != "blob"
            or not (
                path in {"README.md", "REUSE.toml", ".gitattributes"}
                or (_TAG.fullmatch(tag) and basename == DOCUMENTATION_FILENAME)
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


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ContractAtlasError(f"authored documentation has a duplicate field: {key}")
        result[key] = value
    return result


def _text(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def validate_authored_documentation(
    payload: bytes, closure: Mapping[str, object]
) -> dict[str, Any]:
    """Accept prose and stable references; reference inventories have other owners."""

    try:
        document = json.loads(payload.decode("utf-8"), object_pairs_hook=_object)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ContractAtlasError("authored documentation is not valid UTF-8 JSON") from exc
    if (
        not isinstance(document, dict)
        or set(document) != {"format", "explanations", "guides"}
        or document["format"] != SOURCE_FORMAT
        or not isinstance(document["explanations"], list)
        or not isinstance(document["guides"], list)
    ):
        raise ContractAtlasError("authored documentation must contain only explanations and guides")
    identities = {
        str(item["id"]) for item in cast(Sequence[Mapping[str, object]], closure["elements"])
    }
    seen: set[str] = set()
    for item in document["explanations"]:
        if (
            not isinstance(item, dict)
            or set(item) != {"element_id", "text"}
            or not isinstance(item["element_id"], str)
            or item["element_id"] not in identities
            or item["element_id"] in seen
            or not _text(item["text"])
        ):
            raise ContractAtlasError("authored explanation is malformed, duplicate, or unresolved")
        seen.add(item["element_id"])
    seen.clear()
    for guide in document["guides"]:
        if not isinstance(guide, dict) or set(guide) != {"id", "title", "text", "subjects"}:
            raise ContractAtlasError("authored guide has unreviewed fields")
        subjects = guide["subjects"]
        if (
            not all(_text(guide[key]) for key in ("id", "title", "text"))
            or guide["id"] in seen
            or not isinstance(subjects, list)
            or not subjects
            or not all(isinstance(subject, str) and subject in identities for subject in subjects)
            or len(set(subjects)) != len(subjects)
        ):
            raise ContractAtlasError("authored guide is malformed, duplicate, or unresolved")
        seen.add(guide["id"])
    return cast(dict[str, Any], document)


@dataclass(frozen=True)
class AuthoredDocumentation:
    """An exact selected authoring commit and unchanged source document."""

    tag: str
    commit: str
    path: str
    payload: bytes

    @classmethod
    def resolve(
        cls, repository: Path, tag: str, commit: str, *, source_sha: str | None = None
    ) -> AuthoredDocumentation:
        if not _TAG.fullmatch(tag) or not _SHA.fullmatch(commit):
            raise ContractAtlasError("documentation requires an exact v1 tag and authoring commit")
        validate_authoring_tree(repository, commit, source_sha)
        path = f"{tag}/{DOCUMENTATION_FILENAME}"
        try:
            mode = subprocess.check_output(
                ["git", "-C", str(repository), "ls-tree", commit, "--", path], text=True
            ).split()
            if mode[:2] != ["100644", "blob"]:
                raise ContractAtlasError("selected documentation must be a regular authored file")
            payload = subprocess.check_output(
                ["git", "-C", str(repository), "show", f"{commit}:{path}"]
            )
        except subprocess.CalledProcessError as exc:
            raise ContractAtlasError("selected documentation commit/path is unavailable") from exc
        return cls(tag, commit, path, payload)

    def bind(
        self,
        closure: Mapping[str, object],
        audit: Mapping[str, object],
        parsers: Mapping[str, object],
        source_sha: str,
    ) -> tuple[dict[str, object], dict[str, object]]:
        if (
            not _SHA.fullmatch(source_sha)
            or not _SHA.fullmatch(self.commit)
            or not _TAG.fullmatch(self.tag)
            or self.path != f"{self.tag}/{DOCUMENTATION_FILENAME}"
        ):
            raise ContractAtlasError("documentation binding has incomplete release identities")
        authored = validate_authored_documentation(self.payload, closure)
        document = build_cli_documentation_record(closure, parsers)
        document.update(
            release_scope=self.tag,
            build_scope="release",
            source_revision=source_sha,
            explanations=copy.deepcopy(authored["explanations"]),
            guides=copy.deepcopy(authored["guides"]),
        )
        return document, {
            "tag": self.tag,
            "source_sha": source_sha,
            "commit": self.commit,
            "path": self.path,
            "sha256": hashlib.sha256(self.payload).hexdigest(),
            "closure_sha256": canonical_sha256(closure),
            "audit_sha256": canonical_sha256(audit),
            "documentation_record_sha256": canonical_sha256(document),
        }
