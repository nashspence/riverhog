"""Workspace boundary with a deployment-declared plaintext protection mode."""

from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Self

from riverhog_protocol.collection_workflows import canonical_json_bytes
from riverhog_protocol.paths import normalize_relpath
from riverhog_protocol.workspace_protection import DeclaredWorkspaceProtection

_MARKER = ".riverhog-transform-workspace.json"


@dataclass(slots=True)
class TransformWorkspace:
    """A target-owned workspace with an explicit deployment declaration.

    The runtime cannot prove mount encryption, memory backing, or swap policy.
    The deployment must declare encrypted-at-rest storage or memory-backed storage
    without unencrypted swap. The runtime checks path safety and records that
    declaration in a restart-stable marker.
    """

    root: Path
    execution_id: str
    declared_protection: DeclaredWorkspaceProtection

    @classmethod
    def open(
        cls,
        root: Path,
        *,
        execution_id: str,
        declared_protection: DeclaredWorkspaceProtection,
    ) -> TransformWorkspace:
        if declared_protection not in {"encrypted-at-rest", "memory-backed"}:
            raise ValueError("transform workspace protection declaration is invalid")
        base = root.resolve()
        if root.is_symlink() or not base.is_dir():
            raise ValueError("transform workspace root must be a real directory")
        if base.stat().st_mode & 0o077:
            raise ValueError("transform workspace root must not be group- or world-accessible")
        if (
            len(execution_id) != 64
            or execution_id != execution_id.casefold()
            or any(character not in "0123456789abcdef" for character in execution_id)
        ):
            raise ValueError("transform workspace requires an execution SHA-256 identity")
        path = base / execution_id
        path.mkdir(mode=0o700, parents=False, exist_ok=True)
        if path.is_symlink() or not path.is_dir():
            raise ValueError("transform workspace path must be a real directory")
        os.chmod(path, 0o700)
        marker = path / _MARKER
        payload = {
            "format": "riverhog-transform-workspace/v1",
            "execution_id": execution_id,
            "declared_protection": declared_protection,
        }
        encoded = canonical_json_bytes(payload)
        if marker.is_symlink():
            raise ValueError("transform workspace marker must not be a symlink")
        if marker.exists():
            try:
                current = json.loads(marker.read_text())
            except (OSError, json.JSONDecodeError) as exc:
                raise ValueError("transform workspace marker is unreadable") from exc
            if canonical_json_bytes(current) != encoded:
                raise ValueError(
                    "transform workspace is bound to another execution or protection declaration"
                )
        else:
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            flags |= getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(marker, flags, 0o600)
            try:
                os.write(descriptor, encoded)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            directory = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        return cls(root=path, execution_id=execution_id, declared_protection=declared_protection)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        pass

    def resolve(self, relative_path: str) -> Path:
        normalized = normalize_relpath(relative_path)
        candidate = self.root.joinpath(*normalized.split("/"))
        current = self.root
        for part in normalized.split("/"):
            current = current / part
            if current.is_symlink():
                raise ValueError("workspace paths must not traverse symlinks")
            if not current.exists():
                break
        parent = candidate.parent.resolve()
        if parent != self.root and self.root not in parent.parents:
            raise ValueError("workspace path escapes its root")
        return candidate

    def release(self) -> None:
        if self.root.is_symlink() or not self.root.is_dir():
            raise RuntimeError("transform workspace path is no longer a safe directory")
        shutil.rmtree(self.root)


__all__ = ["TransformWorkspace"]
