"""Target-owned durable pre-root evidence for resuming sealed publication."""

from __future__ import annotations

import hashlib
import os
import secrets
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict
from riverhog_canonical_json import canonical_json_bytes
from riverhog_client.canonical_completion import CompletionRecord
from stove0_target_protocol import (
    OperationContract,
    TargetDescriptor,
    TargetJobRequest,
    TargetPreRootResult,
)


class _Manifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    format: Literal["stove0-publication-checkpoint/v1"] = "stove0-publication-checkpoint/v1"
    implementation: TargetDescriptor
    operation: OperationContract
    pre_root: TargetPreRootResult
    execution_bytes: str
    source_context: dict[str, Any]


def _bind_manifest(manifest: _Manifest, request: TargetJobRequest) -> None:
    plan = request.declaration.plan
    pre_root = manifest.pre_root
    if (
        pre_root.job_id != request.declaration.job_id
        or pre_root.request_sha256 != request.request_sha256
        or pre_root.plan_sha256 != plan.plan_sha256
        or manifest.implementation.descriptor_sha256 != plan.target_descriptor_sha256
        or manifest.operation.contract_sha256 != plan.operation_contract_sha256
        or pre_root.production.job_id != pre_root.job_id
        or pre_root.production.plan_sha256 != pre_root.plan_sha256
        or pre_root.execution_evidence.plan_sha256 != pre_root.plan_sha256
        or pre_root.execution_evidence.target_descriptor_sha256 != plan.target_descriptor_sha256
        or pre_root.execution_evidence.operation_contract_sha256 != plan.operation_contract_sha256
    ):
        raise ValueError("publication checkpoint differs from accepted execution authority")


def _sync_directory(root: Path) -> None:
    descriptor = os.open(root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _immutable_file(
    path: Path, chunks: Iterable[bytes], *, expected: tuple[int, str] | None = None
) -> tuple[int, str]:
    """Publish an exact immutable checkpoint file with exclusive creation."""

    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.part")
    size = 0
    digest = hashlib.sha256()
    descriptor = os.open(
        temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            for chunk in chunks:
                for offset in range(0, len(chunk), 128 * 1024):
                    fragment = chunk[offset : offset + 128 * 1024]
                    if stream.write(fragment) != len(fragment):
                        raise OSError("short write while retaining target publication evidence")
                    size += len(fragment)
                    digest.update(fragment)
            stream.flush()
            os.fsync(stream.fileno())
        if expected is not None and (size, digest.hexdigest()) != expected:
            raise ValueError("target execution preimage differs from its sealed identity")
        try:
            # Both files are on the target's state filesystem. The exclusive
            # link preserves the first checkpoint across concurrent retries.
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError as exc:
            if path.is_symlink():
                raise ValueError("target checkpoint paths must not be symlinks") from exc
            old_size = 0
            old_digest = hashlib.sha256()
            with path.open("rb") as existing:
                while part := existing.read(128 * 1024):
                    old_size += len(part)
                    old_digest.update(part)
            if (size, digest.hexdigest()) != (old_size, old_digest.hexdigest()):
                raise ValueError("target publication checkpoint changed") from exc
        _sync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)
    return size, digest.hexdigest()


@dataclass(frozen=True, slots=True)
class TargetCompletionCheckpoint:
    implementation: TargetDescriptor
    operation: OperationContract
    pre_root: TargetPreRootResult
    execution: CompletionRecord
    source_context: Mapping[str, Any]

    @staticmethod
    def manifest_path(state_root: Path, job_id: str) -> Path:
        return state_root / f"{job_id}.completion.json"

    @classmethod
    def retain(
        cls,
        state_root: Path,
        *,
        request: TargetJobRequest,
        implementation: TargetDescriptor,
        operation: OperationContract,
        pre_root: TargetPreRootResult,
        execution: CompletionRecord,
        source_context: Mapping[str, Any],
    ) -> TargetCompletionCheckpoint:
        if execution.kind != "target-execution" or execution.bytes < 1:
            raise ValueError("publication checkpoint requires exact execution evidence")
        if execution.sha256 != pre_root.execution_evidence.execution_sha256:
            raise ValueError("publication checkpoint execution identity differs")
        previous = cls.load(state_root, request=request)
        if previous is not None:
            original = previous.pre_root
            if (
                original != pre_root.model_copy(update={"attempt": original.attempt})
                or previous.implementation != implementation
                or previous.operation != operation
                or previous.execution.bytes != execution.bytes
                or previous.source_context != source_context
            ):
                raise ValueError("sealed target publication evidence changed on retry")
            return previous
        manifest = _Manifest(
            implementation=implementation,
            operation=operation,
            pre_root=pre_root,
            execution_bytes=str(execution.bytes),
            source_context=dict(source_context),
        )
        _bind_manifest(manifest, request)
        path = state_root / f"{pre_root.job_id}.execution-{execution.sha256}.bin"
        _immutable_file(path, execution.read(), expected=(execution.bytes, execution.sha256))
        _immutable_file(
            cls.manifest_path(state_root, pre_root.job_id),
            (canonical_json_bytes(manifest.model_dump(mode="json")),),
        )
        saved = cls.load(state_root, request=request)
        assert saved is not None
        return saved

    @classmethod
    def load(
        cls, state_root: Path, *, request: TargetJobRequest
    ) -> TargetCompletionCheckpoint | None:
        path = cls.manifest_path(state_root, request.declaration.job_id)
        if path.is_symlink():
            raise ValueError("target checkpoint paths must not be symlinks")
        if not path.exists():
            return None
        manifest = _Manifest.model_validate_json(path.read_bytes())
        _bind_manifest(manifest, request)
        pre_root = manifest.pre_root
        size = int(manifest.execution_bytes)
        if size < 1 or str(size) != manifest.execution_bytes:
            raise ValueError("publication checkpoint byte count is not canonical")
        digest = pre_root.execution_evidence.execution_sha256
        execution_path = state_root / f"{pre_root.job_id}.execution-{digest}.bin"
        if execution_path.is_symlink() or execution_path.stat().st_size != size:
            raise ValueError("publication checkpoint execution preimage is missing or substituted")

        def read() -> Iterator[bytes]:
            observed_size = 0
            observed_digest = hashlib.sha256()
            with execution_path.open("rb") as source:
                while chunk := source.read(128 * 1024):
                    observed_size += len(chunk)
                    observed_digest.update(chunk)
                    yield chunk
            if (observed_size, observed_digest.hexdigest()) != (size, digest):
                raise ValueError("publication checkpoint execution preimage changed")

        return cls(
            manifest.implementation,
            manifest.operation,
            pre_root,
            CompletionRecord("target-execution", size, digest, read),
            manifest.source_context,
        )


__all__ = ["TargetCompletionCheckpoint"]
