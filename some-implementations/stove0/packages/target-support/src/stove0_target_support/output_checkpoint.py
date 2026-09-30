"""Target-local restart state for exact produced files, never archive naming evidence."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict
from riverhog_canonical_json import canonical_json_bytes
from riverhog_client.processing import ProcessingWorkspace
from riverhog_client.producer import ProducerFile
from riverhog_protocol import ArtifactId
from riverhog_protocol.workspace_protection import DeclaredWorkspaceProtection
from stove0_protocol import Sha256
from stove0_target_protocol import OutputArtifact, TargetJobRequest

from stove0_target_support.completion_checkpoint import _immutable_file, _sync_directory

_MAX_CHECKPOINT_BYTES = 64 * 1024


class TargetOutputCheckpoint(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    format: Literal["stove0-output-checkpoint/v1"] = "stove0-output-checkpoint/v1"
    job_id: Sha256
    request_sha256: Sha256
    plan_sha256: Sha256
    output: OutputArtifact
    source_edges_sha256: Sha256
    workspace_root: str
    workspace_protection: DeclaredWorkspaceProtection
    workspace_relative_path: str
    materialization_hint: tuple[str, ...] | None
    allow_missing_materialization_hint: bool

    @staticmethod
    def _path(root: Path, request: TargetJobRequest, output_id: str) -> Path:
        directory = root / f"{request.declaration.job_id}.outputs"
        if directory.is_symlink():
            raise ValueError("target output checkpoint directory must not be a symlink")
        directory.mkdir(mode=0o700, exist_ok=True)
        if not directory.is_dir() or directory.stat().st_mode & 0o077:
            raise ValueError("target output checkpoint directory must be private")
        return directory / (hashlib.sha256(output_id.encode("utf-8")).hexdigest() + ".json")

    @staticmethod
    def has_records(root: Path, request: TargetJobRequest) -> bool:
        return TargetOutputCheckpoint.has_job_records(root, request.declaration.job_id)

    @staticmethod
    def has_job_records(root: Path, job_id: Sha256) -> bool:
        directory = root / f"{job_id}.outputs"
        if directory.is_symlink():
            raise ValueError("target output checkpoint directory must not be a symlink")
        try:
            with os.scandir(directory) as records:
                return next(records, None) is not None
        except FileNotFoundError:
            return False

    @staticmethod
    def _member_path(root: Path, request: TargetJobRequest, artifact_id: ArtifactId) -> Path:
        directory = root / f"{request.declaration.job_id}.output-members"
        if directory.is_symlink():
            raise ValueError("target output checkpoint index must not be a symlink")
        directory.mkdir(mode=0o700, exist_ok=True)
        if not directory.is_dir() or directory.stat().st_mode & 0o077:
            raise ValueError("target output checkpoint index must be private")
        return directory / (str(ArtifactId(artifact_id)) + ".json")

    @classmethod
    def _read(cls, path: Path, request: TargetJobRequest) -> TargetOutputCheckpoint | None:
        if path.is_symlink():
            raise ValueError("target output checkpoint must not be a symlink")
        try:
            with path.open("rb") as stream:
                raw = stream.read(_MAX_CHECKPOINT_BYTES + 1)
        except FileNotFoundError:
            return None
        if len(raw) > _MAX_CHECKPOINT_BYTES:
            raise ValueError("target output checkpoint exceeds its bounded record size")
        checkpoint = cls.model_validate_json(raw)
        if (
            checkpoint.job_id != request.declaration.job_id
            or checkpoint.request_sha256 != request.request_sha256
            or checkpoint.plan_sha256 != request.declaration.plan.plan_sha256
            or checkpoint.workspace_protection != request.declaration.declared_workspace_protection
        ):
            raise ValueError("target output checkpoint differs from accepted execution")
        return checkpoint

    @classmethod
    def load(
        cls, root: Path, *, request: TargetJobRequest, output_id: str
    ) -> TargetOutputCheckpoint | None:
        checkpoint = cls._read(cls._path(root, request, output_id), request)
        if checkpoint is not None and checkpoint.output.id != output_id:
            raise ValueError("target output checkpoint differs from accepted execution")
        if checkpoint is not None:
            # A process can stop between the two immutable writes. Rebuild
            # only the index of this exact, validated checkpoint.
            _immutable_file(
                cls._member_path(root, request, checkpoint.output.artifact_id),
                (canonical_json_bytes(checkpoint.model_dump(mode="json")),),
            )
            _sync_directory(root)
        return checkpoint

    @classmethod
    def load_member(
        cls, root: Path, *, request: TargetJobRequest, artifact_id: ArtifactId
    ) -> TargetOutputCheckpoint | None:
        checkpoint = cls._read(cls._member_path(root, request, artifact_id), request)
        if checkpoint is not None and checkpoint.output.artifact_id != artifact_id:
            raise ValueError("target output checkpoint differs from the requested member")
        return checkpoint

    @classmethod
    def retain(
        cls,
        root: Path,
        *,
        request: TargetJobRequest,
        output: OutputArtifact,
        source_edges_sha256: str,
        source: ProducerFile,
        workspace: ProcessingWorkspace,
    ) -> TargetOutputCheckpoint:
        relative = source.source.relative_to(workspace.root).as_posix()
        if workspace.resolve(relative) != source.source:
            raise ValueError("output checkpoint requires a protected workspace file")
        checkpoint = cls(
            job_id=request.declaration.job_id,
            request_sha256=request.request_sha256,
            plan_sha256=request.declaration.plan.plan_sha256,
            output=output,
            source_edges_sha256=source_edges_sha256,
            workspace_root=str(workspace.root),
            workspace_protection=workspace.declared_protection,
            workspace_relative_path=relative,
            materialization_hint=source.materialization_hint,
            allow_missing_materialization_hint=source.allow_missing_materialization_hint,
        )
        encoded = canonical_json_bytes(checkpoint.model_dump(mode="json"))
        if len(encoded) > _MAX_CHECKPOINT_BYTES:
            raise ValueError("target output checkpoint exceeds its bounded record size")
        # Pending bytes stay in the existing protected workspace. Sync them
        # before publishing the metadata that makes them restartable.
        with source.source.open("rb") as stream:
            os.fsync(stream.fileno())
        parent = source.source.parent
        while parent != workspace.root.parent:
            _sync_directory(parent)
            parent = parent.parent
        _immutable_file(cls._path(root, request, output.id), (encoded,))
        _immutable_file(cls._member_path(root, request, output.artifact_id), (encoded,))
        _sync_directory(root)
        return checkpoint

    def producer_file(self, workspace: ProcessingWorkspace) -> ProducerFile:
        if (
            str(workspace.root) != self.workspace_root
            or workspace.execution_id != self.job_id
            or workspace.declared_protection != self.workspace_protection
        ):
            raise ValueError("output checkpoint workspace differs from accepted execution")
        return ProducerFile(
            workspace.resolve(self.workspace_relative_path),
            self.output.artifact_id,
            materialization_hint=self.materialization_hint,
            allow_missing_materialization_hint=self.allow_missing_materialization_hint,
        )


__all__ = ["TargetOutputCheckpoint"]
