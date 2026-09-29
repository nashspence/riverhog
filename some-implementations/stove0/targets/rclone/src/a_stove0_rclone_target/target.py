"""Content-agnostic rclone delivery of an exact Riverhog artifact selection."""

from __future__ import annotations

import hashlib
import os
import subprocess
import threading
from dataclasses import dataclass
from pathlib import Path

from pydantic import JsonValue
from riverhog_protocol import canonical_json_bytes, canonical_json_sha256
from stove0_protocol import JsonSchemaValidationProfile
from stove0_target_support import (
    DEFAULT_TERMINAL_STATE_RETENTION_SECONDS,
    PersistentTargetService,
    TargetDescriptor,
    TargetDescriptorPayload,
    TargetEffectCommitUncertain,
    TargetExecutionCanceled,
    TargetExecutionRuntime,
    TargetExecutionSession,
    TargetJobRequest,
    TargetJobStatus,
    TargetOperationSupport,
    TargetPreflightRequest,
    TargetPreflightResponse,
    TargetServiceError,
)

from a_stove0_rclone_target.contracts import RCLONE_DELIVER_OPERATION

RCLONE_OPTIONS = JsonSchemaValidationProfile.from_schema(
    "a-stove0-rclone-target-options/v1",
    {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": ["destination_identity"],
        "properties": {
            "destination_identity": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
        },
        "additionalProperties": False,
    },
)


@dataclass(frozen=True, slots=True)
class RcloneDestination:
    """Deployment-owned destination; credentials never enter a recipe or receipt."""

    identity: str
    remote: str
    config_path: Path | None = None
    executable: str = "rclone"
    timeout_seconds: int = 86400

    def __post_init__(self) -> None:
        if len(self.identity) != 64 or any(c not in "0123456789abcdef" for c in self.identity):
            raise ValueError("destination identity must be a lowercase SHA-256")
        if not self.remote or self.remote != self.remote.strip():
            raise ValueError("rclone remote must be nonempty and canonical")
        if not self.executable or self.executable != self.executable.strip():
            raise ValueError("rclone executable must be nonempty and canonical")
        if self.config_path is not None and not self.config_path.is_absolute():
            raise ValueError("rclone config path must be absolute")
        if self.timeout_seconds < 1:
            raise ValueError("rclone timeout must be positive")

    def commit(self, *, delivery_id: str, objects_root: Path, manifest_path: Path) -> None:
        """Verify remote bytes, publish the marker last, then read it back exactly."""

        destination = f"{self.remote.rstrip('/')}/{delivery_id}"
        command = [self.executable]
        if self.config_path is not None:
            command.extend(("--config", str(self.config_path)))
        try:
            subprocess.run(
                [*command, "copy", str(objects_root), f"{destination}/objects"],
                check=True,
                capture_output=True,
                timeout=self.timeout_seconds,
            )
            subprocess.run(
                [*command, "check", "--download", str(objects_root), f"{destination}/objects"],
                check=True,
                capture_output=True,
                timeout=self.timeout_seconds,
            )
            marker = f"{destination}/manifest.json"
            subprocess.run(
                [*command, "copyto", str(manifest_path), marker],
                check=True,
                capture_output=True,
                timeout=self.timeout_seconds,
            )
            readback = subprocess.run(
                [*command, "cat", marker],
                check=True,
                capture_output=True,
                timeout=self.timeout_seconds,
            ).stdout
            if readback != manifest_path.read_bytes():
                raise ValueError("rclone delivery marker differs from its sealed manifest")
        except Exception as exc:
            raise TargetEffectCommitUncertain(
                "rclone delivery may have committed; inspect the configured destination"
            ) from exc


class RcloneEffectTargetService(PersistentTargetService):
    def __init__(
        self,
        *,
        state_root: Path,
        workspace_root: Path,
        destination: RcloneDestination,
        source_revision: str = "unknown",
        image_id: str,
        implementation_version: str,
        terminal_state_retention_seconds: int = DEFAULT_TERMINAL_STATE_RETENTION_SECONDS,
    ) -> None:
        self.destination = destination
        self.workspace_root = workspace_root.resolve()
        self.workspace_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.workspace_root, 0o700)
        self.implementation_version = implementation_version
        descriptor = TargetDescriptor.seal(
            TargetDescriptorPayload(
                protocol="stove0-effect-target/v1",
                implementation_id="a-stove0-rclone-target/v1",
                implementation_version=implementation_version,
                source_revision=source_revision,
                image_id=image_id,
                operations=(
                    TargetOperationSupport(
                        operation_id=RCLONE_DELIVER_OPERATION.id,
                        operation_contract_sha256=RCLONE_DELIVER_OPERATION.contract_sha256,
                        result_kind="external-effect",
                        options_schema=RCLONE_OPTIONS,
                    ),
                ),
            )
        )
        super().__init__(
            descriptor=descriptor,
            operations={RCLONE_DELIVER_OPERATION.id: RCLONE_DELIVER_OPERATION},
            state_root=state_root,
            execute=self._execute,
            terminal_state_retention_seconds=terminal_state_retention_seconds,
        )

    def preflight(self, request: TargetPreflightRequest) -> TargetPreflightResponse:
        if request.target_options.get("destination_identity") != self.destination.identity:
            raise TargetServiceError(
                400,
                "invalid_target_request",
                "rclone destination differs from the configured identity",
            )
        return super().preflight(request)

    def _execute(
        self,
        request: TargetJobRequest,
        attempt: int,
        cancellation: threading.Event,
        session: TargetExecutionSession,
    ) -> TargetJobStatus:
        if (
            request.declaration.plan.target_options.get("destination_identity")
            != self.destination.identity
        ):
            raise ValueError("sealed rclone plan differs from configured destination")

        def check() -> None:
            if cancellation.is_set():
                raise TargetExecutionCanceled("rclone delivery was canceled before publication")

        with TargetExecutionRuntime.from_request(
            request,
            cancellation_check=check,
            producer_version=self.implementation_version,
            session=session,
        ) as execution:
            workspace = execution.open_workspace(self.workspace_root)
            try:
                objects_root = workspace.resolve("output/objects")
                entries: list[dict[str, JsonValue]] = []
                total_bytes = 0
                for artifact, claimed in execution.iter_inputs():
                    check()
                    relative = f"{artifact.collection.collection_id}/{artifact.artifact_id}"
                    local = objects_root / relative
                    local.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                    with execution.prepare_inputs((artifact,)) as retrieval:
                        retrieval.download(claimed, local)
                    size, sha256 = _file_identity(local)
                    if (size, sha256) != (int(artifact.bytes), artifact.sha256):
                        raise ValueError(
                            "retrieved artifact differs from the sealed input authority"
                        )
                    entries.append(
                        {
                            "collection": artifact.collection.model_dump(mode="json"),
                            "subject_id": artifact.id,
                            "artifact_id": artifact.artifact_id,
                            "role": artifact.role,
                            "bytes": str(artifact.bytes),
                            "sha256": artifact.sha256,
                            "delivered_path": relative,
                        }
                    )
                    total_bytes += int(artifact.bytes)
                if len(entries) != request.declaration.plan.inputs.selection.artifact_count:
                    raise ValueError("rclone input page count differs from the sealed selection")
                check()
                entries.sort(key=lambda item: str(item["delivered_path"]))
                manifest = canonical_json_bytes(
                    {
                        "format": "stove0-rclone-delivery-manifest/v1",
                        "delivery_id": request.declaration.job_id,
                        "source_selection_sha256": (
                            request.declaration.plan.inputs.selection.selection_sha256
                        ),
                        "artifacts": entries,
                    }
                )
                manifest_path = workspace.resolve("control/manifest.json")
                manifest_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                manifest_path.write_bytes(manifest)
                manifest_sha256 = hashlib.sha256(manifest).hexdigest()
                execution_sha256 = canonical_json_sha256(
                    {
                        "format": "stove0-rclone-target-execution/v1",
                        "plan_sha256": request.declaration.plan.plan_sha256,
                        "manifest_sha256": manifest_sha256,
                    }
                )
                self.destination.commit(
                    delivery_id=request.declaration.job_id,
                    objects_root=objects_root,
                    manifest_path=manifest_path,
                )
                return execution.effect_success(
                    {
                        "format": "stove0-rclone-delivery-receipt/v1",
                        "destination_identity": self.destination.identity,
                        "delivery_id": request.declaration.job_id,
                        "source_selection_sha256": (
                            request.declaration.plan.inputs.selection.selection_sha256
                        ),
                        "manifest_sha256": manifest_sha256,
                        "artifact_count": len(entries),
                        "total_bytes": total_bytes,
                        "verification": "rclone-download-check-and-manifest-readback/v1",
                    },
                    operation=RCLONE_DELIVER_OPERATION,
                    execution_sha256=execution_sha256,
                    attempt=attempt,
                    runtime_evidence={"manifest_sha256": manifest_sha256},
                )
            finally:
                if not execution.completed:
                    workspace.release()


def _file_identity(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        while chunk := stream.read(8 * 1024**2):
            digest.update(chunk)
            size += len(chunk)
    return size, digest.hexdigest()


__all__ = ["RCLONE_OPTIONS", "RcloneDestination", "RcloneEffectTargetService"]
