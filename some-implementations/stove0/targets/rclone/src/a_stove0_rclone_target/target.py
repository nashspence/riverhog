"""Content-agnostic rclone delivery of an exact Riverhog artifact selection."""

from __future__ import annotations

import hashlib
import os
import subprocess
import tempfile
import threading
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import BinaryIO

from a_stove0_materialization_hint_evidence_contract_lib import (
    MATERIALIZATION_HINT_OBSERVER_CONTRACT,
    validate_materialization_hint_facts,
)
from pydantic import JsonValue
from riverhog_canonical_json import format_scalar
from riverhog_materialization import (
    DestinationRules,
    MemberAdvice,
    plan_materialization,
)
from riverhog_protocol import canonical_json_bytes, canonical_json_sha256
from stove0_observer_protocol import ContentObservationEvidence
from stove0_protocol import (
    ArtifactSelection,
    ArtifactSelectionRef,
    JsonSchemaValidationProfile,
    WorkArtifactSubject,
    update_artifact_selection_commitment,
)
from stove0_target_support import (
    DEFAULT_TERMINAL_STATE_RETENTION_SECONDS,
    InputArtifact,
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
        "required": ["destination_identity", "destination_rules_sha256"],
        "properties": {
            "destination_identity": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
            "destination_rules_sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
        },
        "additionalProperties": False,
    },
)


@dataclass(frozen=True, slots=True)
class RcloneDestination:
    """Deployment-owned destination; credentials never enter a recipe or receipt."""

    identity: str
    remote: str
    naming_rules: DestinationRules
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
        if not isinstance(self.naming_rules, DestinationRules):
            raise TypeError("rclone destination requires qualified naming rules")

    @property
    def rules_sha256(self) -> str:
        return canonical_json_sha256(asdict(self.naming_rules))

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
            with tempfile.TemporaryFile(mode="w+b") as readback:
                subprocess.run(
                    [*command, "cat", marker],
                    check=True,
                    stdout=readback,
                    stderr=subprocess.DEVNULL,
                    timeout=self.timeout_seconds,
                )
                readback.seek(0)
                if _stream_identity(readback) != _file_identity(manifest_path):
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
        if (
            request.target_options.get("destination_identity") != self.destination.identity
            or request.target_options.get("destination_rules_sha256")
            != self.destination.rules_sha256
        ):
            raise TargetServiceError(
                400,
                "invalid_target_request",
                "rclone destination or naming rules differ from the configured identity",
            )
        _planned_destinations(
            request.observations, request.inputs.selection, self.destination.naming_rules
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
            or request.declaration.plan.target_options.get("destination_rules_sha256")
            != self.destination.rules_sha256
        ):
            raise ValueError("sealed rclone plan differs from configured destination")
        workflow = request.declaration.controller_evidence.execution_envelope.workflow_plan
        destinations = _planned_destinations(
            workflow.observations,
            request.declaration.plan.inputs.selection,
            self.destination.naming_rules,
        )

        def check() -> None:
            if cancellation.is_set():
                raise TargetExecutionCanceled("rclone delivery was canceled before publication")

        with TargetExecutionRuntime.from_request(
            request,
            cancellation_check=check,
            producer_version=self.implementation_version,
            session=session,
        ) as execution:
            _verify_selected_inputs(
                (artifact for artifact, _claimed in execution.iter_inputs()),
                destinations,
                request.declaration.plan.inputs.selection,
            )
            workspace = execution.open_workspace(self.workspace_root)
            try:
                objects_root = workspace.resolve("output/objects")
                manifest_path = workspace.resolve("control/manifest.json")
                manifest_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                delivered_digest = hashlib.sha256()
                delivered_count = 0

                def delivered_entries() -> Iterator[tuple[str, int, dict[str, JsonValue]]]:
                    nonlocal delivered_count
                    for artifact, claimed in execution.iter_inputs():
                        check()
                        planned = destinations.get(artifact.id)
                        if planned is None:
                            raise ValueError("accepted hint evidence differs from the sealed input")
                        if (
                            planned.subject.collection != artifact.collection
                            or planned.subject.artifact_id != artifact.artifact_id
                            or planned.subject.bytes != artifact.bytes
                            or planned.subject.sha256 != artifact.sha256
                        ):
                            raise ValueError("forwarded hint belongs to another sealed input")
                        update_artifact_selection_commitment(
                            delivered_digest,
                            ordinal=delivered_count,
                            artifact=WorkArtifactSubject.model_validate(
                                artifact.model_dump(mode="json")
                            ),
                        )
                        delivered_count += 1
                        relative = planned.relative_path
                        local = objects_root / relative
                        local.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                        with execution.prepare_inputs((artifact,)) as retrieval:
                            retrieval.download(claimed, local)
                        size, sha256 = _file_identity(local)
                        if (size, sha256) != (int(artifact.bytes), artifact.sha256):
                            raise ValueError(
                                "retrieved artifact differs from the sealed input authority"
                            )
                        yield (
                            artifact.id,
                            int(artifact.bytes),
                            {
                                "collection": artifact.collection.model_dump(mode="json"),
                                "subject_id": artifact.id,
                                "artifact_id": str(artifact.artifact_id),
                                "role": artifact.role,
                                "bytes": str(artifact.bytes),
                                "sha256": artifact.sha256,
                                "delivered_path": relative,
                                "materialization_reason": planned.reason,
                                "canonical_occurrence": planned.occurrence,
                                "canonical_primary_binding": planned.primary_binding,
                            },
                        )

                artifact_count, total_bytes, manifest_sha256 = _write_delivery_manifest(
                    manifest_path,
                    delivery_id=request.declaration.job_id,
                    selection=request.declaration.plan.inputs.selection,
                    entries=delivered_entries(),
                )
                if (
                    delivered_count != artifact_count
                    or delivered_digest.hexdigest()
                    != request.declaration.plan.inputs.selection.selection_sha256
                ):
                    raise ValueError("rclone delivered inputs differ from the exact selection")
                check()
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
                        "artifact_count": artifact_count,
                        "total_bytes": format_scalar("nonnegative", total_bytes),
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
    with path.open("rb") as stream:
        return _stream_identity(stream)


def _stream_identity(stream: BinaryIO) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    while chunk := stream.read(8 * 1024**2):
        digest.update(chunk)
        size += len(chunk)
    return size, digest.hexdigest()


def _write_delivery_manifest(
    path: Path,
    *,
    delivery_id: str,
    selection: ArtifactSelectionRef,
    entries: Iterable[tuple[str, int, Mapping[str, JsonValue]]],
) -> tuple[int, int, str]:
    """Stream one canonical marker after validating the complete ordered input extent."""

    digest = hashlib.sha256()
    count = 0
    total_bytes = 0
    previous_id: str | None = None
    with path.open("xb") as manifest:

        def write(part: bytes) -> None:
            manifest.write(part)
            digest.update(part)

        write(b'{"artifacts":[')
        for subject_id, member_bytes, entry in entries:
            if previous_id is not None and subject_id <= previous_id:
                raise ValueError("rclone input pages are not ordered by subject identity")
            previous_id = subject_id
            if count:
                write(b",")
            write(canonical_json_bytes(dict(entry)))
            count += 1
            total_bytes += member_bytes
        if count != selection.artifact_count or total_bytes != int(selection.total_bytes):
            raise ValueError("rclone input pages differ from the sealed selection")
        write(b'],"delivery_id":')
        write(canonical_json_bytes(delivery_id))
        write(b',"format":"stove0-rclone-delivery-manifest/v1","source_selection_sha256":')
        write(canonical_json_bytes(selection.selection_sha256))
        write(b"}")
    return count, total_bytes, digest.hexdigest()


@dataclass(frozen=True, slots=True)
class _PlannedDelivery:
    relative_path: str
    reason: str
    occurrence: dict[str, JsonValue]
    primary_binding: dict[str, JsonValue]
    subject: WorkArtifactSubject


def _verify_selected_inputs(
    inputs: Iterable[InputArtifact],
    planned: Mapping[str, _PlannedDelivery],
    selection: ArtifactSelectionRef,
) -> None:
    """Check every controller-selected member and role before staging delivery bytes."""

    digest = hashlib.sha256()
    count = 0
    total_bytes = 0
    for artifact in inputs:
        item = planned.get(artifact.id)
        if (
            item is None
            or item.subject.collection != artifact.collection
            or item.subject.artifact_id != artifact.artifact_id
            or item.subject.bytes != artifact.bytes
            or item.subject.sha256 != artifact.sha256
        ):
            raise ValueError("accepted hint evidence differs from the exact input selection")
        update_artifact_selection_commitment(
            digest,
            ordinal=count,
            artifact=WorkArtifactSubject.model_validate(artifact.model_dump(mode="json")),
        )
        count += 1
        total_bytes += int(artifact.bytes)
    if (
        count != selection.artifact_count
        or total_bytes != int(selection.total_bytes)
        or digest.hexdigest() != selection.selection_sha256
    ):
        raise ValueError("accepted hint evidence differs from the exact input selection")


def _planned_destinations(
    evidence: tuple[ContentObservationEvidence, ...],
    selection: ArtifactSelectionRef,
    rules: DestinationRules,
) -> dict[str, _PlannedDelivery]:
    """Interpret only controller-accepted hint facts for the exact sealed selection."""

    selected = tuple(
        item
        for item in evidence
        if item.request.observer_contract_id == MATERIALIZATION_HINT_OBSERVER_CONTRACT.id
    )
    if not selected:
        raise ValueError(
            "rclone delivery requires accepted canonical hint evidence for every input"
        )
    subjects = []
    advice: dict[int, list[MemberAdvice]] = defaultdict(list)
    subject_ids: dict[tuple[int, str], str] = {}
    occurrence_by_subject: dict[str, dict[str, JsonValue]] = {}
    binding_by_subject: dict[str, dict[str, JsonValue]] = {}
    subject_by_id: dict[str, WorkArtifactSubject] = {}
    for item in selected:
        if (
            item.request.observer_contract_sha256
            != MATERIALIZATION_HINT_OBSERVER_CONTRACT.contract_sha256
            or item.request.read_actions != ("read-provenance",)
            or item.request.options
            or item.result.facts_schema != MATERIALIZATION_HINT_OBSERVER_CONTRACT.facts_schema
            or item.result.facts is None
        ):
            raise ValueError("forwarded hint evidence has the wrong accepted contract")
        facts = validate_materialization_hint_facts(item.result.facts, item.request.subjects)
        for subject, fact in zip(item.request.subjects, facts.artifacts, strict=True):
            subjects.append(subject)
            key = (subject.collection.collection_id, subject.artifact_id)
            if key in subject_ids:
                raise ValueError("hint evidence repeats a selected collection member")
            subject_ids[key] = subject.id
            subject_by_id[subject.id] = subject
            occurrence_by_subject[subject.id] = fact.occurrence.model_dump(mode="json")
            binding_by_subject[subject.id] = fact.primary_binding.model_dump(mode="json")
            advice[key[0]].append(MemberAdvice(subject.artifact_id, fact.materialization_hint))
    observed = ArtifactSelection.seal(subjects)
    # Observer subjects can carry the preclassification role; target roles are
    # assigned by the accepted recipe. Execution checks every immutable member
    # against the sealed input selection before any delivery.
    if (
        observed.artifact_count != selection.artifact_count
        or observed.total_bytes != selection.total_bytes
    ):
        raise ValueError("forwarded hint evidence does not cover the input selection")
    planned: dict[str, _PlannedDelivery] = {}
    for collection_id, members in advice.items():
        prefix = str(collection_id)
        remaining = rules.relative_path_bytes - len(prefix.encode("utf-8")) - 1
        if remaining < 1:
            raise ValueError("destination cannot represent a collection-qualified path")
        per_collection = replace(rules, relative_path_bytes=remaining)
        for row in plan_materialization(members, rules=per_collection):
            components = (prefix, *row.components)
            if not rules.fits(components):
                raise ValueError("planned delivery exceeds qualified destination limits")
            subject_id = subject_ids[(collection_id, row.artifact_id)]
            planned[subject_id] = _PlannedDelivery(
                "/".join(components),
                row.reason,
                occurrence_by_subject[subject_id],
                binding_by_subject[subject_id],
                subject_by_id[subject_id],
            )
    return planned


__all__ = ["RCLONE_OPTIONS", "RcloneDestination", "RcloneEffectTargetService"]
