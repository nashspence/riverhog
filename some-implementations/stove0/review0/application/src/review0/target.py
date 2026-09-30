"""Review0's single collection-producing Stove0 target."""

from __future__ import annotations

import hashlib
import os
import threading
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from jsonschema import Draft202012Validator
from pydantic import BaseModel, ConfigDict, JsonValue
from review0_contracts import (
    REVIEW_INDEX_ROLE,
    REVIEW_MATERIALIZE_OPERATION,
    ReviewMaterializeIntent,
    validate_review_materialize_intent,
)
from review0_sampler_client import ReviewSamplerClient
from review0_sampler_protocol import (
    SamplerDescriptor,
    SamplerInput,
    SamplerRequest,
    SamplerRequestPayload,
    SamplerResult,
    SamplerWindow,
    validate_result,
)
from riverhog_canonical_json import canonical_json_bytes
from riverhog_client import ProducerFile
from riverhog_client.processing import ProcessingWorkspace
from riverhog_protocol import canonical_json_sha256
from riverhog_protocol.artifact_identity import ArtifactId
from stove0_protocol import JsonSchemaValidationProfile, OciImageId
from stove0_target_support import (
    DEFAULT_TERMINAL_STATE_RETENTION_SECONDS,
    OutputArtifact,
    PersistentTargetService,
    TargetDescriptor,
    TargetDescriptorPayload,
    TargetExecutionCanceled,
    TargetExecutionFailure,
    TargetExecutionInapplicable,
    TargetExecutionRuntime,
    TargetExecutionSession,
    TargetJobRequest,
    TargetJobStatus,
    TargetOperationSupport,
    TargetPreflightRequest,
    TargetPreflightResponse,
    TargetServiceError,
)

_SAMPLER_OPTION_PROPERTIES: dict[str, JsonValue] = {
    "sampler_registration_id": {
        "type": "string",
        "pattern": "^[a-z0-9](?:[a-z0-9._-]{0,118}[a-z0-9])?$",
    },
    "sampler_timeout_seconds": {"type": "integer", "minimum": 1, "maximum": 86400},
    "maximum_output_bytes": {
        "type": "integer",
        "minimum": 1,
        "maximum": 1024**4,
    },
    "sampler_descriptor_sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
}


MATERIALIZE_OPTIONS = JsonSchemaValidationProfile.from_schema(
    "review0-materialize-options/v1",
    {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": ["sampler_registration_id"],
        "properties": _SAMPLER_OPTION_PROPERTIES,
        "additionalProperties": False,
    },
)


class _SamplerCheckpoint(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    target_request_sha256: str
    request: SamplerRequest
    result: SamplerResult


@dataclass(frozen=True, slots=True)
class SamplerRegistration:
    id: str
    client: ReviewSamplerClient
    descriptor_sha256: str
    image_id: OciImageId

    def descriptor(self) -> SamplerDescriptor:
        descriptor = self.client.descriptor()
        if descriptor.descriptor_sha256 != self.descriptor_sha256:
            raise RuntimeError(f"configured sampler descriptor changed: {self.id}")
        if descriptor.image_id != self.image_id:
            raise RuntimeError(f"configured sampler ImageID changed: {self.id}")
        return descriptor


class ReviewMaterializeTargetService(PersistentTargetService):
    """Coordinate a sampler and publish one finalized Riverhog collection."""

    def __init__(
        self,
        *,
        state_root: Path,
        workspace_root: Path,
        samplers: tuple[SamplerRegistration, ...],
        source_revision: str = "unknown",
        image_id: str,
        implementation_version: str,
        terminal_state_retention_seconds: int = DEFAULT_TERMINAL_STATE_RETENTION_SECONDS,
    ) -> None:
        if not samplers or [item.id for item in samplers] != sorted(item.id for item in samplers):
            raise ValueError("review sampler registrations must be nonempty and ordered")
        if len({item.id for item in samplers}) != len(samplers):
            raise ValueError("review sampler registrations must be unique")
        self.workspace_root = workspace_root.resolve()
        self.workspace_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.workspace_root, 0o700)
        self.samplers = {item.id: item for item in samplers}
        self.implementation_version = implementation_version
        operation = REVIEW_MATERIALIZE_OPERATION
        descriptor = TargetDescriptor.seal(
            TargetDescriptorPayload(
                protocol="stove0-transform-target/v1",
                implementation_id="review0/v1",
                implementation_version=implementation_version,
                source_revision=source_revision,
                image_id=image_id,
                operations=(
                    TargetOperationSupport(
                        operation_id=operation.id,
                        operation_contract_sha256=operation.contract_sha256,
                        result_kind=operation.result_kind,
                        options_schema=MATERIALIZE_OPTIONS,
                    ),
                ),
            )
        )
        super().__init__(
            descriptor=descriptor,
            operations={operation.id: operation},
            state_root=state_root,
            execute=self._execute,
            intent_semantic_validators={
                operation.intent_semantics.profile_sha256: validate_review_materialize_intent
            },
            terminal_state_retention_seconds=terminal_state_retention_seconds,
        )

    def preflight(self, request: TargetPreflightRequest) -> TargetPreflightResponse:
        try:
            intent = ReviewMaterializeIntent.model_validate(request.intent)
        except ValueError as exc:
            raise TargetServiceError(
                400,
                "invalid_target_request",
                "review target intent is invalid",
            ) from exc
        try:
            sampler_id = str(request.target_options["sampler_registration_id"])
        except (KeyError, ValueError) as exc:
            raise TargetServiceError(
                400,
                "invalid_target_request",
                "review target requires a sampler registration",
            ) from exc
        try:
            registration = self.samplers[sampler_id]
        except KeyError as exc:
            raise TargetServiceError(
                400,
                "invalid_target_request",
                f"review sampler is not configured: {sampler_id}",
            ) from exc
        descriptor = registration.descriptor()
        try:
            Draft202012Validator(descriptor.portable_intent_schema.document).validate(
                intent.variant.portable_intent
            )
        except Exception as exc:
            raise TargetServiceError(
                400,
                "invalid_target_request",
                "review variant intent is invalid for the selected sampler",
            ) from exc
        expected: dict[str, JsonValue] = {
            "sampler_descriptor_sha256": descriptor.descriptor_sha256,
        }
        for key, value in expected.items():
            supplied = request.target_options.get(key)
            if supplied is not None and supplied != value:
                raise TargetServiceError(
                    400,
                    "invalid_target_request",
                    f"configured review {key} differs from the requested value",
                )
        return self._seal_preflight(request, execution_parameters=expected)

    def close(self) -> None:
        super().close()
        for registration in self.samplers.values():
            registration.client.close()

    def readiness(self) -> dict[str, str]:
        return {
            registration.id: registration.descriptor().descriptor_sha256
            for registration in self.samplers.values()
        }

    def _execute(
        self,
        request: TargetJobRequest,
        attempt: int,
        cancellation: threading.Event,
        session: TargetExecutionSession,
    ) -> TargetJobStatus:
        intent = ReviewMaterializeIntent.model_validate(request.declaration.plan.intent)
        sample_plan = intent.sample_plan
        portable_intent = intent.variant.portable_intent
        variant_id = intent.variant.id
        options = request.declaration.plan.target_options
        sampler_id = str(options["sampler_registration_id"])
        try:
            registration = self.samplers[sampler_id]
        except KeyError as exc:
            raise TargetExecutionInapplicable(
                "sampler-not-configured", f"Review sampler is not configured: {sampler_id}"
            ) from exc
        descriptor = registration.descriptor()
        if (
            request.declaration.plan.execution_parameters.get("sampler_descriptor_sha256")
            != descriptor.descriptor_sha256
        ):
            raise RuntimeError("sealed review plan differs from the selected sampler")
        Draft202012Validator(descriptor.portable_intent_schema.document).validate(portable_intent)
        timeout = options.get("sampler_timeout_seconds", 86400)
        maximum = options.get("maximum_output_bytes", 8 * 1024**3)
        if isinstance(timeout, bool) or not isinstance(timeout, int):
            raise ValueError("sampler_timeout_seconds must be an integer")
        if isinstance(maximum, bool) or not isinstance(maximum, int):
            raise ValueError("maximum_output_bytes must be an integer")

        def check() -> None:
            if cancellation.is_set():
                raise TargetExecutionCanceled("review target was canceled")

        with TargetExecutionRuntime.from_request(
            request,
            cancellation_check=check,
            producer_version=self.implementation_version,
            session=session,
        ) as execution:
            workspace = execution.open_workspace(self.workspace_root)
            try:
                windows = tuple(
                    SamplerWindow(
                        id=f"sample-{index:04d}",
                        input_id=window.artifact_id,
                        start_ms=window.start_ms,
                        duration_ms=window.duration_ms,
                        output_path=f"output/review/samples/{index:04d}-{window.artifact_id}{_suffix(descriptor)}",
                    )
                    for index, window in enumerate(sample_plan.windows, start=1)
                )
                artifacts: list[OutputArtifact] = []
                publication = execution.open_collection_publication(
                    implementation=self.descriptor()
                )
                samples: list[dict[str, object]] = []
                sampler_requests: list[SamplerRequest] = []
                sampler_results: list[SamplerResult] = []
                allowed_output_paths: set[str] = set()
                produced_bytes = 0
                for artifact, claimed in execution.iter_inputs():
                    artifact_windows = tuple(
                        item for item in windows if item.input_id == artifact.id
                    )
                    if not artifact_windows:
                        continue
                    check()
                    relative = f"input/{artifact.id}/payload"
                    source = workspace.resolve(relative)
                    source.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                    try:
                        remaining_bytes = maximum - produced_bytes
                        if remaining_bytes < 1:
                            raise RuntimeError("review output exceeded its sealed byte budget")
                        sampler_request = SamplerRequest.seal(
                            SamplerRequestPayload(
                                sampler_descriptor_sha256=descriptor.descriptor_sha256,
                                workspace_id=workspace.execution_id,
                                inputs=(
                                    SamplerInput(
                                        id=artifact.id,
                                        path=relative,
                                        bytes=artifact.bytes,
                                        sha256=artifact.sha256,
                                    ),
                                ),
                                windows=artifact_windows,
                                portable_intent=portable_intent,
                                maximum_output_bytes=remaining_bytes,
                                timeout_seconds=timeout,
                                cancellation_path="control/cancel",
                            )
                        )
                        step_key = "review-sampler:" + artifact.id
                        retained = session.load_step(step_key)
                        if retained is not None:
                            checkpoint = _SamplerCheckpoint.model_validate_json(retained)
                            if (
                                checkpoint.target_request_sha256 != request.request_sha256
                                or checkpoint.request != sampler_request
                            ):
                                raise ValueError(
                                    "sampler checkpoint differs from the accepted invocation"
                                )
                            sampler_result = checkpoint.result
                            validate_result(sampler_result, sampler_request, descriptor)
                        else:
                            with execution.prepare_inputs((artifact,)) as retrieval:
                                retrieval.download(claimed, source)
                            sampler_result = self._sample(
                                registration,
                                sampler_request,
                                cancellation=cancellation,
                                workspace=workspace,
                            )
                            validate_result(sampler_result, sampler_request, descriptor)
                            _require_sampler_success(sampler_result)
                            session.retain_step(
                                step_key,
                                canonical_json_bytes(
                                    _SamplerCheckpoint(
                                        target_request_sha256=request.request_sha256,
                                        request=sampler_request,
                                        result=sampler_result,
                                    ).model_dump(mode="json")
                                ),
                            )
                        _require_sampler_success(sampler_result)
                        sampler_requests.append(sampler_request)
                        sampler_results.append(sampler_result)
                        current_paths = {output.path for output in sampler_result.outputs}
                        allowed_output_paths.update(current_paths)
                        produced_bytes += sum(output.bytes for output in sampler_result.outputs)
                        for output in sorted(sampler_result.outputs, key=lambda item: item.path):
                            resumed = publication.resume_output(
                                output.id,
                                derived_from=output.derived_from,
                                materialization_hint=None,
                                allow_missing_materialization_hint=True,
                            )
                            if resumed is not None:
                                artifacts.append(resumed)
                                window = next(item for item in windows if item.id == output.id)
                                samples.append(
                                    {
                                        "artifact_id": output.id,
                                        "source_artifact_id": window.input_id,
                                        "output_artifact_id": resumed.artifact_id,
                                        "start_ms": window.start_ms,
                                        "duration_ms": window.duration_ms,
                                    }
                                )
                                continue
                            _verify_output_set(
                                workspace,
                                allowed=allowed_output_paths,
                                required={output.path},
                            )
                            path = workspace.resolve(output.path)
                            _verify_file(path, output.bytes, output.sha256)
                            member_id = _member_id(request.declaration.plan.plan_sha256, output.id)
                            output_artifact = OutputArtifact.model_validate(
                                dict(
                                    id=output.id,
                                    role=descriptor.output_role,
                                    artifact_id=member_id,
                                    bytes=str(output.bytes),
                                    sha256=output.sha256,
                                )
                            )
                            artifacts.append(output_artifact)
                            publication.append(
                                ProducerFile(
                                    path, member_id, allow_missing_materialization_hint=True
                                ),
                                output_artifact,
                                derived_from=output.derived_from,
                            )
                            window = next(item for item in windows if item.id == output.id)
                            samples.append(
                                {
                                    "artifact_id": output.id,
                                    "source_artifact_id": window.input_id,
                                    "output_artifact_id": member_id,
                                    "start_ms": window.start_ms,
                                    "duration_ms": window.duration_ms,
                                }
                            )
                    finally:
                        source.unlink(missing_ok=True)
                if not sampler_results:
                    raise RuntimeError("review sample plan produced no executable sampler groups")
                sampler_result_sha256 = canonical_json_sha256(
                    {
                        "format": "review0-sampler-result-set/v1",
                        "results": [item.result_sha256 for item in sampler_results],
                    }
                )
                index = publication.resume_output(
                    "review-index",
                    derived_from=(item.id for item, _claimed in execution.iter_inputs()),
                    materialization_hint=None,
                    allow_missing_materialization_hint=True,
                )
                if index is None:
                    index_path = workspace.resolve("output/review/summary.json")
                    index_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                    index_path.write_bytes(
                        canonical_json_bytes(
                            {
                                "format": "review0-index/v1",
                                "variant_id": variant_id,
                                "sample_plan": sample_plan.model_dump(mode="json"),
                                "sampler_descriptor": descriptor.model_dump(mode="json"),
                                "sampler_request_sha256s": [
                                    item.request_sha256 for item in sampler_requests
                                ],
                                "sampler_result_sha256s": [
                                    item.result_sha256 for item in sampler_results
                                ],
                                "sampler_result_set_sha256": sampler_result_sha256,
                                "samples": samples,
                            }
                        )
                    )
                    index_bytes, index_sha = file_identity(index_path)
                    index_member_id = _member_id(
                        request.declaration.plan.plan_sha256, "review-index"
                    )
                    index = OutputArtifact.model_validate(
                        dict(
                            id="review-index",
                            role=REVIEW_INDEX_ROLE,
                            artifact_id=index_member_id,
                            bytes=str(index_bytes),
                            sha256=index_sha,
                        )
                    )
                    artifacts.append(index)
                    publication.append(
                        ProducerFile(
                            index_path,
                            index_member_id,
                            allow_missing_materialization_hint=True,
                        ),
                        index,
                        derived_from=(item.id for item, _claimed in execution.iter_inputs()),
                    )
                else:
                    artifacts.append(index)
                declared = tuple(sorted(artifacts, key=lambda item: item.id))
                for artifact, _claimed in execution.iter_inputs():
                    execution.declare_disposition(artifact.id, "transformed")
                execution_preimage = _execution_preimage(
                    request.declaration.plan.plan_sha256,
                    sampler_result_sha256,
                    declared,
                )
                execution_sha256 = hashlib.sha256(execution_preimage).hexdigest()
                return publication.finish_success(
                    operation=REVIEW_MATERIALIZE_OPERATION,
                    execution_sha256=execution_sha256,
                    execution_preimage=execution_preimage,
                    attempt=attempt,
                    runtime_evidence={
                        "sampler_descriptor_sha256": descriptor.descriptor_sha256,
                        "sampler_result_set_sha256": sampler_result_sha256,
                    },
                )
            finally:
                if not execution.completed:
                    execution.release_workspace(workspace)

    @staticmethod
    def _sample(
        registration: SamplerRegistration,
        request: SamplerRequest,
        *,
        cancellation: threading.Event,
        workspace: ProcessingWorkspace,
    ) -> SamplerResult:
        stopped = threading.Event()

        def watch() -> None:
            while not stopped.wait(0.1):
                if cancellation.is_set():
                    path = workspace.resolve(request.cancellation_path)
                    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                    path.write_bytes(b"review0-cancel/v1\n")
                    return

        watcher = threading.Thread(target=watch, daemon=True, name="review-sampler-cancel")
        watcher.start()
        try:
            return registration.client.sample(request)
        finally:
            stopped.set()


def _suffix(descriptor: SamplerDescriptor) -> str:
    if descriptor.output_role.endswith("audio/v1"):
        return ".opus"
    if descriptor.output_role.endswith("video/v1"):
        return ".mkv"
    raise ValueError("sampler descriptor has an unsupported review output role")


def _require_sampler_success(result: SamplerResult) -> None:
    if result.state == "succeeded":
        return
    if result.state == "canceled":
        raise TargetExecutionCanceled("review sampler was canceled")
    if result.state == "inapplicable":
        outcome = result.inapplicable
        if outcome is None:
            raise RuntimeError("review sampler omitted its inapplicable outcome")
        raise TargetExecutionInapplicable(outcome.code, outcome.message)
    failure = result.failure
    if failure is None:
        raise RuntimeError("review sampler omitted its failure outcome")
    raise TargetExecutionFailure(
        failure.code,
        failure.message,
        retryable=failure.retryable,
    )


def _execution_sha256(
    plan_sha256: str, sampler_result_sha256: str, outputs: Sequence[OutputArtifact]
) -> str:
    return hashlib.sha256(
        _execution_preimage(plan_sha256, sampler_result_sha256, outputs)
    ).hexdigest()


def _execution_preimage(
    plan_sha256: str,
    sampler_result_sha256: str,
    outputs: Sequence[OutputArtifact],
) -> bytes:
    """Identify exact review execution semantics independently of an attempt."""

    return canonical_json_bytes(
        {
            "format": "review0-execution/v1",
            "plan_sha256": plan_sha256,
            "sampler_result_sha256": sampler_result_sha256,
            "outputs": [item.model_dump(mode="json") for item in outputs],
        }
    )


def _member_id(plan_sha256: str, output_id: str) -> ArtifactId:
    return ArtifactId(
        canonical_json_sha256(
            {
                "format": "review0-output-member-id/v1",
                "plan_sha256": plan_sha256,
                "output_id": output_id,
            }
        )
    )


def file_identity(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        while chunk := stream.read(8 * 1024**2):
            size += len(chunk)
            digest.update(chunk)
    return size, digest.hexdigest()


def _verify_file(path: Path, expected_bytes: int, expected_sha256: str) -> None:
    size, sha256 = file_identity(path)
    if (size, sha256) != (expected_bytes, expected_sha256):
        raise RuntimeError("sampler output differs from its declared identity")


def _verify_output_set(
    workspace: ProcessingWorkspace,
    *,
    allowed: set[str],
    required: set[str],
) -> None:
    output_root = workspace.resolve("output")
    actual = {
        path.relative_to(workspace.root).as_posix()
        for path in output_root.rglob("*")
        if path.is_file()
    }
    if not required <= actual or not actual <= allowed:
        raise RuntimeError("sampler workspace contains an undeclared output")


__all__ = [
    "MATERIALIZE_OPTIONS",
    "ReviewMaterializeTargetService",
    "SamplerRegistration",
    "file_identity",
]
