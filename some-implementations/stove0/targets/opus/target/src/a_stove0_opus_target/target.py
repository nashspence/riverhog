"""One-operation Opus collection transform target."""

from __future__ import annotations

import hashlib
import importlib.metadata
import os
import shutil
import threading
from collections.abc import Sequence
from pathlib import Path

from a_stove0_media_archive_contract_lib import (
    AUDIO_ARCHIVE_OPERATION,
    AUDIO_ARCHIVE_ROLE,
    METADATA_XMP_ROLE,
    SOURCE_ARTIFACT_ROLE,
    AudioArchiveIntent,
    validate_audio_archive_intent,
)
from a_stove0_media_archive_lib import (
    MaterializationDecisionRequired,
    MediaArchiveProjection,
    MediaPublicationPlan,
    accepted_source_hints,
    append_leaf_suffix,
    ffmpeg_container_metadata_args,
    render_projection_xmp,
    replace_final_suffix,
    resolve_media_archive_preflight_projection,
    seal_publication_plan,
)
from riverhog_canonical_json import canonical_json_bytes
from riverhog_client import ProducerFile
from riverhog_protocol import canonical_json_sha256
from riverhog_protocol.artifact_identity import ArtifactId
from stove0_protocol import JsonSchemaValidationProfile
from stove0_target_support import (
    DEFAULT_TERMINAL_STATE_RETENTION_SECONDS,
    OutputArtifact,
    PersistentTargetService,
    TargetDescriptor,
    TargetDescriptorPayload,
    TargetExecutionCanceled,
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

from a_stove0_opus_target.common import OpusContentError, file_identity, run_ffmpeg, tool_version

OPTIONS = JsonSchemaValidationProfile.from_schema(
    "a-stove0-opus-target-options/v1",
    {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "ffmpeg_timeout_seconds": {"type": "integer", "minimum": 1, "maximum": 86400},
            "allow_missing_materialization_hint": {"type": "boolean"},
        },
        "additionalProperties": False,
    },
)


def _version() -> str:
    try:
        return importlib.metadata.version("a-stove0-opus-target")
    except importlib.metadata.PackageNotFoundError:
        return "development"


class OpusTargetService(PersistentTargetService):
    def __init__(
        self,
        *,
        state_root: Path,
        workspace_root: Path,
        ffmpeg: str = "ffmpeg",
        source_revision: str = "unknown",
        image_id: str,
        terminal_state_retention_seconds: int = DEFAULT_TERMINAL_STATE_RETENTION_SECONDS,
    ) -> None:
        self.workspace_root = workspace_root.resolve()
        self.workspace_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.workspace_root, 0o700)
        self.ffmpeg = ffmpeg
        descriptor = TargetDescriptor.seal(
            TargetDescriptorPayload(
                implementation_id="a-stove0-opus-target/v1",
                implementation_version=_version(),
                source_revision=source_revision,
                image_id=image_id,
                operations=(
                    TargetOperationSupport(
                        operation_id=AUDIO_ARCHIVE_OPERATION.id,
                        operation_contract_sha256=AUDIO_ARCHIVE_OPERATION.contract_sha256,
                        options_schema=OPTIONS,
                    ),
                ),
            )
        )
        super().__init__(
            descriptor=descriptor,
            operations={AUDIO_ARCHIVE_OPERATION.id: AUDIO_ARCHIVE_OPERATION},
            state_root=state_root,
            execute=self._execute,
            intent_semantic_validators={
                AUDIO_ARCHIVE_OPERATION.intent_semantics.profile_sha256: (
                    validate_audio_archive_intent
                )
            },
            terminal_state_retention_seconds=terminal_state_retention_seconds,
        )

    def preflight(self, request: TargetPreflightRequest) -> TargetPreflightResponse:
        try:
            intent = AudioArchiveIntent.model_validate(request.intent)
            projection = resolve_media_archive_preflight_projection(
                request,
                policy=intent.metadata_projection,
            )
            hints, hint_results = accepted_source_hints(request)
            allow_missing = request.target_options.get("allow_missing_materialization_hint", False)
            if type(allow_missing) is not bool:
                raise ValueError("allow_missing_materialization_hint must be a JSON boolean")
            proposals: dict[str, tuple[str, ...] | None] = {}
            for item in projection.items:
                media_hint = replace_final_suffix(hints[item.input_artifact_id], ".opus")
                proposals[_output_id("opus", item.derived_from)] = media_hint
                proposals[_output_id("metadata-xmp", item.derived_from)] = append_leaf_suffix(
                    media_hint, ".xmp"
                )
            for retained in projection.retained_xmp_sidecars:
                proposals[_output_id("source-xmp", (retained.input_artifact_id,))] = hints[
                    retained.input_artifact_id
                ]
            publication_decisions = seal_publication_plan(
                proposals,
                hint_result_sha256s=hint_results,
                allow_missing=allow_missing,
            )
        except MaterializationDecisionRequired as exc:
            raise TargetServiceError(400, "materialization_decision_required", str(exc)) from exc
        except (KeyError, ValueError) as exc:
            raise TargetServiceError(400, "invalid_target_request", str(exc)) from exc
        return self._seal_preflight(
            request,
            execution_parameters={
                "media_projection": projection.model_dump(mode="json"),
                "publication_decisions": publication_decisions.model_dump(
                    mode="json", exclude_none=True
                ),
            },
        )

    def _execute(
        self,
        request: TargetJobRequest,
        attempt: int,
        cancellation: threading.Event,
        session: TargetExecutionSession,
    ) -> TargetJobStatus:
        intent = AudioArchiveIntent.model_validate(request.declaration.plan.intent)
        options = request.declaration.plan.target_options
        timeout = options.get("ffmpeg_timeout_seconds", 86400)
        if isinstance(timeout, bool) or not isinstance(timeout, int):
            raise ValueError("ffmpeg_timeout_seconds must be an integer")
        projection = MediaArchiveProjection.model_validate(
            request.declaration.plan.execution_parameters["media_projection"]
        )
        publication_decisions = MediaPublicationPlan.from_json_value(
            request.declaration.plan.execution_parameters["publication_decisions"]
        )
        try:
            projection.validate_plan_evidence(request.declaration.plan.observation_result_sha256s)
            if not set(publication_decisions.hint_result_sha256s) <= set(
                request.declaration.plan.observation_result_sha256s
            ):
                raise ValueError("accepted hint evidence is absent from the target plan")
        except ValueError as error:
            raise RuntimeError("media projection differs from the target plan evidence") from error

        def check() -> None:
            if cancellation.is_set():
                raise TargetExecutionCanceled("Opus target was canceled")

        with TargetExecutionRuntime.from_request(
            request,
            cancellation_check=check,
            producer_version=_version(),
            session=session,
        ) as execution:
            workspace = execution.open_workspace(self.workspace_root)
            try:
                resolved = execution.iter_inputs()
                resolved_by_id = {
                    artifact.id: (artifact, claimed) for artifact, claimed in resolved
                }
                outputs: list[OutputArtifact] = []
                publication = execution.open_collection_publication(
                    implementation=self.descriptor()
                )
                for item in projection.items:
                    check()
                    artifact, claimed = resolved_by_id[item.input_artifact_id]
                    source = workspace.resolve(f"input/{artifact.id}")
                    source.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                    with execution.prepare_inputs((artifact,)) as retrieval:
                        retrieval.download(claimed, source)
                    try:
                        relative = f"audio/{item.input_artifact_id}/archive.opus"
                        destination = workspace.resolve(f"output/{relative}")
                        destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                        temporary = destination.with_name(f".{destination.name}.part.opus")
                        try:
                            run_ffmpeg(
                                [
                                    self.ffmpeg,
                                    "-hide_banner",
                                    "-nostdin",
                                    "-y",
                                    "-i",
                                    str(source),
                                    "-vn",
                                    "-c:a",
                                    "libopus",
                                    "-b:a",
                                    f"{intent.bitrate_kbps}k",
                                    *ffmpeg_container_metadata_args(item),
                                    str(temporary),
                                ],
                                log_root=workspace.root,
                                timeout_seconds=timeout,
                                canceled=cancellation.is_set,
                            )
                        except OpusContentError as exc:
                            raise TargetExecutionInapplicable(
                                "opus-inapplicable", str(exc)
                            ) from exc
                        os.replace(temporary, destination)
                        size, sha256 = file_identity(destination)
                        output_id = _output_id("opus", item.derived_from)
                        output = OutputArtifact.model_validate(
                            dict(
                                id=output_id,
                                role=AUDIO_ARCHIVE_ROLE,
                                artifact_id=_member_id(
                                    request.declaration.plan.plan_sha256, output_id
                                ),
                                bytes=str(size),
                                sha256=sha256,
                            )
                        )
                        outputs.append(output)
                        output_decision = publication_decisions.decision_for(output.id)
                        publication.append(
                            ProducerFile(
                                destination,
                                output.artifact_id,
                                materialization_hint=output_decision.components,
                                allow_missing_materialization_hint=(
                                    output_decision.allow_missing_materialization_hint
                                ),
                            ),
                            output,
                            derived_from=item.derived_from,
                        )
                        xmp_relative = f"audio/{item.input_artifact_id}/archive.opus.xmp"
                        xmp = workspace.resolve(f"output/{xmp_relative}")
                        xmp.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                        xmp.write_bytes(
                            render_projection_xmp(item, tags=intent.metadata_projection.tags)
                        )
                        xmp_size, xmp_sha256 = file_identity(xmp)
                        xmp_output_id = _output_id("metadata-xmp", item.derived_from)
                        xmp_output = OutputArtifact.model_validate(
                            dict(
                                id=xmp_output_id,
                                describes_output_id=output_id,
                                role=METADATA_XMP_ROLE,
                                artifact_id=_member_id(
                                    request.declaration.plan.plan_sha256, xmp_output_id
                                ),
                                bytes=str(xmp_size),
                                sha256=xmp_sha256,
                            )
                        )
                        outputs.append(xmp_output)
                        xmp_decision = publication_decisions.decision_for(xmp_output.id)
                        publication.append(
                            ProducerFile(
                                xmp,
                                xmp_output.artifact_id,
                                materialization_hint=xmp_decision.components,
                                allow_missing_materialization_hint=(
                                    xmp_decision.allow_missing_materialization_hint
                                ),
                            ),
                            xmp_output,
                            derived_from=item.derived_from,
                        )
                    finally:
                        source.unlink(missing_ok=True)
                for retained in projection.retained_xmp_sidecars:
                    artifact, claimed = resolved_by_id[retained.input_artifact_id]
                    source = workspace.resolve(f"input/{artifact.id}")
                    source.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                    with execution.prepare_inputs((artifact,)) as retrieval:
                        retrieval.download(claimed, source)
                    try:
                        retained_relative = (
                            f"audio/~source-artifacts/{retained.input_artifact_id}.xmp"
                        )
                        destination = workspace.resolve(f"output/{retained_relative}")
                        destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                        shutil.copyfile(source, destination)
                        retained_size, retained_sha256 = file_identity(destination)
                        retained_output_id = _output_id("source-xmp", (retained.input_artifact_id,))
                        retained_output = OutputArtifact.model_validate(
                            dict(
                                id=retained_output_id,
                                role=SOURCE_ARTIFACT_ROLE,
                                artifact_id=_member_id(
                                    request.declaration.plan.plan_sha256, retained_output_id
                                ),
                                bytes=str(retained_size),
                                sha256=retained_sha256,
                            )
                        )
                        outputs.append(retained_output)
                        retained_decision = publication_decisions.decision_for(retained_output.id)
                        publication.append(
                            ProducerFile(
                                destination,
                                retained_output.artifact_id,
                                materialization_hint=retained_decision.components,
                                allow_missing_materialization_hint=(
                                    retained_decision.allow_missing_materialization_hint
                                ),
                            ),
                            retained_output,
                            derived_from=(retained.input_artifact_id,),
                        )
                    finally:
                        source.unlink(missing_ok=True)
                declared = tuple(sorted(outputs, key=lambda item: item.id))
                publication_decisions.require_exact_outputs(tuple(item.id for item in declared))
                for input_id in sorted(resolved_by_id):
                    execution.declare_disposition(input_id, "transformed")
                execution_preimage = _execution_preimage(
                    request.declaration.plan.plan_sha256,
                    declared,
                )
                execution_sha256 = hashlib.sha256(execution_preimage).hexdigest()
                return publication.finish_success(
                    operation=AUDIO_ARCHIVE_OPERATION,
                    execution_sha256=execution_sha256,
                    execution_preimage=execution_preimage,
                    attempt=attempt,
                    runtime_evidence={
                        "ffmpeg": tool_version(self.ffmpeg),
                    },
                )
            finally:
                if not execution.completed:
                    workspace.release()


def _execution_sha256(plan_sha256: str, outputs: Sequence[OutputArtifact]) -> str:
    return hashlib.sha256(_execution_preimage(plan_sha256, outputs)).hexdigest()


def _execution_preimage(
    plan_sha256: str,
    outputs: Sequence[OutputArtifact],
) -> bytes:
    """Identify exact Opus execution semantics independently of an attempt."""

    return canonical_json_bytes(
        {
            "format": "a-stove0-opus-target-execution/v1",
            "plan_sha256": plan_sha256,
            "outputs": [item.model_dump(mode="json") for item in outputs],
        }
    )


def _output_id(kind: str, derived_from: Sequence[str]) -> str:
    return (
        f"{kind}-{canonical_json_sha256({'kind': kind, 'derived_from': sorted(derived_from)})[:32]}"
    )


def _member_id(plan_sha256: str, output_id: str) -> ArtifactId:
    return ArtifactId(
        canonical_json_sha256(
            {
                "format": "stove0-output-member-id/v1",
                "plan_sha256": plan_sha256,
                "output_id": output_id,
            }
        )
    )


__all__ = ["OPTIONS", "OpusTargetService"]
