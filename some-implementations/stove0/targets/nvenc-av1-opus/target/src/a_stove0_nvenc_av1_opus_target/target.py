"""One-operation NVENC AV1 + Opus collection transform target."""

from __future__ import annotations

import importlib.metadata
import os
import shutil
import threading
from collections.abc import Sequence
from pathlib import Path

from a_stove0_media_archive_contract_lib import (
    AV1_OPUS_ARCHIVE_OPERATION,
    AV1_OPUS_ARCHIVE_ROLE,
    METADATA_XMP_ROLE,
    SOURCE_ARTIFACT_ROLE,
    Av1OpusArchiveIntent,
    validate_av1_opus_archive_intent,
)
from a_stove0_media_archive_lib import (
    MaterializationDecisionRequired,
    MediaArchiveProjection,
    MediaProjectionItem,
    MediaPublicationPlan,
    accepted_source_hints,
    append_leaf_suffix,
    ffmpeg_container_metadata_args,
    render_projection_xmp,
    replace_final_suffix,
    resolve_media_archive_preflight_projection,
    seal_publication_plan,
    sibling_hint,
)
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

from a_stove0_nvenc_av1_opus_target.common import (
    NvencContentError,
    file_identity,
    run_ffmpeg,
    tool_version,
)
from a_stove0_nvenc_av1_opus_target.media_source_artifacts import build_strict_source_artifacts

_PROJECTION_SCHEMA = MediaArchiveProjection.model_json_schema()
_PUBLICATION_SCHEMA = MediaPublicationPlan.model_json_schema()
OPTIONS = JsonSchemaValidationProfile.from_schema(
    "a-stove0-nvenc-av1-opus-target-options/v1",
    {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "ffmpeg_timeout_seconds": {"type": "integer", "minimum": 1, "maximum": 86400},
            "preset": {"enum": ["p1", "p2", "p3", "p4", "p5", "p6", "p7"]},
            "media_projection": {
                key: value for key, value in _PROJECTION_SCHEMA.items() if key != "$defs"
            },
            "publication_decisions": {
                key: value for key, value in _PUBLICATION_SCHEMA.items() if key != "$defs"
            },
            "allow_missing_materialization_hint": {"type": "boolean"},
        },
        "$defs": {
            **_PROJECTION_SCHEMA.get("$defs", {}),
            **_PUBLICATION_SCHEMA.get("$defs", {}),
        },
        "additionalProperties": False,
    },
)


def _version() -> str:
    try:
        return importlib.metadata.version("a-stove0-nvenc-av1-opus-target")
    except importlib.metadata.PackageNotFoundError:
        return "development"


class NvencAv1OpusTargetService(PersistentTargetService):
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
                implementation_id="a-stove0-nvenc-av1-opus-target/v1",
                implementation_version=_version(),
                source_revision=source_revision,
                image_id=image_id,
                operations=(
                    TargetOperationSupport(
                        operation_id=AV1_OPUS_ARCHIVE_OPERATION.id,
                        operation_contract_sha256=AV1_OPUS_ARCHIVE_OPERATION.contract_sha256,
                        options_schema=OPTIONS,
                    ),
                ),
            )
        )
        self.target_descriptor = descriptor
        super().__init__(
            descriptor=descriptor,
            operations={AV1_OPUS_ARCHIVE_OPERATION.id: AV1_OPUS_ARCHIVE_OPERATION},
            state_root=state_root,
            execute=self._execute,
            intent_semantic_validators={
                AV1_OPUS_ARCHIVE_OPERATION.intent_semantics.profile_sha256: (
                    validate_av1_opus_archive_intent
                )
            },
            terminal_state_retention_seconds=terminal_state_retention_seconds,
        )

    def preflight(self, request: TargetPreflightRequest) -> TargetPreflightResponse:
        try:
            intent = Av1OpusArchiveIntent.model_validate(request.intent)
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
                media_hint = replace_final_suffix(hints[item.input_artifact_id], ".mkv")
                proposals[_output_id("video", item.derived_from)] = media_hint
                proposals[_output_id("metadata-xmp", item.derived_from)] = (
                    append_leaf_suffix(media_hint, ".xmp")
                )
                proposals[_output_id("source-artifacts", (item.input_artifact_id,))] = (
                    sibling_hint(media_hint, "source-artifacts.tar.zst")
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
            supplied = request.target_options.get("media_projection")
            if (
                supplied is not None
                and MediaArchiveProjection.model_validate(supplied) != projection
            ):
                raise ValueError("supplied media projection differs from target preflight")
            supplied_decisions = request.target_options.get("publication_decisions")
            if (
                supplied_decisions is not None
                and MediaPublicationPlan.from_json_value(supplied_decisions)
                != publication_decisions
            ):
                raise ValueError("supplied publication decisions differ from accepted hints")
        except MaterializationDecisionRequired as exc:
            raise TargetServiceError(400, "materialization_decision_required", str(exc)) from exc
        except (KeyError, ValueError) as exc:
            raise TargetServiceError(400, "invalid_target_request", str(exc)) from exc
        effective = request.model_copy(
            update={
                "target_options": {
                    **request.target_options,
                    "media_projection": projection.model_dump(mode="json"),
                    "publication_decisions": publication_decisions.model_dump(
                        mode="json", exclude_none=True
                    ),
                }
            }
        )
        return super().preflight(effective)

    def _execute(
        self,
        request: TargetJobRequest,
        attempt: int,
        cancellation: threading.Event,
        session: TargetExecutionSession,
    ) -> TargetJobStatus:
        intent = Av1OpusArchiveIntent.model_validate(request.declaration.plan.intent)
        options = request.declaration.plan.target_options
        timeout = options.get("ffmpeg_timeout_seconds", 86400)
        if isinstance(timeout, bool) or not isinstance(timeout, int):
            raise ValueError("ffmpeg_timeout_seconds must be an integer")
        preset = str(options.get("preset", "p7"))
        projection = MediaArchiveProjection.model_validate(options["media_projection"])
        publication_decisions = MediaPublicationPlan.from_json_value(
            options["publication_decisions"]
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
                raise TargetExecutionCanceled("NVENC AV1 + Opus target was canceled")

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
                publication = execution.open_collection_publication()
                for item in projection.items:
                    check()
                    artifact, claimed = resolved_by_id[item.input_artifact_id]
                    source = workspace.resolve(f"input/{artifact.id}")
                    source.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                    with execution.prepare_inputs((artifact,)) as retrieval:
                        retrieval.download(claimed, source)
                    try:
                        relative = f"video/{item.input_artifact_id}/archive.mkv"
                        destination = workspace.resolve(f"output/{relative}")
                        destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                        command = self._command(source, destination, intent, preset, item)
                        effective = command
                        try:
                            run_ffmpeg(
                                command,
                                log_root=workspace.root,
                                timeout_seconds=timeout,
                                canceled=cancellation.is_set,
                            )
                        except NvencContentError as first:
                            if intent.salvage != "safe-remux":
                                raise TargetExecutionInapplicable(
                                    "nvenc-av1-opus-inapplicable", str(first)
                                ) from first
                            remuxed = workspace.resolve(f"salvage/{item.input_artifact_id}.mkv")
                            remuxed.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                            try:
                                run_ffmpeg(
                                    [
                                        self.ffmpeg,
                                        "-hide_banner",
                                        "-nostdin",
                                        "-y",
                                        "-err_detect",
                                        "ignore_err",
                                        "-i",
                                        str(source),
                                        "-map",
                                        "0",
                                        "-c",
                                        "copy",
                                        str(remuxed),
                                    ],
                                    log_root=workspace.root,
                                    timeout_seconds=timeout,
                                    canceled=cancellation.is_set,
                                )
                                effective = self._command(
                                    remuxed,
                                    destination,
                                    intent,
                                    preset,
                                    item,
                                )
                                run_ffmpeg(
                                    effective,
                                    log_root=workspace.root,
                                    timeout_seconds=timeout,
                                    canceled=cancellation.is_set,
                                )
                            except NvencContentError as exc:
                                raise TargetExecutionInapplicable(
                                    "nvenc-av1-opus-salvage-inapplicable", str(exc)
                                ) from exc
                        video = self._output(
                            destination,
                            output_id=_output_id("video", item.derived_from),
                            plan_sha256=request.declaration.plan.plan_sha256,
                            role=AV1_OPUS_ARCHIVE_ROLE,
                        )
                        outputs.append(video)
                        xmp_relative = f"video/{item.input_artifact_id}/archive.mkv.xmp"
                        xmp = workspace.resolve(f"output/{xmp_relative}")
                        xmp.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                        xmp.write_bytes(
                            render_projection_xmp(item, tags=intent.metadata_projection.tags)
                        )
                        xmp_output = self._output(
                            xmp,
                            output_id=_output_id("metadata-xmp", item.derived_from),
                            plan_sha256=request.declaration.plan.plan_sha256,
                            role=METADATA_XMP_ROLE,
                        )
                        outputs.append(xmp_output)
                        bundle_relative = (
                            f"video/{item.input_artifact_id}/source-artifacts.tar.zst"
                        )
                        bundle = workspace.resolve(f"output/{bundle_relative}")
                        build_strict_source_artifacts(
                            source=source,
                            archive=destination,
                            bundle=bundle,
                            encode_command=effective,
                            intent=intent,
                            target_options=options,
                            target_descriptor_sha256=self.target_descriptor.descriptor_sha256,
                            plan_sha256=request.declaration.plan.plan_sha256,
                        )
                        source_artifact = self._output(
                            bundle,
                            output_id=_output_id(
                                "source-artifacts",
                                (item.input_artifact_id,),
                            ),
                            plan_sha256=request.declaration.plan.plan_sha256,
                            role=SOURCE_ARTIFACT_ROLE,
                        )
                        outputs.append(source_artifact)
                        video_decision = publication_decisions.decision_for(video.id)
                        publication.append(
                            ProducerFile(
                                destination,
                                video.artifact_id,
                                materialization_hint=video_decision.components,
                                allow_missing_materialization_hint=(
                                    video_decision.allow_missing_materialization_hint
                                ),
                            ),
                            video,
                            derived_from=item.derived_from,
                        )
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
                        bundle_decision = publication_decisions.decision_for(source_artifact.id)
                        publication.append(
                            ProducerFile(
                                bundle,
                                source_artifact.artifact_id,
                                materialization_hint=bundle_decision.components,
                                allow_missing_materialization_hint=(
                                    bundle_decision.allow_missing_materialization_hint
                                ),
                            ),
                            source_artifact,
                            derived_from=(item.input_artifact_id,),
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
                            f"video/~source-artifacts/{retained.input_artifact_id}.xmp"
                        )
                        destination = workspace.resolve(f"output/{retained_relative}")
                        destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                        shutil.copyfile(source, destination)
                        retained_output = self._output(
                            destination,
                            output_id=_output_id(
                                "source-xmp",
                                (retained.input_artifact_id,),
                            ),
                            plan_sha256=request.declaration.plan.plan_sha256,
                            role=SOURCE_ARTIFACT_ROLE,
                        )
                        outputs.append(retained_output)
                        retained_decision = publication_decisions.decision_for(
                            retained_output.id
                        )
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
                execution_sha256 = _execution_sha256(
                    request.declaration.plan.plan_sha256,
                    declared,
                )
                return publication.finish_success(
                    operation=AV1_OPUS_ARCHIVE_OPERATION,
                    execution_sha256=execution_sha256,
                    attempt=attempt,
                    runtime_evidence={
                        "ffmpeg": tool_version(self.ffmpeg),
                    },
                )
            finally:
                if not execution.completed:
                    workspace.release()

    def _command(
        self,
        source: Path,
        destination: Path,
        intent: Av1OpusArchiveIntent,
        preset: str,
        projection: MediaProjectionItem,
    ) -> list[str]:
        filters = (
            [] if intent.max_height is None else ["-vf", f"scale=-2:min(ih\\,{intent.max_height})"]
        )
        return [
            self.ffmpeg,
            "-hide_banner",
            "-nostdin",
            "-y",
            "-i",
            str(source),
            *filters,
            "-c:v",
            "av1_nvenc",
            "-preset",
            preset,
            "-cq",
            str(intent.quality),
            "-c:a",
            "libopus",
            "-b:a",
            f"{intent.audio_bitrate_kbps}k",
            *ffmpeg_container_metadata_args(projection),
            str(destination),
        ]

    @staticmethod
    def _output(
        source: Path,
        *,
        output_id: str,
        plan_sha256: str,
        role: str,
    ) -> OutputArtifact:
        size, sha256 = file_identity(source)
        return OutputArtifact.model_validate(
            dict(
                id=output_id,
                role=role,
                artifact_id=_member_id(plan_sha256, output_id),
                bytes=str(size),
                sha256=sha256,
            )
        )


def _execution_sha256(
    plan_sha256: str,
    outputs: Sequence[OutputArtifact],
) -> str:
    """Identify exact AV1/Opus execution semantics independently of an attempt."""

    return canonical_json_sha256(
        {
            "format": "a-stove0-nvenc-av1-opus-target-execution/v1",
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


__all__ = ["OPTIONS", "NvencAv1OpusTargetService"]
