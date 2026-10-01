from __future__ import annotations

import hashlib
import json
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock

import pytest
import yaml
from fastapi.testclient import TestClient
from review0 import ReviewMaterializeTargetService
from review0 import app as review_app
from review0 import target as review_support
from review0.app import create_app
from review0.http import Review0Config, SamplerConfig
from review0.target import SamplerRegistration
from review0_contracts import (
    REVIEW_MATERIALIZE_OPERATION,
    REVIEW_SOURCE_ROLE,
    ReviewSamplePlan,
    ReviewSamplePlanPayload,
    ReviewSampleWindow,
)
from review0_sampler_client import ReviewSamplerClient
from review0_sampler_protocol import (
    SamplerDescriptor,
    SamplerDescriptorPayload,
    SamplerFailure,
    SamplerInapplicable,
    SamplerOutput,
    SamplerResult,
    SamplerResultPayload,
)
from riverhog_canonical_json import require_canonical_json
from riverhog_client.processing import ProcessingWorkspace
from riverhog_protocol import canonical_json_sha256
from stove0_protocol import (
    ArtifactSelection,
    CollectionRootIdentityRef,
    JsonSchemaValidationProfile,
    WorkArtifactSubject,
)
from stove0_target_protocol import TargetInputAuthority
from stove0_target_support import (
    InputArtifact,
    OutputArtifact,
    TargetExecutionCanceled,
    TargetExecutionFailure,
    TargetExecutionInapplicable,
    TargetJobRequest,
    TargetPreflightRequest,
    TargetServiceError,
)


def _sha(character: str) -> str:
    return character * 64


def _input_authority(*inputs: InputArtifact) -> TargetInputAuthority:
    return TargetInputAuthority.from_selection(
        ArtifactSelection.seal(
            tuple(
                WorkArtifactSubject.model_validate(item.model_dump(mode="json")) for item in inputs
            )
        )
    )


def _sample_plan() -> ReviewSamplePlan:
    return ReviewSamplePlan.seal(
        ReviewSamplePlanPayload(
            samples_per_artifact=1,
            window_duration_ms=1000,
            windows=(
                ReviewSampleWindow(
                    artifact_id="source",
                    start_ms=0,
                    duration_ms=1000,
                ),
            ),
        )
    )


class FixtureSamplerClient:
    def __init__(self, descriptor: SamplerDescriptor) -> None:
        self.value = descriptor
        self.closed = False

    def descriptor(self, *, refresh: bool = False) -> SamplerDescriptor:
        del refresh
        return self.value

    def close(self) -> None:
        self.closed = True


def _sampler() -> tuple[SamplerRegistration, FixtureSamplerClient]:
    descriptor = SamplerDescriptor.seal(
        SamplerDescriptorPayload(
            implementation_id="fixture.sampler/v1",
            implementation_version="1.0.0",
            source_revision="fixture",
            image_id="sha256:" + _sha("8"),
            primary_operation_id="stove0.media.audio-archive/v1",
            primary_operation_contract_sha256=_sha("7"),
            portable_intent_schema=JsonSchemaValidationProfile.from_schema(
                "fixture.opus-intent/v1",
                {
                    "type": "object",
                    "properties": {"bitrate_kbps": {"type": "integer"}},
                    "required": ["bitrate_kbps"],
                    "additionalProperties": False,
                },
            ),
            output_role="stove0.review.audio/v1",
        )
    )
    client = FixtureSamplerClient(descriptor)
    return (
        SamplerRegistration(
            id="opus",
            client=cast(ReviewSamplerClient, client),
            descriptor_sha256=descriptor.descriptor_sha256,
            image_id=descriptor.image_id,
        ),
        client,
    )


def test_review_preflight_seals_exact_sampler_identity_and_one_operation(
    tmp_path: Path,
) -> None:
    registration, sampler_client = _sampler()
    target = ReviewMaterializeTargetService(
        state_root=tmp_path / "state",
        workspace_root=tmp_path / "workspace",
        samplers=(registration,),
        source_revision="fixture",
        image_id="sha256:" + _sha("9"),
        implementation_version="0.1.0",
    )
    try:
        request = TargetPreflightRequest(
            invocation_sha256=_sha("b"),
            operation_id=REVIEW_MATERIALIZE_OPERATION.id,
            operation_contract_sha256=REVIEW_MATERIALIZE_OPERATION.contract_sha256,
            inputs=_input_authority(
                InputArtifact(
                    id="source",
                    role=REVIEW_SOURCE_ROLE,
                    collection=CollectionRootIdentityRef(
                        collection_id=str(1),
                        archive_root_sha256=_sha("1"),
                        artifact_set_identity=_sha("2"),
                    ),
                    artifact_id=_sha("4"),
                    bytes=str(12),
                    sha256=_sha("3"),
                )
            ),
            intent={
                "sample_plan": _sample_plan().model_dump(mode="json"),
                "variant": {
                    "id": "opus-96",
                    "portable_intent": {"bitrate_kbps": 96},
                },
            },
            target_options={"sampler_registration_id": "opus"},
        )
        preflight = target.preflight(request)

        assert target.descriptor().image_id == "sha256:" + _sha("9")
        assert [item.operation_id for item in target.descriptor().operations] == [
            REVIEW_MATERIALIZE_OPERATION.id
        ]
        assert preflight.plan.target_options == request.target_options
        assert preflight.plan.execution_parameters == {
            "sampler_descriptor_sha256": registration.descriptor_sha256,
        }
        invalid_intent = request.intent.copy()
        invalid_plan = dict(invalid_intent["sample_plan"])
        invalid_plan["sample_plan_sha256"] = _sha("4")
        invalid_intent["sample_plan"] = invalid_plan
        with pytest.raises(TargetServiceError, match="intent is invalid"):
            target.preflight(request.model_copy(update={"intent": invalid_intent}))
        with pytest.raises(TargetServiceError, match="sampler_descriptor_sha256") as exc_info:
            target.preflight(
                request.model_copy(
                    update={
                        "target_options": {
                            **request.target_options,
                            "sampler_descriptor_sha256": _sha("6"),
                        }
                    }
                )
            )
        assert exc_info.value.status == 400
    finally:
        target.close()
    assert sampler_client.closed


def test_review_process_exposes_only_target_descriptor(tmp_path: Path) -> None:
    registration, _sampler_client = _sampler()
    target = ReviewMaterializeTargetService(
        state_root=tmp_path / "state",
        workspace_root=tmp_path / "workspace",
        samplers=(registration,),
        source_revision="fixture",
        image_id="sha256:" + _sha("9"),
        implementation_version="0.1.0",
    )
    with TestClient(create_app(token="review-secret", target=target)) as client:
        response = client.get(
            "/v1/target",
            headers={"Authorization": "Bearer review-secret"},
        )
        assert response.status_code == 200
        assert response.json()["implementation_id"] == "review0/v1"
        assert (
            client.get(
                "/v1/sampler",
                headers={"Authorization": "Bearer review-secret"},
            ).status_code
            == 404
        )


def test_review_support_preserves_sampler_terminal_classification() -> None:
    registration, _client = _sampler()
    common = {
        "request_sha256": _sha("1"),
        "sampler_descriptor_sha256": registration.descriptor_sha256,
    }
    retryable = SamplerResult.seal(
        SamplerResultPayload(
            **common,
            state="failed",
            failure=SamplerFailure(
                code="sampler-infrastructure",
                message="temporary sampler failure",
                retryable=True,
            ),
        )
    )
    inapplicable = SamplerResult.seal(
        SamplerResultPayload(
            **common,
            state="inapplicable",
            inapplicable=SamplerInapplicable(
                code="unsupported-content",
                message="fixture content is unsupported",
            ),
        )
    )
    canceled = SamplerResult.seal(SamplerResultPayload(**common, state="canceled"))

    with pytest.raises(TargetExecutionFailure) as retryable_error:
        review_support._require_sampler_success(retryable)
    assert retryable_error.value.retryable is True
    with pytest.raises(TargetExecutionInapplicable, match="fixture content"):
        review_support._require_sampler_success(inapplicable)
    with pytest.raises(TargetExecutionCanceled, match="canceled"):
        review_support._require_sampler_success(canceled)


def test_review_execution_identity_is_the_canonical_semantic_result() -> None:
    output = OutputArtifact(
        id="review-index",
        role="stove0.review.index/v1",
        artifact_id=_sha("5"),
        bytes=str(12),
        sha256=_sha("4"),
    )
    expected = canonical_json_sha256(
        {
            "format": "review0-execution/v1",
            "plan_sha256": _sha("1"),
            "sampler_result_sha256": _sha("3"),
            "outputs": [output.model_dump(mode="json")],
        }
    )

    assert (
        review_support._execution_sha256(
            _sha("1"),
            _sha("3"),
            (output,),
        )
        == expected
    )


def test_review_output_member_identity_is_bound_to_plan_and_output_key() -> None:
    first = review_support._member_id(_sha("1"), "sample-0001")
    assert first == review_support._member_id(_sha("1"), "sample-0001")
    assert first != review_support._member_id(_sha("1"), "sample-0002")
    assert first != review_support._member_id(_sha("2"), "sample-0001")


@pytest.mark.parametrize("resumed", [False, True])
def test_review_execution_publishes_canonical_index_with_opaque_member_ids(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, resumed: bool
) -> None:
    registration, _client = _sampler()
    target = ReviewMaterializeTargetService(
        state_root=tmp_path / "state",
        workspace_root=tmp_path / "workspace",
        samplers=(registration,),
        source_revision="fixture",
        image_id="sha256:" + _sha("9"),
        implementation_version="0.1.0",
    )
    source = InputArtifact(
        id="source",
        role=REVIEW_SOURCE_ROLE,
        collection=CollectionRootIdentityRef(
            collection_id="1", archive_root_sha256=_sha("1"), artifact_set_identity=_sha("2")
        ),
        artifact_id=_sha("4"),
        bytes="6",
        sha256=hashlib.sha256(b"source").hexdigest(),
    )
    preflight = target.preflight(
        TargetPreflightRequest(
            invocation_sha256=_sha("b"),
            operation_id=REVIEW_MATERIALIZE_OPERATION.id,
            operation_contract_sha256=REVIEW_MATERIALIZE_OPERATION.contract_sha256,
            inputs=_input_authority(source),
            intent={
                "sample_plan": _sample_plan().model_dump(mode="json"),
                "variant": {"id": "opus-96", "portable_intent": {"bitrate_kbps": 96}},
            },
            target_options={"sampler_registration_id": "opus"},
        )
    )
    request = cast(
        TargetJobRequest,
        SimpleNamespace(declaration=SimpleNamespace(plan=preflight.plan), request_sha256=_sha("c")),
    )
    workspace = ProcessingWorkspace.open(
        target.workspace_root, execution_id=_sha("d"), declared_protection="memory-backed"
    )
    execution = MagicMock()
    execution.__enter__.return_value = execution
    execution.open_workspace.return_value = workspace
    execution.iter_inputs.side_effect = lambda: iter(((source, source),))
    execution.completed = False
    retrieval = execution.prepare_inputs.return_value.__enter__.return_value
    retrieval.download.side_effect = lambda _claimed, destination: destination.write_bytes(
        b"source"
    )
    publication = execution.open_collection_publication.return_value
    member_id = review_support._member_id(preflight.plan.plan_sha256, "sample-0001")
    output = OutputArtifact(
        id="sample-0001",
        role=registration.descriptor().output_role,
        artifact_id=member_id,
        bytes="4",
        sha256=hashlib.sha256(b"opus").hexdigest(),
    )
    publication.resume_output.side_effect = (output if resumed else None, None)
    monkeypatch.setattr(
        review_support.TargetExecutionRuntime, "from_request", lambda *_a, **_k: execution
    )

    def sample(_registration, sampler_request, *, workspace, **_kwargs):
        window = sampler_request.windows[0]
        path = workspace.resolve(window.output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"opus")
        return SamplerResult.seal(
            SamplerResultPayload(
                request_sha256=sampler_request.request_sha256,
                sampler_descriptor_sha256=registration.descriptor_sha256,
                state="succeeded",
                outputs=(
                    SamplerOutput(
                        id=window.id,
                        path=window.output_path,
                        bytes=4,
                        sha256=output.sha256,
                        media_type="audio/ogg",
                        derived_from=(source.id,),
                    ),
                ),
            )
        )

    monkeypatch.setattr(target, "_sample", sample)
    session = MagicMock()
    session.load_step.return_value = None
    try:
        target._execute(request, 1, threading.Event(), session)
        encoded = workspace.resolve("output/review/summary.json").read_bytes()
        require_canonical_json(encoded)
        summary = json.loads(encoded)
        assert summary["samples"] == [
            {
                "artifact_id": "sample-0001",
                "source_artifact_id": "source",
                "output_artifact_id": str(member_id),
                "start_ms": 0,
                "duration_ms": 1000,
            }
        ]
        assert publication.append.call_args.args[1].id == "review-index"
        publication.finish_success.assert_called_once()
        assert publication.append.call_count == (1 if resumed else 2)
    finally:
        target.close()


def test_review_process_yaml_and_wiring_are_connected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    token_file = tmp_path / "review.token"
    token_file.write_text("file-secret\n", encoding="utf-8")
    registrations = (_sampler()[0],)
    sampler_token_file = tmp_path / "sampler.token"
    sampler_token_file.write_text("sampler-secret\n", encoding="utf-8")
    config_path = tmp_path / "materializer.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "token_file": str(token_file),
                "samplers": [
                    {
                        "id": "opus",
                        "base_url": "https://sampler.invalid",
                        "token_file": str(sampler_token_file),
                        "descriptor_sha256": _sha("1"),
                        "image_id": "sha256:" + _sha("2"),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    assert review_app.REVIEW0_CONFIG_SCHEMA == Review0Config.model_json_schema()
    assert review_app.load_config(config_path).samplers[0].id == "opus"
    monkeypatch.setenv("REVIEW0_CONFIG", str(config_path))
    monkeypatch.setattr(review_app, "sampler_registrations", lambda _config: registrations)

    monkeypatch.setenv("REVIEW0_HOST", "127.0.0.8")
    monkeypatch.setenv("REVIEW0_PORT", "8188")
    monkeypatch.setenv("REVIEW0_STATE_ROOT", str(tmp_path / "state"))
    monkeypatch.setenv("REVIEW0_WORKSPACE", str(tmp_path / "workspace"))
    monkeypatch.setenv("REVIEW0_SOURCE_REVISION", "fixture-revision")
    monkeypatch.setenv("REVIEW0_IMAGE_ID", "sha256:" + _sha("9"))
    configured: dict[str, object] = {}

    class ConfiguredTarget:
        def __init__(self, **kwargs: object) -> None:
            configured.update(kwargs)

        def close(self) -> None:
            pass

        def readiness(self) -> None:
            pass

    def run(_app: object, *, host: str, port: int) -> None:
        configured["host"] = host
        configured["port"] = port

    monkeypatch.setattr(review_app, "ReviewMaterializeTargetService", ConfiguredTarget)
    monkeypatch.setattr(review_app.uvicorn, "run", run)

    assert review_app.main([]) == 0
    assert configured == {
        "state_root": tmp_path / "state",
        "workspace_root": tmp_path / "workspace",
        "samplers": registrations,
        "source_revision": "fixture-revision",
        "image_id": "sha256:" + _sha("9"),
        "implementation_version": "0.1.0",
        "terminal_state_retention_seconds": 2_592_000,
        "host": "127.0.0.8",
        "port": 8188,
    }


def test_review_support_registration_count_is_defined_by_deployment(tmp_path: Path) -> None:
    samplers = tuple(
        SamplerConfig(
            id=f"sampler-{index:03}",
            base_url=f"https://sampler-{index:03}.invalid",
            token_file=tmp_path / f"sampler-{index:03}.token",
            descriptor_sha256=_sha("1"),
            image_id="sha256:" + _sha("2"),
        )
        for index in range(33)
    )

    assert (
        Review0Config(token_file=tmp_path / "target.token", samplers=samplers).samplers == samplers
    )
