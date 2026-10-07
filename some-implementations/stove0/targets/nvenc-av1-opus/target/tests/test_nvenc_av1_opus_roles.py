from __future__ import annotations

import hashlib
import json
import tarfile
import threading
import time
from pathlib import Path
from typing import Any

import pytest
from a_review0_nvenc_av1_opus_sampler import NvencAv1OpusReviewSampler
from a_review0_nvenc_av1_opus_sampler.app import create_app as create_sampler_app
from a_stove0_media_archive_contract_lib import (
    AV1_OPUS_ARCHIVE_OPERATION,
    METADATA_XMP_ROLE,
    SOURCE_ARTIFACT_ROLE,
    Av1OpusArchiveIntent,
)
from a_stove0_media_archive_lib import (
    MediaArchiveProjection,
    MediaPublicationPlan,
)
from a_stove0_nvenc_av1_opus_target import NvencAv1OpusTargetService, source_artifacts
from a_stove0_nvenc_av1_opus_target import target as nvenc_target
from a_stove0_nvenc_av1_opus_target.app import create_target_app
from a_stove0_nvenc_av1_opus_target.common import NvencContentError, run_ffmpeg
from review0_sampler_lib import SamplerHttpBinding
from review0_sampler_protocol import (
    SamplerInput,
    SamplerRequest,
    SamplerRequestPayload,
    SamplerWindow,
)
from riverhog_protocol import canonical_json_sha256
from stove0_target_support import (
    OutputArtifact,
    TargetExecutionInapplicable,
    TargetHttpBinding,
    validate_preflight_response_against_request,
)

from tests.fixtures.stove0_media import media_preflight_request, sealed_media_job


def _sha(character: str) -> str:
    return character * 64


def test_actual_nvenc_payload_waits_for_a_component_owned_permit_and_releases_after_stop(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    from a_stove0_nvenc_av1_opus_target import app as target_app
    from fastapi.testclient import TestClient

    probed, entered, released = (threading.Event() for _ in range(3))
    allowed, activations, executions, withdrawn = set(), [], [], []
    owners = []
    active = []

    class Permit:
        def __init__(self, owner):
            self.owner = owner

        def activate(self, cancellation, *, deadline):
            assert time.monotonic() < deadline and not cancellation.is_set()
            active.append(self.owner)
            activations.append(self.owner)

        def release(self, *, deadline):
            assert executions[-1] == self.owner.invocation_id
            active.clear()
            released.set()

    class Admission:
        def probe(self, owner, *, deadline):
            assert time.monotonic() < deadline
            owners.append(owner)
            probed.set()
            return Permit(owner) if owner.invocation_id in allowed else None

        def withdraw(self, owner, *, deadline):
            withdrawn.append(owner)

    # The real component executor reaches its first native-tool boundary only
    # after activation. This fixture stops there; GPU encoding is provider proof.
    def tool_version(_command):
        assert len(active) == 1
        executions.append(active[0].invocation_id)
        entered.set()
        raise TargetExecutionInapplicable(
            "fixture-stop", "The guarded native boundary was reached."
        )

    monkeypatch.setattr(nvenc_target, "tool_version", tool_version)
    monkeypatch.setattr(
        NvencAv1OpusTargetService,
        "_validate_live_authority",
        lambda _self, _request, *, deadline: None,
    )
    monkeypatch.setattr(
        target_app.subprocess,
        "run",
        lambda *_args, **_kwargs: type(
            "EncoderList", (), {"returncode": 0, "stdout": b"av1_nvenc"}
        )(),
    )

    def service_at_state():
        return NvencAv1OpusTargetService(
            state_root=tmp_path / "state",
            workspace_root=tmp_path / "workspace",
            image_id="sha256:" + _sha("9"),
            execution_admission=Admission(),
        )

    service = service_at_state()
    intent = {
        "codec": "av1",
        "container": "mkv",
        "quality": 23,
        "audio_bitrate_kbps": 128,
        "salvage": "safe-remux",
    }
    requests = tuple(
        sealed_media_job(service, AV1_OPUS_ARCHIVE_OPERATION, {**intent, "quality": quality})
        for quality in (23, 25, 27)
    )
    headers = {"Authorization": "Bearer fixture-target"}

    def put(client, request):
        before = time.monotonic()
        response = client.put(
            f"/v1/jobs/{request.declaration.job_id}",
            json=request.model_dump(mode="json"),
            headers=headers,
        )
        assert response.status_code == 200, response.text
        assert time.monotonic() - before < 1
        return response.json()

    with TestClient(create_target_app(token="fixture-target", target=service)) as client:
        for request in requests:
            assert put(client, request)["state"] == "queued"
        assert probed.wait(5)
        assert not service._sessions and service._dispatch.payload_count == 0
        assert not entered.is_set() and not tuple(service.workspace_root.iterdir())
        assert client.get("/v1/target", headers=headers).status_code == 200
        assert client.get("/health/live").status_code == 200
        assert client.get("/health/ready").status_code == 200
        preflight = media_preflight_request(AV1_OPUS_ARCHIVE_OPERATION, intent)
        assert (
            client.post(
                "/v1/preflight", json=preflight.model_dump(mode="json"), headers=headers
            ).status_code
            == 200
        )
        with pytest.raises(SystemExit) as version:
            target_app._parser().parse_args(["--version"])
        assert version.value.code == 0 and not activations
        allowed.add(requests[1].request_sha256)
        assert entered.wait(5) and released.wait(5)
        assert (
            client.get(f"/v1/jobs/{requests[0].declaration.job_id}", headers=headers).json()[
                "state"
            ]
            == "queued"
        )
    assert owners and all(owner in withdrawn for owner in set(owners))
    old_owners = set(owners)
    service = service_at_state()
    entered.clear()
    released.clear()
    with TestClient(create_target_app(token="fixture-target", target=service)) as client:
        for index in (0, 2):
            assert service.get_job(requests[index].declaration.job_id).state == "queued"
        assert not service._dispatch.payload_count and not service._sessions
        for index in (0, 2):
            assert put(client, requests[index])["attempt"] == 1
        canceled = requests[2]
        response = client.post(
            f"/v1/jobs/{canceled.declaration.job_id}/cancel",
            json=canceled.accepted().model_dump(mode="json"),
            headers=headers,
        )
        assert response.status_code == 200 and response.json()["state"] == "canceled"
        allowed.update((requests[0].request_sha256, canceled.request_sha256))
        assert put(client, canceled)["state"] == "canceled"
        assert entered.wait(5) and released.wait(5)
        assert service.get_job(requests[0].declaration.job_id).state == "inapplicable"
        assert not active
    assert executions == [requests[1].request_sha256, requests[0].request_sha256]
    assert all(owner not in old_owners for owner in activations[1:])


def test_source_artifact_zstd_command_is_configurable(monkeypatch: Any) -> None:
    monkeypatch.setenv("A_STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD", "fixture-zstd")
    monkeypatch.setattr(source_artifacts.shutil, "which", lambda command: f"/tools/{command}")

    assert source_artifacts._zstd_command() == "/tools/fixture-zstd"


def test_nvenc_execution_identity_is_the_canonical_semantic_result() -> None:
    output = OutputArtifact(
        id="archive-source",
        role="stove0.media.video-archive/v1",
        artifact_id=_sha("2"),
        bytes=str(12),
        sha256=_sha("3"),
    )
    expected = canonical_json_sha256(
        {
            "format": "a-stove0-nvenc-av1-opus-target-execution/v1",
            "plan_sha256": _sha("1"),
            "outputs": [output.model_dump(mode="json")],
        }
    )

    assert nvenc_target._execution_sha256(_sha("1"), (output,)) == expected


def test_paired_nvenc_roles_bind_av1_opus_semantics_and_isolated_contracts(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    image_id = "sha256:" + _sha("9")
    workspace_root = tmp_path / "review-workspace"
    workspace_root.mkdir(mode=0o700)
    sampler = NvencAv1OpusReviewSampler(
        workspace_root=workspace_root,
        source_revision="fixture",
        image_id=image_id,
    )
    target = NvencAv1OpusTargetService(
        state_root=tmp_path / "target-state",
        workspace_root=tmp_path / "target-workspace",
        source_revision="fixture",
        image_id=image_id,
    )
    request = _request(workspace_root, sampler)

    def encode(command: list[str], **_kwargs: object) -> None:
        assert "av1_nvenc" in command
        assert "libopus" in command
        Path(command[-1]).write_bytes(b"representative-av1-opus")

    monkeypatch.setattr("a_review0_nvenc_av1_opus_sampler.sampler.run_ffmpeg", encode)
    target_response = TargetHttpBinding(target).handle("GET", "/v1/target", b"")
    sampler_binding = SamplerHttpBinding(sampler)
    sampler_response = sampler_binding.handle("GET", "/v1/sampler", b"")
    sampled = sampler_binding.handle("POST", "/v1/sample", request.model_dump_json().encode())

    target_payload = json.loads(target_response.body)
    sampler_payload = json.loads(sampler_response.body)
    assert target_response.status == 200
    assert target_payload["image_id"] == image_id
    assert [item["operation_id"] for item in target_payload["operations"]] == [
        AV1_OPUS_ARCHIVE_OPERATION.id
    ]
    assert sampler_response.status == 200
    assert sampler_payload["primary_operation_id"] == AV1_OPUS_ARCHIVE_OPERATION.id
    assert sampled.status == 200
    assert json.loads(sampled.body)["state"] == "succeeded"
    assert {
        route.path for route in create_target_app(token="target-secret", target=target).routes
    } >= {
        "/v1/target",
        "/v1/preflight",
        "/v1/jobs/{job_id}",
        "/v1/jobs/{job_id}/cancel",
    }
    assert {
        route.path for route in create_sampler_app(token="sampler-secret", sampler=sampler).routes
    } >= {
        "/v1/sampler",
        "/v1/sample",
    }
    target.close()


def test_nvenc_preflight_and_encode_share_one_exact_projection(tmp_path: Path) -> None:
    target = NvencAv1OpusTargetService(
        state_root=tmp_path / "state",
        workspace_root=tmp_path / "workspace",
        source_revision="fixture",
        image_id="sha256:" + _sha("9"),
    )
    intent_document = {
        "codec": "av1",
        "container": "mkv",
        "quality": 23,
        "audio_bitrate_kbps": 128,
        "salvage": "safe-remux",
        "metadata_projection": {
            "device_make": "Example Camera Corp",
            "device_model": "Example Camera One",
            "gps": {"latitude": 45.5, "longitude": -122.6},
            "creators": ["Alex Example", "River Example"],
            "tags": ["archive/example"],
            "field_preferences": [
                {
                    "name": "capture-time",
                    "fields": ["XMP-xmp:CreateDate", "EXIF:DateTimeOriginal"],
                }
            ],
        },
    }
    request = media_preflight_request(
        AV1_OPUS_ARCHIVE_OPERATION,
        intent_document,
        target_options={"preset": "p4"},
    )

    response = target.preflight(request)
    validate_preflight_response_against_request(response, request)
    projection = MediaArchiveProjection.model_validate(
        response.plan.execution_parameters["media_projection"]
    )
    item = projection.items[0]
    command = target._command(
        Path("/input/clip.mov"),
        Path("/output/clip.mkv"),
        Av1OpusArchiveIntent.model_validate(intent_document),
        "p4",
        item,
    )

    assert item.derived_from == ("primary", "sidecar")
    assert "creation_time=2025-02-03T04:05:06-08:00" in command
    assert "ARTIST=Alex Example; River Example" in command
    assert command[-1] == "/output/clip.mkv"
    assert response.plan.observation_result_sha256s == tuple(
        sorted(item.result.result_sha256 for item in request.observations)
    )
    publication = MediaPublicationPlan.from_json_value(
        response.plan.execution_parameters["publication_decisions"]
    )
    assert {item.components for item in publication.decisions} == {
        ("Camera", "clip.mkv"),
        ("Camera", "clip.mkv.xmp"),
        ("Camera", "source-artifacts.tar.zst"),
        ("Camera", "clip.xmp"),
    }
    assert {output.role for output in AV1_OPUS_ARCHIVE_OPERATION.outputs} == {
        "stove0.media.av1-opus-archive/v1",
        METADATA_XMP_ROLE,
        SOURCE_ARTIFACT_ROLE,
    }
    assert next(
        item
        for item in AV1_OPUS_ARCHIVE_OPERATION.inputs
        if item.role == "stove0.media.xmp-source/v1"
    ).allowed_dispositions == ("transformed",)
    target.close()


def _request(
    workspace_root: Path,
    sampler: NvencAv1OpusReviewSampler,
) -> SamplerRequest:
    payload = b"fixture-video"
    workspace_id = _sha("7")
    source = workspace_root / workspace_id / "input/source.mkv"
    source.parent.mkdir(mode=0o700, parents=True)
    source.write_bytes(payload)
    source.chmod(0o400)
    (source.parents[1] / "output").mkdir()
    (source.parents[1] / "control").mkdir()
    return SamplerRequest.seal(
        SamplerRequestPayload(
            sampler_descriptor_sha256=sampler.descriptor().descriptor_sha256,
            workspace_id=workspace_id,
            inputs=(
                SamplerInput(
                    id="source",
                    path="input/source.mkv",
                    bytes=len(payload),
                    sha256=hashlib.sha256(payload).hexdigest(),
                    media_type="video/x-matroska",
                ),
            ),
            windows=(
                SamplerWindow(
                    id="sample-0001",
                    input_id="source",
                    start_ms=0,
                    duration_ms=1000,
                    output_path="output/review/sample.mkv",
                ),
            ),
            portable_intent={
                "codec": "av1",
                "container": "mkv",
                "quality": 23,
                "audio_bitrate_kbps": 96,
                "salvage": "safe-remux",
            },
            maximum_output_bytes=1024,
            timeout_seconds=30,
            cancellation_path="control/cancel",
        )
    )


def test_source_artifact_bundle_is_canonical_and_audited(tmp_path: Path) -> None:
    work = tmp_path / "work"
    archive = tmp_path / "clip.mkv"
    archive.write_bytes(b"archive")
    artifacts = source_artifacts._assemble_source_artifact_bundle_inputs(
        work_dir=work,
        src="clip.mp4",
        output=archive.name,
        source_metadata={"format": {"format_name": "mov"}, "streams": []},
        source_container={"supported": True, "mode": "iso_bmff_rebuild"},
        container_inventory=[],
        container_artifacts=[],
        exports=[],
        stream_transforms=[],
        dropped_items=[],
        encode_cmd=["ffmpeg", "-i", "{source}", "{archive}"],
        selected_output_path=archive,
        encode_output_path=archive,
    )
    first = tmp_path / "first.source-artifacts.tar"
    second = tmp_path / "second.source-artifacts.tar"

    assert source_artifacts._build_source_artifacts_bundle(
        first,
        artifacts,
        src="clip.mp4",
        output=archive.name,
    )
    assert source_artifacts._build_source_artifacts_bundle(
        second,
        artifacts,
        src="clip.mp4",
        output=archive.name,
    )
    assert first.read_bytes() == second.read_bytes()
    with tarfile.open(first, "r") as bundle:
        member = bundle.extractfile("manifest.json")
        assert member is not None
        manifest = json.loads(member.read())
        assert manifest["kind"] == "stove0.media.source-artifacts/v1"
        assert all(item.mtime == 0 for item in bundle.getmembers())
    audit = source_artifacts._audit_source_artifacts_bundle(first)
    assert audit["ok"] is True
    assert audit["rebuild_supported"] is True
    assert audit["artifacts_checked"] == len(artifacts)


def test_failed_nvenc_command_removes_bounded_diagnostic_log(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    class FailedProcess:
        returncode = 7

        def poll(self) -> int:
            return self.returncode

    def popen(_command: list[str], **kwargs: Any) -> FailedProcess:
        kwargs["stderr"].write(b"fixture tool failure")
        kwargs["stderr"].flush()
        return FailedProcess()

    monkeypatch.setattr("a_stove0_nvenc_av1_opus_target.common.subprocess.Popen", popen)
    with pytest.raises(NvencContentError, match="fixture tool failure"):
        run_ffmpeg(
            ["fixture-tool"],
            log_root=tmp_path,
            timeout_seconds=1,
            canceled=lambda: False,
        )
    assert tuple(tmp_path.glob(".ffmpeg-*.log")) == ()
