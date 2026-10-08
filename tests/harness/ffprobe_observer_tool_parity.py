"""Actual FFprobe qualification inside its supplied runtime image."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import cast

from a_stove0_ffprobe_observer import FfprobeObserver
from a_stove0_ffprobe_streams_contract_lib import (
    FFPROBE_STREAMS_INTERFACE,
    FFPROBE_STREAMS_OBSERVER_CONTRACT,
    FFprobeStreamFacts,
)
from a_stove0_media_sampling_contract_lib import (
    MEDIA_SAMPLING_INTERFACE,
    MEDIA_SAMPLING_OBSERVER_CONTRACT,
)
from riverhog_canonical_json import canonical_json_bytes
from stove0_observer_protocol import ContentObservationRequest, ContentObservationRequestPayload
from stove0_observer_support import ContentObservationRuntime
from stove0_protocol import ArtifactSelection, CollectionRootIdentityRef, WorkArtifactSubject
from stove0_protocol.observation_evidence import ObservationQuestion, ObservationQuestionPayload


class Workspace:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(mode=0o700, parents=True)
        self.released = False

    def release(self) -> None:
        shutil.rmtree(self.root)
        self.released = True


class Runtime:
    def __init__(self, root: Path, payload: bytes) -> None:
        self.root = root
        self.payload = payload
        self.workspace: Workspace | None = None

    def heartbeat(self) -> None:
        pass

    def open_workspace(self, root: Path) -> Workspace:
        assert root == self.root
        self.workspace = Workspace(root / "invocation")
        return self.workspace

    def materialize(
        self, subject: WorkArtifactSubject, *, workspace: Workspace, relative_path: str
    ) -> Path:
        assert int(subject.bytes) == len(self.payload)
        assert subject.sha256 == hashlib.sha256(self.payload).hexdigest()
        destination = workspace.root.joinpath(*relative_path.split("/"))
        destination.parent.mkdir(mode=0o700, parents=True)
        destination.write_bytes(self.payload)
        return destination


def observe(root: Path, payload: bytes, contract_id: str):
    observer = FfprobeObserver(
        workspace_root=root,
        image_id=os.environ["A_STOVE0_FFPROBE_OBSERVER_IMAGE_ID"],
        source_revision=os.environ.get("A_STOVE0_FFPROBE_OBSERVER_SOURCE_REVISION", "unknown"),
    )
    descriptor = observer.descriptor()
    support = descriptor.support_for(contract_id)
    contract, interface = {
        FFPROBE_STREAMS_OBSERVER_CONTRACT.id: (
            FFPROBE_STREAMS_OBSERVER_CONTRACT,
            FFPROBE_STREAMS_INTERFACE,
        ),
        MEDIA_SAMPLING_OBSERVER_CONTRACT.id: (
            MEDIA_SAMPLING_OBSERVER_CONTRACT,
            MEDIA_SAMPLING_INTERFACE,
        ),
    }[contract_id]
    subjects = (
        WorkArtifactSubject(
            id="media",
            role="stove0.source/v1",
            collection=CollectionRootIdentityRef(
                collection_id="1",
                archive_root_sha256="2" * 64,
                artifact_set_identity="3" * 64,
            ),
            artifact_id="4" * 64,
            bytes=str(len(payload)),
            sha256=hashlib.sha256(payload).hexdigest(),
        ),
    )
    selection = ArtifactSelection.seal(subjects).ref()
    question = ObservationQuestion.seal(
        ObservationQuestionPayload(
            work_id="1" * 64,
            task_id="probe",
            observer_contract=interface.observer_contract,
            interface=interface.ref,
            scope=selection,
            subject_ports={"subjects": selection},
            read_actions=contract.read_actions,
        )
    )
    request = ContentObservationRequest.seal(
        ContentObservationRequestPayload(
            work_id=question.work_id,
            task_id=question.task_id,
            question_sha256=question.question_sha256,
            interface=question.interface,
            observer_registration_id="ffprobe-tool-parity",
            observer_descriptor_sha256=descriptor.descriptor_sha256,
            observer_contract_id=support.contract_id,
            observer_contract_sha256=support.contract_sha256,
            subjects=subjects,
            maximum_result_bytes=256 * 1024,
        )
    )
    runtime = Runtime(root, payload)
    result = observer.observe(request, cast(ContentObservationRuntime, runtime))
    assert runtime.workspace is not None and runtime.workspace.released
    return result


def main() -> None:
    payload = sys.stdin.buffer.read(256 * 1024 + 1)
    assert payload and len(payload) <= 256 * 1024
    with TemporaryDirectory(prefix="ffprobe-parity-") as scratch:
        root = Path(scratch)
        result = observe(root, payload, FFPROBE_STREAMS_OBSERVER_CONTRACT.id)
        assert result.state == "observed", result.model_dump(mode="json")
        assert result.facts is not None
        facts = FFprobeStreamFacts.model_validate_json(canonical_json_bytes(result.facts))
        artifact = facts.artifacts[0]
        assert artifact.artifact_id == "media"
        assert artifact.has_audio and artifact.has_non_attached_video
        assert (artifact.audio_stream_count, artifact.video_stream_count) == (1, 1)
        assert artifact.format.name and "mp4" in artifact.format.name
        assert artifact.format.duration_ms is not None and artifact.format.duration_ms >= 1000
        video = next(row for row in artifact.streams if row.codec_type == "video")
        audio = next(row for row in artifact.streams if row.codec_type == "audio")
        assert (video.codec_name, video.profile, video.pixel_format) == ("h264", "High", "yuv420p")
        assert (video.width, video.height, video.fps_milli) == (320, 180, 24000)
        assert (video.color_space, video.color_transfer, video.color_primaries) == (
            "bt709",
            "bt709",
            "bt709",
        ), (video.color_space, video.color_transfer, video.color_primaries)
        assert (audio.codec_name, audio.sample_rate, audio.channels, audio.channel_layout) == (
            "aac",
            48000,
            2,
            "stereo",
        )
        assert audio.bit_rate is not None and audio.bit_rate > 0
        raw = base64.b64decode(artifact.report_base64, validate=True)
        assert len(raw) == artifact.report_bytes
        assert hashlib.sha256(raw).hexdigest() == artifact.report_sha256
        assert len(artifact.executable_sha256) == 64 and artifact.ffprobe_version.startswith(
            "ffprobe"
        )
        sampling = observe(root, payload, MEDIA_SAMPLING_OBSERVER_CONTRACT.id)
        assert sampling.state == "observed" and sampling.facts is not None
        sampled = sampling.facts["artifacts"][0]
        assert sampled["artifact_id"] == "media" and sampled["duration_ms"] >= 1000
        assert sampled["sampleable_ranges"]
        failed = observe(root, b"malformed non-media", FFPROBE_STREAMS_OBSERVER_CONTRACT.id)
        assert failed.state == "failed", failed.model_dump(mode="json")
        print(
            json.dumps(
                {
                    "format": "stove0-ffprobe-tool-parity/v1",
                    "ffprobe_version": artifact.ffprobe_version,
                    "executable_sha256": artifact.executable_sha256,
                    "report_sha256": artifact.report_sha256,
                    "stream_count": len(artifact.streams),
                    "registrations": [
                        FFPROBE_STREAMS_OBSERVER_CONTRACT.id,
                        MEDIA_SAMPLING_OBSERVER_CONTRACT.id,
                    ],
                },
                sort_keys=True,
            )
        )


if __name__ == "__main__":
    main()
