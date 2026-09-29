from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

from a_stove0_ffprobe_observer import FfprobeObserver
from a_stove0_ffprobe_observer import app as observer_app
from a_stove0_ffprobe_observer.app import create_app
from a_stove0_ffprobe_streams_contract_lib import FFPROBE_STREAMS_OBSERVER_CONTRACT
from a_stove0_media_sampling_contract_lib import MEDIA_SAMPLING_OBSERVER_CONTRACT
from fastapi.testclient import TestClient
from stove0_observer_protocol import ContentObservationRequest, ContentObservationRequestPayload
from stove0_observer_support import ContentObservationRuntime
from stove0_protocol import (
    CollectionRootIdentityRef,
    WorkArtifactSubject,
)


def _sha(character: str) -> str:
    return character * 64


class FixtureWorkspace:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(mode=0o700, parents=True)
        self.released = False

    def resolve(self, relative_path: str) -> Path:
        return self.root.joinpath(*relative_path.split("/"))

    def release(self) -> None:
        self.released = True


class FixtureRuntime:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.workspace: FixtureWorkspace | None = None
        self.heartbeats = 0

    def heartbeat(self) -> None:
        self.heartbeats += 1

    def open_workspace(self, _root: Path) -> FixtureWorkspace:
        self.workspace = FixtureWorkspace(self.root)
        return self.workspace

    def materialize(
        self,
        _subject: WorkArtifactSubject,
        *,
        workspace: FixtureWorkspace,
        relative_path: str,
    ) -> Path:
        destination = workspace.resolve(relative_path)
        destination.parent.mkdir(mode=0o700, parents=True)
        destination.write_bytes(b"immutable-media")
        return destination


def test_ffprobe_observer_reports_contract_facts_and_exact_image(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    observer = FfprobeObserver(
        ffprobe="fixture-ffprobe",
        workspace_root=tmp_path / "observer-workspace",
        source_revision="fixture",
        image_id="sha256:" + _sha("9"),
    )
    descriptor = observer.descriptor()
    support = descriptor.support_for(MEDIA_SAMPLING_OBSERVER_CONTRACT.id)
    request = ContentObservationRequest.seal(
        ContentObservationRequestPayload(
            work_id=_sha("1"),
            observer_registration_id="ffprobe-sampling",
            observer_descriptor_sha256=descriptor.descriptor_sha256,
            observer_contract_id=support.contract_id,
            observer_contract_sha256=support.contract_sha256,
            subjects=(
                WorkArtifactSubject(
                    id="camera-source",
                    role="stove0.review.source/v1",
                    collection=CollectionRootIdentityRef(
                        collection_id=str(1),
                        archive_root_sha256=_sha("2"),
                        content_identity=_sha("3"),
                    ),
                    artifact_id=_sha("5"),
                    bytes=str(15),
                    sha256=_sha("4"),
                ),
            ),
            maximum_result_bytes=256 * 1024,
        )
    )

    def run(command: list[str], **_kwargs: object) -> SimpleNamespace:
        if "-version" in command:
            return SimpleNamespace(returncode=0, stdout="ffprobe fixture\n", stderr="")
        return SimpleNamespace(
            returncode=0,
            stdout=json.dumps(
                {"format": {"duration": "12.5"}, "streams": [{"duration": "12.4"}]}
            ).encode(),
            stderr=b"",
        )

    monkeypatch.setattr(
        "a_stove0_ffprobe_observer.observer.subprocess.run",
        run,
    )
    runtime = FixtureRuntime(tmp_path / "request")
    result = observer.observe(request, cast(ContentObservationRuntime, runtime))

    assert descriptor.image_id == "sha256:" + _sha("9")
    assert support.contract_id == MEDIA_SAMPLING_OBSERVER_CONTRACT.id
    assert result.state == "observed"
    assert result.facts == {
        "artifacts": [
            {
                "artifact_id": "camera-source",
                "duration_ms": 12500,
                "sampleable_ranges": [
                    {"start_ms": 0, "duration_ms": 12500},
                ],
            }
        ]
    }
    assert runtime.heartbeats == 1
    assert runtime.workspace is not None and runtime.workspace.released


def test_stream_registration_reports_exact_subject_bound_container_and_streams(
    tmp_path: Path,
) -> None:
    report = json.dumps(
        {
            "format": {"format_name": "wav", "duration": "1.000000", "bit_rate": "768000"},
            "streams": [
                {
                    "index": 0,
                    "codec_type": "audio",
                    "codec_name": "pcm_s16le",
                    "sample_rate": "48000",
                    "channels": 1,
                    "channel_layout": "mono",
                }
            ],
        },
        separators=(",", ":"),
    )
    tool = tmp_path / "fixture-ffprobe"
    tool.write_text(
        "#!/bin/sh\n"
        'if [ "$1" = "-version" ]; then printf \'ffprobe fixture\\n\'; '
        f"else printf '%s' '{report}'; fi\n",
        encoding="utf-8",
    )
    tool.chmod(0o755)
    observer = FfprobeObserver(
        ffprobe=str(tool),
        source_revision="fixture",
        image_id="sha256:" + _sha("9"),
        workspace_root=tmp_path / "workspace",
    )
    descriptor = observer.descriptor()
    assert [item.contract_id for item in descriptor.contracts] == [
        FFPROBE_STREAMS_OBSERVER_CONTRACT.id,
        MEDIA_SAMPLING_OBSERVER_CONTRACT.id,
    ]
    support = descriptor.support_for(FFPROBE_STREAMS_OBSERVER_CONTRACT.id)
    request = ContentObservationRequest.seal(
        ContentObservationRequestPayload(
            work_id=_sha("1"),
            observer_registration_id="ffprobe-streams",
            observer_descriptor_sha256=descriptor.descriptor_sha256,
            observer_contract_id=support.contract_id,
            observer_contract_sha256=support.contract_sha256,
            subjects=(
                WorkArtifactSubject(
                    id="source",
                    role="stove0.source/v1",
                    collection=CollectionRootIdentityRef(
                        collection_id="1",
                        archive_root_sha256=_sha("2"),
                        content_identity=_sha("3"),
                    ),
                    artifact_id=_sha("5"),
                    bytes="15",
                    sha256=_sha("4"),
                ),
            ),
            maximum_result_bytes=4 * 1024 * 1024,
        )
    )
    runtime = FixtureRuntime(tmp_path / "request")
    result = observer.observe(request, cast(ContentObservationRuntime, runtime))
    assert result.state == "observed"
    assert result.facts is not None
    row = cast(dict[str, Any], result.facts["artifacts"][0])
    assert row["artifact_id"] == "source"
    assert row["format"]["duration_ms"] == 1000
    assert row["streams"][0]["sample_rate"] == 48000
    assert row["has_audio"] and not row["has_non_attached_video"]
    assert row["report_sha256"] == hashlib.sha256(report.encode()).hexdigest()
    assert row["executable_sha256"] == hashlib.sha256(tool.read_bytes()).hexdigest()
    assert result.execution_evidence["ffprobe_version"] == "ffprobe fixture"
    assert runtime.workspace is not None and runtime.workspace.released

    tool.write_text(
        "#!/bin/sh\n"
        'if [ "$1" = "-version" ]; then printf \'ffprobe fixture\\n\'; '
        "else printf '{}'; fi\n",
        encoding="utf-8",
    )
    tool.chmod(0o755)
    malformed = observer.observe(
        request,
        cast(ContentObservationRuntime, FixtureRuntime(tmp_path / "malformed-request")),
    )
    assert malformed.state == "failed"
    assert malformed.failure is not None and malformed.failure.code == "invalid-stream-report"


def test_observer_process_exposes_only_observer_contract() -> None:
    observer = FfprobeObserver(
        source_revision="fixture",
        image_id="sha256:" + _sha("9"),
    )
    client = TestClient(create_app(token="observer-secret", observer=observer))
    response = client.get(
        "/v1/observer",
        headers={"Authorization": "Bearer observer-secret"},
    )
    assert response.status_code == 200
    assert response.json()["implementation_id"] == "a-stove0-ffprobe-observer/v1"
    assert (
        client.get(
            "/v1/target",
            headers={"Authorization": "Bearer observer-secret"},
        ).status_code
        == 404
    )


def test_observer_process_environment_is_connected(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    token_file = tmp_path / "observer.token"
    token_file.write_text("file-secret\n", encoding="utf-8")
    monkeypatch.setenv("A_STOVE0_FFPROBE_OBSERVER_TOKEN_FILE", str(token_file))
    monkeypatch.delenv("A_STOVE0_FFPROBE_OBSERVER_TOKEN", raising=False)
    assert observer_app._secret() == "file-secret"
    monkeypatch.delenv("A_STOVE0_FFPROBE_OBSERVER_TOKEN_FILE")
    monkeypatch.setenv("A_STOVE0_FFPROBE_OBSERVER_TOKEN", "direct-secret")
    monkeypatch.setenv("A_STOVE0_FFPROBE_OBSERVER_HOST", "127.0.0.7")
    monkeypatch.setenv("A_STOVE0_FFPROBE_OBSERVER_PORT", "8177")
    monkeypatch.setenv("STOVE0_FFPROBE_BIN", "fixture-ffprobe")
    monkeypatch.setenv(
        "A_STOVE0_FFPROBE_OBSERVER_WORKSPACE",
        str(tmp_path / "workspace"),
    )
    monkeypatch.setenv("A_STOVE0_FFPROBE_OBSERVER_SOURCE_REVISION", "fixture-revision")
    monkeypatch.setenv("A_STOVE0_FFPROBE_OBSERVER_IMAGE_ID", "sha256:" + _sha("8"))
    created: dict[str, object] = {}

    class ConfiguredObserver:
        def __init__(self, **kwargs: object) -> None:
            created.update(kwargs)
            self.ffprobe = str(kwargs["ffprobe"])

    def run(_app: object, *, host: str, port: int) -> None:
        created["host"] = host
        created["port"] = port

    monkeypatch.setattr(observer_app, "FfprobeObserver", ConfiguredObserver)
    monkeypatch.setattr(observer_app.uvicorn, "run", run)

    assert observer_app.main([]) == 0
    assert created == {
        "ffprobe": "fixture-ffprobe",
        "workspace_root": tmp_path / "workspace",
        "source_revision": "fixture-revision",
        "image_id": "sha256:" + _sha("8"),
        "host": "127.0.0.7",
        "port": 8177,
    }
