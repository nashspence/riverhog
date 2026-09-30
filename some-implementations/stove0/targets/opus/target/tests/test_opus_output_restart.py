from __future__ import annotations

import hashlib
import threading
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from a_stove0_media_archive_contract_lib import (
    AUDIO_ARCHIVE_OPERATION,
    SOURCE_ROLE,
    XMP_SOURCE_ROLE,
)
from a_stove0_opus_target import OpusTargetService
from a_stove0_opus_target import target as owner
from riverhog_client import ProducerArtifactCustody
from riverhog_client.processing import (
    ClaimedCollectionRuntimeRegistry,
    CollectionTransformRuntime,
    ProcessingWorkspace,
)
from riverhog_protocol import ServiceUnavailable
from stove0_target_protocol import InputArtifact
from stove0_target_support import (
    TargetCollectionPublication,
    TargetExecutionRuntime,
    TargetExecutionSession,
)

from tests.fixtures.stove0_media import sealed_media_job


def test_opus_executor_resumes_partial_receipted_production_without_encoding_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = OpusTargetService(
        state_root=tmp_path / "state",
        workspace_root=tmp_path / "workspace",
        image_id="sha256:" + "9" * 64,
        source_revision="fixture",
    )
    request = sealed_media_job(
        target, AUDIO_ARCHIVE_OPERATION, {"codec": "opus", "container": "opus", "bitrate_kbps": 96}
    )
    evidence = request.declaration.controller_evidence.execution_envelope.workflow_plan.observations
    inputs = tuple(
        InputArtifact.model_validate(
            {
                **item.model_dump(mode="json"),
                "role": SOURCE_ROLE if item.id == "primary" else XMP_SOURCE_ROLE,
            }
        )
        for item in evidence[0].request.subjects
    )
    encoded = []
    downloads = []
    declarations = {}
    receipted = {}
    fail_next = True
    result = object()

    def encode(command, **_kwargs):
        encoded.append(tuple(command))
        Path(command[-1]).write_bytes(b"produced opus bytes")

    monkeypatch.setattr(owner, "run_ffmpeg", encode)
    monkeypatch.setattr(owner, "tool_version", lambda _tool: "fixture ffmpeg version")

    class Callback:
        def iter_inputs(self, job_id):
            assert job_id == request.declaration.job_id
            return iter(inputs)

        def declare_target_execution_output(self, _job_id, artifact):
            previous = declarations.setdefault(artifact.id, artifact)
            assert previous == artifact

        def declare_target_execution_source_edge(self, _job_id, _edge):
            pass

        def declare_target_execution_disposition(self, _job_id, _disposition):
            pass

        def close(self):
            pass

    class Producer:
        def set_pending_source_resolver(self, resolver):
            self.resolver = resolver

        def resume_artifact_custody(self, identity):
            prior = receipted.get(identity.artifact_id)
            assert prior is None or prior == identity
            return object() if prior is not None else None

    class Runtime(CollectionTransformRuntime):
        def __init__(self):
            self.producer = Producer()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def open_workspace(self, root, *, declared_protection):
            return ProcessingWorkspace.open(
                root,
                execution_id=request.declaration.job_id,
                declared_protection=declared_protection,
            )

        def open_incremental_publication(self, **_kwargs):
            return SimpleNamespace(producer=self.producer)

        @contextmanager
        def prepare_inputs(self, artifacts, **_kwargs):
            assert len(artifacts) == 1

            def download(artifact, destination):
                downloads.append(artifact.artifact_id)
                destination.write_bytes(b"retained source bytes")

            yield SimpleNamespace(download=download)

        def append_incremental_output(self, _writer, source, *, identity, **_kwargs):
            nonlocal fail_next
            assert hashlib.sha256(source.source.read_bytes()).hexdigest() == identity.sha256
            if len(receipted) == 1 and fail_next:
                fail_next = False
                raise ServiceUnavailable("simulated crash after first output custody")
            receipted[identity.artifact_id] = identity
            return (ProducerArtifactCustody(identity, None),)

    def from_request(_cls, accepted, *, session, **_kwargs):
        execution = TargetExecutionRuntime(accepted, Runtime(), session=session)
        execution._input_client.close()
        execution._input_client = Callback()
        return execution

    def finish(publication, *, execution_preimage, runtime_evidence, **_kwargs):
        assert len(declarations) == len(receipted) == 3
        assert runtime_evidence == {"ffmpeg": "fixture ffmpeg version"}
        assert execution_preimage
        publication.execution._completed = True
        return result

    monkeypatch.setattr(TargetExecutionRuntime, "from_request", classmethod(from_request))
    monkeypatch.setattr(TargetCollectionPublication, "finish_success", finish)
    try:
        first = TargetExecutionSession(
            request, 1, ClaimedCollectionRuntimeRegistry(), state_root=target.state_root
        )
        with pytest.raises(ServiceUnavailable, match="first output custody"):
            target._execute(request, 1, threading.Event(), first)
        assert len(encoded) == 1
        assert len(receipted) == 1
        before = dict(declarations)
        restarted = TargetExecutionSession(
            request, 2, ClaimedCollectionRuntimeRegistry(), state_root=target.state_root
        )
        assert target._execute(request, 2, threading.Event(), restarted) is result
        assert len(encoded) == 1
        assert len(downloads) == 2  # primary once, original sidecar once
        assert all(declarations[key] == value for key, value in before.items())
        assert not (target.workspace_root / request.declaration.job_id).exists()
    finally:
        target.close()
