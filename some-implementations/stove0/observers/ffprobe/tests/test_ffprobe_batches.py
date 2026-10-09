"""Batching retains exact facts and one-source workspace bounds, including failure."""

from typing import cast

import pytest
from a_stove0_ffprobe_observer import FfprobeObserver
from a_stove0_ffprobe_observer import observer as implementation
from a_stove0_ffprobe_streams_contract_lib import (
    FFPROBE_STREAMS_INTERFACE,
    FFPROBE_STREAMS_OBSERVER_CONTRACT,
)
from stove0_observer_protocol import ContentObservationRequest
from stove0_observer_support import ContentObservationRuntime
from stove0_protocol import CollectionRootIdentityRef, WorkArtifactSubject, canonical_json_bytes
from test_ffprobe_observer import FixtureRuntime

from tests.stove0_observation_fixtures import observation_payload


def _subjects(count):
    return tuple(
        WorkArtifactSubject(
            id=f"subject-{index:04}",
            role="stove0.source/v1",
            collection=CollectionRootIdentityRef(
                collection_id="1", archive_root_sha256="a" * 64, artifact_set_identity="b" * 64
            ),
            artifact_id=f"{index:064x}",
            bytes="15",
            sha256="c" * 64,
        )
        for index in range(count)
    )


def _request(observer, subjects):
    descriptor = observer.descriptor()
    contract = FFPROBE_STREAMS_OBSERVER_CONTRACT
    return ContentObservationRequest.seal(
        observation_payload(
            contract=contract,
            interface=FFPROBE_STREAMS_INTERFACE,
            work_id="e" * 64,
            observer_registration_id="ffprobe-streams",
            observer_descriptor_sha256=descriptor.descriptor_sha256,
            observer_contract_id=contract.id,
            observer_contract_sha256=contract.contract_sha256,
            subjects=subjects,
            maximum_result_bytes=4 * 1024 * 1024,
        )
    )


@pytest.fixture
def tool(monkeypatch):
    calls = []
    monkeypatch.setattr(implementation, "ffprobe_tool_identity", lambda _: ("fixture", "f" * 64))

    def report(_tool, source, **_kwargs):
        calls.append(source)
        return canonical_json_bytes(
            {
                "format": {"format_name": "wav", "duration": "0.25"},
                "streams": [{"index": 0, "codec_type": "audio", "codec_name": "pcm_s16le"}],
            }
        )

    monkeypatch.setattr(implementation, "bounded_ffprobe_report", report)
    return calls, report


class BoundedRuntime(FixtureRuntime):
    def materialize(self, subject, *, workspace, relative_path):
        # A batch must release the previous potentially large source before
        # downloading another; checking only final cleanup would miss this.
        assert not [path for path in workspace.root.rglob("*") if path.is_file()]
        destination = workspace.resolve(relative_path)
        destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        destination.write_bytes(b"immutable-media")
        return destination


def test_stream_batch_matches_individual_facts_and_releases_each_source(tmp_path, tool):
    observer = FfprobeObserver(image_id="sha256:" + "d" * 64)
    support = observer.descriptor().support_for(FFPROBE_STREAMS_OBSERVER_CONTRACT.id)
    assert support.preferred_subject_batch_size == 16
    subjects = _subjects(16)
    runtime = BoundedRuntime(tmp_path / "batch")
    batch = observer.observe(_request(observer, subjects), cast(ContentObservationRuntime, runtime))
    assert batch.state == "observed"
    assert runtime.workspace.released
    assert not any(path.is_file() for path in runtime.workspace.root.rglob("*"))
    individual = []
    for index, subject in enumerate(subjects):
        result = observer.observe(
            _request(observer, (subject,)),
            cast(ContentObservationRuntime, BoundedRuntime(tmp_path / f"individual-{index}")),
        )
        assert result.state == "observed"
        individual.extend(result.facts["artifacts"])
    assert batch.facts["artifacts"] == individual
    assert len(tool[0]) == 32


def test_invalid_late_subject_fails_the_whole_batch_and_cleans_workspace(
    tmp_path, tool, monkeypatch
):
    observer = FfprobeObserver(image_id="sha256:" + "d" * 64)
    subjects = _subjects(16)
    original = tool[1]

    def fail_last(*args, **kwargs):
        if len(tool[0]) == 15:
            raise ValueError("invalid final subject")
        return original(*args, **kwargs)

    monkeypatch.setattr(implementation, "bounded_ffprobe_report", fail_last)
    runtime = BoundedRuntime(tmp_path / "failed")
    result = observer.observe(
        _request(observer, subjects), cast(ContentObservationRuntime, runtime)
    )
    assert result.state == "failed" and result.facts is None
    assert result.failure.code == "invalid-stream-report"
    assert runtime.workspace.released
    assert not any(path.is_file() for path in runtime.workspace.root.rglob("*"))


def test_cancellation_between_subjects_does_not_leak_a_partial_result(tmp_path, tool):
    observer = FfprobeObserver(image_id="sha256:" + "d" * 64)

    class Canceled(RuntimeError):
        pass

    class Runtime(BoundedRuntime):
        def heartbeat(self):
            super().heartbeat()
            if self.heartbeats == 3:
                raise Canceled("the live invocation was canceled")

    runtime = Runtime(tmp_path / "canceled")
    with pytest.raises(Canceled):
        observer.observe(
            _request(observer, _subjects(16)), cast(ContentObservationRuntime, runtime)
        )
    assert len(tool[0]) == 2
    assert runtime.workspace.released
    assert not any(path.is_file() for path in runtime.workspace.root.rglob("*"))
