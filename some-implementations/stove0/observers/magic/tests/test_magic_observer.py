from __future__ import annotations

import hashlib
from contextlib import contextmanager
from pathlib import Path
from typing import Any, cast

import pytest
from a_stove0_magic_facts_contract_lib import MAGIC_OBSERVER_CONTRACT, validate_magic_facts
from a_stove0_magic_facts_contract_lib.contracts import MAGIC_CONFORMANCE_VECTORS
from a_stove0_magic_observer import FileMagic, MagicObserver
from stove0_observer_protocol import (
    CollectionRootIdentityRef,
    ContentObservationRequest,
    ContentObservationRequestPayload,
    WorkArtifactSubject,
)
from stove0_observer_support import ContentObservationRuntime

_GIF = b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00"


def _request(observer: MagicObserver) -> ContentObservationRequest:
    subject = WorkArtifactSubject(
        id="sample",
        role="stove0.source/v1",
        collection=CollectionRootIdentityRef(
            collection_id="1", archive_root_sha256="a" * 64, artifact_set_identity="b" * 64
        ),
        artifact_id="c" * 64,
        bytes=str(len(_GIF)),
        sha256=hashlib.sha256(_GIF).hexdigest(),
    )
    support = observer.descriptor().support_for(MAGIC_OBSERVER_CONTRACT.id)
    return ContentObservationRequest.seal(
        ContentObservationRequestPayload(
            work_id="d" * 64,
            observer_registration_id="magic",
            observer_descriptor_sha256=observer.descriptor().descriptor_sha256,
            observer_contract_id=support.contract_id,
            observer_contract_sha256=support.contract_sha256,
            read_actions=("read-inputs",),
            subjects=(subject,),
        )
    )


def test_magic_semantics_bind_exact_subject_and_complete_sample() -> None:
    for vector in MAGIC_CONFORMANCE_VECTORS.vectors:
        if vector.accepted:
            validate_magic_facts(vector.facts, vector.subjects, {})
        else:
            with pytest.raises(ValueError):
                validate_magic_facts(vector.facts, vector.subjects, {})


def test_real_file_tool_reports_bounded_bytes_without_a_source_name() -> None:
    engine = FileMagic(Path("/usr/bin/file"), Path("/usr/share/misc/magic.mgc"))
    observer = MagicObserver(engine, image_id="sha256:" + "e" * 64)
    request = _request(observer)

    class Runtime:
        def heartbeat(self) -> None:
            pass

        @contextmanager
        def stream(self, _subject: WorkArtifactSubject, *, start: int, end: int) -> Any:
            assert start == 0
            yield iter((_GIF[:end],))

    result = observer.observe(request, cast(ContentObservationRuntime, Runtime()))
    assert result.state == "observed"
    assert result.facts is not None
    fact = result.facts["artifacts"][0]
    assert fact["mime_type"] == "image/gif"
    assert fact["complete_payload"] is True
    assert fact["sample_sha256"] == hashlib.sha256(_GIF).hexdigest()

    class Truncated(Runtime):
        @contextmanager
        def stream(self, _subject: WorkArtifactSubject, *, start: int, end: int) -> Any:
            yield iter((_GIF[: end - 1],))

    failed = observer.observe(request, cast(ContentObservationRuntime, Truncated()))
    assert failed.state == "failed"
    assert failed.facts is None
