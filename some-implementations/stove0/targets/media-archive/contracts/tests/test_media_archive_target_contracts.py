from __future__ import annotations

import math

import pytest
from a_stove0_media_archive_contract_lib import (
    AUDIO_ARCHIVE_INTENT_CONFORMANCE_VECTORS,
    AUDIO_ARCHIVE_INTENT_SEMANTICS,
    AUDIO_ARCHIVE_OPERATION,
    AV1_OPUS_ARCHIVE_INTENT_CONFORMANCE_VECTORS,
    AV1_OPUS_ARCHIVE_INTENT_SEMANTICS,
    AV1_OPUS_ARCHIVE_OPERATION,
    SOURCE_ARTIFACT_ROLE,
    Av1OpusArchiveIntent,
    validate_audio_archive_intent,
    validate_av1_opus_archive_intent,
)


def test_media_archive_operations_retain_exact_v1_retirement_semantics() -> None:
    source_artifacts = next(
        output
        for output in AV1_OPUS_ARCHIVE_OPERATION.outputs
        if output.role == SOURCE_ARTIFACT_ROLE
    )

    assert AUDIO_ARCHIVE_OPERATION.id == "stove0.media.audio-archive/v1"
    assert (
        AUDIO_ARCHIVE_OPERATION.contract_sha256
        == "ebbce54d0380c00fe10e79992791f23208e70a7d6e5bb2608b7fae49e7ae03c6"
    )
    assert AUDIO_ARCHIVE_OPERATION.source_retirement_permitted is False
    assert AV1_OPUS_ARCHIVE_OPERATION.id == "stove0.media.av1-opus-archive/v1"
    assert (
        AV1_OPUS_ARCHIVE_OPERATION.contract_sha256
        == "b1b4e9b710b3cecdfe62c3c01bd1b5dac9693741a3ed8bda08c527a0077edf33"
    )
    assert AV1_OPUS_ARCHIVE_OPERATION.source_retirement_permitted is True
    assert source_artifacts.minimum == 1
    assert "preserve_source_artifacts" not in Av1OpusArchiveIntent.model_json_schema()["properties"]
    assert AUDIO_ARCHIVE_OPERATION.intent_semantics == AUDIO_ARCHIVE_INTENT_SEMANTICS
    assert AV1_OPUS_ARCHIVE_OPERATION.intent_semantics == AV1_OPUS_ARCHIVE_INTENT_SEMANTICS


def test_media_archive_semantic_profile_executes_projection_rules() -> None:
    with pytest.raises(ValueError, match="GPS coordinates"):
        validate_av1_opus_archive_intent(
            {
                "metadata_projection": {
                    "gps": {"latitude": math.nan, "longitude": 0.0},
                }
            }
        )


def test_media_archive_semantic_vectors_are_bound_and_executable() -> None:
    assert AUDIO_ARCHIVE_INTENT_CONFORMANCE_VECTORS.profile_id == (
        AUDIO_ARCHIVE_INTENT_SEMANTICS.id
    )
    assert AUDIO_ARCHIVE_INTENT_SEMANTICS.conformance_vectors_sha256 == (
        AUDIO_ARCHIVE_INTENT_CONFORMANCE_VECTORS.sha256
    )
    for vector in AUDIO_ARCHIVE_INTENT_CONFORMANCE_VECTORS.vectors:
        if vector.accepted:
            validate_audio_archive_intent(vector.intent)
        else:
            with pytest.raises(ValueError):
                validate_audio_archive_intent(vector.intent)

    assert AV1_OPUS_ARCHIVE_INTENT_CONFORMANCE_VECTORS.profile_id == (
        AV1_OPUS_ARCHIVE_INTENT_SEMANTICS.id
    )
    assert AV1_OPUS_ARCHIVE_INTENT_SEMANTICS.conformance_vectors_sha256 == (
        AV1_OPUS_ARCHIVE_INTENT_CONFORMANCE_VECTORS.sha256
    )
    for vector in AV1_OPUS_ARCHIVE_INTENT_CONFORMANCE_VECTORS.vectors:
        if vector.accepted:
            validate_av1_opus_archive_intent(vector.intent)
        else:
            with pytest.raises(ValueError):
                validate_av1_opus_archive_intent(vector.intent)
