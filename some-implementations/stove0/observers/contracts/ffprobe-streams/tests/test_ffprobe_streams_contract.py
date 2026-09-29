from __future__ import annotations

import hashlib
import json

import pytest
from a_stove0_ffprobe_streams_contract_lib import (
    FFPROBE_STREAMS_OBSERVER_CONTRACT,
    artifact_facts,
    parse_ffprobe_report,
    validate_ffprobe_stream_facts,
)
from a_stove0_ffprobe_streams_contract_lib.contracts import (
    FFPROBE_STREAMS_FACTS_CONFORMANCE_VECTORS,
    FFPROBE_STREAMS_FACTS_SEMANTICS,
)
from pydantic import ValidationError
from stove0_protocol import CollectionRootIdentityRef, WorkArtifactSubject


def _subject() -> WorkArtifactSubject:
    return WorkArtifactSubject(
        id="source",
        role="stove0.source/v1",
        collection=CollectionRootIdentityRef(
            collection_id="1",
            archive_root_sha256="a" * 64,
            artifact_set_identity="b" * 64,
        ),
        artifact_id="c" * 64,
        bytes="123",
        sha256="d" * 64,
    )


def _report() -> bytes:
    return json.dumps(
        {
            "format": {
                "format_name": "mov,mp4,m4a,3gp,3g2,mj2",
                "format_long_name": "QuickTime / MOV",
                "duration": "12.500000",
                "bit_rate": "1200000",
                "size": "123456",
                "tags": {"major_brand": "isom"},
            },
            "streams": [
                {
                    "index": 0,
                    "codec_type": "video",
                    "codec_name": "h264",
                    "codec_long_name": "H.264 / AVC",
                    "codec_tag_string": "avc1",
                    "profile": "High",
                    "pix_fmt": "yuv420p",
                    "width": 1920,
                    "height": 1080,
                    "avg_frame_rate": "30000/1001",
                    "r_frame_rate": "30/1",
                    "color_range": "tv",
                    "color_space": "bt709",
                    "color_transfer": "bt709",
                    "color_primaries": "bt709",
                    "duration": "12.400000",
                    "bit_rate": "1000000",
                    "disposition": {"attached_pic": 0, "default": 1},
                    "tags": {"handler_name": "VideoHandler"},
                },
                {
                    "index": 1,
                    "codec_type": "audio",
                    "codec_name": "aac",
                    "codec_long_name": "AAC",
                    "profile": "LC",
                    "sample_rate": "48000",
                    "channels": 2,
                    "channel_layout": "stereo",
                    "bit_rate": "192000",
                    "duration": "12.500000",
                    "tags": {"language": "eng"},
                },
                {
                    "index": 2,
                    "codec_type": "video",
                    "codec_name": "mjpeg",
                    "disposition": {"attached_pic": 1},
                },
            ],
        },
        separators=(",", ":"),
    ).encode()


def test_stream_observation_restores_routing_surface_with_exact_report() -> None:
    raw = _report()
    row = artifact_facts("source", raw, ffprobe_version="ffprobe 7.0", executable_sha256="e" * 64)
    facts = {"artifacts": [row.model_dump(mode="json")]}
    validate_ffprobe_stream_facts(facts, (_subject(),))
    assert row.format.name == "mov,mp4,m4a,3gp,3g2,mj2"
    assert row.format.duration_ms == 12500
    assert row.format.bit_rate == 1200000
    assert row.format.tags["major_brand"] == "isom"
    assert row.has_audio and row.has_non_attached_video
    assert (row.audio_stream_count, row.video_stream_count) == (1, 2)
    video, audio, cover = row.streams
    assert (video.codec_name, video.profile, video.pixel_format) == ("h264", "High", "yuv420p")
    assert (video.width, video.height, video.fps_milli) == (1920, 1080, 29970)
    assert (video.color_space, video.color_transfer, video.color_primaries) == (
        "bt709",
        "bt709",
        "bt709",
    )
    assert (audio.codec_name, audio.sample_rate, audio.channels, audio.channel_layout) == (
        "aac",
        48000,
        2,
        "stereo",
    )
    assert (audio.bit_rate, audio.duration_ms, audio.tags["language"]) == (192000, 12500, "eng")
    assert cover.attached_pic
    assert row.report_sha256 == hashlib.sha256(raw).hexdigest()
    assert FFPROBE_STREAMS_OBSERVER_CONTRACT.id == "stove0.ffprobe.streams/v1"


def test_cover_art_alone_is_not_non_attached_video() -> None:
    report = json.loads(_report())
    report["streams"] = [report["streams"][2]]
    row = parse_ffprobe_report("source", json.dumps(report).encode())
    assert row["video_stream_count"] == 1
    assert not row["has_non_attached_video"]


def test_missing_or_malformed_probe_is_failure_not_negative_evidence() -> None:
    for raw in (b"", b"{}", b'{"streams":[]}', b'{"format":{},"streams":"bad"}'):
        with pytest.raises(ValueError):
            parse_ffprobe_report("source", raw)
    report = json.loads(_report())
    report["streams"][1]["index"] = 0
    with pytest.raises(ValueError, match="not unique"):
        parse_ffprobe_report("source", json.dumps(report).encode())


def test_report_preimage_and_subject_cannot_be_substituted() -> None:
    row = artifact_facts(
        "source", _report(), ffprobe_version="ffprobe 7.0", executable_sha256="e" * 64
    ).model_dump(mode="json")
    with pytest.raises(ValidationError, match="exact report commitment"):
        validate_ffprobe_stream_facts(
            {"artifacts": [{**row, "report_sha256": "0" * 64}]}, (_subject(),)
        )
    with pytest.raises(ValueError, match="exact requested subjects"):
        validate_ffprobe_stream_facts(
            {"artifacts": [{**row, "artifact_id": "other"}]}, (_subject(),)
        )
    forged = {**row, "has_audio": False}
    with pytest.raises(ValidationError, match="typed facts differ"):
        validate_ffprobe_stream_facts({"artifacts": [forged]}, (_subject(),))


def test_semantic_vectors_are_bound_and_executable() -> None:
    vectors = FFPROBE_STREAMS_FACTS_CONFORMANCE_VECTORS
    assert vectors.profile_id == FFPROBE_STREAMS_FACTS_SEMANTICS.id
    assert vectors.sha256 == FFPROBE_STREAMS_FACTS_SEMANTICS.conformance_vectors_sha256
    for vector in vectors.vectors:
        if vector.accepted:
            validate_ffprobe_stream_facts(vector.facts, vector.subjects)
        else:
            with pytest.raises(ValueError):
                validate_ffprobe_stream_facts(vector.facts, vector.subjects)
