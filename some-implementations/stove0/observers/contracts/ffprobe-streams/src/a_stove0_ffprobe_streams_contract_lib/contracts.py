"""Typed, subject-bound FFprobe facts with an exact bounded report preimage."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
from collections.abc import Mapping, Sequence
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from fractions import Fraction
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator
from stove0_observer_protocol import (
    ContentObservationRequest,
    JsonSchemaValidationProfile,
    ObserverContract,
    ObserverContractPayload,
    SemanticFactsConformanceVectors,
    SemanticValidationProfile,
    SemanticValidationProfilePayload,
    SemanticValidatorBinding,
    WorkArtifactSubject,
    canonical_json_bytes,
)

FFPROBE_STREAMS_OBSERVATION_ID = "stove0.ffprobe.streams/v1"
FFPROBE_STREAMS_OPTIONS_SCHEMA_ID = "stove0.ffprobe.streams-options/v1"
FFPROBE_STREAMS_FACTS_SCHEMA_ID = "stove0.ffprobe.streams-facts/v1"
MAX_REPORT_BYTES = 1024 * 1024
MAX_STREAMS = 256


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class FFprobeStream(_Model):
    index: int = Field(ge=0)
    codec_type: str = Field(min_length=1)
    codec_name: str | None = None
    codec_long_name: str | None = None
    codec_tag_string: str | None = None
    profile: str | None = None
    pixel_format: str | None = None
    width: int | None = Field(default=None, ge=0)
    height: int | None = Field(default=None, ge=0)
    average_frame_rate: str | None = None
    nominal_frame_rate: str | None = None
    fps_milli: int | None = Field(default=None, ge=0)
    sample_aspect_ratio: str | None = None
    display_aspect_ratio: str | None = None
    color_range: str | None = None
    color_space: str | None = None
    color_transfer: str | None = None
    color_primaries: str | None = None
    sample_rate: int | None = Field(default=None, ge=0)
    channels: int | None = Field(default=None, ge=0)
    channel_layout: str | None = None
    bit_rate: int | None = Field(default=None, ge=0)
    duration: str | None = None
    duration_ms: int | None = Field(default=None, ge=0)
    attached_pic: bool
    disposition: dict[str, int]
    tags: dict[str, str]


class FFprobeFormat(_Model):
    name: str | None = None
    long_name: str | None = None
    duration: str | None = None
    duration_ms: int | None = Field(default=None, ge=0)
    bit_rate: int | None = Field(default=None, ge=0)
    size: int | None = Field(default=None, ge=0)
    start_time: str | None = None
    tags: dict[str, str]


class FFprobeArtifactFacts(_Model):
    artifact_id: str = Field(min_length=1, max_length=160)
    report_base64: str = Field(min_length=1, max_length=2 * MAX_REPORT_BYTES)
    report_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    report_bytes: int = Field(ge=1, le=MAX_REPORT_BYTES)
    ffprobe_version: str = Field(min_length=1, max_length=200)
    executable_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    format: FFprobeFormat
    streams: tuple[FFprobeStream, ...] = Field(max_length=MAX_STREAMS)
    has_audio: bool
    has_non_attached_video: bool
    audio_stream_count: int = Field(ge=0, le=MAX_STREAMS)
    video_stream_count: int = Field(ge=0, le=MAX_STREAMS)

    @model_validator(mode="after")
    def validate_report(self) -> Self:
        try:
            raw = base64.b64decode(self.report_base64, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise ValueError("FFprobe report is not canonical base64") from exc
        if (
            len(raw) != self.report_bytes
            or len(raw) > MAX_REPORT_BYTES
            or base64.b64encode(raw).decode("ascii") != self.report_base64
            or hashlib.sha256(raw).hexdigest() != self.report_sha256
        ):
            raise ValueError("FFprobe exact report commitment differs")
        derived = self.model_dump(
            mode="json",
            exclude={
                "report_base64",
                "report_sha256",
                "report_bytes",
                "ffprobe_version",
                "executable_sha256",
            },
        )
        if derived != parse_ffprobe_report(self.artifact_id, raw):
            raise ValueError("FFprobe typed facts differ from the exact report")
        return self


class FFprobeStreamFacts(_Model):
    artifacts: tuple[FFprobeArtifactFacts, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def canonical_artifacts(self) -> Self:
        ids = [item.artifact_id for item in self.artifacts]
        if ids != sorted(ids) or len(ids) != len(set(ids)):
            raise ValueError("FFprobe artifacts must be unique and ordered")
        return self


def _json_constant(value: str) -> object:
    raise ValueError(f"nonfinite JSON constant in FFprobe report: {value}")


def _text(row: Mapping[str, Any], key: str) -> str | None:
    value = row.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"FFprobe {key} must be text")
    return value


def _nonnegative_integer(row: Mapping[str, Any], key: str) -> int | None:
    value = row.get(key)
    if value is None or value == "N/A":
        return None
    if type(value) is int and value >= 0:
        return value
    if isinstance(value, str) and value.isascii() and value.isdecimal():
        return int(value)
    raise ValueError(f"FFprobe {key} must be a nonnegative integer")


def _duration_ms(value: str | None) -> int | None:
    if value in (None, "N/A"):
        return None
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("FFprobe duration is malformed") from exc
    if not parsed.is_finite() or parsed < 0 or parsed > Decimal(2**63):
        raise ValueError("FFprobe duration is outside the supported domain")
    return int((parsed * 1000).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _fps_milli(average: str | None, nominal: str | None) -> int | None:
    for value in (average, nominal):
        if value in (None, "N/A", "0/0"):
            continue
        try:
            parsed = Fraction(value)
        except (ValueError, ZeroDivisionError) as exc:
            raise ValueError("FFprobe frame rate is malformed") from exc
        if parsed < 0 or parsed > 1_000_000:
            raise ValueError("FFprobe frame rate is outside the supported domain")
        if parsed:
            return int(
                (Decimal(parsed.numerator) * 1000 / parsed.denominator).quantize(
                    Decimal("1"), rounding=ROUND_HALF_UP
                )
            )
    return None


def _tags(row: Mapping[str, Any]) -> dict[str, str]:
    value = row.get("tags", {})
    if not isinstance(value, dict) or any(
        not isinstance(key, str) or not isinstance(item, str) for key, item in value.items()
    ):
        raise ValueError("FFprobe tags must be text pairs")
    return value


def _stream(row: Mapping[str, Any]) -> FFprobeStream:
    index = row.get("index")
    kind = row.get("codec_type")
    if type(index) is not int or index < 0 or not isinstance(kind, str) or not kind:
        raise ValueError("FFprobe stream index/type is missing or malformed")
    disposition = row.get("disposition", {})
    if not isinstance(disposition, dict) or any(
        not isinstance(key, str) or type(value) is not int or value not in (0, 1)
        for key, value in disposition.items()
    ):
        raise ValueError("FFprobe stream disposition is malformed")
    attached = disposition.get("attached_pic", 0)
    average = _text(row, "avg_frame_rate")
    nominal = _text(row, "r_frame_rate")
    duration = _text(row, "duration")
    return FFprobeStream(
        index=index,
        codec_type=kind,
        codec_name=_text(row, "codec_name"),
        codec_long_name=_text(row, "codec_long_name"),
        codec_tag_string=_text(row, "codec_tag_string"),
        profile=_text(row, "profile"),
        pixel_format=_text(row, "pix_fmt"),
        width=_nonnegative_integer(row, "width"),
        height=_nonnegative_integer(row, "height"),
        average_frame_rate=average,
        nominal_frame_rate=nominal,
        fps_milli=_fps_milli(average, nominal),
        sample_aspect_ratio=_text(row, "sample_aspect_ratio"),
        display_aspect_ratio=_text(row, "display_aspect_ratio"),
        color_range=_text(row, "color_range"),
        color_space=_text(row, "color_space"),
        color_transfer=_text(row, "color_transfer"),
        color_primaries=_text(row, "color_primaries"),
        sample_rate=_nonnegative_integer(row, "sample_rate"),
        channels=_nonnegative_integer(row, "channels"),
        channel_layout=_text(row, "channel_layout"),
        bit_rate=_nonnegative_integer(row, "bit_rate"),
        duration=duration,
        duration_ms=_duration_ms(duration),
        attached_pic=bool(attached),
        disposition=disposition,
        tags=_tags(row),
    )


def parse_ffprobe_report(artifact_id: str, raw: bytes) -> dict[str, Any]:
    """Parse one exact report; absence or corruption is never a nonmedia result."""
    if not raw or len(raw) > MAX_REPORT_BYTES:
        raise ValueError("FFprobe report is empty or exceeds the bounded contract")
    try:
        report = json.loads(raw, parse_constant=_json_constant)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("FFprobe report is malformed JSON") from exc
    if not isinstance(report, dict):
        raise ValueError("FFprobe report must be an object")
    streams = report.get("streams")
    format_row = report.get("format")
    if not isinstance(streams, list) or not isinstance(format_row, dict):
        raise ValueError("FFprobe report lacks stream or container facts")
    if len(streams) > MAX_STREAMS or any(not isinstance(row, dict) for row in streams):
        raise ValueError("FFprobe stream set is malformed or exceeds its bound")
    parsed_streams = tuple(_stream(row) for row in streams)
    indices = [stream.index for stream in parsed_streams]
    if len(indices) != len(set(indices)):
        raise ValueError("FFprobe stream indices are not unique")
    duration = _text(format_row, "duration")
    parsed_format = FFprobeFormat(
        name=_text(format_row, "format_name"),
        long_name=_text(format_row, "format_long_name"),
        duration=duration,
        duration_ms=_duration_ms(duration),
        bit_rate=_nonnegative_integer(format_row, "bit_rate"),
        size=_nonnegative_integer(format_row, "size"),
        start_time=_text(format_row, "start_time"),
        tags=_tags(format_row),
    )
    return {
        "artifact_id": artifact_id,
        "format": parsed_format.model_dump(mode="json"),
        "streams": [stream.model_dump(mode="json") for stream in parsed_streams],
        "has_audio": any(stream.codec_type == "audio" for stream in parsed_streams),
        "has_non_attached_video": any(
            stream.codec_type == "video" and not stream.attached_pic for stream in parsed_streams
        ),
        "audio_stream_count": sum(stream.codec_type == "audio" for stream in parsed_streams),
        "video_stream_count": sum(stream.codec_type == "video" for stream in parsed_streams),
    }


def artifact_facts(
    artifact_id: str, raw: bytes, *, ffprobe_version: str, executable_sha256: str
) -> FFprobeArtifactFacts:
    return FFprobeArtifactFacts.model_validate_json(
        canonical_json_bytes(
            {
                **parse_ffprobe_report(artifact_id, raw),
                "report_base64": base64.b64encode(raw).decode("ascii"),
                "report_sha256": hashlib.sha256(raw).hexdigest(),
                "report_bytes": len(raw),
                "ffprobe_version": ffprobe_version,
                "executable_sha256": executable_sha256,
            }
        )
    )


def validate_ffprobe_stream_facts(
    facts: Mapping[str, object], subjects: Sequence[WorkArtifactSubject]
) -> FFprobeStreamFacts:
    document = FFprobeStreamFacts.model_validate_json(canonical_json_bytes(dict(facts)))
    if tuple(item.artifact_id for item in document.artifacts) != tuple(
        subject.id for subject in subjects
    ):
        raise ValueError("FFprobe facts must cover the exact requested subjects")
    return document


def _validate_observation(request: ContentObservationRequest, facts: Mapping[str, object]) -> None:
    validate_ffprobe_stream_facts(facts, request.subjects)


FFPROBE_STREAMS_OPTIONS_SCHEMA = JsonSchemaValidationProfile.from_schema(
    FFPROBE_STREAMS_OPTIONS_SCHEMA_ID,
    {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
)
FFPROBE_STREAMS_FACTS_SCHEMA = JsonSchemaValidationProfile.from_schema(
    FFPROBE_STREAMS_FACTS_SCHEMA_ID,
    FFprobeStreamFacts.model_json_schema(),
)
_SAMPLE_REPORT = canonical_json_bytes(
    {
        "format": {"format_name": "wav", "duration": "1.000000"},
        "streams": [{"index": 0, "codec_type": "audio", "codec_name": "pcm_s16le"}],
    }
)
_SAMPLE_FACT = artifact_facts(
    "sample", _SAMPLE_REPORT, ffprobe_version="ffprobe fixture", executable_sha256="f" * 64
).model_dump(mode="json")
_SAMPLE_SUBJECT = {
    "id": "sample",
    "role": "stove0.source/v1",
    "collection": {
        "collection_id": "1",
        "archive_root_sha256": "a" * 64,
        "artifact_set_identity": "b" * 64,
    },
    "artifact_id": "c" * 64,
    "bytes": "1",
    "sha256": "d" * 64,
}
FFPROBE_STREAMS_FACTS_CONFORMANCE_VECTORS = SemanticFactsConformanceVectors.model_validate(
    {
        "profile_id": "stove0.ffprobe.streams-facts-semantics/v1",
        "vectors": [
            {
                "id": "accepted-exact-report-and-subject",
                "accepted": True,
                "subjects": [_SAMPLE_SUBJECT],
                "facts": {"artifacts": [_SAMPLE_FACT]},
            },
            {
                "id": "rejected-report-digest",
                "accepted": False,
                "subjects": [_SAMPLE_SUBJECT],
                "facts": {"artifacts": [{**_SAMPLE_FACT, "report_sha256": "0" * 64}]},
            },
            {
                "id": "rejected-wrong-subject",
                "accepted": False,
                "subjects": [_SAMPLE_SUBJECT],
                "facts": {"artifacts": [{**_SAMPLE_FACT, "artifact_id": "other"}]},
            },
        ],
    }
)
FFPROBE_STREAMS_FACTS_SEMANTICS = SemanticValidationProfile.seal(
    SemanticValidationProfilePayload(
        id="stove0.ffprobe.streams-facts-semantics/v1",
        rules=(
            "stove0.ffprobe.streams.exact-report/v1",
            "stove0.ffprobe.streams.exact-subjects/v1",
            "stove0.ffprobe.streams.typed-facts/v1",
        ),
        conformance_vectors_sha256=FFPROBE_STREAMS_FACTS_CONFORMANCE_VECTORS.sha256,
    )
)
FFPROBE_STREAMS_SEMANTIC_VALIDATOR = SemanticValidatorBinding.from_profile(
    FFPROBE_STREAMS_FACTS_SEMANTICS, _validate_observation
)
FFPROBE_STREAMS_OBSERVER_CONTRACT = ObserverContract.seal(
    ObserverContractPayload(
        id=FFPROBE_STREAMS_OBSERVATION_ID,
        options_schema=FFPROBE_STREAMS_OPTIONS_SCHEMA,
        facts_schema=FFPROBE_STREAMS_FACTS_SCHEMA,
        facts_semantics=FFPROBE_STREAMS_FACTS_SEMANTICS,
    )
)


__all__ = [
    "FFPROBE_STREAMS_OBSERVATION_ID",
    "FFPROBE_STREAMS_OBSERVER_CONTRACT",
    "FFPROBE_STREAMS_SEMANTIC_VALIDATOR",
    "FFprobeArtifactFacts",
    "FFprobeFormat",
    "FFprobeStream",
    "FFprobeStreamFacts",
    "MAX_REPORT_BYTES",
    "artifact_facts",
    "parse_ffprobe_report",
    "validate_ffprobe_stream_facts",
]
