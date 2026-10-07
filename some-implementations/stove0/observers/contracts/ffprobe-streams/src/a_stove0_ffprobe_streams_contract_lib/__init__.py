"""FFprobe stream observation contract and deterministic parser."""

from .contracts import (
    FFPROBE_STREAMS_FACTS_CONFORMANCE_VECTORS,
    FFPROBE_STREAMS_OBSERVATION_ID,
    FFPROBE_STREAMS_OBSERVER_CONTRACT,
    FFPROBE_STREAMS_SEMANTIC_VALIDATOR,
    MAX_REPORT_BYTES,
    FFprobeArtifactFacts,
    FFprobeFormat,
    FFprobeStream,
    FFprobeStreamFacts,
    artifact_facts,
    parse_ffprobe_report,
    validate_ffprobe_stream_facts,
)

__all__ = [
    "FFPROBE_STREAMS_FACTS_CONFORMANCE_VECTORS",
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

from .interfaces import FFPROBE_STREAMS_INTERFACE, FFPROBE_STREAMS_INTERFACE_VECTORS

__all__ += ["FFPROBE_STREAMS_INTERFACE", "FFPROBE_STREAMS_INTERFACE_VECTORS"]
