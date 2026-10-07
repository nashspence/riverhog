"""Exact structural recipe interface owned by this observation contract."""

from stove0_observer_protocol.interfaces import subject_interface

from .contracts import FFPROBE_STREAMS_FACTS_CONFORMANCE_VECTORS, FFPROBE_STREAMS_OBSERVER_CONTRACT

FFPROBE_STREAMS_INTERFACE, FFPROBE_STREAMS_INTERFACE_VECTORS = subject_interface(
    contract=FFPROBE_STREAMS_OBSERVER_CONTRACT,
    facts_vectors=FFPROBE_STREAMS_FACTS_CONFORMANCE_VECTORS,
    subject_at="/artifact_id",
    id="stove0.ffprobe.streams.interface/v1",
)
