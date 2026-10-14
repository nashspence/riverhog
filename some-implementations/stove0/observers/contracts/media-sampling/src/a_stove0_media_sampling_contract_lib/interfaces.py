"""Exact structural recipe interface owned by this observation contract."""

from stove0_observer_protocol.interfaces import subject_interface

from .contracts import MEDIA_SAMPLING_FACTS_CONFORMANCE_VECTORS, MEDIA_SAMPLING_OBSERVER_CONTRACT

MEDIA_SAMPLING_INTERFACE, MEDIA_SAMPLING_INTERFACE_VECTORS = subject_interface(
    contract=MEDIA_SAMPLING_OBSERVER_CONTRACT,
    facts_vectors=MEDIA_SAMPLING_FACTS_CONFORMANCE_VECTORS,
    subject_at="/artifact_id",
    id="stove0.media.sampling.interface/v1",
)
