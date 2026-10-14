"""Exact structural recipe interface owned by this observation contract."""

from stove0_observer_protocol.interfaces import subject_interface

from .contracts import MEDIA_METADATA_FACTS_CONFORMANCE_VECTORS, MEDIA_METADATA_OBSERVER_CONTRACT

MEDIA_METADATA_INTERFACE, MEDIA_METADATA_INTERFACE_VECTORS = subject_interface(
    contract=MEDIA_METADATA_OBSERVER_CONTRACT,
    facts_vectors=MEDIA_METADATA_FACTS_CONFORMANCE_VECTORS,
    subject_at="/artifact_id",
    id="stove0.media.metadata.interface/v1",
    status={
        "kind": "records",
        "records_at": "/artifacts",
        "subject_at": "/artifact_id",
        "value_at": "/state",
        "values": {"observed": "complete", "unsupported": "unsupported"},
    },
)
