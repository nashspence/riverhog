"""Exact structural recipe interface owned by this observation contract."""

from stove0_observer_protocol.interfaces import subject_interface

from .contracts import MAGIC_CONFORMANCE_VECTORS, MAGIC_OBSERVER_CONTRACT

MAGIC_INTERFACE, MAGIC_INTERFACE_VECTORS = subject_interface(
    contract=MAGIC_OBSERVER_CONTRACT,
    facts_vectors=MAGIC_CONFORMANCE_VECTORS,
    subject_at="/subject_id",
    id="stove0.magic.interface/v1",
)
