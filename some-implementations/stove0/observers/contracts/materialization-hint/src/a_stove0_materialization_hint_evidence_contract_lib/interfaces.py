"""Exact structural recipe interface owned by this observation contract."""

from stove0_observer_protocol.interfaces import subject_interface

from .contracts import (
    MATERIALIZATION_HINT_CONFORMANCE_VECTORS,
    MATERIALIZATION_HINT_OBSERVER_CONTRACT,
)

MATERIALIZATION_HINT_INTERFACE, MATERIALIZATION_HINT_INTERFACE_VECTORS = subject_interface(
    contract=MATERIALIZATION_HINT_OBSERVER_CONTRACT,
    facts_vectors=MATERIALIZATION_HINT_CONFORMANCE_VECTORS,
    subject_at="/subject_id",
    id="stove0.riverhog-materialization-hint.interface/v1",
)
