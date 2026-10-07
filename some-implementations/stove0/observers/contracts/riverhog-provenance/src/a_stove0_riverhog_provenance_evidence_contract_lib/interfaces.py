"""Exact primary member facts and canonical describes relation interface."""

from stove0_observer_protocol.interfaces import subject_interface

from .contracts import CORE_PROVENANCE_CONFORMANCE_VECTORS, CORE_PROVENANCE_OBSERVER_CONTRACT

_DESCRIBES = "https://nashspence.github.io/riverhog/v1/provenance/relations/describes"
_LOOKUP = {"source": "self", "view": "artifacts", "keys": ["/occurrence", "/state"]}

CORE_PROVENANCE_INTERFACE, CORE_PROVENANCE_INTERFACE_VECTORS = subject_interface(
    contract=CORE_PROVENANCE_OBSERVER_CONTRACT,
    facts_vectors=CORE_PROVENANCE_CONFORMANCE_VECTORS,
    subject_at="/subject_id",
    id="stove0.riverhog-provenance.interface/v1",
    partitioning="whole-scope",
    additional_views={
        "describes": {
            "kind": "relation",
            "records": {"records_at": "/artifacts", "nested_records_at": "/claims"},
            "where": {"test": {"path": "/predicate", "op": "eq", "value": _DESCRIBES}},
            "require": {"test": {"path": "/value_type", "op": "eq", "value": "reference"}},
            "primary": {"kind": "exact-endpoint", "at": "/object", "lookup": _LOOKUP},
            "associated": {"kind": "exact-endpoint", "at": "/subject", "lookup": _LOOKUP},
            "coverage": ["subjects"],
            "status": {
                "kind": "records",
                "records_at": "/artifacts",
                "subject_at": "/subject_id",
                "value_at": "/history_extent",
                "values": {"bound-and-required-history": "complete"},
            },
        }
    },
)
