"""Complete filename preference tiers over exact accepted provenance inputs."""

from a_stove0_riverhog_provenance_evidence_contract_lib.interfaces import CORE_PROVENANCE_INTERFACE
from stove0_observer_protocol.interfaces import seal_owned_interface
from stove0_protocol.observation_interfaces import ObservationInterfaceVector

from .contracts import FILENAME_CONFORMANCE_VECTORS, FILENAME_OBSERVER_CONTRACT

_STATUS = {
    "kind": "records",
    "records_at": "/statuses",
    "subject_at": "/subject_id",
    "value_at": "/status",
    "values": {
        "usable": "complete",
        "no-locator": "complete",
        "insufficient": "insufficient",
        "unsupported": "unsupported",
        "ambiguous": "ambiguous",
    },
}
_VECTORS = tuple(
    ObservationInterfaceVector(
        id=vector.id,
        accepted=vector.accepted,
        subjects=vector.subjects,
        options=vector.options,
        facts=vector.facts,
    )
    for vector in FILENAME_CONFORMANCE_VECTORS.vectors
)
_VECTORS += (
    ObservationInterfaceVector(
        id="accepted-complete-empty",
        accepted=True,
        subjects=(),
        options={"primary_ids": [], "sidecar_ids": []},
        facts=None,
    ),
)

FILENAME_INTERFACE, FILENAME_INTERFACE_VECTORS = seal_owned_interface(
    contract=FILENAME_OBSERVER_CONTRACT,
    id="stove0.filename-prefix-sidecars.interface/v1",
    partitioning="whole-scope",
    empty_scope="complete-empty",
    vectors=_VECTORS,
    inputs={
        "primary": {"kind": "subjects", "option_ids_at": "/primary_ids"},
        "sidecar": {"kind": "subjects", "option_ids_at": "/sidecar_ids"},
        "provenance": {
            "kind": "evidence",
            "contracts": [CORE_PROVENANCE_INTERFACE.observer_contract],
            "interfaces": [CORE_PROVENANCE_INTERFACE.ref],
            "covers": ["primary", "sidecar"],
            "option_slots_at": "/provenance_slots",
        },
    },
    views={
        tier: {
            "kind": "relation",
            "records": {"records_at": "/candidates"},
            "where": {"test": {"path": "/rule", "op": "eq", "value": rule}},
            "require": True,
            "primary": {"kind": "subject-id", "at": "/primary_id"},
            "associated": {"kind": "subject-id", "at": "/sidecar_id"},
            "coverage": ["primary", "sidecar"],
            "status": _STATUS,
        }
        for tier, rule in (("full_leaf", "full-leaf"), ("stem", "stem"))
    },
)
