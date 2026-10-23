"""Native-valid assertion sources for canonical discovery qualification."""

from __future__ import annotations

from dataclasses import dataclass

from riverhog_protocol import ProfilePin
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    assertion,
    create_journal,
    reference,
)
from riverhog_provenance_contracts import ContractCatalog, ProvenanceContractBinding


@dataclass(frozen=True, slots=True)
class NativeScalarFixture:
    raw: bytes
    catalog: ContractCatalog
    assertion_id: str
    profile: ProfilePin


def native_scalar_fixture(count: int) -> NativeScalarFixture:
    """Put one exact profiled array inside a complete native journal."""
    if count < 0:
        raise ValueError("scalar count must be nonnegative")
    schema_id = "urn:test:canonical-discovery:scalar-array"
    contract = ProvenanceContractBinding(
        contract_id="urn:test:canonical-discovery:scalar-contract",
        schemas=[
            {
                "$id": schema_id,
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "array",
                "items": {"type": "integer"},
            }
        ],
    )
    catalog = ContractCatalog((contract,))
    pin = ProfilePin(
        contract_id=contract.contract_id,
        contract_sha256=contract.contract_sha256,
        schema_id=schema_id,
    )
    observed = BoundedSourceObserver().observe(BytesSource(b"opaque fixture member"))
    extension = assertion(
        "extension",
        observed.observer_agent_id,
        subject=reference(observed.artifact_id, "artifact"),
        property="urn:test:canonical-discovery:scalar-values",
        value={
            "type": "json",
            "value": {"profile": pin.model_dump(mode="json"), "data": list(range(count))},
        },
    )
    graph = observed.graph_fragment()
    graph["extensions"] = [extension]
    return NativeScalarFixture(
        raw=create_journal(
            graph,
            recorded_by_agent_id=observed.observer_agent_id,
            catalog=catalog,
        ),
        catalog=catalog,
        assertion_id=extension["assertion_id"],
        profile=pin,
    )
