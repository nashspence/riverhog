"""Native-valid assertion sources for canonical discovery qualification."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

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
class NativeProfileFixture:
    raw: bytes
    catalog: ContractCatalog
    assertion_id: str
    profile: ProfilePin


def native_profile_fixture(data: Any, schema: dict[str, Any]) -> NativeProfileFixture:
    """Put one exact declared profile value inside a complete native journal."""
    schema_id = "urn:test:canonical-discovery:profile-value"
    contract = ProvenanceContractBinding(
        contract_id="urn:test:canonical-discovery:profile-contract",
        schemas=[
            {
                "$id": schema_id,
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                **schema,
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
        property="urn:test:canonical-discovery:profile-data",
        value={
            "type": "json",
            "value": {"profile": pin.model_dump(mode="json"), "data": data},
        },
    )
    graph = observed.graph_fragment()
    graph["extensions"] = [extension]
    return NativeProfileFixture(
        raw=create_journal(
            graph,
            recorded_by_agent_id=observed.observer_agent_id,
            catalog=catalog,
        ),
        catalog=catalog,
        assertion_id=extension["assertion_id"],
        profile=pin,
    )


def native_scalar_fixture(count: int) -> NativeProfileFixture:
    """Put one exact profiled array inside a complete native journal."""
    if count < 0:
        raise ValueError("scalar count must be nonnegative")
    return native_profile_fixture(
        list(range(count)), {"type": "array", "items": {"type": "integer"}}
    )
