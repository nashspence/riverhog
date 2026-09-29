"""One authoritative set of wire schemas and immutable contract catalogs."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from functools import lru_cache
from importlib.resources import files
from typing import Any

from jsonschema import Draft202012Validator
from referencing import Registry
from referencing.jsonschema import DRAFT202012

from ._vocabulary import ENTRY_SCHEMA, PROFILE
from .binding import ProvenanceContractBinding
from .codec import require_portable_json


@lru_cache(maxsize=1)
def _sealed_core() -> ProvenanceContractBinding:
    documents = []
    for child in sorted(files(__package__).joinpath("schemas").iterdir(), key=lambda p: p.name):
        if child.name.endswith(".json"):
            documents.append(json.loads(child.read_text(encoding="utf-8")))
    return ProvenanceContractBinding(contract_id=PROFILE, schemas=documents)


def core_contract() -> ProvenanceContractBinding:
    return _sealed_core()


def core_schemas() -> dict[str, dict[str, Any]]:
    return core_contract().schemas


def schema_document(schema_id: str = ENTRY_SCHEMA) -> dict[str, Any]:
    try:
        return core_schemas()[schema_id]
    except KeyError as exc:
        raise ValueError(f"unknown core schema: {schema_id}") from exc


class ContractCatalog:
    """No network retrieval, code execution or unpinned schema replacement."""

    def __init__(self, contracts: Iterable[ProvenanceContractBinding] = ()) -> None:
        all_bindings = (core_contract(), *tuple(contracts))
        self._bindings: dict[tuple[str, str], ProvenanceContractBinding] = {}
        schemas: dict[str, dict[str, Any]] = {}
        for binding in all_bindings:
            key = (binding.contract_id, binding.contract_sha256)
            self._bindings[key] = binding
            for schema_id, document in binding.schemas.items():
                if schema_id in schemas and schemas[schema_id] != document:
                    raise ValueError(f"conflicting schema identity: {schema_id}")
                schemas[schema_id] = document
        registry = Registry().with_resources(
            (key, DRAFT202012.create_resource(value)) for key, value in schemas.items()
        )
        self._validators = {
            key: Draft202012Validator(value, registry=registry) for key, value in schemas.items()
        }

    def validate(self, schema_id: str, value: Any) -> None:
        require_portable_json(value)
        if schema_id not in self._validators:
            raise ValueError(f"schema is not present in the sealed catalog: {schema_id}")
        findings = sorted(self._validators[schema_id].iter_errors(value), key=lambda e: str(e.path))
        if findings:
            messages = [f"{list(e.absolute_path)}: {e.message}" for e in findings[:12]]
            raise ValueError("schema validation failed:\n" + "\n".join(messages))

    def validate_profile(self, reference: Mapping[str, str], value: Any) -> bool:
        """False means unverified, never 'valid'; known invalid profiles raise."""
        key = (reference["contract_id"], reference["contract_sha256"])
        binding = self._bindings.get(key)
        if binding is None:
            return False
        schema_id = reference["schema_id"]
        if schema_id not in binding.schemas:
            raise ValueError("profile schema is not in the specified exact contract pack")
        self.validate(schema_id, value)
        return True


def validate_graph_shape(value: Mapping[str, Any]) -> None:
    ContractCatalog().validate(PROFILE + "/graph-fragment.schema.json", dict(value))


def validate_entry_shape(value: Mapping[str, Any]) -> None:
    ContractCatalog().validate(ENTRY_SCHEMA, dict(value))


def profile_reference(schema_id: str) -> dict[str, str]:
    binding = core_contract()
    if schema_id not in binding.schemas:
        raise ValueError("not a core schema")
    return {
        "contract_id": binding.contract_id,
        "contract_sha256": binding.contract_sha256,
        "schema_id": schema_id,
    }
