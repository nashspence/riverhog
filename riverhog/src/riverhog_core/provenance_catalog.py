"""Exact installed provenance schema packs for strict construction admission."""

from __future__ import annotations

from functools import lru_cache
from importlib.metadata import entry_points

from riverhog_provenance_contracts import (
    PROVENANCE_CONTRACT_ENTRY_POINT_GROUP,
    ContractCatalog,
    ProvenanceContractBinding,
)


@lru_cache(maxsize=1)
def admission_provenance_catalog() -> ContractCatalog:
    installed = sorted(
        entry_points(group=PROVENANCE_CONTRACT_ENTRY_POINT_GROUP),
        key=lambda value: value.name,
    )
    if len({item.name for item in installed}) != len(installed):
        raise RuntimeError("installed provenance contract providers reuse a name")
    bindings = []
    for item in installed:
        binding = item.load()
        if not isinstance(binding, ProvenanceContractBinding):
            raise RuntimeError(f"invalid installed provenance contract provider: {item.name}")
        bindings.append(binding)
    return ContractCatalog(bindings)


__all__ = ["admission_provenance_catalog"]
