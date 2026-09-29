"""Structural entry/fragment validation and exact supplied profile catalogs."""

from collections.abc import Mapping
from typing import Any

from riverhog_provenance_contracts import (
    ContractCatalog,
    validate_entry_shape,
    validate_graph_shape,
)


def validate_entry_document(value: Mapping[str, Any]) -> None:
    """Structural validation only; use validate_journal for history constraints."""
    validate_entry_shape(value)


def validate_graph_fragment(value: Mapping[str, Any]) -> None:
    """Structural validation only; use validate_graph for resolved effective graphs."""
    validate_graph_shape(value)


__all__ = ["ContractCatalog", "validate_entry_document", "validate_graph_fragment"]
