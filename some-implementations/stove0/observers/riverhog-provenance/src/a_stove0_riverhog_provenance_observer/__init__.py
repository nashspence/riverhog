"""Exact, subject-bound Riverhog provenance facts for selected Stove0 observers."""

from .facts import extract_core_facts, extract_materialization_hint_fact
from .observer import RiverhogProvenanceObserver

__all__ = [
    "RiverhogProvenanceObserver",
    "extract_core_facts",
    "extract_materialization_hint_fact",
]
