"""Selected byte-only libmagic observation contract."""

from .contracts import (
    MAGIC_OBSERVATION_ID,
    MAGIC_OBSERVER_CONTRACT,
    MAGIC_SEMANTIC_VALIDATOR,
    MagicFacts,
    validate_magic_facts,
)

__all__ = [
    "MAGIC_OBSERVATION_ID",
    "MAGIC_OBSERVER_CONTRACT",
    "MAGIC_SEMANTIC_VALIDATOR",
    "MagicFacts",
    "validate_magic_facts",
]
