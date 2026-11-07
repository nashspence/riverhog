"""Operator-selected storage capacity for complete validation evidence."""

from __future__ import annotations

import os

DEFAULT_VALIDATION_MAX_BYTES = 4 * 1024 * 1024 * 1024


def validation_storage_budget() -> int:
    """Use one positive byte budget for local retention and Actions evidence intake."""
    try:
        budget = int(
            os.environ.get("RIVERHOG_VALIDATION_MAX_BYTES", str(DEFAULT_VALIDATION_MAX_BYTES))
        )
    except ValueError as error:
        raise ValueError("validation evidence storage budget must be a positive integer") from error
    if budget < 1:
        raise ValueError("validation evidence storage budget must be a positive integer")
    return budget
