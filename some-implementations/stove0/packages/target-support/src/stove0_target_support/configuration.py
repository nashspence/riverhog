"""Connected deployment configuration shared by maintained target services."""

from __future__ import annotations

import os
from collections.abc import Mapping

from riverhog_client import PrimaryStatePolicy

from stove0_target_support.persistent import DEFAULT_TERMINAL_STATE_RETENTION_SECONDS

TARGET_TERMINAL_STATE_RETENTION_ENV = "STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS"


def terminal_state_retention_seconds(
    environ: Mapping[str, str] | None = None,
) -> int:
    values = os.environ if environ is None else environ
    raw = values.get(
        TARGET_TERMINAL_STATE_RETENTION_ENV,
        str(DEFAULT_TERMINAL_STATE_RETENTION_SECONDS),
    )
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{TARGET_TERMINAL_STATE_RETENTION_ENV} must be an integer") from exc
    if value < 1:
        raise ValueError(f"{TARGET_TERMINAL_STATE_RETENTION_ENV} must be positive")
    return value


__all__ = [
    "TARGET_TERMINAL_STATE_RETENTION_ENV",
    "terminal_state_retention_seconds",
    "publication_state_policy",
]


def publication_state_policy(
    prefix: str, environ: Mapping[str, str] | None = None
) -> PrimaryStatePolicy:
    """Load an explicit stable service/account scope and operator byte budgets."""
    values = os.environ if environ is None else environ
    key = prefix + "_PRIMARY_STATE"
    raw = values.get(key)
    if not raw:
        raise ValueError(f"{key} must declare the publication state scope and byte budgets")
    return PrimaryStatePolicy.model_validate_json(raw)
