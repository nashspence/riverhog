"""Deployment policy for disposable observer terminal state."""

import os
from collections.abc import Mapping

DEFAULT_TERMINAL_STATE_RETENTION_SECONDS = 30 * 24 * 60 * 60
OBSERVER_TERMINAL_STATE_RETENTION_ENV = "STOVE0_OBSERVER_TERMINAL_STATE_RETENTION_SECONDS"


def terminal_state_retention_seconds(environment: Mapping[str, str] | None = None) -> int:
    values = os.environ if environment is None else environment
    raw = values.get(
        OBSERVER_TERMINAL_STATE_RETENTION_ENV, str(DEFAULT_TERMINAL_STATE_RETENTION_SECONDS)
    )
    try:
        value = int(raw)
    except ValueError:
        raise ValueError("observer terminal-state retention must be a positive integer") from None
    if value < 1:
        raise ValueError("observer terminal-state retention must be positive")
    return value
