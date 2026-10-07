"""A shared finite allowance for related synchronous HTTP control calls."""

from __future__ import annotations

import math
import time
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

_DEADLINE: ContextVar[float | None] = ContextVar("http_control_deadline", default=None)


class ControlBudgetExhausted(TimeoutError):
    """The caller must save its continuation and return before another contact."""


def finite_control_seconds(value: float) -> float:
    if isinstance(value, bool) or not math.isfinite(value) or value <= 0:
        raise ValueError("HTTP control allowance must be finite and positive")
    return value


def control_timeout(maximum: float = 5.0) -> float:
    maximum = finite_control_seconds(maximum)
    deadline = _DEADLINE.get()
    if deadline is None:
        return maximum
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ControlBudgetExhausted("HTTP control allowance is exhausted")
    return min(maximum, remaining)


def check_control_budget() -> None:
    control_timeout()


@contextmanager
def control_budget(seconds: float = 5.0) -> Iterator[None]:
    deadline = time.monotonic() + finite_control_seconds(seconds)
    enclosing = _DEADLINE.get()
    token = _DEADLINE.set(deadline if enclosing is None else min(deadline, enclosing))
    try:
        yield
    finally:
        _DEADLINE.reset(token)
