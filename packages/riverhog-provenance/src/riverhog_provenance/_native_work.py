"""One volatile work admission shared by an original Native execution quantum."""

from __future__ import annotations

from dataclasses import dataclass

from .errors import JournalValidationCapacity


@dataclass(frozen=True, slots=True)
class WorkAmount:
    rows: int = 0
    octets: int = 0
    source_octets: int = 0
    transitions: int = 0

    def __post_init__(self) -> None:
        if any(type(value) is not int or value < 0 for value in self.coordinates):
            raise ValueError("Native work quantities must be nonnegative integers")

    @property
    def coordinates(self) -> tuple[int, int, int, int]:
        return self.rows, self.octets, self.source_octets, self.transitions

    def __add__(self, other: WorkAmount) -> WorkAmount:
        return WorkAmount(
            *(a + b for a, b in zip(self.coordinates, other.coordinates, strict=True))
        )

    def __sub__(self, other: WorkAmount) -> WorkAmount:
        return WorkAmount(
            *(a - b for a, b in zip(self.coordinates, other.coordinates, strict=True))
        )

    def fits(self, other: WorkAmount) -> bool:
        return all(a <= b for a, b in zip(self.coordinates, other.coordinates, strict=True))


class NativeWorkCapacity(JournalValidationCapacity):
    """The original quantum cannot admit another existing work unit."""


class WorkQuantum:
    """Reserve before work, consume actual work, receive the unused reservation.

    This object belongs to one synchronous executor invocation. Children borrow
    that same object through its original epoch; creating a child or committing
    scratch does not replenish it. Consumption survives a semantic rollback,
    because the inspected or represented work already happened. Reservations
    measure admission rather than actual consumption and cannot become useful
    execution credit or optional-recovery accounting.
    """

    def __init__(self, limits: WorkAmount) -> None:
        if not isinstance(limits, WorkAmount) or not all(limits.coordinates):
            raise ValueError("Native quantum limits must all be positive")
        self._limits = limits
        self._consumed = WorkAmount()
        self._held = WorkAmount()
        self._execution: object | None = None

    def bind_execution(self, original: object) -> None:
        if self._execution is not None:
            raise RuntimeError("the original Native work quantum is single-invocation")
        self._execution = original

    @property
    def limits(self) -> WorkAmount:
        return self._limits

    @property
    def consumed(self) -> WorkAmount:
        return self._consumed

    @property
    def held(self) -> WorkAmount:
        return self._held

    @property
    def remaining(self) -> WorkAmount:
        return self._limits - self._consumed - self._held

    def reserve(self, amount: WorkAmount) -> WorkReservation:
        if not isinstance(amount, WorkAmount) or not any(amount.coordinates):
            raise ValueError("a Native work reservation must admit positive work")
        if not amount.fits(self.remaining):
            raise NativeWorkCapacity("original Native quantum cannot admit the next work unit")
        reservation = WorkReservation(self, amount)
        self._held = self._held + amount
        return reservation

    def consume(self, amount: WorkAmount) -> None:
        """Debit a known exact quantity without retaining a prospective unit."""
        with self.reserve(amount) as reservation:
            reservation.consume(amount)

    def require_received(self) -> None:
        if any(self._held.coordinates):
            raise RuntimeError("Native quantum has unreceived original work reservations")


class WorkReservation:
    """One original unit's remaining admission, received only by that unit."""

    def __init__(self, original: WorkQuantum, amount: WorkAmount) -> None:
        self._original = original
        self._remaining = amount
        self._closed = False

    def consume(self, amount: WorkAmount) -> None:
        if self._closed:
            raise RuntimeError("original Native work reservation is already received")
        if not isinstance(amount, WorkAmount) or not amount.fits(self._remaining):
            raise NativeWorkCapacity("actual Native work exceeds its original reservation")
        original = self._original
        consumed = original._consumed + amount
        held = original._held - amount
        remaining = self._remaining - amount
        original._consumed, original._held, self._remaining = consumed, held, remaining

    def close(self) -> None:
        if not self._closed:
            self._original._held = self._original._held - self._remaining
            self._remaining = WorkAmount()
            self._closed = True

    def __enter__(self) -> WorkReservation:
        if self._closed:
            raise RuntimeError("original Native work reservation is already received")
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()
