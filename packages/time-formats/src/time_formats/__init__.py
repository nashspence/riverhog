"""Riverhog UTC timestamp encoding and exact nanosecond arithmetic.

Canonical timestamps use a fixed nine-digit fractional second. This keeps
lexical ordering equal to instant ordering in string-backed catalog columns.
Resolution or uncertainty of an observation belongs in separate evidence.
"""

from __future__ import annotations

import re
import time
from datetime import UTC, datetime, timedelta, timezone
from typing import Annotated

from pydantic import AfterValidator, StringConstraints

_DURATION_RE = re.compile(r"^(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$")
_RFC3339_RE = re.compile(
    r"^(?P<year>[0-9]{4})-(?P<month>[0-9]{2})-(?P<day>[0-9]{2})"
    r"[Tt](?P<hour>[0-9]{2}):(?P<minute>[0-9]{2}):(?P<second>[0-9]{2})"
    r"(?:\.(?P<fraction>[0-9]{1,9}))?"
    r"(?P<offset>[Zz]|[+-][0-9]{2}:[0-9]{2})$"
)
CANONICAL_UTC_TIMESTAMP_PATTERN = (
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{9}Z$"
)
_CANONICAL_RE = re.compile(CANONICAL_UTC_TIMESTAMP_PATTERN)
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_NS_PER_SECOND = 1_000_000_000


def parse_duration(value: str) -> timedelta:
    match = _DURATION_RE.match(value.strip())
    if not match or not any(match.groups()):
        raise ValueError(
            f"invalid duration {value!r}: expected format like '2d', '24h', '30m', '90s'"
        )
    days, hours, minutes, seconds = (int(item or 0) for item in match.groups())
    return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)


def utc_now() -> datetime:
    return datetime.now(UTC)


def utc_epoch_ns_now() -> int:
    return time.time_ns()


def epoch_ns_from_datetime(value: datetime) -> int:
    """Convert a timezone-aware datetime without inventing submicrosecond digits."""

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("UTC timestamps require timezone-aware datetimes")
    delta = value.astimezone(UTC) - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * _NS_PER_SECOND + delta.microseconds * 1_000


def format_utc_ns(epoch_ns: int) -> str:
    """Serialize one Unix nanosecond instant in sortable UTC form."""

    if isinstance(epoch_ns, bool) or not isinstance(epoch_ns, int):
        raise TypeError("epoch_ns must be an integer")
    seconds, nanoseconds = divmod(epoch_ns, _NS_PER_SECOND)
    moment = _EPOCH + timedelta(seconds=seconds)
    return (
        f"{moment.year:04d}-{moment.month:02d}-{moment.day:02d}"
        f"T{moment.hour:02d}:{moment.minute:02d}:{moment.second:02d}"
        f".{nanoseconds:09d}Z"
    )


def format_utc_timestamp(value: datetime) -> str:
    return format_utc_ns(epoch_ns_from_datetime(value))


def parse_utc_timestamp(value: str) -> int:
    """Parse RFC 3339 UTC/offset input to Unix nanoseconds without rounding.

    Leap seconds cannot be represented by the Unix instant model and are
    rejected. RFC 3339's -00:00 means an unknown local offset, not UTC.
    """

    if not isinstance(value, str) or (match := _RFC3339_RE.fullmatch(value)) is None:
        raise ValueError("timestamp must be RFC 3339 with a timezone offset")
    offset_text = match.group("offset")
    if offset_text == "-00:00":
        raise ValueError("timestamp has an unknown local offset")
    if offset_text.lower() == "z":
        tz = UTC
    else:
        hours = int(offset_text[1:3])
        minutes = int(offset_text[4:6])
        if hours > 23 or minutes > 59:
            raise ValueError("timestamp has an invalid timezone offset")
        offset = timedelta(hours=hours, minutes=minutes)
        tz = timezone(offset if offset_text[0] == "+" else -offset)
    try:
        local = datetime(
            int(match.group("year")),
            int(match.group("month")),
            int(match.group("day")),
            int(match.group("hour")),
            int(match.group("minute")),
            int(match.group("second")),
            tzinfo=tz,
        )
        delta = local.astimezone(UTC) - _EPOCH
    except (OverflowError, ValueError) as exc:
        raise ValueError("timestamp has an invalid calendar date or time") from exc
    fraction = (match.group("fraction") or "").ljust(9, "0")
    return (delta.days * 86_400 + delta.seconds) * _NS_PER_SECOND + int(fraction or "0")


def datetime_from_utc_timestamp(value: str) -> datetime:
    """Return a datetime only when conversion would retain every nanosecond."""

    epoch_ns = parse_utc_timestamp(value)
    seconds, nanoseconds = divmod(epoch_ns, _NS_PER_SECOND)
    if nanoseconds % 1_000:
        raise ValueError("timestamp has nanoseconds that datetime cannot represent")
    return _EPOCH + timedelta(seconds=seconds, microseconds=nanoseconds // 1_000)


def normalize_utc_timestamp(value: str) -> str:
    return format_utc_ns(parse_utc_timestamp(value))


def add_utc_timestamp(value: str, duration: timedelta) -> str:
    """Add an exact timedelta without dropping a timestamp's nanosecond tail."""

    duration_ns = (
        duration.days * 86_400 + duration.seconds
    ) * _NS_PER_SECOND + duration.microseconds * 1_000
    return format_utc_ns(parse_utc_timestamp(value) + duration_ns)


def require_canonical_utc_timestamp(value: str) -> str:
    if not isinstance(value, str) or _CANONICAL_RE.fullmatch(value) is None:
        raise ValueError("timestamp must use fixed nine-digit UTC representation")
    parse_utc_timestamp(value)
    return value


CanonicalUtcTimestamp = Annotated[
    str,
    StringConstraints(
        min_length=30,
        max_length=30,
        pattern=CANONICAL_UTC_TIMESTAMP_PATTERN,
    ),
    AfterValidator(require_canonical_utc_timestamp),
]


def utc_timestamp_now() -> str:
    return format_utc_ns(utc_epoch_ns_now())


__all__ = [
    "CANONICAL_UTC_TIMESTAMP_PATTERN",
    "CanonicalUtcTimestamp",
    "add_utc_timestamp",
    "datetime_from_utc_timestamp",
    "epoch_ns_from_datetime",
    "format_utc_ns",
    "format_utc_timestamp",
    "normalize_utc_timestamp",
    "parse_duration",
    "parse_utc_timestamp",
    "require_canonical_utc_timestamp",
    "utc_epoch_ns_now",
    "utc_now",
    "utc_timestamp_now",
]
