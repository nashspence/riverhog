from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import TypeAdapter, ValidationError
from time_formats import (
    CanonicalUtcTimestamp,
    add_utc_timestamp,
    format_utc_ns,
    format_utc_timestamp,
    normalize_utc_timestamp,
    parse_utc_timestamp,
    require_canonical_utc_timestamp,
)


def test_utc_timestamp_has_fixed_nanosecond_width() -> None:
    assert (
        format_utc_timestamp(datetime(2026, 7, 15, 16, 2, 3, tzinfo=UTC))
        == "2026-07-15T16:02:03.000000000Z"
    )


def test_utc_timestamp_normalizes_offsets() -> None:
    eastern = timezone(-timedelta(hours=4))
    value = datetime(2026, 7, 15, 12, 2, 3, 456789, tzinfo=eastern)

    assert format_utc_timestamp(value) == "2026-07-15T16:02:03.456789000Z"


def test_parse_utc_timestamp_keeps_all_nine_digits() -> None:
    assert parse_utc_timestamp("2026-07-15T12:02:03.456789123-04:00") == (
        parse_utc_timestamp("2026-07-15T16:02:03Z") + 456_789_123
    )
    assert normalize_utc_timestamp("2026-07-15t12:02:03.456789123-04:00") == (
        "2026-07-15T16:02:03.456789123Z"
    )
    assert format_utc_ns(-1) == "1969-12-31T23:59:59.999999999Z"


def test_canonical_timestamp_validates_calendar_and_does_not_rewrite() -> None:
    adapter = TypeAdapter(CanonicalUtcTimestamp)
    value = "2026-07-15T16:02:03.456789123Z"
    assert adapter.validate_python(value) == value
    assert require_canonical_utc_timestamp(value) == value
    assert adapter.json_schema()["pattern"].endswith(r"\.[0-9]{9}Z$")
    for invalid in (
        "2026-07-15T16:02:03.456789Z",
        "2026-07-15T16:02:03.456789123+00:00",
        "2026-07-15T16:02:03.456789123z",
        "2026-02-30T16:02:03.456789123Z",
        "2026-07-15T16:02:60.456789123Z",
    ):
        with pytest.raises(ValidationError):
            adapter.validate_python(invalid)


def test_utc_timestamp_arithmetic_preserves_nanoseconds_and_order() -> None:
    first = "2026-07-15T16:02:03.000000001Z"
    second = add_utc_timestamp(first, timedelta(microseconds=1))
    assert second == "2026-07-15T16:02:03.000001001Z"
    assert first < second
    assert parse_utc_timestamp(second) - parse_utc_timestamp(first) == 1_000


def test_utc_timestamp_requires_timezone_context() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        format_utc_timestamp(datetime(2026, 7, 15, 16, 2, 3))

    with pytest.raises(ValueError, match="timezone offset"):
        parse_utc_timestamp("2026-07-15T16:02:03.000000000")


@pytest.mark.parametrize(
    "value",
    [
        "2026-07-15T16:02:03.1234567890Z",
        "2026-07-15T16:02:03-00:00",
        "2026-07-15T16:02:60Z",
        "2026-02-30T16:02:03Z",
        "2026-07-15T16:02:03+24:00",
        " 2026-07-15T16:02:03Z",
    ],
)
def test_parse_utc_timestamp_rejects_unknown_or_unrepresentable_input(value: str) -> None:
    with pytest.raises(ValueError):
        parse_utc_timestamp(value)
