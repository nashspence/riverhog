from __future__ import annotations

from collections.abc import Generator, Iterable, Iterator, Sequence
from dataclasses import dataclass

from riverhog_core.ports.archive_objects import ResumableWriteConstraints


@dataclass(frozen=True, slots=True)
class WriteSegmentPlan:
    """An operational byte range over an already-authoritative opaque object."""

    number: int
    archive_part_number: int
    archive_part_offset: int
    stored_bytes: int


def plan_write_segments(
    archive_part_bytes: Sequence[int],
    constraints: ResumableWriteConstraints,
) -> tuple[WriteSegmentPlan, ...]:
    """Map immutable archive parts to adapter-compatible write segments.

    The mapping never changes archive bytes or boundaries. It only subdivides an
    archive part when the adapter cannot accept that part as one write segment.
    """

    if not archive_part_bytes or any(size < 1 for size in archive_part_bytes):
        raise ValueError("archive parts must contain positive byte counts")
    minimum = constraints.minimum_nonfinal_segment_bytes
    maximum = constraints.maximum_segment_bytes
    if minimum < 1 or (maximum is not None and maximum < minimum):
        raise ValueError("adapter write constraints are invalid")

    return tuple(iter_write_segments(archive_part_bytes, constraints))


def iter_write_segments(
    archive_part_bytes: Iterable[int],
    constraints: ResumableWriteConstraints,
) -> Iterator[WriteSegmentPlan]:
    """Yield the adapter mapping without retaining the object's segment history."""

    minimum = constraints.minimum_nonfinal_segment_bytes
    maximum = constraints.maximum_segment_bytes
    if minimum < 1 or (maximum is not None and maximum < minimum):
        raise ValueError("adapter write constraints are invalid")

    parts = iter(archive_part_bytes)
    try:
        current = next(parts)
    except StopIteration as exc:
        raise ValueError("archive parts must contain positive byte counts") from exc
    archive_index = 1
    segment_number = 0
    for following in parts:
        if current < 1:
            raise ValueError("archive parts must contain positive byte counts")
        segment_number = yield from _iter_archive_part_segments(
            current,
            archive_part_number=archive_index,
            segment_number=segment_number,
            minimum=minimum,
            maximum=maximum,
            object_final=False,
            maximum_segment_count=constraints.maximum_segment_count,
        )
        archive_index += 1
        current = following
    if current < 1:
        raise ValueError("archive parts must contain positive byte counts")
    yield from _iter_archive_part_segments(
        current,
        archive_part_number=archive_index,
        segment_number=segment_number,
        minimum=minimum,
        maximum=maximum,
        object_final=True,
        maximum_segment_count=constraints.maximum_segment_count,
    )


def _iter_archive_part_segments(
    total: int,
    *,
    archive_part_number: int,
    segment_number: int,
    minimum: int,
    maximum: int | None,
    object_final: bool,
    maximum_segment_count: int | None,
) -> Generator[WriteSegmentPlan, None, int]:
    offset = 0
    for size in _segment_sizes(
        total,
        minimum=minimum,
        maximum=maximum,
        object_final=object_final,
    ):
        segment_number += 1
        if maximum_segment_count is not None and segment_number > maximum_segment_count:
            raise ValueError("authoritative object exceeds the adapter write-segment count")
        yield WriteSegmentPlan(
            number=segment_number,
            archive_part_number=archive_part_number,
            archive_part_offset=offset,
            stored_bytes=size,
        )
        offset += size
    return segment_number


def _segment_sizes(
    total: int,
    *,
    minimum: int,
    maximum: int | None,
    object_final: bool,
) -> Iterator[int]:
    if maximum is None or total <= maximum:
        if not object_final and total < minimum:
            raise ValueError(
                "authoritative object cannot satisfy adapter write-segment constraints"
            )
        yield total
        return

    count = (total + maximum - 1) // maximum
    final_minimum = 1 if object_final else minimum
    required = (count - 1) * minimum + final_minimum
    if total < required:
        raise ValueError("authoritative object cannot satisfy adapter write-segment constraints")
    remaining = total - required
    for index in range(count):
        size = final_minimum if index == count - 1 else minimum
        accepted = min(maximum - size, remaining)
        size += accepted
        remaining -= accepted
        yield size
    if remaining:
        raise ValueError("authoritative object cannot satisfy adapter write-segment constraints")


__all__ = ["WriteSegmentPlan", "iter_write_segments", "plan_write_segments"]
