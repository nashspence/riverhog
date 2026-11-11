"""Source-owned native resource declarations using the maintained growth vocabulary.

Native counters record actual backend entries and complete admitted control widths.
"""

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)

from . import Resource


def declaration(operation: str, limits: tuple[tuple[str, int, str], ...]) -> Declaration:
    return Declaration(
        id=f"retained-posix.{operation}.growth/v1",
        authority="retained-posix",
        subjects=(f"retained_posix.growth.{operation}",),
        scope="one call on the same caller-retained process-local resource owner",
        starts_at="explicit original-owner retention and resource admission",
        ends_at="recorded native outcome, including refusal or quarantined uncertainty",
        dimensions=(
            Dimension("unrelated_files", COUNT, "Unrelated namespace population, not enumerated."),
            Dimension("path_octets", OCTETS, "Complete already-encoded admitted path width."),
        ),
        bounds=tuple(
            Bound(name, COUNT, "upper", Expression("constant", value=limit, unit=COUNT), meaning)
            for name, limit, meaning in limits
        )
        + (
            Bound(
                "control_octets",
                OCTETS,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        Expression(
                            "product",
                            terms=(
                                Expression("constant", value=4),
                                Expression("dimension", name="path_octets"),
                            ),
                        ),
                        Expression(
                            "constant", value=3 if operation == "acquire" else 0, unit=OCTETS
                        ),
                    ),
                )
                if operation == "acquire"
                else Expression("constant", value=0, unit=OCTETS),
                "Complete operands of every actual native path scan/comparison.",
            ),
        ),
        conditions=(
            "GIL-enabled Linux/Darwin CPython with cooperative cancellation.",
            "Owners stay retained; borrowed descriptors are never replaced or closed.",
        ),
        invariants=(
            "Each resource lifetime has at most one acquisition and one native close dispatch.",
            "No pathname, inode, descriptor absence/reuse, GC or Future state proves receiving.",
        ),
        limitations=(
            "Counts are native backend syscall entries, not kernel path or filesystem IO costs.",
            "Path validation is linear in already-admitted encoded path length.",
            "No wall-clock bound or aggregate process/allocator memory claim.",
        ),
    )


@declare_growth(
    declaration(
        "acquire",
        (
            ("open_calls", 1, "At most one openat entry, no implicit retry."),
            ("dup_calls", 0, "Path acquisition does not duplicate descriptors."),
            ("fstat_calls", 1, "One type validation after acquisition."),
            ("close_calls", 0, "No constructor/finalizer cleanup hides a close result."),
        ),
    )
)
def acquire(
    owner: Resource,
    path: bytes,
    *,
    writable: bool = False,
    parent: Resource | None = None,
    create: bool = False,
    exclusive: bool = False,
) -> None:
    owner.acquire(path, writable=writable, parent=parent, create=create, exclusive=exclusive)


@declare_growth(
    declaration(
        "receive",
        (
            ("open_calls", 0, "Receiving cannot reopen a path."),
            ("dup_calls", 0, "Receiving cannot duplicate another handle."),
            ("fstat_calls", 0, "No reuse or inode probe decides receiving."),
            ("close_calls", 1, "At most one dispatch, zero after success or unknown."),
        ),
    )
)
def receive(owner: Resource) -> None:
    owner.close()
