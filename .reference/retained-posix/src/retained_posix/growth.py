"""Source-owned reference declarations using the maintained growth vocabulary.

This module is explicit developer evidence; importing the neutral receiver itself
requires no repository implementation or optional instrumentation dependency.
"""
from riverhog_growth_contracts import (
    COUNT, Bound, Declaration, Dimension, Expression, declare_growth,
)


def declaration(operation, limits):
    return Declaration(
        id=f"retained-posix.{operation}.growth/v1",
        authority="retained-posix",
        subjects=(f"retained_posix.growth.{operation}",),
        scope="one call on the same caller-retained process-local resource owner",
        starts_at="explicit original-owner retention and resource admission",
        ends_at="recorded native outcome, including refusal or quarantined uncertainty",
        dimensions=(Dimension("unrelated_files",COUNT,"Unrelated namespace population, not enumerated."),),
        bounds=tuple(
            Bound(name,COUNT,"upper",Expression("constant",value=limit,unit=COUNT),meaning)
            for name,limit,meaning in limits
        ),
        conditions=(
            "GIL-enabled CPython on the selected Linux/Darwin backend; cooperative cancellation only.",
            "No external close/detach/dup2 of a borrowed descriptor; original owners remain retained.",
        ),
        invariants=(
            "A resource has at most one acquisition and one native close dispatch across its lifetime.",
            "No pathname, inode, descriptor absence/reuse, GC or Future state proves receiving.",
        ),
        limitations=(
            "Counts are native backend syscall entries, not kernel path or filesystem IO costs.",
            "Path validation is linear in already-admitted encoded path length.",
            "No wall-clock bound or aggregate process/allocator memory claim.",
        ),
    )


@declare_growth(declaration("acquire",(
    ("open_calls",1,"At most one openat entry, no implicit retry."),
    ("dup_calls",0,"Path acquisition does not duplicate descriptors."),
    ("fstat_calls",1,"One type validation after acquisition."),
    ("close_calls",0,"No constructor/finalizer cleanup hides a close result."),
)))
def acquire(owner,path,**options):
    return owner.acquire(path,**options)


@declare_growth(declaration("receive",(
    ("open_calls",0,"Receiving cannot reopen a path."),
    ("dup_calls",0,"Receiving cannot duplicate another handle."),
    ("fstat_calls",0,"No reuse or inode probe decides receiving."),
    ("close_calls",1,"At most one dispatch, zero after success or unknown."),
)))
def receive(owner):
    return owner.close()
