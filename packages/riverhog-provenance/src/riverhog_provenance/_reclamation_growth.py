"""Native file-format ownership composed with neutral physical extent disposal."""

from __future__ import annotations

import os

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    growth_binding,
)
from workspace_reclamation import reclaim_retired_files


def reclamation_growth(
    *,
    identity: str,
    subject: str,
    names: tuple[str, ...],
) -> Declaration:
    encoded = tuple(os.fsencode(name) for name in names)
    arguments = (
        ("candidate_files", Expression("constant", value=len(names), unit=COUNT)),
        ("name_octets", Expression("constant", value=sum(map(len, encoded)), unit=OCTETS)),
        ("maximum_name_octets", Expression("constant", value=max(map(len, encoded)), unit=OCTETS)),
        *(
            (name, Expression("dimension", name=name))
            for name in (
                "directory_octets",
                "retained_files",
                "retained_octets",
                "octet_budget",
            )
        ),
    )
    child = growth_binding(reclaim_retired_files).declaration
    return Declaration(
        id=identity,
        authority="riverhog-provenance",
        subjects=(subject,),
        scope="native-posix-retired-sqlite-workspace-file-extent-disposal",
        starts_at="receive one durably retired owned workspace under its exclusive kernel lease",
        ends_at="synchronize one admitted extent removal and report remaining owned files",
        dimensions=(
            Dimension("directory_octets", OCTETS, "Complete encoded original directory path."),
            Dimension(
                "retained_octets", OCTETS, "Logical extents of the remaining owned data files."
            ),
            Dimension("retained_files", COUNT, "Existing files among the four SQLite data names."),
            Dimension("octet_budget", OCTETS, "Explicit maximum logical extent removed this unit."),
        ),
        bounds=tuple(
            Bound(
                bound.metric,
                bound.unit,
                bound.relation,
                Expression(
                    "delegate",
                    name="files",
                    metric=bound.metric,
                    terms=(Expression("constant", value=1, unit=COUNT),),
                    arguments=arguments,
                ),
                bound.meaning,
            )
            for bound in child.bounds
        ),
        conditions=(
            "The caller durably retired this exact job and excludes its writer and every reader.",
            "Private POSIX directory/file custody and the four current SQLite names are preserved.",
        ),
        invariants=(
            "Archive and retained source authority remain outside this disposable projection.",
            "Each confirmed unit removes at most its admitted extent without reconstructing bytes.",
            "The lease, directory and unrelated files remain with the application owner.",
        ),
        limitations=(
            "Syscall counts exclude kernel index work, physical I/O, wait time and CPU.",
            "Extent octets describe logical file ranges rather than physically allocated blocks.",
            "Retirement, kernel leases and final directory/catalog removal are separate.",
        ),
    )
