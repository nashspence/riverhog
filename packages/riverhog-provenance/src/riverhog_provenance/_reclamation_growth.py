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
        scope="native-posix-retired-sqlite-file-set-extent-disposal",
        starts_at="receive one durably retired owned file set under its exclusive kernel lease",
        ends_at="synchronize one admitted extent removal and report remaining owned files",
        dimensions=(
            Dimension("directory_octets", OCTETS, "Complete encoded original directory path."),
            Dimension(
                "retained_octets", OCTETS, "Logical extents of the remaining owned data files."
            ),
            Dimension(
                "retained_files",
                COUNT,
                "Existing files among the selected known SQLite data names.",
            ),
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
            (
                "The caller durably retired this exact file set and excludes "
                "its writers and every reader."
            ),
            (
                "Private POSIX directory/file custody and the selected current "
                "SQLite names are preserved."
            ),
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


def reclamation_receiving_growth() -> Declaration:
    return Declaration(
        id="riverhog-provenance.generation-reclamation-receiving.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance.journal_reclamation.received_generation_reclamation",),
        scope="interpret one original synchronized file receipt in the Native generation namespace",
        starts_at="inspect the caller-retained neutral original and exact expected directory pin",
        ends_at="return its received progress, pending absence, or an exact identity rejection",
        dimensions=(
            Dimension("unrelated_files", COUNT, "Independent namespace entries, never probed."),
            Dimension("retained_octets", OCTETS, "Independent retained file extents, never read."),
        ),
        bounds=(
            *(
                Bound(
                    metric,
                    COUNT,
                    "upper",
                    Expression("constant", value=1, unit=COUNT),
                    "At most one original receipt read and each fixed identity/namespace check.",
                )
                for metric in ("receipt_reads", "directory_checks", "namespace_checks")
            ),
            Bound(
                "storage_calls",
                COUNT,
                "equal",
                Expression("constant", value=0, unit=COUNT),
                "Receiving interpretation opens, probes, mutates and synchronizes no storage.",
            ),
            Bound(
                "payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "No file or database payload is read or reconstructed.",
            ),
        ),
        conditions=(
            "The receiver is the same neutral original retained before physical effects.",
            "Its immutable receipt contains that original's validated builtin encoded names.",
            "The expected device/inode pair comes from the caller's retained original custody.",
        ),
        invariants=(
            "An unused, active or failed-sync original cannot manufacture synchronized work.",
            "Only the fixed four generation filenames can supply generation-only receiving.",
            "A fixed pair and four fixed names bound control comparisons independently of files.",
            "Progress still distinguishes a partial extent from complete file-set reclamation.",
            "This observation grants no nonpublication, worker join or accounting settlement.",
        ),
        limitations=(
            "Actual physical work, original worker join and durable domain receiving are separate.",
            "Logical checks are not an instruction-count or machine elapsed-time guarantee.",
        ),
    )
