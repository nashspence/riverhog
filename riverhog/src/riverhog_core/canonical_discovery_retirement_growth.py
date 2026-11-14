"""Inclusive primitive bounds for one retained original discovery retirement turn."""

from collections.abc import Callable
from typing import Any

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)


def declare_retirement_growth(operation: Callable[..., Any]) -> None:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dim(name: str) -> Expression:
        return Expression("dimension", name=name)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    def logarithmic(base: int, step: int, name: str) -> Expression:
        return Expression(
            "sum",
            terms=(
                count(base),
                Expression(
                    "product",
                    terms=(
                        count(step),
                        Expression(
                            "log2_ceil",
                            terms=(Expression("sum", terms=(count(2), dim(name))),),
                        ),
                    ),
                ),
            ),
        )

    # Three turn fences, two opening fences and at most twelve child fences
    # plus its phase CAS. Inclusive metrics also receive the Native/neutral
    # extent and all context exits; the helpers are not subtracted or waived.
    limits = {
        "core_sql_statements": 64,
        "core_returned_rows": 64,
        "core_changed_rows": 1,
        "core_opened_connections": 1,
        "core_closed_connections": 1,
        "catalog_sql_statements": 40,
        "catalog_driver_statements": 700,
        "catalog_returned_rows": 320,
        "catalog_changed_rows": 2,
        "uuid_values": 1,
        "open_calls": 16,
        "close_calls": 16,
        "dup_calls": 0,
        "stat_calls": 229,
        "uid_calls": 96,
        "flock_calls": 8,
        "fsync_calls": 10,
        "unlink_calls": 2,
        "rmdir_calls": 1,
        "truncate_calls": 1,
        "mkdir_calls": 0,
        "directory_enumerations": 0,
        "json_decodes": 6,
        "json_encodes": 6,
    }
    declare_growth(
        Declaration(
            id="riverhog-server.discovery-retirement-turn.growth/v1",
            authority="riverhog-server",
            subjects=(
                "riverhog_core.canonical_discovery_retirement.DiscoveryRetirementTurn.advance",
            ),
            scope="one complete original discovery phase or byte extent including actual close",
            starts_at="receive a fresh retained turn and positive byte admission",
            ends_at="receive every original handle before a result or retain an explicit failure",
            dimensions=(
                Dimension("retained_core_rows", COUNT, "Independent retained Core/child rows."),
                Dimension("retained_catalog_rows", COUNT, "Independent retained catalog rows."),
                Dimension(
                    "control_octets",
                    OCTETS,
                    "Complete original job, header, binding and backend/mutation control widths.",
                ),
                Dimension(
                    "path_octets", OCTETS, "Complete encoded original parent and child paths."
                ),
                Dimension(
                    "octet_budget", OCTETS, "Positive admitted disposal bytes for this turn."
                ),
            ),
            bounds=(
                *(
                    Bound(
                        metric,
                        COUNT,
                        "upper" if value else "equal",
                        count(value),
                        "Inclusive turn, opening, one child/corpus/root/metadata/parent owner "
                        "and all actual Native/neutral/descriptor receiving primitives.",
                    )
                    for metric, value in limits.items()
                ),
                Bound(
                    "core_vm_instructions",
                    COUNT,
                    "upper",
                    logarithmic(65536, 8192, "retained_core_rows"),
                    "Fixed Core schema/header controls and indexed original child work.",
                ),
                Bound(
                    "catalog_vm_instructions",
                    COUNT,
                    "upper",
                    logarithmic(655360, 40960, "retained_catalog_rows"),
                    "At most eighteen indexed original fences, phase CAS "
                    "and final catalog outcome.",
                ),
                *(
                    Bound(
                        metric, OCTETS, "upper", scale(multiplier, dim("control_octets")), meaning
                    )
                    for metric, multiplier, meaning in (
                        ("core_returned_octets", 64, "Complete original Core controls."),
                        ("core_parameter_octets", 128, "Complete original Core predicates."),
                        ("catalog_returned_octets", 320, "Complete selected catalog controls."),
                        ("catalog_parameter_octets", 1400, "Complete predicates and mutations."),
                        ("json_decoded_octets", 6, "Complete selected original child bindings."),
                        ("json_encoded_octets", 6, "Complete selected original child bindings."),
                    )
                ),
                Bound(
                    "encoded_path_octets",
                    OCTETS,
                    "upper",
                    Expression(
                        "sum",
                        terms=(
                            scale(96, dim("path_octets")),
                            Expression("constant", value=2048, unit=OCTETS),
                        ),
                    ),
                    "Every acquisition, custody probe, fixed-name mutation and actual extent path.",
                ),
                Bound(
                    "native_control_octets",
                    OCTETS,
                    "upper",
                    Expression(
                        "sum",
                        terms=(
                            scale(20, dim("path_octets")),
                            Expression("constant", value=15, unit=OCTETS),
                        ),
                    ),
                    "Complete native operands of original namespace, extent and parent receivers.",
                ),
                Bound(
                    "hashed_octets",
                    OCTETS,
                    "upper",
                    Expression("constant", value=16, unit=OCTETS),
                    "At most one phase-version UUID; no source hashing.",
                ),
                Bound(
                    "removed_octets",
                    OCTETS,
                    "upper",
                    dim("octet_budget"),
                    "At most one admitted Native or Core file extent.",
                ),
                *(
                    Bound(
                        metric,
                        OCTETS,
                        "equal",
                        Expression("constant", value=0, unit=OCTETS),
                        "No payload replay or archive continuation I/O.",
                    )
                    for metric in ("source_payload_octets", "encoded_octets")
                ),
            ),
            conditions=(
                "Maintained indexed Core and catalog schemas and private POSIX custody apply.",
                "Every helper cost and context exit is measured inclusively; no delegated work "
                "is excluded from this declaration.",
                "A fresh instance admits one attempt. The caller retains the instance before "
                "acquisition and through uncertain close and actual execution join.",
            ),
            invariants=(
                "One original job is selected by the caller; no archive or sibling census occurs.",
                "Root locks are nonblocking and released before Core or file work.",
                "Confirmed routing opens only before the irreversible metadata phase; partial "
                "metadata is disposed without reopening it.",
                "Child and Core close receiving precede release of the original parent lease.",
                "A missing catalog row never supplies physical disposal or terminal authority.",
                "No namespace, control, database, binding or semantic worker is reconstructed.",
            ),
            limitations=(
                "Physical filesystem/SQLite pager I/O and latency remain backend owned.",
                "Global scheduling, metadata admission, execution charges, actual worker join and "
                "application terminal semantics remain with their composing execution owner.",
            ),
        )
    )(operation)
