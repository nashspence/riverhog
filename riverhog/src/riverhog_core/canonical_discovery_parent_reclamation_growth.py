"""Complete catalog and namespace work for one original parent receiving attempt."""

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


def declare_parent_reclamation_growth(operation: Callable[..., Any]) -> None:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dim(name: str) -> Expression:
        return Expression("dimension", name=name)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    limits = {
        "core_sql_statements": 0,
        "core_returned_rows": 0,
        "core_changed_rows": 0,
        "core_opened_connections": 0,
        "core_closed_connections": 0,
        "catalog_sql_statements": 7,
        "catalog_driver_statements": 134,
        "catalog_returned_rows": 64,
        "catalog_changed_rows": 1,
        "uuid_values": 1,
        "open_calls": 5,
        "close_calls": 5,
        "stat_calls": 79,
        "uid_calls": 24,
        "flock_calls": 3,
        "fsync_calls": 3,
        "unlink_calls": 1,
        "rmdir_calls": 1,
        "mkdir_calls": 0,
        "truncate_calls": 0,
        "directory_enumerations": 0,
        "json_decodes": 0,
        "json_encodes": 0,
    }
    declaration = Declaration(
        id="riverhog-server.discovery-parent-reclamation.growth/v1",
        authority="riverhog-server",
        subjects=(
            "riverhog_core.canonical_discovery_parent_reclamation."
            "DiscoveryParentRetirement.reclaim",
        ),
        scope="one original parent receiving attempt including actual descriptor closure",
        starts_at="receive a fresh retained owner with exact original retired job and root",
        ends_at="return synchronized namespace absence or busy, or propagate a retained failure",
        dimensions=(
            Dimension("retained_catalog_rows", COUNT, "Independent retained catalog rows."),
            Dimension("control_octets", OCTETS, "Complete original job and mutation widths."),
            Dimension("path_octets", OCTETS, "Complete encoded selected namespace path."),
        ),
        bounds=(
            *(
                Bound(
                    metric,
                    COUNT,
                    "upper" if value else "equal",
                    count(value),
                    "Three retired point fences, at most one phase CAS and fixed exact "
                    "namespace primitives; no Core or payload access.",
                )
                for metric, value in limits.items()
            ),
            Bound(
                "core_vm_instructions",
                COUNT,
                "equal",
                count(0),
                "No Core storage is opened or interpreted.",
            ),
            Bound(
                "catalog_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        count(131072),
                        Expression(
                            "product",
                            terms=(
                                count(8192),
                                Expression(
                                    "log2_ceil",
                                    terms=(
                                        Expression(
                                            "sum", terms=(count(2), dim("retained_catalog_rows"))
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Indexed exact retired identity fences and one versioned phase mutation.",
            ),
            *(
                Bound(metric, OCTETS, "upper", scale(multiplier, dim("control_octets")), meaning)
                for metric, multiplier, meaning in (
                    ("core_returned_octets", 0, "No Core rows."),
                    ("core_parameter_octets", 0, "No Core predicates."),
                    ("catalog_returned_octets", 64, "Complete selected retired rows."),
                    ("catalog_parameter_octets", 272, "Complete old and proposed controls."),
                    ("json_decoded_octets", 0, "No JSON decoding."),
                    ("json_encoded_octets", 0, "No JSON encoding."),
                )
            ),
            Bound(
                "encoded_path_octets",
                OCTETS,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        scale(20, dim("path_octets")),
                        Expression("constant", value=512, unit=OCTETS),
                    ),
                ),
                "Full anchor/root/selected identity arguments and thirteen exact data/control "
                "names, including unlink and rmdir; no sibling traversal.",
            ),
            Bound(
                "hashed_octets",
                OCTETS,
                "upper",
                Expression("constant", value=16, unit=OCTETS),
                "One actual phase-version UUID hash.",
            ),
            *(
                Bound(
                    metric,
                    OCTETS,
                    "equal",
                    Expression("constant", value=0, unit=OCTETS),
                    "No payload or continuation I/O.",
                )
                for metric in ("source_payload_octets", "encoded_octets")
            ),
        ),
        conditions=(
            "All creators select under the actual root-inode lock before their currentness "
            "callback. Retired state is absorbing.",
            "Confirmed jobs already hold the durable directory phase after actual file, "
            "worker and handle receiving. Unconfirmed metadata admits only empty known prefixes.",
            "The configured root parent is owner-controlled; private root/selected directories "
            "and single-link controls use maintained no-follow POSIX custody.",
            "Each attempt owns at most five descriptors. Uncertain closure retains this owner "
            "and blocks completion until actual close receiving.",
        ),
        invariants=(
            "Missing catalog rows never prove physical receiving.",
            "No root, marker, lease, database or payload is created, repaired or interpreted.",
            "Unknown entries remain and prevent exact directory completion.",
            "Shared global root and coordinator remain intact.",
            "Directory phase precedes lease unlink; positive fsync and actual closure precede "
            "a namespace-received result. Catalog deletion has separate authority.",
        ),
        limitations=(
            "Kernel path/index traversal, physical I/O and lock/fsync latency retain "
            "filesystem ownership; counts cover actual maintained Python primitives.",
            "File extents, confirmed Core/Native closure, catalog deletion, terminal "
            "disposition and service scheduling remain separately owned operations.",
        ),
    )
    declare_growth(declaration)(operation)


def declare_parent_close_growth(operation: Callable[..., Any]) -> None:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    declare_growth(
        Declaration(
            id="riverhog-server.discovery-parent-close.growth/v1",
            authority="riverhog-server",
            subjects=(
                "riverhog_core.canonical_discovery_parent_reclamation.DiscoveryParentRetirement.close",
            ),
            scope="one actual retained parent descriptor close attempt",
            starts_at="receive retained partial-opening, busy or uncertain-close custody",
            ends_at="receive actual closure or retain the first uncertain descriptor",
            dimensions=(
                Dimension(
                    "retained_descriptors", COUNT, "Actual occupied descriptor slots at entry."
                ),
            ),
            bounds=(
                Bound(
                    "close_calls",
                    COUNT,
                    "upper",
                    Expression("dimension", name="retained_descriptors"),
                    "Each occupied slot is closed at most once in this attempt.",
                ),
                Bound(
                    "descriptor_slots",
                    COUNT,
                    "upper",
                    count(5),
                    "Only the original anchor, root, coordinator, selected directory and lease.",
                ),
                *(
                    Bound(
                        metric,
                        COUNT,
                        "equal",
                        count(0),
                        "Closure acquires no storage, catalog or namespace authority.",
                    )
                    for metric in (
                        "open_calls",
                        "stat_calls",
                        "flock_calls",
                        "fsync_calls",
                        "unlink_calls",
                        "rmdir_calls",
                        "mkdir_calls",
                        "sql_statements",
                        "sqlite_connections",
                        "directory_enumerations",
                    )
                ),
                Bound(
                    "payload_octets",
                    OCTETS,
                    "equal",
                    Expression("constant", value=0, unit=OCTETS),
                    "No payload access.",
                ),
            ),
            conditions=(
                "The retained owner holds at most its five original descriptor slots.",
                "The caller retains the owner and remaining custody when a close raises.",
            ),
            invariants=(
                "A descriptor slot clears only after its actual close returns.",
                "Uncertain close retains custody and never yields namespace completion.",
                "Received closure is idempotent and never closes reused descriptors.",
            ),
            limitations=(
                "Kernel close latency and process-exit cleanup have operating-system ownership.",
                "Opening, physical receiving and catalog changes retain separate scopes.",
            ),
        )
    )(operation)
