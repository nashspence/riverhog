"""Complete original journals namespace, Core close and catalog phase work."""

from __future__ import annotations

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


def declare_root_reclamation_growth(operation: Callable[..., Any]) -> None:

    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dim(name: str) -> Expression:
        return Expression("dimension", name=name)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    def width(value: int) -> Expression:
        return scale(value, dim("control_octets"))

    def vm(base: int, step: int, dimension: str) -> Expression:
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
                            terms=(Expression("sum", terms=(count(2), dim(dimension))),),
                        ),
                    ),
                ),
            ),
        )

    # One empty-child lookup, initial and namespace currentness checks,
    # and the pre-close check retain five original Core fences; the post-close
    # parent check and exact phase CAS retain their own complete primitives.
    fences = 5
    limits = {
        "core_sql_statements": fences * 3 + 1,
        "core_returned_rows": fences * 3 + 1,
        "core_changed_rows": 0,
        "core_opened_connections": 0,
        "core_closed_connections": 1,
        "catalog_sql_statements": fences * 2 + 1,
        "catalog_driver_statements": fences * 34 + 32,
        "catalog_returned_rows": fences * 16 + 16,
        "catalog_changed_rows": 1,
        "uuid_values": 1,
        "open_calls": 2,
        "close_calls": 3,
        "stat_calls": fences * 8 + 20,
        "uid_calls": fences * 4 + 5,
        "fsync_calls": 2,
        "flock_calls": 2,
        "unlink_calls": 1,
        "rmdir_calls": 1,
        "mkdir_calls": 0,
        "truncate_calls": 0,
        "directory_enumerations": 0,
        "json_decodes": 1,
        "json_encodes": 1,
    }
    declaration = Declaration(
        id="riverhog-server.discovery-journals-root-reclamation.growth/v1",
        authority="riverhog-server",
        subjects=(
            "riverhog_core.canonical_discovery_root_reclamation.reclaim_discovery_journals_root",
        ),
        scope="one exact journals-root receiving, actual Core close and durable metadata phase",
        starts_at="receive open confirmed retired metadata beneath actual exclusive parent custody",
        ends_at=(
            "receive actual namespace/Core closure and one physical/phase outcome, "
            "or propagate failure"
        ),
        dimensions=(
            Dimension("retained_core_rows", COUNT, "Independent retained Core and child rows."),
            Dimension("retained_catalog_rows", COUNT, "Independent retained catalog/job rows."),
            Dimension(
                "control_octets", OCTETS, "Full job/header/registration and mutation widths."
            ),
            Dimension("path_octets", OCTETS, "Complete encoded original parent and child paths."),
        ),
        bounds=(
            *(
                Bound(
                    metric,
                    COUNT,
                    "upper" if value else "equal",
                    count(value),
                    "Direct original routing, catalog, custody, namespace "
                    "and actual close primitives.",
                )
                for metric, value in limits.items()
            ),
            Bound(
                "core_vm_instructions",
                COUNT,
                "upper",
                vm(fences * 4096, fences * 512, "retained_core_rows"),
                "Indexed original point work without Core routing mutation.",
            ),
            Bound(
                "catalog_vm_instructions",
                COUNT,
                "upper",
                vm((fences + 1) * 32768, (fences + 1) * 2048, "retained_catalog_rows"),
                "Exact retired point fences and one versioned phase mutation.",
            ),
            *(
                Bound(metric, OCTETS, "upper", width(multiplier), meaning)
                for metric, multiplier, meaning in (
                    ("core_returned_octets", fences * 3 + 1, "Complete original result widths."),
                    (
                        "core_parameter_octets",
                        fences * 4 + 1,
                        "Complete exact original predicates.",
                    ),
                    ("catalog_returned_octets", (fences + 1) * 16, "Full retired catalog rows."),
                    (
                        "catalog_parameter_octets",
                        (fences + 1) * 68,
                        "Full old and proposed controls.",
                    ),
                    ("json_decoded_octets", 1, "Complete selected Native binding decoding."),
                    ("json_encoded_octets", 1, "Complete canonical original binding encoding."),
                )
            ),
            Bound(
                "encoded_path_octets",
                OCTETS,
                "upper",
                scale(40, dim("path_octets")),
                "Exact original parent/root/control names; no sibling traversal.",
            ),
            Bound(
                "hashed_octets",
                OCTETS,
                "upper",
                Expression("constant", value=16, unit=OCTETS),
                "At most one actual catalog version UUID hash.",
            ),
            *(
                Bound(
                    metric,
                    OCTETS,
                    "equal",
                    Expression("constant", value=0, unit=OCTETS),
                    "No archive payload, source reconstruction or continuation encoding.",
                )
                for metric in ("source_payload_octets", "encoded_octets")
            ),
        ),
        conditions=(
            "The caller retains actual exclusive original parent custody through every "
            "namespace/Core close and worker receiving outcome.",
            "Exact confirmed retired Core storage and durable journals-root phase exist; "
            "completed indexed child and Native corpus receiving are checked again.",
            "Logical SQLite VM excludes backend parser, pager/recovery I/O and comparisons.",
        ),
        invariants=(
            "Unknown entries remain untouched and prevent exact root removal.",
            "Missing root/control prefixes never authorize creation or repair.",
            "Root coordination is nonblocking; busy custody retains Core and catalog phase.",
            "Actual namespace and Core closure precede the exact metadata-phase mutation.",
            "No metadata extent, parent removal, catalog deletion or terminal completion occurs.",
        ),
        limitations=(
            "Physical filesystem/SQLite I/O, allocation and latency remain backend observations.",
            "Metadata extents, parent/catalog completion and terminal resource receiving "
            "retain their separately composed owners.",
        ),
    )
    declare_growth(declaration)(operation)
