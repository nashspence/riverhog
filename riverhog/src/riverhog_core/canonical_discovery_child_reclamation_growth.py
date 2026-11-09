"""Complete direct primitive work and delegated Native extent composition."""

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
    growth_binding,
)


def declare_child_reclamation_growth(
    operation: Callable[..., Any], native: Callable[..., Any]
) -> None:
    provider = growth_binding(native)

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

    # The file-complete branch has five complete routing lookups plus its
    # initial currentness and final CAS fences: twelve original-owner checks.
    fences = 12
    limits = {
        "core_sql_statements": fences * 3 + 5,
        "core_returned_rows": fences * 3 + 5,
        "core_changed_rows": 1,
        "core_opened_connections": 0,
        "core_closed_connections": 0,
        "catalog_sql_statements": fences * 2 + 1,
        "catalog_driver_statements": fences * 34 + 32,
        "catalog_returned_rows": fences * 16 + 16,
        "catalog_changed_rows": 1,
        "uuid_values": 1,
        "open_calls": 6,
        "close_calls": 6,
        "stat_calls": 128,
        "uid_calls": 64,
        "fsync_calls": 8,
        "flock_calls": 3,
        "unlink_calls": 1,
        "rmdir_calls": 1,
        "mkdir_calls": 0,
        "truncate_calls": 0,
        "directory_enumerations": 0,
        "json_decodes": 5,
        "json_encodes": 5,
        "native_calls_limit": 1,
    }
    declaration = Declaration(
        id="riverhog-server.discovery-child-reclamation.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_child_reclamation.reclaim_discovery_child",),
        scope="one exact original child file extent or durable selected namespace/routing unit",
        starts_at="receive open confirmed retired metadata beneath actual exclusive parent custody",
        ends_at=(
            "receive every acquired context and one physical/phase outcome, or propagate failure"
        ),
        dimensions=(
            Dimension("retained_core_rows", COUNT, "Independent retained Core and child rows."),
            Dimension("retained_catalog_rows", COUNT, "Independent retained catalog/job rows."),
            Dimension(
                "control_octets", OCTETS, "Full job/header/registration and mutation widths."
            ),
            Dimension("path_octets", OCTETS, "Complete encoded original parent and child paths."),
            Dimension(
                "native_calls", COUNT, "Actual Native extent calls, independently capped at one."
            ),
            *(
                Dimension("native_" + item.name, item.unit, item.meaning)
                for item in provider.declaration.dimensions
            ),
        ),
        bounds=(
            *(
                Bound(
                    metric,
                    COUNT,
                    "upper" if value else "equal",
                    count(value),
                    "Direct original routing, catalog, custody and namespace primitives; "
                    "Native extent primitives remain separately delegated.",
                )
                for metric, value in limits.items()
            ),
            Bound("native_calls", COUNT, "equal", dim("native_calls"), "Actual delegated calls."),
            Bound(
                "core_vm_instructions",
                COUNT,
                "upper",
                vm(fences * 4096, fences * 512, "retained_core_rows"),
                "Indexed original point work, one routing deletion and fixed transaction controls.",
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
                    ("core_returned_octets", fences * 3 + 5, "Complete original result widths."),
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
                    ("json_decoded_octets", 5, "Complete selected Native binding decoding."),
                    ("json_encoded_octets", 5, "Complete canonical original binding encoding."),
                )
            ),
            Bound(
                "encoded_path_octets",
                OCTETS,
                "upper",
                scale(70, dim("path_octets")),
                "Actual exact relative/absolute path arguments; no sibling traversal.",
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
            *(
                Bound(
                    "native_" + bound.metric,
                    bound.unit,
                    bound.relation,
                    Expression(
                        "delegate",
                        name="native",
                        metric=bound.metric,
                        terms=(dim("native_calls"),),
                        arguments=tuple(
                            (item.name, dim("native_" + item.name))
                            for item in provider.declaration.dimensions
                        ),
                    ),
                    bound.meaning,
                )
                for bound in provider.declaration.bounds
            ),
        ),
        conditions=(
            "The caller retains original exclusive parent custody and the exact partial Core "
            "handle through actual close/worker receiving.",
            "Current confirmed original Core storage, original registration and maintained "
            "indexes exist; Native extent work has a positive byte admission.",
            "Direct metrics exclude only the explicitly sampled Native delegate; complete "
            "Core/catalog SQL/VM, opaque widths and namespace work remain observed.",
        ),
        invariants=(
            "A byte unit holds the original exclusive child lease and excludes every reader.",
            "Actual file absence is synchronized before the durable child-directory transition.",
            "Directory completion precedes routing deletion; the selected catalog identity "
            "survives every unlink/rmdir/routing/phase interruption.",
            "Unopened prefixes are received without creating controls, reading payloads or "
            "deleting unclassified files.",
            "No empty lookup, failed call or bounded partial extent reports parent completion.",
        ),
        limitations=(
            "Physical filesystem/SQLite I/O, allocation and latency remain backend observations.",
            "Parent/corpus/root/metadata/catalog completion and terminal resource discharge "
            "retain their separately composed owners.",
        ),
    )
    declare_growth(declaration, delegates=(("native", native),))(operation)
