"""Complete catalog/custody work and delegated Core metadata extent composition."""

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


def declare_metadata_reclamation_growth(
    operation: Callable[..., Any], files: Callable[..., Any]
) -> None:
    provider = growth_binding(files)

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

    # Initial and post-extent custody/catalog checks plus the final phase CAS
    # retain three exact retired point fences without opening Core storage.
    fences = 3
    limits = {
        "core_sql_statements": 0,
        "core_returned_rows": 0,
        "core_changed_rows": 0,
        "core_opened_connections": 0,
        "core_closed_connections": 0,
        "catalog_sql_statements": fences * 2 + 1,
        "catalog_driver_statements": fences * 34 + 32,
        "catalog_returned_rows": fences * 16 + 16,
        "catalog_changed_rows": 1,
        "uuid_values": 1,
        "open_calls": 0,
        "close_calls": 0,
        "stat_calls": fences * 6 + 5,
        "uid_calls": fences * 3,
        "fsync_calls": 1,
        "flock_calls": 0,
        "unlink_calls": 0,
        "rmdir_calls": 0,
        "mkdir_calls": 0,
        "truncate_calls": 0,
        "directory_enumerations": 0,
        "json_decodes": 0,
        "json_encodes": 0,
        "files_calls_limit": 1,
    }
    declaration = Declaration(
        id="riverhog-server.discovery-metadata-reclamation.growth/v1",
        authority="riverhog-server",
        subjects=(
            "riverhog_core.canonical_discovery_metadata_reclamation.reclaim_discovery_metadata",
        ),
        scope="one original Core metadata extent and synchronized final file receiving",
        starts_at=(
            "receive exact durable metadata phase beneath actual original exclusive parent custody"
        ),
        ends_at=(
            "receive every acquired context and one physical/phase outcome, or propagate failure"
        ),
        dimensions=(
            Dimension("retained_catalog_rows", COUNT, "Independent retained catalog/job rows."),
            Dimension(
                "control_octets", OCTETS, "Full job/header/registration and mutation widths."
            ),
            Dimension("path_octets", OCTETS, "Complete encoded original parent and child paths."),
            Dimension(
                "files_calls", COUNT, "Actual Neutral extent calls, independently capped at one."
            ),
            *(
                Dimension("files_" + item.name, item.unit, item.meaning)
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
                    "Direct original routing, catalog, custody and corpus control primitives; "
                    "Neutral extent primitives remain separately delegated.",
                )
                for metric, value in limits.items()
            ),
            Bound("files_calls", COUNT, "equal", dim("files_calls"), "Actual delegated calls."),
            Bound(
                "core_vm_instructions",
                COUNT,
                "upper",
                count(0),
                "No Core storage is opened or interpreted.",
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
                    ("core_returned_octets", 0, "No Core result is read."),
                    (
                        "core_parameter_octets",
                        0,
                        "No Core storage predicate is issued.",
                    ),
                    ("catalog_returned_octets", (fences + 1) * 16, "Full retired catalog rows."),
                    (
                        "catalog_parameter_octets",
                        (fences + 1) * 68,
                        "Full old and proposed controls.",
                    ),
                    ("json_decoded_octets", 0, "Complete selected retired controls decoding."),
                    ("json_encoded_octets", 0, "Complete canonical original binding encoding."),
                )
            ),
            Bound(
                "encoded_path_octets",
                OCTETS,
                "upper",
                scale(20, dim("path_octets")),
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
                    "files_" + bound.metric,
                    bound.unit,
                    bound.relation,
                    Expression(
                        "delegate",
                        name="files",
                        metric=bound.metric,
                        terms=(dim("files_calls"),),
                        arguments=tuple(
                            (item.name, dim("files_" + item.name))
                            for item in provider.declaration.dimensions
                        ),
                    ),
                    bound.meaning,
                )
                for bound in provider.declaration.bounds
            ),
        ),
        conditions=(
            "Every actual Core/Native handle and worker is received before the original "
            "exclusive parent custody can be released.",
            "The exact irreversible metadata phase retains original job/storage/version "
            "identity after partial database truncation.",
            "Direct primitives exclude only the explicitly sampled neutral file delegate; "
            "complete catalog controls, custody and prior-name probes remain observed.",
        ),
        invariants=(
            "Core metadata is never reopened, parsed, reconstructed or repaired.",
            "Only four exact Core metadata filenames enter the neutral extent operation.",
            "Prior Native corpus files and journals root must remain absent.",
            "Actual synchronized file absence precedes the exact directory-phase mutation.",
            "No failed or partial extent reports parent, catalog or terminal completion.",
        ),
        limitations=(
            "Physical filesystem/catalog I/O, allocation and latency remain backend observations.",
            "Cold no-control prefix and final parent/catalog receiving retain separate owners.",
        ),
    )
    declare_growth(declaration, delegates=(("files", files),))(operation)
