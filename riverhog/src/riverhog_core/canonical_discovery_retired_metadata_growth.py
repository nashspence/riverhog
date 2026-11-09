"""Indexed original retired metadata and actual owned-handle growth scopes."""

from __future__ import annotations

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)


def _declaration(operation: str) -> Declaration:
    opening, closing = operation == "open", operation == "close"
    retiring = operation == "retire_child"
    fences = 5 if retiring else 0 if closing else 2 if operation in {"open", "child"} else 1
    statements = (
        20 if retiring else 16 if opening else 0 if closing else 7 if operation == "child" else 3
    )
    rows = 16 if retiring else 8 if opening else 0 if closing else 7 if operation == "child" else 3

    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def octets(value: int) -> Expression:
        return Expression("constant", value=value, unit=OCTETS)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    def width(value: int) -> Expression:
        return scale(value, Expression("dimension", name="control_octets"))

    def logarithm(name: str) -> Expression:
        return Expression(
            "log2_ceil",
            terms=(Expression("sum", terms=(Expression("dimension", name=name), count(2))),),
        )

    limits = {
        "core_sql_statements": statements,
        "core_returned_rows": rows,
        "core_changed_rows": int(retiring),
        "core_opened_connections": int(opening),
        "core_closed_connections": int(closing),
        "catalog_sql_statements": fences * 2,
        "catalog_driver_statements": fences * 34,
        "catalog_returned_rows": fences * 16,
        "catalog_changed_rows": 0,
        "uuid_values": 0,
        "open_calls": int(opening or retiring),
        "close_calls": 7 if closing else int(retiring),
        "stat_calls": fences * 8 + (4 if opening else 3 if retiring else 0),
        "uid_calls": fences * 4 + (2 if opening else int(retiring)),
        "fsync_calls": int(retiring),
        "directory_enumerations": 0,
        "json_decodes": int(operation in {"child", "retire_child"}),
        "json_encodes": 2 if retiring else int(operation == "child"),
    }
    return Declaration(
        id="riverhog-server.discovery-retired-metadata-" + operation + ".growth/v1",
        authority="riverhog-server",
        subjects=(
            "riverhog_core.canonical_discovery_retired_metadata.RetiredDiscoveryMetadata."
            + operation,
        ),
        scope="one exact original retired Core handle, child lookup or received routing retirement",
        starts_at=(
            "receive the confirmed detached job/version and actual exclusive "
            "original parent custody"
        ),
        ends_at="return or reject one original routing result, or confirm actual handle closure",
        dimensions=(
            Dimension(
                "retained_core_rows", COUNT, "Independent retained Core and registered child rows."
            ),
            Dimension("retained_catalog_rows", COUNT, "Independent retained catalog/job rows."),
            Dimension(
                "control_octets",
                OCTETS,
                "Complete job, header, original child binding and backend control widths.",
            ),
            Dimension("path_octets", OCTETS, "Complete encoded selected Core file path width."),
        ),
        bounds=(
            *(
                Bound(
                    metric,
                    COUNT,
                    "upper" if value else "equal",
                    count(value),
                    "Fixed indexed fences and actual owned connection/custody primitives.",
                )
                for metric, value in limits.items()
            ),
            Bound(
                "core_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        count(4096 * max(fences, 1) if not closing else 0),
                        Expression(
                            "product", terms=(count(512 * fences), logarithm("retained_core_rows"))
                        ),
                    ),
                ),
                "Singleton and original-key lookups without retained-row traversal.",
            ),
            Bound(
                "catalog_vm_instructions",
                COUNT,
                "upper",
                scale(
                    fences,
                    Expression(
                        "sum",
                        terms=(
                            count(32768),
                            Expression(
                                "product", terms=(count(2048), logarithm("retained_catalog_rows"))
                            ),
                        ),
                    ),
                ),
                "Exact already-retired point reads; no live generation join or mutation.",
            ),
            Bound(
                "core_returned_octets",
                OCTETS,
                "upper",
                width(rows),
                "Complete header and selected original child, including variable-width bindings.",
            ),
            Bound(
                "core_parameter_octets",
                OCTETS,
                "upper",
                width(fences * 4 + int(retiring)),
                "Exact original point keys and complete selected registration mutation.",
            ),
            Bound(
                "catalog_returned_octets",
                OCTETS,
                "upper",
                width(fences * 16),
                "Complete retained job rows and backend control results.",
            ),
            Bound(
                "catalog_parameter_octets",
                OCTETS,
                "upper",
                width(fences * 68),
                "Complete exact immutable labels and retired state/storage/version predicates.",
            ),
            Bound(
                "json_decoded_octets",
                OCTETS,
                "upper",
                width(int(operation in {"child", "retire_child"})),
                "Decode only the one selected complete Native binding.",
            ),
            Bound(
                "json_encoded_octets",
                OCTETS,
                "upper",
                width(2 if retiring else int(operation == "child")),
                "Native canonical verification and an exact deletion retain full binding widths.",
            ),
            Bound(
                "hashed_octets",
                OCTETS,
                "equal",
                octets(0),
                "No source or payload hashing and no catalog version generation.",
            ),
            Bound(
                "encoded_octets",
                OCTETS,
                "equal",
                octets(0),
                "No archive hash continuation is serialized.",
            ),
            Bound(
                "encoded_path_octets",
                OCTETS,
                "upper",
                scale(
                    fences * 5 + (2 if opening else 1 if retiring else 0),
                    Expression("dimension", name="path_octets"),
                ),
                "Exact owned root/directory/lease/file paths only.",
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                octets(0),
                "No archived or Native journal/member payload is replayed.",
            ),
        ),
        conditions=(
            "The caller serializes the original handle and retains the actual exclusive parent "
            "lease until close is confirmed.",
            "Original confirmed Core storage and its current schema/indexes exist; missing or "
            "damaged routing is never rebuilt.",
            "The already-retired catalog branch is a point read rather than live-worker "
            "reauthorization or phase reset.",
            "Logical SQLite instructions exclude native pager/recovery I/O, parser internals and "
            "B-tree comparisons.",
        ),
        invariants=(
            "No file, directory, lease, schema, live worker or Native source is created.",
            "Exact catalog storage confirmation, original header/cookie and original held "
            "file/lease descriptors fence every lookup.",
            "A selected child retains its original worker, journal, directory, catalog and "
            "complete Native replay binding.",
            "An empty lookup supplies no byte-disposal, namespace-completion or "
            "terminal-confirmation proof.",
            "Routing deletion requires durable selected-child state and synchronized exact "
            "namespace absence; no other registration or catalog phase changes.",
            "Uncertain child close retains at most four callbacks before the three original "
            "namespace/file descriptors; uncertain close rejects further work while "
            "its caller retains custody.",
        ),
        limitations=(
            "Physical filesystem/SQLite I/O and downstream Native reclamation remain separate "
            "scopes.",
        ),
    )


def declare_retired_metadata_growth(owner: type) -> None:
    for operation in ("open", "verify_current", "child", "retire_child", "close"):
        declare_growth(_declaration(operation))(getattr(owner, operation))
