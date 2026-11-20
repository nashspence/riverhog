"""Indexed original-custody readback, independent of collection-wide projections."""

from riverhog_growth_contracts import COUNT, OCTETS, Bound, Declaration, Dimension, Expression


def custody_readback_declaration() -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def width(value: int) -> Expression:
        return Expression(
            "product",
            terms=(
                Expression("constant", value=value),
                Expression("dimension", name="control_octets"),
            ),
        )

    limits = {
        "sql_statements": count(5),
        "returned_rows": count(4),
        "bound_parameter_octets": width(4),
        "returned_metadata_octets": width(4),
        "receipt_validations": count(1),
        "receipt_validation_octets": width(1),
        "source_payload_octets": width(0),
        "changed_rows": count(0),
        "sqlite_vm_instructions": Expression(
            "sum",
            terms=(
                count(512),
                Expression(
                    "product",
                    terms=(
                        count(512),
                        Expression(
                            "log2_ceil",
                            terms=(
                                Expression(
                                    "sum",
                                    terms=(
                                        count(2),
                                        Expression("dimension", name="retained_members"),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        ),
    }
    return Declaration(
        id="riverhog-server.artifact-custody-readback.growth/v1",
        authority="riverhog-server",
        subjects=(
            "riverhog_core.services.collection_uploads."
            "SqlAlchemyCollectionUploadService.get_artifact",
        ),
        scope="one exact accepted or published member custody lookup",
        starts_at="receive exact collection/member identity under current route read authority",
        ends_at="return the original custody document or report unavailable/corrupt evidence",
        dimensions=(
            Dimension("retained_members", COUNT, "All unrelated retained collection members."),
            Dimension(
                "control_octets", OCTETS, "Largest exact member row, receipt or request control."
            ),
        ),
        bounds=tuple(
            Bound(
                metric,
                OCTETS if metric.endswith("octets") else COUNT,
                "upper",
                limit,
                "Indexed collection/member points and at most one complete receipt validation.",
            )
            for metric, limit in limits.items()
        ),
        conditions=(
            "The caller independently checks current creator or scoped deletion read authority.",
            "Current catalog schema, member primary keys and local database indexes are required.",
        ),
        invariants=(
            "Publication preserves the original receipt; it never reconstructs archive authority.",
            "No archive download, collection traversal, receipt rewrite or payload read occurs.",
            "Collection description, completion and planner documents are not selected.",
            "Absent operational evidence stays unavailable even with recoverable archive content.",
        ),
        limitations=(
            "Protocol validation parses and self-checks one full control in linear byte work.",
            "Database lock waits, filesystem page faults and transport costs retain their owners.",
            "Receipt publication and archive index rebuilding are separately owned operations.",
        ),
    )
