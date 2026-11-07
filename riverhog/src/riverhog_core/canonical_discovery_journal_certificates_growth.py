"""Direct certificate point work and explicitly observed metadata/Native ports."""

from __future__ import annotations

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


def declare_growth_for_journal_certificates(namespace: dict[str, Any]) -> None:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def octets(value: int) -> Expression:
        return Expression("constant", value=value, unit=OCTETS)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    width = Expression("dimension", name="metadata_octets")
    depth = Expression(
        "log2_ceil",
        terms=(
            Expression(
                "sum",
                terms=(
                    count(2),
                    Expression("dimension", name="retained_rows"),
                ),
            ),
        ),
    )
    for mode in ("__init__", "verify_current", "edge"):
        constructing, proving = mode == "__init__", mode == "edge"
        operation = getattr(namespace["DiscoveryJournalGraphCertificates"], mode)
        limits = {
            "sql_statements": count(8 if proving else 1),
            "returned_rows": count(8 if proving else 1),
            "returned_octets": scale(8 if proving else 1, width),
            "parameter_octets": scale(128, width),
            "changed_rows": count(0),
            "sqlite_vm_instructions": Expression(
                "sum",
                terms=(
                    count(32768),
                    Expression("product", terms=(count(8192), depth)),
                ),
            ),
            "metadata_read_calls": count(1),
            "rank_progress_calls": count(1),
            "epoch_frame_calls": count(int(proving)),
            "native_calls": count(6 if proving else int(constructing)),
            "native_input_octets": scale(32, width) if proving or constructing else octets(0),
            "native_output_octets": scale(32, width) if proving or constructing else octets(0),
            "json_encodes": count(int(constructing)),
            "json_input_octets": scale(16, width) if constructing else octets(0),
            "json_output_octets": scale(16, width) if constructing else octets(0),
            "hashed_octets": scale(16, width) if constructing else octets(0),
            "scalar_encoded_octets": scale(512, width),
            "scalar_decoded_octets": scale(128, width),
            "identity_checks": count(4 if proving else 0),
            "identity_input_octets": scale(4, width) if proving else octets(0),
        }
        declare_growth(
            Declaration(
                id="riverhog-server.discovery-journal-certificate-"
                + mode.strip("_").replace("_", "-")
                + ".growth/v1",
                authority="riverhog-server",
                subjects=(operation.__module__ + "." + operation.__qualname__,),
                scope="direct fixed manifest/frame/graph/arc/rank point work and qualified ports",
                starts_at="receive exact completed original ranks and borrowed metadata custody",
                ends_at="return pinned detached proof, consistent fallback or explicit rejection",
                dimensions=(
                    Dimension("retained_rows", COUNT, "Actual independent retained private rows."),
                    Dimension(
                        "metadata_octets", OCTETS, "Complete keys, manifests, rows and requests."
                    ),
                ),
                bounds=tuple(
                    Bound(
                        metric,
                        OCTETS if metric.endswith("octets") else COUNT,
                        "upper",
                        value,
                        "Fixed point reads and fixed-shape source/control/proof metadata.",
                    )
                    for metric, value in limits.items()
                ),
                conditions=(
                    "Caller retains original metadata/kernel/worker custody throughout replay.",
                    "The maintained point keys and terminal rank/union algorithms are present.",
                    "Metadata contexts, rank progress, original frame and Native methods retain "
                    "their own qualified contracts; direct calls and full "
                    "inputs/outputs are observed.",
                    "Only immutable binding/manifest metadata is cached; "
                    "every read checks currentness.",
                ),
                invariants=(
                    "Proofs bind original frame bytes, epoch, kind, arc membership and both ranks.",
                    "An entire cyclic or deferred epoch/kind consistently uses Native "
                    "online checks.",
                    "No source payload, graph census, rank write or replacement "
                    "reader is introduced.",
                    "Certificates never override ordered schema, reference or terminal semantics.",
                ),
                limitations=(
                    "Direct logical SQL/byte work excludes qualified port bodies and "
                    "physical latency.",
                    "Rank preparation and full Native/workflow composition are "
                    "separate operations.",
                ),
            )
        )(operation)
