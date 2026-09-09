#!/usr/bin/env python3
"""Generate or verify nonnormative cross-cutting implementation policy witnesses."""

from __future__ import annotations

import argparse
import ast
import json
import sys
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "qualification/policies/implementation-witnesses.json"
SCHEMA = "riverhog-implementation-policy-witnesses/v1"
CLASSIFICATIONS = frozenset(
    {
        "external_contract",
        "internal_correctness",
        "operational_policy",
    }
)


@dataclass(frozen=True, slots=True)
class AuthorityPolicyWitness:
    """One scoped policy retained only while current executable proof supports it."""

    id: str
    classification: str
    scopes: tuple[str, ...]
    invariant: str
    applies_when: str
    test_node_ids: tuple[str, ...]
    gates: tuple[str, ...]


WITNESSES = (
    AuthorityPolicyWitness(
        id="exact-external-effect-authority/v1",
        classification="internal_correctness",
        scopes=(
            "collection-description-publication",
            "collection-tag-head-publication",
        ),
        invariant=(
            "Seal immutable intent and atomically acquire the exact destination attempt, "
            "ownership, and effect precondition before provider I/O; retries reconcile that "
            "attempt and conflicting intent cannot reuse it."
        ),
        applies_when=(
            "An external state-changing effect and its database acceptance cannot be committed "
            "atomically."
        ),
        test_node_ids=(
            "tests/unit/test_collection_descriptions.py::"
            "test_primary_description_claim_excludes_replica_worker_for_the_same_destination",
            "tests/unit/test_collection_descriptions.py::"
            "test_replica_description_claim_excludes_primary_writer_for_the_same_destination",
            "tests/unit/test_collection_tags.py::"
            "test_delayed_old_head_writer_cannot_overwrite_newer_acknowledged_authority",
            "tests/integration/test_catalog_schema_postgres.py::"
            "test_postgres_primary_description_claim_excludes_replica_worker",
            "tests/integration/test_catalog_schema_postgres.py::"
            "test_postgres_mutable_replica_attempt_serializes_reconciliation_before_newer_desired",
        ),
        gates=("make unit", "make postgres-concurrency", "make compose-smoke"),
    ),
    AuthorityPolicyWitness(
        id="structured-authority-dependency-liveness/v1",
        classification="internal_correctness",
        scopes=("collection-tag-authority",),
        invariant=(
            "Accept a structured published authority only while its required dependency closure "
            "is durably and continuously protected; establish successor ownership before prior "
            "protection is released. Content equality alone does not establish stored liveness."
        ),
        applies_when=(
            "A published authority references independently reclaimable immutable dependencies."
        ),
        test_node_ids=(
            "tests/unit/test_collection_tags.py::"
            "test_tag_edit_inherits_complete_published_subtrees_without_rewalking_them",
            "tests/unit/test_collection_tags.py::"
            "test_pending_mutation_reuses_a_retiring_root_without_a_retention_gap",
            "tests/unit/test_collection_tags.py::"
            "test_delayed_gc_cannot_delete_a_node_republished_by_a_newer_authority",
            "tests/unit/test_collection_tags.py::"
            "test_provider_nodes_for_retained_exact_revisions_remain_recoverable",
            "tests/integration/test_catalog_schema_postgres.py::"
            "test_postgres_reused_tag_node_gc_and_publication_workers_converge",
        ),
        gates=(
            "make unit",
            "make postgres-concurrency",
            "make compose-smoke",
            "make provider-qualification",
        ),
    ),
    AuthorityPolicyWitness(
        id="external-result-acceptance-and-cleanup-custody/v1",
        classification="internal_correctness",
        scopes=(
            "collection-description-publication",
            "collection-tag-publication",
            "collection-tag-node-reclamation",
            "mutable-document-reclamation",
        ),
        invariant=(
            "Report completion only after transactional acceptance of the exact external result, "
            "and retain its exact cleanup custody across ambiguity, replacement, retirement, and "
            "restart until reclamation is reconciled. Once destructive work claims an exact stored "
            "revision, reuse establishes a separately receipted protected state."
        ),
        applies_when=(
            "An accepted or superseded external result has provider state Riverhog may later need "
            "to reconcile or reclaim."
        ),
        test_node_ids=(
            "tests/unit/test_collection_descriptions.py::"
            "test_description_projection_waits_for_durable_publication_and_restarts",
            "tests/unit/test_collection_descriptions.py::"
            "test_description_replacement_reconciles_receipts_after_an_ambiguous_response",
            "tests/unit/test_collection_descriptions.py::"
            "test_superseded_document_cleanup_receipt_survives_collection_retirement",
            "tests/unit/test_collection_tags.py::"
            "test_superseded_tag_heads_retain_exact_cleanup_custody",
            "tests/unit/test_collection_tags.py::"
            "test_tag_node_gc_resumes_idempotently_after_an_ambiguous_delete",
            "tests/unit/test_collection_tags.py::"
            "test_delayed_success_from_old_tag_gc_cannot_consume_successor",
            "tests/unit/test_collection_tags.py::"
            "test_delayed_failure_from_old_tag_gc_cannot_reschedule_successor",
            "tests/integration/test_catalog_schema_postgres.py::"
            "test_postgres_delayed_success_from_old_tag_gc_cannot_consume_successor",
            "tests/integration/test_catalog_schema_postgres.py::"
            "test_postgres_delayed_failure_from_old_tag_gc_cannot_reschedule_successor",
            "tests/integration/test_catalog_schema_postgres.py::"
            "test_postgres_superseded_document_cleanup_workers_claim_distinct_receipts",
        ),
        gates=(
            "make unit",
            "make postgres-concurrency",
            "make compose-smoke",
            "make provider-qualification",
        ),
    ),
    AuthorityPolicyWitness(
        id="exact-read-authority-lifetime/v1",
        classification="external_contract",
        scopes=(
            "catalog-sync-authority",
            "collection-tag-revision",
        ),
        invariant=(
            "Keep an exact read authority available under its declared retention rules or return "
            "its explicit expiry/reset result, while every access still enforces current "
            "authorization and reclaimed history cannot become silent omission."
        ),
        applies_when=(
            "A public read contract issues an exact authority with a declared retained lifetime."
        ),
        test_node_ids=(
            "tests/unit/test_catalog_sync.py::"
            "test_catalog_sync_cursor_fails_closed_for_authority_changes",
            "tests/unit/test_catalog_sync.py::test_catalog_sync_cursor_expiry_is_explicit",
            "tests/unit/test_catalog_sync.py::"
            "test_catalog_sync_history_reaping_has_an_explicit_gap_error",
            "tests/unit/test_runtime_config.py::"
            "test_catalog_sync_cursor_lifetimes_fit_the_retained_history",
            "tests/unit/test_collection_tags.py::"
            "test_exact_tag_revisions_expire_with_the_catalog_history_that_names_them",
        ),
        gates=("make unit", "make database-qualification"),
    ),
    AuthorityPolicyWitness(
        id="bounded-obligation-progression/v1",
        classification="operational_policy",
        scopes=(
            "mutable-collection-publication",
            "mutable-document-reclamation",
        ),
        invariant=(
            "Admit only values representable throughout their mandatory path; advance obligations "
            "that may exceed one bounded step or cross an external effect through durable "
            "restartable continuation, count attempted work separately from semantic completion, "
            "and do not starve unrelated runnable work."
        ),
        applies_when=(
            "A maintained operation may require segmented work, an external effect, retry backoff, "
            "or scheduling alongside independent obligations."
        ),
        test_node_ids=(
            "tests/unit/test_collection_descriptions.py::"
            "test_maximum_description_document_is_retained_before_provider_io",
            "tests/unit/test_collection_tags.py::"
            "test_tag_publication_budget_defers_but_does_not_limit_logical_mutation",
            "tests/unit/test_collection_tags.py::"
            "test_persistent_tag_gc_failure_is_bounded_and_does_not_starve_other_publication",
            "tests/unit/test_collection_tags.py::"
            "test_tag_node_reclamation_counts_every_edge_and_node_row_at_work_one",
            "tests/integration/test_catalog_schema_postgres.py::"
            "test_postgres_description_attempt_accepts_maximum_canonical_document",
            "tests/integration/test_catalog_schema_postgres.py::"
            "test_postgres_tag_history_cleanup_serializes_its_row_work_budget",
        ),
        gates=(
            "make unit",
            "make postgres-concurrency",
            "make database-qualification",
            "make compose-smoke",
        ),
    ),
)


class AuthorityPolicyWitnessError(RuntimeError):
    """Raised when a retained policy is no longer tied to current executable proof."""


def _test_symbol_exists(root: Path, node_id: str) -> bool:
    path_value, separator, symbol = node_id.partition("::")
    if not separator or not symbol or "::" in symbol:
        return False
    path = root / path_value
    if not path.is_file():
        return False
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=path.as_posix())
    return any(
        isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == symbol
        for node in tree.body
    )


def policy_projection(root: Path = ROOT) -> dict[str, object]:
    """Return scoped policies only after every proof binding resolves exactly."""

    identities = [witness.id for witness in WITNESSES]
    if len(identities) != len(set(identities)):
        raise AuthorityPolicyWitnessError("authority policy identities are not unique")
    for witness in WITNESSES:
        if witness.classification not in CLASSIFICATIONS:
            raise AuthorityPolicyWitnessError(
                f"authority policy has invalid classification: {witness.id}"
            )
        if not witness.scopes or not witness.invariant or not witness.applies_when:
            raise AuthorityPolicyWitnessError(f"authority policy is unscoped: {witness.id}")
        if not witness.test_node_ids or not witness.gates:
            raise AuthorityPolicyWitnessError(
                f"authority policy lacks executable proof: {witness.id}"
            )
        missing_tests = [
            node_id for node_id in witness.test_node_ids if not _test_symbol_exists(root, node_id)
        ]
        if missing_tests:
            raise AuthorityPolicyWitnessError(
                f"authority policy has stale test nodes: {witness.id}: {missing_tests}"
            )
    policies: list[dict[str, object]] = [
        {
            "id": witness.id,
            "classification": witness.classification,
            "scopes": list(witness.scopes),
            "invariant": witness.invariant,
            "applies_when": witness.applies_when,
            "test_node_ids": list(witness.test_node_ids),
            "gates": list(witness.gates),
        }
        for witness in WITNESSES
    ]
    return {
        "schema": SCHEMA,
        "authority": "nonnormative-implementation-correctness",
        "policies": policies,
        "coverage": {
            "policies": len(policies),
            "classifications": dict(
                sorted(Counter(str(policy["classification"]) for policy in policies).items())
            ),
            "test_links": sum(len(witness.test_node_ids) for witness in WITNESSES),
        },
    }


def _render() -> str:
    return json.dumps(policy_projection(), indent=2, sort_keys=True) + "\n"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("check", help="Verify the checked-in implementation policy evidence.")
    subparsers.add_parser("update", help="Regenerate implementation policy evidence.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        rendered = _render()
        if args.command == "update":
            OUTPUT.parent.mkdir(parents=True, exist_ok=True)
            OUTPUT.write_text(rendered, encoding="utf-8")
        elif not OUTPUT.is_file() or OUTPUT.read_text(encoding="utf-8") != rendered:
            raise AuthorityPolicyWitnessError(
                "implementation policy evidence is stale; "
                "run `make implementation-policy-update` and review the diff"
            )
        print(
            json.dumps(
                {
                    "output": OUTPUT.relative_to(ROOT).as_posix(),
                    "policies": len(WITNESSES),
                    "status": "updated" if args.command == "update" else "current",
                },
                sort_keys=True,
            )
        )
        return 0
    except AuthorityPolicyWitnessError as exc:
        print(f"implementation policy validation failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "AuthorityPolicyWitness",
    "AuthorityPolicyWitnessError",
    "CLASSIFICATIONS",
    "OUTPUT",
    "SCHEMA",
    "WITNESSES",
    "main",
    "policy_projection",
]
