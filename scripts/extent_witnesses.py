"""Nonnormative test bindings and open obligations for segmented v1 extents."""

from __future__ import annotations

import ast
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

PROGRESSION_OBLIGATIONS = frozenset(
    {
        "bounded_step",
        "forward_progress",
        "multiple_segments",
        "no_silent_truncation",
        "restart",
    }
)


@dataclass(frozen=True, slots=True)
class SegmentedExtentWitness:
    """Candidate tests for an owned progression rule, without a proof assertion."""

    id: str
    owner: str
    reasons: tuple[str, ...]
    test_node_ids: tuple[str, ...]
    gates: tuple[str, ...]
    test_scopes: tuple[str, ...] = ()


WITNESSES = (
    SegmentedExtentWitness(
        id="riverhog-upload-work-progression/v1",
        owner="riverhog",
        reasons=("bounded-actionable-work-acquisition",),
        test_node_ids=(
            "packages/riverhog-protocol/tests/test_collection_upload_transport.py::"
            "test_bounded_upload_work_batch_binds_assignment_and_checkpoint_state",
            "tests/unit/test_archive_write_reaper.py::"
            "test_archive_maintenance_drains_bounded_progress_before_idle_interval",
            "tests/unit/test_incremental_collection_producer.py::"
            "test_many_artifact_publication_retains_only_the_unsealed_pack_window",
        ),
        gates=("make unit", "make compose-smoke"),
    ),
    SegmentedExtentWitness(
        id="riverhog-archive-volume-part-progression/v1",
        owner=(
            "https://nashspence.github.io/riverhog/v1/schemas/"
            "collection-archive-volume-v1.schema.json"
        ),
        reasons=("bounded-archive-volume-parts",),
        test_node_ids=(
            "tests/unit/test_incremental_plan.py::"
            "test_artifact_at_a_time_construction_seals_the_exact_one_shot_v1_plans",
            "tests/unit/test_pack_upload.py::"
            "test_checkpoint_resumes_across_riverhog_and_adapter_restart",
            "tests/unit/test_raw_upload.py::"
            "test_raw_upload_resumes_on_server_defined_age_part_boundaries",
        ),
        gates=("make unit", "make compose-smoke", "make provider-qualification"),
    ),
    SegmentedExtentWitness(
        id="riverhog-storage-write-segment-progression/v1",
        owner="riverhog-storage-adapter-protocol",
        reasons=("bounded-storage-write-segment-page",),
        test_node_ids=(
            "packages/riverhog-storage-adapter-support/tests/"
            "test_storage_adapter_support.py::"
            "test_http_write_traversal_crosses_pages_and_process_restart",
            "some-implementations/riverhog/storage/filesystem/tests/"
            "test_filesystem_storage_adapter.py::"
            "test_segment_traversal_is_bounded_exact_and_restartable",
            "some-implementations/riverhog/storage/s3-support/tests/"
            "test_s3_storage_adapter.py::"
            "test_resumable_write_reconciles_segments_and_lost_completion",
        ),
        gates=("make unit", "make compose-smoke", "make provider-qualification"),
    ),
    SegmentedExtentWitness(
        id="riverhog-work-authority-append/v1",
        owner="riverhog",
        reasons=("bounded-authority-append",),
        test_node_ids=(
            "some-implementations/stove0/application/tests/test_riverhog_adapter.py::"
            "test_post_root_settlement_restarts_from_bounded_portable_inventory_progress",
            "tests/integration/test_collection_deletion_concurrency.py::"
            "test_postgres_exact_output_intent_creation_resumes_one_upload",
        ),
        gates=("make unit", "make postgres-concurrency", "make compose-smoke"),
    ),
    SegmentedExtentWitness(
        id="riverhog-work-disposition-append/v1",
        owner="riverhog",
        reasons=("bounded-disposition-append",),
        test_node_ids=(
            "some-implementations/stove0/application/tests/test_riverhog_adapter.py::"
            "test_post_root_settlement_restarts_from_bounded_portable_inventory_progress",
            "tests/integration/test_collection_deletion_concurrency.py::"
            "test_postgres_concurrent_first_disposition_and_output_create_one_set",
        ),
        gates=("make unit", "make postgres-concurrency", "make compose-smoke"),
    ),
    SegmentedExtentWitness(
        id="riverhog-provenance-volume-progression/v1",
        owner=(
            "https://nashspence.github.io/riverhog/v1/schemas/"
            "riverhog-provenance-bindings-v1.schema.json"
        ),
        reasons=("bounded-provenance-binding-volume",),
        test_node_ids=(
            "packages/riverhog-provenance/tests/test_segmented_archive.py::"
            "test_ordered_segmented_provenance_authority_round_trips",
            "packages/riverhog-provenance/tests/test_segmented_archive.py::"
            "test_provenance_segmentation_limits_one_volume_not_the_logical_total",
            "tests/unit/test_collection_uploads.py::"
            "test_provenance_append_persists_next_ordinal_across_retry_and_restart",
        ),
        gates=("make unit", "make compose-smoke", "make provider-qualification"),
    ),
    SegmentedExtentWitness(
        id="riverhog-raw-digest-progression/v1",
        owner="riverhog",
        reasons=("bounded-raw-digest-append",),
        test_node_ids=(
            "tests/unit/test_raw_ingress_manifest.py::"
            "test_raw_source_is_hashed_once_into_small_authority_and_bounded_batches",
            "tests/unit/test_raw_upload.py::"
            "test_raw_upload_resumes_on_server_defined_age_part_boundaries",
        ),
        gates=("make unit", "make compose-smoke", "make provider-qualification"),
    ),
    SegmentedExtentWitness(
        id="riverhog-retrieval-work-progression/v1",
        owner="riverhog",
        reasons=("bounded-retrieval-work-request",),
        test_node_ids=(
            "tests/unit/test_retrieval_service.py::"
            "test_retrieval_plan_resumes_across_more_than_two_internal_segment_pages",
            "tests/integration/test_catalog_schema_postgres.py::"
            "test_postgres_retrieval_plan_advances_in_bounded_restartable_steps",
        ),
        gates=("make unit", "make postgres-concurrency", "make compose-smoke"),
    ),
    SegmentedExtentWitness(
        id="riverhog-read-collection-progression/v1",
        owner="riverhog",
        reasons=("bounded-route-page", "bounded-route-progression"),
        test_node_ids=(
            "tests/unit/test_public_interface_parity.py::"
            "test_public_read_collection_selectors_are_bounded_and_frozen",
            "tests/unit/test_collection_reads.py::"
            "test_collection_list_query_count_is_independent_of_page_rows",
            "tests/unit/test_collection_reads.py::"
            "test_collection_encryption_filters_preserve_catalog_authorization",
            "packages/http-api-contracts/tests/test_browse_tokens.py::"
            "test_browse_token_round_trips_opaque_binary_position_across_restart",
            "packages/http-api-contracts/tests/test_browse_tokens.py::"
            "test_browse_token_fails_closed_outside_its_request_binding",
            "tests/unit/test_operation_lifecycle_api.py::"
            "test_riverhog_official_client_positive_disposable_lifecycle",
            "tests/unit/test_cli_json_output.py::"
            "test_collection_list_json_emits_the_api_response_without_a_second_model",
            "tests/unit/test_catalog_sync.py::"
            "test_catalog_sync_crosses_many_pages_and_repairs_fixed_frontier_changes",
            "tests/integration/test_lifecycle_event_concurrency.py::"
            "test_event_reads_and_concurrent_context_reapers_do_only_bounded_work",
        ),
        gates=("make unit", "make postgres-concurrency", "make database-qualification"),
        test_scopes=(
            "Structural OpenAPI checks for bounded read selectors; no traversal is executed.",
            "Collection-list service query count and archive-object loading on one page; "
            "does not test traversal or restart.",
            "Collection-list service encryption filters and catalog authorization on one page.",
            "Shared token codec reconstructs a position after codec recreation with the same "
            "signing configuration; does not prove route wiring or database traversal.",
            "Shared token codec rejects changed operation, principal, and selectors; "
            "does not prove each route supplies those bindings correctly.",
            "Real API and official client lifecycle includes a one-page collection list; "
            "its restart assertion concerns events, not collection-list continuation.",
            "Collection-list CLI accepts a current-schema fixture in both output modes, "
            "preserves the entire JSON response, and displays its description and encryption "
            "in human output; uses a fake client and does not establish general output parity.",
            "Catalog-sync fixed-frontier traversal and repair; ordinary mutable collection "
            "browsing has different semantics and gains no snapshot-completeness claim.",
            "Lifecycle-event reads and concurrent context reaping; not collection browsing.",
        ),
    ),
    SegmentedExtentWitness(
        id="stove0-read-collection-progression/v1",
        owner="stove0",
        reasons=("bounded-route-page", "bounded-route-progression"),
        test_node_ids=(
            "tests/unit/test_public_interface_parity.py::"
            "test_public_read_collection_selectors_are_bounded_and_frozen",
            "some-implementations/stove0/application/tests/test_cli.py::"
            "test_stove0_bounded_pages_keep_rich_and_json_cli_parity",
            "some-implementations/stove0/application/tests/test_work_state.py::"
            "test_sql_operational_retention_scans_bounded_pages_without_parsing_all_work",
        ),
        gates=("make unit", "make database-qualification", "make compose-smoke"),
    ),
    SegmentedExtentWitness(
        id="a-riverhog-ftp-spool-status-progression/v1",
        owner="a-riverhog-ftp-spool",
        reasons=("bounded-route-progression",),
        test_node_ids=(
            "some-implementations/riverhog/ingress/ftp/tests/test_ftp_spool_custody.py::"
            "test_status_pages_sources_without_claiming_an_exact_backlog_snapshot",
            "some-implementations/riverhog/ingress/ftp/tests/test_ftp_spool_custody.py::"
            "test_large_unavailable_backlog_is_bounded_then_drains_exactly_after_restart",
            "some-implementations/riverhog/ingress/ftp/tests/test_ftp_spool_custody.py::"
            "test_completion_discovery_progresses_beyond_persistent_prefix_across_restart",
            "some-implementations/riverhog/ingress/ftp/tests/test_ftp_spool_custody.py::"
            "test_adapter_owned_completion_authority_is_restartable_and_hidden",
            "some-implementations/riverhog/ingress/ftp/tests/test_ftp_spool_custody.py::"
            "test_full_batch_returns_before_bad_lookahead_record",
            "some-implementations/riverhog/ingress/ftp/tests/test_ftp_spool_custody.py::"
            "test_completion_failure_capacity_backpressures_at_exact_cursor",
            "some-implementations/riverhog/ingress/ftp/tests/test_ftp_spool_custody.py::"
            "test_lowered_admission_capacity_preserves_bounded_status_and_claim_drain",
            "some-implementations/riverhog/ingress/ftp/tests/test_ftp_spool_custody.py::"
            "test_durable_receipt_finishes_claim_cleanup_after_process_stop",
            "some-implementations/riverhog/ingress/ftp/tests/test_ftp_spool_custody.py::"
            "test_completion_event_replay_guard_survives_until_exact_log_tip",
            "some-implementations/riverhog/ingress/ftp/tests/test_ftp_spool_custody.py::"
            "test_completion_event_replay_state_reclaims_in_bounded_restartable_pages",
            "some-implementations/riverhog/ingress/ftp/tests/test_ftp_listener.py::"
            "test_incomplete_upload_is_not_handed_off_and_resumes_after_restart",
            "some-implementations/riverhog/ingress/ftp/tests/test_ftp_listener.py::"
            "test_success_ack_follows_exact_durable_handoff_and_path_reuse",
            "some-implementations/riverhog/ingress/ftp/tests/test_ftp_listener.py::"
            "test_same_path_replay_and_new_event_partition_into_restartable_claims",
            "some-implementations/riverhog/ingress/ftp/tests/test_ftp_listener.py::"
            "test_listener_recovers_published_intent_after_exact_adapter_acquisition",
            "some-implementations/riverhog/ingress/ftp/tests/test_ftp_listener.py::"
            "test_intent_retirement_and_adapter_acquisition_race_converges_repeatedly",
            "some-implementations/riverhog/ingress/ftp/tests/test_ftp_spool_custody.py::"
            "test_completed_portable_sidecar_follows_payload_into_claim",
            "some-implementations/riverhog/ingress/ftp/tests/test_ftp_spool_api_parity.py::"
            "test_management_api_and_client_share_versioned_routes",
        ),
        gates=("make unit", "make compose-smoke"),
    ),
    SegmentedExtentWitness(
        id="riverhog-upload-registration-progression/v1",
        owner="riverhog",
        reasons=("bounded-upload-registration",),
        test_node_ids=(
            "packages/riverhog-client/tests/test_transform.py::"
            "test_producer_streams_bounded_batches_without_limiting_collection_size",
            "tests/integration/test_collection_upload_custody_concurrency.py::"
            "test_distinct_concurrent_registrations_preserve_both_members",
        ),
        gates=("make unit", "make postgres-concurrency", "make compose-smoke"),
    ),
    SegmentedExtentWitness(
        id="riverhog-upload-tag-staging-progression/v1",
        owner="riverhog",
        reasons=("bounded-upload-staging-step; collection-tag-set-is-unbounded",),
        test_node_ids=(
            "tests/unit/test_incremental_collection_producer.py::"
            "test_incremental_producer_stages_unbounded_logical_tags_in_bounded_requests",
            "packages/riverhog-protocol/tests/test_collection_tag_protocol.py::"
            "test_late_tag_page_seeks_by_fixed_identity_without_rescanning_prior_tags",
            "tests/unit/test_collection_tags.py::"
            "test_provider_nodes_for_retained_exact_revisions_remain_recoverable",
            "tests/unit/test_collection_tags.py::"
            "test_tag_history_cleanup_bounds_all_subordinate_rows_and_restarts",
            "tests/unit/test_collection_tags.py::"
            "test_tag_node_reclamation_counts_every_edge_and_node_row_at_work_one",
            "tests/integration/test_catalog_schema_postgres.py::"
            "test_postgres_tag_history_cleanup_serializes_its_row_work_budget",
            "tests/integration/test_catalog_schema_postgres.py::"
            "test_postgres_tag_mutation_protects_an_aba_root_before_its_first_commit",
            "tests/integration/test_collection_upload_custody_concurrency.py::"
            "test_postgres_upload_protects_a_reused_tag_root_before_its_first_commit",
        ),
        gates=("make unit", "make compose-smoke", "make provider-qualification"),
    ),
    SegmentedExtentWitness(
        id="riverhog-upload-unit-source-progression/v1",
        owner="riverhog",
        reasons=("bounded-upload-unit-source-map",),
        test_node_ids=(
            "tests/unit/test_incremental_collection_producer.py::"
            "test_many_artifact_publication_retains_only_the_unsealed_pack_window",
            "tests/unit/test_pack_volume.py::"
            "test_pack_unit_wire_payload_is_only_concatenated_source_bytes",
            "tests/unit/test_pack_upload.py::"
            "test_checkpoint_resumes_across_riverhog_and_adapter_restart",
        ),
        gates=("make unit", "make compose-smoke", "make provider-qualification"),
    ),
)


class ExtentWitnessError(RuntimeError):
    """Raised when implementation proof bindings no longer resolve."""


def _test_source(root: Path, node_id: str) -> dict[str, object] | None:
    path_value, separator, symbol = node_id.partition("::")
    if not separator or not symbol or "::" in symbol:
        return None
    path = root / path_value
    if not path.is_file():
        return None
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=path.as_posix())
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == symbol:
            return {"path": path_value, "symbol": symbol, "line": node.lineno}
    return None


def bind_segmented_decisions(
    root: Path,
    decisions: Sequence[Mapping[str, object]],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Check routing and test-symbol existence, without certifying behavioral claims."""

    identities = [witness.id for witness in WITNESSES]
    if len(identities) != len(set(identities)):
        raise ExtentWitnessError("segmented extent witness identities are not unique")
    for witness in WITNESSES:
        if witness.test_scopes and len(witness.test_scopes) != len(witness.test_node_ids):
            raise ExtentWitnessError(
                f"segmented extent witness has incomplete test scopes: {witness.id}"
            )
        if not witness.test_node_ids or not witness.gates:
            raise ExtentWitnessError(f"segmented extent witness is not executable: {witness.id}")
        missing = [
            node_id for node_id in witness.test_node_ids if _test_source(root, node_id) is None
        ]
        if missing:
            raise ExtentWitnessError(
                f"segmented extent witness has stale test nodes: {witness.id}: {missing}"
            )

    used: set[str] = set()
    links: list[dict[str, object]] = []
    for decision in decisions:
        if decision.get("policy") != "segmented_no_total_max":
            continue
        owner = str(decision.get("owner"))
        reason = str(decision.get("reason"))
        matched = [
            witness.id
            for witness in WITNESSES
            if witness.owner == owner and reason in witness.reasons
        ]
        if not matched:
            raise ExtentWitnessError(
                "segmented extent has no candidate test binding: "
                f"{decision.get('id')} ({owner}; {reason})"
            )
        used.update(matched)
        links.append(
            {
                "id": decision["id"],
                "segmented_extent_witnesses": matched,
            }
        )
    unused = sorted(set(identities) - used)
    if unused:
        raise ExtentWitnessError(
            f"segmented extent witnesses have no contract decision owner: {unused}"
        )
    records: list[dict[str, object]] = [
        {
            "id": witness.id,
            "owner": witness.owner,
            "reasons": list(witness.reasons),
            "test_node_ids": list(witness.test_node_ids),
            "gates": list(witness.gates),
            "unestablished_claims": sorted(PROGRESSION_OBLIGATIONS),
            "test_scopes": [
                {"node_id": node_id, "scope": scope, "source": _test_source(root, node_id)}
                for node_id, scope in zip(witness.test_node_ids, witness.test_scopes, strict=True)
            ]
            if witness.test_scopes
            else [],
        }
        for witness in WITNESSES
        if witness.id in used
    ]
    return records, links


__all__ = [
    "SegmentedExtentWitness",
    "ExtentWitnessError",
    "PROGRESSION_OBLIGATIONS",
    "WITNESSES",
    "bind_segmented_decisions",
]
