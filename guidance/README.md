# Guiding heuristics — non-binding

Generated from [heuristics.toml](heuristics.toml); edit that register, not this page.

These are guiding heuristics only: non-binding and non-contractual. Recording, adopting, using, testing, or rendering a heuristic does not create, extend, interpret, override, or relax an external contract. Contract meaning comes only from independently designated contract authorities.

Only deliberately adopted, currently useful guidance belongs on reviewed main. Draft copies and inferred candidates are not adoption. A heuristic may inform a design discussion, including one about future contracts; any contract change is a separate decision through its own authority. Departures from guidance need no waiver. Independent contracts, checks, and repository requirements still apply.

Related tests are contextual links, not evidence of adoption, complete coverage, or a passing run. Their independent roles are unchanged. An empty list means no related test is recorded, not that guidance is invalid.

Additions and changes are reviewed as guidance only. Remove retired entries; Git keeps their history. `make guidance` checks bookkeeping, not adherence to the guidance.

## audience-aware-interface-design (non-binding)

When extending a maintained interface family, consider its intended audience and reuse the
operation classification before deciding whether to add a human command, a client primitive,
or a standard-protocol path.

Scope: Design review of Riverhog, Stove0, and FTP-adapter interface additions; the existing
operation matrix remains independently authoritative for its own classified surfaces.

Rationale: This makes parity a deliberate, scoped design choice rather than an assumption that every
endpoint needs every interface. It does not add, change, or interpret any operation-matrix
or contract entry.

Related tests (context only):

- [`tests/unit/test_operation_qualification.py::test_generated_operation_matrix_is_complete_and_fail_closed`](../tests/unit/test_operation_qualification.py) — Checks current matrix identities, classifications, and client/CLI presence for relevant
classes; not end-to-end behavioral equivalence.
- [`tests/unit/test_operation_qualification.py::test_operation_audiences_distinguish_commands_wires_and_protocols`](../tests/unit/test_operation_qualification.py) — Checks representative current audience classifications, not full interface parity.

## bounded-obligation-progression (non-binding)

Prefer durable, restartable continuation for work that can exceed one bounded step; account
for attempted work separately from semantic completion and consider the progress of
unrelated runnable work.

Scope: Design and maintenance of mutable-collection publication and mutable-document reclamation,
including segmented work, provider effects, and retry scheduling.

Rationale: This favors bounded resource use without casually turning an operational budget into a
logical size limit. Exact limits, progress obligations, and completion semantics come from
their independent authorities.

Related tests (context only):

- [`tests/unit/test_collection_descriptions.py::test_maximum_description_document_is_retained_before_provider_io`](../tests/unit/test_collection_descriptions.py) — Existing witness association for representability before provider I/O.
- [`tests/unit/test_collection_tags.py::test_tag_publication_budget_defers_but_does_not_limit_logical_mutation`](../tests/unit/test_collection_tags.py) — Existing witness association for an operational budget distinct from logical mutation size.
- [`tests/unit/test_collection_tags.py::test_persistent_tag_gc_failure_is_bounded_and_does_not_starve_other_publication`](../tests/unit/test_collection_tags.py) — Existing witness association for bounded failed reclamation alongside publication.
- [`tests/unit/test_collection_tags.py::test_tag_node_reclamation_counts_every_edge_and_node_row_at_work_one`](../tests/unit/test_collection_tags.py) — Existing witness association for row-level work accounting.
- [`tests/integration/test_catalog_schema_postgres.py::test_postgres_description_attempt_accepts_maximum_canonical_document`](../tests/integration/test_catalog_schema_postgres.py) — Existing PostgreSQL association for description representability.
- [`tests/integration/test_catalog_schema_postgres.py::test_postgres_tag_history_cleanup_serializes_its_row_work_budget`](../tests/integration/test_catalog_schema_postgres.py) — Existing PostgreSQL association for cleanup-budget accounting.

## derive-references-from-canonical-sources (non-binding)

Prefer generating secondary reference material and checking it against its established
executable source instead of keeping a parallel hand-maintained inventory.

Scope: Design of repository reference generators and release-installation outputs; consider the
same approach when adding documentation about existing contract surfaces.

Rationale: This reduces opportunities for competing descriptions to diverge. The guidance neither
appoints a source as contractual authority nor makes a generated document contractual.

Related tests (context only):

- [`tests/unit/test_release_installation.py::test_installation_artifacts_are_derived_and_mutually_consistent`](../tests/unit/test_release_installation.py) — Builds and verifies fixture installation artifacts, manifest relationships, and reference
content; the external-lock export is stubbed. This is not a real-platform installation
result.

## exact-external-effect-authority (non-binding)

Prefer sealing explicit intent and acquiring destination-specific ownership before starting
an external effect; use that same attempt when reconciling ambiguous outcomes.

Scope: Design and maintenance of collection-description and collection-tag-head publication where
database acceptance and provider I/O cannot commit atomically.

Rationale: This favors a design in which delayed work and retries remain attributable to one intended
effect. It does not prescribe a public protocol or weaken independently required
correctness.

Related tests (context only):

- [`tests/unit/test_collection_descriptions.py::test_primary_description_claim_excludes_replica_worker_for_the_same_destination`](../tests/unit/test_collection_descriptions.py) — Existing witness association for primary-versus-replica destination exclusion; not a newly
audited general proof.
- [`tests/unit/test_collection_descriptions.py::test_replica_description_claim_excludes_primary_writer_for_the_same_destination`](../tests/unit/test_collection_descriptions.py) — Existing witness association for the reverse destination-exclusion case.
- [`tests/unit/test_collection_tags.py::test_delayed_old_head_writer_cannot_overwrite_newer_acknowledged_authority`](../tests/unit/test_collection_tags.py) — Existing witness association for a delayed older head writer.
- [`tests/integration/test_catalog_schema_postgres.py::test_postgres_primary_description_claim_excludes_replica_worker`](../tests/integration/test_catalog_schema_postgres.py) — Existing PostgreSQL association for competing publication ownership.
- [`tests/integration/test_catalog_schema_postgres.py::test_postgres_mutable_replica_attempt_serializes_reconciliation_before_newer_desired`](../tests/integration/test_catalog_schema_postgres.py) — Existing PostgreSQL association for reconciliation ordering.

## external-result-acceptance-and-cleanup-custody (non-binding)

Prefer explicit durable acceptance and cleanup receipts for external results, including
superseded results; carry that bookkeeping through ambiguity, restart, retirement, and
reuse.

Scope: Design and maintenance of collection-description publication, collection-tag publication,
tag-node reclamation, and mutable-document reclamation.

Rationale: This favors recoverable accounting over inferring completion from a provider call returning.
It neither defines an external completion guarantee nor waives existing regression checks.

Related tests (context only):

- [`tests/unit/test_collection_descriptions.py::test_description_projection_waits_for_durable_publication_and_restarts`](../tests/unit/test_collection_descriptions.py) — Existing witness association for publication acceptance and restart.
- [`tests/unit/test_collection_descriptions.py::test_description_replacement_reconciles_receipts_after_an_ambiguous_response`](../tests/unit/test_collection_descriptions.py) — Existing witness association for ambiguous replacement responses.
- [`tests/unit/test_collection_descriptions.py::test_superseded_document_cleanup_receipt_survives_collection_retirement`](../tests/unit/test_collection_descriptions.py) — Existing witness association for cleanup custody across retirement.
- [`tests/unit/test_collection_tags.py::test_superseded_tag_heads_retain_exact_cleanup_custody`](../tests/unit/test_collection_tags.py) — Existing witness association for superseded tag-head receipts.
- [`tests/unit/test_collection_tags.py::test_tag_node_gc_resumes_idempotently_after_an_ambiguous_delete`](../tests/unit/test_collection_tags.py) — Existing witness association for ambiguous deletion and retry.
- [`tests/unit/test_collection_tags.py::test_delayed_success_from_old_tag_gc_cannot_consume_successor`](../tests/unit/test_collection_tags.py) — Existing witness association for delayed successful reclamation results.
- [`tests/unit/test_collection_tags.py::test_delayed_failure_from_old_tag_gc_cannot_reschedule_successor`](../tests/unit/test_collection_tags.py) — Existing witness association for delayed failed reclamation results.
- [`tests/integration/test_catalog_schema_postgres.py::test_postgres_delayed_success_from_old_tag_gc_cannot_consume_successor`](../tests/integration/test_catalog_schema_postgres.py) — Existing PostgreSQL association for delayed successful reclamation results.
- [`tests/integration/test_catalog_schema_postgres.py::test_postgres_delayed_failure_from_old_tag_gc_cannot_reschedule_successor`](../tests/integration/test_catalog_schema_postgres.py) — Existing PostgreSQL association for delayed failed reclamation results.
- [`tests/unit/test_collection_tags.py::test_unrelated_head_advance_preserves_interrupted_tag_gc_until_reuse_is_safe`](../tests/unit/test_collection_tags.py) — Existing witness association for interrupted reclamation during head advancement.
- [`tests/integration/test_catalog_schema_postgres.py::test_postgres_unrelated_head_advance_preserves_interrupted_tag_gc_until_reuse_is_safe`](../tests/integration/test_catalog_schema_postgres.py) — Existing PostgreSQL association for interrupted reclamation during head advancement.
- [`tests/integration/test_catalog_schema_postgres.py::test_postgres_superseded_document_cleanup_workers_claim_distinct_receipts`](../tests/integration/test_catalog_schema_postgres.py) — Existing PostgreSQL association for concurrent cleanup receipt claims.

## presentation-is-a-projection (non-binding)

Prefer deriving human and machine presentations from the same operation result, with output
mode changing presentation rather than the selected operation or its inputs.

Scope: Maintenance of existing Piggity collection-list, collection-show, and archive-store views.
Consider the same approach for comparable new views; this record does not declare universal
interface parity.

Rationale: A shared result reduces duplicated response models and accidental divergence. Human text can
summarize; this guidance does not require textual equality or promise that every JSON field
appears in human output.

Related tests (context only):

- [`tests/unit/test_cli_json_output.py::test_collection_list_json_emits_the_api_response_without_a_second_model`](../tests/unit/test_cli_json_output.py) — Uses a fake client: identical list-call arguments, full JSON payload, and selected human
fields. No live-API or traversal claim.
- [`tests/unit/test_cli_json_output.py::test_collection_show_human_and_json_use_one_identical_api_response`](../tests/unit/test_cli_json_output.py) — Uses a fake client: the same collection lookup, complete JSON payload, and selected human
fields.
- [`tests/unit/test_cli_json_output.py::test_archive_store_views_project_the_same_api_models_in_human_and_json`](../tests/unit/test_cli_json_output.py) — Uses a fake client: matching list/show calls and JSON payloads, plus selected human fields.

## scope-evidence-to-what-was-exercised (non-binding)

Prefer describing a check at the level it actually exercises, distinguishing structural
correspondence, fake-client checks, live behavior, and exact-run results.

Scope: Maintenance of qualification reporting and cross-interface parity explanations; related test
links in this register remain contextual only.

Rationale: This avoids turning test presence or a narrow successful response into a broader assurance.
Evidence required by an actual contract is still handled independently.

Related tests (context only):

- [`tests/unit/test_operation_qualification.py::test_exact_sha_evidence_contains_only_generated_current_rows`](../tests/unit/test_operation_qualification.py) — Uses constructed timing evidence and checks that broader lifecycle, CLI projection, bounded-
access, and restart qualifications remain not established; this test does not itself
establish those behaviors.

## structured-authority-dependency-liveness (non-binding)

Prefer establishing successor protection for referenced immutable dependencies before
releasing predecessor protection; distinguish content equality from retained storage.

Scope: Design and maintenance of collection-tag authority whose nodes can be reclaimed
independently.

Rationale: This favors explicit lifetime accounting across reuse, publication, and reclamation. Any
actual retention promise remains independently defined.

Related tests (context only):

- [`tests/unit/test_collection_tags.py::test_tag_edit_inherits_complete_published_subtrees_without_rewalking_them`](../tests/unit/test_collection_tags.py) — Existing witness association for subtree reuse.
- [`tests/unit/test_collection_tags.py::test_pending_mutation_reuses_a_retiring_root_without_a_retention_gap`](../tests/unit/test_collection_tags.py) — Existing witness association for reuse during retirement.
- [`tests/unit/test_collection_tags.py::test_delayed_gc_cannot_delete_a_node_republished_by_a_newer_authority`](../tests/unit/test_collection_tags.py) — Existing witness association for delayed reclamation and republication.
- [`tests/unit/test_collection_tags.py::test_provider_nodes_for_retained_exact_revisions_remain_recoverable`](../tests/unit/test_collection_tags.py) — Existing witness association for nodes of retained revisions; does not create a retention
promise.
- [`tests/integration/test_catalog_schema_postgres.py::test_postgres_reused_tag_node_gc_and_publication_workers_converge`](../tests/integration/test_catalog_schema_postgres.py) — Existing PostgreSQL association for reclamation/publication races.

## use-jcs-for-json (non-binding)

Prefer RFC 8785 JSON Canonicalization Scheme (JCS) for JSON throughout the repository.
Use the shared riverhog-canonical-json package rather than local serializer options when
emitting or checking JSON bytes. Treat a required different spelling as an explicit
interface choice, never as an interchangeable canonical form.

Scope: New and revised JSON producers and consumers, especially contract records, protocol
messages, persisted evidence, and values used in identity or digest calculations.

Rationale: One canonical spelling prevents equivalent JSON values from drifting across packages,
renderers, and hash boundaries. This guidance does not change independently specified
wire contracts or make a human-readable diagnostic an identity record.

Related tests (context only):

- [`packages/riverhog-canonical-json/tests/test_canonical_json.py::test_jcs_orders_utf16_keys_and_preserves_unicode_code_points`](../packages/riverhog-canonical-json/tests/test_canonical_json.py) — Checks the shared encoder's key order, Unicode output, and digest for one representative
value; does not cover every JSON producer in the repository.
- [`tests/unit/test_contract_html_rendering.py::test_every_declared_leaf_value_is_visible_without_omission_or_mutation`](../tests/unit/test_contract_html_rendering.py) — Checks the contract HTML's exact displayed leaf values against JCS spelling for values
that are shown as JSON; does not make the HTML an independent contract authority.
