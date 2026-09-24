# riverhog-catalog: Durable State

[Atlas](../../../index.md) · [Authority](../index.md) · [Policies](../../../policies/index.md)

Persisted structures, schema heads, and v1 transition obligations.

## Contract elements

| Exact unit | Kind |
|---|---|
| [Schema identity](riverhog-catalog-durable-state-identity.md) | Schema identity |
| [app_key_access_grants](riverhog-catalog-app-key-access-grants.md) | Relational table |
| [app_keys](riverhog-catalog-app-keys.md) | Relational table |
| [archive_copy_jobs](riverhog-catalog-archive-copy-jobs.md) | Relational table |
| [archive_copy_object_uploads](riverhog-catalog-archive-copy-object-uploads.md) | Relational table |
| [archive_copy_retirements](riverhog-catalog-archive-copy-retirements.md) | Relational table |
| [archive_download_reservations](riverhog-catalog-archive-download-reservations.md) | Relational table |
| [archive_download_usage](riverhog-catalog-archive-download-usage.md) | Relational table |
| [catalog_events](riverhog-catalog-catalog-events.md) | Relational table |
| [catalog_sync_state](riverhog-catalog-catalog-sync-state.md) | Relational table |
| [collection_archive_copies](riverhog-catalog-collection-archive-copies.md) | Relational table |
| [collection_archive_file_objects](riverhog-catalog-collection-archive-file-objects.md) | Relational table |
| [collection_archive_object_uploads](riverhog-catalog-collection-archive-object-uploads.md) | Relational table |
| [collection_archive_objects](riverhog-catalog-collection-archive-objects.md) | Relational table |
| [collection_deletions](riverhog-catalog-collection-deletions.md) | Relational table |
| [collection_derivations](riverhog-catalog-collection-derivations.md) | Relational table |
| [collection_description_publications](riverhog-catalog-collection-description-publications.md) | Relational table |
| [collection_file_provenance](riverhog-catalog-collection-file-provenance.md) | Relational table |
| [collection_files](riverhog-catalog-collection-files.md) | Relational table |
| [collection_mutable_document_publication_attempts](riverhog-catalog-collection-mutable-document-publication-attempts.md) | Relational table |
| [collection_mutable_document_reclamations](riverhog-catalog-collection-mutable-document-reclamations.md) | Relational table |
| [collection_processing_capabilities](riverhog-catalog-collection-processing-capabilities.md) | Relational table |
| [collection_processing_capability_artifacts](riverhog-catalog-collection-processing-capability-artifacts.md) | Relational table |
| [collection_processing_claim_artifacts](riverhog-catalog-collection-processing-claim-artifacts.md) | Relational table |
| [collection_processing_claim_inputs](riverhog-catalog-collection-processing-claim-inputs.md) | Relational table |
| [collection_processing_claims](riverhog-catalog-collection-processing-claims.md) | Relational table |
| [collection_processing_disposition_outputs](riverhog-catalog-collection-processing-disposition-outputs.md) | Relational table |
| [collection_processing_disposition_sets](riverhog-catalog-collection-processing-disposition-sets.md) | Relational table |
| [collection_processing_dispositions](riverhog-catalog-collection-processing-dispositions.md) | Relational table |
| [collection_processing_outcomes](riverhog-catalog-collection-processing-outcomes.md) | Relational table |
| [collection_provenance_entities](riverhog-catalog-collection-provenance-entities.md) | Relational table |
| [collection_provenance_external_state_references](riverhog-catalog-collection-provenance-external-state-references.md) | Relational table |
| [collection_provenance_journal_agents](riverhog-catalog-collection-provenance-journal-agents.md) | Relational table |
| [collection_provenance_journal_chunks](riverhog-catalog-collection-provenance-journal-chunks.md) | Relational table |
| [collection_provenance_journals](riverhog-catalog-collection-provenance-journals.md) | Relational table |
| [collection_provenance_verification_agents](riverhog-catalog-collection-provenance-verification-agents.md) | Relational table |
| [collection_provenance_verification_entities](riverhog-catalog-collection-provenance-verification-entities.md) | Relational table |
| [collection_provenance_verification_entries](riverhog-catalog-collection-provenance-verification-entries.md) | Relational table |
| [collection_provenance_verification_external_states](riverhog-catalog-collection-provenance-verification-external-states.md) | Relational table |
| [collection_provenance_verification_reachability](riverhog-catalog-collection-provenance-verification-reachability.md) | Relational table |
| [collection_provenance_verifications](riverhog-catalog-collection-provenance-verifications.md) | Relational table |
| [collection_tag_memberships](riverhog-catalog-collection-tag-memberships.md) | Relational table |
| [collection_tag_mutation_node_references](riverhog-catalog-collection-tag-mutation-node-references.md) | Relational table |
| [collection_tag_mutations](riverhog-catalog-collection-tag-mutations.md) | Relational table |
| [collection_tag_node_edges](riverhog-catalog-collection-tag-node-edges.md) | Relational table |
| [collection_tag_node_gc](riverhog-catalog-collection-tag-node-gc.md) | Relational table |
| [collection_tag_node_reclamations](riverhog-catalog-collection-tag-node-reclamations.md) | Relational table |
| [collection_tag_nodes](riverhog-catalog-collection-tag-nodes.md) | Relational table |
| [collection_tag_publication_frontier](riverhog-catalog-collection-tag-publication-frontier.md) | Relational table |
| [collection_tag_publications](riverhog-catalog-collection-tag-publications.md) | Relational table |
| [collection_tag_published_nodes](riverhog-catalog-collection-tag-published-nodes.md) | Relational table |
| [collection_tag_revisions](riverhog-catalog-collection-tag-revisions.md) | Relational table |
| [collection_tag_visibility](riverhog-catalog-collection-tag-visibility.md) | Relational table |
| [collection_tags](riverhog-catalog-collection-tags.md) | Relational table |
| [collection_upload_files](riverhog-catalog-collection-upload-files.md) | Relational table |
| [collection_upload_provenance_archive_volumes](riverhog-catalog-collection-upload-provenance-archive-volumes.md) | Relational table |
| [collection_upload_provenance_journal_chunks](riverhog-catalog-collection-upload-provenance-journal-chunks.md) | Relational table |
| [collection_upload_provenance_journals](riverhog-catalog-collection-upload-provenance-journals.md) | Relational table |
| [collection_upload_provenance_reachability](riverhog-catalog-collection-upload-provenance-reachability.md) | Relational table |
| [collection_upload_provenance_sources](riverhog-catalog-collection-upload-provenance-sources.md) | Relational table |
| [collection_upload_provenance_validation_facts](riverhog-catalog-collection-upload-provenance-validation-facts.md) | Relational table |
| [collection_upload_raw_part_digests](riverhog-catalog-collection-upload-raw-part-digests.md) | Relational table |
| [collection_upload_tag_node_references](riverhog-catalog-collection-upload-tag-node-references.md) | Relational table |
| [collection_upload_tag_publication_frontier](riverhog-catalog-collection-upload-tag-publication-frontier.md) | Relational table |
| [collection_upload_tags](riverhog-catalog-collection-upload-tags.md) | Relational table |
| [collection_uploads](riverhog-catalog-collection-uploads.md) | Relational table |
| [collections](riverhog-catalog-collections.md) | Relational table |
| [ix_collection_processing_capability_artifacts_order](riverhog-catalog-ix-collection-processing-capability-artifacts-order.md) | Unique index |
| [ix_collection_processing_claim_artifacts_order](riverhog-catalog-ix-collection-processing-claim-artifacts-order.md) | Unique index |
| [ix_collection_processing_outcomes_order](riverhog-catalog-ix-collection-processing-outcomes-order.md) | Unique index |
| [ix_processing_disposition_outputs_order](riverhog-catalog-ix-processing-disposition-outputs-order.md) | Unique index |
| [ix_processing_dispositions_order](riverhog-catalog-ix-processing-dispositions-order.md) | Unique index |
| [key_download_reservations](riverhog-catalog-key-download-reservations.md) | Relational table |
| [key_download_usage](riverhog-catalog-key-download-usage.md) | Relational table |
| [lifecycle_events](riverhog-catalog-lifecycle-events.md) | Relational table |
| [retrieval_cache_accounting_reconciliations](riverhog-catalog-retrieval-cache-accounting-reconciliations.md) | Relational table |
| [retrieval_cache_leases](riverhog-catalog-retrieval-cache-leases.md) | Relational table |
| [retrieval_cache_objects](riverhog-catalog-retrieval-cache-objects.md) | Relational table |
| [retrieval_cache_population_claims](riverhog-catalog-retrieval-cache-population-claims.md) | Relational table |
| [retrieval_cache_populations](riverhog-catalog-retrieval-cache-populations.md) | Relational table |
| [retrieval_cache_store_accounting](riverhog-catalog-retrieval-cache-store-accounting.md) | Relational table |
| [retrieval_job_object_progress](riverhog-catalog-retrieval-job-object-progress.md) | Relational table |
| [retrieval_jobs](riverhog-catalog-retrieval-jobs.md) | Relational table |
| [retrieval_plan_files](riverhog-catalog-retrieval-plan-files.md) | Relational table |
| [retrieval_plan_objects](riverhog-catalog-retrieval-plan-objects.md) | Relational table |
| [retrieval_plan_placements](riverhog-catalog-retrieval-plan-placements.md) | Relational table |
| [retrieval_plans](riverhog-catalog-retrieval-plans.md) | Relational table |
| [ux_app_keys_token_sha256](riverhog-catalog-ux-app-keys-token-sha256.md) | Unique index |
| [ux_collection_archive_object_uploads_sequence](riverhog-catalog-ux-collection-archive-object-uploads-sequence.md) | Unique index |
| [ux_collection_upload_files_order](riverhog-catalog-ux-collection-upload-files-order.md) | Unique index |
| [ux_collection_uploads_principal_idempotency_key](riverhog-catalog-ux-collection-uploads-principal-idempotency-key.md) | Unique index |
