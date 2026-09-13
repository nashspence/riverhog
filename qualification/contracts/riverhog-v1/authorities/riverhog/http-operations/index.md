# riverhog: HTTP Operations

[Atlas](../../../index.md) · [Authority](../index.md) · [Policies](../../../policies/index.md)

Callable HTTP operations.

Contract elements: **109** · Extent decisions: **99**

| Policy | Count |
|---|---:|
| [compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0) | 109 |
| [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb) | 28 |
| [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0) | 59 |

## Semantic dossiers

| Dossier | Extent decisions |
|---|---:|
| [DELETE /v1/apps/{app}/keys/{key_id}/access](delete-v1-apps-app-keys-key-id-access.md) | 1 |
| [DELETE /v1/archive/copies/{collection_id}/{destination_store}](delete-v1-archive-copies-collection-id-destination-store.md) | 0 |
| [DELETE /v1/collections/{collection_id}/provenance/verification](delete-v1-collections-collection-id-provenance-verification.md) | 0 |
| [DELETE /v1/retrieval-jobs/{job_id}](delete-v1-retrieval-jobs-job-id.md) | 0 |
| [GET /health/live](get-health-live.md) | 0 |
| [GET /health/ready](get-health-ready.md) | 0 |
| [GET /v1/app-key-access](get-v1-app-key-access.md) | 2 |
| [GET /v1/apps](get-v1-apps.md) | 2 |
| [GET /v1/apps/{app}/keys](get-v1-apps-app-keys.md) | 2 |
| [GET /v1/archive/copies](get-v1-archive-copies.md) | 2 |
| [GET /v1/archive/copies/{collection_id}/{destination_store}](get-v1-archive-copies-collection-id-destination-store.md) | 0 |
| [GET /v1/archive/stores](get-v1-archive-stores.md) | 2 |
| [GET /v1/archive/stores/{store}](get-v1-archive-stores-store.md) | 0 |
| [GET /v1/catalog-sync/changes](get-v1-catalog-sync-changes.md) | 3 |
| [GET /v1/catalog-sync/checkpoint](get-v1-catalog-sync-checkpoint.md) | 0 |
| [GET /v1/catalog-sync/collections](get-v1-catalog-sync-collections.md) | 3 |
| [GET /v1/catalog/collections/{collection_id}/inventory](get-v1-catalog-collections-collection-id-inventory.md) | 3 |
| [GET /v1/collection-processing-claims](get-v1-collection-processing-claims.md) | 2 |
| [GET /v1/collection-processing-claims/{claim_id}](get-v1-collection-processing-claims-claim-id.md) | 1 |
| [GET /v1/collection-processing-claims/{claim_id}/derivation](get-v1-collection-processing-claims-claim-id-derivation.md) | 1 |
| [GET /v1/collection-processing-claims/{claim_id}/derivation/dispositions](get-v1-collection-processing-claims-claim-id-derivation-dispositions.md) | 3 |
| [GET /v1/collection-processing-claims/{claim_id}/derivation/output-edges](get-v1-collection-processing-claims-claim-id-derivation-output-edges.md) | 3 |
| [GET /v1/collection-processing-claims/{claim_id}/inputs](get-v1-collection-processing-claims-claim-id-inputs.md) | 3 |
| [GET /v1/collection-processing-claims/{claim_id}/outcomes](get-v1-collection-processing-claims-claim-id-outcomes.md) | 3 |
| [GET /v1/collection-processing-claims/{claim_id}/plan/artifacts](get-v1-collection-processing-claims-claim-id-plan-artifacts.md) | 3 |
| [GET /v1/collection-upload-sessions](get-v1-collection-upload-sessions.md) | 2 |
| [GET /v1/collection-upload-sessions/{collection_id}](get-v1-collection-upload-sessions-collection-id.md) | 0 |
| [GET /v1/collection-upload-sessions/{collection_id}/files](get-v1-collection-upload-sessions-collection-id-files.md) | 2 |
| [GET /v1/collection-upload-sessions/{collection_id}/provenance/journals/{journal_id}](get-v1-collection-upload-sessions-collection-id-provenance-journals-journal-id.md) | 0 |
| [GET /v1/collection-upload-sessions/{collection_id}/volumes/{volume_id}/units/{unit}](get-v1-collection-upload-sessions-collection-id-volumes-volume-id-units-unit.md) | 0 |
| [GET /v1/collection-upload-sessions/{collection_id}/work](get-v1-collection-upload-sessions-collection-id-work.md) | 1 |
| [GET /v1/collections](get-v1-collections.md) | 3 |
| [GET /v1/collections/{collection_id}](get-v1-collections-collection-id.md) | 0 |
| [GET /v1/collections/{collection_id}/archive-copies](get-v1-collections-collection-id-archive-copies.md) | 2 |
| [GET /v1/collections/{collection_id}/derivation](get-v1-collections-collection-id-derivation.md) | 0 |
| [GET /v1/collections/{collection_id}/provenance/files](get-v1-collections-collection-id-provenance-files.md) | 2 |
| [GET /v1/collections/{collection_id}/provenance/files/{path}](get-v1-collections-collection-id-provenance-files-path.md) | 1 |
| [GET /v1/collections/{collection_id}/provenance/journals/{journal_id}](get-v1-collections-collection-id-provenance-journals-journal-id.md) | 0 |
| [GET /v1/collections/{collection_id}/provenance/journals/{journal_id}/agents](get-v1-collections-collection-id-provenance-journals-journal-id-agents.md) | 2 |
| [GET /v1/collections/{collection_id}/provenance/trace/{path}](get-v1-collections-collection-id-provenance-trace-path.md) | 3 |
| [GET /v1/collections/{collection_id}/provenance/verification](get-v1-collections-collection-id-provenance-verification.md) | 0 |
| [GET /v1/collections/{collection_id}/tags](get-v1-collections-collection-id-tags.md) | 3 |
| [GET /v1/collections/{collection_id}/tags:contains](get-v1-collections-collection-id-tags-contains.md) | 1 |
| [GET /v1/download-quota](get-v1-download-quota.md) | 0 |
| [GET /v1/download-quotas](get-v1-download-quotas.md) | 2 |
| [GET /v1/events](get-v1-events.md) | 2 |
| [GET /v1/retrieval-cache](get-v1-retrieval-cache.md) | 0 |
| [GET /v1/retrieval-cache/objects](get-v1-retrieval-cache-objects.md) | 2 |
| [GET /v1/retrieval-cache/objects/{collection_id}/{source_store}/{object_id}](get-v1-retrieval-cache-objects-collection-id-source-store-object-id.md) | 0 |
| [GET /v1/retrieval-jobs/{job_id}](get-v1-retrieval-jobs-job-id.md) | 0 |
| [GET /v1/retrieval-jobs/{job_id}/content](get-v1-retrieval-jobs-job-id-content.md) | 0 |
| [GET /v1/retrieval-plans/{plan_id}](get-v1-retrieval-plans-plan-id.md) | 0 |
| [GET /v1/retrieval-plans/{plan_id}/files](get-v1-retrieval-plans-plan-id-files.md) | 3 |
| [GET /v1/search](get-v1-search.md) | 2 |
| [GET /v1/tags](get-v1-tags.md) | 2 |
| [HEAD /v1/collections/{collection_id}/provenance/journals/{journal_id}](head-v1-collections-collection-id-provenance-journals-journal-id.md) | 0 |
| [HEAD /v1/retrieval-jobs/{job_id}/content](head-v1-retrieval-jobs-job-id-content.md) | 0 |
| [PATCH /v1/collection-upload-sessions/{collection_id}/provenance/journals/{journal_id}](patch-v1-collection-upload-sessions-collection-id-provenance-journals-journal-id.md) | 1 |
| [POST /v1/apps/{app}/keys](post-v1-apps-app-keys.md) | 0 |
| [POST /v1/apps/{app}/keys/{key_id}/access](post-v1-apps-app-keys-key-id-access.md) | 1 |
| [POST /v1/apps/{app}/keys/{key_id}/revoke](post-v1-apps-app-keys-key-id-revoke.md) | 1 |
| [POST /v1/apps/{app}/keys/{key_id}/rotate](post-v1-apps-app-keys-key-id-rotate.md) | 1 |
| [POST /v1/archive/copies](post-v1-archive-copies.md) | 0 |
| [POST /v1/archive/copies/retire](post-v1-archive-copies-retire.md) | 0 |
| [POST /v1/archive/copies/retirement-plan](post-v1-archive-copies-retirement-plan.md) | 0 |
| [POST /v1/collection-processing-claims](post-v1-collection-processing-claims.md) | 0 |
| [POST /v1/collection-processing-claims/{claim_id}/abandon](post-v1-collection-processing-claims-claim-id-abandon.md) | 1 |
| [POST /v1/collection-processing-claims/{claim_id}/capabilities](post-v1-collection-processing-claims-claim-id-capabilities.md) | 1 |
| [POST /v1/collection-processing-claims/{claim_id}/capabilities/{capability_id}/artifacts/seal](post-v1-collection-processing-claims-claim-id-capabilities-capability-id-artifacts-seal.md) | 1 |
| [POST /v1/collection-processing-claims/{claim_id}/derivation/seal](post-v1-collection-processing-claims-claim-id-derivation-seal.md) | 1 |
| [POST /v1/collection-processing-claims/{claim_id}/inputs/seal](post-v1-collection-processing-claims-claim-id-inputs-seal.md) | 1 |
| [POST /v1/collection-processing-claims/{claim_id}/outcomes/settle](post-v1-collection-processing-claims-claim-id-outcomes-settle.md) | 1 |
| [POST /v1/collection-processing-claims/{claim_id}/plan](post-v1-collection-processing-claims-claim-id-plan.md) | 1 |
| [POST /v1/collection-processing-claims/{claim_id}/plan/artifacts/seal](post-v1-collection-processing-claims-claim-id-plan-artifacts-seal.md) | 1 |
| [POST /v1/collection-processing-claims/{claim_id}/release](post-v1-collection-processing-claims-claim-id-release.md) | 1 |
| [POST /v1/collection-processing-claims/{claim_id}/renew](post-v1-collection-processing-claims-claim-id-renew.md) | 1 |
| [POST /v1/collection-processing-claims/{claim_id}/restart](post-v1-collection-processing-claims-claim-id-restart.md) | 1 |
| [POST /v1/collection-processing-claims/{claim_id}/retirement](post-v1-collection-processing-claims-claim-id-retirement.md) | 1 |
| [POST /v1/collection-processing-claims/{claim_id}/settle](post-v1-collection-processing-claims-claim-id-settle.md) | 1 |
| [POST /v1/collection-upload-sessions](post-v1-collection-upload-sessions.md) | 0 |
| [POST /v1/collection-upload-sessions/{collection_id}/cancel](post-v1-collection-upload-sessions-collection-id-cancel.md) | 0 |
| [POST /v1/collection-upload-sessions/{collection_id}/complete](post-v1-collection-upload-sessions-collection-id-complete.md) | 0 |
| [POST /v1/collection-upload-sessions/{collection_id}/discard](post-v1-collection-upload-sessions-collection-id-discard.md) | 0 |
| [POST /v1/collection-upload-sessions/{collection_id}/discard-plan](post-v1-collection-upload-sessions-collection-id-discard-plan.md) | 0 |
| [POST /v1/collection-upload-sessions/{collection_id}/files](post-v1-collection-upload-sessions-collection-id-files.md) | 0 |
| [POST /v1/collection-upload-sessions/{collection_id}/heartbeat](post-v1-collection-upload-sessions-collection-id-heartbeat.md) | 0 |
| [POST /v1/collection-upload-sessions/{collection_id}/provenance/journals/{journal_id}/seal](post-v1-collection-upload-sessions-collection-id-provenance-journals-journal-id-seal.md) | 0 |
| [POST /v1/collection-upload-sessions/{collection_id}/raw-part-digests](post-v1-collection-upload-sessions-collection-id-raw-part-digests.md) | 0 |
| [POST /v1/collection-upload-sessions/{collection_id}/tags](post-v1-collection-upload-sessions-collection-id-tags.md) | 0 |
| [POST /v1/collections/{collection_id}/delete](post-v1-collections-collection-id-delete.md) | 0 |
| [POST /v1/collections/{collection_id}/deletion-plan](post-v1-collections-collection-id-deletion-plan.md) | 1 |
| [POST /v1/collections/{collection_id}/provenance/verification](post-v1-collections-collection-id-provenance-verification.md) | 0 |
| [POST /v1/collections/{collection_id}/tags:add](post-v1-collections-collection-id-tags-add.md) | 0 |
| [POST /v1/collections/{collection_id}/tags:remove](post-v1-collections-collection-id-tags-remove.md) | 0 |
| [POST /v1/retrieval-jobs](post-v1-retrieval-jobs.md) | 0 |
| [POST /v1/retrieval-jobs/{job_id}/ack](post-v1-retrieval-jobs-job-id-ack.md) | 0 |
| [POST /v1/retrieval-jobs/{job_id}/renew](post-v1-retrieval-jobs-job-id-renew.md) | 0 |
| [POST /v1/retrieval-plans](post-v1-retrieval-plans.md) | 0 |
| [POST /v1/retrieval-plans/{plan_id}/advance](post-v1-retrieval-plans-plan-id-advance.md) | 0 |
| [PUT /v1/apps/{app}/keys/{key_id}/access](put-v1-apps-app-keys-key-id-access.md) | 1 |
| [PUT /v1/apps/{app}/keys/{key_id}/download-quota](put-v1-apps-app-keys-key-id-download-quota.md) | 1 |
| [PUT /v1/collection-processing-claims/{claim_id}/capabilities/{capability_id}/artifacts](put-v1-collection-processing-claims-claim-id-capabilities-capability-id-artifacts.md) | 1 |
| [PUT /v1/collection-processing-claims/{claim_id}/derivation/dispositions](put-v1-collection-processing-claims-claim-id-derivation-dispositions.md) | 1 |
| [PUT /v1/collection-processing-claims/{claim_id}/derivation/output-edges](put-v1-collection-processing-claims-claim-id-derivation-output-edges.md) | 1 |
| [PUT /v1/collection-processing-claims/{claim_id}/inputs](put-v1-collection-processing-claims-claim-id-inputs.md) | 1 |
| [PUT /v1/collection-processing-claims/{claim_id}/plan/artifacts](put-v1-collection-processing-claims-claim-id-plan-artifacts.md) | 1 |
| [PUT /v1/collection-upload-sessions/{collection_id}/provenance/journals/{journal_id}](put-v1-collection-upload-sessions-collection-id-provenance-journals-journal-id.md) | 0 |
| [PUT /v1/collection-upload-sessions/{collection_id}/volumes/{volume_id}/units/{unit}](put-v1-collection-upload-sessions-collection-id-volumes-volume-id-units-unit.md) | 0 |
| [PUT /v1/collections/{collection_id}/description](put-v1-collections-collection-id-description.md) | 0 |
