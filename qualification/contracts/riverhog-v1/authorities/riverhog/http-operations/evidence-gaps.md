# riverhog: HTTP Operations: evidence gaps

[Atlas](../../../index.md) · [Reference navigation](index.md)

This view covers **28 affected contract elements** within riverhog: HTTP Operations. Only their recorded evidence groups are included.

The named contract groups have recorded evidence gaps in the following guarantees. Each group's page identifies its exact open guarantees and candidate tests:

- Each step stays within its declared limits.
- Continuing the work makes progress toward its declared completion.
- The operation works across multiple pages or chunks.
- Required data or work is not silently left out.
- Work can resume after a restart as its contract requires.

These guarantees let large tasks proceed in smaller steps: a limit on one page or chunk must not become a hidden limit on the whole task. Returning a first page correctly does not establish that continuation or recovery works. Capacity may explicitly reject, defer, or throttle work; it must not silently omit work.

Existing tests may establish individual cases. The gaps retain their recorded group-wide scope and do not establish a bug in every linked contract. Completion follows each contract's rules; mutable browsing carries no implied snapshot guarantee.

The table identifies the exact evidence groups for each element. Its element link opens the local explanation and governing rules; each evidence group gives its specific open guarantees and candidate tests.

| Contract element | Evidence group |
|---|---|
| [riverhog: GET /v1/app-key-access](get-v1-app-key-access.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: GET /v1/apps](get-v1-apps.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: GET /v1/apps/{app}/keys](get-v1-apps-app-keys.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: GET /v1/archive/copy-jobs](get-v1-archive-copy-jobs.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: GET /v1/archive/stores](get-v1-archive-stores.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: GET /v1/catalog-sync/changes](get-v1-catalog-sync-changes.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: GET /v1/catalog-sync/collections](get-v1-catalog-sync-collections.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: GET /v1/catalog/collections/{collection_id}/inventory](get-v1-catalog-collections-collection-id-inventory.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: GET /v1/collection-processing-claims](get-v1-collection-processing-claims.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: GET /v1/collection-processing-claims/{claim_id}/derivation/dispositions](get-v1-collection-processing-claims-claim-id-derivation-dispositions.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: GET /v1/collection-processing-claims/{claim_id}/derivation/output-edges](get-v1-collection-processing-claims-claim-id-derivation-output-edges.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: GET /v1/collection-processing-claims/{claim_id}/inputs](get-v1-collection-processing-claims-claim-id-inputs.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: GET /v1/collection-processing-claims/{claim_id}/outcomes](get-v1-collection-processing-claims-claim-id-outcomes.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: GET /v1/collection-processing-claims/{claim_id}/plan/artifacts](get-v1-collection-processing-claims-claim-id-plan-artifacts.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: GET /v1/collection-upload-sessions](get-v1-collection-upload-sessions.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: GET /v1/collection-upload-sessions/{collection_id}/files](get-v1-collection-upload-sessions-collection-id-files.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: GET /v1/collections/{collection_id}/archive-copies](get-v1-collections-collection-id-archive-copies.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: GET /v1/collections/{collection_id}/provenance/files](get-v1-collections-collection-id-provenance-files.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: GET /v1/collections/{collection_id}/provenance/journals/{journal_id}/agents](get-v1-collections-collection-id-provenance-journals-journal-id-agents.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: GET /v1/collections/{collection_id}/provenance/trace/{path}](get-v1-collections-collection-id-provenance-trace-path.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: GET /v1/collections/{collection_id}/tags](get-v1-collections-collection-id-tags.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: GET /v1/download-quotas](get-v1-download-quotas.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: GET /v1/events](get-v1-events.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: GET /v1/retrieval-cache/objects](get-v1-retrieval-cache-objects.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: GET /v1/retrieval-plans/{plan_id}/files](get-v1-retrieval-plans-plan-id-files.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: GET /v1/search](get-v1-search.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: GET /v1/tags](get-v1-tags.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: POST /v1/collections:search](post-v1-collections-search.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
