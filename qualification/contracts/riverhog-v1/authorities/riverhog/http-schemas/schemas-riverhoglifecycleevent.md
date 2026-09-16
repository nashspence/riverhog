# schemas: RiverhogLifecycleEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-riverhoglifecycleevent:4ec3cde711 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-05d5a98472"></a>

- <a id="s-7610d187c0"></a>`discriminator`: `{"mapping":{"io.riverhog.riverhog.archive_copy.canceled":"#/components/schemas/ArchiveCopyCanceledEvent","io.riverhog.riverhog.archive_copy.completed":"#/components/schemas/ArchiveCopyCompletedEvent","io.riverhog.riverhog.archive_copy.issue":"#/components/schemas/ArchiveCopyIssueEvent","io.riverhog.riverhog.archive_copy.requested":"#/components/schemas/ArchiveCopyRequestedEvent","io.riverhog.riverhog.collection.deleted":"#/components/schemas/CollectionDeletedEvent","io.riverhog.riverhog.collection.finalized":"#/components/schemas/CollectionFinalizedEvent","io.riverhog.riverhog.retrieval.canceled":"#/components/schemas/RetrievalCanceledEvent","io.riverhog.riverhog.retrieval.completed":"#/components/schemas/RetrievalCompletedEvent","io.riverhog.riverhog.retrieval.expired":"#/components/schemas/RetrievalExpiredEvent","io.riverhog.riverhog.retrieval.failed":"#/components/schemas/RetrievalFailedEvent","io.riverhog.riverhog.retrieval.issue":"#/components/schemas/RetrievalIssueEvent","io.riverhog.riverhog.retrieval.ready":"#/components/schemas/RetrievalReadyEvent","io.riverhog.riverhog.retrieval.renewed":"#/components/schemas/RetrievalRenewedEvent","io.riverhog.riverhog.retrieval.requested":"#/components/schemas/RetrievalRequestedEvent"},"propertyName":"type"}`

### Exactly one must match (`oneOf`)

| Alternative | Schema |
|---|---|
| <a id="s-e329a936f8"></a>1 | #/components/schemas/CollectionFinalizedEvent |
| <a id="s-647809a727"></a>2 | #/components/schemas/CollectionDeletedEvent |
| <a id="s-0373d6bc05"></a>3 | #/components/schemas/ArchiveCopyRequestedEvent |
| <a id="s-a41fa3dd49"></a>4 | #/components/schemas/ArchiveCopyCompletedEvent |
| <a id="s-7e38c2bdf8"></a>5 | #/components/schemas/ArchiveCopyIssueEvent |
| <a id="s-b0544dd01d"></a>6 | #/components/schemas/ArchiveCopyCanceledEvent |
| <a id="s-91af0a56a3"></a>7 | #/components/schemas/RetrievalRequestedEvent |
| <a id="s-bd66337253"></a>8 | #/components/schemas/RetrievalReadyEvent |
| <a id="s-c4ae8c3d56"></a>9 | #/components/schemas/RetrievalRenewedEvent |
| <a id="s-20fd46f18e"></a>10 | #/components/schemas/RetrievalCompletedEvent |
| <a id="s-bd3fce7c7a"></a>11 | #/components/schemas/RetrievalCanceledEvent |
| <a id="s-aac32b2632"></a>12 | #/components/schemas/RetrievalExpiredEvent |
| <a id="s-04a9b2de8a"></a>13 | #/components/schemas/RetrievalIssueEvent |
| <a id="s-f63dc058cd"></a>14 | #/components/schemas/RetrievalFailedEvent |

## Maintained corroboration

### Referenced contract dossiers

- [ArchiveCopyCanceledEvent](schemas-archivecopycanceledevent.md)
- [ArchiveCopyCompletedEvent](schemas-archivecopycompletedevent.md)
- [ArchiveCopyIssueEvent](schemas-archivecopyissueevent.md)
- [ArchiveCopyRequestedEvent](schemas-archivecopyrequestedevent.md)
- [CollectionDeletedEvent](schemas-collectiondeletedevent.md)
- [CollectionFinalizedEvent](schemas-collectionfinalizedevent.md)
- [RetrievalCanceledEvent](schemas-retrievalcanceledevent.md)
- [RetrievalCompletedEvent](schemas-retrievalcompletedevent.md)
- [RetrievalExpiredEvent](schemas-retrievalexpiredevent.md)
- [RetrievalFailedEvent](schemas-retrievalfailedevent.md)
- [RetrievalIssueEvent](schemas-retrievalissueevent.md)
- [RetrievalReadyEvent](schemas-retrievalreadyevent.md)
- [RetrievalRenewedEvent](schemas-retrievalrenewedevent.md)
- [RetrievalRequestedEvent](schemas-retrievalrequestedevent.md)

## Governing policies

- <a id="pa-7ecc1c9f7e"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RiverhogLifecycleEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 69f21b5465255b2ea035e2798606f1662500dde6f394a0abf8e0e647836b8663 -->

```json
{
  "discriminator": {
    "mapping": {
      "io.riverhog.riverhog.archive_copy.canceled": "#/components/schemas/ArchiveCopyCanceledEvent",
      "io.riverhog.riverhog.archive_copy.completed": "#/components/schemas/ArchiveCopyCompletedEvent",
      "io.riverhog.riverhog.archive_copy.issue": "#/components/schemas/ArchiveCopyIssueEvent",
      "io.riverhog.riverhog.archive_copy.requested": "#/components/schemas/ArchiveCopyRequestedEvent",
      "io.riverhog.riverhog.collection.deleted": "#/components/schemas/CollectionDeletedEvent",
      "io.riverhog.riverhog.collection.finalized": "#/components/schemas/CollectionFinalizedEvent",
      "io.riverhog.riverhog.retrieval.canceled": "#/components/schemas/RetrievalCanceledEvent",
      "io.riverhog.riverhog.retrieval.completed": "#/components/schemas/RetrievalCompletedEvent",
      "io.riverhog.riverhog.retrieval.expired": "#/components/schemas/RetrievalExpiredEvent",
      "io.riverhog.riverhog.retrieval.failed": "#/components/schemas/RetrievalFailedEvent",
      "io.riverhog.riverhog.retrieval.issue": "#/components/schemas/RetrievalIssueEvent",
      "io.riverhog.riverhog.retrieval.ready": "#/components/schemas/RetrievalReadyEvent",
      "io.riverhog.riverhog.retrieval.renewed": "#/components/schemas/RetrievalRenewedEvent",
      "io.riverhog.riverhog.retrieval.requested": "#/components/schemas/RetrievalRequestedEvent"
    },
    "propertyName": "type"
  },
  "oneOf": [
    {
      "$ref": "#/components/schemas/CollectionFinalizedEvent"
    },
    {
      "$ref": "#/components/schemas/CollectionDeletedEvent"
    },
    {
      "$ref": "#/components/schemas/ArchiveCopyRequestedEvent"
    },
    {
      "$ref": "#/components/schemas/ArchiveCopyCompletedEvent"
    },
    {
      "$ref": "#/components/schemas/ArchiveCopyIssueEvent"
    },
    {
      "$ref": "#/components/schemas/ArchiveCopyCanceledEvent"
    },
    {
      "$ref": "#/components/schemas/RetrievalRequestedEvent"
    },
    {
      "$ref": "#/components/schemas/RetrievalReadyEvent"
    },
    {
      "$ref": "#/components/schemas/RetrievalRenewedEvent"
    },
    {
      "$ref": "#/components/schemas/RetrievalCompletedEvent"
    },
    {
      "$ref": "#/components/schemas/RetrievalCanceledEvent"
    },
    {
      "$ref": "#/components/schemas/RetrievalExpiredEvent"
    },
    {
      "$ref": "#/components/schemas/RetrievalIssueEvent"
    },
    {
      "$ref": "#/components/schemas/RetrievalFailedEvent"
    }
  ]
}
```

</details>
