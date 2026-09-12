# schemas: RiverhogLifecycleEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-riverhoglifecycleevent:ced69167d5 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RiverhogLifecycleEvent`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ArchiveCopyCanceledEvent](schemas-archivecopycanceledevent.md)
- [schemas: ArchiveCopyCompletedEvent](schemas-archivecopycompletedevent.md)
- [schemas: ArchiveCopyIssueEvent](schemas-archivecopyissueevent.md)
- [schemas: ArchiveCopyRequestedEvent](schemas-archivecopyrequestedevent.md)
- [schemas: CollectionDeletedEvent](schemas-collectiondeletedevent.md)
- [schemas: CollectionFinalizedEvent](schemas-collectionfinalizedevent.md)
- [schemas: RetrievalCanceledEvent](schemas-retrievalcanceledevent.md)
- [schemas: RetrievalCompletedEvent](schemas-retrievalcompletedevent.md)
- [schemas: RetrievalExpiredEvent](schemas-retrievalexpiredevent.md)
- [schemas: RetrievalFailedEvent](schemas-retrievalfailedevent.md)
- [schemas: RetrievalIssueEvent](schemas-retrievalissueevent.md)
- [schemas: RetrievalReadyEvent](schemas-retrievalreadyevent.md)
- [schemas: RetrievalRenewedEvent](schemas-retrievalrenewedevent.md)
- [schemas: RetrievalRequestedEvent](schemas-retrievalrequestedevent.md)

## Contract summary

| Field | Shape |
|---|---|
| `discriminator` | object (2 fields) |
| `oneOf` | array (14 items) |

## Complete owned contract

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
