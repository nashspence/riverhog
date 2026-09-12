# schemas: CollectionUploadWorkBatchDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadworkbatchdocument:1e155e37b4 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadWorkBatchDocument`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/bounded-segment/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: CollectionUploadUnitAssignmentDocument](schemas-collectionuploadunitassignmentdocument.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | maximum=64, minimum=None, reason=bounded-actionable-work-acquisition |

## Contract summary

- `title`: CollectionUploadWorkBatchDocument
- `description`: A bounded acquisition step over currently actionable upload units.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `committed_payload_bytes` | yes | integer |  |
| `complete` | yes | boolean |  |
| `planning_complete` | yes | boolean |  |
| `work` | yes | array |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1d4a81eca38f9ee605b1fa6d735fa52ca26238ca6b110b93a24ede24349ae9f2 -->

```json
{
  "additionalProperties": false,
  "description": "A bounded acquisition step over currently actionable upload units.",
  "properties": {
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "committed_payload_bytes": {
      "minimum": 0,
      "title": "Committed Payload Bytes",
      "type": "integer"
    },
    "complete": {
      "title": "Complete",
      "type": "boolean"
    },
    "planning_complete": {
      "title": "Planning Complete",
      "type": "boolean"
    },
    "work": {
      "items": {
        "$ref": "#/components/schemas/CollectionUploadUnitAssignmentDocument"
      },
      "maxItems": 64,
      "title": "Work",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "repeated-acquisition-until-complete",
        "reason": "bounded-actionable-work-acquisition"
      }
    }
  },
  "required": [
    "collection_id",
    "planning_complete",
    "complete",
    "committed_payload_bytes",
    "work"
  ],
  "title": "CollectionUploadWorkBatchDocument",
  "type": "object"
}
```
