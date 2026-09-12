# schemas: CollectionUploadWorkBatchDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadworkbatchdocument:1e155e37b4 -->

A bounded acquisition step over currently actionable upload units.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-7ff4df50566a"></a>
- <a id="s-846950f272d7"></a>`title`: CollectionUploadWorkBatchDocument
- <a id="s-e3b8f0d45d04"></a>`description`: A bounded acquisition step over currently actionable upload units.
- <a id="s-cd8632738e2d"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0f1e40b93bd3"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-86f4d750af84"></a>`committed_payload_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-9c4aca2092ed"></a>`complete` | yes | type="boolean" |  |
| <a id="s-b5d308340306"></a>`planning_complete` | yes | type="boolean" |  |
| <a id="s-4617d6f48d3e"></a>`work` | yes | type="array"; maxItems=64; items=(#/components/schemas/CollectionUploadUnitAssignmentDocument); additional keys=`x-riverhog-extent` |  |

### Progression, limits, and lifecycle

#### [extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594af)

Shared facts for every subject below: maximum=64; minimum=null; progression={"progression":"repeated-acquisition-until-complete"}; reason="bounded-actionable-work-acquisition"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field work](#s-4617d6f48d3e) | `cardinality · items · segmented_no_total_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: CollectionUploadUnitAssignmentDocument](schemas-collectionuploadunitassignmentdocument.md)

## Governing policies

- <a id="pa-3265c3e570b3"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-2f266e77ef30"></a>[extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594af)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadWorkBatchDocument`

### Exact owned JSON

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
