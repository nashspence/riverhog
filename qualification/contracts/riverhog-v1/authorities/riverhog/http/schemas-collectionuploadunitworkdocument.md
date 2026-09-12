# schemas: CollectionUploadUnitWorkDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadunitworkdocument:97931cc282 -->

One exact unit and its durable upload checkpoint state.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- `title`: CollectionUploadUnitWorkDocument
- `description`: One exact unit and its durable upload checkpoint state.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `payload_bytes` | yes | type="integer"; minimum=0 |  |
| `plaintext_bytes` | yes | type="integer"; minimum=0 |  |
| `sources` | yes | type="array"; maxItems=1000; items=(#/components/schemas/CollectionUploadUnitSourceDocument); additional keys=`x-riverhog-extent` |  |
| `state` | yes | type="string"; enum=["pending","committed"] |  |
| `unit` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | maximum=1000, minimum=None, reason=bounded-upload-unit-source-map |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionUploadUnitSourceDocument](schemas-collectionuploadunitsourcedocument.md)

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/bounded-segment/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadUnitWorkDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 48b2f9a9abca13bad3e86663409b1fd07c2e03c86481f225818208887b2ac196 -->

```json
{
  "additionalProperties": false,
  "description": "One exact unit and its durable upload checkpoint state.",
  "properties": {
    "payload_bytes": {
      "minimum": 0,
      "title": "Payload Bytes",
      "type": "integer"
    },
    "plaintext_bytes": {
      "minimum": 0,
      "title": "Plaintext Bytes",
      "type": "integer"
    },
    "sources": {
      "items": {
        "$ref": "#/components/schemas/CollectionUploadUnitSourceDocument"
      },
      "maxItems": 1000,
      "title": "Sources",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "collection-volume-sequence",
        "reason": "bounded-upload-unit-source-map"
      }
    },
    "state": {
      "enum": [
        "pending",
        "committed"
      ],
      "title": "State",
      "type": "string"
    },
    "unit": {
      "minimum": 0,
      "title": "Unit",
      "type": "integer"
    }
  },
  "required": [
    "unit",
    "payload_bytes",
    "plaintext_bytes",
    "sources",
    "state"
  ],
  "title": "CollectionUploadUnitWorkDocument",
  "type": "object"
}
```
