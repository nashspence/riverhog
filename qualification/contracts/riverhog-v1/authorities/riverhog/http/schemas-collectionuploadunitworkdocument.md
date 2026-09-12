# schemas: CollectionUploadUnitWorkDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadunitworkdocument:97931cc282 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadUnitWorkDocument`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/bounded-segment/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: CollectionUploadUnitSourceDocument](schemas-collectionuploadunitsourcedocument.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | maximum=1000, minimum=None, reason=bounded-upload-unit-source-map |

## Contract summary

- `title`: CollectionUploadUnitWorkDocument
- `description`: One exact unit and its durable upload checkpoint state.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `payload_bytes` | yes | integer |  |
| `plaintext_bytes` | yes | integer |  |
| `sources` | yes | array |  |
| `state` | yes | string |  |
| `unit` | yes | integer |  |

## Complete owned contract

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
