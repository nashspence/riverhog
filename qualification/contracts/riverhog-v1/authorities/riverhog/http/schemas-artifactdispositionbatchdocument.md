# schemas: ArtifactDispositionBatchDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-artifactdispositionbatchdocument:d059522e18 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArtifactDispositionBatchDocument`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/bounded-segment/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ArtifactDispositionDocument](schemas-artifactdispositiondocument.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | maximum=128, minimum=1, reason=bounded-disposition-append |

## Contract summary

- `title`: ArtifactDispositionBatchDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `dispositions` | yes | array |  |
| `fence` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b1905264e4be951db4e91aec60d3b0a67e294b4bbdd8bdaf7a98c87b545f79e3 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "dispositions": {
      "items": {
        "$ref": "#/components/schemas/ArtifactDispositionDocument"
      },
      "maxItems": 128,
      "minItems": 1,
      "title": "Dispositions",
      "type": "array",
      "uniqueItems": true,
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "sealed-disposition-authority",
        "reason": "bounded-disposition-append"
      }
    },
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    }
  },
  "required": [
    "fence",
    "dispositions"
  ],
  "title": "ArtifactDispositionBatchDocument",
  "type": "object"
}
```
