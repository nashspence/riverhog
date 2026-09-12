# schemas: ArtifactDispositionOutputBatchDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-artifactdispositionoutputbatchdocument:4253497f48 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArtifactDispositionOutputBatchDocument`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/bounded-segment/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ArtifactDispositionOutputDocument](schemas-artifactdispositionoutputdocument.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | maximum=128, minimum=1, reason=bounded-disposition-append |

## Contract summary

- `title`: ArtifactDispositionOutputBatchDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `fence` | yes | integer |  |
| `outputs` | yes | array |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9a8553cb62301ed415596ad8bfe5a4c99594157b30d5ee67a82bc913a7e4f211 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    },
    "outputs": {
      "items": {
        "$ref": "#/components/schemas/ArtifactDispositionOutputDocument"
      },
      "maxItems": 128,
      "minItems": 1,
      "title": "Outputs",
      "type": "array",
      "uniqueItems": true,
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "sealed-disposition-authority",
        "reason": "bounded-disposition-append"
      }
    }
  },
  "required": [
    "fence",
    "outputs"
  ],
  "title": "ArtifactDispositionOutputBatchDocument",
  "type": "object"
}
```
