# schemas: ArtifactDispositionBatchDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-artifactdispositionbatchdocument:d059522e18 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- `title`: ArtifactDispositionBatchDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `dispositions` | yes | type="array"; minItems=1; maxItems=128; items=(#/components/schemas/ArtifactDispositionDocument); additional keys=`uniqueItems`, `x-riverhog-extent` |  |
| `fence` | yes | type="integer"; minimum=1 |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | maximum=128, minimum=1, reason=bounded-disposition-append |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactDispositionDocument](schemas-artifactdispositiondocument.md)

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

- `/external_contract/http_openapi/riverhog/components/schemas/ArtifactDispositionBatchDocument`

### Exact owned JSON

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
