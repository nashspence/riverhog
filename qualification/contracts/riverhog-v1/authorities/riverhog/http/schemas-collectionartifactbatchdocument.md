# schemas: CollectionArtifactBatchDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionartifactbatchdocument:921d81467b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- `title`: CollectionArtifactBatchDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `artifacts` | yes | type="array"; minItems=1; maxItems=128; items=(#/components/schemas/CollectionArtifactIdentityDocument); additional keys=`uniqueItems`, `x-riverhog-extent` |  |
| `fence` | yes | type="integer"; minimum=1 |  |
| `start_ordinal` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | maximum=128, minimum=1, reason=bounded-authority-append |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionArtifactIdentityDocument](schemas-collectionartifactidentitydocument.md)

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

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionArtifactBatchDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 469c71d8d60e04f3a4aca348b18563579dcf35173ecb0758d55bb914324e21f3 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "artifacts": {
      "items": {
        "$ref": "#/components/schemas/CollectionArtifactIdentityDocument"
      },
      "maxItems": 128,
      "minItems": 1,
      "title": "Artifacts",
      "type": "array",
      "uniqueItems": true,
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "start_ordinal",
        "reason": "bounded-authority-append"
      }
    },
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    },
    "start_ordinal": {
      "minimum": 0,
      "title": "Start Ordinal",
      "type": "integer"
    }
  },
  "required": [
    "fence",
    "start_ordinal",
    "artifacts"
  ],
  "title": "CollectionArtifactBatchDocument",
  "type": "object"
}
```
