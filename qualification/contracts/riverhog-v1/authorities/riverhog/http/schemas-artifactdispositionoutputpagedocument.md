# schemas: ArtifactDispositionOutputPageDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-artifactdispositionoutputpagedocument:242468dcba -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArtifactDispositionOutputPageDocument`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/route-progression/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ArtifactDispositionOutputDocument](schemas-artifactdispositionoutputdocument.md)
- [schemas: ArtifactDispositionSetIdentityDocument](schemas-artifactdispositionsetidentitydocument.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | maximum=128, reason=bounded-route-page |

## Contract summary

- `title`: ArtifactDispositionOutputPageDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `authority` | yes | #/components/schemas/ArtifactDispositionSetIdentityDocument |  |
| `next_ordinal` | no | object (2 fields) |  |
| `outputs` | yes | array |  |
| `start_ordinal` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7fd3a513c6f8ce999a747fd64b7cf8d921fbccea66aa2295b85c0ed7540d3ac6 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "authority": {
      "$ref": "#/components/schemas/ArtifactDispositionSetIdentityDocument"
    },
    "next_ordinal": {
      "anyOf": [
        {
          "minimum": 1,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Next Ordinal"
    },
    "outputs": {
      "items": {
        "$ref": "#/components/schemas/ArtifactDispositionOutputDocument"
      },
      "maxItems": 128,
      "title": "Outputs",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "authority-bound-start_ordinal",
        "reason": "bounded-authority-page"
      }
    },
    "start_ordinal": {
      "minimum": 0,
      "title": "Start Ordinal",
      "type": "integer"
    }
  },
  "required": [
    "authority",
    "start_ordinal",
    "outputs"
  ],
  "title": "ArtifactDispositionOutputPageDocument",
  "type": "object"
}
```
