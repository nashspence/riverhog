# schemas: ProcessingOutcomePageDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-processingoutcomepagedocument:44efd07e7b -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingOutcomePageDocument`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/route-progression/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ExactSetAuthorityDocument](schemas-exactsetauthoritydocument.md)
- [schemas: ProcessingOutcomeIdentityDocument](schemas-processingoutcomeidentitydocument.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | maximum=128, reason=bounded-route-page |

## Contract summary

- `title`: ProcessingOutcomePageDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `authority` | yes | #/components/schemas/ExactSetAuthorityDocument |  |
| `next_ordinal` | no | object (2 fields) |  |
| `outcomes` | yes | array |  |
| `start_ordinal` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 54b774bdbd2db22e10c18834277f07064e74c84bd6c884b3bf64b5552f73aaf1 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "authority": {
      "$ref": "#/components/schemas/ExactSetAuthorityDocument"
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
    "outcomes": {
      "items": {
        "$ref": "#/components/schemas/ProcessingOutcomeIdentityDocument"
      },
      "maxItems": 128,
      "title": "Outcomes",
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
    "outcomes"
  ],
  "title": "ProcessingOutcomePageDocument",
  "type": "object"
}
```
