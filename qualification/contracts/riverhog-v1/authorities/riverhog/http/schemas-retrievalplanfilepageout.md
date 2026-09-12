# schemas: RetrievalPlanFilePageOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalplanfilepageout:d5463153ff -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalPlanFilePageOut`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/route-progression/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: RetrievalPlanFileOut](schemas-retrievalplanfileout.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `segmented_no_total_max` | maximum=100, reason=bounded-route-page |
| value | schema-value | `contract_max` | maximum=10000, minimum=1, reason=schema-maximum |
| value | schema-value | `contract_max` | maximum=10000, minimum=0, reason=schema-maximum |

## Contract summary

- `title`: RetrievalPlanFilePageOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `complete` | yes | boolean |  |
| `etag` | yes | string |  |
| `files` | yes | array |  |
| `format` | yes | string |  |
| `next_ordinal` | no | object (2 fields) |  |
| `plan_id` | yes | string |  |
| `start_ordinal` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bef4038a36e91bbf83a288398920aeeb633f4507becfdc7557f1fcb611608872 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "complete": {
      "title": "Complete",
      "type": "boolean"
    },
    "etag": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Etag",
      "type": "string"
    },
    "files": {
      "items": {
        "$ref": "#/components/schemas/RetrievalPlanFileOut"
      },
      "maxItems": 100,
      "title": "Files",
      "type": "array"
    },
    "format": {
      "const": "riverhog-retrieval-plan-files/v1",
      "title": "Format",
      "type": "string"
    },
    "next_ordinal": {
      "anyOf": [
        {
          "maximum": 10000,
          "minimum": 1,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Next Ordinal"
    },
    "plan_id": {
      "title": "Plan Id",
      "type": "string"
    },
    "start_ordinal": {
      "maximum": 10000,
      "minimum": 0,
      "title": "Start Ordinal",
      "type": "integer"
    }
  },
  "required": [
    "format",
    "plan_id",
    "etag",
    "start_ordinal",
    "complete",
    "files"
  ],
  "title": "RetrievalPlanFilePageOut",
  "type": "object"
}
```
