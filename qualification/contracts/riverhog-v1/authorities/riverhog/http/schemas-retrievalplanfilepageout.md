# schemas: RetrievalPlanFilePageOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalplanfilepageout:d5463153ff -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

- `title`: RetrievalPlanFilePageOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `complete` | yes | type="boolean" |  |
| `etag` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `files` | yes | type="array"; maxItems=100; items=(#/components/schemas/RetrievalPlanFileOut) |  |
| `format` | yes | type="string"; const="riverhog-retrieval-plan-files/v1" |  |
| `next_ordinal` | no | anyOf=type="integer"; minimum=1; maximum=10000 \| type="null" |  |
| `plan_id` | yes | type="string" |  |
| `start_ordinal` | yes | type="integer"; minimum=0; maximum=10000 |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `segmented_no_total_max` | maximum=100, reason=bounded-route-page |
| value | schema-value | `contract_max` | maximum=10000, minimum=1, reason=schema-maximum |
| value | schema-value | `contract_max` | maximum=10000, minimum=0, reason=schema-maximum |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: RetrievalPlanFileOut](schemas-retrievalplanfileout.md)

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/route-progression/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalPlanFilePageOut`

### Exact owned JSON

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
