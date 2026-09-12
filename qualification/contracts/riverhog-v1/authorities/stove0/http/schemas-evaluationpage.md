# schemas: EvaluationPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-evaluationpage:0889bab8dd -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/EvaluationPage`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/route-progression/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: EvaluationView](schemas-evaluationview.md)
- [schemas: JsonValue](schemas-jsonvalue.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | reason=bounded-route-page |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: EvaluationPage
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `evaluations` | yes | array |  |
| `filters` | yes | object |  |
| `next_page_token` | yes | object (1 fields) |  |
| `order` | yes | string |  |
| `page_size` | yes | integer |  |
| `sort` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cee0d603fb538ed2c5a3cbf91eac88d0608b0b76d76e243725d7daca7f568a9d -->

```json
{
  "additionalProperties": false,
  "properties": {
    "evaluations": {
      "items": {
        "$ref": "#/components/schemas/EvaluationView"
      },
      "title": "Evaluations",
      "type": "array"
    },
    "filters": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Filters",
      "type": "object"
    },
    "next_page_token": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/BrowsePageToken"
        },
        {
          "type": "null"
        }
      ]
    },
    "order": {
      "enum": [
        "asc",
        "desc"
      ],
      "title": "Order",
      "type": "string"
    },
    "page_size": {
      "maximum": 100,
      "minimum": 1,
      "title": "Page Size",
      "type": "integer"
    },
    "sort": {
      "enum": [
        "updated_at",
        "phase",
        "evaluation_id"
      ],
      "title": "Sort",
      "type": "string"
    }
  },
  "required": [
    "page_size",
    "next_page_token",
    "sort",
    "order",
    "filters",
    "evaluations"
  ],
  "title": "EvaluationPage",
  "type": "object"
}
```
