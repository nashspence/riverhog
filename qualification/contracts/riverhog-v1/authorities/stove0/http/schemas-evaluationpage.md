# schemas: EvaluationPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-evaluationpage:0889bab8dd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

- `title`: EvaluationPage
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `evaluations` | yes | type="array"; items=(#/components/schemas/EvaluationView) |  |
| `filters` | yes | type="object"; additional keys=`additionalProperties` |  |
| `next_page_token` | yes | anyOf=#/components/schemas/BrowsePageToken \| type="null" |  |
| `order` | yes | type="string"; enum=["asc","desc"] |  |
| `page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| `sort` | yes | type="string"; enum=["updated_at","phase","evaluation_id"] |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | reason=bounded-route-page |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: EvaluationView](schemas-evaluationview.md)
- [schemas: JsonValue](schemas-jsonvalue.md)

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/route-progression/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/EvaluationPage`

### Exact owned JSON

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
