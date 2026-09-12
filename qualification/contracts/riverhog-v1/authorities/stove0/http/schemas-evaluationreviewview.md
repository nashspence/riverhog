# schemas: EvaluationReviewView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-evaluationreviewview:969db1f909 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 5 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/EvaluationReviewView`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `contract_max` | maximum=4000, minimum=1, reason=schema-maximum |
| value | schema-value | `contract_max` | maximum=5, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=40, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=160, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=160, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: EvaluationReviewView
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `note` | no | object (2 fields) |  |
| `rating` | no | object (2 fields) |  |
| `updated_at` | yes | string |  |
| `updated_by` | yes | string |  |
| `variant_id` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b523bdd6e34cda0cc29b30ba551749cf3dc07b5c971d74a87b8906f5961e1117 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "note": {
      "anyOf": [
        {
          "maxLength": 4000,
          "minLength": 1,
          "pattern": "^\\S(?:[\\s\\S]*\\S)?$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Note"
    },
    "rating": {
      "anyOf": [
        {
          "maximum": 5,
          "minimum": 1,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Rating"
    },
    "updated_at": {
      "maxLength": 40,
      "minLength": 1,
      "title": "Updated At",
      "type": "string"
    },
    "updated_by": {
      "maxLength": 160,
      "minLength": 1,
      "title": "Updated By",
      "type": "string"
    },
    "variant_id": {
      "maxLength": 160,
      "minLength": 1,
      "title": "Variant Id",
      "type": "string"
    }
  },
  "required": [
    "variant_id",
    "updated_by",
    "updated_at"
  ],
  "title": "EvaluationReviewView",
  "type": "object"
}
```
