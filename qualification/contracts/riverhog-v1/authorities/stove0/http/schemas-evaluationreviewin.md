# schemas: EvaluationReviewIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-evaluationreviewin:e6d7e04264 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/EvaluationReviewIn`

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

## Contract summary

- `title`: EvaluationReviewIn
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `note` | no | object (2 fields) |  |
| `rating` | no | object (2 fields) |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 22ba7fe36a5ebd9409bb48d166bbeac955c2bf05eeb07b27d5a6185157f277a3 -->

```json
{
  "additionalProperties": false,
  "anyOf": [
    {
      "properties": {
        "rating": {
          "type": "integer"
        }
      },
      "required": [
        "rating"
      ]
    },
    {
      "properties": {
        "note": {
          "type": "string"
        }
      },
      "required": [
        "note"
      ]
    }
  ],
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
    }
  },
  "title": "EvaluationReviewIn",
  "type": "object"
}
```
