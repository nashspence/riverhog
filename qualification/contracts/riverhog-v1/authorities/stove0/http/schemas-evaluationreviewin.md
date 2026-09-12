# schemas: EvaluationReviewIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-evaluationreviewin:e6d7e04264 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

- `title`: EvaluationReviewIn
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `note` | no | anyOf=type="string"; minLength=1; maxLength=4000; pattern="^\\S(?:[\\s\\S]*\\S)?$" \| type="null" |  |
| `rating` | no | anyOf=type="integer"; minimum=1; maximum=5 \| type="null" |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `contract_max` | maximum=4000, minimum=1, reason=schema-maximum |
| value | schema-value | `contract_max` | maximum=5, minimum=1, reason=schema-maximum |

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/EvaluationReviewIn`

### Exact owned JSON

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
