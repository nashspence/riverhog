# schemas: EvaluationView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-evaluationview:2ec5062688 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/EvaluationView`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: EvaluationChildView](schemas-evaluationchildview.md)
- [schemas: EvaluationDefinition](schemas-evaluationdefinition.md)
- [schemas: EvaluationReviewView](schemas-evaluationreviewview.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: EvaluationView
- `description`: Operator projection of a materialized evaluation, not its identity.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `children` | yes | array |  |
| `definition` | yes | #/components/schemas/EvaluationDefinition |  |
| `evaluation_id` | yes | string |  |
| `format` | no | string |  |
| `phase` | yes | string |  |
| `reviews` | no | array |  |
| `revision` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 94f5278bcc3670da961904a0eec89e1172e5874922b8ab76ff47c80dae0703e3 -->

```json
{
  "additionalProperties": false,
  "description": "Operator projection of a materialized evaluation, not its identity.",
  "properties": {
    "children": {
      "items": {
        "$ref": "#/components/schemas/EvaluationChildView"
      },
      "title": "Children",
      "type": "array"
    },
    "definition": {
      "$ref": "#/components/schemas/EvaluationDefinition"
    },
    "evaluation_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Evaluation Id",
      "type": "string"
    },
    "format": {
      "const": "stove0-evaluation-view/v1",
      "default": "stove0-evaluation-view/v1",
      "title": "Format",
      "type": "string"
    },
    "phase": {
      "enum": [
        "planning",
        "running",
        "partially_complete",
        "complete",
        "failed",
        "canceled"
      ],
      "title": "Phase",
      "type": "string"
    },
    "reviews": {
      "default": [],
      "items": {
        "$ref": "#/components/schemas/EvaluationReviewView"
      },
      "title": "Reviews",
      "type": "array"
    },
    "revision": {
      "minimum": 1,
      "title": "Revision",
      "type": "integer"
    }
  },
  "required": [
    "evaluation_id",
    "definition",
    "phase",
    "revision",
    "children"
  ],
  "title": "EvaluationView",
  "type": "object"
}
```
